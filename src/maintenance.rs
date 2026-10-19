//! Background compaction, TTL expiry sweeping, and index-snapshot scheduling.

use crate::api::write::local_sweep_expired_batch;
use crate::cluster::ownership::Ownership;
use crate::consensus::config::is_system_collection;
use crate::replication::{WriteConcern, DEFAULT_WTIMEOUT_MS};
use crate::state::AppState;
use crate::storage::{Collection, Retention, SpaceUsage};
use serde::Deserialize;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

const SWEEP_BATCH_SIZE: usize = 64;

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceConfig {
    #[serde(default = "default_maintenance_enabled")]
    pub enabled: bool,
    #[serde(default = "default_maintenance_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_compaction_dead_ratio")]
    pub compaction_dead_ratio: f64,
    #[serde(default = "default_compaction_min_bytes")]
    pub compaction_min_wal_bytes: u64,
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval_secs: u64,
    #[serde(default = "default_wal_retention_bytes")]
    pub wal_retention_bytes: u64,
}

fn default_maintenance_enabled() -> bool { true }
fn default_maintenance_interval() -> u64 { 60 }
fn default_compaction_dead_ratio() -> f64 { 0.4 }
fn default_compaction_min_bytes() -> u64 { 8 * 1024 * 1024 }
fn default_snapshot_interval() -> u64 { 300 }
// A little over one WAL rotation, so a replica lagging by up to a whole rotation still repairs
// from frames. Past it the tail costs more to carry than the snapshot it saves.
fn default_wal_retention_bytes() -> u64 { 64 * 1024 * 1024 }

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: default_maintenance_enabled(),
            interval_secs: default_maintenance_interval(),
            compaction_dead_ratio: default_compaction_dead_ratio(),
            compaction_min_wal_bytes: default_compaction_min_bytes(),
            snapshot_interval_secs: default_snapshot_interval(),
            wal_retention_bytes: default_wal_retention_bytes(),
        }
    }
}

impl MaintenanceConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=1.0).contains(&self.compaction_dead_ratio) {
            return Err(format!("maintenance.compaction_dead_ratio must be between 0.0 and 1.0, got {}", self.compaction_dead_ratio));
        }
        if self.enabled && self.interval_secs == 0 {
            return Err("maintenance.interval_secs must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// Compaction breaks a replica's chain and forces a full snapshot resync on the next repair, so this is
/// leader-only. A mostly-dead but tiny log is not worth rewriting: the ratio and the floor both hold.
fn should_compact(usage: &SpaceUsage, cfg: &MaintenanceConfig, is_leader: bool) -> bool {
    is_leader
        && usage.total_bytes >= cfg.compaction_min_wal_bytes
        && usage.dead_ratio() >= cfg.compaction_dead_ratio
}

/// What a compaction of this collection must not destroy: the frames the furthest-behind target needs,
/// bounded by `max_bytes`. `min_reclaim_bytes` is the scheduler's guard only -- `/compact` always runs.
pub fn retention_for(state: &AppState, collection: &str, cfg: &MaintenanceConfig, scheduled: bool)
    -> Retention
{
    let targets = state.replication_targets();
    if targets.is_empty() {
        return Retention::none();
    }
    Retention {
        above_lsn: targets.iter().map(|t| state.matched_lsn(t, collection)).min().unwrap_or(0),
        max_bytes: cfg.wal_retention_bytes,
        min_reclaim_bytes: if scheduled { cfg.compaction_min_wal_bytes } else { 0 },
    }
}

/// Sweeps expired keys on the leader through the normal replicated delete path.
pub async fn sweep_collection(state: &AppState, name: &str, col: &Arc<Collection>) -> usize {
    if !state.is_leader() || is_system_collection(name) || col.is_dropped() {
        return 0;
    }
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    let ownership = state.scan_ownership(name);
    let expired = col.expired_keys(now_ms, &|k| ownership.includes(k));
    if expired.is_empty() {
        return 0;
    }

    let mut total_deleted = 0;
    for chunk in expired.chunks(SWEEP_BATCH_SIZE) {
        if !state.is_leader() || col.is_dropped() {
            break;
        }
        let _gate = state.write_gate.read().await;
        let mut owned_chunk = Vec::with_capacity(chunk.len());
        for (k, exp) in chunk {
            if matches!(state.ownership(name, k), None | Some(Ownership::Ours)) {
                owned_chunk.push((k.clone(), *exp));
            }
        }
        if !owned_chunk.is_empty() {
            match local_sweep_expired_batch(
                state,
                name,
                owned_chunk,
                WriteConcern::Majority,
                Duration::from_millis(DEFAULT_WTIMEOUT_MS),
            ).await {
                Ok(n) => total_deleted += n,
                Err(resp) => {
                    warn!(target: "maintenance", collection = %name, status = %resp.status(), "TTL sweep batch failed");
                    break;
                }
            }
        }
        drop(_gate);
        tokio::task::yield_now().await;
    }
    if total_deleted > 0 {
        info!(target: "maintenance", collection = %name, expired = total_deleted, "Swept expired documents");
    }
    total_deleted
}

// Index snapshots are not gated: they add a file and remove nothing, and a replica that never
// snapshots replays every WAL at boot.
pub fn maintenance_task(state: AppState, cfg: MaintenanceConfig) {
    let db = match state.db.as_ref() {
        Some(db) => db.clone(),
        None => return,
    };
    tokio::spawn(async move {
        info!(target: "maintenance", interval_secs = cfg.interval_secs,
            dead_ratio = cfg.compaction_dead_ratio, min_wal_bytes = cfg.compaction_min_wal_bytes,
            snapshot_interval_secs = cfg.snapshot_interval_secs, "Scheduler active");

        let mut last_snapshot: HashMap<String, (std::time::Instant, u64)> = HashMap::new();

        loop {
            tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;

            let collections: Vec<(String, Arc<Collection>)> = {
                db.collections.read().unwrap().iter().map(|(n, c)| (n.clone(), c.clone())).collect()
            };

            for (name, col) in collections {
                if col.released.load(Ordering::SeqCst) {
                    last_snapshot.remove(&name);
                    continue;
                }

                if state.is_leader() && !is_system_collection(&name) {
                    sweep_collection(&state, &name, &col).await;
                }

                let probe = col.clone();
                let usage = match tokio::task::spawn_blocking(move || probe.space_usage()).await {
                    Ok(Ok(u)) => u,
                    Ok(Err(e)) => {
                        warn!(target: "maintenance", "Could not measure '{}': {}", name, e);
                        continue;
                    },
                    Err(e) => {
                        warn!(target: "maintenance", "Measurement task failed for '{}': {}", name, e);
                        continue;
                    }
                };

                let compacted = if should_compact(&usage, &cfg, state.is_leader()) {
                    info!(target: "maintenance", collection = %name, dead_ratio = usage.dead_ratio(),
                        dead_bytes = usage.dead_bytes(), total_bytes = usage.total_bytes,
                        live_keys = usage.live_keys, "Dead-byte threshold reached; compacting");

                    let target = col.clone();
                    let retention = retention_for(&state, &name, &cfg, true);
                    match tokio::task::spawn_blocking(move || target.compact(retention)).await {
                        Ok(Ok(())) => true,
                        Ok(Err(ref e)) if e.kind() == io::ErrorKind::WouldBlock => false,
                        Ok(Err(e)) => {
                            warn!(target: "maintenance", "Compaction of '{}' failed: {}", name, e);
                            false
                        },
                        Err(e) => {
                            error!(target: "maintenance", "Compaction task for '{}' panicked: {}", name, e);
                            false
                        }
                    }
                } else {
                    false
                };

                let due = match last_snapshot.get(&name) {
                    Some((at, _)) => at.elapsed().as_secs() >= cfg.snapshot_interval_secs,
                    None => true,
                };
                let last_saved_lsn = last_snapshot.get(&name).map(|(_, l)| *l);
                let current_lsn = col.last_appended_lsn();

                if !(due || compacted) {
                    continue;
                }
                if !compacted && last_saved_lsn == Some(current_lsn) {
                    last_snapshot.insert(name, (std::time::Instant::now(), current_lsn));
                    continue;
                }

                let target = col.clone();
                match tokio::task::spawn_blocking(move || target.save_index()).await {
                    Ok(Ok(saved_lsn)) => {
                        last_snapshot.insert(name, (std::time::Instant::now(), saved_lsn));
                    },
                    Ok(Err(e)) => warn!(target: "maintenance", "Snapshot of '{}' failed: {}", name, e),
                    Err(e) => error!(target: "maintenance", "Snapshot task for '{}' panicked: {}", name, e),
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;

    fn maint(ratio: f64, min_bytes: u64) -> MaintenanceConfig {
        MaintenanceConfig {
            enabled: true,
            interval_secs: 60,
            compaction_dead_ratio: ratio,
            compaction_min_wal_bytes: min_bytes,
            snapshot_interval_secs: 300,
            wal_retention_bytes: default_wal_retention_bytes(),
        }
    }

    /// The floor is the *furthest behind* target, not the average or the nearest: keeping frames
    /// only the fastest replica still needs protects nobody.
    #[tokio::test]
    async fn the_floor_is_the_furthest_behind_replication_target() {
        use crate::test_support::{next_test_port, temp_root, TestNode};

        let root = temp_root();
        let (ahead, behind) = (
            format!("http://127.0.0.1:{}", next_test_port()),
            format!("http://127.0.0.1:{}", next_test_port()),
        );
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.replicas = vec![ahead.clone(), behind.clone()];
        leader.start();
        let state = leader.state.clone().unwrap();
        let cfg = maint(0.4, 8 << 20);

        {
            let repl = state.replication.as_ref().unwrap();
            let mut g = repl.write().unwrap();
            g.progress.observe_ack(&ahead, "c", 900);
            g.progress.observe_ack(&behind, "c", 12);
        }

        let r = retention_for(&state, "c", &cfg, true);
        assert_eq!(r.above_lsn, 12, "the replica at 900 needs nothing the one at 12 does not");
        assert_eq!(r.max_bytes, cfg.wal_retention_bytes);
        assert_eq!(r.min_reclaim_bytes, cfg.compaction_min_wal_bytes);

        assert_eq!(retention_for(&state, "never-written", &cfg, true).above_lsn, 0,
            "a collection no target has acked counts as needing all of it, and the budget bounds it");
        assert_eq!(retention_for(&state, "c", &cfg, false).min_reclaim_bytes, 0,
            "an operator asking for the space back gets the rewrite either way");

        leader.kill();
    }

    #[tokio::test]
    async fn a_node_with_no_replication_target_retains_nothing() {
        use crate::test_support::{next_test_port, temp_root, TestNode};

        let root = temp_root();
        let mut solo = TestNode::new("solo", next_test_port(), &root, "primary");
        solo.start();
        let state = solo.state.clone().unwrap();

        let r = retention_for(&state, "c", &maint(0.4, 8 << 20), true);
        assert_eq!(r.max_bytes, 0, "nothing downstream to protect is the pre-M15 behaviour");
        assert_eq!(r.min_reclaim_bytes, 0, "and so nothing to weigh a rewrite against either");

        solo.kill();
    }

    #[test]
    fn compaction_trigger_respects_both_ratio_and_floor() {
        let cfg = maint(0.4, 1000);

        let mostly_dead_but_tiny = SpaceUsage { total_bytes: 900, live_bytes: 10, live_keys: 1 };
        assert!(!should_compact(&mostly_dead_but_tiny, &cfg, true),
            "a log under the byte floor must not be compacted no matter how dead it is");

        let big_but_fresh = SpaceUsage { total_bytes: 10_000, live_bytes: 9_000, live_keys: 10 };
        assert!(!should_compact(&big_but_fresh, &cfg, true), "10% dead is below the 40% threshold");

        let big_and_dead = SpaceUsage { total_bytes: 10_000, live_bytes: 6_000, live_keys: 10 };
        assert!(should_compact(&big_and_dead, &cfg, true), "exactly at the threshold must trigger");

        let empty = SpaceUsage { total_bytes: 0, live_bytes: 0, live_keys: 0 };
        assert_eq!(empty.dead_ratio(), 0.0, "an empty log must not divide by zero");
        assert!(!should_compact(&empty, &cfg, true));
    }

    #[test]
    fn a_replica_never_compacts_however_dead_its_log() {
        let cfg = maint(0.4, 1000);
        let past_every_threshold = SpaceUsage { total_bytes: 10_000_000, live_bytes: 1, live_keys: 1 };

        assert!(should_compact(&past_every_threshold, &cfg, true));
        assert!(!should_compact(&past_every_threshold, &cfg, false),
            "compaction drops superseded frames, which breaks the chain repair streams over");
    }

    /// The pure decision above can be correct while the loop passes a constant, so this drives the
    /// real scheduler on a replica and on a leader over the same log.
    #[tokio::test]
    async fn the_scheduler_compacts_only_where_it_leads_but_snapshots_everywhere() {
        use crate::storage::index::INDEX_FILENAME;
        use crate::storage::Database;
        use crate::test_support::{live_put, temp_root, wait_for};

        async fn tick(is_leader: bool) -> (u64, bool) {
            let root = temp_root();
            let db = Arc::new(Database::new(&root).unwrap());
            let col = db.get_collection("c").unwrap();
            for v in 1..=20 {
                live_put(&col, "a", v);
            }
            let churned = col.space_usage().unwrap();
            assert!(churned.dead_bytes() > 0, "the log must be worth compacting for this to test anything");

            let config: NodeConfig = serde_json::from_value(serde_json::json!({
                "node_id": "n1", "role": "shard", "shard_role": "primary",
                "listen_addr": "127.0.0.1:1", "data_dir": root.to_string_lossy(),
            })).unwrap();
            let state = crate::state::AppState::for_admission_test(config, db.clone(), is_leader);

            let mut cfg = maint(0.1, 0);
            cfg.interval_secs = 1;
            cfg.snapshot_interval_secs = 0;
            maintenance_task(state, cfg);

            let snapshot = root.join("c").join(INDEX_FILENAME);
            let probe = col.clone();
            wait_for(Duration::from_secs(6), || {
                probe.space_usage().map(|u| u.dead_bytes() == 0).unwrap_or(false) && snapshot.exists()
            }).await;

            let dead = col.space_usage().unwrap().dead_bytes();
            let snapshotted = snapshot.exists();
            (dead, snapshotted)
        }

        let (leader_dead, leader_snapshot) = tick(true).await;
        assert_eq!(leader_dead, 0, "the leader's scheduler must reclaim the dead bytes");
        assert!(leader_snapshot);

        let (replica_dead, replica_snapshot) = tick(false).await;
        assert!(replica_dead > 0, "a replica's scheduler must leave the log alone");
        assert!(replica_snapshot,
            "but it must still snapshot, or it replays every WAL at boot for no reason");
    }

    #[test]
    fn maintenance_config_defaults_and_validation() {
        let cfg = MaintenanceConfig::default();
        assert!(cfg.enabled);
        assert!(cfg.validate().is_ok());

        let parsed: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1"}"#).unwrap();
        assert!(parsed.maintenance.enabled, "maintenance must default on when the block is absent");
        assert_eq!(parsed.maintenance.compaction_dead_ratio, 0.4);

        let partial: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1",
                "maintenance":{"compaction_dead_ratio":0.75}}"#).unwrap();
        assert_eq!(partial.maintenance.compaction_dead_ratio, 0.75);
        assert_eq!(partial.maintenance.snapshot_interval_secs, 300, "unspecified knobs keep their defaults");

        let bad: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1",
                "maintenance":{"compaction_dead_ratio":1.5}}"#).unwrap();
        assert!(bad.validate().is_err(), "an out-of-range ratio must be rejected at boot");

        let zero: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1",
                "maintenance":{"interval_secs":0}}"#).unwrap();
        assert!(zero.validate().is_err(), "a zero interval would spin the scheduler");
    }

    #[tokio::test]
    async fn ttl_and_expire_at_lifecycle_crud_patch_bulk_and_precedence() {
        use crate::test_support::{next_test_port, temp_root, TestNode};
        use reqwest::StatusCode;

        let root = temp_root();
        let mut node = TestNode::new("solo", next_test_port(), &root, "primary");
        node.start();
        let url = node.url();
        let client = reqwest::Client::new();
        let state = node.state.clone().unwrap();

        let r = client.post(format!("{}/collections/c/docs", url))
            .json(&serde_json::json!({"value": {"v": 1}, "expire_at": 0}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        let r = client.put(format!("{}/collections/c/docs/bad", url))
            .json(&serde_json::json!({"value": {"v": 1}, "expire_at": 0}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        let r = client.put(format!("{}/collections/c/docs/keep", url))
            .json(&serde_json::json!({"value": {"v": 1}}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);

        let r = client.put(format!("{}/collections/c/ttl", url))
            .json(&serde_json::json!({"ttl_secs": 0}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        let r = client.patch(format!("{}/collections/c/docs/keep", url))
            .json(&serde_json::json!({"value": {"v": 2}, "expire_at": 0}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        let r = client.post(format!("{}/collections/c/docs/bulk", url))
            .json(&serde_json::json!([{"id": "b1", "value": {"v": 1}, "expire_at": 0}]))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        let r = client.put(format!("{}/collections/c/docs/past", url))
            .json(&serde_json::json!({"value": {"v": 10}, "expire_at": 1000}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);

        // A PATCH without `expire_at` keeps the one the document has.
        let r = client.patch(format!("{}/collections/c/docs/past", url))
            .json(&serde_json::json!({"value": {"v": 11}}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        // Expiry is not a read filter: only the sweep removes the document.
        let r = client.get(format!("{}/collections/c/docs/past", url)).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.json::<serde_json::Value>().await.unwrap()["v"], 11);

        let far_future = 9_999_999_999_999u64;
        let r = client.post(format!("{}/collections/c/docs/bulk", url))
            .json(&serde_json::json!([
                {"id": "bulk_past", "value": {"v": 1}, "expire_at": 2000},
                {"id": "bulk_future", "value": {"v": 2}, "expire_at": far_future}
            ]))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);

        let col = state.db.as_ref().unwrap().get_collection("c").unwrap();
        let swept = sweep_collection(&state, "c", &col).await;
        assert_eq!(swept, 2);

        assert_eq!(client.get(format!("{}/collections/c/docs/past", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
        assert_eq!(client.get(format!("{}/collections/c/docs/bulk_past", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
        assert_eq!(client.get(format!("{}/collections/c/docs/bulk_future", url)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(client.get(format!("{}/collections/c/docs/keep", url)).send().await.unwrap().status(), StatusCode::OK);

        let r = client.get(format!("{}/collections/c/ttl", url)).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.json::<serde_json::Value>().await.unwrap()["ttl_secs"], serde_json::Value::Null);

        let r = client.put(format!("{}/collections/c/ttl", url))
            .json(&serde_json::json!({"ttl_secs": 3600}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        let r = client.get(format!("{}/collections/c/ttl", url)).send().await.unwrap();
        assert_eq!(r.json::<serde_json::Value>().await.unwrap()["ttl_secs"], 3600);

        // A document's own `expire_at` wins over the collection TTL, earlier or later.
        let r = client.put(format!("{}/collections/c/docs/override_short", url))
            .json(&serde_json::json!({"value": {"v": 1}, "expire_at": 500}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
        assert_eq!(sweep_collection(&state, "c", &col).await, 1);
        assert_eq!(client.get(format!("{}/collections/c/docs/override_short", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

        client.put(format!("{}/collections/c/ttl", url))
            .json(&serde_json::json!({"ttl_secs": 1}))
            .send().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        // `keep` has no `expire_at`, so the collection TTL applies to it.
        assert_eq!(sweep_collection(&state, "c", &col).await, 1);
        assert_eq!(client.get(format!("{}/collections/c/docs/keep", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
        assert_eq!(client.get(format!("{}/collections/c/docs/bulk_future", url)).send().await.unwrap().status(), StatusCode::OK);

        let r = client.delete(format!("{}/collections/c/ttl", url)).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let r = client.get(format!("{}/collections/c/ttl", url)).send().await.unwrap();
        assert_eq!(r.json::<serde_json::Value>().await.unwrap()["ttl_secs"], serde_json::Value::Null);

        node.kill();
    }

    #[tokio::test]
    async fn sweep_emits_tombstones_replicates_appears_on_cdc_and_guards_against_concurrent_overwrite() {
        use crate::api::write::local_sweep_expired_batch;
        use crate::replication::WriteConcern;
        use crate::test_support::{
            stream_client, temp_root, voter_group, wait_for, SseTap, DEFAULT_HEARTBEAT_TIMEOUT_SECS,
        };
        use reqwest::StatusCode;

        let root = temp_root();
        let mut nodes = voter_group(&root, 2, DEFAULT_HEARTBEAT_TIMEOUT_SECS).await;
        let leader_url = nodes[0].url();
        let client = reqwest::Client::new();

        let r = client.put(format!("{}/collections/c/docs/exp1?w=majority", leader_url))
            .json(&serde_json::json!({"value": {"v": 1}, "expire_at": 1000}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);

        // A candidate sampled before an overwrite must not delete the new version.
        let leader_state = nodes[0].state.clone().unwrap();
        let col = leader_state.db.as_ref().unwrap().get_collection("c").unwrap();
        let stale_candidates = col.expired_keys(5000, &|_| true);
        assert_eq!(stale_candidates.len(), 1);

        let r = client.put(format!("{}/collections/c/docs/exp1?w=majority", leader_url))
            .json(&serde_json::json!({"value": {"v": 2}, "expire_at": 9_999_999_999_999u64}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        let deleted = local_sweep_expired_batch(
            &leader_state,
            "c",
            stale_candidates,
            WriteConcern::Majority,
            Duration::from_secs(5),
        ).await.unwrap();
        assert_eq!(deleted, 0, "stale sweep candidate must not delete a concurrently overwritten key");
        assert_eq!(client.get(format!("{}/collections/c/docs/exp1", leader_url)).send().await.unwrap().status(), StatusCode::OK);

        // The feed records only while subscribed, so the tap opens before `exp2` is written.
        let opened = stream_client().get(format!("{}/collections/c/changes", leader_url))
            .send().await.unwrap();
        assert_eq!(opened.status(), StatusCode::OK);
        let tap = SseTap::open(opened);
        assert_eq!(tap.wait_for_events("open", 1, Duration::from_secs(5)).await.len(), 1);

        let r = client.put(format!("{}/collections/c/docs/exp2?w=majority", leader_url))
            .json(&serde_json::json!({"value": {"v": 99}, "expire_at": 1000}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);

        let swept = sweep_collection(&leader_state, "c", &col).await;
        assert_eq!(swept, 1);

        let rep_state = nodes[1].state.clone().unwrap();
        wait_for(Duration::from_secs(5), || {
            rep_state.db.as_ref().unwrap().get_collection("c").unwrap().get("exp2").unwrap().is_none()
        }).await;
        assert!(rep_state.db.as_ref().unwrap().get_collection("c").unwrap().get("exp2").unwrap().is_none());

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let is_sweep = |e: &crate::test_support::SseEvent| e.data["key"] == "exp2" && e.data["op"] == "delete";
        while std::time::Instant::now() < deadline && !tap.named("change").iter().any(is_sweep) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(tap.named("change").iter().any(is_sweep),
            "expiry sweep must appear on the change feed as an ordinary delete: {:?}", tap.events());
        drop(tap);

        for n in &mut nodes {
            n.kill();
        }
    }

    #[tokio::test]
    async fn router_fanout_ttl_and_bulk_expire_at_across_shards() {
        use crate::test_support::{sharded_cluster, temp_root};
        use reqwest::StatusCode;

        let root = temp_root();
        let (mut shards, mut router) = sharded_cluster(&root, 2).await;
        let rurl = router.url();
        let client = reqwest::Client::new();

        let r = client.post(format!("{}/collections/c/docs/bulk", rurl))
            .json(&serde_json::json!([
                {"id": "k1", "value": {"v": 1}, "expire_at": 1000},
                {"id": "k2", "value": {"v": 2}, "expire_at": 1000},
                {"id": "k3", "value": {"v": 3}, "expire_at": 9_999_999_999_999u64}
            ]))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);

        let r = client.put(format!("{}/collections/c/ttl", rurl))
            .json(&serde_json::json!({"ttl_secs": 600}))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        let r = client.get(format!("{}/collections/c/ttl", rurl)).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.json::<serde_json::Value>().await.unwrap()["ttl_secs"], 600);

        for s in &shards {
            let col = s.state.as_ref().unwrap().db.as_ref().unwrap().get_collection("c").unwrap();
            assert_eq!(col.committed_ttl(), Some(600));
            sweep_collection(s.state.as_ref().unwrap(), "c", &col).await;
        }

        assert_eq!(client.get(format!("{}/collections/c/docs/k1", rurl)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
        assert_eq!(client.get(format!("{}/collections/c/docs/k2", rurl)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
        assert_eq!(client.get(format!("{}/collections/c/docs/k3", rurl)).send().await.unwrap().status(), StatusCode::OK);

        let r = client.delete(format!("{}/collections/c/ttl", rurl)).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let r = client.get(format!("{}/collections/c/ttl", rurl)).send().await.unwrap();
        assert_eq!(r.json::<serde_json::Value>().await.unwrap()["ttl_secs"], serde_json::Value::Null);

        router.kill();
        for s in &mut shards {
            s.kill();
        }
    }

    #[tokio::test]
    async fn wal_replay_and_snapshot_preserve_ttl_and_expire_at() {
        use crate::storage::Database;
        use crate::test_support::temp_root;

        let root = temp_root();
        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            let (_, lsn1, _, _) = col.set_ttl(Some(120), 1).unwrap();
            col.apply_committed(lsn1).unwrap();
            let (_, lsn2, _, _) = col.put_with_expiry("k1".into(), serde_json::json!({"a": 1}), Some(5000), 1).unwrap();
            col.apply_committed(lsn2).unwrap();
            col.save_index().unwrap();
            col.flush_watermark_full().unwrap();
        }
        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            assert_eq!(col.committed_ttl(), Some(120));
            assert_eq!(col.expire_at_including_staged("k1"), Some(5000));
            // Without the snapshot the next open replays the WALs.
            let _ = std::fs::remove_file(root.join("c").join(crate::storage::index::INDEX_FILENAME));
        }
        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            assert_eq!(col.committed_ttl(), Some(120));
            assert_eq!(col.expire_at_including_staged("k1"), Some(5000));
        }
    }
}

