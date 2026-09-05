//! Background compaction and index-snapshot scheduling.

use crate::state::AppState;
use crate::storage::{Collection, Retention, SpaceUsage};
use serde::Deserialize;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

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

/// Compaction drops superseded frames, which breaks a replica's chain and forces the leader into a
/// full snapshot resync on the next repair. Leader-only, on both this path and `/compact`.
/// A mostly-dead but tiny log is not worth rewriting, so the ratio and the floor must both hold.
fn should_compact(usage: &SpaceUsage, cfg: &MaintenanceConfig, is_leader: bool) -> bool {
    is_leader
        && usage.total_bytes >= cfg.compaction_min_wal_bytes
        && usage.dead_ratio() >= cfg.compaction_dead_ratio
}

/// What a compaction of this collection must not destroy: the frames the furthest-behind
/// replication target still needs. A target this node has heard nothing from for the collection
/// counts as needing everything, which `max_bytes` is what bounds.
///
/// `min_reclaim_bytes` is the scheduler's guard and not an operator's: `/compact` asks for the
/// space back and gets the rewrite whether or not the tail leaves much to reclaim.
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
}
