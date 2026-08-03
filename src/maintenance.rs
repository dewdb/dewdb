//! Background compaction and index-snapshot scheduling.

use crate::storage::{Collection, Database, SpaceUsage};
use serde::Deserialize;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Deserialize, Clone, Debug)]
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
}

fn default_maintenance_enabled() -> bool { true }
fn default_maintenance_interval() -> u64 { 60 }
fn default_compaction_dead_ratio() -> f64 { 0.4 }
fn default_compaction_min_bytes() -> u64 { 8 * 1024 * 1024 }
fn default_snapshot_interval() -> u64 { 300 }

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: default_maintenance_enabled(),
            interval_secs: default_maintenance_interval(),
            compaction_dead_ratio: default_compaction_dead_ratio(),
            compaction_min_wal_bytes: default_compaction_min_bytes(),
            snapshot_interval_secs: default_snapshot_interval(),
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

// Both conditions must hold: a mostly-dead but tiny log is not worth rewriting.
fn should_compact(usage: &SpaceUsage, cfg: &MaintenanceConfig) -> bool {
    usage.total_bytes >= cfg.compaction_min_wal_bytes && usage.dead_ratio() >= cfg.compaction_dead_ratio
}

pub fn maintenance_task(db: Arc<Database>, cfg: MaintenanceConfig) {
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

                let compacted = if should_compact(&usage, &cfg) {
                    info!(target: "maintenance", collection = %name, dead_ratio = usage.dead_ratio(),
                        dead_bytes = usage.dead_bytes(), total_bytes = usage.total_bytes,
                        live_keys = usage.live_keys, "Dead-byte threshold reached; compacting");

                    let target = col.clone();
                    match tokio::task::spawn_blocking(move || target.compact()).await {
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
        }
    }

    #[test]
    fn compaction_trigger_respects_both_ratio_and_floor() {
        let cfg = maint(0.4, 1000);

        let mostly_dead_but_tiny = SpaceUsage { total_bytes: 900, live_bytes: 10, live_keys: 1 };
        assert!(!should_compact(&mostly_dead_but_tiny, &cfg),
            "a log under the byte floor must not be compacted no matter how dead it is");

        let big_but_fresh = SpaceUsage { total_bytes: 10_000, live_bytes: 9_000, live_keys: 10 };
        assert!(!should_compact(&big_but_fresh, &cfg), "10% dead is below the 40% threshold");

        let big_and_dead = SpaceUsage { total_bytes: 10_000, live_bytes: 6_000, live_keys: 10 };
        assert!(should_compact(&big_and_dead, &cfg), "exactly at the threshold must trigger");

        let empty = SpaceUsage { total_bytes: 0, live_bytes: 0, live_keys: 0 };
        assert_eq!(empty.dead_ratio(), 0.0, "an empty log must not divide by zero");
        assert!(!should_compact(&empty, &cfg));
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
