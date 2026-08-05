//! AppState: shared handle to storage, config, replication state, router caches.

use crate::config::NodeConfig;
use crate::consensus::ReplicationState;
use crate::metrics::Metrics;
use crate::ring::shard_owns;
use crate::storage::Database;
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::fs;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

pub struct PrimaryOverride {
    pub url: String,
    pub cached_at: std::time::Instant,
}

// Cached to avoid re-probing per request; expires to retry the configured primary.
const OVERRIDE_TTL_SECS: u64 = 30;

#[derive(Clone)]
pub struct AppState {
    pub db: Option<Arc<Database>>,
    pub config: Arc<NodeConfig>,
    pub client: reqwest::Client,
    pub replication: Option<Arc<RwLock<ReplicationState>>>,
    pub primary_overrides: Arc<std::sync::Mutex<HashMap<String, PrimaryOverride>>>,
    pub shard_failover_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    pub repair_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    pub resyncing: Arc<std::sync::Mutex<HashSet<String>>>,
    pub read_rr: Arc<AtomicUsize>,
    pub metrics: Arc<Metrics>,
    /// Node-wide cap on concurrent outbound replication requests. Shared across every write, unlike
    /// a per-call semaphore, which bounds one write's fan-out and nothing else.
    pub replication_slots: Arc<tokio::sync::Semaphore>,
}

impl AppState {
    pub fn is_leader(&self) -> bool {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().is_leader;
        }
        false
    }

    pub fn is_shard(&self) -> bool {
        self.config.role == "shard"
    }

    /// `Err` means the collection has more durable-but-uncommitted frames than the bound allows.
    /// Checked before the append: once a frame is on disk it is staged, and the staging buffer only
    /// drains on commit, so admitting here is the last point where growth can still be refused.
    pub fn admit_write(&self, collection: &str) -> Result<(), usize> {
        let bound = self.config.flow_control.max_uncommitted_frames;
        if bound == 0 || !self.is_leader() {
            return Ok(());
        }
        let pending = self
            .db
            .as_ref()
            .and_then(|db| db.get_collection(collection).ok())
            .map_or(0, |col| col.pending_len());

        if pending >= bound {
            self.metrics.note_write_rejected();
            return Err(pending);
        }
        Ok(())
    }

    pub fn current_term(&self) -> u64 {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().term;
        }
        0
    }

    // Acks from an older term are recorded but never advance the watermark:
    // only current-term entries count toward a quorum.
    pub fn note_ack(&self, replica: &str, collection: &str, lsn: u64, ack_term: u64) -> u64 {
        let repl = match self.replication.as_ref() {
            Some(r) => r,
            None => return 0,
        };
        let leader_durable = self
            .db
            .as_ref()
            .and_then(|db| db.get_collection(collection).ok())
            .map_or(0, |col| col.durable_lsn());

        let committed = {
            let mut g = repl.write().unwrap();
            g.progress.observe_ack(replica, collection, lsn);
            if !g.is_leader || ack_term != g.term {
                g.progress.committed(collection)
            } else {
                let replicas = g.replicas.clone();
                g.progress.advance(collection, leader_durable, &replicas)
            }
        };
        self.apply_committed(collection, committed);
        committed
    }

    /// Where to resume sending to this replica. `None` means we hold no cursor and must probe.
    pub fn sent_through(&self, replica: &str, collection: &str) -> Option<u64> {
        self.replication.as_ref()
            .and_then(|r| r.read().unwrap().progress.sent_through(replica, collection))
    }

    pub fn note_sent(&self, replica: &str, collection: &str, lsn: u64) {
        if let Some(r) = self.replication.as_ref() {
            r.write().unwrap().progress.note_sent(replica, collection, lsn);
        }
    }

    pub fn rewind_replica(&self, replica: &str, collection: &str, lsn: u64) {
        if let Some(r) = self.replication.as_ref() {
            r.write().unwrap().progress.rewind_to(replica, collection, lsn);
        }
    }

    pub fn committed_lsn(&self, collection: &str) -> u64 {
        match self.replication.as_ref() {
            Some(r) => r.read().unwrap().progress.committed(collection),
            None => 0,
        }
    }

    pub fn matched_lsn(&self, replica: &str, collection: &str) -> u64 {
        match self.replication.as_ref() {
            Some(r) => r.read().unwrap().progress.matched(replica, collection),
            None => 0,
        }
    }

    pub fn all_committed(&self) -> Vec<(String, u64)> {
        match self.replication.as_ref() {
            Some(r) => r.read().unwrap().progress.all_committed(),
            None => Vec::new(),
        }
    }

    pub fn max_committed_lsn(&self) -> u64 {
        match self.replication.as_ref() {
            Some(r) => r.read().unwrap().progress.max_committed(),
            None => 0,
        }
    }

    // A leader with no replicas commits on its own durability, so single-node still progresses.
    pub fn advance_own_commit(&self, collection: &str, durable_lsn: u64) -> u64 {
        let repl = match self.replication.as_ref() {
            Some(r) => r,
            None => return 0,
        };
        let committed = {
            let mut g = repl.write().unwrap();
            if !g.is_leader {
                g.progress.committed(collection)
            } else {
                let replicas = g.replicas.clone();
                g.progress.advance(collection, durable_lsn, &replicas)
            }
        };
        self.apply_committed(collection, committed);
        committed
    }

    pub fn note_leader_committed(&self, collection: &str, lsn: u64) {
        if let Some(repl) = self.replication.as_ref() {
            let mut g = repl.write().unwrap();
            let slot = g.leader_committed.entry(collection.to_string()).or_insert(0);
            if lsn > *slot {
                *slot = lsn;
            }
        }
        self.apply_committed(collection, lsn);
    }

    /// Own quorum when leading, the leader's reported watermark when following.
    pub fn committed_hint(&self, collection: &str) -> u64 {
        match self.replication.as_ref() {
            Some(r) => {
                let g = r.read().unwrap();
                if g.is_leader {
                    g.progress.committed(collection)
                } else {
                    g.leader_committed.get(collection).copied().unwrap_or(0)
                }
            },
            None => 0,
        }
    }

    // Never call with the replication lock held: this takes pending and index,
    // while note_ack reaches the collections lock in the opposite order.
    pub fn apply_committed(&self, collection: &str, committed: u64) {
        if committed == 0 {
            return;
        }
        if let Some(col) = self.db.as_ref().and_then(|db| db.get_collection(collection).ok()) {
            col.apply_committed(committed);
        }
    }

    #[cfg(test)]
    pub fn for_admission_test(
        config: crate::config::NodeConfig,
        db: Arc<Database>,
        is_leader: bool,
    ) -> Self {
        use crate::consensus::{Progress, ReplicationState};
        Self {
            db: Some(db),
            replication: Some(Arc::new(RwLock::new(ReplicationState {
                term: 1,
                is_leader,
                voted_for: None,
                last_heartbeat: None,
                last_replication: None,
                was_receiving_replication: false,
                heartbeat_running: false,
                primary_addr: None,
                replicas: Vec::new(),
                last_known_primary_position: None,
                progress: Progress::new(),
                leader_committed: HashMap::new(),
            }))),
            replication_slots: Arc::new(tokio::sync::Semaphore::new(
                config.flow_control.max_inflight_requests.max(1))),
            client: reqwest::Client::new(),
            config: Arc::new(config),
            primary_overrides: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shard_failover_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            repair_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            resyncing: Arc::new(std::sync::Mutex::new(HashSet::new())),
            read_rr: Arc::new(AtomicUsize::new(0)),
            metrics: Arc::new(Metrics::new()),
        }
    }

    pub fn get_replicas(&self) -> Vec<String> {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().replicas.clone();
        }
        Vec::new()
    }

    pub fn get_effective_shard_url(&self, hash: u64) -> Option<(String, String, Vec<String>)> {
        for shard in &self.config.shard_map {
            if shard_owns(shard, hash) {
                let mut overrides = self.primary_overrides.lock().unwrap();
                if let Some(ov) = overrides.get(&shard.node_url) {
                    if ov.cached_at.elapsed().as_secs() < OVERRIDE_TTL_SECS {
                        return Some((ov.url.clone(), shard.node_url.clone(), shard.replica_urls.clone()));
                    } else {
                        overrides.remove(&shard.node_url);
                    }
                }
                return Some((shard.node_url.clone(), shard.node_url.clone(), shard.replica_urls.clone()));
            }
        }
        None
    }

    pub fn set_primary_override(&self, original_url: &str, new_url: &str) {
        let mut overrides = self.primary_overrides.lock().unwrap();
        overrides.insert(original_url.to_string(), PrimaryOverride {
            url: new_url.to_string(),
            cached_at: std::time::Instant::now(),
        });
    }

    pub fn clear_primary_override(&self, original_url: &str) {
        self.primary_overrides.lock().unwrap().remove(original_url);
    }

    pub fn effective_primary(&self, original_url: &str) -> String {
        let mut overrides = self.primary_overrides.lock().unwrap();
        if let Some(ov) = overrides.get(original_url) {
            if ov.cached_at.elapsed().as_secs() < OVERRIDE_TTL_SECS {
                return ov.url.clone();
            }
            overrides.remove(original_url);
        }
        original_url.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{stage_put, temp_root};

    fn config_with_bound(root: &std::path::Path, bound: usize) -> NodeConfig {
        serde_json::from_value(serde_json::json!({
            "node_id": "n1",
            "role": "shard",
            "shard_role": "primary",
            "listen_addr": "127.0.0.1:1",
            "data_dir": root.to_string_lossy(),
            "flow_control": { "max_uncommitted_frames": bound },
        })).unwrap()
    }

    #[tokio::test]
    async fn writes_are_refused_once_the_uncommitted_backlog_hits_the_bound() {
        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let state = AppState::for_admission_test(config_with_bound(&root, 3), db.clone(), true);
        let col = db.get_collection("t").unwrap();

        for i in 0..2 {
            stage_put(&col, &format!("k{}", i), i);
            assert!(state.admit_write("t").is_ok(), "under the bound the write is admitted");
        }

        stage_put(&col, "k2", 2);
        assert_eq!(state.admit_write("t"), Err(3),
            "at the bound the leader must refuse, or the staging buffer grows without limit \
             for as long as commits are stalled");
        assert_eq!(state.metrics.writes_rejected(), 1);

        // A quorum catching up drains the buffer and reopens the door.
        col.apply_committed(col.last_appended_lsn());
        assert!(state.admit_write("t").is_ok(), "backpressure must lift once commits catch up");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_follower_is_never_backpressured_and_zero_disables_the_bound() {
        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let col = db.get_collection("t").unwrap();
        for i in 0..5 {
            stage_put(&col, &format!("k{}", i), i);
        }

        let follower = AppState::for_admission_test(config_with_bound(&root, 1), db.clone(), false);
        assert!(follower.admit_write("t").is_ok(),
            "a follower refusing replicated frames would look like a gap to the leader; its buffer \
             is bounded by the leader's own admission control instead");

        let unbounded = AppState::for_admission_test(config_with_bound(&root, 0), db.clone(), true);
        assert!(unbounded.admit_write("t").is_ok(), "0 opts out of the bound");

        let _ = fs::remove_dir_all(&root);
    }
}
