//! AppState: shared handle to storage, config, replication state, router caches.

use crate::cluster::metadata::{
    adopt, merge_catalog, sanitize_catalog, Adoption, ClusterMetadata, IndexCatalog, Migration,
    ViewId,
};
use crate::cluster::migration::MigrationRuns;
use crate::cluster::ownership::{classify, group_owner, group_owns, Ownership};
use crate::config::NodeConfig;
use crate::consensus::config::CONFIG_LOG;
use crate::consensus::ReplicationState;
use crate::metrics::{Metrics, NodeLoad};
use crate::ring::{shard_owns, BuiltRing};
use crate::storage::frame::Configuration;
use crate::storage::Database;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

pub struct PrimaryOverride {
    pub url: String,
    pub cached_at: std::time::Instant,
}

pub(crate) struct NodeLoadSample {
    load: NodeLoad,
    sampled_at: std::time::Instant,
}

pub struct RoutedRead {
    url: String,
    counts: Arc<std::sync::Mutex<HashMap<String, u64>>>,
}

#[derive(Clone)]
pub(crate) struct ScanOwnership {
    ring: Option<Arc<BuiltRing>>,
    group: Option<String>,
    collection: String,
    fingerprint: u64,
}

impl ScanOwnership {
    pub fn includes(&self, key: &str) -> bool {
        match &self.ring {
            None => true,
            Some(ring) => self.group.as_deref()
                .is_some_and(|group| group_owns(ring, group, &self.collection, key)),
        }
    }

    /// The partitioning this verdict was taken against, taken from the same view as the ring so
    /// the two cannot disagree. Stamped into the page's cursor and compared on the next one.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
}

impl Drop for RoutedRead {
    fn drop(&mut self) {
        let Ok(mut counts) = self.counts.lock() else { return };
        if let Some(count) = counts.get_mut(&self.url) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.url);
            }
        }
    }
}

/// Why a write was not admitted. `Backlog` is transient and the same request succeeds once commits
/// drain; `TooWide` never can, because one request asks for more than the whole bound.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    Backlog { in_flight: usize },
    TooWide { frames: usize },
}

/// Capacity held between admission and the append that consumes it. Released on drop, so a write
/// that fails anywhere before staging gives its slots back without the caller unwinding them.
pub struct FrameReservation {
    collection: String,
    frames: usize,
    held: Arc<std::sync::Mutex<HashMap<String, usize>>>,
}

impl Drop for FrameReservation {
    fn drop(&mut self) {
        let Ok(mut held) = self.held.lock() else { return };
        if let Some(count) = held.get_mut(&self.collection) {
            *count = count.saturating_sub(self.frames);
            if *count == 0 {
                held.remove(&self.collection);
            }
        }
    }
}

// Cached to avoid re-probing per request; expires to retry the configured primary.
const OVERRIDE_TTL_SECS: u64 = 30;
const NODE_LOAD_TTL_SECS: u64 = 10;

#[derive(Clone)]
pub struct AppState {
    pub db: Option<Arc<Database>>,
    pub config: Arc<NodeConfig>,
    /// The credential set in force, seeded from config and re-read while the node runs. Read through
    /// `auth()`, never `config.auth`, so a key removed from the file stops working without a restart.
    pub(crate) auth: Arc<RwLock<crate::auth::AuthConfig>>,
    pub client: reqwest::Client,
    /// The same credentials without a request deadline. Only the router-coordinated change stream
    /// uses it: every other call to a peer is one request that must not outlive its own timeout.
    pub stream_client: reqwest::Client,
    pub replication: Option<Arc<RwLock<ReplicationState>>>,
    pub primary_overrides: Arc<std::sync::Mutex<HashMap<String, PrimaryOverride>>>,
    pub shard_failover_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    pub repair_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    pub resyncing: Arc<std::sync::Mutex<HashSet<String>>>,
    pub read_rr: Arc<AtomicUsize>,
    pub(crate) node_loads: Arc<std::sync::Mutex<HashMap<String, NodeLoadSample>>>,
    pub(crate) routed_reads: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// Frames an admitted write has not staged yet, per collection. Counted with `pending_len`, or
    /// a bulk request and a set of concurrent writes each pass one pre-append sample (IB-014).
    pub(crate) frame_reservations: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    pub metrics: Arc<Metrics>,
    /// Node-wide cap on concurrent outbound replication requests. Shared across every write, unlike
    /// a per-call semaphore, which bounds one write's fan-out and nothing else.
    pub replication_slots: Arc<tokio::sync::Semaphore>,
    /// Node-wide cap on concurrent aggregation and sorted-query scans. A request's `max_docs` bounds one walk;
    /// this bounds how many of them hold blocking threads at once (IB-025).
    pub scan_slots: Arc<tokio::sync::Semaphore>,
    /// The live topology. Seeded from config on a node's first boot, durable thereafter, and the
    /// only thing the routing path reads -- `config.shard_map` is a bootstrap value, not an authority.
    pub cluster: Arc<RwLock<ClusterMetadata>>,
    /// Token ring for the current view. Derived, never authoritative: keyed by version so it cannot
    /// drift from the view it came from, and rebuilt on the first lookup after a change.
    pub ring_cache: Arc<std::sync::Mutex<RingCache>>,
    /// Progress of a handover this node is driving. Runtime only: a half-copied shard is this
    /// node's business, not a fact the cluster needs to agree on.
    pub migrations: Arc<std::sync::Mutex<MigrationRuns>>,
    /// Local mirror of the `_webhooks` registration catalogue, plus node-local delivery counters.
    pub webhooks: Arc<crate::webhook::WebhookStore>,
    /// The barrier that holds writes still on this node. Data movement drains it, to settle writes that
    /// decided ownership under the old view; a leadership transfer holds it to stop the tail moving.
    pub write_gate: Arc<tokio::sync::RwLock<()>>,
    pub campaign: Arc<tokio::sync::Mutex<()>>,
    pub election_history: Arc<tokio::sync::RwLock<()>>,
    pub membership_changes: Arc<crate::consensus::reconfigure::MembershipChanges>,
}

/// Whether `incoming` carries a catalogue entry `current` does not already hold. Cheaper than the
/// merge, and the merge itself needs the write lock this answers under a read one.
fn catalog_adds_to(current: &IndexCatalog, incoming: &IndexCatalog) -> bool {
    let mut probe = current.clone();
    merge_catalog(&mut probe, incoming)
}

#[derive(Default)]
pub struct RingCache {
    version: u64,
    ring: Option<Arc<BuiltRing>>,
    target: Option<Arc<BuiltRing>>,
}

impl AppState {
    /// Serializes incoming replication with snapshot installation for one collection.
    pub fn snapshot_install_lock(&self, collection: &str) -> Arc<tokio::sync::Mutex<()>> {
        let key = format!("snapshot-install:{}", collection);
        let mut locks = self.repair_locks.lock().unwrap();
        locks.entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub fn migration_reset_lock(&self, id: &str, source: &str) -> Arc<tokio::sync::Mutex<()>> {
        let key = format!("migration-reset:{}:{}", id, crate::util::node_key(source));
        let mut locks = self.repair_locks.lock().unwrap();
        locks.entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub fn auth(&self) -> std::sync::RwLockReadGuard<'_, crate::auth::AuthConfig> {
        self.auth.read().unwrap()
    }

    /// Replaces the two client tiers, returning whether they moved. `internal_secret` and
    /// `upstream_api_key` are baked into this node's outbound clients at boot and are not rotatable.
    pub fn rotate_client_keys(&self, api_keys: Vec<String>, admin_keys: Vec<String>) -> bool {
        let mut held = self.auth.write().unwrap();
        if held.api_keys == api_keys && held.admin_keys == admin_keys {
            return false;
        }
        held.api_keys = api_keys;
        held.admin_keys = admin_keys;
        true
    }

    pub fn is_leader(&self) -> bool {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().is_leader;
        }
        false
    }

    pub fn is_shard(&self) -> bool {
        self.config.role == "shard"
    }

    /// Admits a write of `frames` frames, before the append: the staging buffer drains only on commit,
    /// so this is the last refusal point. The reservation outlives the append (IB-014).
    pub fn admit_write(&self, collection: &str, frames: usize)
        -> Result<Option<FrameReservation>, Refusal>
    {
        let bound = self.config.flow_control.max_uncommitted_frames;
        // Not `!is_leader()`: a leader deposed mid-write still holds staged frames that no quorum
        // will ever commit, which is when the bound matters most rather than least (bugs.md C26).
        if bound == 0 || self.replication.is_none() {
            return Ok(None);
        }
        // Refused on the request alone, before the sample: draining cannot make room for it, so
        // answering with retriable backpressure would leave the client looping.
        if frames > bound {
            self.metrics.note_write_rejected();
            return Err(Refusal::TooWide { frames });
        }
        // One lock over the sample and the reservation, or two requests decide against the same
        // count and both append. Nothing holds a collection's `pending` while taking this.
        let mut held = self.frame_reservations.lock().unwrap();
        let pending = self
            .db
            .as_ref()
            .and_then(|db| db.get_collection(collection).ok())
            .map_or(0, |col| col.pending_len());
        let in_flight = pending.saturating_add(held.get(collection).copied().unwrap_or(0));

        if in_flight.saturating_add(frames) > bound {
            drop(held);
            self.metrics.note_write_rejected();
            return Err(Refusal::Backlog { in_flight });
        }
        *held.entry(collection.to_string()).or_insert(0) += frames;
        drop(held);
        Ok(Some(FrameReservation {
            collection: collection.to_string(),
            frames,
            held: self.frame_reservations.clone(),
        }))
    }

    pub fn current_term(&self) -> u64 {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().term;
        }
        0
    }

    /// The term this node may append under, read as one sample so leadership and term cannot be
    /// taken from either side of a demotion. `None` is a node with no authority to append.
    pub fn leader_term(&self) -> Option<u64> {
        let g = self.replication.as_ref()?.read().unwrap();
        g.is_leader.then_some(g.term)
    }

    /// Whether an append made under `term` still has leadership behind it. Catches both halves of a
    /// step-down: `relinquish_leadership` keeps the term and drops the claim, `apply_demotion` moves it.
    pub fn still_leading(&self, term: u64) -> bool {
        self.leader_term() == Some(term)
    }

    // Acks from an older term are recorded but never advance the watermark:
    // only current-term entries count toward a quorum.
    pub fn note_ack(&self, replica: &str, collection: &str, lsn: u64, ack_term: u64) -> io::Result<u64> {
        let repl = match self.replication.as_ref() {
            Some(r) => r,
            None => return Ok(0),
        };
        let leader_durable = self
            .db
            .as_ref()
            .and_then(|db| db.get_collection(collection).ok())
            .map_or(0, |col| col.durable_lsn());
        let own = self.own_url();

        let committed = {
            let mut g = repl.write().unwrap();
            g.progress.observe_ack(replica, collection, lsn);
            // An ack is a reply, so it is outbound contact as much as a probe is.
            if g.is_leader {
                g.progress.note_contact(replica, std::time::Instant::now());
            }
            if !g.is_leader || ack_term != g.term {
                g.progress.committed(collection)
            } else {
                let quorum = g.quorum(&own);
                g.progress.advance(collection, &own, leader_durable, &quorum)
            }
        };
        self.apply_committed(collection, committed)?;
        Ok(committed)
    }

    /// This leader's own append, which is what lets `advance` commit at that LSN or above.
    /// Ignored unless we are the leader: only our own term's entries close the Figure 8 window.
    pub fn note_leader_append(&self, collection: &str, lsn: u64) {
        if let Some(repl) = self.replication.as_ref() {
            let mut g = repl.write().unwrap();
            if g.is_leader {
                g.progress.note_leader_append(collection, lsn);
            }
        }
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

    /// Whether this leader has appended to the collection in its own term. `advance` needs one before
    /// it can commit an inherited tail, so a leader without one can never publish it.
    pub fn has_term_floor(&self, collection: &str) -> bool {
        match self.replication.as_ref() {
            Some(r) => {
                let g = r.read().unwrap();
                g.is_leader && g.progress.term_floor(collection).is_some()
            },
            None => false,
        }
    }

    /// Whether an entry of this leader's own term has committed for the collection. Until one has,
    /// `committed_lsn` can sit below entries a previous leader committed and this node holds staged.
    pub fn has_current_term_commit(&self, collection: &str) -> bool {
        match self.replication.as_ref() {
            Some(r) => {
                let g = r.read().unwrap();
                g.progress.term_floor(collection)
                    .is_some_and(|floor| g.progress.committed(collection) >= floor)
            },
            None => false,
        }
    }

    /// The voter half of a leader's lease round: how long this node refuses votes, and the deadline.
    /// Never more than the ask, nor more than this node's own contact already commits it to.
    pub fn grant_novote(&self, asker_term: u64, asked: std::time::Duration) -> std::time::Duration {
        let repl = match self.replication.as_ref() {
            Some(r) => r,
            None => return std::time::Duration::ZERO,
        };
        let timeout = std::time::Duration::from_secs(self.config.heartbeat_timeout_secs);
        let mut g = repl.write().unwrap();
        // A leader at a term this node has left is not one whose reads may rest on our silence.
        if asker_term < g.term {
            return std::time::Duration::ZERO;
        }
        let age = crate::consensus::lease::contact_age(g.last_heartbeat, g.last_replication);
        let granted = asked.min(crate::consensus::lease::grantable(age, timeout));
        if granted.is_zero() {
            return std::time::Duration::ZERO;
        }
        let until = std::time::Instant::now() + granted;
        g.novote_until = Some(g.novote_until.map_or(until, |held| held.max(until)));
        granted
    }

    /// Records what a voter granted in reply to our probe, dated from before the probe was sent, both
    /// ends on our clock. `term` is the probe's; a reply outliving it would resurrect a retired lease.
    pub fn note_lease_grant(
        &self,
        voter: &str,
        term: u64,
        sent: std::time::Instant,
        sent_wall: std::time::SystemTime,
        granted: std::time::Duration,
    ) {
        if granted.is_zero() || !self.quorum_config().contains(voter) {
            return;
        }
        let capped = crate::consensus::lease::accept_promise(
            granted, std::time::Duration::from_secs(self.config.heartbeat_timeout_secs));
        if let Some(repl) = self.replication.as_ref() {
            let mut g = repl.write().unwrap();
            if g.is_leader && g.term == term && !g.handing_over {
                g.leases.note_promise(voter, sent, sent_wall, capped);
            }
        }
    }

    /// Whether a majority still promises not to vote, which is what a quorum read would otherwise spend
    /// a round on. A handover in flight answers no: the target will grant past its own promise.
    pub fn holds_read_lease(&self) -> bool {
        let config = self.quorum_config();
        let own = self.own_url();
        match self.replication.as_ref() {
            Some(r) => {
                let g = r.read().unwrap();
                !g.handing_over && g.leases.held(
                    &config, &own, std::time::Instant::now(), std::time::SystemTime::now())
            },
            None => false,
        }
    }

    /// Marks a handover in flight. Set for the length of one, and cleared on every way out of it,
    /// including the ones that leave this node still leading.
    pub fn set_handing_over(&self, handing_over: bool) {
        if let Some(repl) = self.replication.as_ref() {
            repl.write().unwrap().handing_over = handing_over;
        }
    }

    /// A request of ours reached `replica` and came back, whatever it asked. The only evidence
    /// this node has that its own outbound path works.
    pub fn note_replica_contact(&self, replica: &str) {
        if let Some(repl) = self.replication.as_ref() {
            let mut g = repl.write().unwrap();
            if g.is_leader {
                g.progress.note_contact(replica, std::time::Instant::now());
            }
        }
    }

    /// Whether this node would accept `from` handing its own office away: a voter answers for the leader
    /// it follows, a leader for itself. It is what lets a transferred election past `withholds_vote`.
    pub fn honours_transfer(&self, from: Option<&str>) -> bool {
        let (Some(from), Some(repl)) = (from, self.replication.as_ref()) else { return false };
        let own = self.own_url();
        let g = repl.read().unwrap();
        match g.is_leader {
            true => crate::util::same_endpoint(from, &own),
            false => g.primary_addr.as_deref()
                .is_some_and(|p| crate::util::same_endpoint(p, from)),
        }
    }

    /// CheckQuorum: a majority of the configuration has answered us within `within`.
    pub fn holds_contact_quorum(&self, within: std::time::Duration) -> bool {
        let config = self.quorum_config();
        let own = self.own_url();
        match self.replication.as_ref() {
            Some(r) => r.write().unwrap().progress.contact_quorum(
                &config, &own, std::time::Instant::now(), within),
            None => true,
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
    pub fn advance_own_commit(&self, collection: &str, durable_lsn: u64) -> io::Result<u64> {
        let repl = match self.replication.as_ref() {
            Some(r) => r,
            None => return Ok(0),
        };
        let own = self.own_url();
        let committed = {
            let mut g = repl.write().unwrap();
            if !g.is_leader {
                g.progress.committed(collection)
            } else {
                let quorum = g.quorum(&own);
                g.progress.advance(collection, &own, durable_lsn, &quorum)
            }
        };
        self.apply_committed(collection, committed)?;
        Ok(committed)
    }

    // Call under the collection's snapshot-install lock so matching, durability and publication stay ordered.
    pub fn note_leader_committed(&self, collection: &str, term: u64, lsn: u64, matched: Option<u64>) -> io::Result<()> {
        let Some(db) = self.db.as_ref() else { return Ok(()) };
        let Some(col) = db.lookup_collection(collection)? else { return Ok(()) };
        let durable = col.durable_lsn();
        let committed = if let Some(repl) = self.replication.as_ref() {
            let mut g = repl.write().unwrap();
            if g.is_leader || g.term != term {
                return Ok(());
            }
            let prefix = g.leader_matched.entry(collection.to_string()).or_insert((term, 0));
            if prefix.0 != term {
                *prefix = (term, 0);
            }
            if let Some(matched) = matched {
                prefix.1 = prefix.1.max(matched.min(durable));
            }
            lsn.min(prefix.1).min(durable)
        } else { return Ok(()) };
        self.apply_committed(collection, committed)
    }

    // Never call with the replication lock held: this takes pending and index,
    // while note_ack reaches the collections lock in the opposite order.
    pub fn apply_committed(&self, collection: &str, committed: u64) -> io::Result<()> {
        if committed == 0 {
            return Ok(());
        }
        if let Some(db) = self.db.as_ref() {
            db.get_collection(collection)?.apply_committed(committed)?;
        }
        if collection == CONFIG_LOG {
            self.refresh_configuration();
        }
        if collection == crate::webhook::WEBHOOK_PROGRESS_LOG {
            crate::webhook::reconcile_registrations(self)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn for_routing_test(config: crate::config::NodeConfig) -> Self {
        let cluster = ClusterMetadata::seed_from_config(&config);
        let data_dir = config.data_dir.clone();
        Self {
            db: None,
            replication: None,
            replication_slots: Arc::new(tokio::sync::Semaphore::new(1)),
            scan_slots: Arc::new(tokio::sync::Semaphore::new(crate::aggregate::MAX_CONCURRENT_SCANS)),
            client: reqwest::Client::new(),
            stream_client: reqwest::Client::new(),
            auth: Arc::new(RwLock::new(config.auth.clone())),
            config: Arc::new(config),
            primary_overrides: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shard_failover_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            repair_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            resyncing: Arc::new(std::sync::Mutex::new(HashSet::new())),
            read_rr: Arc::new(AtomicUsize::new(0)),
            node_loads: Arc::new(std::sync::Mutex::new(HashMap::new())),
            routed_reads: Arc::new(std::sync::Mutex::new(HashMap::new())),
            frame_reservations: Arc::new(std::sync::Mutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::new()),
            cluster: Arc::new(RwLock::new(cluster)),
            ring_cache: Arc::new(std::sync::Mutex::new(RingCache::default())),
            migrations: Arc::new(std::sync::Mutex::new(MigrationRuns::default())),
            webhooks: Arc::new(crate::webhook::WebhookStore::restored(&data_dir)),
            write_gate: Arc::new(tokio::sync::RwLock::new(())),
            campaign: Arc::new(tokio::sync::Mutex::new(())),
            election_history: Arc::new(tokio::sync::RwLock::new(())),
            membership_changes: Arc::new(Default::default()),
        }
    }

    #[cfg(test)]
    pub fn for_admission_test(
        config: crate::config::NodeConfig,
        db: Arc<Database>,
        is_leader: bool,
    ) -> Self {
        use crate::consensus::{Progress, ReplicationState};
        let cluster = ClusterMetadata::seed_from_config(&config);
        let data_dir = config.data_dir.clone();
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
                leases: Default::default(),
                handing_over: false,
                novote_until: None,
                booted_at: std::time::Instant::now(),
                leader_matched: HashMap::new(),
                configuration: None,
            }))),
            replication_slots: Arc::new(tokio::sync::Semaphore::new(
                config.flow_control.max_inflight_requests.max(1))),
            scan_slots: Arc::new(tokio::sync::Semaphore::new(crate::aggregate::MAX_CONCURRENT_SCANS)),
            client: reqwest::Client::new(),
            stream_client: reqwest::Client::new(),
            auth: Arc::new(RwLock::new(config.auth.clone())),
            config: Arc::new(config),
            primary_overrides: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shard_failover_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            repair_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            resyncing: Arc::new(std::sync::Mutex::new(HashSet::new())),
            read_rr: Arc::new(AtomicUsize::new(0)),
            node_loads: Arc::new(std::sync::Mutex::new(HashMap::new())),
            routed_reads: Arc::new(std::sync::Mutex::new(HashMap::new())),
            frame_reservations: Arc::new(std::sync::Mutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::new()),
            cluster: Arc::new(RwLock::new(cluster)),
            ring_cache: Arc::new(std::sync::Mutex::new(RingCache::default())),
            migrations: Arc::new(std::sync::Mutex::new(MigrationRuns::default())),
            webhooks: Arc::new(crate::webhook::WebhookStore::restored(&data_dir)),
            write_gate: Arc::new(tokio::sync::RwLock::new(())),
            campaign: Arc::new(tokio::sync::Mutex::new(())),
            election_history: Arc::new(tokio::sync::RwLock::new(())),
            membership_changes: Arc::new(Default::default()),
        }
    }

    /// The quorum every decision is taken against: the newest configuration in the config log, and the
    /// view-derived voting set until there is one. Never call while holding the replication lock.
    pub fn quorum_config(&self) -> Configuration {
        if let Some(repl) = self.replication.as_ref() {
            let installed = repl.read().unwrap().configuration.clone();
            if let Some(config) = installed {
                return config;
            }
        }
        Configuration::simple(self.view_voting_set())
    }

    /// Installs the configuration in force, returning whether it changed. A leader's replica list
    /// moves with it: a member of the outgoing half still decides, so it must still be sent frames.
    pub fn install_configuration(&self, config: Configuration) -> bool {
        let repl = match self.replication.as_ref() {
            Some(r) => r,
            None => return false,
        };
        let own = self.own_url();
        let mut g = repl.write().unwrap();
        if g.configuration.as_ref() == Some(&config) {
            return false;
        }
        g.replicas = config.members().into_iter()
            .filter(|m| !crate::util::same_endpoint(m, &own))
            .collect();
        g.configuration = Some(config);
        true
    }

    /// Re-reads the configuration in force. Every arrival path must call it, because the entry takes
    /// effect where it lands. A log with no configuration never uninstalls one -- a resync looks the same.
    pub fn refresh_configuration(&self) {
        let latest = self.db.as_ref()
            .and_then(|db| db.existing_collection(CONFIG_LOG))
            .and_then(|col| col.latest_config());
        let Some(config) = latest else { return };

        if self.install_configuration(config.clone()) {
            tracing::info!(target: "membership", voters = ?config.voters, outgoing = ?config.outgoing,
                "Configuration in force");
        }
    }

    /// Quorum membership, this node included, derived from the view when that view is real and names
    /// this node; config otherwise, since a seed is one node's opinion of the cluster.
    fn view_voting_set(&self) -> Vec<String> {
        let own = self.own_url();
        {
            let view = self.cluster.read().unwrap();
            if !view.seeded {
                // Narrowed to this node's shard group before counting: `voting_shards` is the flat
                // member list, and a majority taken over it spans groups that share no log.
                let voters = match (view.shard_group(&own), view.groups_known()) {
                    (Some(group), _) => view.voting_shards().into_iter()
                        .filter(|v| group.iter().any(|g| crate::util::same_endpoint(g, v)))
                        .collect(),
                    (None, false) => view.voting_shards(),
                    (None, true) => Vec::new(),
                };
                if voters.iter().any(|v| crate::util::same_endpoint(v, &own)) {
                    return voters;
                }
            }
        }

        // Both halves, not just `peers`: `become_leader` has always taken the commit quorum over the
        // union, and an election counting fewer nodes than the commit index does is a second leader.
        let mut out = vec![own];
        for candidate in self.config.replicas.iter().chain(self.config.peers.iter()) {
            if !out.iter().any(|u| crate::util::same_endpoint(u, candidate)) {
                out.push(candidate.clone());
            }
        }
        out
    }

    /// Everyone in the quorum, this node included. Both halves while a change is in flight: the
    /// outgoing half still votes and still has to hold entries, so it is still asked and still sent.
    pub fn voting_set(&self) -> Vec<String> {
        self.quorum_config().members()
    }

    /// The quorum membership other than this node: who to ask for votes, and whose acks count.
    pub fn voting_peers(&self) -> Vec<String> {
        let own = self.own_url();
        self.voting_set().into_iter()
            .filter(|v| !crate::util::same_endpoint(v, &own))
            .collect()
    }

    pub fn voting_replicas(&self) -> Vec<String> {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().replicas.clone();
        }
        Vec::new()
    }

    pub fn own_url(&self) -> String {
        self.config.own_url()
    }

    /// Learners in the live view that named this node as their primary.
    pub fn learner_replicas(&self) -> Vec<String> {
        let own = self.own_url();
        let voting = self.voting_replicas();
        self.cluster.read().unwrap()
            .learners_following(&own)
            .into_iter()
            .filter(|url| !crate::util::same_endpoint(url, &own))
            // A node in both sets is already in the quorum; sending twice would double-count it.
            .filter(|url| !voting.iter().any(|v| crate::util::same_endpoint(v, url)))
            .collect()
    }

    /// Everyone who receives frames. A superset of the quorum set: learners are shipped data so
    /// they can catch up, but never counted, so admitting one cannot move a commit index.
    pub fn replication_targets(&self) -> Vec<String> {
        let mut targets = self.voting_replicas();
        targets.extend(self.learner_replicas());
        targets
    }

    pub fn is_voting_replica(&self, url: &str) -> bool {
        self.voting_replicas().iter().any(|v| crate::util::same_endpoint(v, url))
    }

    /// Non-voting for either reason: booted that way, or named so by the view. The config half covers
    /// the window between boot and admission, where `peers` is empty and a majority of one is reachable.
    pub fn is_learner(&self) -> bool {
        self.config.is_learner() || self.view_names_us_learner()
    }

    /// Whether this node is in the quorum: standing for election and granting a vote are one right. A
    /// configuration decides it outright; without one, admission never promotes a node booted a learner.
    pub fn in_quorum(&self) -> bool {
        let installed = self.replication.as_ref()
            .and_then(|r| r.read().unwrap().configuration.clone());
        match installed {
            Some(config) => config.contains(&self.own_url()),
            None => !self.is_learner(),
        }
    }

    fn view_names_us_learner(&self) -> bool {
        self.cluster.read().unwrap().is_learner(&self.own_url())
    }

    pub fn cluster_view(&self) -> ClusterMetadata {
        self.cluster.read().unwrap().clone()
    }

    /// What a heartbeat advertises, so a peer can order this node's view against its own without
    /// fetching it.
    pub fn cluster_view_id(&self) -> ViewId {
        self.cluster.read().unwrap().view_id()
    }

    pub fn cluster_version(&self) -> u64 {
        self.cluster.read().unwrap().version
    }

    /// Shard owners in the live view, deduped: one node may own several ranges.
    pub fn shard_owners(&self) -> Vec<(String, Vec<String>)> {
        self.cluster.read().unwrap().shard_owners()
    }

    /// Owners and the fingerprint of the partitioning they came from, under one read. Sampling them
    /// separately can stamp a cursor with a version the page it describes was not served from.
    pub fn partitioning(&self) -> (u64, Vec<(String, Vec<String>)>) {
        let view = self.cluster.read().unwrap();
        (view.partition_fingerprint(), view.shard_owners())
    }

    /// Persists only what it adopted, and durable before visible: a view refused in memory must not
    /// reach disk, and a node must not serve a topology a restart would rewind to the config seed.
    pub fn adopt_cluster(&self, mut incoming: ClusterMetadata) -> Adoption {
        // Ahead of the validate and the save, not only of `adopt`: what this persists when the view
        // wins is `incoming` itself, and an unaddressable catalogue entry must not reach disk.
        for why in sanitize_catalog(&mut incoming.index_catalog) {
            tracing::warn!(target: "cluster", version = incoming.version,
                "Dropped an index catalogue entry from an offered view: {}", why);
        }
        // Decided against the current view first, so the fsync below happens outside every lock.
        let superseding = {
            if let Err(why) = incoming.validate() {
                return Adoption::Rejected(why);
            }
            let current = self.cluster.read().unwrap();
            let superseding = incoming.supersedes(&current);
            // A view that loses on topology is still worth taking for its catalogue, which is
            // versioned per collection and merged rather than replaced.
            if !superseding && !catalog_adds_to(&current.index_catalog, &incoming.index_catalog) {
                return Adoption::Stale { current: current.version };
            }
            superseding
        };

        if superseding {
            if let Err(e) = incoming.save(&self.config.data_dir) {
                // Still adopted: a lost write costs a re-fetch, while refusing would strand this
                // node on a topology the cluster has already left.
                tracing::warn!(target: "cluster", error = %e, version = incoming.version,
                    "Could not persist cluster view; adopting it in memory anyway");
            }
        }

        // Re-checked under the write lock: a newer view may have landed during the write, and that
        // one wins. `adopt` validates and compares again rather than trusting the decision above.
        let (outcome, catalog_moved) = {
            let mut current = self.cluster.write().unwrap();
            let before = current.index_catalog.clone();
            let outcome = adopt(&mut current, incoming);
            let moved = current.index_catalog != before;
            (outcome, moved)
        };

        // What reaches disk is the merge, not what arrived, so this runs after the adoption. The
        // durable-before-visible rule above covers the topology, the half a restart could serve wrongly.
        if catalog_moved {
            self.persist_cluster_view();
        }
        if matches!(outcome, Adoption::Adopted { .. }) {
            self.follow_from_view();
            self.react_to_migration();
        }
        outcome
    }

    /// Records an index definition as a fact about the collection rather than about this group; returns
    /// whether the catalogue moved. Not a topology publish, so it needs no leader.
    pub fn record_index_catalog(&self, collection: &str, change: &crate::storage::IndexChange) -> bool {
        let moved = {
            let mut view = self.cluster.write().unwrap();
            view.record_index_change(&self.config.node_id, collection, change)
        };
        if moved {
            self.persist_cluster_view();
        }
        moved
    }

    /// Empties a dropped collection's catalogue entry rather than removing it, so a node that
    /// missed the drop cannot win the merge and put the definitions back.
    pub fn forget_collection_indexes(&self, collection: &str) -> bool {
        let moved = {
            let mut view = self.cluster.write().unwrap();
            view.forget_collection_indexes(&self.config.node_id, collection)
        };
        if moved {
            self.persist_cluster_view();
        }
        moved
    }

    /// Takes whatever `incoming` has that this node's catalogue does not. Returns whether anything
    /// moved, which is what tells the gossip loop it has something new to hand on.
    pub fn merge_index_catalog(&self, incoming: &IndexCatalog) -> bool {
        let moved = {
            let mut view = self.cluster.write().unwrap();
            merge_catalog(&mut view.index_catalog, incoming)
        };
        if moved {
            self.persist_cluster_view();
        }
        moved
    }

    pub fn index_catalog(&self) -> IndexCatalog {
        self.cluster.read().unwrap().index_catalog.clone()
    }

    pub fn catalog_fingerprint(&self) -> u64 {
        self.cluster.read().unwrap().catalog_fingerprint()
    }

    fn persist_cluster_view(&self) {
        let view = self.cluster.read().unwrap().clone();
        if let Err(e) = view.save(&self.config.data_dir) {
            tracing::warn!(target: "cluster", error = %e,
                "Could not persist the cluster view; it stands in memory and re-converges by gossip");
        }
    }

    /// Every view adoption is a chance for a handover to have started, finished, or been abandoned, so a
    /// node that learns of a plan by propagation participates exactly as one told directly.
    pub fn react_to_migration(&self) {
        let migration = match self.migration() {
            Some(m) => m,
            // The plan is gone: it landed as a new ring or was abandoned. The record stays, because
            // cleanup needs the handed-over keys and checks the ring first. Writes unfreeze regardless.
            None => return,
        };
        crate::cluster::migration::ensure_running(self, &migration);
        // Coordination resumes from here, not from promotion alone, so a leader that learns of the plan
        // afterwards picks it up too. The rebalancer would on its tick, but it is off by default.
        if self.is_leader()
            && crate::cluster::rebalance::is_coordinator(self, &self.cluster_view())
        {
            crate::api::migrate::resume_migration_coordination(self);
        }
    }

    /// Points a node admitted at runtime at the primary the view assigned it. Without this it polls
    /// nobody, never hears a commit watermark, and holds everything it is sent staged and unreadable.
    pub fn follow_from_view(&self) {
        // Read and drop the cluster lock before touching replication: learner_replicas takes them
        // in the opposite order, and neither may hold both.
        let follows = {
            let view = self.cluster.read().unwrap();
            view.member(&self.own_url())
                .filter(|m| !m.voting && m.role == "shard")
                .and_then(|m| m.follows.clone())
        };
        let follows = match follows {
            Some(f) if !crate::util::same_endpoint(&f, &self.own_url()) => f,
            _ => return,
        };

        let repl = match self.replication.as_ref() {
            Some(r) => r,
            None => return,
        };
        let mut g = repl.write().unwrap();
        if g.is_leader || g.primary_addr.as_deref() == Some(follows.as_str()) {
            return;
        }
        tracing::info!(target: "membership", primary = %follows, "Following the primary named in the cluster view");
        g.primary_addr = Some(follows);
        g.last_heartbeat = Some(std::time::Instant::now());
    }

    /// Gives a newly admitted learner a send cursor so the replication driver picks it up on its
    /// next tick, instead of waiting for a write to expose the gap.
    pub fn begin_tracking_learner(&self, url: &str) {
        let collections = match self.db.as_ref() {
            Some(db) => db.list_collections().unwrap_or_default(),
            None => return,
        };
        if let Some(repl) = self.replication.as_ref() {
            repl.write().unwrap().progress.begin_tracking(url, &collections);
        }
    }

    /// Both rings for `view`, laid out once per cluster version. Takes only `ring_cache`, and the
    /// caller already holds `cluster`, so the two are always acquired in that order.
    pub(crate) fn rings_for(&self, view: &ClusterMetadata) -> (Option<Arc<BuiltRing>>, Option<Arc<BuiltRing>>) {
        {
            let cache = self.ring_cache.lock().unwrap();
            if cache.version == view.version {
                return (cache.ring.clone(), cache.target.clone());
            }
        }
        let ring = view.ring.as_ref().map(|r| Arc::new(r.build()));
        let target = view.migration.as_ref().map(|m| Arc::new(m.target.build()));

        let mut cache = self.ring_cache.lock().unwrap();
        // A newer version may have landed while we were building; that one wins and rebuilds later.
        if cache.version <= view.version {
            cache.version = view.version;
            cache.ring = ring.clone();
            cache.target = target.clone();
        }
        (ring, target)
    }

    pub fn built_ring(&self) -> Option<Arc<BuiltRing>> {
        let view = self.cluster.read().unwrap();
        self.rings_for(&view).0
    }

    pub fn migration(&self) -> Option<Migration> {
        self.cluster.read().unwrap().migration.clone()
    }

    /// Whether this node may hold `key`. `None` where ownership does not apply -- no ring, or this
    /// node outside it -- which keeps single-shard and range-based clusters unaffected.
    pub fn ownership(&self, collection: &str, key: &str) -> Option<Ownership> {
        // One acquisition for the view and the rings built from it: a verdict mixing a ring from
        // one version with a migration from the next would be wrong for the request that saw it.
        let view = self.cluster.read().unwrap();
        let (ring, target) = self.rings_for(&view);
        classify(&view, ring.as_deref()?, target.as_deref(), &self.own_url(), collection, key)
    }

    /// One ownership view for a whole scan, so a topology adoption cannot split a page.
    pub(crate) fn scan_ownership(&self, collection: &str) -> ScanOwnership {
        let view = self.cluster.read().unwrap();
        let (ring, _) = self.rings_for(&view);
        let group = ring.as_ref().and_then(|ring| group_owner(ring, &self.own_url()));
        ScanOwnership {
            ring,
            group,
            collection: collection.to_string(),
            fingerprint: view.partition_fingerprint(),
        }
    }

    pub fn get_effective_shard_url(&self, hash: u64) -> Option<(String, String, Vec<String>)> {
        if let Some(ring) = self.built_ring() {
            let owner = ring.owner(hash)?;
            return Some(self.with_override(&owner.node_url, owner.replica_urls.clone()));
        }

        let shards = self.cluster.read().unwrap().shards.clone();
        for shard in &shards {
            if shard_owns(shard, hash) {
                return Some(self.with_override(&shard.node_url, shard.replica_urls.clone()));
            }
        }
        None
    }

    /// `(where to send now, the configured owner, its replicas)`. Failover is a property of the
    /// owner, not of how the owner was chosen, so both ownership models come through here.
    fn with_override(&self, owner: &str, replicas: Vec<String>) -> (String, String, Vec<String>) {
        let mut overrides = self.primary_overrides.lock().unwrap();
        if let Some(ov) = overrides.get(owner) {
            if ov.cached_at.elapsed().as_secs() < OVERRIDE_TTL_SECS {
                return (ov.url.clone(), owner.to_string(), replicas);
            }
            overrides.remove(owner);
        }
        (owner.to_string(), owner.to_string(), replicas)
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

    /// The cached answer only, with no fallback to `original_url`. Callers that probe on a miss
    /// need to tell "nothing known" apart from "the configured owner still leads".
    pub fn cached_primary(&self, original_url: &str) -> Option<String> {
        let mut overrides = self.primary_overrides.lock().unwrap();
        match overrides.get(original_url) {
            Some(ov) if ov.cached_at.elapsed().as_secs() < OVERRIDE_TTL_SECS => Some(ov.url.clone()),
            Some(_) => { overrides.remove(original_url); None },
            None => None,
        }
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

    pub fn note_node_load(&self, url: &str, load: NodeLoad) {
        self.node_loads.lock().unwrap().insert(
            crate::util::node_key(url),
            NodeLoadSample { load, sampled_at: std::time::Instant::now() },
        );
    }

    pub fn clear_node_load(&self, url: &str) {
        self.node_loads.lock().unwrap().remove(&crate::util::node_key(url));
    }

    pub fn fresh_node_loads(&self) -> HashMap<String, NodeLoad> {
        let mut samples = self.node_loads.lock().unwrap();
        samples.retain(|_, sample| sample.sampled_at.elapsed().as_secs() < NODE_LOAD_TTL_SECS);
        let mut loads: HashMap<String, NodeLoad> = samples
            .iter()
            .map(|(url, sample)| (url.clone(), sample.load))
            .collect();
        drop(samples);

        for (url, pending) in self.routed_reads.lock().unwrap().iter() {
            if let Some(load) = loads.get_mut(url) {
                load.inflight = load.inflight.saturating_add(*pending);
            }
        }
        loads
    }

    pub fn track_routed_read(&self, url: &str) -> RoutedRead {
        let url = crate::util::node_key(url);
        let mut counts = self.routed_reads.lock().unwrap();
        let count = counts.entry(url.clone()).or_insert(0);
        *count = count.saturating_add(1);
        drop(counts);
        RoutedRead { url, counts: self.routed_reads.clone() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::metadata::Member;
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
            assert!(state.admit_write("t", 1).is_ok(), "under the bound the write is admitted");
        }

        stage_put(&col, "k2", 2);
        assert_eq!(state.admit_write("t", 1).err(), Some(Refusal::Backlog { in_flight: 3 }),
            "at the bound the leader must refuse, or the staging buffer grows without limit \
             for as long as commits are stalled");
        assert_eq!(state.metrics.writes_rejected(), 1);

        // A quorum catching up drains the buffer and reopens the door.
        col.apply_committed(col.last_appended_lsn()).unwrap();
        assert!(state.admit_write("t", 1).is_ok(), "backpressure must lift once commits catch up");
    }

    /// C26: nothing on the replication path calls `admit_write`, so the skip only reached the local
    /// write paths, where a non-leader is refused at the handler or deposed mid-write.
    #[tokio::test]
    async fn a_deposed_leader_is_still_backpressured_and_zero_disables_the_bound() {
        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let col = db.get_collection("t").unwrap();
        for i in 0..5 {
            stage_put(&col, &format!("k{}", i), i);
        }

        let deposed = AppState::for_admission_test(config_with_bound(&root, 1), db.clone(), false);
        assert_eq!(deposed.admit_write("t", 1).err(), Some(Refusal::Backlog { in_flight: 5 }),
            "losing leadership stopped the bound applying to a buffer that can no longer drain: \
             nothing this node still accepts will ever reach a quorum");

        let unbounded = AppState::for_admission_test(config_with_bound(&root, 0), db.clone(), true);
        assert!(unbounded.admit_write("t", 10).is_ok(), "0 opts out of the bound");
    }

    /// IB-014: the bound was checked against a pre-append count with nothing reserved, so one bulk
    /// request appended as many frames as it liked and concurrent writes each passed the same sample.
    #[tokio::test]
    async fn a_batch_is_admitted_as_a_whole_and_reservations_stop_concurrent_writes_sharing_a_count() {
        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let state = AppState::for_admission_test(config_with_bound(&root, 4), db.clone(), true);
        let col = db.get_collection("t").unwrap();

        assert_eq!(state.admit_write("t", 10).err(), Some(Refusal::TooWide { frames: 10 }),
            "ten documents in one bulk write staged ten frames under a bound of four");

        stage_put(&col, "k0", 0);
        assert_eq!(state.admit_write("t", 4).err(), Some(Refusal::Backlog { in_flight: 1 }),
            "a batch has to fit beside what is already staged, not just against the bound");

        let held = state.admit_write("t", 3).unwrap().expect("3 fits beside 1 staged frame");
        assert_eq!(state.admit_write("t", 1).err(), Some(Refusal::Backlog { in_flight: 4 }),
            "an admitted write that has not appended yet still occupies its slots");

        drop(held);
        assert!(state.admit_write("t", 3).is_ok(),
            "a write that never reached its append must give the capacity back");
    }

    #[tokio::test]
    async fn the_bound_counts_a_reservation_until_the_frame_it_covers_is_staged() {
        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let state = AppState::for_admission_test(config_with_bound(&root, 2), db.clone(), true);
        let col = db.get_collection("t").unwrap();

        let first = state.admit_write("t", 1).unwrap().unwrap();
        // What the write path does: append under the reservation, release it once staged.
        stage_put(&col, "k0", 0);
        drop(first);
        assert_eq!(col.pending_len(), 1);

        let second = state.admit_write("t", 1).unwrap().unwrap();
        assert_eq!(state.admit_write("t", 1).err(), Some(Refusal::Backlog { in_flight: 2 }),
            "one staged frame plus one reserved fills a bound of two");
        drop(second);
    }

    #[tokio::test]
    async fn the_quorum_is_this_nodes_shard_group_not_every_shard_in_the_cluster() {
        use crate::ring::{HashRing, RingShard};

        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let config: NodeConfig = serde_json::from_value(serde_json::json!({
            "node_id": "b1", "role": "shard", "shard_role": "primary",
            "listen_addr": "127.0.0.1:9601",
            "data_dir": root.to_string_lossy(),
            "replicas": ["http://127.0.0.1:9602", "http://127.0.0.1:9603"],
            "peers": ["http://127.0.0.1:9602", "http://127.0.0.1:9603"],
        })).unwrap();
        let state = AppState::for_admission_test(config, db, true);

        let mut view = state.cluster_view();
        view.version += 1;
        view.seeded = false;
        view.updated_by = "operator".into();
        // Another shard's nodes are members and voters too: the list has no shard affinity.
        for url in ["http://127.0.0.1:9701", "http://127.0.0.1:9702"] {
            view.members.push(Member {
                url: url.into(), node_id: None, role: "shard".into(),
                shard_role: None, voting: true, follows: None,
            });
        }
        view.ring = Some(HashRing { vnodes: 128, shards: vec![
            RingShard {
                node_url: "http://127.0.0.1:9601".into(),
                replica_urls: vec!["http://127.0.0.1:9602".into(), "http://127.0.0.1:9603".into()],
            },
            RingShard {
                node_url: "http://127.0.0.1:9701".into(),
                replica_urls: vec!["http://127.0.0.1:9702".into()],
            },
        ]});
        assert!(matches!(state.adopt_cluster(view), Adoption::Adopted { .. }));

        let mut voters = state.voting_set();
        voters.sort();
        assert_eq!(voters, vec![
            "http://127.0.0.1:9601".to_string(),
            "http://127.0.0.1:9602".to_string(),
            "http://127.0.0.1:9603".to_string(),
        ], "counting all five makes an election need three votes from a group that has three \
            nodes, and become_leader would replicate this group's frames into the other one");
    }

    #[tokio::test]
    async fn admitting_a_learner_never_moves_the_quorum() {
        use crate::cluster::metadata::{plan_join, JoinRequest};
        use crate::replication::{parse_write_concern, write_concern::required_acks};

        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let mut config: NodeConfig = serde_json::from_value(serde_json::json!({
            "node_id": "n1", "role": "shard", "shard_role": "primary",
            "listen_addr": "127.0.0.1:9501",
            "data_dir": root.to_string_lossy(),
            "replicas": ["http://127.0.0.1:9502", "http://127.0.0.1:9503"],
            "peers": ["http://127.0.0.1:9502", "http://127.0.0.1:9503"],
        })).unwrap();
        config.flow_control.max_uncommitted_frames = 0;

        let state = AppState::for_admission_test(config, db.clone(), true);
        state.replication.as_ref().unwrap().write().unwrap().replicas =
            vec!["http://127.0.0.1:9502".into(), "http://127.0.0.1:9503".into()];

        let majority = parse_write_concern(Some("majority")).unwrap();
        let before = required_acks(&majority, state.voting_replicas().len());
        assert_eq!(before, 2, "two of three");
        assert_eq!(state.replication_targets().len(), 2);

        let mut req = JoinRequest {
            url: "http://127.0.0.1:9504".into(), node_id: None, role: None,
            shard_role: None, follows: None, voting: None,
        };
        req.follows = Some("http://127.0.0.1:9501".into());
        let next = plan_join(&state.cluster_view(), "n1", "http://127.0.0.1:9501", &req).unwrap();
        assert!(matches!(state.adopt_cluster(next), Adoption::Adopted { .. }));

        assert_eq!(state.voting_replicas().len(), 2,
            "the quorum set must not grow when a learner joins");
        assert_eq!(required_acks(&majority, state.voting_replicas().len()), before,
            "w=majority must still mean the same number of acks, or a learner could satisfy it");
        assert_eq!(state.learner_replicas(), vec!["http://127.0.0.1:9504".to_string()]);
        assert_eq!(state.replication_targets().len(), 3,
            "but the learner must still be shipped frames");

        assert!(!state.is_voting_replica("http://127.0.0.1:9504"),
            "an ack from here must never be counted");
        assert!(state.is_voting_replica("http://127.0.0.1:9502"));

        // A learner acking everything must not by itself commit anything.
        let col = db.get_collection("t").unwrap();
        let lsn = stage_put(&col, "k", 1);
        // Stand in for the leader's own fsync landing, which is what note_ack counts as its vote,
        // and for the write path recording the append that lets a quorum there commit at all.
        col.durable_lsn.store(lsn, std::sync::atomic::Ordering::SeqCst);
        state.note_leader_append("t", lsn);

        state.note_ack("http://127.0.0.1:9504", "t", lsn, state.current_term()).unwrap();
        assert_eq!(state.committed_lsn("t"), 0,
            "leader plus a learner is one of three, not a majority");

        state.note_ack("http://127.0.0.1:9502", "t", lsn, state.current_term()).unwrap();
        assert_eq!(state.committed_lsn("t"), lsn, "leader plus a voter is a majority");
    }

    #[tokio::test]
    async fn a_learner_knows_not_to_stand_for_election() {
        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let config: NodeConfig = serde_json::from_value(serde_json::json!({
            "node_id": "n4", "role": "shard", "shard_role": "replica",
            "listen_addr": "127.0.0.1:9504",
            "data_dir": root.to_string_lossy(),
        })).unwrap();

        let state = AppState::for_admission_test(config, db, false);
        assert!(!state.is_learner(),
            "a node absent from the view keeps its configured behaviour; a view that has not \
             arrived yet must not silently strip a vote");

        // The node's own seed already lists it as voting, so this replaces that entry rather than
        // adding a second one -- which is exactly what the join path does.
        let mut view = state.cluster_view().with_member("operator", crate::cluster::metadata::Member {
            url: "http://127.0.0.1:9504".into(), node_id: None, role: "shard".into(),
            shard_role: Some("replica".into()), voting: false, follows: Some("http://127.0.0.1:9501".into()),
        });
        view.version = 5;
        assert!(matches!(state.adopt_cluster(view), Adoption::Adopted { .. }));

        assert!(state.is_learner(),
            "once the view says non-voting, this node must recognise itself as a learner");
    }


    #[test]
    fn a_promoted_follower_inherits_the_rest_of_the_cluster_as_its_quorum() {
        let root = temp_root();
        let state = shard_state(&root, serde_json::json!(["http://127.0.0.1:9"]),
            serde_json::json!(["http://127.0.0.1:2", "http://127.0.0.1:3"]));

        assert_eq!(state.voting_peers(), vec![
            "http://127.0.0.1:9".to_string(),
            "http://127.0.0.1:2".to_string(),
            "http://127.0.0.1:3".to_string(),
        ], "configured replicas come first, peers fill in the rest, and this node is never in it");

        let deduped = shard_state(&root, serde_json::json!(["http://127.0.0.1:2/"]),
            serde_json::json!(["http://127.0.0.1:2", "http://127.0.0.1:3"]));
        assert_eq!(deduped.voting_peers().len(), 2,
            "the same endpoint written differently must not be counted twice");

        let alone = shard_state(&root, serde_json::json!([]), serde_json::json!([]));
        assert!(alone.voting_peers().is_empty());
        assert_eq!(alone.voting_set().len(), 1, "a sole node is still its own quorum");
    }

    #[test]
    fn the_election_quorum_counts_replicas_the_commit_quorum_already_waits_for() {
        let root = temp_root();
        let state = shard_state(&root, serde_json::json!(["http://127.0.0.1:2", "http://127.0.0.1:3"]),
            serde_json::json!([]));

        assert_eq!(state.voting_set().len(), 3,
            "counting only `peers` here makes this a majority of one while its writes still              need two acks, so every such node elects itself");
    }

    #[test]
    fn a_published_view_replaces_the_configured_quorum() {
        let root = temp_root();
        let state = shard_state(&root, serde_json::json!([]),
            serde_json::json!(["http://127.0.0.1:2", "http://127.0.0.1:3"]));

        let mut view = state.cluster_view();
        view.members = ["http://127.0.0.1:1", "http://127.0.0.1:2"].iter()
            .map(|url| voter(url))
            .collect();
        view.version = 5;
        view.seeded = false;
        assert!(matches!(state.adopt_cluster(view), Adoption::Adopted { .. }));

        assert_eq!(state.voting_set().len(), 2,
            "an agreed view is what stops two nodes computing different majorities from              their own configs");
    }

    #[test]
    fn a_view_that_names_no_voters_leaves_the_quorum_where_it_was() {
        let root = temp_root();
        let state = shard_state(&root, serde_json::json!([]),
            serde_json::json!(["http://127.0.0.1:2", "http://127.0.0.1:3"]));

        // `voting` defaults to false, so a hand-written view omitting it names no voters at all.
        let mut view = state.cluster_view();
        view.members = vec![Member {
            url: "http://shard-x:9999".into(), node_id: None, role: "shard".into(),
            shard_role: Some("primary".into()), voting: false, follows: None,
        }];
        view.version = 5;
        view.seeded = false;
        assert!(matches!(state.adopt_cluster(view), Adoption::Adopted { .. }));

        assert_eq!(state.voting_set().len(), 3,
            "a view that does not name this node a voter must not strip its quorum to nothing");
    }

    const HALF: u64 = 9223372036854775808;

    fn voter(url: &str) -> Member {
        Member {
            url: url.to_string(), node_id: None, role: "shard".into(),
            shard_role: None, voting: true, follows: None,
        }
    }

    fn shard_state(root: &std::path::Path, replicas: serde_json::Value, peers: serde_json::Value) -> AppState {
        AppState::for_routing_test(serde_json::from_value(serde_json::json!({
            "node_id": "n1",
            "role": "shard",
            "shard_role": "primary",
            "listen_addr": "127.0.0.1:1",
            "data_dir": root.to_string_lossy(),
            "replicas": replicas,
            "peers": peers,
        })).unwrap())
    }

    fn router_state(root: &std::path::Path) -> AppState {
        AppState::for_routing_test(serde_json::from_value(serde_json::json!({
            "node_id": "r1",
            "role": "router",
            "listen_addr": "127.0.0.1:1",
            "data_dir": root.to_string_lossy(),
            "shard_map": [
                {"start_hash": 0, "end_hash": HALF, "node_url": "http://a", "replica_urls": ["http://a2"]},
                {"start_hash": HALF, "end_hash": 0, "node_url": "http://b", "replica_urls": ["http://b2"]}],
        })).unwrap())
    }

    #[test]
    fn router_reservations_temporarily_raise_a_nodes_reported_load() {
        let root = temp_root();
        let state = router_state(&root);
        state.note_node_load("http://a", NodeLoad { inflight: 2, latency_ewma_us: 1_000 });

        let routed = state.track_routed_read("http://a/");
        assert_eq!(state.fresh_node_loads()["a"].inflight, 3);

        drop(routed);
        assert_eq!(state.fresh_node_loads()["a"].inflight, 2);
    }

    #[test]
    fn routing_follows_the_adopted_view_not_the_configured_shard_map() {
        let root = temp_root();
        let state = router_state(&root);

        // Seeded from config, so the starting behaviour matches the old config-only routing.
        let (owner, _, replicas) = state.get_effective_shard_url(10).unwrap();
        assert_eq!(owner, "http://a");
        assert_eq!(replicas, vec!["http://a2".to_string()]);

        // The same range, now owned by a node the config file has never heard of.
        let moved = ClusterMetadata {
            version: 2,
            updated_by: "operator".into(),
            seeded: false,
            ring: None,
            migration: None,
            members: Vec::new(),
            shards: vec![
                crate::ring::ShardInfo {
                    start_hash: 0, end_hash: HALF,
                    node_url: "http://c".into(), replica_urls: vec!["http://c2".into()] },
                crate::ring::ShardInfo {
                    start_hash: HALF, end_hash: 0,
                    node_url: "http://b".into(), replica_urls: vec!["http://b2".into()] },
            ],
            index_catalog: Default::default(),
        };
        assert_eq!(state.adopt_cluster(moved), Adoption::Adopted { from: 1, to: 2 });

        let (owner, original, replicas) = state.get_effective_shard_url(10).unwrap();
        assert_eq!(owner, "http://c",
            "a key must route by the live view; reading config.shard_map here would still say http://a");
        assert_eq!(original, "http://c");
        assert_eq!(replicas, vec!["http://c2".to_string()]);

        assert_eq!(state.get_effective_shard_url(HALF + 1).unwrap().0, "http://b",
            "the untouched range keeps its owner");

        assert_eq!(state.shard_owners().len(), 2);
        assert_eq!(state.cluster_version(), 2);
    }

    #[test]
    fn a_refused_view_leaves_routing_and_disk_untouched() {
        let root = temp_root();
        let state = router_state(&root);
        let dir = root.to_string_lossy().to_string();

        let holed = ClusterMetadata {
            version: 50,
            updated_by: "operator".into(),
            seeded: false,
            ring: None,
            migration: None,
            members: Vec::new(),
            // Only half the ring: every key above HALF would route nowhere.
            shards: vec![crate::ring::ShardInfo {
                start_hash: 0, end_hash: HALF,
                node_url: "http://c".into(), replica_urls: Vec::new() }],
            index_catalog: Default::default(),
        };
        assert!(matches!(state.adopt_cluster(holed), Adoption::Rejected(_)));

        assert_eq!(state.cluster_version(), 1, "a refused view must not take effect in memory");
        assert_eq!(state.get_effective_shard_url(HALF + 1).unwrap().0, "http://b",
            "keys must keep routing where they did before the bad update");
        assert!(ClusterMetadata::load(&dir).unwrap().map_or(true, |v| v.version == 1),
            "a refused view must not reach disk, or the next boot adopts what we just rejected");
    }
}
