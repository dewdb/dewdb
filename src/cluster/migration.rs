//! Online shard handover with a bulk copy and a brief finalization barrier.
//! Sources drive idempotent batches while cluster metadata coordinates cutover.

use crate::cluster::metadata::{Migration, MigrationPhase};
use crate::consensus::config::CONFIG_LOG;
use crate::replication::stream::replicate_and_await;
use crate::replication::write_concern::WriteQuorum;
use crate::ring::{hash_key, keyspace_movement, HashRing};
use crate::state::AppState;
use crate::storage::frame::{HandoverRecord, MAX_FRAME_SIZE};
use crate::storage::FrameHeader;
use crate::util::{same_endpoint, write_atomic};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::Path;
use tracing::{info, warn};

const MIGRATION_FILE: &str = "migration.meta";

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct DataMovementConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_delay_ms")]
    pub batch_delay_ms: u64,
}

fn default_batch_size() -> usize { 64 }
fn default_batch_delay_ms() -> u64 { 5 }

impl Default for DataMovementConfig {
    fn default() -> Self {
        Self { batch_size: default_batch_size(), batch_delay_ms: default_batch_delay_ms() }
    }
}

impl DataMovementConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > 1024 {
            return Err("data_movement.batch_size must be between 1 and 1024".to_string());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MigrateBatch {
    pub migration_id: String,
    #[serde(default)]
    pub phase: MigrationPhase,
    pub collection: String,
    pub docs: Vec<MigrateDoc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MigrateDoc {
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MigrateReset {
    pub migration_id: String,
    pub phase: MigrationPhase,
    pub source: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationProgress {
    pub id: String,
    pub phase: MigrationPhase,
    /// The ring this handover was for. Cleanup compares it against the live ring: a plan that was
    /// abandoned rather than completed must never be treated as licence to delete.
    #[serde(skip)]
    pub target: HashRing,
    pub pushed: usize,
    pub total: usize,
    pub done: bool,
    pub error: Option<String>,
    /// Keys handed over, so cleanup after the flip deletes exactly what moved rather than
    /// re-deriving the set from a ring that may have changed again.
    #[serde(skip)]
    pub handed_over: HashSet<(String, String)>,
}

#[derive(Default)]
pub struct MigrationRuns {
    pub current: Option<MigrationProgress>,
    /// The phase a `push_until_done` task is alive for right now. `current` cannot stand in for it:
    /// a task that returns because leadership moved leaves its record behind (bugs.md L21).
    pub pushing: Option<(String, MigrationPhase)>,
    /// Prevents duplicate coordinator loops after recovery.
    pub coordinating: Option<String>,
    /// Sources the coordinator here is still waiting on. Published so a stalled handover is
    /// visible through the API rather than only in this node's logs.
    pub waiting_on: Vec<String>,
    pub completed_resets: HashSet<(String, String)>,
}

impl MigrationRuns {
    pub fn restored(data_dir: &str) -> Self {
        let meta = MigrationMeta::load(data_dir);
        Self {
            current: meta.completed.map(|c| MigrationProgress {
                id: c.id,
                phase: c.phase,
                target: c.target,
                pushed: c.handed_over.len(),
                total: c.handed_over.len(),
                done: true,
                error: None,
                handed_over: c.handed_over,
            }),
            pushing: None,
            coordinating: None,
            waiting_on: Vec::new(),
            completed_resets: meta.completed_resets,
        }
    }
}

/// The part of a handover that has to outlive the process: what this node handed over, and which
/// sources it has already reset for. Both drive a deletion, and a lost record leaves a stale copy
/// that shadows the live one if the node comes back into the ring.
#[derive(Serialize, Deserialize, Default)]
pub struct MigrationMeta {
    pub completed: Option<CompletedHandover>,
    #[serde(default)]
    pub completed_resets: HashSet<(String, String)>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CompletedHandover {
    pub id: String,
    pub phase: MigrationPhase,
    pub target: HashRing,
    pub handed_over: HashSet<(String, String)>,
}

impl MigrationMeta {
    pub fn load(data_dir: &str) -> Self {
        fs::read(Path::new(data_dir).join(MIGRATION_FILE))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default()
    }

    pub fn save(&self, data_dir: &str) -> io::Result<()> {
        let bytes = serde_json::to_vec(self).map_err(io::Error::other)?;
        write_atomic(Path::new(data_dir), MIGRATION_FILE, &bytes)
    }
}

/// Written when a phase finishes, not per batch: an unfinished record is never read back, since a
/// restart mid-copy re-plans from the view. Restoring one would be worse than losing it, because
/// `ensure_running` reads a matching id and phase as already running and would not restart.
fn persist(state: &AppState) {
    let meta = {
        let runs = state.migrations.lock().unwrap();
        MigrationMeta {
            completed: runs.current.as_ref().filter(|p| p.done).map(|p| CompletedHandover {
                id: p.id.clone(),
                phase: p.phase,
                target: p.target.clone(),
                handed_over: p.handed_over.clone(),
            }),
            completed_resets: runs.completed_resets.clone(),
        }
    };
    if let Err(e) = meta.save(&state.config.data_dir) {
        warn!(target: "migration", error = %e,
            "Could not persist handover bookkeeping; a restart before cleanup strands the copies");
    }
}

/// Starts the copy for `migration` unless this node is already running it. Called on every view
/// adoption, so it must be cheap and idempotent for the common case of no change.
pub fn ensure_running(state: &AppState, migration: &Migration) {
    let key = (migration.id.clone(), migration.phase);
    {
        let runs = state.migrations.lock().unwrap();
        // A finished phase is not redone; an unfinished record is no evidence that anything is
        // still pushing it, since the task drops its record where it stopped (bugs.md L21).
        if runs.pushing.as_ref() == Some(&key) || runs.current.as_ref()
            .is_some_and(|p| p.id == migration.id && p.phase == migration.phase && p.done)
        {
            return;
        }
    }
    if state.db.is_none() || !state.is_leader() {
        // Replicas receive the moved keys through their own leader's replication, not from here.
        return;
    }
    {
        // Claimed before the keyspace scan below, so two adoptions racing here cannot both push.
        // Keyed by phase, not by plan: the next phase must be startable while the last one's task
        // is still winding down, or the handover stops at the phase boundary.
        let mut runs = state.migrations.lock().unwrap();
        if runs.pushing.as_ref() == Some(&key) {
            return;
        }
        runs.pushing = Some(key.clone());
    }
    let reservation = PushGuard { state: state.clone(), key };

    let outgoing = match plan_outgoing(state, migration) {
        Ok(work) => work,
        Err(e) => {
            warn!(target: "migration", id = %migration.id, error = %e, "Could not plan the move");
            return;
        },
    };

    let total: usize = outgoing.values().map(Vec::len).sum();
    let done = total == 0 && migration.phase == MigrationPhase::Copy;
    {
        let mut runs = state.migrations.lock().unwrap();
        runs.current = Some(MigrationProgress {
            id: migration.id.clone(),
            phase: migration.phase,
            target: migration.target.clone(),
            pushed: 0,
            total,
            done,
            error: None,
            handed_over: HashSet::new(),
        });
    }

    if done {
        persist(state);
        info!(target: "migration", id = %migration.id, "Nothing to hand over from this node");
        return;
    }

    info!(target: "migration", id = %migration.id, keys = total, "Handing over keys");
    let state = state.clone();
    let plan = migration.clone();
    tokio::spawn(async move {
        let _reservation = reservation;
        push_until_done(state, plan).await
    });
}

/// Clears `pushing` however the push ends, so a phase whose pusher stopped early is restartable.
struct PushGuard {
    state: AppState,
    key: (String, MigrationPhase),
}

impl Drop for PushGuard {
    fn drop(&mut self) {
        let mut runs = self.state.migrations.lock().unwrap();
        if runs.pushing.as_ref() == Some(&self.key) {
            runs.pushing = None;
        }
    }
}

/// Retries for as long as the plan is in the view. A destination that is briefly down, or has not
/// yet adopted the plan and so refuses the batch, is the normal case rather than a failure -- and
/// giving up would leave ownership frozen with no way forward but an abort.
async fn push_until_done(state: AppState, migration: Migration) {
    let id = migration.id.clone();
    let mut attempt: u32 = 0;

    loop {
        match state.migration() {
            Some(m) if m.id == id && m.phase == migration.phase => {},
            // Completed by someone else, abandoned, or replaced. Nothing left to push.
            _ => return,
        }
        if !state.is_leader() {
            record_error(&state, &id, migration.phase,
                "leadership changed during migration".to_string());
            return;
        }

        // Taken and dropped, not held: what the barrier has to close is the window where a write
        // decided it owned a key under the pre-finalizing view and has not appended yet. Draining
        // those settles it -- every write that starts after this sees `Ownership::Moving` and is
        // refused, so the scan and the round trips below cannot be overtaken. Holding it across
        // them instead blocks every write on the node for the length of a keyspace scan.
        if migration.phase == MigrationPhase::Finalizing {
            drop(state.write_gate.write().await);
        }
        if !state.migration().is_some_and(|m| {
            m.id == id && m.phase == migration.phase
        }) {
            return;
        }

        let outgoing = match plan_outgoing(&state, &migration) {
            Ok(work) => work,
            Err(e) => {
                record_error(&state, &id, migration.phase, e);
                backoff(attempt).await;
                attempt += 1;
                continue;
            },
        };

        let total: usize = outgoing.values().map(Vec::len).sum();
        {
            let mut runs = state.migrations.lock().unwrap();
            match runs.current.as_mut().filter(|p| {
                p.id == id && p.phase == migration.phase
            }) {
                Some(p) => { p.total = total; p.pushed = 0; },
                None => return,
            }
        }

        let result = if migration.phase == MigrationPhase::Finalizing {
            reset_destinations(&state, &migration).await
        } else {
            Ok(())
        };
        let result = match result {
            Ok(()) => push_all(&state, &migration, outgoing).await,
            Err(e) => Err(e),
        };

        match result {
            Ok(()) => {
                if !state.is_leader() {
                    record_error(&state, &id, migration.phase,
                        "leadership changed during migration".to_string());
                    return;
                }
                {
                    let mut runs = state.migrations.lock().unwrap();
                    if let Some(p) = runs.current.as_mut().filter(|p| {
                        p.id == id && p.phase == migration.phase
                    }) {
                        p.done = true;
                        p.error = None;
                        info!(target: "migration", id = %id, pushed = p.pushed, "Handover complete");
                    }
                }
                persist(&state);
                replicate_handover(&state, HandoverRecord {
                    id: id.clone(),
                    target: migration.target.clone(),
                }).await;
                return;
            },
            Err(e) => {
                warn!(target: "migration", id = %id, attempt, error = %e, "Handover attempt failed");
                record_error(&state, &id, migration.phase, e);
                backoff(attempt).await;
                attempt += 1;
            },
        }
    }
}

fn record_error(state: &AppState, id: &str, phase: MigrationPhase, error: String) {
    let mut runs = state.migrations.lock().unwrap();
    if let Some(p) = runs.current.as_mut().filter(|p| p.id == id && p.phase == phase) {
        p.done = false;
        p.error = Some(error);
    }
}

// Quick at first so a destination that is merely a moment behind costs almost nothing, then wide
// enough that a node that is down does not turn into a busy loop against it.
async fn backoff(attempt: u32) {
    let ms = 200u64 << attempt.min(4);
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

/// Which of our keys go where. Grouped by destination so one connection carries many keys.
fn plan_outgoing(state: &AppState, migration: &Migration)
    -> Result<HashMap<String, Vec<(String, String)>>, String>
{
    let db = state.db.as_ref().ok_or("no database")?;
    let view = state.cluster_view();
    let ring = view.ring.as_ref().ok_or("no ring to move away from")?.build();
    let target = migration.target.build();
    let own = state.own_url();

    let mine = source_group(view.ring.as_ref().unwrap(), &own)
        .ok_or("this node is not in the ring")?;

    let mut out: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for collection in db.list_collections().map_err(|e| e.to_string())? {
        let col = db.get_collection(&collection).map_err(|e| e.to_string())?;
        col.for_each_key(None, None, None, |key| {
            let hash = hash_key(&collection, key);
            let now = ring.owner(hash).map(|s| s.node_url.as_str()).unwrap_or_default();
            if same_endpoint(now, &mine) {
                if let Some(next) = target.owner(hash)
                    .filter(|s| !same_endpoint(&s.node_url, &mine))
                {
                    out.entry(next.node_url.clone()).or_default()
                        .push((collection.clone(), key.to_string()));
                }
            }
            true
        });
    }
    Ok(out)
}

fn source_group(ring: &HashRing, own: &str) -> Option<String> {
    ring.shards.iter()
        .find(|s| same_endpoint(&s.node_url, own)
            || s.replica_urls.iter().any(|r| same_endpoint(r, own)))
        .map(|s| s.node_url.clone())
}

fn destinations(state: &AppState, migration: &Migration) -> Result<Vec<String>, String> {
    let view = state.cluster_view();
    let ring = view.ring.as_ref().ok_or("no ring to move away from")?;
    let mine = source_group(ring, &state.own_url()).ok_or("this node is not in the ring")?;
    let mut seen = HashSet::new();
    Ok(keyspace_movement(&ring.build(), &migration.target.build()).transfers.into_iter()
        .filter(|transfer| same_endpoint(&transfer.from, &mine))
        .map(|transfer| transfer.to)
        .filter(|destination| seen.insert(crate::util::node_key(destination)))
        .collect())
}

async fn reset_destinations(state: &AppState, migration: &Migration) -> Result<(), String> {
    let source = source_group(
        state.cluster_view().ring.as_ref().ok_or("no ring to move away from")?,
        &state.own_url(),
    ).ok_or("this node is not in the ring")?;

    for destination in destinations(state, migration)? {
        let reset = MigrateReset {
            migration_id: migration.id.clone(),
            phase: migration.phase,
            source: source.clone(),
        };
        let url = format!("{}/internal/migrate-reset", destination);
        let response = state.client.post(&url).json(&reset).send().await
            .map_err(|e| format!("{} unreachable: {}", destination, e))?;
        if !response.status().is_success() {
            return Err(format!("{} refused final reset: {}", destination, response.status()));
        }
    }
    Ok(())
}

/// Raw JSON bytes per request: `data_movement.batch_size` bounds the count only, so a batch of large
/// documents is one the destination refuses (`H19`). Conservative -- a second number here is `H13`.
const MIGRATE_BATCH_BYTES: usize = MAX_FRAME_SIZE as usize;

/// Splits by size, under a count split that already happened. The first document of a group is
/// always taken: one over the budget still has to travel.
fn size_bounded_docs(docs: Vec<MigrateDoc>) -> Vec<Vec<MigrateDoc>> {
    let mut groups: Vec<Vec<MigrateDoc>> = Vec::new();
    let mut bytes = 0usize;
    for doc in docs {
        let size = doc.key.len() + serde_json::to_vec(&doc.value).map_or(0, |v| v.len());
        match groups.last_mut() {
            Some(group) if bytes + size <= MIGRATE_BATCH_BYTES => {
                bytes += size;
                group.push(doc);
            },
            _ => {
                bytes = size;
                groups.push(vec![doc]);
            },
        }
    }
    groups
}

async fn push_all(
    state: &AppState,
    migration: &Migration,
    outgoing: HashMap<String, Vec<(String, String)>>,
) -> Result<(), String> {
    let db = state.db.as_ref().ok_or("no database")?.clone();

    for (destination, keys) in outgoing {
        // Batched per collection: the receiver writes into one collection at a time.
        let mut by_collection: HashMap<String, Vec<String>> = HashMap::new();
        for (collection, key) in keys {
            by_collection.entry(collection).or_default().push(key);
        }

        for (collection, keys) in by_collection {
            let col = db.get_collection(&collection).map_err(|e| e.to_string())?;
            for chunk in keys.chunks(state.config.data_movement.batch_size) {
                let mut docs = Vec::with_capacity(chunk.len());
                for key in chunk {
                    // Read committed state only. An uncommitted write may still be revoked by a
                    // leader change, and handing it over would make it durable somewhere else.
                    match col.get(key) {
                        Ok(Some(value)) => docs.push(MigrateDoc { key: key.clone(), value }),
                        Ok(None) => continue,
                        Err(e) => return Err(format!("reading {}/{}: {}", collection, key, e)),
                    }
                }
                if docs.is_empty() {
                    continue;
                }

                let sent = docs.len();
                for group in size_bounded_docs(docs) {
                    let batch = MigrateBatch {
                        migration_id: migration.id.clone(),
                        phase: migration.phase,
                        collection: collection.clone(),
                        docs: group,
                    };
                    let url = format!("{}/internal/migrate", destination);
                    let response = state.client.post(&url).json(&batch).send().await
                        .map_err(|e| format!("{} unreachable: {}", destination, e))?;
                    if !response.status().is_success() {
                        return Err(format!("{} refused the batch: {}", destination, response.status()));
                    }
                }

                {
                    let mut runs = state.migrations.lock().unwrap();
                    if let Some(p) = runs.current.as_mut().filter(|p| {
                        p.id == migration.id && p.phase == migration.phase
                    }) {
                        p.pushed += sent;
                        for key in chunk {
                            p.handed_over.insert((collection.clone(), key.clone()));
                        }
                    } else {
                        return Err("migration was replaced while copying".to_string());
                    }
                }

                if migration.phase == MigrationPhase::Finalizing
                    || state.config.data_movement.batch_delay_ms == 0
                {
                    tokio::task::yield_now().await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        state.config.data_movement.batch_delay_ms,
                    )).await;
                }
            }
        }
    }
    Ok(())
}

/// Per record, which is one per phase completion and small: an id and a ring.
const HANDOVER_COMMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Puts the handover record in this group's replicated log. `migration.meta` brings it back through
/// a restart of the same process but not to a peer elected in its place, and after the flip the plan
/// is out of the view, so a new leader has nothing to re-derive it from (bugs.md `C18`).
///
/// Not awaited by the caller's success: the handover itself is already done, and a record that did
/// not commit leaves cleanup exactly where it was before this existed rather than worse.
pub(crate) async fn replicate_handover(state: &AppState, record: HandoverRecord) {
    let db = match state.db.as_ref() {
        Some(db) => db.clone(),
        None => return,
    };
    let col = match db.get_collection(CONFIG_LOG) {
        Ok(col) => col,
        Err(e) => {
            warn!(target: "migration", error = %e, "Could not open the config log for the handover record");
            return;
        },
    };

    let term = state.current_term();
    let appended = {
        let col = col.clone();
        tokio::task::spawn_blocking(move || col.record_handover(record, term)).await
    };
    let (frame, _wal_id, _offset, lsn) = match appended {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => {
            warn!(target: "migration", error = %e, "Could not append the handover record");
            return;
        },
        Err(e) => {
            warn!(target: "migration", error = %e, "Handover record append task failed");
            return;
        },
    };

    state.note_leader_append(CONFIG_LOG, lsn);
    let commit = col.enqueue_commit();
    let prev_lsn = FrameHeader::parse(&frame).map_or(0, |h| h.prev_lsn);
    let commit_index = state.committed_lsn(CONFIG_LOG);
    let quorum = WriteQuorum::Majority(state.quorum_config());

    let replicating = replicate_and_await(
        state.clone(), CONFIG_LOG.to_string(), frame, term, commit_index, lsn, prev_lsn,
        quorum.clone(), HANDOVER_COMMIT_TIMEOUT,
    );
    let (holders, committed) = tokio::join!(replicating, commit);
    if !quorum.met(&holders) {
        warn!(target: "migration", acks = holders.len(),
            "Handover record did not reach a quorum; a leader change before cleanup strands the copies");
    }
    if let Ok(Err(e)) = committed {
        warn!(target: "migration", error = %e, "Handover record was not made durable here");
    }
}

/// The ring this node moved keys for under `id`, from the replicated record if it is there and from
/// this node's own run if it is not. Both answer the same question; only the first survives the
/// group electing someone else.
fn handover_target(state: &AppState, id: &str) -> Option<HashRing> {
    let recorded = state.db.as_ref()
        .and_then(|db| db.existing_collection(CONFIG_LOG))
        .and_then(|col| col.committed_handover())
        .filter(|record| record.id == id)
        .map(|record| record.target);
    recorded.or_else(|| {
        let runs = state.migrations.lock().unwrap();
        runs.current.as_ref().filter(|p| p.id == id && p.done).map(|p| p.target.clone())
    })
}

/// Every key this node holds that `ring` says belongs to another group. A node the ring does not
/// name at all owns nothing, which is the shard dropped from the ring -- the one most likely to be
/// sitting on a whole shard's worth of copies.
fn keys_not_ours(state: &AppState, ring: &HashRing) -> HashSet<(String, String)> {
    let db = match state.db.as_ref() {
        Some(db) => db,
        None => return HashSet::new(),
    };
    let built = ring.build();
    let mine = source_group(ring, &state.own_url());
    let mut out = HashSet::new();

    for collection in db.list_collections().unwrap_or_default() {
        // A system log is this group's own consensus state and is not part of the keyspace.
        if crate::consensus::config::is_system_collection(&collection) {
            continue;
        }
        let col = match db.get_collection(&collection) {
            Ok(col) => col,
            Err(_) => continue,
        };
        col.for_each_key(None, None, None, |key| {
            let hash = hash_key(&collection, key);
            let ours = mine.as_deref().is_some_and(|mine| {
                built.owner(hash).is_some_and(|shard| same_endpoint(&shard.node_url, mine))
            });
            if !ours {
                out.insert((collection.clone(), key.to_string()));
            }
            true
        });
    }
    out
}

/// Keys this node handed over, but only once the ring it was handing them over *for* is the ring
/// actually in force. An abandoned plan leaves the same record behind, and acting on it would
/// delete keys this node still owns.
///
/// Derived rather than remembered: the set of moved keys has no bound, so a log entry carrying it
/// would fit in neither a frame nor a replicate body, and the ring that moved them answers the same
/// question in one small record. The caller re-checks live ownership per key regardless.
pub fn handed_over_after_flip(state: &AppState, id: &str) -> HashSet<(String, String)> {
    let live = match state.cluster_view().ring {
        Some(ring) => ring,
        None => return HashSet::new(),
    };
    match handover_target(state, id) {
        Some(target) if target == live => keys_not_ours(state, &live),
        _ => HashSet::new(),
    }
}

pub fn progress(state: &AppState) -> Option<MigrationProgress> {
    state.migrations.lock().unwrap().current.clone()
}

pub fn reset_completed(state: &AppState, id: &str, source: &str) -> bool {
    state.migrations.lock().unwrap().completed_resets
        .contains(&(id.to_string(), crate::util::node_key(source)))
}

pub fn mark_reset_completed(state: &AppState, id: &str, source: &str) {
    state.migrations.lock().unwrap().completed_resets
        .insert((id.to_string(), crate::util::node_key(source)));
    persist(state);
}

pub fn forget(state: &AppState, id: &str) {
    let mut runs = state.migrations.lock().unwrap();
    if runs.current.as_ref().is_some_and(|current| current.id == id) {
        runs.current = None;
    }
    runs.completed_resets.retain(|(migration_id, _)| migration_id != id);
    drop(runs);

    let prefix = format!("migration-reset:{}:", id);
    state.repair_locks.lock().unwrap().retain(|key, _| !key.starts_with(&prefix));

    persist(state);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn docs_of(sizes: &[usize]) -> Vec<MigrateDoc> {
        sizes.iter().enumerate().map(|(i, n)| MigrateDoc {
            key: format!("k{}", i),
            value: serde_json::json!({"pad": "x".repeat(*n)}),
        }).collect()
    }

    /// `batch_size` bounds the count only, so a handover of documents near the write limit built a
    /// request the destination refused and aborted the migration. One over the budget goes alone.
    #[test]
    fn a_handover_batch_is_bounded_by_bytes_as_well_as_by_count() {
        // Short of an exact third: `docs_of` wraps the padding in a key and a field name, and the
        // budget counts those too.
        let third = MIGRATE_BATCH_BYTES / 3 - 1024;
        let groups = size_bounded_docs(docs_of(&[third, third, third, third]));
        assert_eq!(groups.iter().map(|g| g.len()).collect::<Vec<_>>(), vec![3, 1]);

        let solo = size_bounded_docs(docs_of(&[16, MIGRATE_BATCH_BYTES + 1, 16]));
        assert_eq!(solo.iter().map(|g| g.len()).collect::<Vec<_>>(), vec![1, 1, 1]);
        assert_eq!(solo[1][0].key, "k1", "the oversized document is sent, not skipped");

        assert!(size_bounded_docs(Vec::new()).is_empty());
    }

    #[test]
    fn movement_batches_are_bounded_and_existing_configs_get_defaults() {
        let default: DataMovementConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(default.batch_size, 64);
        assert_eq!(default.batch_delay_ms, 5);
        assert!(default.validate().is_ok());

        let zero: DataMovementConfig = serde_json::from_str(r#"{"batch_size":0}"#).unwrap();
        assert!(zero.validate().is_err());

        let oversized: DataMovementConfig =
            serde_json::from_str(r#"{"batch_size":1025}"#).unwrap();
        assert!(oversized.validate().is_err());
    }

    #[test]
    fn a_completed_handover_round_trips_through_disk() {
        let dir = crate::test_support::temp_root();
        let root = dir.to_string_lossy().to_string();

        assert!(MigrationRuns::restored(&root).current.is_none(),
            "no file means no handover to finish, not an error");

        let target = HashRing { vnodes: 128, shards: Vec::new() };
        MigrationMeta {
            completed: Some(CompletedHandover {
                id: "m1".into(),
                phase: MigrationPhase::Finalizing,
                target: target.clone(),
                handed_over: HashSet::from([
                    ("t".to_string(), "k1".to_string()),
                    ("t".to_string(), "k2".to_string()),
                ]),
            }),
            completed_resets: HashSet::from([("m1".to_string(), "127.0.0.1:1".to_string())]),
        }.save(&root).unwrap();

        let runs = MigrationRuns::restored(&root);
        let current = runs.current.expect("a completed handover must come back");
        assert_eq!(current.id, "m1");
        assert_eq!(current.phase, MigrationPhase::Finalizing);
        assert_eq!(current.target, target,
            "cleanup compares this against the live ring before deleting anything");
        assert!(current.done, "only a finished handover is ever written, so it is done by definition");
        assert_eq!(current.handed_over.len(), 2);
        assert!(current.handed_over.contains(&("t".to_string(), "k2".to_string())));
        assert!(runs.completed_resets.contains(&("m1".to_string(), "127.0.0.1:1".to_string())));
        assert!(runs.coordinating.is_none(), "a coordinator loop is per process, never restored");
    }

    #[test]
    fn legacy_batches_default_to_the_bulk_copy_phase() {
        let batch: MigrateBatch = serde_json::from_value(serde_json::json!({
            "migration_id": "m1", "collection": "t", "docs": [],
        })).unwrap();
        assert_eq!(batch.phase, MigrationPhase::Copy);

        let migration: Migration = serde_json::from_value(serde_json::json!({
            "id": "m1", "started_by": "old-node",
            "target": {"vnodes": 128, "shards": []},
        })).unwrap();
        assert_eq!(migration.phase, MigrationPhase::Copy);
    }
}
