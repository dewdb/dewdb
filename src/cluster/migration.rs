//! Moving keys to their new owners while the cluster keeps serving.
//!
//! Every shard drives its own outgoing keys. There is no work queue to coordinate and no single
//! node whose failure strands the move: a shard that adopts a view naming it as a source starts
//! pushing, and a shard that restarts mid-move starts again from the top. Pushes are idempotent
//! (a repeated key is the same value written twice), so restarting costs time, never correctness.
//!
//! Writes to a key that is moving are refused for the length of the copy. That is the honest cost
//! of this design: reads stay available throughout and keys outside the moving set are untouched,
//! but the moving fraction is briefly read-only. Removing that is commit 43's job, and needs the
//! source to ship a delta after the freeze rather than freeze for the whole copy.

use crate::cluster::metadata::Migration;
use crate::ring::HashRing;
use crate::ring::hash_key;
use crate::state::AppState;
use crate::util::same_endpoint;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, warn};

/// Keys per request. Large enough that a wide migration is not one round trip per key, small
/// enough that a batch stays well inside the body limits the replication path already lives with.
const PUSH_BATCH: usize = 128;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MigrateBatch {
    pub migration_id: String,
    pub collection: String,
    pub docs: Vec<MigrateDoc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MigrateDoc {
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationProgress {
    pub id: String,
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
    pub handed_over: Vec<(String, String)>,
}

#[derive(Default)]
pub struct MigrationRuns {
    pub current: Option<MigrationProgress>,
    /// The handover coordinator is restartable, but only one polling loop should run per process.
    pub coordinating: Option<String>,
}

/// Starts the copy for `migration` unless this node is already running it. Called on every view
/// adoption, so it must be cheap and idempotent for the common case of no change.
pub fn ensure_running(state: &AppState, migration: &Migration) {
    {
        let runs = state.migrations.lock().unwrap();
        if runs.current.as_ref().is_some_and(|p| p.id == migration.id) {
            return;
        }
    }
    if state.db.is_none() || !state.is_leader() {
        // Replicas receive the moved keys through their own leader's replication, not from here.
        return;
    }

    let outgoing = match plan_outgoing(state, migration) {
        Ok(work) => work,
        Err(e) => {
            warn!(target: "migration", id = %migration.id, error = %e, "Could not plan the move");
            return;
        },
    };

    let total: usize = outgoing.values().map(|v| v.len()).sum();
    {
        let mut runs = state.migrations.lock().unwrap();
        runs.current = Some(MigrationProgress {
            id: migration.id.clone(),
            target: migration.target.clone(),
            pushed: 0,
            total,
            done: total == 0,
            error: None,
            handed_over: Vec::new(),
        });
    }

    if total == 0 {
        info!(target: "migration", id = %migration.id, "Nothing to hand over from this node");
        return;
    }

    info!(target: "migration", id = %migration.id, keys = total, "Handing over keys");
    let state = state.clone();
    let plan = migration.clone();
    tokio::spawn(async move { push_until_done(state, plan).await });
}

/// Retries for as long as the plan is in the view. A destination that is briefly down, or has not
/// yet adopted the plan and so refuses the batch, is the normal case rather than a failure -- and
/// giving up would leave ownership frozen with no way forward but an abort.
async fn push_until_done(state: AppState, migration: Migration) {
    let id = migration.id.clone();
    let mut attempt: u32 = 0;

    loop {
        match state.migration() {
            Some(m) if m.id == id => {},
            // Completed by someone else, abandoned, or replaced. Nothing left to push.
            _ => return,
        }

        // Re-planned each attempt: writes may have landed, and a retry must not ship a stale list.
        let outgoing = match plan_outgoing(&state, &migration) {
            Ok(work) => work,
            Err(e) => {
                record_error(&state, &id, e);
                backoff(attempt).await;
                attempt += 1;
                continue;
            },
        };

        let total: usize = outgoing.values().map(|v| v.len()).sum();
        {
            let mut runs = state.migrations.lock().unwrap();
            match runs.current.as_mut().filter(|p| p.id == id) {
                Some(p) => { p.total = total; p.pushed = 0; },
                None => return,
            }
        }

        match push_all(&state, &id, outgoing).await {
            Ok(()) => {
                let mut runs = state.migrations.lock().unwrap();
                if let Some(p) = runs.current.as_mut().filter(|p| p.id == id) {
                    p.done = true;
                    p.error = None;
                    info!(target: "migration", id = %id, pushed = p.pushed, "Handover complete");
                }
                return;
            },
            Err(e) => {
                warn!(target: "migration", id = %id, attempt, error = %e, "Handover attempt failed");
                record_error(&state, &id, e);
                backoff(attempt).await;
                attempt += 1;
            },
        }
    }
}

fn record_error(state: &AppState, id: &str, error: String) {
    let mut runs = state.migrations.lock().unwrap();
    if let Some(p) = runs.current.as_mut().filter(|p| p.id == id) {
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

    // Our group, not our process: after a failover the answering node is a replica url.
    let mine = view.ring.as_ref().unwrap().shards.iter()
        .find(|s| same_endpoint(&s.node_url, &own)
            || s.replica_urls.iter().any(|r| same_endpoint(r, &own)))
        .ok_or("this node is not in the ring")?
        .node_url.clone();

    let mut out: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for collection in db.list_collections().map_err(|e| e.to_string())? {
        let col = db.get_collection(&collection).map_err(|e| e.to_string())?;
        for key in col.range_from(None, None, None) {
            let hash = hash_key(&collection, &key);
            let now = ring.owner(hash).map(|s| s.node_url.as_str()).unwrap_or_default();
            if !same_endpoint(now, &mine) {
                continue;
            }
            let next = match target.owner(hash) {
                Some(s) if !same_endpoint(&s.node_url, &mine) => s.node_url.clone(),
                _ => continue,
            };
            out.entry(next).or_default().push((collection.clone(), key));
        }
    }
    Ok(out)
}

async fn push_all(
    state: &AppState,
    id: &str,
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
            for chunk in keys.chunks(PUSH_BATCH) {
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
                let batch = MigrateBatch {
                    migration_id: id.to_string(),
                    collection: collection.clone(),
                    docs,
                };
                let url = format!("{}/internal/migrate", destination);
                let response = state.client.post(&url).json(&batch).send().await
                    .map_err(|e| format!("{} unreachable: {}", destination, e))?;
                if !response.status().is_success() {
                    return Err(format!("{} refused the batch: {}", destination, response.status()));
                }

                let mut runs = state.migrations.lock().unwrap();
                if let Some(p) = runs.current.as_mut().filter(|p| p.id == id) {
                    p.pushed += sent;
                    for key in chunk {
                        let entry = (collection.clone(), key.clone());
                        if !p.handed_over.contains(&entry) {
                            p.handed_over.push(entry);
                        }
                    }
                } else {
                    // The plan changed under us -- aborted, or superseded by a newer one.
                    return Err("migration was replaced while copying".to_string());
                }
            }
        }
    }
    Ok(())
}

/// Keys this node handed over, but only once the ring it was handing them over *for* is the ring
/// actually in force. An abandoned plan leaves the same record behind, and acting on it would
/// delete keys this node still owns.
pub fn handed_over_after_flip(state: &AppState, id: &str) -> Vec<(String, String)> {
    let live = state.cluster_view().ring;
    let runs = state.migrations.lock().unwrap();
    runs.current.as_ref()
        .filter(|p| p.id == id && p.done)
        .filter(|p| live.as_ref() == Some(&p.target))
        .map(|p| p.handed_over.clone())
        .unwrap_or_default()
}

pub fn progress(state: &AppState) -> Option<MigrationProgress> {
    state.migrations.lock().unwrap().current.clone()
}

pub fn forget(state: &AppState) {
    state.migrations.lock().unwrap().current = None;
}
