//! /internal/* endpoints that cluster nodes call on each other.

use crate::cluster::metadata::{Adoption, ClusterMetadata};
use crate::cluster::metadata::MigrationPhase;
use crate::cluster::migration::{MigrateBatch, MigrateReset};
use crate::consensus::{
    decide_vote, demote, heartbeat_poll_task, local_log_tails, LogTail, ReplicationMeta,
    VoteRequest, VoteResponse,
};
use crate::model::err_json;
use crate::replication::snapshot::{replica_sync_from_primary, snapshot_body, SNAPSHOT_CONTENT_TYPE};
use crate::replication::{DropRequest, ReplicateRequest, ResyncRequest};
use crate::state::AppState;
use crate::storage::{FrameHeader, LogEntry, ReplicaApply, HEADER_LEN};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::sync::atomic::Ordering;
use tracing::{info, warn};

pub async fn internal_drop_handler(
    State(state): State<AppState>,
    Json(req): Json<DropRequest>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() || state.is_leader() {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({
            "status": "not_a_replica",
            "term": state.current_term(),
        }))).into_response();
    }

    let our_term = state.current_term();
    if req.term < our_term {
        return (StatusCode::CONFLICT, Json(serde_json::json!({
            "status": "stale_term",
            "term": our_term,
        }))).into_response();
    }

    let db = state.db.as_ref().unwrap().clone();
    let name = req.collection.clone();
    match tokio::task::spawn_blocking(move || db.drop_collection(&name)).await {
        Ok(Ok(existed)) => {
            info!(target: "replica", "Dropped collection '{}' on primary's instruction", req.collection);
            (StatusCode::OK, Json(serde_json::json!({"status": "dropped", "existed": existed}))).into_response()
        },
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn replicate_handler(
    State(state): State<AppState>,
    Json(req): Json<ReplicateRequest>,
) -> impl axum::response::IntoResponse {
    // A higher term deposes us, and that has to happen before the gate below: refusing there is
    // what left a deposed leader accepting writes until something else happened to notice.
    if state.is_leader() && req.term > state.current_term() {
        demote(&state, req.term).await;
    }

    if !state.is_shard() || state.is_leader() {
        let our_term = state.current_term();
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({
            "status": "not_a_replica",
            "term": our_term,
        }))).into_response();
    }

    // Mark before gating so acknowledged in-flight replication drains before snapshot installation.
    if state.resyncing.lock().unwrap().contains(&req.collection) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Snapshot resync in progress").into_response();
    }
    let install_lock = state.snapshot_install_lock(&req.collection);
    let _install_guard = install_lock.lock().await;
    if state.resyncing.lock().unwrap().contains(&req.collection) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Snapshot resync in progress").into_response();
    }

    let our_term = state.current_term();
    if req.term < our_term {
        return (StatusCode::CONFLICT, Json(serde_json::json!({
            "status": "stale_term",
            "term": our_term,
        }))).into_response();
    }

    if req.term > our_term {
        if let Some(ref repl) = state.replication {
            let new_term = {
                let mut r = repl.write().unwrap();
                if req.term > r.term {
                    r.term = req.term;
                    r.voted_for = None;
                    Some(r.term)
                } else {
                    None
                }
            };
            if let Some(t) = new_term {
                match (ReplicationMeta { term: t, is_leader: false, voted_for: None }).save(&state.config.data_dir) {
                    Ok(()) => info!(target: "replicate", "Adopted higher term {} from primary", t),
                    // A lost adoption rewinds to the older (term, vote) pair, under which nothing was granted.
                    Err(e) => warn!(target: "replicate", error = %e,
                        "Adopted term {} in memory but could not persist it", t),
                }
            }
        }
    }

    // Only after the term gate: a deposed leader's watermark publishes staged entries that the
    // current leader may still revoke, and commit-gated visibility is what makes them revocable.
    if let Some(idx) = req.commit_index {
        state.note_leader_committed(&req.collection, idx);
        if let Some(ref repl) = state.replication {
            let mut r = repl.write().unwrap();
            r.last_known_primary_position = Some(idx);
        }
    }

    let db = match state.db.as_ref() {
        Some(db) => db.clone(),
        None => return (StatusCode::INTERNAL_SERVER_ERROR, "No database on this node").into_response(),
    };

    let col = match db.get_collection(&req.collection) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    // Header is authoritative; a mismatched request field means a confused sender, not a link to apply.
    if let Some(header) = FrameHeader::parse(&req.wal_frame) {
        if header.lsn != req.lsn {
            return (StatusCode::BAD_REQUEST, "Frame lsn does not match request lsn").into_response();
        }
        if header.prev_lsn != req.prev_lsn {
            return (StatusCode::BAD_REQUEST, "Frame prev_lsn does not match request prev_lsn").into_response();
        }
    }

    let mut batch = Vec::with_capacity(1 + req.frames.len());
    batch.push(req.wal_frame);
    batch.extend(req.frames);

    let col_clone = col.clone();
    // Appends run to the first refusal, then one fsync covers the whole accepted run. Per-frame
    // syncing here is what made catch-up cost a disk flush per entry.
    let appended = tokio::task::spawn_blocking(move || {
        let mut staged: Vec<(u64, String, u64, u64, Option<Vec<u8>>)> = Vec::new();
        let mut highest_applied = 0u64;
        let mut refusal = None;

        for frame in &batch {
            match col_clone.append_raw_frame(frame) {
                Ok(ReplicaApply::Applied { wal_id, offset, lsn }) => {
                    highest_applied = highest_applied.max(lsn);
                    let payload = if frame.len() > HEADER_LEN { &frame[HEADER_LEN..] } else { &[][..] };
                    match serde_json::from_slice::<LogEntry>(payload) {
                        Ok(LogEntry::Put { key, .. }) => {
                            staged.push((lsn, key, wal_id, offset, Some(payload.to_vec())))
                        },
                        Ok(LogEntry::Del { key, .. }) => staged.push((lsn, key, wal_id, offset, None)),
                        Err(_) => {},
                    }
                },
                // Already held: keep going, later entries in the batch may still be new.
                Ok(ReplicaApply::Duplicate { .. }) => continue,
                Ok(other) => {
                    refusal = Some(Ok(other));
                    break;
                },
                Err(e) => {
                    refusal = Some(Err(e));
                    break;
                },
            }
        }
        (highest_applied, staged, refusal)
    }).await;

    let (highest, staged, refusal) = match appended {
        Ok(v) => v,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    // Keyed on what was appended, not on what parsed: an unparseable payload is still on disk
    // and still needs the sync before we report it durable.
    if highest > 0 {
        match col.enqueue_commit().await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
        for (lsn, key, wal_id, offset, payload) in staged {
            let entry = payload.map(|p| col.build_entry(wal_id, offset, &p));
            col.stage(lsn, key, wal_id, offset, entry);
        }
        // The leader's watermark trails this frame by a message; visibility waits for the next one.
        col.apply_committed(state.committed_hint(&req.collection));
    }

    if let Some(ref repl) = state.replication {
        let mut r = repl.write().unwrap();
        r.last_replication = Some(std::time::Instant::now());
        r.was_receiving_replication = true;
    }

    match refusal {
        // Nothing new in the whole batch: the old single-frame reply, which callers still parse.
        None if highest == 0 => (StatusCode::OK, Json(serde_json::json!({
            "status": "duplicate",
            "last_lsn": col.last_appended_lsn(),
        }))).into_response(),
        None => (StatusCode::OK, Json(serde_json::json!({"status": "applied", "lsn": highest}))).into_response(),
        Some(Ok(ReplicaApply::Gap { last_lsn, last_term })) => {
            warn!(target: "replicate", "Gap detected: got prev_lsn {} but replica is at lsn {}", req.prev_lsn, last_lsn);
            (StatusCode::CONFLICT, Json(serde_json::json!({
                "status": "gap",
                "last_lsn": last_lsn,
                "last_term": last_term,
            }))).into_response()
        },
        Some(Ok(ReplicaApply::Divergent { last_lsn, last_term })) => {
            warn!(target: "replicate", collection = %req.collection, lsn = req.lsn, term = req.term,
                last_lsn, last_term,
                "Log divergence: our tail came from a superseded leader, awaiting snapshot");
            (StatusCode::CONFLICT, Json(serde_json::json!({
                "status": "divergent",
                "last_lsn": last_lsn,
                "last_term": last_term,
            }))).into_response()
        },
        Some(Ok(_)) => (StatusCode::OK, Json(serde_json::json!({"status": "applied", "lsn": highest}))).into_response(),
        Some(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn resync_handler(
    State(state): State<AppState>,
    Json(req): Json<ResyncRequest>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() || state.is_leader() {
        return (StatusCode::FORBIDDEN, "Only replica nodes accept resync").into_response();
    }

    let primary_addr = match state.replication.as_ref().and_then(|r| r.read().unwrap().primary_addr.clone()) {
        Some(a) => a,
        None => return (StatusCode::BAD_REQUEST, "No primary configured").into_response(),
    };

    let col = req.collection.clone();

    {
        let mut set = state.resyncing.lock().unwrap();
        if set.contains(&col) {
            return (StatusCode::OK, "resync already in progress").into_response();
        }
        set.insert(col.clone());
    }

    let db = state.db.as_ref().unwrap().clone();
    let client = state.client.clone();
    let repl = state.replication.clone();
    let resyncing = state.resyncing.clone();
    let install_lock = state.snapshot_install_lock(&col);

    tokio::spawn(async move {
        let _install_guard = install_lock.lock().await;
        if let Err(e) = replica_sync_from_primary(&client, &primary_addr, &db, &col).await {
            warn!(target: "resync", "Failed for '{}': {}", col, e);
        } else {
            if let Some(r) = repl {
                let mut g = r.write().unwrap();
                g.last_replication = Some(std::time::Instant::now());
                g.was_receiving_replication = true;
            }
            if let Err(e) = db.recompute_durable_lsn() {
                warn!(target: "resync", "could not recompute durable LSN for '{}': {}", col, e);
            }
        }
        resyncing.lock().unwrap().remove(&col);
    });

    (StatusCode::OK, "resync started").into_response()
}

pub async fn cluster_view_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    (StatusCode::OK, Json(state.cluster_view())).into_response()
}

/// Offers a view to this node. Idempotent: re-offering what we already hold is `200 stale`, not an
/// error, since propagation retries and the sender cannot know what we adopted from someone else.
pub async fn cluster_update_handler(
    State(state): State<AppState>,
    Json(incoming): Json<ClusterMetadata>,
) -> impl axum::response::IntoResponse {
    let offered = incoming.version;
    match state.adopt_cluster(incoming) {
        Adoption::Adopted { from, to } => {
            info!(target: "cluster", "Adopted cluster view v{} (was v{})", to, from);
            (StatusCode::OK, Json(serde_json::json!({
                "status": "adopted", "version": to, "previous": from,
            }))).into_response()
        },
        Adoption::Stale { current } => (StatusCode::OK, Json(serde_json::json!({
            "status": "stale", "version": current, "offered": offered,
        }))).into_response(),
        Adoption::Rejected(why) => {
            warn!(target: "cluster", "Refused cluster view v{}: {}", offered, why);
            (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({
                "status": "rejected", "reason": why, "version": state.cluster_version(),
            }))).into_response()
        },
    }
}

/// Receives keys handed over by their current owner. Deliberately outside the ownership check: the
/// point of the batch is that this node does not own these keys yet.
pub async fn migrate_handler(
    State(state): State<AppState>,
    Json(batch): Json<MigrateBatch>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() || !state.is_leader() {
        return err_json(StatusCode::CONFLICT,
            "handover batches go to the destination group's leader".to_string());
    }
    match state.migration() {
        Some(m) if m.id == batch.migration_id && m.phase == batch.phase => {},
        // Refused rather than absorbed: a batch from a plan we do not hold would write keys that
        // nothing in our view says are ours, and nothing would ever clean them up.
        _ => return err_json(StatusCode::CONFLICT, format!(
            "no migration {} is in progress here", batch.migration_id)),
    }

    let wc = crate::replication::parse_write_concern(None);
    let wtimeout = std::time::Duration::from_millis(crate::replication::DEFAULT_WTIMEOUT_MS);
    let written = batch.docs.len();
    let items = batch.docs.into_iter().map(|doc| (doc.key, doc.value)).collect();
    if let Err(resp) = crate::api::write::local_write_batch(
        &state, &batch.collection, items, wc, wtimeout,
    ).await {
        return resp;
    }

    (StatusCode::OK, Json(serde_json::json!({"status": "received", "written": written}))).into_response()
}

pub async fn migrate_reset_handler(
    State(state): State<AppState>,
    Json(req): Json<MigrateReset>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() || !state.is_leader() {
        return err_json(StatusCode::CONFLICT,
            "handover resets go to the destination group's leader".to_string());
    }
    let migration = match state.migration() {
        Some(m) if m.id == req.migration_id
            && m.phase == req.phase
            && m.phase == MigrationPhase::Finalizing => m,
        _ => return err_json(StatusCode::CONFLICT, format!(
            "migration {} is not finalizing here", req.migration_id)),
    };
    let reset_lock = state.migration_reset_lock(&req.migration_id, &req.source);
    let _reset_guard = reset_lock.lock().await;
    if crate::cluster::migration::reset_completed(&state, &req.migration_id, &req.source) {
        return (StatusCode::OK, Json(serde_json::json!({
            "status": "reset", "removed": 0, "repeated": true,
        }))).into_response();
    }

    let view = state.cluster_view();
    let current = match view.ring {
        Some(ring) => ring.build(),
        None => return err_json(StatusCode::CONFLICT, "no source ring is active".to_string()),
    };
    let target = migration.target.build();
    let own = state.own_url();
    let destination = match migration.target.shards.iter().find(|shard| {
        crate::util::same_endpoint(&shard.node_url, &own)
            || shard.replica_urls.iter().any(|url| crate::util::same_endpoint(url, &own))
    }) {
        Some(shard) => shard.node_url.clone(),
        None => return err_json(StatusCode::CONFLICT,
            "this node is not a destination in the target ring".to_string()),
    };
    let valid_transfer = crate::ring::keyspace_movement(&current, &target).transfers
        .iter().any(|transfer| {
            crate::util::same_endpoint(&transfer.from, &req.source)
                && crate::util::same_endpoint(&transfer.to, &destination)
        });
    if !valid_transfer {
        return err_json(StatusCode::CONFLICT,
            "the requested source has no keyspace moving to this destination".to_string());
    }

    let db = match state.db.as_ref() {
        Some(db) => db.clone(),
        None => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "no database".to_string()),
    };
    let mut stale = Vec::new();
    let collections = match db.list_collections() {
        Ok(names) => names,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    for collection in collections {
        let col = match db.get_collection(&collection) {
            Ok(col) => col,
            Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        for key in col.range_from(None, None, None) {
            let hash = crate::ring::hash_key(&collection, &key);
            let from_source = current.owner(hash)
                .is_some_and(|owner| crate::util::same_endpoint(&owner.node_url, &req.source));
            let to_destination = target.owner(hash)
                .is_some_and(|owner| crate::util::same_endpoint(&owner.node_url, &destination));
            if from_source && to_destination {
                stale.push((collection.clone(), key));
            }
        }
    }

    let wc = crate::replication::parse_write_concern(None);
    let wtimeout = std::time::Duration::from_millis(crate::replication::DEFAULT_WTIMEOUT_MS);
    let mut removed = 0usize;
    for (collection, key) in stale {
        match crate::api::write::local_write(&state, &collection, key, None, wc, wtimeout).await {
            Ok(_) => removed += 1,
            Err(resp) => return resp,
        }
    }
    crate::cluster::migration::mark_reset_completed(&state, &req.migration_id, &req.source);

    (StatusCode::OK, Json(serde_json::json!({"status": "reset", "removed": removed}))).into_response()
}

pub async fn migration_status_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    let plan = state.migration();
    (StatusCode::OK, Json(serde_json::json!({
        "node_id": state.config.node_id,
        "migration": plan.as_ref().map(|m| m.id.clone()),
        "progress": crate::cluster::migration::progress(&state),
    }))).into_response()
}

#[derive(Deserialize)]
pub struct CleanupRequest {
    pub migration_id: String,
}

/// Deletes the keys this node handed over, once the flip is visible here. Driven by the coordinator
/// after the ring lands, never by the source on its own: a node whose view is behind would be
/// deleting keys it still owns.
pub async fn migrate_cleanup_handler(
    State(state): State<AppState>,
    Json(req): Json<CleanupRequest>,
) -> impl axum::response::IntoResponse {
    if state.migration().is_some() {
        return err_json(StatusCode::CONFLICT,
            "the handover is still in the view here; ownership has not moved yet".to_string());
    }

    let handed_over = crate::cluster::migration::handed_over_after_flip(&state, &req.migration_id);
    let wc = crate::replication::parse_write_concern(None);
    let wtimeout = std::time::Duration::from_millis(crate::replication::DEFAULT_WTIMEOUT_MS);
    let mut removed = 0usize;

    for (collection, key) in handed_over {
        // Re-checked one key at a time against the live view. Anything we do own now is not ours
        // to delete, whatever the handover recorded.
        if state.ownership(&collection, &key) == Some(crate::cluster::ownership::Ownership::Ours) {
            continue;
        }
        // Through the write path, not a bare append: a tombstone has to be staged, committed and
        // applied to disappear from the index, and it has to reach this group's replicas too.
        if crate::api::write::local_write(&state, &collection, key, None, wc, wtimeout).await.is_ok() {
            removed += 1;
        }
    }
    crate::cluster::migration::forget(&state, &req.migration_id);

    info!(target: "migration", id = %req.migration_id, removed, "Cleaned up handed-over keys");
    (StatusCode::OK, Json(serde_json::json!({"status": "cleaned", "removed": removed}))).into_response()
}

/// Whether this node holds any data. Asked before a ring change reassigns ownership, so it is
/// computed on demand rather than folded into the heartbeat every follower polls twice a second.
pub async fn data_summary_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    // An unreadable data directory answers "populated": the caller is deciding whether it is safe
    // to move ownership, and not knowing is not the same as knowing there is nothing to lose.
    let has_data = state.db.as_ref().is_some_and(|db| db.has_any_data().unwrap_or(true));
    (StatusCode::OK, Json(serde_json::json!({
        "node_id": state.config.node_id,
        "has_data": has_data,
    }))).into_response()
}

pub async fn heartbeat_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    let term = state.current_term();
    let role = if state.is_leader() { "primary" } else { "replica" };
    let mut load = state.metrics.node_load();
    load.inflight = load.inflight.saturating_sub(1);
    (StatusCode::OK, Json(serde_json::json!({
        "term": term,
        "role": role,
        "node_id": state.config.node_id,
        // Lets a peer notice a topology change without fetching the whole view every poll.
        "cluster_version": state.cluster_version(),
        "durable_lsn": state.db.as_ref().map_or(0, |db| db.durable_lsn.load(Ordering::SeqCst)),
        "commit_index": state.max_committed_lsn(),
        "load": {
            "inflight": load.inflight,
            "latency_ewma_us": load.latency_ewma_us,
        },
        // Followers need this to publish the last entry of an otherwise idle cluster.
        "committed": state.all_committed().into_iter()
            .map(|(k, v)| (k, serde_json::Value::from(v)))
            .collect::<serde_json::Map<String, serde_json::Value>>(),
    }))).into_response()
}

pub async fn vote_handler(
    State(state): State<AppState>,
    Json(req): Json<VoteRequest>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() {
        return (StatusCode::FORBIDDEN, "Not a voting node").into_response();
    }

    let repl = match state.replication.as_ref() {
        Some(r) => r,
        None => return (StatusCode::FORBIDDEN, "No replication state").into_response(),
    };

    let my_lsn = state.db.as_ref().map_or(0, |db| db.durable_lsn.load(Ordering::SeqCst));
    let my_log_term = state.db.as_ref().map_or(0, |db| db.last_log_term.load(Ordering::SeqCst));
    // Collected before the replication lock: local_log_tails reaches the collections lock.
    let my_logs = local_log_tails(&state);
    let my_summary = LogTail { last_term: my_log_term, last_lsn: my_lsn };

    let (granted, resp_term, restart_poll, persist) = {
        let mut g = repl.write().unwrap();
        let was_leader = g.is_leader;
        let old_term = g.term;

        let d = decide_vote(g.term, &g.voted_for, &my_logs, my_summary, &req);

        let mut restart = false;
        g.term = d.term;
        g.voted_for = d.voted_for.clone();

        if d.term > old_term && was_leader {
            g.is_leader = false;
            // Standing down by granting a vote is still standing down; same rule as apply_demotion.
            g.progress.reset();
            restart = !g.heartbeat_running;
            g.heartbeat_running = true;
        }

        if d.granted {
            g.last_heartbeat = Some(std::time::Instant::now());
        }

        let persist = ReplicationMeta { term: g.term, is_leader: g.is_leader, voted_for: g.voted_for.clone() };
        (d.granted, d.term, restart, persist)
    };

    if restart_poll {
        heartbeat_poll_task(state.clone());
    }

    // Election safety: a vote promised before it is durable can be cast twice in one term after a restart.
    if let Err(e) = persist.save(&state.config.data_dir) {
        warn!(target: "vote", error = %e,
            "Could not persist term/vote; denying the vote rather than promising one we may forget");
        return (StatusCode::OK, Json(VoteResponse { term: resp_term, vote_granted: false })).into_response();
    }

    if granted {
        info!(target: "vote", "Granted vote to {} for term {}", req.candidate_id, req.term);
    }

    (StatusCode::OK, Json(VoteResponse { term: resp_term, vote_granted: granted })).into_response()
}

#[derive(Deserialize)]
pub struct SnapshotQuery {
    pub collection: String,
}

pub async fn snapshot_handler(
    State(state): State<AppState>,
    Query(params): Query<SnapshotQuery>,
) -> impl axum::response::IntoResponse {
    if !state.is_leader() {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Only primary nodes serve snapshots"}))).into_response();
    }

    let db = match state.db.as_ref() {
        Some(db) => db.clone(),
        None => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "No database".to_string()),
    };

    let col = match db.get_collection(&params.collection) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, SNAPSHOT_CONTENT_TYPE)
        .body(snapshot_body(col))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{make_frame, next_test_port, put_doc_http, temp_root, TestNode};
    use std::collections::HashMap;
    use std::fs;

    async fn heartbeat(node: &TestNode) -> serde_json::Value {
        reqwest::Client::new()
            .get(format!("{}/internal/heartbeat", node.url()))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    /// A solo primary, so one `w=1` write is a quorum of one and lands in the commit watermark.
    async fn leader_with_one_committed_write(root: &std::path::Path) -> TestNode {
        let mut leader = TestNode::new("solo", next_test_port(), root, "primary");
        // Long enough that the poll started by standing down cannot re-elect this node mid-test.
        leader.heartbeat_timeout_secs = 30;
        leader.start();

        let client = reqwest::Client::new();
        assert_eq!(put_doc_http(&client, &leader.url(), "k1", 1).await, StatusCode::CREATED);

        let hb = heartbeat(&leader).await;
        assert_eq!(hb["role"], "primary");
        assert!(hb["commit_index"].as_u64().unwrap() > 0, "the write is committed and advertised");
        assert!(hb["committed"]["t"].as_u64().unwrap() > 0);
        leader
    }

    fn assert_no_watermark(hb: &serde_json::Value) {
        assert_eq!(hb["role"], "replica");
        assert_eq!(hb["commit_index"].as_u64(), Some(0),
            "evidence gathered in a term this node no longer holds is not a commit watermark");
        assert_eq!(hb["committed"].as_object().map(|m| m.len()), Some(0),
            "a follower that still lists per-collection watermarks will publish on them");
    }

    /// One frame on the wire to `/internal/replicate`, with the request fields and the frame
    /// header supplied separately so a test can disagree between them on purpose.
    async fn replicate(
        node: &TestNode,
        term: u64,
        lsn: u64,
        prev_lsn: u64,
        commit_index: u64,
        frame: Vec<u8>,
    ) -> (StatusCode, serde_json::Value) {
        let request = ReplicateRequest {
            collection: "t".to_string(),
            term,
            lsn,
            prev_lsn,
            commit_index: Some(commit_index),
            wal_frame: frame,
            frames: Vec::new(),
        };
        let response = reqwest::Client::new()
            .post(format!("{}/internal/replicate", node.url()))
            .json(&request)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.json::<serde_json::Value>().await
            .unwrap_or(serde_json::Value::Null);
        (status, body)
    }

    #[tokio::test]
    async fn a_stale_leaders_commit_index_cannot_publish_a_staged_entry() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.primary_addr = Some("http://127.0.0.1:1".to_string());
        replica.start();

        let (status, _) = replicate(&replica, 5, 1, 0, 0, make_frame(5, 1, 0, 0, "k1", 1)).await;
        assert_eq!(status, StatusCode::OK);

        let col = replica.state.as_ref().unwrap()
            .db.as_ref().unwrap()
            .get_collection("t").unwrap();
        assert_eq!(col.pending_len(), 1, "the frame is durable but nothing has committed it");
        assert!(col.get("k1").unwrap().is_none(), "a staged entry is invisible");

        // A leader deposed back at term 3 claims lsn 1 committed. It may be wrong: the term-5
        // leader can still revoke that entry, which is the whole point of staging it.
        let (status, body) = replicate(&replica, 3, 2, 1, 1, make_frame(3, 2, 1, 5, "k2", 2)).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["status"], "stale_term");

        assert!(col.get("k1").unwrap().is_none(),
            "a watermark from a superseded term published an entry the current leader may revoke");
        assert_eq!(col.pending_len(), 1, "the entry is still staged, not lost");

        replica.kill();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_leader_steps_down_for_a_higher_term_but_not_for_its_own() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        // Long enough that the poll started by stepping down cannot re-elect this node mid-test.
        leader.heartbeat_timeout_secs = 30;
        leader.start();
        assert!(leader.is_leader(), "a solo primary leads from boot");

        let (status, body) = replicate(&leader, 0, 1, 0, 0, make_frame(0, 1, 0, 0, "k1", 1)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["status"], "not_a_replica");
        assert!(leader.is_leader(), "a term it already holds is not a demotion");

        let (status, body) = replicate(&leader, 7, 1, 0, 0, make_frame(7, 1, 0, 0, "k1", 1)).await;
        assert!(!leader.is_leader(),
            "a higher term deposes a leader; refusing it leaves two leaders serving writes");
        assert_eq!(leader.term(), 7);
        assert_eq!(status, StatusCode::OK, "having stepped down, it serves the frame as a follower");
        assert_eq!(body["status"], "applied");

        let meta = ReplicationMeta::load(&leader.data_dir.to_string_lossy()).unwrap().unwrap();
        assert_eq!(meta.term, 7);
        assert!(!meta.is_leader, "the step-down has to survive a restart, or it comes back leading");

        leader.kill();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_deposed_leader_stops_advertising_the_watermark_it_committed() {
        let root = temp_root();
        let mut leader = leader_with_one_committed_write(&root).await;

        // The frame itself is refused as divergent -- this node's tail came from the old term.
        // Standing down happens first and is what the assertions below are about.
        let _ = replicate(&leader, 9, 1, 0, 0, make_frame(9, 1, 0, 0, "k2", 2)).await;
        assert!(!leader.is_leader());

        assert_no_watermark(&heartbeat(&leader).await);

        leader.kill();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn granting_a_vote_at_a_higher_term_clears_the_watermark_too() {
        let root = temp_root();
        let mut leader = leader_with_one_committed_write(&root).await;

        // Stands down without ever going through demote(), which is why the vote path needs the
        // same reset rather than relying on apply_demotion to have done it.
        let vote = VoteRequest {
            term: 9,
            candidate_id: "challenger".to_string(),
            last_lsn: 100,
            last_term: 9,
            logs: HashMap::new(),
        };
        let response = reqwest::Client::new()
            .post(format!("{}/internal/vote", leader.url()))
            .json(&vote)
            .send()
            .await
            .unwrap()
            .json::<VoteResponse>()
            .await
            .unwrap();
        assert!(response.vote_granted, "a fresher candidate at a higher term wins the vote");
        assert!(!leader.is_leader());

        assert_no_watermark(&heartbeat(&leader).await);

        leader.kill();
        let _ = fs::remove_dir_all(&root);
    }
}
