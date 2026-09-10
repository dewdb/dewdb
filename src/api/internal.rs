//! /internal/* endpoints that cluster nodes call on each other.

use crate::cluster::metadata::{Adoption, ClusterMetadata};
use crate::cluster::metadata::MigrationPhase;
use crate::cluster::migration::{MigrateBatch, MigrateReset};
use crate::consensus::config::CONFIG_LOG;
use crate::consensus::lease;
use crate::consensus::election::run_election;
use crate::consensus::{
    decide_pre_vote, decide_vote, demote, heartbeat_poll_task, log_summary,
    ReplicationMeta, VoteRequest, VoteResponse,
};
use crate::model::err_json;
use crate::replication::snapshot::{replica_sync_from_primary, snapshot_body, SNAPSHOT_CONTENT_TYPE};
use crate::replication::{ReplicateRequest, ResyncRequest};
use crate::state::AppState;
use crate::storage::{FrameHeader, ReplicaApply};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{info, warn};

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
        let mut highest_applied = 0u64;
        let mut matched = None;
        let mut previous = None;
        let mut refusal = None;

        for frame in &batch {
            let header = match FrameHeader::parse(frame) {
                Some(h) if h.lsn > h.prev_lsn
                    && previous.is_none_or(|p| p == (h.prev_term, h.prev_lsn)) => h,
                _ => {
                    refusal = Some(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData, "Invalid replication frame chain")));
                    break;
                },
            };
            match col_clone.append_raw_frame(frame) {
                Ok(ReplicaApply::Applied { lsn, .. }) => highest_applied = highest_applied.max(lsn),
                // A duplicate proves its own position, not the follower's remaining tail.
                Ok(ReplicaApply::Duplicate { .. }) => {},
                Ok(other) => {
                    refusal = Some(Ok(other));
                    break;
                },
                Err(e) => {
                    refusal = Some(Err(e));
                    break;
                },
            }
            matched = Some(header.lsn);
            previous = Some((header.term, header.lsn));
        }
        (highest_applied, matched, refusal)
    }).await;

    let (highest, matched, refusal) = match appended {
        Ok(v) => v,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    if highest > 0 {
        match col.enqueue_commit().await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
        // Configuration entries govern voting from append, before they commit.
        if req.collection == CONFIG_LOG {
            state.refresh_configuration();
        }
    }

    if matched.is_some() {
        if let Err(e) = state.note_leader_committed(&req.collection, req.term, req.commit_index.unwrap_or(0), matched) {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }

    if let Some(ref repl) = state.replication {
        let mut r = repl.write().unwrap();
        if matched.is_some() && r.term == req.term {
            if let Some(idx) = req.commit_index {
                r.last_known_primary_position = Some(idx);
            }
        }
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
            // `applied` is where truncation stops, so it is the highest point the leader can back
            // up to and still be replacing entries this node is free to drop.
            let applied = col.applied_lsn();
            warn!(target: "replicate", collection = %req.collection, lsn = req.lsn, term = req.term,
                last_lsn, last_term, applied,
                "Log divergence below our tail; asking the leader to resume from our watermark");
            (StatusCode::CONFLICT, Json(serde_json::json!({
                "status": "divergent",
                "last_lsn": last_lsn,
                "last_term": last_term,
                "applied": applied,
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
            // A snapshot install replaces applied.meta wholesale, so it can install, replace or
            // withdraw a configuration entry this node was deciding against.
            state.refresh_configuration();
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

/// Handover writes are the one place `w=1` is a data-loss bug rather than a latency choice: the source
/// deletes what the destination acknowledged. A group with no replicas still needs one ack.
fn handover_write_concern() -> (crate::replication::WriteConcern, std::time::Duration) {
    (
        crate::replication::WriteConcern::Majority,
        std::time::Duration::from_millis(crate::replication::DEFAULT_WTIMEOUT_MS),
    )
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

    let (wc, wtimeout) = handover_write_concern();
    let written = batch.docs.len();
    let items = batch.docs.into_iter().map(|doc| (doc.key, doc.value)).collect();
    let outcomes = match crate::api::write::local_migration_write_batch(
        &state, &batch.collection, items, wc, wtimeout,
    ).await {
        Ok(outcomes) => outcomes,
        Err(resp) => return resp,
    };
    if let Some(short) = outcomes.iter().find(|outcome| !outcome.met) {
        return err_json(StatusCode::SERVICE_UNAVAILABLE, format!(
            "handover batch reached {} of {} nodes", short.acks, short.required));
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
        col.for_each_key(None, None, None, |key| {
            let hash = crate::ring::hash_key(&collection, key);
            let from_source = current.owner(hash)
                .is_some_and(|owner| crate::util::same_endpoint(&owner.node_url, &req.source));
            let to_destination = target.owner(hash)
                .is_some_and(|owner| crate::util::same_endpoint(&owner.node_url, &destination));
            if from_source && to_destination {
                stale.push((collection.clone(), key.to_string()));
            }
            true
        });
    }

    let (wc, wtimeout) = handover_write_concern();
    let mut removed = 0usize;
    for (collection, key) in stale {
        match crate::api::write::local_migration_write(&state, &collection, key, None, wc, wtimeout).await {
            Ok(outcome) if outcome.met => removed += 1,
            // Marking the reset complete over a tombstone one node holds would let the copy it was
            // meant to clear come back with the next leader.
            Ok(outcome) => return err_json(StatusCode::SERVICE_UNAVAILABLE, format!(
                "reset tombstone reached {} of {} nodes", outcome.acks, outcome.required)),
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
/// after the ring lands, never by the source: a node whose view is behind would delete owned keys.
pub async fn migrate_cleanup_handler(
    State(state): State<AppState>,
    Json(req): Json<CleanupRequest>,
) -> impl axum::response::IntoResponse {
    // The only migration handler that had no leadership check, so cleanup could be accepted by a
    // follower that holds no record and answer `200 cleaned, removed: 0` (bugs.md H16).
    if !state.is_shard() || !state.is_leader() {
        return err_json(StatusCode::CONFLICT,
            "handover cleanup goes to the source group's leader".to_string());
    }
    if state.migration().is_some() {
        return err_json(StatusCode::CONFLICT,
            "the handover is still in the view here; ownership has not moved yet".to_string());
    }

    let handed_over = crate::cluster::migration::handed_over_after_flip(&state, &req.migration_id);
    let (wc, wtimeout) = handover_write_concern();
    let mut removed = 0usize;
    let mut short = 0usize;

    for (collection, key) in handed_over {
        // Re-checked one key at a time against the live view. Anything we do own now is not ours
        // to delete, whatever the handover recorded.
        if state.ownership(&collection, &key) == Some(crate::cluster::ownership::Ownership::Ours) {
            continue;
        }
        // Through the write path, not a bare append: a tombstone has to be staged, committed and
        // applied to disappear from the index, and it has to reach this group's replicas too.
        match crate::api::write::local_migration_write(&state, &collection, key, None, wc, wtimeout).await {
            Ok(outcome) if outcome.met => removed += 1,
            _ => short += 1,
        }
    }

    // The record survives a partial cleanup so a later call can finish it. Forgetting here leaves a
    // tombstone this group's quorum does not hold, and a replica elected without it still has the key.
    if short == 0 {
        crate::cluster::migration::forget(&state, &req.migration_id);
        info!(target: "migration", id = %req.migration_id, removed, "Cleaned up handed-over keys");
        return (StatusCode::OK, Json(serde_json::json!({
            "status": "cleaned", "removed": removed,
        }))).into_response();
    }

    warn!(target: "migration", id = %req.migration_id, removed, pending = short,
        "Cleanup left keys behind; their tombstones did not reach a quorum here");
    (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
        "status": "partial", "removed": removed, "pending": short,
    }))).into_response()
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

/// A leader's lease round rides on the probe it sends anyway: `lease_ms` is how long it asks this node
/// to refuse votes, at `term`. Absent from a peer that predates leases, which costs a round.
#[derive(Deserialize)]
pub struct HeartbeatQuery {
    pub term: Option<u64>,
    pub lease_ms: Option<u64>,
}

pub async fn heartbeat_handler(
    State(state): State<AppState>,
    Query(q): Query<HeartbeatQuery>,
) -> impl axum::response::IntoResponse {
    let term = state.current_term();

    // Answered as a duration, not a deadline: the asker dates it from before it sent this, so the
    // flight comes off the lease instead of being covered by a margin. See bugs.md M16.
    let novote_ms = match (q.term, q.lease_ms) {
        (Some(asker_term), Some(ms)) if ms > 0 =>
            state.grant_novote(asker_term, Duration::from_millis(ms)).as_millis() as u64,
        _ => 0,
    };

    let role = if state.is_leader() { "primary" } else { "replica" };
    let cluster_id = state.cluster_view_id();
    let mut load = state.metrics.node_load();
    load.inflight = load.inflight.saturating_sub(1);
    (StatusCode::OK, Json(serde_json::json!({
        "term": term,
        "role": role,
        "node_id": state.config.node_id,
        "novote_ms": novote_ms,
        // Lets a peer notice a topology change without fetching the whole view every poll. The
        // version alone cannot order two concurrent publications, so the tiebreak rides with it.
        "cluster_version": cluster_id.version,
        "cluster_updated_by": cluster_id.updated_by,
        "cluster_seeded": cluster_id.seeded,
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

/// The leader telling this node to stand for election now, without waiting out a contact timeout it
/// would never see. Accepted only from the leader this node follows, and answered "asked", not "won".
pub async fn timeout_now_handler(
    State(state): State<AppState>,
    Json(req): Json<crate::consensus::transfer::TimeoutNowRequest>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() || !state.in_quorum() {
        return (StatusCode::FORBIDDEN, "Not a voting node").into_response();
    }
    if req.term < state.current_term() {
        return err_json(StatusCode::CONFLICT, format!(
            "handover offered at term {}, which this node has already left for {}",
            req.term, state.current_term()));
    }
    if !state.honours_transfer(Some(&req.leader)) {
        return err_json(StatusCode::CONFLICT, format!(
            "{} is not the leader this node is following; not standing on its say-so", req.leader));
    }

    info!(target: "transfer", from = %req.leader, term = req.term,
        "Told to stand for election; the leader is handing over");
    let state2 = state.clone();
    let from = req.leader.clone();
    tokio::spawn(async move { run_election(&state2, 0, Some(from)).await });

    (StatusCode::ACCEPTED, Json(serde_json::json!({ "status": "standing" }))).into_response()
}

/// Raft §9.6. Answers the vote handler's question and touches nothing: no term, no recorded vote,
/// no disk. A candidate that could not win learns so without costing the group an election.
pub async fn pre_vote_handler(
    State(state): State<AppState>,
    Json(req): Json<VoteRequest>,
) -> impl axum::response::IntoResponse {
    let _history = state.election_history.read().await;
    if !state.is_shard() || !state.in_quorum() {
        return (StatusCode::FORBIDDEN, "Not a voting node").into_response();
    }

    let repl = match state.replication.as_ref() {
        Some(r) => r,
        None => return (StatusCode::FORBIDDEN, "No replication state").into_response(),
    };

    // Collected before the replication lock: local_log_tails reaches the collections lock.
    let my_logs = match crate::consensus::election::try_local_log_tails(&state) {
        Ok(logs) => logs,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let my_summary = log_summary(&state, &my_logs);

    let (term, granted) = {
        let g = repl.read().unwrap();
        let must_withhold = lease::withholds_vote(
            lease::contact_age(g.last_heartbeat, g.last_replication),
            g.novote_until,
            g.booted_at.elapsed(),
            Duration::from_secs(state.config.heartbeat_timeout_secs),
            std::time::Instant::now());
        let granted = decide_pre_vote(
            g.term, &my_logs, my_summary, g.configuration.as_ref(), &req, must_withhold);
        (g.term, granted)
    };

    (StatusCode::OK, Json(VoteResponse { term, vote_granted: granted })).into_response()
}

pub async fn vote_handler(
    State(state): State<AppState>,
    Json(req): Json<VoteRequest>,
) -> impl axum::response::IntoResponse {
    let _history = state.election_history.read().await;
    // Granting is quorum participation, not just standing, so it takes the same predicate the
    // candidate side does: a node absent from every threshold can only push a candidate past one.
    if !state.is_shard() || !state.in_quorum() {
        return (StatusCode::FORBIDDEN, "Not a voting node").into_response();
    }

    let repl = match state.replication.as_ref() {
        Some(r) => r,
        None => return (StatusCode::FORBIDDEN, "No replication state").into_response(),
    };

    // Collected before the replication lock: local_log_tails reaches the collections lock.
    let my_logs = match crate::consensus::election::try_local_log_tails(&state) {
        Ok(logs) => logs,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let my_summary = log_summary(&state, &my_logs);
    let (my_config, contact, granted_until, since_boot) = {
        let g = repl.read().unwrap();
        (g.configuration.clone(),
         lease::contact_age(g.last_heartbeat, g.last_replication),
         g.novote_until,
         g.booted_at.elapsed())
    };
    // A leader handing office over is the one case where fresh contact is not a reason to refuse:
    // the contact is *from* the node standing aside, and only that node's name gets past this.
    let must_withhold = lease::withholds_vote(contact, granted_until, since_boot,
        Duration::from_secs(state.config.heartbeat_timeout_secs), std::time::Instant::now())
        && !state.honours_transfer(req.transfer_from.as_deref());

    let (granted, resp_term, restart_poll, persist) = {
        let mut g = repl.write().unwrap();
        let was_leader = g.is_leader;
        let old_term = g.term;

        let d = decide_vote(g.term, &g.voted_for, &my_logs, my_summary, my_config.as_ref(),
            &req, must_withhold);

        let mut restart = false;
        g.term = d.term;
        g.voted_for = d.voted_for.clone();

        if d.term > old_term && was_leader {
            g.is_leader = false;
            // Standing down by granting a vote is still standing down; same rule as apply_demotion.
            g.progress.reset();
            g.leases.clear();
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

pub async fn election_histories_handler(State(state): State<AppState>) -> axum::response::Response {
    let _history = state.election_history.read().await;
    match crate::consensus::election::try_local_log_tails(&state) {
        Ok(logs) => Json(crate::consensus::recovery::ElectionHistories {
            term: state.current_term(), logs,
        }).into_response(),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn election_snapshot_handler(
    State(state): State<AppState>,
    Query(params): Query<SnapshotQuery>,
) -> axum::response::Response {
    serve_snapshot(&state, &params.collection)
}

pub async fn snapshot_handler(
    State(state): State<AppState>,
    Query(params): Query<SnapshotQuery>,
) -> impl axum::response::IntoResponse {
    if !state.is_leader() {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Only primary nodes serve snapshots"}))).into_response();
    }

    serve_snapshot(&state, &params.collection)
}

fn serve_snapshot(state: &AppState, collection: &str) -> axum::response::Response {

    let db = match state.db.as_ref() {
        Some(db) => db.clone(),
        None => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "No database".to_string()),
    };

    // Serving with `get_collection` created what it was asked for, so any name cost a directory, a wal
    // and a commit task (H18). Empty is wrong too: the asker cannot tell it from a genuine miss.
    let col = match db.lookup_collection(collection) {
        Ok(Some(c)) => c,
        Ok(None) => return err_json(StatusCode::NOT_FOUND,
            format!("no collection '{}' on this node", collection)),
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
    use crate::test_support::{
        make_drop_frame, make_frame, next_test_port, put_doc_http, temp_root, TestNode,
    };
    use std::collections::HashMap;

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

    /// IB-023: the gate that decides whether to fetch a peer's view orders by the same pair
    /// adoption does, so the whole pair has to reach the wire and read back unchanged.
    #[tokio::test]
    async fn the_heartbeat_advertises_the_whole_view_ordering_identity() {
        let root = temp_root();
        let mut node = TestNode::new("solo", next_test_port(), &root, "primary");
        node.heartbeat_timeout_secs = 30;
        node.start();

        let hb = heartbeat(&node).await;
        let ours = node.state.clone().unwrap().cluster_view_id();
        assert_eq!(hb["cluster_version"].as_u64(), Some(ours.version));
        assert_eq!(hb["cluster_updated_by"].as_str(), ours.updated_by.as_deref());
        assert_eq!(hb["cluster_seeded"].as_bool(), Some(ours.seeded));
        assert_eq!(crate::cluster::probe::parse_view_id(&hb), ours,
            "a peer must read back the identity this node holds, or the gate compares a guess");
    }

    /// The promise a leader's lease is built on, at the handler that has to keep it. Nothing here
    /// is about this node's own election: it is the round the leader is no longer paying for.
    #[tokio::test]
    async fn a_follower_with_fresh_leader_contact_refuses_a_vote_it_would_otherwise_grant() {
        let root = temp_root();
        let mut follower = TestNode::new("replica", next_test_port(), &root, "replica");
        follower.heartbeat_timeout_secs = 30;
        // A primary that does not answer, so nothing but this test moves the contact clock.
        follower.primary_addr = Some("http://127.0.0.1:1".to_string());
        follower.start();
        let state = follower.state.clone().unwrap();
        let repl = state.replication.clone().unwrap();
        let term = state.current_term();

        let body = serde_json::json!({
            "term": term + 4, "candidate_id": "challenger", "last_lsn": 100, "last_term": term + 4});
        let ask = || async {
            reqwest::Client::new()
                .post(format!("{}/internal/vote", follower.url()))
                .json(&body).send().await.unwrap()
                .json::<VoteResponse>().await.unwrap()
        };

        repl.write().unwrap().last_heartbeat = Some(std::time::Instant::now());
        let refused = ask().await;
        assert!(!refused.vote_granted,
            "a leader is answering reads on this node's promise not to vote for the next candidate");
        assert!(state.current_term() >= term + 4,
            "refusing is not following: the term still advances, so this node stops trusting \
             the leader it just protected, and is campaigning within the timeout");

        {
            let mut g = repl.write().unwrap();
            g.last_heartbeat = Some(std::time::Instant::now() - Duration::from_secs(60));
            g.booted_at = std::time::Instant::now() - Duration::from_secs(60);
        }
        assert!(ask().await.vote_granted,
            "and the same request wins once the leader has gone quiet, or nothing could ever elect");

        follower.kill();
    }

    /// The voter half of the leader-initiated round: what is granted is a duration on this node's clock,
    /// bounded by the ask and by contact, and recorded -- so clearing the contact clock cannot release it.
    #[tokio::test]
    async fn a_voter_grants_no_more_silence_than_its_own_contact_already_commits_it_to() {
        let root = temp_root();
        let mut follower = TestNode::new("replica", next_test_port(), &root, "replica");
        follower.heartbeat_timeout_secs = 30;
        follower.primary_addr = Some("http://127.0.0.1:1".to_string());
        follower.start();
        let state = follower.state.clone().unwrap();
        let repl = state.replication.clone().unwrap();
        let term = 5;
        repl.write().unwrap().term = term;
        let window = crate::consensus::lease::refusal_window(Duration::from_secs(30));

        let base = follower.url();
        let ask = |asker_term: u64, ms: u64| {
            let url = format!("{}/internal/heartbeat", base);
            async move {
                reqwest::Client::new().get(&url)
                    .query(&[("term", asker_term.to_string()), ("lease_ms", ms.to_string())])
                    .send().await.unwrap()
                    .json::<serde_json::Value>().await.unwrap()["novote_ms"].as_u64().unwrap()
            }
        };

        // Nothing has been heard from any leader, so there is nothing to grant on the strength of.
        repl.write().unwrap().last_heartbeat = None;
        assert_eq!(ask(term, 60_000).await, 0);

        repl.write().unwrap().last_heartbeat = Some(std::time::Instant::now());
        let granted = ask(term, 60_000).await;
        assert!(granted > 0 && granted <= window.as_millis() as u64,
            "a leader asking for more than the voter's own window gets the window, not the ask");
        assert_eq!(ask(term, 50).await, 50, "and no more than it asked for");
        assert_eq!(ask(term - 1, 5_000).await, 0,
            "a leader at a term this node has left may not rest its reads on our silence");

        // The step-down path clears contact; the grant is what the leader is still counting.
        {
            let mut g = repl.write().unwrap();
            g.last_heartbeat = None;
            g.last_replication = None;
            g.booted_at = std::time::Instant::now() - Duration::from_secs(60);
            assert!(g.novote_until.is_some_and(|u| u > std::time::Instant::now()));
        }
        let body = serde_json::json!({
            "term": term + 4, "candidate_id": "challenger", "last_lsn": 100, "last_term": term + 4});
        let vote = reqwest::Client::new()
            .post(format!("{}/internal/vote", follower.url()))
            .json(&body).send().await.unwrap()
            .json::<VoteResponse>().await.unwrap();
        assert!(!vote.vote_granted,
            "the grant outlives the contact it was computed from, or the lease resting on it is not one");

        follower.kill();
    }

    /// The whole safety of the lease bypass: only the leader a voter is actually following can spend its
    /// authority to stand something else up. Everything else stays refused.
    #[tokio::test]
    async fn only_the_leader_a_voter_follows_can_get_a_vote_past_its_lease() {
        let root = temp_root();
        let leader_url = format!("http://127.0.0.1:{}", next_test_port());
        let mut follower = TestNode::new("replica", next_test_port(), &root, "replica");
        follower.heartbeat_timeout_secs = 30;
        follower.primary_addr = Some(leader_url.clone());
        follower.start();
        let state = follower.state.clone().unwrap();
        let repl = state.replication.clone().unwrap();
        let term = state.current_term();

        // Squarely inside the refusal window, which is where a healthy cluster's voters all sit.
        repl.write().unwrap().last_heartbeat = Some(std::time::Instant::now());

        let ask = |transfer_from: Option<String>| {
            let url = follower.url();
            async move {
                let mut body = serde_json::json!({
                    "term": term + 3, "candidate_id": "challenger",
                    "last_lsn": 100, "last_term": term + 3});
                if let Some(from) = transfer_from {
                    body["transfer_from"] = serde_json::Value::from(from);
                }
                reqwest::Client::new()
                    .post(format!("{}/internal/vote", url))
                    .json(&body).send().await.unwrap()
                    .json::<VoteResponse>().await.unwrap()
            }
        };

        assert!(!ask(None).await.vote_granted, "vacuous unless the lease is holding here");
        assert!(!ask(Some("http://127.0.0.1:1".to_string())).await.vote_granted,
            "any node claiming a handover could force an election on a healthy cluster");
        assert!(!ask(Some(follower.url())).await.vote_granted,
            "and naming the voter itself is not naming the leader it follows");
        assert!(ask(Some(leader_url)).await.vote_granted,
            "the leader standing aside is the one case fresh contact from it must not refuse");

        follower.kill();
    }

    /// The endpoint is reachable by anything that clears `/internal/*`, so it answers the same
    /// question the vote does: is the sender the leader this node follows.
    #[tokio::test]
    async fn a_node_that_is_not_the_leader_cannot_order_a_voter_to_stand() {
        let root = temp_root();
        let leader_url = format!("http://127.0.0.1:{}", next_test_port());
        let mut follower = TestNode::new("replica", next_test_port(), &root, "replica");
        follower.heartbeat_timeout_secs = 30;
        follower.primary_addr = Some(leader_url.clone());
        follower.start();
        let state = follower.state.clone().unwrap();
        let term = state.current_term();

        let tell = |leader: String, at: u64| {
            let url = follower.url();
            async move {
                reqwest::Client::new()
                    .post(format!("{}/internal/timeout-now", url))
                    .json(&serde_json::json!({ "term": at, "leader": leader }))
                    .send().await.unwrap().status()
            }
        };

        assert_eq!(tell("http://127.0.0.1:1".to_string(), term).await, StatusCode::CONFLICT,
            "anyone able to reach this endpoint could otherwise depose a healthy leader at will");
        assert_eq!(state.current_term(), term, "and a refusal must not cost the voter a term");

        assert_eq!(tell(leader_url, term).await, StatusCode::ACCEPTED,
            "the leader this node follows is exactly who may hand office away");
        follower.kill();
    }

    /// The restart hole. A promise the process before this one made can still be counted by a
    /// leader, and nothing on disk remembers making it, so boot withholds exactly as contact does.
    #[tokio::test]
    async fn a_node_that_has_just_booted_refuses_a_vote_even_having_heard_from_nobody() {
        let root = temp_root();
        let mut follower = TestNode::new("replica", next_test_port(), &root, "replica");
        follower.heartbeat_timeout_secs = 30;
        follower.primary_addr = Some("http://127.0.0.1:1".to_string());
        follower.start();
        let state = follower.state.clone().unwrap();
        let repl = state.replication.clone().unwrap();
        let term = state.current_term();

        let body = serde_json::json!({
            "term": term + 4, "candidate_id": "challenger", "last_lsn": 100, "last_term": term + 4});
        let ask = || async {
            reqwest::Client::new()
                .post(format!("{}/internal/vote", follower.url()))
                .json(&body).send().await.unwrap()
                .json::<VoteResponse>().await.unwrap()
        };

        // No contact of any kind: whatever this refuses on is the boot clock and nothing else.
        {
            let mut g = repl.write().unwrap();
            g.last_heartbeat = None;
            g.last_replication = None;
        }
        assert!(!ask().await.vote_granted,
            "a leader can still be counting the promise this node's previous process made");

        // Up longer than the window, so any promise that process made has certainly run out.
        repl.write().unwrap().booted_at = std::time::Instant::now() - Duration::from_secs(60);
        assert!(ask().await.vote_granted,
            "and a node that has been up and heard nothing is exactly who has to elect the leader");

        follower.kill();
    }

    /// Raft §9.6 and bugs.md C20. The refusal itself is commit 48's; what is new is that reaching it
    /// costs the voter nothing, so a candidate that cannot win no longer deposes a leader on its way.
    #[tokio::test]
    async fn a_pre_vote_leaves_the_voters_term_and_vote_exactly_where_they_were() {
        let root = temp_root();
        let mut follower = TestNode::new("replica", next_test_port(), &root, "replica");
        follower.heartbeat_timeout_secs = 30;
        follower.primary_addr = Some("http://127.0.0.1:1".to_string());
        follower.start();
        let state = follower.state.clone().unwrap();
        let repl = state.replication.clone().unwrap();
        let term = state.current_term();
        let voted_before = repl.read().unwrap().voted_for.clone();

        let body = serde_json::json!({
            "term": term + 7, "candidate_id": "challenger", "last_lsn": 100, "last_term": term + 7});
        let ask = || async {
            reqwest::Client::new()
                .post(format!("{}/internal/pre-vote", follower.url()))
                .json(&body).send().await.unwrap()
                .json::<VoteResponse>().await.unwrap()
        };

        repl.write().unwrap().last_heartbeat = Some(std::time::Instant::now());
        let refused = ask().await;
        assert!(!refused.vote_granted, "a voter still hearing from its leader would not vote either");
        assert_eq!(refused.term, term, "and it answers with the term it still holds");
        assert_eq!(state.current_term(), term,
            "the term is the whole point: raising it here is what used to depose the leader");
        assert_eq!(repl.read().unwrap().voted_for, voted_before, "and no vote is spent either");

        {
            let mut g = repl.write().unwrap();
            g.last_heartbeat = Some(std::time::Instant::now() - Duration::from_secs(60));
            g.booted_at = std::time::Instant::now() - Duration::from_secs(60);
        }
        assert!(ask().await.vote_granted, "once the leader is quiet the same candidate is told yes");
        assert_eq!(state.current_term(), term, "still without a term moving, which the real vote does");

        follower.kill();
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
    }

    #[tokio::test]
    async fn ib005_replication_retries_failed_commit_persistence_on_duplicate_frames() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.start();
        assert_eq!(replicate(&replica, 1, 1, 0, 1, make_frame(1, 1, 0, 0, "a", 1)).await.0, StatusCode::OK);
        let state = replica.state.clone().unwrap();
        let col = state.db.as_ref().unwrap().get_collection("t").unwrap();
        let path = replica.data_dir.join("t");
        let blocked = path.join("applied.meta.tmp");
        std::fs::create_dir(&blocked).unwrap();
        for _ in 0..2 {
            let frame = crate::test_support::make_drop_frame(1, 2, 1, 1);
            assert_eq!(replicate(&replica, 1, 2, 1, 2, frame).await.0, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(crate::storage::Collection::recorded_watermark(&path).unwrap(), Some(1));
            assert!(col.is_dropped());
        }
        assert!(state.note_leader_committed("t", 1, 2, None).is_err());
        std::fs::remove_dir(&blocked).unwrap();
        let frame = crate::test_support::make_drop_frame(1, 2, 1, 1);
        assert_eq!(replicate(&replica, 1, 2, 1, 2, frame).await.0, StatusCode::OK);
        assert_eq!(crate::storage::Collection::recorded_watermark(&path).unwrap(), Some(2));
        drop(col);
        replica.kill();
        state.db.as_ref().unwrap().release_collection("t").unwrap();
        let reopened = state.db.as_ref().unwrap().get_collection("t").unwrap();
        assert!(reopened.is_dropped());
        assert_eq!(reopened.pending_len(), 0);
    }

    #[tokio::test]
    async fn ib001_commit_hint_replaces_conflicting_tail_before_publishing_changes() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.start();
        assert_eq!(replicate(&replica, 1, 1, 0, 1, make_frame(1, 1, 0, 0, "base", 1)).await.0, StatusCode::OK);
        assert_eq!(replicate(&replica, 1, 2, 1, 1, make_frame(1, 2, 1, 1, "stale", 2)).await.0, StatusCode::OK);
        let state = replica.state.as_ref().unwrap();
        let col = state.db.as_ref().unwrap().get_collection("t").unwrap();
        let mut feed = col.changefeed.subscribe(Some(1), col.applied_lsn()).unwrap();

        let (status, body) = replicate(&replica, 2, 3, 1, 99, make_frame(2, 3, 1, 1, "correct", 3)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(col.applied_lsn(), 3);
        assert_eq!(col.last_appended(), (2, 3));
        assert!(col.get("stale").unwrap().is_none());
        assert!(col.get("correct").unwrap().is_some());
        let events = tokio::time::timeout(Duration::from_secs(1), feed.next_batch()).await.unwrap().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key, "correct");
        assert_eq!(events[0].lsn, 3);
        assert_eq!(crate::storage::Collection::recorded_watermark(&col.root_path).unwrap(), Some(3));
        replica.kill();
    }

    #[tokio::test]
    async fn ib001_duplicates_and_heartbeats_commit_only_the_current_terms_matched_prefix() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.start();
        for lsn in 1..=2 {
            assert_eq!(replicate(&replica, 1, lsn, lsn - 1, 0,
                make_frame(1, lsn, lsn - 1, if lsn == 1 { 0 } else { 1 }, &format!("k{lsn}"), lsn as i64)).await.0, StatusCode::OK);
        }
        let state = replica.state.as_ref().unwrap();
        let col = state.db.as_ref().unwrap().get_collection("t").unwrap();
        let lock = state.snapshot_install_lock("t");
        {
            let _guard = lock.lock().await;
            state.replication.as_ref().unwrap().write().unwrap().term = 2;
            state.note_leader_committed("t", 2, 99, None).unwrap();
            assert_eq!(col.applied_lsn(), 0, "old-term matching is not heartbeat evidence");
        }
        assert_eq!(replicate(&replica, 2, 1, 0, 99, make_frame(1, 1, 0, 0, "k1", 1)).await.1["status"], "duplicate");
        assert_eq!(col.applied_lsn(), 1);
        assert!(col.get("k2").unwrap().is_none());
        {
            let _guard = lock.lock().await;
            state.note_leader_committed("t", 2, 99, None).unwrap();
            assert_eq!(col.applied_lsn(), 1);
        }
        assert_eq!(replicate(&replica, 2, 3, 1, 0, make_frame(2, 3, 1, 1, "k3", 3)).await.0, StatusCode::OK);
        assert_eq!(col.applied_lsn(), 1, "an earlier oversized hint must not commit a later append");
        {
            let _guard = lock.lock().await;
            state.note_leader_committed("t", 1, 99, None).unwrap();
            assert_eq!(col.applied_lsn(), 1, "a delayed heartbeat cannot spend newer matching evidence");
            state.note_leader_committed("t", 2, 99, None).unwrap();
            assert_eq!(col.applied_lsn(), 3, "an idle matched tail still commits by heartbeat");
        }
        replica.kill();
    }

    #[tokio::test]
    async fn ib001_refused_frames_cannot_publish_a_conflicting_tail() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.start();
        assert_eq!(replicate(&replica, 1, 1, 0, 0, make_frame(1, 1, 0, 0, "staged", 1)).await.0, StatusCode::OK);
        let col = replica.state.as_ref().unwrap().db.as_ref().unwrap().get_collection("t").unwrap();
        let mut corrupt = make_frame(2, 2, 1, 1, "bad", 2);
        *corrupt.last_mut().unwrap() ^= 1;
        for (lsn, prev, frame) in [
            (2, 1, vec![0]),
            (2, 1, corrupt),
            (3, 1, make_frame(2, 2, 1, 1, "mismatch", 2)),
            (3, 2, make_frame(2, 3, 2, 2, "gap", 3)),
            (2, 1, make_frame(2, 2, 1, 2, "divergent", 2)),
        ] {
            assert!(!replicate(&replica, 2, lsn, prev, 99, frame).await.0.is_success());
            assert_eq!(col.applied_lsn(), 0);
            assert!(col.get("staged").unwrap().is_none());
        }
        replica.kill();
    }

    #[tokio::test]
    async fn ib001_a_partial_batch_commits_only_its_accepted_prefix() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.start();
        let mut corrupt = make_frame(2, 3, 2, 2, "bad", 3);
        *corrupt.last_mut().unwrap() ^= 1;
        let response = replicate_handler(State(replica.state.as_ref().unwrap().clone()), Json(ReplicateRequest {
            collection: "t".into(), term: 2, lsn: 1, prev_lsn: 0, commit_index: Some(99),
            wal_frame: make_frame(2, 1, 0, 0, "k1", 1),
            frames: vec![make_frame(2, 2, 1, 2, "k2", 2), corrupt],
        })).await.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let col = replica.state.as_ref().unwrap().db.as_ref().unwrap().get_collection("t").unwrap();
        assert_eq!(col.applied_lsn(), 2);
        assert_eq!(col.durable_lsn(), 2);
        assert!(col.get("k2").unwrap().is_some());
        assert!(col.get("bad").unwrap().is_none());
        replica.kill();
    }

    #[tokio::test]
    async fn a_replica_replaces_an_uncommitted_tail_rather_than_asking_for_a_snapshot() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.primary_addr = Some("http://127.0.0.1:1".to_string());
        replica.start();

        for lsn in 1..=3u64 {
            let (status, _) = replicate(&replica, 5, lsn, lsn - 1, 0,
                make_frame(5, lsn, lsn - 1, if lsn == 1 { 0 } else { 5 }, &format!("k{}", lsn), lsn as i64)).await;
            assert_eq!(status, StatusCode::OK);
        }

        let (status, body) = replicate(&replica, 6, 3, 2, 0, make_frame(6, 3, 2, 5, "k3", 99)).await;
        assert_eq!(status, StatusCode::OK, "an uncommitted tail is the leader's to replace: {:?}", body);
        assert_eq!(body["status"], "applied");

        let col = replica.state.as_ref().unwrap()
            .db.as_ref().unwrap()
            .get_collection("t").unwrap();
        assert_eq!(col.last_appended(), (6, 3));
        assert_eq!(col.pending_len(), 3, "the superseded frame is gone, not stacked under its replacement");

        replica.kill();
    }

    /// The whole of commit 46 rests on this field: without it the leader has no point below the
    /// replica's tail that it knows they agree on, and a snapshot is the only way back.
    #[tokio::test]
    async fn a_divergent_refusal_carries_the_watermark_the_leader_can_back_up_to() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.primary_addr = Some("http://127.0.0.1:1".to_string());
        replica.start();

        assert_eq!(replicate(&replica, 5, 1, 0, 0, make_frame(5, 1, 0, 0, "k1", 1)).await.0, StatusCode::OK);
        assert_eq!(replicate(&replica, 5, 2, 1, 1, make_frame(5, 2, 1, 5, "k2", 2)).await.0, StatusCode::OK);

        let col = replica.state.as_ref().unwrap()
            .db.as_ref().unwrap()
            .get_collection("t").unwrap();
        let watermark = col.applied_lsn();
        assert!(watermark > 0, "the fixture needs something committed to name");

        // Same position as our tail, different history: the conflict is at lsn 2 or below.
        let (status, body) = replicate(&replica, 6, 3, 2, 0, make_frame(6, 3, 2, 6, "k3", 3)).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["status"], "divergent");
        assert_eq!(body["last_lsn"], 2);
        assert_eq!(body["applied"], watermark, "the leader resumes from here instead of snapshotting");

        replica.kill();
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
    }

    /// M9b: `/internal/drop` refused before it read the term, so a deposed leader kept a dropped
    /// collection and a follower applied a higher term's drop without adopting the term.
    #[tokio::test]
    async fn a_leader_steps_down_for_a_drop_from_a_higher_term() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.heartbeat_timeout_secs = 30;
        leader.start();
        assert!(leader.is_leader());

        let client = reqwest::Client::new();
        assert_eq!(put_doc_http(&client, &leader.url(), "k1", 1).await, StatusCode::CREATED);

        let db = leader.state.as_ref().unwrap().db.as_ref().unwrap().clone();
        let col = db.get_collection("t").unwrap();
        let (prev_term, prev_lsn) = col.last_appended();

        let (status, body) = replicate(
            &leader, 9, prev_lsn + 1, prev_lsn, prev_lsn + 1,
            make_drop_frame(9, prev_lsn + 1, prev_lsn, prev_term),
        ).await;

        assert!(!leader.is_leader(), "a drop from a higher term deposes a leader like any entry");
        assert_eq!(leader.term(), 9, "and the term is adopted, not just the payload");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "applied");

        assert!(col.is_dropped());
        assert!(col.get("k1").unwrap().is_none(), "the drop takes every key below it");
        assert!(db.live_collections().unwrap().is_empty());

        leader.kill();
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
    }

    #[tokio::test]
    async fn granting_a_vote_at_a_higher_term_clears_the_watermark_too() {
        let root = temp_root();
        let mut leader = leader_with_one_committed_write(&root).await;

        // Out of the boot window, which this test has no stake in: a node that just came up
        // withholds its vote for one refusal window, and the path under test is the grant.
        leader.state.as_ref().unwrap().replication.as_ref().unwrap().write().unwrap()
            .booted_at = std::time::Instant::now() - Duration::from_secs(60);

        // Stands down without ever going through demote(), which is why the vote path needs the
        // same reset rather than relying on apply_demotion to have done it.
        let vote = VoteRequest {
            term: 9,
            candidate_id: "challenger".to_string(),
            last_lsn: 100,
            last_term: 9,
            logs: HashMap::new(),
            candidate_url: None,
            transfer_from: None,
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
    }
}
