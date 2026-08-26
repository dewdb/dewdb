//! The local write pipeline shared by every mutating handler.

use crate::model::err_json;
use crate::replication::stream::{replicate_and_await, replicate_to_peers};
use crate::replication::WriteConcern;
use crate::replication::write_concern::required_acks;
use crate::json::merge_patch;
use crate::state::AppState;
use crate::storage::{Collection, FrameHeader};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::io;
use std::sync::Arc;
use std::time::Duration;

pub struct WriteOutcome {
    pub met: bool,
    pub acks: usize,
    pub required: usize,
    pub existed: bool,
}

struct PendingWrite {
    pub frame: Vec<u8>,
    pub term: u64,
    pub lsn: u64,
    pub existed: bool,
    /// Outstanding local fsync. Held so replication can start before it lands; `None` when the
    /// caller already synced, as the batch path does once for the whole batch.
    pub commit: Option<CommitWait>,
}

type CommitWait = tokio::sync::oneshot::Receiver<Result<(), String>>;

async fn settle_commit(commit: CommitWait) -> Result<(), axum::response::Response> {
    match commit.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e)),
        Err(e) => Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn local_write_inner(
    state: &AppState,
    col: &Arc<Collection>,
    key: String,
    value: Option<serde_json::Value>,
) -> Result<PendingWrite, axum::response::Response> {
    let col_clone = col.clone();
    let key_clone = key.clone();
    let term = state.current_term();
    // Sampled under the key lock: created/replaced must reflect this write, not a racing one.
    let existed = col.exists(&key);

    let write_res = tokio::task::spawn_blocking(move || {
        match value {
            Some(v) => col_clone.put(key_clone, v, term),
            None => col_clone.delete(key_clone, term),
        }
    }).await;

    let (frame, _wal_id, _offset, lsn) = match write_res {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    let commit = col.enqueue_commit();
    state.note_leader_append(&col.name, lsn);

    Ok(PendingWrite { frame, term, lsn, existed, commit: Some(commit) })
}

async fn finish_write(
    state: &AppState,
    col_name: &str,
    pending: PendingWrite,
    wc: WriteConcern,
    wtimeout: Duration,
) -> Result<WriteOutcome, axum::response::Response> {
    let PendingWrite { frame, term, lsn, existed, commit } = pending;

    if !state.is_leader() {
        if let Some(c) = commit {
            settle_commit(c).await?;
        }
        return Ok(WriteOutcome { met: true, acks: 1, required: 1, existed });
    }

    // Quorum set only: a learner acknowledging must never help satisfy a write concern.
    let replicas = state.voting_replicas();
    let required = required_acks(&wc, replicas.len());
    // From the header, not lsn - 1: the previous LSN usually belongs to another collection.
    let prev_lsn = FrameHeader::parse(&frame).map_or(0, |h| h.prev_lsn);
    // Predates this frame, which is what lets the send start before the fsync lands. Followers
    // already expect a trailing watermark and publish on the next message carrying a higher one.
    let commit_index = state.committed_lsn(col_name);

    let acks = if required <= 1 {
        replicate_to_peers(state.clone(), col_name.to_string(), frame, term, commit_index, lsn, prev_lsn);
        if let Some(c) = commit {
            settle_commit(c).await?;
        }
        1
    } else {
        let replicating = replicate_and_await(
            state.clone(), col_name.to_string(), frame, term, commit_index, lsn, prev_lsn,
            required, wtimeout,
        );
        match commit {
            // The local disk write and the replica round trips are independent, so the client waits
            // for the slower of the two instead of their sum.
            Some(c) => {
                let (acks, committed) = tokio::join!(replicating, settle_commit(c));
                committed?;
                acks
            },
            None => replicating.await,
        }
    };

    // Only after the fsync above: counting our own durability early would put an entry in the
    // commit index that this node could still lose.
    let own_durable = state
        .db
        .as_ref()
        .and_then(|db| db.get_collection(col_name).ok())
        .map_or(0, |col| col.durable_lsn());
    state.advance_own_commit(col_name, own_durable);

    Ok(WriteOutcome { met: acks >= required, acks, required, existed })
}

/// 503 rather than 500: the write is not wrong, the leader is too far ahead of its quorum, and the
/// same request will succeed once commits catch up.
pub fn backpressure_response(collection: &str, pending: usize, bound: usize) -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(axum::http::header::RETRY_AFTER, "1")],
        axum::Json(serde_json::json!({
            "error": "replication backlog too large",
            "collection": collection,
            "uncommitted_frames": pending,
            "max_uncommitted_frames": bound,
        })),
    ).into_response()
}

pub async fn local_write(
    state: &AppState,
    col_name: &str,
    key: String,
    value: Option<serde_json::Value>,
    wc: WriteConcern,
    wtimeout: Duration,
) -> Result<WriteOutcome, axum::response::Response> {
    if let Err(pending) = state.admit_write(col_name) {
        return Err(backpressure_response(
            col_name, pending, state.config.flow_control.max_uncommitted_frames));
    }

    let db = state.db.as_ref().unwrap();
    let col = match db.get_collection(col_name) {
        Ok(c) => c,
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    let pending = {
        let _guard = col.key_lock(&key).lock().await;
        local_write_inner(state, &col, key, value).await?
    };

    finish_write(state, col_name, pending, wc, wtimeout).await
}

pub async fn local_patch(
    state: &AppState,
    col_name: &str,
    key: String,
    patch: serde_json::Value,
    wc: WriteConcern,
    wtimeout: Duration,
) -> Result<Option<WriteOutcome>, axum::response::Response> {
    if let Err(pending) = state.admit_write(col_name) {
        return Err(backpressure_response(
            col_name, pending, state.config.flow_control.max_uncommitted_frames));
    }

    let db = state.db.as_ref().unwrap();
    let col = match db.get_collection(col_name) {
        Ok(c) => c,
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    let pending = {
        let _guard = col.key_lock(&key).lock().await;

        let col_read = col.clone();
        let key_read = key.clone();
        let current = match tokio::task::spawn_blocking(move || col_read.get_including_staged(&key_read)).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
            Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        };

        let mut doc = match current {
            Some(d) => d,
            None => return Ok(None),
        };

        merge_patch(&mut doc, &patch);

        local_write_inner(state, &col, key, Some(doc)).await?
    };

    Ok(Some(finish_write(state, col_name, pending, wc, wtimeout).await?))
}

async fn local_write_batch_inner(
    state: &AppState,
    col: &Arc<Collection>,
    items: Vec<(String, serde_json::Value)>,
) -> Result<Vec<PendingWrite>, axum::response::Response> {
    let term = state.current_term();
    let existed: Vec<bool> = items.iter().map(|(key, _)| col.exists(key)).collect();

    let col_clone = col.clone();
    let write_res = tokio::task::spawn_blocking(move || {
        let mut out = Vec::with_capacity(items.len());
        for (key, value) in items {
            let (frame, wal_id, offset, lsn) = col_clone.put(key.clone(), value, term)?;
            out.push((key, frame, wal_id, offset, lsn));
        }
        Ok::<_, io::Error>(out)
    }).await;

    let frames = match write_res {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    match col.enqueue_commit().await {
        Ok(Ok(())) => {},
        Ok(Err(e)) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e)),
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }

    for (_key, _frame, _wal_id, _offset, lsn) in &frames {
        state.note_leader_append(&col.name, *lsn);
    }

    Ok(frames.into_iter().zip(existed.into_iter())
        .map(|((_, frame, _, _, lsn), existed)| PendingWrite { frame, term, lsn, existed, commit: None })
        .collect())
}

pub async fn local_write_batch(
    state: &AppState,
    col_name: &str,
    items: Vec<(String, serde_json::Value)>,
    wc: WriteConcern,
    wtimeout: Duration,
) -> Result<Vec<WriteOutcome>, axum::response::Response> {
    if let Err(pending) = state.admit_write(col_name) {
        return Err(backpressure_response(
            col_name, pending, state.config.flow_control.max_uncommitted_frames));
    }

    let db = state.db.as_ref().unwrap();
    let col = match db.get_collection(col_name) {
        Ok(c) => c,
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    let mut stripes: Vec<usize> = items.iter().map(|(key, _)| col.key_stripe(key)).collect();
    stripes.sort_unstable();
    // Locking per key deadlocks as soon as two keys in the batch share a stripe.
    stripes.dedup();

    let mut _guards = Vec::with_capacity(stripes.len());
    for stripe in stripes {
        _guards.push(col.key_locks[stripe].lock().await);
    }

    let pending = local_write_batch_inner(state, &col, items).await?;

    futures::future::join_all(
        pending.into_iter().map(|p| finish_write(state, col_name, p, wc, wtimeout))
    ).await.into_iter().collect()
}
