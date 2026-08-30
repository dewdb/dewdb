//! The local write pipeline shared by every mutating handler.

use crate::model::err_json;
use crate::replication::stream::{replicate_and_await, replicate_to_peers};
use crate::replication::WriteConcern;
use crate::replication::write_concern::write_quorum;
use crate::json::merge_patch;
use crate::state::AppState;
use crate::storage::{Collection, FrameHeader};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::collections::HashSet;
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
    // Staged included, so a replace of a key whose previous write has not committed is not a create.
    let existed = col.exists_including_staged(&key);

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

    // Resolved against the configuration in force, not a replica count: while a change is in
    // flight a majority means a majority of each half. A learner acknowledging never counts.
    let quorum = write_quorum(&wc, &state.quorum_config());
    let required = quorum.required();
    let own = state.own_url();
    // From the header, not lsn - 1: the previous LSN usually belongs to another collection.
    let prev_lsn = FrameHeader::parse(&frame).map_or(0, |h| h.prev_lsn);
    // Predates this frame, which is what lets the send start before the fsync lands. Followers
    // already expect a trailing watermark and publish on the next message carrying a higher one.
    let commit_index = state.committed_lsn(col_name);

    let holders = if quorum.met(std::slice::from_ref(&own)) {
        replicate_to_peers(state.clone(), col_name.to_string(), frame, term, commit_index, lsn, prev_lsn);
        if let Some(c) = commit {
            settle_commit(c).await?;
        }
        vec![own]
    } else {
        let replicating = replicate_and_await(
            state.clone(), col_name.to_string(), frame, term, commit_index, lsn, prev_lsn,
            quorum.clone(), wtimeout,
        );
        match commit {
            // The local disk write and the replica round trips are independent, so the client waits
            // for the slower of the two instead of their sum.
            Some(c) => {
                let (holders, committed) = tokio::join!(replicating, settle_commit(c));
                committed?;
                holders
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

    Ok(WriteOutcome { met: quorum.met(&holders), acks: holders.len(), required, existed })
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

/// The drop as a replicated log entry: it commits the way a write does, so a quorum holds it before
/// the client hears success, a replica that was down for it picks it up from the log, and a leader
/// elected afterwards replays it instead of having to be told.
pub async fn local_drop(
    state: &AppState,
    col_name: &str,
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
        // Every stripe, in order: a drop removes every key, so an in-flight read-modify-write on
        // any of them must land on one side of it or the other.
        let mut _guards = Vec::with_capacity(col.key_locks.len());
        for lock in col.key_locks.iter() {
            _guards.push(lock.lock().await);
        }

        let term = state.current_term();
        let col_clone = col.clone();
        let appended = tokio::task::spawn_blocking(move || col_clone.drop_marker(term)).await;
        let (frame, _wal_id, _offset, lsn) = match appended {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
            Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        };

        let commit = col.enqueue_commit();
        state.note_leader_append(col_name, lsn);
        PendingWrite { frame, term, lsn, existed: true, commit: Some(commit) }
    };

    finish_write(state, col_name, pending, wc, wtimeout).await
}

async fn local_write_batch_inner(
    state: &AppState,
    col: &Arc<Collection>,
    items: Vec<(String, serde_json::Value)>,
) -> Result<Vec<PendingWrite>, axum::response::Response> {
    let term = state.current_term();
    // A key repeated inside one batch is replaced by its second write, and the pre-batch state
    // cannot show that: every sample here is taken before the first `put`.
    let mut batched: HashSet<&str> = HashSet::new();
    let existed: Vec<bool> = items.iter()
        .map(|(key, _)| !batched.insert(key.as_str()) || col.exists_including_staged(key))
        .collect();

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

#[cfg(test)]
mod tests {
    use crate::test_support::{temp_root, three_node_cluster};
    use axum::http::StatusCode;
    use std::time::Duration;

    /// M5: `existed` was sampled from the committed index, so with the quorum down — every write
    /// durable and none of them committed — a replace and a delete both reported nothing was there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn created_or_replaced_is_decided_against_the_uncommitted_tail() {
        let root = temp_root();
        let (n1, mut n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::new();

        n2.kill();
        n3.kill();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let url = format!("{}/collections/t/docs/k?w=majority&wtimeout=1000", n1.url());
        let write = |body: Option<serde_json::Value>| {
            let (c, url) = (client.clone(), url.clone());
            async move {
                let r = match body {
                    Some(v) => c.put(&url).json(&serde_json::json!({"value": v})).send().await,
                    None => c.delete(&url).send().await,
                }.unwrap();
                let status = r.status();
                (status, r.json::<serde_json::Value>().await.unwrap())
            }
        };

        let (status, body) = write(Some(serde_json::json!({"v": 1}))).await;
        assert_eq!(status, StatusCode::ACCEPTED, "no quorum, so the write is staged: {}", body);
        assert_eq!(body["status"], "created");

        let (status, body) = write(Some(serde_json::json!({"v": 2}))).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["status"], "replaced", "unfixed this reported a second create: {}", body);

        let (_status, body) = write(None).await;
        assert_eq!(body["existed"], true, "unfixed this deleted a key it said was not there: {}", body);

        let _ = std::fs::remove_dir_all(&root);
    }
}
