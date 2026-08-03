//! The local write pipeline shared by every mutating handler.

use crate::model::err_json;
use crate::replication::stream::{replicate_and_await, replicate_to_peers};
use crate::replication::WriteConcern;
use crate::replication::write_concern::required_acks;
use crate::json::merge_patch;
use crate::state::AppState;
use crate::storage::{Collection, FrameHeader, HEADER_LEN};
use axum::http::StatusCode;
use std::io;
use std::sync::atomic::Ordering;
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
}

async fn local_write_inner(
    state: &AppState,
    col: &Arc<Collection>,
    key: String,
    value: Option<serde_json::Value>,
) -> Result<PendingWrite, axum::response::Response> {
    let col_clone = col.clone();
    let key_clone = key.clone();
    let is_delete = value.is_none();
    let term = state.current_term();
    // Sampled before the append while the key lock is held, so created/replaced
    // reflects what this write actually did.
    let existed = col.exists(&key);

    let write_res = tokio::task::spawn_blocking(move || {
        match value {
            Some(v) => col_clone.put(key_clone, v, term),
            None => col_clone.delete(key_clone, term),
        }
    }).await;

    let (frame, wal_id, offset, lsn) = match write_res {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    match col.enqueue_commit().await {
        Ok(Ok(())) => {},
        Ok(Err(e)) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e)),
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }

    {
        // Ordering: the index is published only after the frame is fsynced, so a reader
        // can never observe a write that a crash would lose.
        let mut index = col.index.write().unwrap();
        if is_delete {
            index.remove(&key);
        } else {
            col.apply_index_put(&mut index, key.clone(), col.build_entry(wal_id, offset, &frame[HEADER_LEN..]));
        }
    }

    Ok(PendingWrite { frame, term, lsn, existed })
}

async fn finish_write(
    state: &AppState,
    col_name: &str,
    pending: PendingWrite,
    wc: WriteConcern,
    wtimeout: Duration,
) -> WriteOutcome {
    if !state.is_leader() {
        return WriteOutcome { met: true, acks: 1, required: 1, existed: pending.existed };
    }

    let db = state.db.as_ref().unwrap();
    let commit_index = db.global_commit_index.load(Ordering::SeqCst);
    let replicas = state.get_replicas();
    let required = required_acks(&wc, replicas.len());
    // From the frame header, not lsn - 1: the previous LSN belongs to whichever
    // collection was written last, which is usually a different one.
    let prev_lsn = FrameHeader::parse(&pending.frame).map_or(0, |h| h.prev_lsn);

    let acks = if required <= 1 {
        replicate_to_peers(
            state.clone(),
            col_name.to_string(),
            pending.frame,
            pending.term,
            commit_index,
            pending.lsn,
            prev_lsn,
        );
        1
    } else {
        replicate_and_await(
            state.clone(),
            col_name.to_string(),
            pending.frame,
            pending.term,
            commit_index,
            pending.lsn,
            prev_lsn,
            required,
            wtimeout,
        ).await
    };

    WriteOutcome { met: acks >= required, acks, required, existed: pending.existed }
}

pub async fn local_write(
    state: &AppState,
    col_name: &str,
    key: String,
    value: Option<serde_json::Value>,
    wc: WriteConcern,
    wtimeout: Duration,
) -> Result<WriteOutcome, axum::response::Response> {
    let db = state.db.as_ref().unwrap();
    let col = match db.get_collection(col_name) {
        Ok(c) => c,
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    let pending = {
        let _guard = col.key_lock(&key).lock().await;
        local_write_inner(state, &col, key, value).await?
    };

    Ok(finish_write(state, col_name, pending, wc, wtimeout).await)
}

pub async fn local_patch(
    state: &AppState,
    col_name: &str,
    key: String,
    patch: serde_json::Value,
    wc: WriteConcern,
    wtimeout: Duration,
) -> Result<Option<WriteOutcome>, axum::response::Response> {
    let db = state.db.as_ref().unwrap();
    let col = match db.get_collection(col_name) {
        Ok(c) => c,
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    let pending = {
        let _guard = col.key_lock(&key).lock().await;

        let col_read = col.clone();
        let key_read = key.clone();
        let current = match tokio::task::spawn_blocking(move || col_read.get(&key_read)).await {
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

    Ok(Some(finish_write(state, col_name, pending, wc, wtimeout).await))
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

    {
        let mut index = col.index.write().unwrap();
        for (key, frame, wal_id, offset, _) in &frames {
            let entry = col.build_entry(*wal_id, *offset, &frame[HEADER_LEN..]);
            col.apply_index_put(&mut index, key.clone(), entry);
        }
    }

    Ok(frames.into_iter().zip(existed.into_iter())
        .map(|((_, frame, _, _, lsn), existed)| PendingWrite { frame, term, lsn, existed })
        .collect())
}

pub async fn local_write_batch(
    state: &AppState,
    col_name: &str,
    items: Vec<(String, serde_json::Value)>,
    wc: WriteConcern,
    wtimeout: Duration,
) -> Result<Vec<WriteOutcome>, axum::response::Response> {
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

    Ok(futures::future::join_all(
        pending.into_iter().map(|p| finish_write(state, col_name, p, wc, wtimeout))
    ).await)
}
