//! Outbound replication and gap repair.

use super::protocol::{
    applied_through, classify_conflict, ConflictKind, ReplicateRequest, ResyncRequest,
};
use crate::consensus::demote;
use crate::replication::protocol::forbidden_term;
use crate::replication::write_concern::WriteQuorum;
use crate::state::AppState;
use crate::storage::frame::MAX_FRAME_SIZE;
use crate::storage::FrameHeader;
use axum::http::StatusCode;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

// Bounds the receiver's blocking append run. It does not bound the request: frames range over four
// orders of magnitude, so the size budget below is what keeps a batch inside `MAX_INTERNAL_BODY`.
const REPLICATION_BATCH_FRAMES: usize = 64;

/// Raw bytes per request, encoded to base64 by `wal_frame`. One max-size frame's worth, so the
/// encoded body fits `MAX_INTERNAL_BODY` and any single frame the log holds fits a batch alone.
const REPLICATION_BATCH_BYTES: usize = MAX_FRAME_SIZE as usize;

// A couple of misses are normal under load, so backoff only starts after that. The cap keeps a
// long-dead replica polled often enough that it rejoins promptly.
const DRIVE_MISSES_BEFORE_BACKOFF: u32 = 2;
const DRIVE_BACKOFF_SHIFT_CAP: u32 = 4;

/// Per `(replica, collection)`, not per replica: one lock for independent logs means every repair but
/// one returns having done nothing. Prefixed -- `repair_locks` is shared with the install paths.
fn repair_lock(state: &AppState, replica_url: &str, collection: &str) -> Arc<tokio::sync::Mutex<()>> {
    let key = format!("repair:{}|{}", replica_url, collection);
    let mut locks = state.repair_locks.lock().unwrap();
    locks.entry(key)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// One verdict per replica per round. A replica is only idle if *none* of its collections moved;
/// scoring each work item separately counted one round as several misses.
fn score_replicas(outcomes: Vec<(String, bool)>) -> Vec<(String, bool)> {
    let mut by_replica: Vec<(String, bool)> = Vec::new();
    for (replica, moved) in outcomes {
        match by_replica.iter_mut().find(|(r, _)| *r == replica) {
            Some((_, seen)) => *seen |= moved,
            None => by_replica.push((replica, moved)),
        }
    }
    by_replica
}

/// Ticks to skip after `misses` consecutive rounds that made no progress. Only the periodic driver
/// backs off; a write still triggers repair immediately, so this delays nothing but the idle case.
fn drive_backoff_ticks(misses: u32) -> u32 {
    if misses < DRIVE_MISSES_BEFORE_BACKOFF {
        return 0;
    }
    1u32 << (misses - DRIVE_MISSES_BEFORE_BACKOFF).min(DRIVE_BACKOFF_SHIFT_CAP)
}

// Fire-and-forget: frames arrive out of order and a reported gap is routine, not a fault.
pub fn replicate_to_peers(
    state: AppState,
    collection: String,
    frame: Vec<u8>,
    term: u64,
    commit_index: u64,
    lsn: u64,
    prev_lsn: u64,
) {
    let replicas = state.replication_targets();
    if replicas.is_empty() {
        return;
    }
    let client = state.client.clone();
    tokio::spawn(async move {
        let mut handles = Vec::new();
        for replica_url in replicas {
            let client = client.clone();
            let col = collection.clone();
            let frame = frame.clone();
            let state = state.clone();
            handles.push(tokio::spawn(async move {
                if stream_backlog_if_behind(&state, &replica_url, &col, prev_lsn, lsn).await.is_some() {
                    return;
                }

                let url = format!("{}/internal/replicate", replica_url);
                let req_body = ReplicateRequest {
                    collection: col.clone(),
                    term,
                    lsn,
                    prev_lsn,
                    commit_index: Some(commit_index),
                    wal_frame: frame,
                    frames: Vec::new(),
                };
                // Released before any repair below, which acquires its own slots per batch.
                let slots = state.replication_slots.clone();
                let slot = slots.acquire().await;
                let response = client.post(&url).json(&req_body).send().await;
                drop(slot);
                match response {
                    Ok(r) if r.status().is_success() => {
                        state.note_sent(&replica_url, &col, lsn);
                        if let Err(e) = state.note_ack(&replica_url, &col, lsn, term) {
                            warn!(target: "replication", error = %e, "Failed to persist local commit watermark");
                        }
                    },
                    Ok(r) if r.status() == StatusCode::CONFLICT => {
                        let body = r.json::<serde_json::Value>().await.ok();
                        match classify_conflict(&body) {
                            ConflictKind::StaleTerm(t) => {
                                warn!(target: "replication", "Replica {} reports higher term {}; demoting", replica_url, t);
                                demote(&state, t).await;
                            },
                            ConflictKind::Divergent { last_lsn, applied } => {
                                state.metrics.note_divergence();
                                match applied {
                                    Some(watermark) => {
                                        warn!(target: "replication", "Replica {} diverges from us at lsn {} (its last_lsn={}); resuming from its watermark {}", replica_url, lsn, last_lsn, watermark);
                                        state.rewind_replica(&replica_url, &col, watermark);
                                        let _ = repair_replica(state.clone(), replica_url.clone(), col, watermark, None, 0).await;
                                    },
                                    None => {
                                        warn!(target: "replication", "Replica {} diverges from us at lsn {} (its last_lsn={}) and reports no watermark; snapshotting it", replica_url, lsn, last_lsn);
                                        // The snapshot decides its tail, so the cursor is stale either way.
                                        state.rewind_replica(&replica_url, &col, last_lsn);
                                        trigger_resync(&state, &replica_url, &col).await;
                                    },
                                }
                            },
                            ConflictKind::Gap(last_lsn, last_term) => {
                                state.metrics.note_gap();
                                warn!(target: "replication", "Replica {} gap at lsn {} (its last_lsn={}), starting repair", replica_url, lsn, last_lsn);
                                state.rewind_replica(&replica_url, &col, last_lsn);
                                let _ = repair_replica(state, replica_url.clone(), col, last_lsn, Some(last_term), 0).await;
                            }
                        }
                    },
                    Ok(r) if r.status() == StatusCode::FORBIDDEN => {
                        let body = r.json::<serde_json::Value>().await.ok();
                        let their_term = forbidden_term(&body);
                        if their_term > term {
                            warn!(target: "replication", "Replica {} rejected us with higher term {}; demoting", replica_url, their_term);
                            demote(&state, their_term).await;
                        }
                    },
                    Ok(r) => {
                        warn!(target: "replication", "Replica {} returned {} (term={}, lsn={})", replica_url, r.status(), term, lsn);
                    },
                    Err(e) => {
                        warn!(target: "replication", "Replica {} failed: {} (term={}, lsn={})", replica_url, e, term, lsn);
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    });
}

/// Makes progress the leader's standing job rather than a side effect of client traffic; an idle
/// cluster used to leave a lagging replica lagging. Safe to fire repeatedly: repair coalesces.
pub fn replication_drive_task(state: AppState) {
    let interval = Duration::from_millis(state.config.flow_control.drive_interval_ms.max(50));
    tokio::spawn(async move {
        // Local to this task, so no shared state and no locking. Keyed by replica.
        let mut misses: HashMap<String, u32> = HashMap::new();
        let mut skips: HashMap<String, u32> = HashMap::new();

        loop {
            tokio::time::sleep(interval).await;
            if !state.is_leader() {
                continue;
            }
            let db = match state.db.as_ref() {
                Some(d) => d.clone(),
                None => return,
            };
            // Before the scan, because it is what may give the scan something to send: promotion
            // is not the only moment a leader holds a tail with no current-term entry (bugs.md C30).
            crate::consensus::publish_inherited_tails(&state);

            let replicas = state.replication_targets();
            if replicas.is_empty() {
                continue;
            }

            let mut work: Vec<(String, String, u64, bool)> = Vec::new();
            for name in db.list_collections().unwrap_or_default() {
                let tail = match db.get_collection(&name) {
                    Ok(col) => col.last_appended_lsn(),
                    Err(_) => continue,
                };
                if tail == 0 {
                    continue;
                }
                for replica in replicas.iter() {
                    // A replica that has failed repeatedly is polled on a widening interval, so a
                    // node that is simply down does not cost a full scan and a connect every tick.
                    let due = skips.get(replica).copied().unwrap_or(0) == 0;
                    if !due {
                        continue;
                    }
                    let cursor = state.sent_through(replica, &name);
                    match cursor {
                        Some(c) if c < tail => work.push((replica.clone(), name.clone(), c, false)),
                        // Nothing to send, or no cursor to send from (IB-055, bugs.md C30): the
                        // tail frame asks either way, and a refusal names where the next tick resumes.
                        _ if state.matched_lsn(replica, &name) < tail => {
                            work.push((replica.clone(), name.clone(), cursor.unwrap_or(0), true));
                        },
                        _ => {},
                    }
                }
            }

            for count in skips.values_mut() {
                *count = count.saturating_sub(1);
            }

            let outcomes = futures::future::join_all(work.into_iter().map(|(replica, name, cursor, confirm)| {
                let state = state.clone();
                async move {
                    if confirm {
                        // Progress here is evidence, not a cursor: the frame was already sent.
                        return (replica.clone(), confirm_tail(&state, &replica, &name).await);
                    }
                    let before = state.sent_through(&replica, &name).unwrap_or(cursor);
                    let _ = repair_replica(state.clone(), replica.clone(), name.clone(), cursor, None, 0).await;
                    // Judged on whether the cursor moved, not on the return value: a repair that
                    // coalesced behind another worker reports false without anything being wrong.
                    let moved = state.sent_through(&replica, &name).unwrap_or(before) > before;
                    (replica, moved)
                }
            })).await;

            for (replica, moved) in score_replicas(outcomes) {
                if moved {
                    misses.remove(&replica);
                    skips.remove(&replica);
                } else {
                    let n = misses.entry(replica.clone()).or_insert(0);
                    *n = n.saturating_add(1);
                    let ticks = drive_backoff_ticks(*n);
                    if ticks > 0 {
                        skips.insert(replica, ticks);
                    }
                }
            }
        }
    });
}

/// Drives one replica to this leader's tail on every collection, and reports whether it got there.
/// One pass -- the caller repeats it against a deadline, holding the write gate so the tail is still.
pub(crate) async fn catch_up_replica(state: &AppState, replica: &str) -> bool {
    let Some(db) = state.db.as_ref().cloned() else { return false };
    let mut caught_up = true;

    for name in db.list_collections().unwrap_or_default() {
        let Ok(col) = db.get_collection(&name) else { continue };
        let tail = col.last_appended_lsn();
        if tail == 0 || state.matched_lsn(replica, &name) >= tail {
            continue;
        }
        // With a backlog we stream it, with none and no ack on record the tail frame is re-sent to
        // be answered (bugs.md C30). A missing cursor streams from 0 here, unlike the driver (IB-058).
        match state.sent_through(replica, &name) {
            Some(cursor) if cursor >= tail => { confirm_tail(state, replica, &name).await; },
            cursor => {
                repair_replica(state.clone(), replica.to_string(), name.clone(),
                    cursor.unwrap_or(0), None, tail).await;
            },
        }
        if state.matched_lsn(replica, &name) < tail {
            caught_up = false;
        }
    }
    caught_up
}

// Repair invariant: an unbroken predecessor chain from the replica's tail.
// LSNs are sparse per collection, and a chain broken by compaction forces a snapshot.
fn chain_prefix(after_lsn: u64, after_term: u64, mut frames: Vec<(u64, Vec<u8>)>) -> Vec<(u64, Vec<u8>)> {
    frames.sort_by_key(|(lsn, _)| *lsn);

    let mut prev_lsn = after_lsn;
    let mut prev_term = after_term;
    let mut out = Vec::new();

    for (lsn, frame) in frames {
        let header = match FrameHeader::parse(&frame) {
            Some(h) => h,
            None => break,
        };
        if header.prev_lsn != prev_lsn || header.prev_term != prev_term {
            break;
        }
        prev_lsn = lsn;
        prev_term = header.term;
        out.push((lsn, frame));
    }
    out
}

/// Whether the replica took it, which the caller needs: a snapshot carries our log, so the send
/// cursor moves with it, and only an accepted resync has earned that.
async fn trigger_resync(state: &AppState, replica_url: &str, collection: &str) -> bool {
    let url = format!("{}/internal/resync", replica_url);
    let body = ResyncRequest { collection: collection.to_string() };
    match state.client.post(&url).json(&body).send().await {
        Ok(r) if r.status().is_success() => {
            state.metrics.note_resync();
            info!(target: "repair", "Triggered snapshot resync on {} for '{}'", replica_url, collection);
            true
        },
        Ok(r) => {
            warn!(target: "repair", "Resync trigger on {} returned {}", replica_url, r.status());
            false
        },
        Err(e) => {
            warn!(target: "repair", "Resync trigger on {} failed: {}", replica_url, e);
            false
        },
    }
}

/// Asks a replica to confirm it holds our tail by re-sending it: `matched` is cleared at promotion and
/// a snapshot install acks nothing, so a leader can hold a committed tail it cannot prove (C30).
async fn confirm_tail(state: &AppState, replica_url: &str, collection: &str) -> bool {
    let db = match state.db.as_ref() {
        Some(d) => d.clone(),
        None => return false,
    };
    let col = match db.get_collection(collection) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let tail = col.last_appended_lsn();
    if tail == 0 {
        return false;
    }

    // The one frame at the tail: the range excludes everything below it, whatever the LSN spacing.
    let scan = col.clone();
    let frames = match tokio::task::spawn_blocking(move || scan.read_frames_after(tail - 1, tail)).await {
        Ok(Ok(f)) => f,
        _ => return false,
    };
    // Retired under us, so the tail has moved; the next tick reads the new one.
    let (lsn, frame) = match frames.into_iter().next_back() {
        Some(f) => f,
        None => return false,
    };
    let header = match FrameHeader::parse(&frame) {
        Some(h) => h,
        None => return false,
    };

    let term = state.current_term();
    let req = ReplicateRequest {
        collection: collection.to_string(),
        term,
        lsn,
        prev_lsn: header.prev_lsn,
        commit_index: Some(state.committed_lsn(collection)),
        wal_frame: frame,
        frames: Vec::new(),
    };
    let url = format!("{}/internal/replicate", replica_url);
    let _slot = state.replication_slots.acquire().await;
    match state.client.post(&url).json(&req).send().await {
        Ok(r) if r.status().is_success() => {
            let body = r.json::<serde_json::Value>().await.ok();
            // Never above our own tail: a replica whose log runs past ours is evidence for its own
            // entries, not for one of ours.
            let held = applied_through(&body).unwrap_or(lsn).min(lsn);
            state.note_sent(replica_url, collection, held);
            if let Err(e) = state.note_ack(replica_url, collection, held, term) {
                warn!(target: "replication", error = %e, "Failed to persist local commit watermark");
            }
            true
        },
        Ok(r) if r.status() == StatusCode::CONFLICT => {
            let body = r.json::<serde_json::Value>().await.ok();
            match classify_conflict(&body) {
                ConflictKind::StaleTerm(t) => {
                    demote(state, t).await;
                    false
                },
                // Where the replica really is, which is what the backlog pass needs; it runs on the
                // next tick now that the cursor names a position the replica reported.
                ConflictKind::Divergent { last_lsn, applied } => {
                    state.rewind_replica(replica_url, collection, applied.unwrap_or(last_lsn));
                    false
                },
                ConflictKind::Gap(last_lsn, _) => {
                    state.rewind_replica(replica_url, collection, last_lsn);
                    false
                },
            }
        },
        Ok(r) if r.status() == StatusCode::FORBIDDEN => {
            let body = r.json::<serde_json::Value>().await.ok();
            let their_term = forbidden_term(&body);
            if their_term > term {
                demote(state, their_term).await;
            }
            false
        },
        _ => false,
    }
}

// One streaming pass can be overtaken by new writes, so the driver re-checks the tail. Capped so a
// collection under sustained load cannot pin the task and the lock indefinitely.
const REPAIR_PASSES: usize = 8;

/// At most one repairer per replica; callers that find one running return false and rely on the holder,
/// which re-checks the tail before it exits. Returns whether the replica reached `needed_lsn`.
async fn repair_replica(
    state: AppState,
    replica_url: String,
    collection: String,
    reported_last_lsn: u64,
    reported_last_term: Option<u64>,
    needed_lsn: u64,
) -> bool {
    let lock = repair_lock(&state, &replica_url, &collection);
    let _guard = match lock.try_lock() {
        Ok(g) => g,
        // Coalesced behind a running repair. A caller with no LSN to answer about wanted the work done,
        // and waiting here would serialise every frame of a bulk write behind a full pass (H12).
        Err(_) if needed_lsn == 0 => return false,
        // A write concern does need the answer, and reporting a miss without waiting fails a write a
        // majority holds (H11). A repair that read its target too early never carried this frame.
        Err(_) => {
            let queued = lock.lock().await;
            if state.matched_lsn(&replica_url, &collection) >= needed_lsn {
                return true;
            }
            queued
        },
    };

    let mut after = reported_last_lsn;
    let mut after_term = reported_last_term;

    for _ in 0..REPAIR_PASSES {
        match stream_chain_once(&state, &replica_url, &collection, after, after_term).await {
            Some(reached) if reached > after => {
                after = reached;
                // Past the first pass we are chaining within our own log.
                after_term = None;
            },
            Some(_) => break,
            None => return false,
        }
    }
    // Running out of passes is not evidence of anything: a replica still short of the frame being
    // acked must not count toward its write concern.
    after >= needed_lsn
}

/// Splits a chained run by frame count and raw bytes both. The first frame of a batch is always
/// taken, so one over the budget travels alone rather than never -- not sending it is `H13`.
fn size_bounded_batches(frames: &[(u64, Vec<u8>)]) -> Vec<&[(u64, Vec<u8>)]> {
    let mut batches = Vec::new();
    let mut start = 0;
    while start < frames.len() {
        let mut end = start + 1;
        let mut bytes = frames[start].1.len();
        while end < frames.len()
            && end - start < REPLICATION_BATCH_FRAMES
            && bytes + frames[end].1.len() <= REPLICATION_BATCH_BYTES
        {
            bytes += frames[end].1.len();
            end += 1;
        }
        batches.push(&frames[start..end]);
        start = end;
    }
    batches
}

/// One pass: read what the replica is missing, verify it chains, ship it in batches.
/// `Some(lsn)` is how far it got, `None` means the pass failed and repair should stop.
async fn stream_chain_once(
    state: &AppState,
    replica_url: &str,
    collection: &str,
    reported_last_lsn: u64,
    reported_last_term: Option<u64>,
) -> Option<u64> {
    let replica_url = replica_url.to_string();
    let collection = collection.to_string();
    let state = state.clone();

    let db = state.db.as_ref()?.clone();
    let col = db.get_collection(&collection).ok()?;

    // This collection's tail, not the global commit index: another collection may hold the highest LSN.
    let target = col.last_appended_lsn();

    if reported_last_lsn >= target {
        return Some(reported_last_lsn);
    }

    let col_scan = col.clone();
    let after = reported_last_lsn;
    let frames = match tokio::task::spawn_blocking(move || col_scan.read_frames_after(after, target)).await {
        Ok(Ok(f)) => f,
        _ => return None,
    };

    let after_term = match reported_last_term {
        Some(t) => t,
        None => frames.iter()
            .min_by_key(|(lsn, _)| *lsn)
            .and_then(|(_, f)| FrameHeader::parse(f))
            .map_or(0, |h| h.prev_term),
    };

    let chained = chain_prefix(reported_last_lsn, after_term, frames);
    let reaches_target = chained.last().map_or(false, |(lsn, _)| *lsn >= target);

    if !reaches_target {
        info!(target: "repair", "Replica {} too far behind for '{}' (last_lsn={}, target={}), falling back to snapshot", replica_url, collection, reported_last_lsn, target);
        // The cursor goes with the snapshot, which carries our log through `target`: left below it,
        // the next pass re-reads the same hole and escalates another one, forever (bugs.md C30).
        if trigger_resync(&state, &replica_url, &collection).await {
            state.note_sent(&replica_url, &collection, target);
        }
        return None;
    }

    let term = state.current_term();
    let commit_index = state.committed_lsn(&collection);
    let sent = chained.len();
    let mut prev = reported_last_lsn;

    // Pipelined: one round trip and one remote fsync per batch instead of per frame. The chain is
    // already contiguous here, so the receiver can apply the run without asking for anything else.
    for batch in size_bounded_batches(&chained) {
        let (lsn, head) = match batch.first() {
            Some((lsn, frame)) => (*lsn, frame.clone()),
            None => break,
        };
        let batch_last = batch.last().map_or(lsn, |(l, _)| *l);
        let prev_lsn = FrameHeader::parse(&head).map_or(prev, |h| h.prev_lsn);
        let req = ReplicateRequest {
            collection: collection.clone(),
            term,
            lsn,
            prev_lsn,
            commit_index: Some(commit_index),
            wal_frame: head,
            frames: batch[1..].iter().map(|(_, f)| f.clone()).collect(),
        };
        let url = format!("{}/internal/replicate", replica_url);
        state.metrics.note_batch(batch.len());
        // Released before the next chunk, so a long backfill yields slots instead of holding one
        // for its whole duration.
        let slot = state.replication_slots.acquire().await;
        let response = state.client.post(&url).json(&req).send().await;
        drop(slot);
        match response {
            Ok(r) if r.status().is_success() => {
                let body = r.json::<serde_json::Value>().await.ok();
                let acked = applied_through(&body).unwrap_or(batch_last);
                prev = prev.max(acked);
                state.note_sent(&replica_url, &collection, prev);

                // A peer that predates batching applies only the head and says so. The chunks are
                // cut in advance, so continuing would step over the frames it skipped.
                if acked < batch_last {
                    warn!(target: "repair", "Replica {} accepted only up to lsn {} of a {}-frame batch; \
                        resuming from there", replica_url, acked, batch.len());
                    break;
                }
            },
            Ok(r) if r.status() == StatusCode::CONFLICT => {
                let body = r.json::<serde_json::Value>().await.ok();
                match classify_conflict(&body) {
                    ConflictKind::StaleTerm(t) => {
                        warn!(target: "repair", "Replica {} reports higher term {} during backfill; demoting", replica_url, t);
                        demote(&state, t).await;
                        return None;
                    },
                    // We already resumed from a point the replica named; diverging from that one
                    // leaves nothing above its watermark to back up to.
                    ConflictKind::Divergent { last_lsn, .. } => {
                        warn!(target: "repair", "Replica {} diverges from us at lsn {} (its last_lsn={}) during backfill; snapshotting it", replica_url, lsn, last_lsn);
                        state.rewind_replica(&replica_url, &collection, last_lsn);
                        if trigger_resync(&state, &replica_url, &collection).await {
                            state.note_sent(&replica_url, &collection, target);
                        }
                        return None;
                    },
                    ConflictKind::Gap(last_lsn, _) => {
                        warn!(target: "repair", "Replica {} still gapped during backfill at lsn {}, falling back to snapshot", replica_url, lsn);
                        state.rewind_replica(&replica_url, &collection, last_lsn);
                        if trigger_resync(&state, &replica_url, &collection).await {
                            state.note_sent(&replica_url, &collection, target);
                        }
                        return None;
                    }
                }
            },
            Ok(r) if r.status() == StatusCode::FORBIDDEN => {
                let body = r.json::<serde_json::Value>().await.ok();
                let their_term = forbidden_term(&body);
                if their_term > term {
                    warn!(target: "repair", "Replica {} rejected us with higher term {}; demoting", replica_url, their_term);
                    demote(&state, their_term).await;
                }
                return None;
            },
            // A refusal that is not a conflict refuses the same frames on every later pass, so
            // only a snapshot gets past it. 503 is a snapshot already installing there.
            Ok(r) => {
                let status = r.status();
                warn!(target: "repair", "Replica {} returned {} during backfill", replica_url, status);
                if status != StatusCode::SERVICE_UNAVAILABLE {
                    trigger_resync(&state, &replica_url, &collection).await;
                }
                return None;
            },
            Err(e) => {
                warn!(target: "repair", "Replica {} unreachable during backfill: {}", replica_url, e);
                return None;
            }
        }
    }

    if let Err(e) = state.note_ack(&replica_url, &collection, prev, term) {
        warn!(target: "replication", error = %e, "Failed to persist local commit watermark");
    }
    info!(target: "repair", "Streamed {} frames to {}; caught up to lsn {} for '{}'", sent, replica_url, prev, collection);
    Some(prev)
}

/// Leader-driven: a cursor short of this frame's predecessor means the replica is behind, so stream
/// from the cursor. `Some(caught_up)` handled it here; `None` means send the frame normally.
async fn stream_backlog_if_behind(
    state: &AppState,
    replica_url: &str,
    collection: &str,
    prev_lsn: u64,
    needed_lsn: u64,
) -> Option<bool> {
    match state.sent_through(replica_url, collection) {
        Some(cursor) if cursor < prev_lsn => Some(
            repair_replica(state.clone(), replica_url.to_string(), collection.to_string(), cursor,
                None, needed_lsn).await,
        ),
        _ => None,
    }
}

async fn replicate_one_await(
    state: &AppState,
    replica_url: &str,
    collection: &str,
    frame: &[u8],
    term: u64,
    commit_index: u64,
    lsn: u64,
    prev_lsn: u64,
) -> bool {
    if let Some(caught_up) = stream_backlog_if_behind(state, replica_url, collection, prev_lsn, lsn).await {
        return caught_up;
    }

    let url = format!("{}/internal/replicate", replica_url);
    let req = ReplicateRequest {
        collection: collection.to_string(),
        term,
        lsn,
        prev_lsn,
        commit_index: Some(commit_index),
        wal_frame: frame.to_vec(),
        frames: Vec::new(),
    };
    let _slot = state.replication_slots.acquire().await;
    match state.client.post(&url).json(&req).send().await {
        Ok(r) if r.status().is_success() => {
            state.note_sent(replica_url, collection, lsn);
            if let Err(e) = state.note_ack(replica_url, collection, lsn, term) {
                warn!(target: "replication", error = %e, "Failed to persist local commit watermark");
            }
            true
        },
        Ok(r) if r.status() == StatusCode::CONFLICT => {
            let body = r.json::<serde_json::Value>().await.ok();
            match classify_conflict(&body) {
                ConflictKind::StaleTerm(t) => {
                    demote(state, t).await;
                    false
                },
                ConflictKind::Divergent { last_lsn, applied } => {
                    state.metrics.note_divergence();
                    match applied {
                        Some(watermark) => {
                            warn!(target: "replication", "Replica {} diverges from us at lsn {} (its last_lsn={}); resuming from its watermark {}", replica_url, lsn, last_lsn, watermark);
                            state.rewind_replica(replica_url, collection, watermark);
                            repair_replica(state.clone(), replica_url.to_string(), collection.to_string(),
                                watermark, None, lsn).await
                        },
                        None => {
                            warn!(target: "replication", "Replica {} diverges from us at lsn {} (its last_lsn={}) and reports no watermark; snapshotting it", replica_url, lsn, last_lsn);
                            state.rewind_replica(replica_url, collection, last_lsn);
                            if trigger_resync(state, replica_url, collection).await {
                                state.note_sent(replica_url, collection, lsn);
                            }
                            false
                        },
                    }
                },
                ConflictKind::Gap(last_lsn, last_term) => {
                    state.metrics.note_gap();
                    state.rewind_replica(replica_url, collection, last_lsn);
                    repair_replica(state.clone(), replica_url.to_string(), collection.to_string(), last_lsn,
                        Some(last_term), lsn).await
                }
            }
        },
        Ok(r) if r.status() == StatusCode::FORBIDDEN => {
            let body = r.json::<serde_json::Value>().await.ok();
            let their_term = forbidden_term(&body);
            if their_term > term {
                demote(state, their_term).await;
            }
            false
        },
        _ => false,
    }
}

/// Returns who holds the frame: this node first, then every voter that acknowledged. Identities
/// rather than a count, because a joint quorum cannot be decided from one.
pub async fn replicate_and_await(
    state: AppState,
    collection: String,
    frame: Vec<u8>,
    term: u64,
    commit_index: u64,
    lsn: u64,
    prev_lsn: u64,
    quorum: WriteQuorum,
    timeout: Duration,
) -> Vec<String> {
    let own = state.own_url();
    let replicas = state.replication_targets();
    if replicas.is_empty() || quorum.met(std::slice::from_ref(&own)) {
        replicate_to_peers(state, collection, frame, term, commit_index, lsn, prev_lsn);
        return vec![own];
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<String>>(replicas.len());
    for replica_url in replicas {
        // Learners are shipped the frame but report nothing: counting them would let a write concern
        // be met by nodes outside the quorum, and an election without them would lose their entries.
        let counts = state.is_voting_replica(&replica_url);
        let state = state.clone();
        let col = collection.clone();
        let frame = frame.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let ok = replicate_one_await(&state, &replica_url, &col, &frame, term, commit_index, lsn, prev_lsn).await;
            let _ = tx.send((ok && counts).then_some(replica_url)).await;
        });
    }
    drop(tx);

    let holders = Arc::new(std::sync::Mutex::new(vec![own]));
    let holders_inner = holders.clone();
    let _ = tokio::time::timeout(timeout, async move {
        loop {
            if quorum.met(&holders_inner.lock().unwrap()) {
                return;
            }
            match rx.recv().await {
                Some(Some(replica)) => holders_inner.lock().unwrap().push(replica),
                Some(None) => {},
                None => break,
            }
        }
    }).await;

    let out = holders.lock().unwrap().clone();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Retention;
    use crate::storage::{Database, ReplicaApply};
    use crate::test_support::{
        get_raw, live_put, make_frame, next_test_port, put_doc_at, put_doc_http, put_value,
        read_doc_http, temp_root, three_node_cluster, wait_for, wait_for_doc, TestNode,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn repair_state(root: &std::path::Path) -> AppState {
        let config = serde_json::from_value(serde_json::json!({
            "node_id": "n1", "role": "shard", "shard_role": "primary",
            "listen_addr": "127.0.0.1:1", "data_dir": root.to_string_lossy(),
        })).unwrap();
        let db = Arc::new(Database::new(root).unwrap());
        AppState::for_admission_test(config, db, true)
    }

    /// H5: one lock per replica meant a second collection's repair `try_lock`ed, failed, and
    /// returned false without sending anything — scored as a miss and paid for with backoff.
    #[tokio::test]
    async fn repairs_to_one_replica_do_not_block_each_other_across_collections() {
        let root = temp_root();
        let state = repair_state(&root);
        let replica = "http://127.0.0.1:9502";

        let users = repair_lock(&state, replica, "users");
        let orders = repair_lock(&state, replica, "orders");

        let _held = users.try_lock().expect("first repair takes its own lock");
        assert!(orders.try_lock().is_ok(),
            "a repair on another collection must not wait behind this one: they are separate logs");
        assert!(users.try_lock().is_err(),
            "but two repairs of the same log must still coalesce rather than duplicate the stream");

        // repair_locks is a shared namespace; migration cleanup prunes it by prefix.
        assert!(repair_lock(&state, replica, "users").try_lock().is_err(),
            "the same pair must resolve to the same lock, not a fresh one each call");
        let keys: Vec<String> = state.repair_locks.lock().unwrap().keys().cloned().collect();
        assert!(keys.iter().all(|k| k.starts_with("repair:")),
            "repair keys must stay in their own namespace: {:?}", keys);
    }

    #[test]
    fn a_replica_is_idle_only_when_none_of_its_collections_moved() {
        let mixed = score_replicas(vec![
            ("http://r1".into(), false),
            ("http://r1".into(), true),
            ("http://r1".into(), false),
        ]);
        assert_eq!(mixed, vec![("http://r1".to_string(), true)],
            "one round is one verdict; three work items counted as three misses before");

        let stalled = score_replicas(vec![("http://r1".into(), false), ("http://r1".into(), false)]);
        assert_eq!(stalled, vec![("http://r1".to_string(), false)],
            "a replica making no progress anywhere must still back off");

        let two = score_replicas(vec![("http://r1".into(), true), ("http://r2".into(), false)]);
        assert_eq!(two, vec![("http://r1".to_string(), true), ("http://r2".to_string(), false)]);
    }

    // A peer that applies the head of a batch and reports only that: the case the batch loop's
    // early break exists for.
    async fn head_only_apply(
        axum::extract::State(hits): axum::extract::State<Arc<AtomicUsize>>,
        axum::Json(req): axum::Json<ReplicateRequest>,
    ) -> axum::Json<serde_json::Value> {
        hits.fetch_add(1, Ordering::SeqCst);
        axum::Json(serde_json::json!({"status": "applied", "lsn": req.lsn}))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replica_that_never_catches_up_is_not_acked() {
        let root = temp_root();
        let hits = Arc::new(AtomicUsize::new(0));
        let stub_port = next_test_port();
        let stub = format!("http://127.0.0.1:{}", stub_port);

        let app = axum::Router::new()
            .route("/internal/replicate", axum::routing::post(head_only_apply))
            .with_state(hits.clone());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", stub_port)).await.unwrap();
        tokio::spawn(async move { let _ = axum::serve(listener, app).await; });

        // Left out of the config on purpose: nothing ships here on its own, so the repair below is
        // the only traffic and the repair lock is uncontended.
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        for i in 1..=20 {
            assert!(put_doc_http(&client, &leader.url(), &format!("k{}", i), i).await.is_success());
        }
        let tail = state.db.as_ref().unwrap().get_collection("t").unwrap().last_appended_lsn();
        assert_eq!(tail, 20);

        let acked = repair_replica(state.clone(), stub, "t".to_string(), 0, None, tail).await;
        let passes = hits.load(Ordering::SeqCst);

        assert_eq!(passes, REPAIR_PASSES,
            "the repair should have spent every pass advancing one frame at a time");
        assert!(!acked,
            "the replica stopped at lsn {} of {}, but exhausting the passes reported it caught up \
             and that boolean is the ack this frame counts toward w=majority with",
            passes, tail);

        leader.kill();
    }
    /// Refuses anything that does not resume from its watermark, and counts the resyncs it is
    /// asked for. A real replica's answer to the same frames, with the truncation left out.
    #[derive(Default)]
    struct DivergentStub {
        prev_lsns: std::sync::Mutex<Vec<u64>>,
        resyncs: AtomicUsize,
    }

    const STUB_WATERMARK: u64 = 3;

    async fn diverge_below_watermark(
        axum::extract::State(stub): axum::extract::State<Arc<DivergentStub>>,
        axum::Json(req): axum::Json<ReplicateRequest>,
    ) -> axum::response::Response {
        use axum::response::IntoResponse;
        stub.prev_lsns.lock().unwrap().push(req.prev_lsn);
        if req.prev_lsn == STUB_WATERMARK {
            return (axum::http::StatusCode::OK,
                axum::Json(serde_json::json!({"status": "applied"}))).into_response();
        }
        (axum::http::StatusCode::CONFLICT, axum::Json(serde_json::json!({
            "status": "divergent",
            "last_lsn": 7,
            "last_term": 1,
            "applied": STUB_WATERMARK,
        }))).into_response()
    }

    async fn count_resync(
        axum::extract::State(stub): axum::extract::State<Arc<DivergentStub>>,
    ) -> axum::http::StatusCode {
        stub.resyncs.fetch_add(1, Ordering::SeqCst);
        axum::http::StatusCode::OK
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_diverged_replica_is_backed_up_to_rather_than_snapshotted() {
        let root = temp_root();
        let stub = Arc::new(DivergentStub::default());
        let stub_port = next_test_port();
        let stub_url = format!("http://127.0.0.1:{}", stub_port);

        let app = axum::Router::new()
            .route("/internal/replicate", axum::routing::post(diverge_below_watermark))
            .route("/internal/resync", axum::routing::post(count_resync))
            .with_state(stub.clone());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", stub_port)).await.unwrap();
        tokio::spawn(async move { let _ = axum::serve(listener, app).await; });

        // Not in the config, so nothing ships here on its own and this frame is the only traffic.
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        for i in 1..=7 {
            assert!(put_doc_http(&client, &leader.url(), &format!("k{}", i), i).await.is_success());
        }

        let col = state.db.as_ref().unwrap().get_collection("t").unwrap();
        let tail = col.last_appended_lsn();
        let (lsn, frame) = col.read_frames_after(0, tail).unwrap().pop().unwrap();
        let prev_lsn = FrameHeader::parse(&frame).unwrap().prev_lsn;

        let held = replicate_one_await(&state, &stub_url, "t", &frame, state.current_term(),
            tail, lsn, prev_lsn).await;

        let seen = stub.prev_lsns.lock().unwrap().clone();
        assert!(held, "the replica holds the frame once the backed-up stream lands; saw {:?}", seen);
        assert_eq!(seen.first(), Some(&prev_lsn), "the first attempt is the ordinary send");
        assert!(seen.contains(&STUB_WATERMARK),
            "the refusal names a watermark and the leader has to resume from it, not from its own              cursor; saw {:?}", seen);

        let (_, divergences, resyncs) = state.metrics.repair_counts();
        assert_eq!(divergences, 1);
        assert_eq!(stub.resyncs.load(Ordering::SeqCst), 0);
        assert_eq!(resyncs, 0,
            "a divergence above the replica's watermark costs a backfill, not a whole collection");

        leader.kill();
    }

    /// Answers every batch with one fixed status, and counts the resyncs it is asked for.
    struct RefusingStub {
        status: StatusCode,
        resyncs: AtomicUsize,
    }

    async fn refuse_batch(
        axum::extract::State(stub): axum::extract::State<Arc<RefusingStub>>,
    ) -> StatusCode {
        stub.status
    }

    async fn count_stub_resync(
        axum::extract::State(stub): axum::extract::State<Arc<RefusingStub>>,
    ) -> StatusCode {
        stub.resyncs.fetch_add(1, Ordering::SeqCst);
        StatusCode::OK
    }

    async fn refusing_stub(status: StatusCode) -> (String, Arc<RefusingStub>) {
        let stub = Arc::new(RefusingStub { status, resyncs: AtomicUsize::new(0) });
        let port = next_test_port();
        let app = axum::Router::new()
            .route("/internal/replicate", axum::routing::post(refuse_batch))
            .route("/internal/resync", axum::routing::post(count_stub_resync))
            .with_state(stub.clone());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
        (format!("http://127.0.0.1:{}", port), stub)
    }

    /// H13: a frame the write path accepts can exceed the replicate body limit, and the refusal is
    /// the same on every later pass, so returning `None` here wedged the collection at one copy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_refused_batch_escalates_to_a_snapshot() {
        let root = temp_root();
        let (too_large, refuser) = refusing_stub(StatusCode::PAYLOAD_TOO_LARGE).await;
        let (installing, installer) = refusing_stub(StatusCode::SERVICE_UNAVAILABLE).await;

        // Not in the config, so the repairs below are the only traffic to either stub.
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        for i in 1..=3 {
            assert!(put_doc_http(&client, &leader.url(), &format!("k{}", i), i).await.is_success());
        }
        let tail = state.db.as_ref().unwrap().get_collection("t").unwrap().last_appended_lsn();

        assert!(!repair_replica(state.clone(), too_large, "t".into(), 0, None, tail).await,
            "a refused batch is not an ack");
        assert_eq!(refuser.resyncs.load(Ordering::SeqCst), 1,
            "no retry changes this answer, so the frames have to travel as a snapshot instead");

        assert!(!repair_replica(state.clone(), installing, "t".into(), 0, None, tail).await);
        assert_eq!(installer.resyncs.load(Ordering::SeqCst), 0,
            "503 is a snapshot already installing there; asking for a second one buys nothing");

        leader.kill();
    }

    fn frames_of(sizes: &[usize]) -> Vec<(u64, Vec<u8>)> {
        sizes.iter().enumerate().map(|(i, n)| (i as u64 + 1, vec![0u8; *n])).collect()
    }

    /// H13: `chunks(REPLICATION_BATCH_FRAMES)` bounded the count and nothing bounded the size, so
    /// 64 frames near the write limit was a request no receiver could accept.
    #[test]
    fn a_batch_is_bounded_by_bytes_as_well_as_by_frames() {
        let tiny = frames_of(&[16; 200]);
        let counts: Vec<usize> = size_bounded_batches(&tiny).iter().map(|b| b.len()).collect();
        assert_eq!(counts, vec![64, 64, 64, 8],
            "small frames must still fill the frame budget of {}", REPLICATION_BATCH_FRAMES);

        let third = REPLICATION_BATCH_BYTES / 3;
        let wide = frames_of(&[third; 7]);
        let batches = size_bounded_batches(&wide);
        assert!(batches.iter().all(|b| b.iter().map(|(_, f)| f.len()).sum::<usize>()
                <= REPLICATION_BATCH_BYTES),
            "no batch may exceed the byte budget, whatever the frame count says");
        assert_eq!(batches.iter().map(|b| b.len()).sum::<usize>(), 7, "every frame is sent once");
        assert_eq!(batches.iter().map(|b| b.len()).collect::<Vec<_>>(), vec![3, 3, 1]);
    }

    /// The other half of the same bound: refusing to send a frame over the budget is the wedge,
    /// so it travels alone and the receiver's limit is sized for exactly one.
    #[test]
    fn a_frame_over_the_byte_budget_travels_alone() {
        let mixed = frames_of(&[16, REPLICATION_BATCH_BYTES + 1, 16]);
        let batches = size_bounded_batches(&mixed);
        assert_eq!(batches.iter().map(|b| b.len()).collect::<Vec<_>>(), vec![1, 1, 1],
            "the oversized frame neither absorbs its neighbours nor is dropped; got {:?}",
            batches.iter().map(|b| b.len()).collect::<Vec<_>>());
        assert_eq!(batches[1][0].0, 2, "it is the frame it was, at its own lsn");
    }

    /// H13 end to end. Before the limits agreed this answered `202 acks=1` and left both followers
    /// without it -- and without every later write, which the chain check put behind it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_document_the_write_path_accepts_replicates_to_a_majority() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::new();

        // Inside MAX_PUBLIC_BODY and above the ~1.5 MB a base64 frame used to fit in a 2 MB body.
        let wide = serde_json::json!({"pad": "x".repeat(1_900 * 1024)});
        let status = put_value(&client, &n1.url(), "t", "wide", wide, "?w=majority&wtimeout=15000").await;
        assert_eq!(status, StatusCode::CREATED,
            "a body the public limit accepts must meet its write concern, not report acks=1");

        // The head-of-line half: a small write after it was stuck behind it forever.
        assert!(put_doc_at(&client, &n1.url(), "t", "after", 1, "?w=majority&wtimeout=15000")
            .await.is_success());

        for follower in [&n2, &n3] {
            let mut held = false;
            let start = std::time::Instant::now();
            while !held && start.elapsed() < Duration::from_secs(15) {
                held = get_raw(&client, &follower.url(), "t", "wide").await;
                if !held {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            assert!(held, "the frame has to reach {} as a frame, not as a snapshot", follower.url());
        }

        let (_, _, resyncs) = n1.state.as_ref().unwrap().metrics.repair_counts();
        assert_eq!(resyncs, 0,
            "the containment half escalated this to a snapshot per repair pass; with the limits              agreeing it costs no snapshot at all");
    }

    #[tokio::test]
    async fn backfill_reads_and_applies_missing_frames() {
        let proot = temp_root();
        let rroot = temp_root();

        let pdb = Database::new(&proot).unwrap();
        let pcol = pdb.get_collection("c").unwrap();
        let mut last = 0;
        for i in 1..=5 {
            last = pcol.put(format!("k{}", i), serde_json::json!({"i": i}), 1).unwrap().3;
        }
        pcol.enqueue_commit().await.unwrap().unwrap();
        pcol.apply_committed(last).unwrap();
        assert_eq!(pdb.durable_lsn.load(Ordering::SeqCst), 5);

        let all = chain_prefix(0, 0, pcol.read_frames_after(0, 5).unwrap());
        let lsns: Vec<u64> = all.iter().map(|(l, _)| *l).collect();
        assert_eq!(lsns, vec![1, 2, 3, 4, 5]);

        let rdb = Database::new(&rroot).unwrap();
        let rcol = rdb.get_collection("c").unwrap();

        for (lsn, fr) in &all[..2] {
            match rcol.append_raw_frame(fr).unwrap() {
                ReplicaApply::Applied { lsn: a, .. } => assert_eq!(a, *lsn),
                other => panic!("expected Applied, got {:?}", other),
            }
        }

        let backfill = chain_prefix(2, 1, pcol.read_frames_after(2, 5).unwrap());
        let bf_lsns: Vec<u64> = backfill.iter().map(|(l, _)| *l).collect();
        assert_eq!(bf_lsns, vec![3, 4, 5]);

        for (lsn, fr) in &backfill {
            match rcol.append_raw_frame(fr).unwrap() {
                ReplicaApply::Applied { lsn: a, .. } => assert_eq!(a, *lsn),
                other => panic!("expected Applied during backfill, got {:?}", other),
            }
        }

        rcol.enqueue_commit().await.unwrap().unwrap();
        rcol.apply_committed(5).unwrap();
        drop(rcol);
        drop(rdb);

        let rdb2 = Database::new(&rroot).unwrap();
        let rcol2 = rdb2.get_collection("c").unwrap();
        for i in 1..=5 {
            assert_eq!(rcol2.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"i": i})));
        }
        assert_eq!(rdb2.durable_lsn.load(Ordering::SeqCst), 5, "replica must reach primary's LSN after backfill");
    }

    #[tokio::test]
    async fn compaction_holes_force_snapshot_fallback() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for v in [1, 2, 3] {
            live_put(&col, "a", v);
        }
        live_put(&col, "b", 9);
        col.enqueue_commit().await.unwrap().unwrap();
        assert_eq!(db.durable_lsn.load(Ordering::SeqCst), 4);

        col.compact(Retention::none()).unwrap();

        let frames = col.read_frames_after(0, 4).unwrap();
        let mut lsns: Vec<u64> = frames.iter().map(|(l, _)| *l).collect();
        lsns.sort();
        assert_eq!(lsns, vec![3, 4], "compaction should drop overwritten lsns 1 and 2");

        let from_zero = chain_prefix(0, 0, col.read_frames_after(0, 4).unwrap());
        assert!(from_zero.is_empty(), "the surviving frames no longer chain onto an empty log -> repair must snapshot");

        let from_two = chain_prefix(2, 1, col.read_frames_after(2, 4).unwrap());
        let two_lsns: Vec<u64> = from_two.iter().map(|(l, _)| *l).collect();
        assert_eq!(two_lsns, vec![3, 4], "a replica already at lsn 2 can still backfill");
    }

    /// M15, the other side of the test above: a replica three frames behind with a retention floor at
    /// its position. Nothing it needs is destroyed, so the repair streams frames instead of snapshotting.
    #[tokio::test]
    async fn a_retained_tail_keeps_the_chain_a_replica_repairs_over() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "a", 1);
        col.enqueue_commit().await.unwrap().unwrap();
        // Where the replica sits: it holds lsn 1 and nothing after it.
        let behind = db.durable_lsn.load(Ordering::SeqCst);

        for v in [2, 3] {
            live_put(&col, "a", v);
        }
        live_put(&col, "b", 9);
        col.enqueue_commit().await.unwrap().unwrap();
        let tip = db.durable_lsn.load(Ordering::SeqCst);

        col.compact(Retention { above_lsn: behind, max_bytes: 1 << 20, min_reclaim_bytes: 0 })
            .unwrap();

        let chained = chain_prefix(behind, 1, col.read_frames_after(behind, tip).unwrap());
        assert_eq!(chained.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
            (behind + 1..=tip).collect::<Vec<_>>(),
            "every frame above the floor has to survive, superseded ones included, or the chain \
             breaks exactly where it did before retention existed");

        assert!(chain_prefix(0, 0, col.read_frames_after(0, tip).unwrap()).is_empty(),
            "and below the floor nothing is promised: that replica still snapshots");
        assert_eq!(col.get("a").unwrap().unwrap()["v"], 3, "the live value still reads back");
        assert_eq!(col.get("b").unwrap().unwrap()["v"], 9);
    }

    #[test]
    fn chain_prefix_stops_where_the_chain_breaks() {
        let f1 = make_frame(1, 1, 0, 0, "k1", 1);
        let f2 = make_frame(1, 4, 1, 1, "k2", 2);
        let f3 = make_frame(1, 9, 4, 1, "k3", 3);

        let sparse = vec![(1u64, f1.clone()), (4, f2.clone()), (9, f3.clone())];
        let all = chain_prefix(0, 0, sparse.clone());
        assert_eq!(all.iter().map(|(l, _)| *l).collect::<Vec<_>>(), vec![1, 4, 9],
            "sparse LSNs still chain; only the predecessor links matter");

        let from_middle = chain_prefix(4, 1, vec![(9, f3.clone())]);
        assert_eq!(from_middle.len(), 1, "a replica sitting at lsn 4 can be streamed lsn 9");

        let hole = chain_prefix(0, 0, vec![(4, f2.clone()), (9, f3.clone())]);
        assert!(hole.is_empty(), "lsn 4 names lsn 1 as its predecessor, which an empty log does not have");

        let wrong_term = chain_prefix(4, 2, vec![(9, f3)]);
        assert!(wrong_term.is_empty(), "matching lsn but mismatched term must not chain");

        let truncated = chain_prefix(0, 0, vec![(1, f1), (9, make_frame(1, 9, 5, 1, "k3", 3))]);
        assert_eq!(truncated.iter().map(|(l, _)| *l).collect::<Vec<_>>(), vec![1],
            "the run stops at the first frame whose predecessor is missing");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn interleaved_collection_writes_replicate_without_repair_traffic() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        let sync = "?w=all&wtimeout=4000";

        for i in 0..5 {
            let k = format!("k{}", i);
            assert!(put_doc_at(&client, &n1.url(), "alpha", &k, i, sync).await.is_success(),
                "write {} to alpha must be accepted", i);
            assert!(put_doc_at(&client, &n1.url(), "beta", &k, 100 + i, sync).await.is_success(),
                "write {} to beta must be accepted", i);
        }

        for replica in [&n2, &n3] {
            for i in 0..5 {
                let k = format!("k{}", i);
                assert!(wait_for_doc(&client, &replica.url(), "alpha", &k, i, Duration::from_secs(10)).await,
                    "{} never received alpha/{}", replica.node_id, k);
                assert!(wait_for_doc(&client, &replica.url(), "beta", &k, 100 + i, Duration::from_secs(10)).await,
                    "{} never received beta/{}", replica.node_id, k);
            }
        }

        let (gaps, divergences, resyncs) = n1.state.as_ref().unwrap().metrics.repair_counts();
        assert_eq!((gaps, divergences, resyncs), (0, 0, 0),
            "alternating writes between two collections must replicate directly; \
             chaining on lsn-1 instead of the collection's own predecessor makes \
             every second write look like a gap and drags in a full snapshot resync");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_returning_replica_is_streamed_its_backlog_without_reporting_a_gap() {
        let root = temp_root();
        let (n1, _n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "k0", 0, "?w=all&wtimeout=4000").await.is_success());
        assert!(wait_for_doc(&client, &n3.url(), "t", "k0", 0, Duration::from_secs(10)).await);

        let (gaps_before, _, _) = n1.state.as_ref().unwrap().metrics.repair_counts();

        n3.kill();
        for i in 1..5 {
            assert!(put_doc_at(&client, &n1.url(), "t", &format!("k{}", i), i, "?w=majority&wtimeout=4000")
                .await.is_success(), "the remaining majority must keep accepting writes");
        }

        n3.start();
        // The cursor stalled at k0 while n3 was gone, so this write is what the leader notices on.
        assert!(put_doc_at(&client, &n1.url(), "t", "k9", 9, "?w=majority&wtimeout=4000").await.is_success());

        for i in 1..5 {
            assert!(wait_for_doc(&client, &n3.url(), "t", &format!("k{}", i), i, Duration::from_secs(15)).await,
                "the backlogged write k{} must reach the returning replica", i);
        }
        assert!(wait_for_doc(&client, &n3.url(), "t", "k9", 9, Duration::from_secs(15)).await);

        let (gaps_after, _, resyncs) = n1.state.as_ref().unwrap().metrics.repair_counts();
        assert_eq!(gaps_after, gaps_before,
            "the leader tracks how far behind each replica is, so it streams the backlog directly; \
             needing a gap report first means the cursor was not consulted");
        assert_eq!(resyncs, 0, "a short backlog must never escalate to a full snapshot");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_backlog_is_shipped_in_batches_not_one_frame_per_round_trip() {
        let root = temp_root();
        let (n1, _n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "k0", 0, "?w=all&wtimeout=4000").await.is_success());
        assert!(wait_for_doc(&client, &n3.url(), "t", "k0", 0, Duration::from_secs(10)).await);

        n3.kill();
        let backlog = 20;
        for i in 1..=backlog {
            assert!(put_doc_at(&client, &n1.url(), "t", &format!("k{}", i), i, "?w=majority&wtimeout=4000")
                .await.is_success());
        }

        n3.start();
        assert!(put_doc_at(&client, &n1.url(), "t", "k99", 99, "?w=majority&wtimeout=4000").await.is_success());
        assert!(wait_for_doc(&client, &n3.url(), "t", &format!("k{}", backlog), backlog, Duration::from_secs(15)).await,
            "the whole backlog must land");
        assert!(wait_for_doc(&client, &n3.url(), "t", "k99", 99, Duration::from_secs(15)).await);

        let metrics = &n1.state.as_ref().unwrap().metrics;
        let (batches, frames) = metrics.batch_counts();
        let widest = metrics.max_batch_frames();

        assert!(widest > 1,
            "a {}-frame backlog must ship as a batch, but the widest request carried {} frame(s): \
             one frame per round trip is the unpipelined path",
            backlog, widest);
        assert!(widest >= (backlog as u64) / 2,
            "the backlog fits inside one {}-frame batch, so the widest request should carry most of \
             it; {} frames means the stream is being cut short", REPLICATION_BATCH_FRAMES, widest);
        assert!(frames > batches,
            "{} frames over {} requests averages one per round trip", frames, batches);
    }

    #[tokio::test]
    async fn a_batch_applies_in_order_with_one_sync_and_stops_at_a_refusal() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("t").unwrap();

        // Contiguous chain, then a frame whose predecessor is never sent.
        let f1 = make_frame(1, 1, 0, 0, "a", 1);
        let f2 = make_frame(1, 2, 1, 1, "b", 2);
        let f3 = make_frame(1, 3, 2, 1, "c", 3);
        let orphan = make_frame(1, 9, 8, 1, "z", 9);

        for f in [&f1, &f2, &f3] {
            assert!(matches!(col.append_raw_frame(f).unwrap(), ReplicaApply::Applied { .. }));
        }
        assert_eq!(col.last_appended_lsn(), 3, "the contiguous run applies in order");

        assert!(matches!(col.append_raw_frame(&f2).unwrap(), ReplicaApply::Duplicate { .. }),
            "a frame already held must report duplicate, not stall the rest of a batch");

        match col.append_raw_frame(&orphan).unwrap() {
            ReplicaApply::Gap { last_lsn, .. } => assert_eq!(last_lsn, 3,
                "the refusal reports our real tail, which is where the leader resumes"),
            other => panic!("expected Gap, got {:?}", other),
        }
        assert_eq!(col.last_appended_lsn(), 3, "a refused frame must not advance the tail");
    }

    #[test]
    fn the_driver_backs_off_on_a_dead_replica_but_never_gives_up() {
        assert_eq!(drive_backoff_ticks(0), 0);
        assert_eq!(drive_backoff_ticks(1), 0, "a miss or two is normal under load");
        assert_eq!(drive_backoff_ticks(2), 1);
        assert_eq!(drive_backoff_ticks(3), 2, "the interval widens as the replica stays silent");
        assert_eq!(drive_backoff_ticks(6), 16);

        let cap = drive_backoff_ticks(u32::MAX);
        assert_eq!(cap, 1 << DRIVE_BACKOFF_SHIFT_CAP,
            "the wait must stay bounded, or a long-dead replica would effectively never be retried");
        assert!(cap <= 16, "a returning replica on an idle cluster waits at most {} ticks", cap);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replica_catches_up_on_an_idle_cluster_with_no_further_writes() {
        let root = temp_root();
        let (n1, _n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "k0", 0, "?w=all&wtimeout=4000").await.is_success());
        assert!(wait_for_doc(&client, &n3.url(), "t", "k0", 0, Duration::from_secs(10)).await);

        n3.kill();
        for i in 1..=6 {
            assert!(put_doc_at(&client, &n1.url(), "t", &format!("k{}", i), i, "?w=majority&wtimeout=4000")
                .await.is_success());
        }

        // Long enough that every write-triggered send has already failed and returned, so the
        // periodic driver is the only thing left that can close the gap.
        tokio::time::sleep(Duration::from_secs(3)).await;
        // Deliberately no write after this point.
        n3.start();

        for i in 1..=6 {
            assert!(wait_for_doc(&client, &n3.url(), "t", &format!("k{}", i), i, Duration::from_secs(15)).await,
                "k{} must reach the returning replica without a write to trigger it; replication that \
                 only runs on client traffic leaves an idle cluster permanently diverged", i);
        }
    }

    /// IB-055: a collection first written while a peer was unreachable leaves the leader no send
    /// cursor for it, and the drive task skipped every collection it held none for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_collection_whose_first_write_a_peer_missed_still_reaches_it() {
        let root = temp_root();
        let (n1, _n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "k0", 0, "?w=all&wtimeout=4000").await.is_success());
        assert!(wait_for_doc(&client, &n3.url(), "t", "k0", 0, Duration::from_secs(10)).await);

        n3.kill();
        // 'late' is created by a write n3 cannot receive, so nothing ever seeds its cursor: not
        // promotion, which ran before the collection existed, and not this send, which fails.
        assert!(put_doc_at(&client, &n1.url(), "late", "k1", 1, "?w=majority&wtimeout=4000")
            .await.is_success());

        tokio::time::sleep(Duration::from_secs(3)).await;
        // Deliberately no write after this point, to either collection.
        n3.start();

        assert!(wait_for_doc(&client, &n3.url(), "late", "k1", 1, Duration::from_secs(20)).await,
            "the driver has to bootstrap a missing cursor; unfixed it skips 'late' forever and the \
             document arrives only if a client writes to that collection again");
    }

    /// C30: a snapshot install produces no ack, so the leader held `matched = 0` for a node holding
    /// everything and re-escalated the same snapshot every tick, leaving its own tail staged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn a_snapshot_install_leaves_the_leader_a_position_it_can_count() {
        let root = temp_root();
        let (n1, _n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "k0", 0, "?w=all&wtimeout=4000").await.is_success());
        assert!(wait_for_doc(&client, &n3.url(), "t", "k0", 0, Duration::from_secs(10)).await);

        n3.kill();
        // Overwrites, because compaction drops a superseded frame and relocates a live one: a run
        // of distinct keys survives it chained, and the returning replica catches up from the WAL.
        for v in 1..=8 {
            assert!(put_doc_at(&client, &n1.url(), "t", "hot", v, "?w=majority&wtimeout=4000")
                .await.is_success(), "the surviving majority must keep accepting writes");
        }

        let col = n1.state.as_ref().unwrap().db.as_ref().unwrap().get_collection("t").unwrap();
        assert!(wait_for(Duration::from_secs(10), || col.pending_len() == 0).await,
            "compaction refuses while anything is still uncommitted");
        col.compact(Retention::none()).expect("compaction");

        // Deliberately no write after this point: where n3 has got to is the leader's to find out.
        n3.start();
        let tail = col.last_appended_lsn();
        let leader = n1.state.as_ref().unwrap();
        let counted = wait_for(Duration::from_secs(30),
            || leader.matched_lsn(&n3.url(), "t") >= tail).await;
        assert!(counted,
            "unfixed the leader snapshots {} forever without ever counting it: matched={} against \
             its own tail {}", n3.url(), leader.matched_lsn(&n3.url(), "t"), tail);
    }

    /// H11: concurrent writes to one collection all take the repair path, each cursor behind its own
    /// predecessor. All but the first coalesced and reported a miss, so a held frame came back 202.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn concurrent_writes_are_not_reported_as_missing_a_quorum_that_held() {
        let root = temp_root();
        let (n1, _n2, _n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "warm", 0, "?w=majority&wtimeout=4000").await.is_success());

        let statuses = futures::future::join_all((1..=12).map(|i| {
            let (c, base) = (client.clone(), n1.url());
            async move {
                put_doc_at(&c, &base, "t", &format!("k{}", i), i, "?w=majority&wtimeout=4000").await
            }
        })).await;

        let staged: Vec<_> = statuses.iter().enumerate()
            .filter(|(_, s)| **s == StatusCode::ACCEPTED)
            .map(|(i, _)| i + 1)
            .collect();
        assert!(staged.is_empty(),
            "a healthy three-node cluster met no quorum for concurrent writes {:?}: {:?}",
            staged, statuses);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_send_is_retried_rather_than_lost() {
        let root = temp_root();
        let (n1, _n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "k0", 0, "?w=all&wtimeout=4000").await.is_success());
        assert!(wait_for_doc(&client, &n3.url(), "t", "k0", 0, Duration::from_secs(10)).await);

        // One w=1 write while n3 is unreachable: its single delivery attempt is guaranteed to fail.
        n3.kill();
        assert_eq!(put_doc_http(&client, &n1.url(), "solo", 42).await, StatusCode::CREATED);
        tokio::time::sleep(Duration::from_secs(3)).await;
        n3.start();

        assert!(wait_for_doc(&client, &n3.url(), "t", "solo", 42, Duration::from_secs(15)).await,
            "a write whose only send attempt failed must still be delivered; without a retry it is \
             lost on that replica until unrelated traffic happens to arrive");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_quorum_acknowledgement_advances_the_commit_index() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        let leader = n1.state.as_ref().unwrap();
        assert_eq!(leader.committed_lsn("t"), 0, "nothing is committed before the first write");

        assert!(put_doc_at(&client, &n1.url(), "t", "k1", 1, "?w=all&wtimeout=4000").await.is_success());

        let tail = leader.db.as_ref().unwrap()
            .get_collection("t").unwrap().last_appended_lsn();
        assert!(tail > 0);
        assert_eq!(leader.committed_lsn("t"), tail,
            "an entry both replicas acknowledged must be committed");
        assert_eq!(leader.max_committed_lsn(), tail);

        drop(n2);
        drop(n3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_entry_only_the_leader_holds_is_never_committed() {
        let root = temp_root();
        let (n1, mut n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "k1", 1, "?w=all&wtimeout=4000").await.is_success());
        let committed_before = n1.state.as_ref().unwrap().committed_lsn("t");
        assert!(committed_before > 0, "the first write reached a quorum");

        n2.kill();
        n3.kill();

        assert_eq!(put_doc_http(&client, &n1.url(), "k2", 2).await, StatusCode::CREATED,
            "the leader still accepts a w=1 write with no reachable replica");
        tokio::time::sleep(Duration::from_millis(800)).await;

        let leader = n1.state.as_ref().unwrap();
        let tail = leader.db.as_ref().unwrap()
            .get_collection("t").unwrap().last_appended_lsn();
        assert!(tail > committed_before, "the entry is durable on the leader");
        assert_eq!(leader.committed_lsn("t"), committed_before,
            "one node of three is not a quorum, so the entry stays uncommitted;              a watermark driven by local fsync would have claimed it committed");
        assert_eq!(read_doc_http(&client, &n1.url(), "k2").await, None,
            "an uncommitted entry must not be readable: a new leader without it could win              the next election and revoke it");
        assert_eq!(read_doc_http(&client, &n1.url(), "k1").await, Some(1),
            "the committed entry is still served");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn each_collection_commits_on_its_own_acknowledgements() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "alpha", "k", 1, "?w=all&wtimeout=4000").await.is_success());

        let leader = n1.state.as_ref().unwrap();
        let alpha = leader.committed_lsn("alpha");
        assert!(alpha > 0);
        assert_eq!(leader.committed_lsn("beta"), 0,
            "acknowledging alpha says nothing about a collection nobody has written");

        assert!(put_doc_at(&client, &n1.url(), "beta", "k", 2, "?w=all&wtimeout=4000").await.is_success());
        assert!(leader.committed_lsn("beta") > alpha, "beta commits at its own, later LSN");
        assert_eq!(leader.committed_lsn("alpha"), alpha, "alpha's watermark is unchanged");

        drop(n2);
        drop(n3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn followers_publish_the_last_write_of_an_idle_cluster() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        // One write then silence: the frame's commit index reaches replicas only on a later message.
        assert!(put_doc_at(&client, &n1.url(), "t", "only", 7, "?w=majority&wtimeout=4000").await.is_success());

        for replica in [&n2, &n3] {
            assert!(
                wait_for_doc(&client, &replica.url(), "t", "only", 7, Duration::from_secs(10)).await,
                "{} never published the entry; an idle cluster must not strand it", replica.node_id);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_committed_write_is_readable_on_the_leader_immediately() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();

        assert!(put_doc_at(&client, &n1.url(), "t", "k", 3, "?w=majority&wtimeout=4000").await.is_success());
        assert_eq!(read_doc_http(&client, &n1.url(), "k").await, Some(3),
            "w=majority returns only after the quorum ack applied the entry");

        drop(n2);
        drop(n3);
    }
}
