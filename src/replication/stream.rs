//! Outbound replication and gap repair.

use super::protocol::{
    applied_through, classify_conflict, ConflictKind, ReplicateRequest, ResyncRequest,
};
use crate::consensus::demote;
use crate::replication::protocol::forbidden_term;
use crate::state::AppState;
use crate::storage::FrameHeader;
use axum::http::StatusCode;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

// Bounds one request's size and the receiver's blocking append run, not just the payload.
const REPLICATION_BATCH_FRAMES: usize = 64;

// A couple of misses are normal under load, so backoff only starts after that. The cap keeps a
// long-dead replica polled often enough that it rejoins promptly.
const DRIVE_MISSES_BEFORE_BACKOFF: u32 = 2;
const DRIVE_BACKOFF_SHIFT_CAP: u32 = 4;

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
                        state.note_ack(&replica_url, &col, lsn, term);
                    },
                    Ok(r) if r.status() == StatusCode::CONFLICT => {
                        let body = r.json::<serde_json::Value>().await.ok();
                        match classify_conflict(&body) {
                            ConflictKind::StaleTerm(t) => {
                                warn!(target: "replication", "Replica {} reports higher term {}; demoting", replica_url, t);
                                demote(&state, t).await;
                            },
                            ConflictKind::Divergent(last_lsn) => {
                                state.metrics.note_divergence();
                                warn!(target: "replication", "Replica {} diverges from us at lsn {} (its last_lsn={}); snapshotting it", replica_url, lsn, last_lsn);
                                // The snapshot decides its tail, so the cursor is stale either way.
                                state.rewind_replica(&replica_url, &col, last_lsn);
                                trigger_resync(&state, &replica_url, &col).await;
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

/// Replication was previously only ever started by a write, so a send that failed was never retried
/// and an idle cluster left a lagging replica lagging forever. This makes progress the leader's
/// standing job instead of a side effect of client traffic.
///
/// Safe to fire repeatedly: repair coalesces to one worker per replica and re-checks the tail.
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
            let replicas = state.replication_targets();
            if replicas.is_empty() {
                continue;
            }

            let mut work = Vec::new();
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
                    // No cursor means we have never sent here and have nothing to resume from;
                    // the reactive path still establishes one on the next write.
                    if let Some(cursor) = state.sent_through(replica, &name) {
                        if cursor < tail {
                            work.push((replica.clone(), name.clone(), cursor));
                        }
                    }
                }
            }

            for count in skips.values_mut() {
                *count = count.saturating_sub(1);
            }

            let outcomes = futures::future::join_all(work.into_iter().map(|(replica, name, cursor)| {
                let state = state.clone();
                async move {
                    let before = state.sent_through(&replica, &name).unwrap_or(cursor);
                    let _ = repair_replica(state.clone(), replica.clone(), name.clone(), cursor, None, 0).await;
                    // Judged on whether the cursor moved, not on the return value: a repair that
                    // coalesced behind another worker reports false without anything being wrong.
                    let moved = state.sent_through(&replica, &name).unwrap_or(before) > before;
                    (replica, moved)
                }
            })).await;

            for (replica, moved) in outcomes {
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

async fn trigger_resync(state: &AppState, replica_url: &str, collection: &str) {
    let url = format!("{}/internal/resync", replica_url);
    let body = ResyncRequest { collection: collection.to_string() };
    match state.client.post(&url).json(&body).send().await {
        Ok(r) if r.status().is_success() => {
            state.metrics.note_resync();
            info!(target: "repair", "Triggered snapshot resync on {} for '{}'", replica_url, collection);
        },
        Ok(r) => warn!(target: "repair", "Resync trigger on {} returned {}", replica_url, r.status()),
        Err(e) => warn!(target: "repair", "Resync trigger on {} failed: {}", replica_url, e),
    }
}

// One streaming pass can be overtaken by new writes, so the driver re-checks the tail. Capped so a
// collection under sustained load cannot pin the task and the lock indefinitely.
const REPAIR_PASSES: usize = 8;

/// At most one repairer per replica: a second would re-read the same range and duplicate the work,
/// which is what turned a single backlog into one request per queued write. Callers that find one
/// running return false and rely on the holder, which re-checks the tail before it exits.
///
/// `reported_last_term` is `Some` only when the replica told us its tail term, and then the chain is
/// validated against it so a divergent tail is caught here. `None` means we are driving from our own
/// cursor, where the predecessor term comes from our own next frame's header.
///
/// Returns whether the replica reached `needed_lsn` — its ack for that frame; 0 when discarded.
async fn repair_replica(
    state: AppState,
    replica_url: String,
    collection: String,
    reported_last_lsn: u64,
    reported_last_term: Option<u64>,
    needed_lsn: u64,
) -> bool {
    let lock = {
        let mut locks = state.repair_locks.lock().unwrap();
        locks.entry(replica_url.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    let _guard = match lock.try_lock() {
        Ok(g) => g,
        Err(_) => return false,
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
        trigger_resync(&state, &replica_url, &collection).await;
        return None;
    }

    let term = state.current_term();
    let commit_index = state.committed_lsn(&collection);
    let sent = chained.len();
    let mut prev = reported_last_lsn;

    // Pipelined: one round trip and one remote fsync per batch instead of per frame. The chain is
    // already contiguous here, so the receiver can apply the run without asking for anything else.
    for batch in chained.chunks(REPLICATION_BATCH_FRAMES) {
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
                    ConflictKind::Divergent(last_lsn) => {
                        warn!(target: "repair", "Replica {} diverges from us at lsn {} (its last_lsn={}); snapshotting it", replica_url, lsn, last_lsn);
                        state.rewind_replica(&replica_url, &collection, last_lsn);
                        trigger_resync(&state, &replica_url, &collection).await;
                        return None;
                    },
                    ConflictKind::Gap(last_lsn, _) => {
                        warn!(target: "repair", "Replica {} still gapped during backfill at lsn {}, falling back to snapshot", replica_url, lsn);
                        state.rewind_replica(&replica_url, &collection, last_lsn);
                        trigger_resync(&state, &replica_url, &collection).await;
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
            Ok(r) => {
                warn!(target: "repair", "Replica {} returned {} during backfill", replica_url, r.status());
                return None;
            },
            Err(e) => {
                warn!(target: "repair", "Replica {} unreachable during backfill: {}", replica_url, e);
                return None;
            }
        }
    }

    state.note_ack(&replica_url, &collection, prev, term);
    info!(target: "repair", "Streamed {} frames to {}; caught up to lsn {} for '{}'", sent, replica_url, prev, collection);
    Some(prev)
}

/// Leader-driven: a cursor short of this frame's predecessor means the replica is behind, and
/// sending only the newest frame would buy nothing but a rejection. Streaming from the cursor
/// instead is what makes repair planned rather than a reaction to the replica's complaint.
///
/// `Some(caught_up)` means the backlog was handled here; `None` means send the frame normally.
/// Both send paths route through this, or one of them silently reverts to reactive repair.
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
            state.note_ack(replica_url, collection, lsn, term);
            true
        },
        Ok(r) if r.status() == StatusCode::CONFLICT => {
            let body = r.json::<serde_json::Value>().await.ok();
            match classify_conflict(&body) {
                ConflictKind::StaleTerm(t) => {
                    demote(state, t).await;
                    false
                },
                ConflictKind::Divergent(last_lsn) => {
                    state.metrics.note_divergence();
                    warn!(target: "replication", "Replica {} diverges from us at lsn {} (its last_lsn={}); snapshotting it", replica_url, lsn, last_lsn);
                    state.rewind_replica(replica_url, collection, last_lsn);
                    trigger_resync(state, replica_url, collection).await;
                    false
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

pub async fn replicate_and_await(
    state: AppState,
    collection: String,
    frame: Vec<u8>,
    term: u64,
    commit_index: u64,
    lsn: u64,
    prev_lsn: u64,
    required_acks: usize,
    timeout: Duration,
) -> usize {
    let replicas = state.replication_targets();
    if replicas.is_empty() || required_acks <= 1 {
        replicate_to_peers(state, collection, frame, term, commit_index, lsn, prev_lsn);
        return 1;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<bool>(replicas.len());
    for replica_url in replicas {
        // Learners are shipped the frame on the same path but report `false`: they hold the data
        // and can be promoted later, yet counting them would let a write concern be met by nodes
        // outside the quorum, and a leader elected without them would not have their entries.
        let counts = state.is_voting_replica(&replica_url);
        let state = state.clone();
        let col = collection.clone();
        let frame = frame.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let ok = replicate_one_await(&state, &replica_url, &col, &frame, term, commit_index, lsn, prev_lsn).await;
            let _ = tx.send(ok && counts).await;
        });
    }
    drop(tx);

    let acks = Arc::new(AtomicUsize::new(1));
    let acks_inner = acks.clone();
    let _ = tokio::time::timeout(timeout, async move {
        while acks_inner.load(Ordering::Relaxed) < required_acks {
            match rx.recv().await {
                Some(true) => { acks_inner.fetch_add(1, Ordering::Relaxed); },
                Some(false) => {},
                None => break,
            }
        }
    }).await;

    acks.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Database, ReplicaApply};
    use crate::test_support::{
        idx, make_frame, next_test_port, put_doc_at, put_doc_http, read_doc_http, temp_root,
        three_node_cluster, wait_for_doc, TestNode,
    };
    use std::fs;
    use std::sync::atomic::AtomicUsize;

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
        let _ = fs::remove_dir_all(&root);
    }
    #[tokio::test]
    async fn backfill_reads_and_applies_missing_frames() {
        let proot = temp_root();
        let rroot = temp_root();

        let pdb = Database::new(&proot).unwrap();
        let pcol = pdb.get_collection("c").unwrap();
        for i in 1..=5 {
            let _ = pcol.put(format!("k{}", i), serde_json::json!({"i": i}), 1).unwrap();
        }
        pcol.enqueue_commit().await.unwrap().unwrap();
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
        drop(rcol);
        drop(rdb);

        let rdb2 = Database::new(&rroot).unwrap();
        let rcol2 = rdb2.get_collection("c").unwrap();
        for i in 1..=5 {
            assert_eq!(rcol2.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"i": i})));
        }
        assert_eq!(rdb2.durable_lsn.load(Ordering::SeqCst), 5, "replica must reach primary's LSN after backfill");

        let _ = fs::remove_dir_all(&proot);
        let _ = fs::remove_dir_all(&rroot);
    }

    #[tokio::test]
    async fn compaction_holes_force_snapshot_fallback() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let _ = col.put("a".into(), serde_json::json!({"v": 1}), 1).unwrap();
        let _ = col.put("a".into(), serde_json::json!({"v": 2}), 1).unwrap();
        let (f, w, o, _) = col.put("a".into(), serde_json::json!({"v": 3}), 1).unwrap();
        col.index.write().unwrap().insert("a".into(), idx(&f, w, o));
        let (f2, w2, o2, _) = col.put("b".into(), serde_json::json!({"v": 9}), 1).unwrap();
        col.index.write().unwrap().insert("b".into(), idx(&f2, w2, o2));
        col.enqueue_commit().await.unwrap().unwrap();
        assert_eq!(db.durable_lsn.load(Ordering::SeqCst), 4);

        col.compact().unwrap();

        let frames = col.read_frames_after(0, 4).unwrap();
        let mut lsns: Vec<u64> = frames.iter().map(|(l, _)| *l).collect();
        lsns.sort();
        assert_eq!(lsns, vec![3, 4], "compaction should drop overwritten lsns 1 and 2");

        let from_zero = chain_prefix(0, 0, col.read_frames_after(0, 4).unwrap());
        assert!(from_zero.is_empty(), "the surviving frames no longer chain onto an empty log -> repair must snapshot");

        let from_two = chain_prefix(2, 1, col.read_frames_after(2, 4).unwrap());
        let two_lsns: Vec<u64> = from_two.iter().map(|(l, _)| *l).collect();
        assert_eq!(two_lsns, vec![3, 4], "a replica already at lsn 2 can still backfill");

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
    }
}
