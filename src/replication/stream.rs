//! Outbound replication and gap repair.

use super::protocol::{classify_conflict, ConflictKind, ReplicateRequest, ResyncRequest};
use crate::consensus::demote;
use crate::replication::protocol::forbidden_term;
use crate::state::AppState;
use crate::storage::FrameHeader;
use axum::http::StatusCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

// Fire-and-forget, so frames can arrive out of order and a reported gap is
// normal rather than a fault; repair resolves it.
pub fn replicate_to_peers(
    state: AppState,
    collection: String,
    frame: Vec<u8>,
    term: u64,
    commit_index: u64,
    lsn: u64,
    prev_lsn: u64,
) {
    let replicas = state.get_replicas();
    if replicas.is_empty() {
        return;
    }
    let client = state.client.clone();
    tokio::spawn(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(3));
        let mut handles = Vec::new();
        for replica_url in replicas {
            let client = client.clone();
            let col = collection.clone();
            let frame = frame.clone();
            let sem = semaphore.clone();
            let state = state.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                let url = format!("{}/internal/replicate", replica_url);
                let req_body = ReplicateRequest {
                    collection: col.clone(),
                    term,
                    lsn,
                    prev_lsn,
                    commit_index: Some(commit_index),
                    wal_frame: frame,
                };
                match client.post(&url).json(&req_body).send().await {
                    Ok(r) if r.status().is_success() => {
                        state.metrics.note_replica_ack(&replica_url, lsn);
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
                                trigger_resync(&state, &replica_url, &col).await;
                            },
                            ConflictKind::Gap(last_lsn, last_term) => {
                                state.metrics.note_gap();
                                warn!(target: "replication", "Replica {} gap at lsn {} (its last_lsn={}), starting repair", replica_url, lsn, last_lsn);
                                let _ = repair_replica(state, replica_url.clone(), col, last_lsn, last_term).await;
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

// Repair invariant: frames must form an unbroken predecessor chain from the
// replica's tail. LSNs are sparse per collection, so "next" is not +1, and a
// chain broken by compaction is the signal to fall back to a snapshot.
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

async fn repair_replica(
    state: AppState,
    replica_url: String,
    collection: String,
    reported_last_lsn: u64,
    reported_last_term: u64,
) -> bool {
    let lock = {
        let mut locks = state.repair_locks.lock().unwrap();
        locks.entry(replica_url.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    let _guard = lock.lock().await;

    let db = match state.db.as_ref() {
        Some(d) => d.clone(),
        None => return false,
    };
    let col = match db.get_collection(&collection) {
        Ok(c) => c,
        Err(_) => return false,
    };

    // Catch up to this collection's tail, not the global commit index: another
    // collection may hold the highest LSN, which this one can never reach.
    let target = col.last_appended_lsn();

    if reported_last_lsn >= target {
        return true;
    }

    let col_scan = col.clone();
    let after = reported_last_lsn;
    let frames = match tokio::task::spawn_blocking(move || col_scan.read_frames_after(after, target)).await {
        Ok(Ok(f)) => f,
        _ => return false,
    };

    let chained = chain_prefix(reported_last_lsn, reported_last_term, frames);
    let reaches_target = chained.last().map_or(false, |(lsn, _)| *lsn >= target);

    if !reaches_target {
        info!(target: "repair", "Replica {} too far behind for '{}' (last_lsn={}, target={}), falling back to snapshot", replica_url, collection, reported_last_lsn, target);
        trigger_resync(&state, &replica_url, &collection).await;
        return false;
    }

    let term = state.current_term();
    let commit_index = db.global_commit_index.load(Ordering::SeqCst);
    let sent = chained.len();
    let mut prev = reported_last_lsn;

    for (lsn, frame) in chained {
        let prev_lsn = FrameHeader::parse(&frame).map_or(prev, |h| h.prev_lsn);
        let req = ReplicateRequest {
            collection: collection.clone(),
            term,
            lsn,
            prev_lsn,
            commit_index: Some(commit_index),
            wal_frame: frame,
        };
        let url = format!("{}/internal/replicate", replica_url);
        match state.client.post(&url).json(&req).send().await {
            Ok(r) if r.status().is_success() => {
                prev = lsn;
            },
            Ok(r) if r.status() == StatusCode::CONFLICT => {
                let body = r.json::<serde_json::Value>().await.ok();
                match classify_conflict(&body) {
                    ConflictKind::StaleTerm(t) => {
                        warn!(target: "repair", "Replica {} reports higher term {} during backfill; demoting", replica_url, t);
                        demote(&state, t).await;
                        return false;
                    },
                    ConflictKind::Divergent(last_lsn) => {
                        warn!(target: "repair", "Replica {} diverges from us at lsn {} (its last_lsn={}); snapshotting it", replica_url, lsn, last_lsn);
                        trigger_resync(&state, &replica_url, &collection).await;
                        return false;
                    },
                    ConflictKind::Gap(..) => {
                        warn!(target: "repair", "Replica {} still gapped during backfill at lsn {}, falling back to snapshot", replica_url, lsn);
                        trigger_resync(&state, &replica_url, &collection).await;
                        return false;
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
                return false;
            },
            Ok(r) => {
                warn!(target: "repair", "Replica {} returned {} during backfill", replica_url, r.status());
                return false;
            },
            Err(e) => {
                warn!(target: "repair", "Replica {} unreachable during backfill: {}", replica_url, e);
                return false;
            }
        }
    }

    state.metrics.note_replica_ack(&replica_url, prev);
    info!(target: "repair", "Streamed {} frames to {}; caught up to lsn {} for '{}'", sent, replica_url, prev, collection);
    prev >= target
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
    let url = format!("{}/internal/replicate", replica_url);
    let req = ReplicateRequest {
        collection: collection.to_string(),
        term,
        lsn,
        prev_lsn,
        commit_index: Some(commit_index),
        wal_frame: frame.to_vec(),
    };
    match state.client.post(&url).json(&req).send().await {
        Ok(r) if r.status().is_success() => {
            state.metrics.note_replica_ack(replica_url, lsn);
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
                    trigger_resync(state, replica_url, collection).await;
                    false
                },
                ConflictKind::Gap(last_lsn, last_term) => {
                    state.metrics.note_gap();
                    repair_replica(state.clone(), replica_url.to_string(), collection.to_string(), last_lsn, last_term).await
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
    let replicas = state.get_replicas();
    if replicas.is_empty() || required_acks <= 1 {
        replicate_to_peers(state, collection, frame, term, commit_index, lsn, prev_lsn);
        return 1;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<bool>(replicas.len());
    for replica_url in replicas {
        let state = state.clone();
        let col = collection.clone();
        let frame = frame.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let ok = replicate_one_await(&state, &replica_url, &col, &frame, term, commit_index, lsn, prev_lsn).await;
            let _ = tx.send(ok).await;
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
    use crate::test_support::{idx, make_frame, temp_root, three_node_cluster, put_doc_at, wait_for_doc};
    use std::fs;

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
        assert_eq!(pdb.global_commit_index.load(Ordering::SeqCst), 5);

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
        assert_eq!(rdb2.global_commit_index.load(Ordering::SeqCst), 5, "replica must reach primary's LSN after backfill");

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
        assert_eq!(db.global_commit_index.load(Ordering::SeqCst), 4);

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
}
