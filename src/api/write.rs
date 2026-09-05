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

    // Nothing to replicate to, not "not currently leading": a node demoted between the handler's
    // check and here answered `201` for an entry the next leader truncates (bugs.md C26).
    if state.replication.is_none() {
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

/// An index definition as a replicated log entry, the way `local_drop` carries a drop. No key
/// locks: the entry orders against concurrent writes by LSN alone, and both readings are correct
/// -- a write below it is picked up by the build, one above it stages the values the new index
/// asks for.
pub async fn local_index_change(
    state: &AppState,
    col_name: &str,
    change: crate::storage::IndexChange,
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

    let term = state.current_term();
    let col_clone = col.clone();
    let appended = tokio::task::spawn_blocking(move || col_clone.define_index(change, term)).await;
    let (frame, _wal_id, _offset, lsn) = match appended {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    let commit = col.enqueue_commit();
    state.note_leader_append(col_name, lsn);
    let pending = PendingWrite { frame, term, lsn, existed: true, commit: Some(commit) };

    finish_write(state, col_name, pending, wc, wtimeout).await
}

/// One replication round for a run of frames appended together, rather than the per-document
/// fan-out `finish_write` gives it (bugs.md H7). Sound because the log is a chain: a replica that
/// acknowledges the last LSN holds every frame below it, so the run shares one holder set.
///
/// The fan-out was not just a cost: on a cold send cursor every document gapped at once and each
/// rewound the replica on a `last_lsn` already stale, so the repairs undid each other's cursor.
async fn finish_write_batch(
    state: &AppState,
    col_name: &str,
    mut pending: Vec<PendingWrite>,
    wc: WriteConcern,
    wtimeout: Duration,
) -> Result<Vec<WriteOutcome>, axum::response::Response> {
    // Normally none: the batch path group-commits before it gets here.
    for p in &mut pending {
        if let Some(commit) = p.commit.take() {
            settle_commit(commit).await?;
        }
    }

    let existed: Vec<bool> = pending.iter().map(|p| p.existed).collect();
    let last = match pending.pop() {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };
    let spread = |met: bool, acks: usize, required: usize| -> Vec<WriteOutcome> {
        existed.iter().map(|&existed| WriteOutcome { met, acks, required, existed }).collect()
    };

    if state.replication.is_none() {
        return Ok(spread(true, 1, 1));
    }

    let quorum = write_quorum(&wc, &state.quorum_config());
    let required = quorum.required();
    let own = state.own_url();
    let PendingWrite { frame, term, lsn, .. } = last;
    let prev_lsn = FrameHeader::parse(&frame).map_or(0, |h| h.prev_lsn);
    let commit_index = state.committed_lsn(col_name);

    // The replica's send cursor sits below this frame's `prev_lsn`, so both paths repair the run
    // ahead of it out of the WAL the group commit already synced.
    let holders = if quorum.met(std::slice::from_ref(&own)) {
        replicate_to_peers(state.clone(), col_name.to_string(), frame, term, commit_index, lsn, prev_lsn);
        vec![own]
    } else {
        replicate_and_await(
            state.clone(), col_name.to_string(), frame, term, commit_index, lsn, prev_lsn,
            quorum.clone(), wtimeout,
        ).await
    };

    let own_durable = state
        .db
        .as_ref()
        .and_then(|db| db.get_collection(col_name).ok())
        .map_or(0, |col| col.durable_lsn());
    state.advance_own_commit(col_name, own_durable);

    Ok(spread(quorum.met(&holders), holders.len(), required))
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

    // The floor is an `or_insert` of the first append in the term, so only the lowest LSN in the
    // run can move it and the rest of the batch would take the progress lock for nothing.
    if let Some((_key, _frame, _wal_id, _offset, lsn)) = frames.first() {
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

    finish_write_batch(state, col_name, pending, wc, wtimeout).await
}

#[cfg(test)]
mod tests {
    use super::local_write;
    use crate::config::NodeConfig;
    use crate::replication::WriteConcern;
    use crate::state::AppState;
    use crate::storage::Database;
    use crate::test_support::{
        next_test_port, temp_root, three_node_cluster, three_node_cluster_with_timeout, wait_for_doc,
        TestNode,
    };
    use std::sync::Arc;
    use axum::http::StatusCode;
    use std::time::Duration;

    /// M5: `existed` was sampled from the committed index, so with the quorum down — every write
    /// durable and none of them committed — a replace and a delete both reported nothing was there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn created_or_replaced_is_decided_against_the_uncommitted_tail() {
        let root = temp_root();
        let (n1, mut n2, mut n3) = three_node_cluster_with_timeout(&root, 30).await;
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
    }

    /// C26: `finish_write` opened with `!state.is_leader()`, meant as "there is nothing to
    /// replicate to". It is equally true of a leader deposed between the handler's leadership check
    /// and the fsync after it, and that node answered `201` with `required: 1` whatever was asked,
    /// for an entry that is unreplicated at a stale term and that the next leader truncates.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_that_lost_leadership_mid_flight_does_not_report_its_concern_met() {
        let root = temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let peers = vec![
            format!("http://127.0.0.1:{}", next_test_port()),
            format!("http://127.0.0.1:{}", next_test_port()),
        ];
        let config: NodeConfig = serde_json::from_value(serde_json::json!({
            "node_id": "n1", "role": "shard", "shard_role": "primary",
            "listen_addr": "127.0.0.1:1",
            "data_dir": root.to_string_lossy(),
            "replicas": peers, "peers": peers,
        })).unwrap();
        // Deposed, not standalone: `replication` is present and `is_leader` is not.
        let state = AppState::for_admission_test(config, db, false);

        let outcome = local_write(&state, "t", "k".into(), Some(serde_json::json!({"v": 1})),
            WriteConcern::Majority, Duration::from_millis(250)).await.unwrap();

        assert_eq!(outcome.required, 2, "a majority of three voters, whoever is leading");
        assert_eq!(outcome.acks, 1, "nothing but this node holds it");
        assert!(!outcome.met, "an acknowledged write the next leader is going to truncate");
    }

    /// H7: the batch called `finish_write` per document, so a bulk of N paid N fan-outs for a run
    /// one repair carries. A replica that refuses a frame it cannot chain is what makes the frames
    /// below the last one observable: they arrive or the concern is not met.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_bulk_write_replicates_as_one_run() {
        use crate::replication::protocol::ReplicateRequest;
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

        #[derive(Default)]
        struct Stub {
            requests: AtomicUsize,
            frames: AtomicUsize,
            applied: AtomicU64,
        }

        async fn replicate(
            axum::extract::State(stub): axum::extract::State<Arc<Stub>>,
            axum::Json(req): axum::Json<ReplicateRequest>,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            stub.requests.fetch_add(1, Ordering::SeqCst);
            let held = stub.applied.load(Ordering::SeqCst);
            if req.prev_lsn > held {
                return (StatusCode::CONFLICT, axum::Json(serde_json::json!({
                    "status": "gap", "last_lsn": held, "last_term": 0,
                }))).into_response();
            }
            if req.lsn <= held {
                return axum::Json(serde_json::json!({"status": "duplicate", "last_lsn": held})).into_response();
            }
            let tail = req.frames.iter()
                .filter_map(|f| super::FrameHeader::parse(f))
                .map(|h| h.lsn)
                .fold(req.lsn, u64::max);
            stub.frames.fetch_add(1 + req.frames.len(), Ordering::SeqCst);
            stub.applied.fetch_max(tail, Ordering::SeqCst);
            axum::Json(serde_json::json!({"status": "applied", "lsn": tail})).into_response()
        }

        const DOCS: usize = 24;
        let root = temp_root();
        let stub = Arc::new(Stub::default());
        let stub_port = next_test_port();
        let app = axum::Router::new()
            .route("/internal/replicate", axum::routing::post(replicate))
            .with_state(stub.clone());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", stub_port)).await.unwrap();
        tokio::spawn(async move { let _ = axum::serve(listener, app).await; });

        let mut leader = TestNode::new("blk1", next_test_port(), &root, "primary");
        leader.replicas = vec![format!("http://127.0.0.1:{}", stub_port)];
        leader.start();

        let payload: Vec<serde_json::Value> = (1..=DOCS)
            .map(|i| serde_json::json!({"id": format!("k{}", i), "value": {"v": i}}))
            .collect();
        let r = reqwest::Client::new()
            .post(format!("{}/collections/t/docs/bulk?w=majority&wtimeout=4000", leader.url()))
            .json(&payload).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED,
            "a batch the only other voter holds meets `w=majority`: {}", r.text().await.unwrap());

        let tail = leader.state.as_ref().unwrap().db.as_ref().unwrap()
            .get_collection("t").unwrap().last_appended_lsn();
        assert_eq!(stub.applied.load(Ordering::SeqCst), tail,
            "the concern was met on the last frame, so the run below it has to be there too");
        assert_eq!(stub.frames.load(Ordering::SeqCst), DOCS, "every document ships exactly once");
        let requests = stub.requests.load(Ordering::SeqCst);
        assert!(requests <= 4,
            "{} documents took {} replicate requests; the run is one repair plus the probe that              establishes the cursor, not a fan-out per document", DOCS, requests);

        leader.kill();
    }

    /// The other half of H7, against real followers rather than a stub. The collection is
    /// deliberately cold: with no send cursor for the replicas, the old per-document fan-out
    /// gapped 23 of these 24 at once and burned the whole `wtimeout` reporting `acks: 1` for
    /// documents both followers held. A `w=all` write first hides it, which is how the benchmark
    /// missed it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_document_in_a_batch_reaches_the_replicas() {
        const DOCS: usize = 24;
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::new();

        let payload: Vec<serde_json::Value> = (1..=DOCS)
            .map(|i| serde_json::json!({"id": format!("k{}", i), "value": {"v": i}}))
            .collect();
        let r = client.post(format!("{}/collections/t/docs/bulk?w=majority&wtimeout=4000", n1.url()))
            .json(&payload).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CREATED,
            "a batch two of three voters hold meets `w=majority`: {}", r.text().await.unwrap());

        for follower in [&n2, &n3] {
            for i in 1..=DOCS {
                assert!(wait_for_doc(&client, &follower.url(), "t", &format!("k{}", i), i as i64,
                        Duration::from_secs(5)).await,
                    "k{} never reached {}", i, follower.url());
            }
        }
    }
}
