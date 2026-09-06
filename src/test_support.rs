//! Test scaffolding, including a live multi-node cluster harness.

use crate::api::build_app;
use crate::auth::build_client;
use crate::cluster::metadata::ClusterMetadata;
use crate::cluster::migration::MigrationRuns;
use crate::config::NodeConfig;
use crate::consensus::{
    heartbeat_poll_task, progress_flush_task, publish_inherited_tails, seed_leader_progress,
    Progress, ReplicationMeta, ReplicationState,
};
use crate::metrics::Metrics;
use crate::state::AppState;
use crate::storage::index::IndexEntry;
use crate::storage::frame::{LogEntry, HEADER_LEN};
use crate::storage::{Collection, Database, FrameHeader, ReadCacheConfig};
use axum::http::StatusCode;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

pub fn idx(frame: &[u8], wal_id: u64, offset: u64) -> IndexEntry {
    IndexEntry { wal_id, offset, len: (frame.len() - HEADER_LEN) as u32, inline: None }
}

/// The existence check is the verdict, not the removal's result: a background task still holding an
/// `Arc<Collection>` recreates its data directory, and the removal reports success either way.
fn gone(root: &Path) -> bool {
    let _ = fs::remove_dir_all(root);
    !root.exists()
}

/// Named by thread, because the harness names a test's thread after the test and captures this
/// line unless the run passes `--nocapture`.
fn leaked(root: &Path) {
    println!("LEAKED {} in {} -- a node under it is still holding a file",
        root.display(), std::thread::current().name().unwrap_or("<unnamed>"));
}

/// Removes a `temp_root`, and says so when it cannot: Windows refuses to unlink a file a live node
/// still holds, so a swallowed failure here leaks a cluster's WAL per sample (bugs.md L13).
pub async fn cleanup(root: &Path) {
    for _ in 0..40 {
        if gone(root) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    leaked(root);
}

/// A `temp_root` that removes itself, so a test that panics or returns early does not leak one
/// (bugs.md L14). Tests bind it before the nodes under it, and reverse drop order then closes their
/// files first; a node outliving it prints `LEAKED` instead of failing, since a transient lock is
/// not the test's own defect.
pub struct TempRoot {
    path: PathBuf,
}

impl std::ops::Deref for TempRoot {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for TempRoot {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        for _ in 0..40 {
            if gone(&self.path) {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        leaked(&self.path);
    }
}

pub fn temp_root() -> TempRoot {
    let path = std::env::temp_dir().join(format!("dewdb-test-{}", Uuid::new_v4()));
    fs::create_dir_all(&path).unwrap();
    TempRoot { path }
}

pub fn make_frame(term: u64, lsn: u64, prev_lsn: u64, prev_term: u64, key: &str, v: i64) -> Vec<u8> {
    let entry = LogEntry::Put {
        key: key.to_string(),
        value: serde_json::json!({"v": v}),
        ts: 0,
    };
    let payload = serde_json::to_vec(&entry).unwrap();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&payload);

    let header = FrameHeader {
        len: payload.len() as u32,
        crc: hasher.finalize(),
        term,
        lsn,
        prev_lsn,
        prev_term,
    };

    let mut frame = header.encode().to_vec();
    frame.extend_from_slice(&payload);
    frame
}

pub fn make_drop_frame(term: u64, lsn: u64, prev_lsn: u64, prev_term: u64) -> Vec<u8> {
    let payload = serde_json::to_vec(&LogEntry::Drop { ts: 0 }).unwrap();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&payload);

    let header = FrameHeader {
        len: payload.len() as u32,
        crc: hasher.finalize(),
        term,
        lsn,
        prev_lsn,
        prev_term,
    };

    let mut frame = header.encode().to_vec();
    frame.extend_from_slice(&payload);
    frame
}

/// Mirrors the leader write path: durable but uncommitted. The append stages it.
pub fn stage_put(col: &Arc<Collection>, key: &str, v: i64) -> u64 {
    col.put(key.into(), serde_json::json!({"v": v}), 1).unwrap().3
}

pub fn stage_delete(col: &Arc<Collection>, key: &str) -> u64 {
    col.delete(key.into(), 1).unwrap().3
}

/// A value too large for the inline cache, so reads must go back to the WAL.
pub fn disk_put(col: &Arc<Collection>, key: &str, fill: &str) -> (u64, u64, u32) {
    let value = serde_json::json!({"v": fill.repeat(600)});
    let (f, wal_id, offset, lsn) = col.put(key.into(), value, 1).unwrap();
    col.apply_committed(lsn);
    assert!(col.index.read().unwrap()[key].inline.is_none(),
        "the value must be too large to inline");
    (wal_id, offset, (f.len() - HEADER_LEN) as u32)
}

pub fn live_put(col: &Arc<Collection>, key: &str, v: i64) {
    col.apply_committed(stage_put(col, key, v));
}

pub struct TestNode {
    pub node_id: String,
    pub addr: String,
    pub data_dir: PathBuf,
    /// "shard" or "router". A router gets no storage and no replication state, as in `main`.
    pub role: String,
    /// Router only: the ranges it routes, as `(owner, replicas)`.
    pub shard_map: Vec<(String, Vec<String>)>,
    pub peers: Vec<String>,
    pub replicas: Vec<String>,
    pub primary_addr: Option<String>,
    pub shard_role: String,
    pub heartbeat_timeout_secs: u64,
    /// "voter" or "learner". A learner never campaigns, whatever the timeout.
    pub membership_mode: String,
    pub allow_unsafe_ring_changes: bool,
    pub data_movement_batch_size: usize,
    pub data_movement_batch_delay_ms: u64,
    /// A `ChangefeedConfig` body; `{}` is the default.
    pub changefeed: serde_json::Value,
    /// A `WebhookConfig` body; `{}` is the default.
    pub webhooks: serde_json::Value,
    /// An `AuthConfig` body; `{}` is the default open configuration.
    pub auth: serde_json::Value,
    pub state: Option<AppState>,
    pub stop: Option<Arc<tokio::sync::Notify>>,
    pub thread: Option<std::thread::JoinHandle<()>>,
}

// Binding :0 and dropping the listener lets the OS hand the same port to two
// concurrent callers, so the counter rather than the OS guarantees uniqueness.
static NEXT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(20000);

/// Slack by default, short only where a test opts in. One second leaves a follower less margin
/// than its own poll interval, so under load the group churns terms and every deadline downstream
/// ends up racing the scheduler (bugs.md L8b).
pub const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 5;

pub fn next_test_port() -> u16 {
    free_port()
}

fn free_port() -> u16 {
    loop {
        let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if port < 20000 {
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn node_config(n: &TestNode) -> NodeConfig {
    // One range per owner, splitting the hash space evenly, so a router needs only the owner list.
    // The last range ends at 0, which is how a ring wraps; a lone owner is the whole ring.
    let owners = n.shard_map.len().max(1) as u128;
    let span = (1u128 << 64) / owners;
    let shard_map: Vec<serde_json::Value> = n.shard_map.iter().enumerate()
        .map(|(i, (owner, replicas))| serde_json::json!({
            "start_hash": (span * i as u128) as u64,
            "end_hash": if i + 1 == n.shard_map.len() { 0u64 } else { (span * (i as u128 + 1)) as u64 },
            "node_url": owner,
            "replica_urls": replicas,
        }))
        .collect();

    let json = serde_json::json!({
        "node_id": n.node_id,
        "role": n.role,
        "shard_map": shard_map,
        "shard_role": if n.role == "router" { serde_json::Value::Null } else { serde_json::json!(n.shard_role) },
        "membership_mode": n.membership_mode,
        "allow_unsafe_ring_changes": n.allow_unsafe_ring_changes,
        "listen_addr": n.addr,
        "peers": n.peers,
        "replicas": n.replicas,
        "primary_addr": n.primary_addr,
        "data_dir": n.data_dir.to_string_lossy(),
        "heartbeat_timeout_secs": n.heartbeat_timeout_secs,
        "election_delay_ms": 200,
        "auth": n.auth,
        "changefeed": n.changefeed,
        "webhooks": n.webhooks,
        "maintenance": { "enabled": false },
        "data_movement": {
            "batch_size": n.data_movement_batch_size,
            "batch_delay_ms": n.data_movement_batch_delay_ms,
        },
    });
    serde_json::from_value(json).unwrap()
}

impl TestNode {
    pub fn new(node_id: &str, port: u16, root: &Path, shard_role: &str) -> Self {
        let data_dir = root.join(node_id);
        fs::create_dir_all(&data_dir).unwrap();
        Self {
            node_id: node_id.to_string(),
            addr: format!("127.0.0.1:{}", port),
            data_dir,
            role: "shard".to_string(),
            shard_map: Vec::new(),
            peers: Vec::new(),
            replicas: Vec::new(),
            primary_addr: None,
            shard_role: shard_role.to_string(),
            heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
            membership_mode: "voter".to_string(),
            allow_unsafe_ring_changes: false,
            data_movement_batch_size: 64,
            data_movement_batch_delay_ms: 5,
            auth: serde_json::json!({}),
            changefeed: serde_json::json!({}),
            webhooks: serde_json::json!({}),
            state: None,
            stop: None,
            thread: None,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn start(&mut self) {
        let config = node_config(self);
        config.validate().unwrap();

        let addr = self.addr.clone();
        let stop = Arc::new(tokio::sync::Notify::new());
        let stop_in_node = stop.clone();
        let (tx, rx) = std::sync::mpsc::channel::<AppState>();

        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let is_router = config.role == "router";
                let db = (!is_router).then(|| Arc::new(
                    Database::with_config(&config.data_dir, ReadCacheConfig::default(),
                        config.changefeed.clone()).unwrap()));

                let meta = ReplicationMeta::load(&config.data_dir).expect("unreadable replication.meta");
                let solo = !config.is_learner()
                    && config.peers.is_empty()
                    && config.shard_role.as_deref() == Some("primary");
                let (term, is_leader, voted_for) = match &meta {
                    Some(m) => (m.term, solo, m.voted_for.clone()),
                    None => (0, !config.is_learner()
                                && config.shard_role.as_deref() == Some("primary"), None),
                };
                if !is_router {
                    ReplicationMeta { term, is_leader, voted_for: voted_for.clone() }
                        .save(&config.data_dir)
                        .expect("could not persist replication state");
                }

                let replication = Arc::new(RwLock::new(ReplicationState {
                    term,
                    is_leader,
                    voted_for,
                    last_heartbeat: None,
                    last_replication: None,
                    was_receiving_replication: false,
                    heartbeat_running: !is_leader,
                    replicas: config.replicas.clone(),
                    primary_addr: config.primary_addr.clone(),
                    last_known_primary_position: None,
                    progress: Progress::new(),
                    leases: Default::default(),
                    handing_over: false,
                    novote_until: None,
                    booted_at: std::time::Instant::now(),
                    leader_matched: HashMap::new(),
                    configuration: None,
                }));

                let state = AppState {
                    db,
                    config: Arc::new(config.clone()),
                    client: build_client(&config.auth, &config.own_url()),
                    stream_client: crate::auth::build_stream_client(&config.auth, &config.own_url()),
                    replication: (!is_router).then_some(replication),
                    primary_overrides: Arc::new(std::sync::Mutex::new(HashMap::new())),
                    shard_failover_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
                    repair_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
                    resyncing: Arc::new(std::sync::Mutex::new(HashSet::new())),
                    read_rr: Arc::new(AtomicUsize::new(0)),
                    node_loads: Arc::new(std::sync::Mutex::new(HashMap::new())),
                    routed_reads: Arc::new(std::sync::Mutex::new(HashMap::new())),
                    metrics: Arc::new(Metrics::new()),
                    replication_slots: Arc::new(tokio::sync::Semaphore::new(
                        config.flow_control.max_inflight_requests.max(1))),
                    cluster: Arc::new(RwLock::new(
                        ClusterMetadata::load(&config.data_dir)
                            .expect("unreadable cluster.meta")
                            .unwrap_or_else(|| {
                                let seeded = ClusterMetadata::seed_from_config(&config);
                                let _ = seeded.save(&config.data_dir);
                                seeded
                            }))),
                    ring_cache: Arc::new(std::sync::Mutex::new(Default::default())),
                    migrations: Arc::new(std::sync::Mutex::new(MigrationRuns::restored(&config.data_dir))),
                    webhooks: Arc::new(crate::webhook::WebhookStore::restored(&config.data_dir)),
                    write_gate: Arc::new(tokio::sync::RwLock::new(())),
                };

                let app = build_app(&state);
                if is_router {
                    crate::cluster::probe::router_probe_task(state.clone());
                } else {
                    state.follow_from_view();
                    state.refresh_configuration();
                    state.react_to_migration();
                    if state.is_leader() {
                        seed_leader_progress(&state);
                        publish_inherited_tails(&state);
                        crate::consensus::reconfigure::resume_change(&state);
                    } else {
                        heartbeat_poll_task(state.clone());
                    }
                    progress_flush_task(state.clone());
                    crate::consensus::leader_contact_task(state.clone());
                    crate::replication::stream::replication_drive_task(state.clone());
                }
                crate::cluster::catalog::index_catalog_task(state.clone());
                crate::webhook::webhook_task(state.clone());

                let listener = bind_with_retry(&addr).await;
                tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                });

                tx.send(state).unwrap();
                stop_in_node.notified().await;
            });

            // Bounded, not detached: `shutdown_background` let a handler mid-write outlive the
            // join, and a cluster-view save then recreated the data directory (bugs.md L14).
            rt.shutdown_timeout(Duration::from_millis(500));
        });

        let state = rx.recv_timeout(Duration::from_secs(10)).expect("node failed to start");
        self.state = Some(state);
        self.stop = Some(stop);
        self.thread = Some(thread);
    }

    pub fn kill(&mut self) {
        self.state = None;
        if let Some(stop) = self.stop.take() {
            stop.notify_one();
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }

    pub fn is_leader(&self) -> bool {
        self.state.as_ref().map_or(false, |s| s.is_leader())
    }

    pub fn term(&self) -> u64 {
        self.state.as_ref().map_or(0, |s| s.current_term())
    }
}

impl Drop for TestNode {
    // Joined, not just signalled: a notify alone leaves the runtime winding down on its own
    // schedule, and a run that drops clusters in a loop ends up with all of them still competing.
    fn drop(&mut self) {
        self.kill();
    }
}

async fn bind_with_retry(addr: &str) -> tokio::net::TcpListener {
    for _ in 0..50 {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => return l,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("could not bind {}", addr);
}

#[derive(Clone, Debug)]
pub struct SseEvent {
    pub name: String,
    pub id: Option<String>,
    pub data: serde_json::Value,
}

/// Reads an SSE response in the background and collects what arrives. Dropping it aborts the read
/// and closes the connection, which is how a test plays a subscriber going away.
pub struct SseTap {
    seen: Arc<std::sync::Mutex<Vec<SseEvent>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for SseTap {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SseTap {
    pub fn open(response: reqwest::Response) -> Self {
        use futures::StreamExt;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let task = tokio::spawn(async move {
            let mut body = response.bytes_stream();
            let mut buffered = String::new();
            while let Some(Ok(chunk)) = body.next().await {
                buffered.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(end) = buffered.find("\n\n") {
                    let block: String = buffered.drain(..end + 2).collect();
                    if let Some(event) = parse_sse_block(&block) {
                        sink.lock().unwrap().push(event);
                    }
                }
            }
        });
        Self { seen, task }
    }

    pub fn events(&self) -> Vec<SseEvent> {
        self.seen.lock().unwrap().clone()
    }

    pub fn named(&self, name: &str) -> Vec<SseEvent> {
        self.events().into_iter().filter(|e| e.name == name).collect()
    }

    pub async fn wait_for_events(&self, name: &str, want: usize, deadline: Duration) -> Vec<SseEvent> {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline && self.named(name).len() < want {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.named(name)
    }
}

/// A keep-alive is a comment line and carries no `event:`, so it parses to nothing.
fn parse_sse_block(block: &str) -> Option<SseEvent> {
    let mut name = None;
    let mut id = None;
    let mut data = String::new();
    for line in block.lines() {
        match line.split_once(':') {
            Some(("event", v)) => name = Some(v.trim().to_string()),
            Some(("id", v)) => id = Some(v.trim().to_string()),
            Some(("data", v)) => data.push_str(v.trim()),
            _ => {},
        }
    }
    Some(SseEvent {
        name: name?,
        id,
        data: serde_json::from_str(&data).unwrap_or(serde_json::Value::Null),
    })
}

pub async fn put_doc_at(client: &reqwest::Client, base: &str, col: &str, key: &str, v: i64, query: &str) -> StatusCode {
    let url = format!("{}/collections/{}/docs/{}{}", base, col, key, query);
    match client.put(&url).json(&serde_json::json!({"value": {"v": v}})).send().await {
        Ok(r) => r.status(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn read_doc_at(client: &reqwest::Client, base: &str, col: &str, key: &str) -> Option<i64> {
    let url = format!("{}/collections/{}/docs/{}", base, col, key);
    let r = client.get(&url).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    r.json::<serde_json::Value>().await.ok()?.get("v")?.as_i64()
}

pub async fn put_doc_http(client: &reqwest::Client, base: &str, key: &str, v: i64) -> StatusCode {
    put_doc_at(client, base, "t", key, v, "").await
}

pub async fn read_doc_http(client: &reqwest::Client, base: &str, key: &str) -> Option<i64> {
    read_doc_at(client, base, "t", key).await
}

pub async fn wait_for_doc(client: &reqwest::Client, base: &str, col: &str, key: &str, want: i64, deadline: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if read_doc_at(client, base, col, key).await == Some(want) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

pub async fn wait_for<F>(deadline: Duration, mut check: F) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

pub fn leaders(nodes: &[&TestNode]) -> Vec<String> {
    nodes.iter().filter(|n| n.is_leader()).map(|n| n.node_id.clone()).collect()
}

pub async fn settle_leader(nodes: &[&TestNode], deadline: Duration) -> Option<String> {
    let start = std::time::Instant::now();
    let mut candidate: Option<(String, std::time::Instant)> = None;

    while start.elapsed() < deadline {
        let current = leaders(nodes);
        if current.len() == 1 {
            let holder = current[0].clone();
            match &candidate {
                Some((id, since)) if *id == holder => {
                    if since.elapsed() >= Duration::from_millis(1200) {
                        return Some(holder);
                    }
                },
                _ => candidate = Some((holder, std::time::Instant::now())),
            }
        } else {
            candidate = None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

pub fn node_by_id<'a>(nodes: &[&'a TestNode], id: &str) -> &'a TestNode {
    nodes.iter().find(|n| n.node_id == id).expect("leader vanished")
}

/// A leader with no peers, so writes commit on their own durability. Isolates local write cost
/// from anything replication does.
pub async fn single_node(root: &Path) -> TestNode {
    let mut n = TestNode::new("solo", free_port(), root, "primary");
    n.start();
    tokio::time::sleep(Duration::from_millis(200)).await;
    n
}

/// A router in front of the given shard groups, one hash range each. Its own data dir holds only
/// `cluster.meta`.
pub async fn router_for(root: &Path, groups: &[(String, Vec<String>)]) -> TestNode {
    let mut r = TestNode::new("router", free_port(), root, "primary");
    r.role = "router".to_string();
    r.shard_map = groups.to_vec();
    r.start();
    tokio::time::sleep(Duration::from_millis(200)).await;
    r
}

/// Two single-node shards behind a router, so a query has to fan out across ranges and merge.
pub async fn two_shard_cluster(root: &Path) -> (TestNode, TestNode, TestNode) {
    let mut s1 = TestNode::new("s1", free_port(), root, "primary");
    let mut s2 = TestNode::new("s2", free_port(), root, "primary");
    s1.start();
    s2.start();
    let router = router_for(root, &[(s1.url(), Vec::new()), (s2.url(), Vec::new())]).await;
    (s1, s2, router)
}

pub async fn get_raw(client: &reqwest::Client, base: &str, col: &str, key: &str) -> bool {
    let url = format!("{}/collections/{}/docs/{}", base, col, key);
    client.get(&url).send().await.map(|r| r.status().is_success()).unwrap_or(false)
}

/// One delivery as the endpoint saw it, headers included: a signature is only worth asserting on
/// alongside the exact bytes it was computed over.
#[derive(Clone, Debug)]
pub struct Delivered {
    pub body: serde_json::Value,
    pub raw: String,
    pub signature: Option<String>,
    pub timestamp: Option<String>,
    pub delivery: Option<String>,
}

struct SinkState {
    received: std::sync::Mutex<Vec<Delivered>>,
    fail_next: std::sync::atomic::AtomicUsize,
    status: std::sync::atomic::AtomicU16,
}

/// A webhook endpoint under a test's control: it records what arrives and can be told to fail,
/// which is how the retry and the backoff are observed from the outside.
pub struct WebhookSink {
    pub url: String,
    state: Arc<SinkState>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for WebhookSink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn sink_handler(
    axum::extract::State(sink): axum::extract::State<Arc<SinkState>>,
    headers: axum::http::HeaderMap,
    body: String,
) -> StatusCode {
    use std::sync::atomic::Ordering;
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string);
    sink.received.lock().unwrap().push(Delivered {
        body: serde_json::from_str(&body).unwrap_or(serde_json::Value::Null),
        raw: body,
        signature: header("x-dew-signature"),
        timestamp: header("x-dew-timestamp"),
        delivery: header("x-dew-delivery"),
    });
    if sink.fail_next.load(Ordering::SeqCst) > 0 {
        sink.fail_next.fetch_sub(1, Ordering::SeqCst);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::from_u16(sink.status.load(Ordering::SeqCst)).unwrap_or(StatusCode::OK)
}

impl WebhookSink {
    pub async fn start() -> Self {
        use std::sync::atomic::{AtomicU16, AtomicUsize};
        let port = free_port();
        let state = Arc::new(SinkState {
            received: std::sync::Mutex::new(Vec::new()),
            fail_next: AtomicUsize::new(0),
            status: AtomicU16::new(200),
        });
        let app = axum::Router::new()
            .route("/hook", axum::routing::post(sink_handler))
            .with_state(state.clone());
        let listener = bind_with_retry(&format!("127.0.0.1:{}", port)).await;
        let task = tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
        Self { url: format!("http://127.0.0.1:{}/hook", port), state, task }
    }

    pub fn fail_next(&self, attempts: usize) {
        self.state.fail_next.store(attempts, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn answer_with(&self, status: u16) {
        self.state.status.store(status, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn received(&self) -> Vec<Delivered> {
        self.state.received.lock().unwrap().clone()
    }

    /// Every event across every delivery, in arrival order. Redeliveries are included: at-least-once
    /// is what the sender promises, so a test that hid them would be asserting the wrong thing.
    pub fn events(&self) -> Vec<serde_json::Value> {
        self.received().iter()
            .filter_map(|d| d.body["events"].as_array().cloned())
            .flatten().collect()
    }

    pub async fn wait_for_events(&self, want: usize, deadline: Duration) -> Vec<serde_json::Value> {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline && self.events().len() < want {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.events()
    }
}

/// Reads a change-stream WebSocket in the background and collects the JSON frames that arrive.
/// Dropping it closes the socket, which is how a test plays a subscriber going away.
pub struct WsTap {
    seen: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for WsTap {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl WsTap {
    /// `Err` is the status the handshake was refused with: every refusal this endpoint owes a
    /// client is answered before the upgrade, so a test can assert on it as an HTTP status.
    pub async fn open(url: &str) -> Result<Self, StatusCode> {
        use futures::StreamExt;
        let ws_url = url.replacen("http://", "ws://", 1);
        let socket = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((socket, _)) => socket,
            Err(tokio_tungstenite::tungstenite::Error::Http(res)) => {
                return Err(StatusCode::from_u16(res.status().as_u16()).unwrap());
            },
            Err(e) => panic!("could not open {}: {}", ws_url, e),
        };

        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let task = tokio::spawn(async move {
            let mut socket = socket;
            while let Some(Ok(message)) = socket.next().await {
                if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                    if let Ok(frame) = serde_json::from_str(&text) {
                        sink.lock().unwrap().push(frame);
                    }
                }
            }
        });
        Ok(Self { seen, task })
    }

    pub fn named(&self, name: &str) -> Vec<serde_json::Value> {
        self.seen.lock().unwrap().iter()
            .filter(|f| f["type"].as_str() == Some(name)).cloned().collect()
    }

    pub async fn wait_for(&self, name: &str, want: usize, deadline: Duration) -> Vec<serde_json::Value> {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline && self.named(name).len() < want {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.named(name)
    }
}

pub async fn put_value(
    client: &reqwest::Client,
    base: &str,
    col: &str,
    key: &str,
    value: serde_json::Value,
    query: &str,
) -> StatusCode {
    let url = format!("{}/collections/{}/docs/{}{}", base, col, key, query);
    match client.put(&url).json(&serde_json::json!({"value": value})).send().await {
        Ok(r) => r.status(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// The default is deliberately slack. A one-second contact timeout leaves a follower 500ms of
/// margin over its own poll interval, so any test holding a leader across multi-second work was one
/// scheduler stall away from an election it never asked for (bugs.md L8b). Tests that *wait* for a
/// failover want the short timeout and say so with `three_node_cluster_with_timeout`.
pub async fn three_node_cluster(root: &Path) -> (TestNode, TestNode, TestNode) {
    three_node_cluster_with_timeout(root, DEFAULT_HEARTBEAT_TIMEOUT_SECS).await
}

/// A longer contact timeout than the default 1s, for tests that deliberately leave the leader
/// without a quorum: CheckQuorum steps such a leader down, and one second is not enough runway to
/// assert what it does while it still holds office.
pub async fn three_node_cluster_with_timeout(
    root: &Path,
    heartbeat_timeout_secs: u64,
) -> (TestNode, TestNode, TestNode) {
    let mut nodes = voter_group(root, 3, heartbeat_timeout_secs).await;
    let n3 = nodes.pop().unwrap();
    let n2 = nodes.pop().unwrap();
    let n1 = nodes.pop().unwrap();
    (n1, n2, n3)
}

/// One shard group of `n` voters, `n1` leading and the rest following it. `w=majority` needs
/// `n / 2 + 1`, so this is how a wider quorum's cost is measured against a narrower one.
pub async fn voter_group(root: &Path, n: usize, heartbeat_timeout_secs: u64) -> Vec<TestNode> {
    let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
    let urls: Vec<String> = ports.iter().map(|p| format!("http://127.0.0.1:{}", p)).collect();

    let mut nodes = Vec::with_capacity(n);
    for (i, port) in ports.iter().enumerate() {
        let role = if i == 0 { "primary" } else { "replica" };
        let mut node = TestNode::new(&format!("n{}", i + 1), *port, root, role);
        node.peers = urls.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, u)| u.clone()).collect();
        if i == 0 {
            node.replicas = urls[1..].to_vec();
        } else {
            node.primary_addr = Some(urls[0].clone());
        }
        node.heartbeat_timeout_secs = heartbeat_timeout_secs;
        nodes.push(node);
    }

    for node in nodes.iter_mut() {
        node.start();
    }

    await_converged(&nodes.iter().collect::<Vec<_>>()).await;
    nodes
}

const CONVERGE_TIMEOUT: Duration = Duration::from_secs(20);

/// Blocks until exactly one of `nodes` leads and every other has heard from it.
///
/// Not a sleep, and every hand-rolled group needs it as much as `voter_group` does: a follower
/// polls 500ms after boot and campaigns at `heartbeat_timeout_secs`, so a fixed wait shorter than
/// both hands the test a leader that is already being deposed -- and every assertion downstream
/// then fails on its own subject instead of on the real cause (bugs.md L8b).
pub async fn await_converged(nodes: &[&TestNode]) {
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    while !cluster_converged(nodes) {
        assert!(std::time::Instant::now() < deadline,
            "cluster of {} did not converge in {:?}: leading {:?}, in contact {:?}",
            nodes.len(), CONVERGE_TIMEOUT, ids(nodes, |n| n.is_leader()), ids(nodes, has_leader_contact));
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn cluster_converged(nodes: &[&TestNode]) -> bool {
    nodes.iter().filter(|n| n.is_leader()).count() == 1
        && nodes.iter().filter(|n| !n.is_leader()).all(|n| has_leader_contact(n))
}

fn has_leader_contact(node: &TestNode) -> bool {
    node.state.as_ref().and_then(|s| s.replication.as_ref()).is_some_and(|r| {
        let g = r.read().unwrap();
        g.last_heartbeat.is_some() || g.last_replication.is_some()
    })
}

fn ids(nodes: &[&TestNode], f: impl Fn(&TestNode) -> bool) -> Vec<String> {
    nodes.iter().filter(|n| f(n)).map(|n| n.node_id.clone()).collect()
}

/// `shards` single-node shard groups behind one router, one hash range each. A write pays the
/// routing hop and a query pays a fan-out across every range.
pub async fn sharded_cluster(root: &Path, shards: usize) -> (Vec<TestNode>, TestNode) {
    let mut owners = Vec::with_capacity(shards);
    for i in 0..shards {
        let mut node = TestNode::new(&format!("s{}", i + 1), free_port(), root, "primary");
        node.start();
        owners.push(node);
    }
    let groups: Vec<(String, Vec<String>)> =
        owners.iter().map(|n| (n.url(), Vec::new())).collect();
    let router = router_for(root, &groups).await;
    (owners, router)
}
