//! Test scaffolding, including a live multi-node cluster harness.

use crate::api::build_app;
use crate::auth::build_client;
use crate::cluster::metadata::ClusterMetadata;
use crate::cluster::migration::MigrationRuns;
use crate::config::NodeConfig;
use crate::consensus::{
    heartbeat_poll_task, progress_flush_task, seed_leader_progress, Progress, ReplicationMeta,
    ReplicationState,
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

pub fn temp_root() -> PathBuf {
    let p = std::env::temp_dir().join(format!("dewdb-test-{}", Uuid::new_v4()));
    fs::create_dir_all(&p).unwrap();
    p
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
    pub state: Option<AppState>,
    pub stop: Option<Arc<tokio::sync::Notify>>,
    pub thread: Option<std::thread::JoinHandle<()>>,
}

// Binding :0 and dropping the listener lets the OS hand the same port to two
// concurrent callers, so the counter rather than the OS guarantees uniqueness.
static NEXT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(20000);

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
            heartbeat_timeout_secs: 1,
            membership_mode: "voter".to_string(),
            allow_unsafe_ring_changes: false,
            data_movement_batch_size: 64,
            data_movement_batch_delay_ms: 5,
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
                    Database::with_cache(&config.data_dir, ReadCacheConfig::default()).unwrap()));

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
            leader_committed: HashMap::new(),
                }));

                let state = AppState {
                    db,
                    config: Arc::new(config.clone()),
                    client: build_client(&config.auth),
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
                    migration_write_gate: Arc::new(tokio::sync::RwLock::new(())),
                };

                let app = build_app(&state);
                if is_router {
                    crate::cluster::probe::router_probe_task(state.clone());
                } else {
                    state.follow_from_view();
                    state.react_to_migration();
                    if state.is_leader() {
                        seed_leader_progress(&state);
                    } else {
                        heartbeat_poll_task(state.clone());
                    }
                    progress_flush_task(state.clone());
                    crate::replication::stream::replication_drive_task(state.clone());
                }

                let listener = bind_with_retry(&addr).await;
                tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                });

                tx.send(state).unwrap();
                stop_in_node.notified().await;
            });

            rt.shutdown_background();
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
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.notify_one();
        }
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

/// A router in front of one shard group. Its own data dir holds only `cluster.meta`.
pub async fn router_for(root: &Path, owner: &str, replicas: &[String]) -> TestNode {
    let mut r = TestNode::new("router", free_port(), root, "primary");
    r.role = "router".to_string();
    r.shard_map = vec![(owner.to_string(), replicas.to_vec())];
    r.start();
    tokio::time::sleep(Duration::from_millis(200)).await;
    r
}

pub async fn get_raw(client: &reqwest::Client, base: &str, col: &str, key: &str) -> bool {
    let url = format!("{}/collections/{}/docs/{}", base, col, key);
    client.get(&url).send().await.map(|r| r.status().is_success()).unwrap_or(false)
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

pub async fn three_node_cluster(root: &Path) -> (TestNode, TestNode, TestNode) {
    let (p1, p2, p3) = (free_port(), free_port(), free_port());
    let (u1, u2, u3) = (
        format!("http://127.0.0.1:{}", p1),
        format!("http://127.0.0.1:{}", p2),
        format!("http://127.0.0.1:{}", p3),
    );

    let mut n1 = TestNode::new("n1", p1, root, "primary");
    n1.peers = vec![u2.clone(), u3.clone()];
    n1.replicas = vec![u2.clone(), u3.clone()];

    let mut n2 = TestNode::new("n2", p2, root, "replica");
    n2.peers = vec![u1.clone(), u3.clone()];
    n2.primary_addr = Some(u1.clone());

    let mut n3 = TestNode::new("n3", p3, root, "replica");
    n3.peers = vec![u1.clone(), u2.clone()];
    n3.primary_addr = Some(u1.clone());

    n1.start();
    n2.start();
    n3.start();
    tokio::time::sleep(Duration::from_millis(300)).await;
    (n1, n2, n3)
}
