use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const WAL_ROTATION_LIMIT: u64 = 50 * 1024 * 1024;
const MAX_RECORD_SIZE: u64 = 10 * 1024 * 1024;
const INDEX_FILENAME: &str = "index-current.bin";
const HEADER_LEN: usize = 24;
const KEY_LOCK_STRIPES: usize = 64;
const DIR_REMOVE_ATTEMPTS: usize = 5;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "op", rename_all = "lowercase")]
enum LogEntry {
    Put {
        key: String,
        value: serde_json::Value,
        ts: u64,
    },
    Del {
        key: String,
        ts: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct IndexEntry {
    wal_id: u64,
    offset: u64,
}

#[derive(Serialize, Deserialize)]
struct IndexSnapshot {
    last_wal_id: u64,
    last_offset: u64,
    last_lsn: u64,
    #[serde(default)]
    last_term: u64,
    map: BTreeMap<String, IndexEntry>,
}

#[derive(Serialize, Deserialize)]
struct LsnMeta {
    commit_lsn: u64,
}

impl LsnMeta {
    fn load(dir: &Path) -> Option<Self> {
        let content = fs::read_to_string(dir.join("lsn.meta")).ok()?;
        serde_json::from_str(&content).ok()
    }

    fn save(&self, dir: &Path) -> io::Result<()> {
        let content = serde_json::to_string(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(dir.join("lsn.meta"), content)
    }
}

#[derive(Deserialize, Clone, Debug)]
struct ShardInfo {
    start_hash: u64,
    end_hash: u64,
    node_url: String,
    #[serde(default)]
    replica_urls: Vec<String>,
}

#[derive(Deserialize, Clone, Debug)]
struct NodeConfig {
    node_id: String,
    role: String,
    listen_addr: String,
    #[serde(default)]
    shard_map: Vec<ShardInfo>,
    #[serde(default)]
    shard_role: Option<String>,
    #[serde(default)]
    primary_addr: Option<String>,
    #[serde(default)]
    replicas: Vec<String>,
    #[serde(default)]
    peers: Vec<String>,
    #[serde(default = "default_heartbeat_timeout")]
    heartbeat_timeout_secs: u64,
    #[serde(default = "default_election_delay")]
    election_delay_ms: u64,
}

fn default_heartbeat_timeout() -> u64 { 6 }
fn default_election_delay() -> u64 { 2000 }

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ReplicateRequest {
    collection: String,
    term: u64,
    lsn: u64,
    prev_lsn: u64,
    commit_index: Option<u64>,
    #[serde(with = "base64_bytes")]
    wal_frame: Vec<u8>,
}

#[derive(Debug)]
enum ReplicaApply {
    Applied { wal_id: u64, offset: u64, lsn: u64 },
    Duplicate { last_lsn: u64 },
    Gap { last_lsn: u64 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ReplicationMeta {
    term: u64,
    is_leader: bool,
    #[serde(default)]
    voted_for: Option<String>,
}

struct ReplicationState {
    term: u64,
    is_leader: bool,
    voted_for: Option<String>,
    last_heartbeat: Option<std::time::Instant>,
    was_receiving_replication: bool,
    last_replication: Option<std::time::Instant>,
    heartbeat_running: bool,
    primary_addr: Option<String>,
    replicas: Vec<String>,
    last_known_primary_position: Option<u64>,
}

impl ReplicationMeta {
    fn load(data_dir: &str) -> Option<Self> {
        let path = PathBuf::from(data_dir).join("replication.meta");
        let content = fs::read_to_string(&path).ok()?;
        serde_json::from_str(&content).ok()
    }

    fn save(&self, data_dir: &str) -> io::Result<()> {
        let path = PathBuf::from(data_dir).join("replication.meta");
        let content = serde_json::to_string(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(&path, content)
    }
}

struct PrimaryOverride {
    url: String,
    cached_at: std::time::Instant,
}

const OVERRIDE_TTL_SECS: u64 = 30;
mod base64_bytes {
    use serde::{Deserialize, Deserializer, Serializer};
    use serde::de;

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Serialize;
        let encoded = base64_encode(bytes);
        encoded.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64_decode(&s).map_err(de::Error::custom)
    }

    pub fn base64_encode(input: &[u8]) -> String {
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut result = String::new();
        for chunk in input.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let combined = (b0 << 16) | (b1 << 8) | b2;
            result.push(CHARS[((combined >> 18) & 0x3F) as usize] as char);
            result.push(CHARS[((combined >> 12) & 0x3F) as usize] as char);
            if chunk.len() > 1 {
                result.push(CHARS[((combined >> 6) & 0x3F) as usize] as char);
            } else {
                result.push('=');
            }
            if chunk.len() > 2 {
                result.push(CHARS[(combined & 0x3F) as usize] as char);
            } else {
                result.push('=');
            }
        }
        result
    }

    pub fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
        let input = input.trim_end_matches('=');
        let mut result = Vec::new();
        let mut buf: u32 = 0;
        let mut bits: u32 = 0;
        for c in input.chars() {
            let val = match c {
                'A'..='Z' => c as u32 - 'A' as u32,
                'a'..='z' => c as u32 - 'a' as u32 + 26,
                '0'..='9' => c as u32 - '0' as u32 + 52,
                '+' => 62,
                '/' => 63,
                _ => return Err(format!("Invalid base64 char: {}", c)),
            };
            buf = (buf << 6) | val;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                result.push((buf >> bits) as u8);
                buf &= (1 << bits) - 1;
            }
        }
        Ok(result)
    }
}

fn hash_key(col: &str, key: &str) -> u64 {
    xxhash_rust::xxh64::xxh64(format!("{}:{}", col, key).as_bytes(), 0)
}

impl NodeConfig {
    fn validate(&self) -> Result<(), String> {
        if self.role == "router" && self.shard_map.is_empty() {
            return Err("Router requires at least one shard".into());
        }
        if self.role == "router" {
            let mut sorted = self.shard_map.clone();
            sorted.sort_by_key(|s| s.start_hash);
            for i in 1..sorted.len() {
                if sorted[i].start_hash != sorted[i - 1].end_hash {
                    return Err("Shard map has uncovered hash ranges".into());
                }
            }
            if sorted.first().unwrap().start_hash == 0 && sorted.last().unwrap().end_hash == 0 {
            } else if sorted.last().unwrap().end_hash == sorted.first().unwrap().start_hash {
            } else {
                return Err("Shard map has uncovered hash ranges".into());
            }
        }
        if self.role == "shard" {
            if let Some(ref sr) = self.shard_role {
                if sr == "replica" && self.primary_addr.is_none() {
                    return Err("Replica shard requires primary_addr".into());
                }
            }
        }
        Ok(())
    }

    fn get_shard_url(&self, hash: u64) -> Option<String> {
        for shard in &self.shard_map {
            if shard.start_hash <= shard.end_hash {
                if hash >= shard.start_hash && hash < shard.end_hash {
                    return Some(shard.node_url.clone());
                }
            } else {
                if hash >= shard.start_hash || hash < shard.end_hash {
                    return Some(shard.node_url.clone());
                }
            }
        }
        None
    }
}

struct Collection {
    name: String,
    root_path: PathBuf,
    data_root: PathBuf,
    index: RwLock<BTreeMap<String, IndexEntry>>,
    wal_writer: std::sync::Mutex<WalsState>,
    key_locks: Vec<tokio::sync::Mutex<()>>,
    commit_notifiers: Arc<std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<Result<(), String>>>>>,
    commit_signal: Arc<tokio::sync::Notify>,
    read_pool: std::sync::Mutex<HashMap<u64, Vec<Arc<std::sync::Mutex<File>>>>>,
    read_pool_counter: AtomicUsize,
    released: AtomicBool,
    compacting: AtomicBool,
    db_global_commit_index: Arc<AtomicU64>,
    db_next_lsn: Arc<AtomicU64>,
    db_last_log_term: Arc<AtomicU64>,
}

struct WalsState {
    current_wal: File,
    current_wal_id: u64,
    current_wal_size: u64,
    last_appended_lsn: u64,
    last_appended_term: u64,
}

const COMMIT_BATCH_THRESHOLD: usize = 32;
const COMMIT_INTERVAL_MS: u64 = 5;

#[derive(Clone)]
struct AppState {
    db: Option<Arc<Database>>,
    config: Arc<NodeConfig>,
    client: reqwest::Client,
    replication: Option<Arc<RwLock<ReplicationState>>>,
    primary_overrides: Arc<std::sync::Mutex<HashMap<String, PrimaryOverride>>>,
    shard_failover_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    repair_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    resyncing: Arc<std::sync::Mutex<HashSet<String>>>,
    read_rr: Arc<AtomicUsize>,
}

impl AppState {
    fn is_leader(&self) -> bool {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().is_leader;
        }
        false
    }

    fn is_shard(&self) -> bool {
        self.config.role == "shard"
    }

    fn current_term(&self) -> u64 {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().term;
        }
        0
    }

    fn get_replicas(&self) -> Vec<String> {
        if let Some(ref repl) = self.replication {
            return repl.read().unwrap().replicas.clone();
        }
        Vec::new()
    }

    fn get_effective_shard_url(&self, hash: u64) -> Option<(String, String, Vec<String>)> {
        for shard in &self.config.shard_map {
            let matches = if shard.start_hash <= shard.end_hash {
                hash >= shard.start_hash && hash < shard.end_hash
            } else {
                hash >= shard.start_hash || hash < shard.end_hash
            };
            if matches {
                let mut overrides = self.primary_overrides.lock().unwrap();
                if let Some(ov) = overrides.get(&shard.node_url) {
                    if ov.cached_at.elapsed().as_secs() < OVERRIDE_TTL_SECS {
                        return Some((ov.url.clone(), shard.node_url.clone(), shard.replica_urls.clone()));
                    } else {
                        overrides.remove(&shard.node_url);
                    }
                }
                return Some((shard.node_url.clone(), shard.node_url.clone(), shard.replica_urls.clone()));
            }
        }
        None
    }

    fn set_primary_override(&self, original_url: &str, new_url: &str) {
        let mut overrides = self.primary_overrides.lock().unwrap();
        overrides.insert(original_url.to_string(), PrimaryOverride {
            url: new_url.to_string(),
            cached_at: std::time::Instant::now(),
        });
    }

    fn clear_primary_override(&self, original_url: &str) {
        self.primary_overrides.lock().unwrap().remove(original_url);
    }

    fn effective_primary(&self, original_url: &str) -> String {
        let mut overrides = self.primary_overrides.lock().unwrap();
        if let Some(ov) = overrides.get(original_url) {
            if ov.cached_at.elapsed().as_secs() < OVERRIDE_TTL_SECS {
                return ov.url.clone();
            }
            overrides.remove(original_url);
        }
        original_url.to_string()
    }
}

struct CompactionGuard<'a> {
    flag: &'a AtomicBool,
}

impl<'a> Drop for CompactionGuard<'a> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

fn remove_file_with_retry(path: &Path) -> io::Result<()> {
    let mut last_err = None;
    for attempt in 0..DIR_REMOVE_ATTEMPTS {
        match fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(20 * (attempt + 1) as u64));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to remove file")))
}

fn remove_dir_with_retry(path: &Path) -> io::Result<()> {
    let mut last_err = None;
    for attempt in 0..DIR_REMOVE_ATTEMPTS {
        match fs::remove_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(20 * (attempt + 1) as u64));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to remove directory")))
}

struct Database {
    root_path: PathBuf,
    collections: RwLock<HashMap<String, Arc<Collection>>>,
    pub global_commit_index: Arc<AtomicU64>,
    pub next_lsn: Arc<AtomicU64>,
    pub last_log_term: Arc<AtomicU64>,
}

impl Database {
    fn new(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let root_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&root_path)?;
        let boot_lsn = LsnMeta::load(&root_path).map(|m| m.commit_lsn).unwrap_or(0);
        if boot_lsn > 0 {
            println!("[db] Restored commit LSN {} from lsn.meta", boot_lsn);
        }
        Ok(Self {
            root_path,
            collections: RwLock::new(HashMap::new()),
            global_commit_index: Arc::new(AtomicU64::new(boot_lsn)),
            next_lsn: Arc::new(AtomicU64::new(boot_lsn)),
            last_log_term: Arc::new(AtomicU64::new(0)),
        })
    }

    fn get_collection(&self, name: &str) -> io::Result<Arc<Collection>> {
        {
            let collections = self.collections.read().unwrap();
            if let Some(col) = collections.get(name) {
                return Ok(col.clone());
            }
        }

        let mut collections = self.collections.write().unwrap();
        if let Some(col) = collections.get(name) {
            return Ok(col.clone());
        }

        let col_path = self.root_path.join(name);
        let col = Arc::new(Collection::open(
            name.to_string(),
            col_path,
            self.global_commit_index.clone(),
            self.next_lsn.clone(),
            self.last_log_term.clone(),
        )?);
        Collection::start_commit_task(col.clone());
        collections.insert(name.to_string(), col.clone());
        Ok(col)
    }

    fn list_collections(&self) -> io::Result<Vec<String>> {
        let mut names: HashSet<String> = self.collections.read().unwrap().keys().cloned().collect();

        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with('.') || name.ends_with(".tmp") || name.ends_with(".old") {
                    continue;
                }
                names.insert(name.to_string());
            }
        }

        let mut out: Vec<String> = names.into_iter().collect();
        out.sort();
        Ok(out)
    }

    fn release_collection(&self, name: &str) -> io::Result<Option<PathBuf>> {
        let existing = self.collections.write().unwrap().remove(name);
        match existing {
            Some(col) => Ok(Some(col.release_handles()?)),
            None => Ok(None),
        }
    }

    fn drop_collection(&self, name: &str) -> io::Result<bool> {
        let tombstone = self.release_collection(name)?;

        let col_path = self.root_path.join(name);
        let existed = col_path.is_dir();

        for candidate in [
            col_path,
            self.root_path.join(format!("{}.tmp", name)),
            self.root_path.join(format!("{}.old", name)),
        ] {
            if candidate.is_dir() {
                remove_dir_with_retry(&candidate)?;
            }
        }

        if let Some(path) = tombstone {
            let _ = fs::remove_file(path);
        }

        Ok(existed)
    }

    fn force_commit_all(&self) {
        let collections = self.collections.read().unwrap();
        for (name, col) in collections.iter() {
            let wal = col.wal_writer.lock().unwrap();
            if let Err(e) = wal.current_wal.sync_data() {
                eprintln!("[{}] Failed to force sync WAL on shutdown: {}", name, e);
            }
            self.global_commit_index.fetch_max(wal.last_appended_lsn, Ordering::SeqCst);
            drop(wal);
            let notifiers: Vec<_> = {
                let mut q = col.commit_notifiers.lock().unwrap();
                std::mem::take(&mut *q)
            };
            let count = notifiers.len();
            for tx in notifiers {
                let _ = tx.send(Ok(()));
            }
            println!("[{}] Flushed {} pending writes.", name, count);
        }
        let meta = LsnMeta { commit_lsn: self.global_commit_index.load(Ordering::SeqCst) };
        if let Err(e) = meta.save(&self.root_path) {
            eprintln!("[db] Failed to persist lsn meta on shutdown: {}", e);
        }
    }
}

impl Collection {
    fn open(
        name: String,
        root_path: PathBuf,
        db_global_commit_index: Arc<AtomicU64>,
        db_next_lsn: Arc<AtomicU64>,
        db_last_log_term: Arc<AtomicU64>,
    ) -> io::Result<Self> {
        fs::create_dir_all(&root_path)?;
        let data_root = root_path.parent().unwrap_or(Path::new(".")).to_path_buf();

        let mut index = BTreeMap::new();
        let mut wal_files = Vec::new();

        let index_path = root_path.join(INDEX_FILENAME);
        let mut snapshot_loaded = false;
        let mut snapshot_wal_id = 0;
        let mut snapshot_offset = 0;
        let mut snapshot_lsn: u64 = 0;
        let mut snapshot_term: u64 = 0;

        if index_path.exists() {
             match File::open(&index_path) {
                Ok(file) => {
                    match bincode::deserialize_from::<_, IndexSnapshot>(BufReader::new(file)) {
                        Ok(snapshot) => {
                            println!("[{}] Loaded persisted snapshot (WAL ID: {}, Offset: {}, LSN: {}) with {} entries.",
                                     name, snapshot.last_wal_id, snapshot.last_offset, snapshot.last_lsn, snapshot.map.len());
                            index = snapshot.map;
                            snapshot_wal_id = snapshot.last_wal_id;
                            snapshot_offset = snapshot.last_offset;
                            snapshot_lsn = snapshot.last_lsn;
                            snapshot_term = snapshot.last_term;
                            snapshot_loaded = true;
                        }
                        Err(e) => eprintln!("[{}] Failed to deserialize snapshot (likely legacy format): {}. Rebuilding from WAL.", name, e),
                    }
                }
                Err(e) => eprintln!("[{}] Failed to open index file: {}", name, e),
             }
        }

        for entry in fs::read_dir(&root_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if fname.starts_with("wal-") && fname.ends_with(".log") {
                    let id_part = &fname[4..fname.len() - 4];
                    if let Ok(id) = id_part.parse::<u64>() {
                        wal_files.push((id, path));
                    }
                }
            }
        }
        wal_files.sort_by_key(|(id, _)| *id);

        let mut max_lsn = snapshot_lsn;
        let mut max_term = snapshot_term;

        let fold = |res: (u64, u64), max_lsn: &mut u64, max_term: &mut u64| {
            let (lsn, term) = res;
            if lsn > *max_lsn {
                *max_lsn = lsn;
                *max_term = term;
            }
        };

        if !snapshot_loaded {
            println!("[{}] Replaying all WALs...", name);
             for (id, path) in &wal_files {
                let r = Self::replay_file_from(*id, path, 0, &mut index)?;
                fold(r, &mut max_lsn, &mut max_term);
            }
        } else {
             for (id, path) in &wal_files {
                 if *id < snapshot_wal_id {
                     continue;
                 } else if *id == snapshot_wal_id {
                     println!("[{}] Resuming WAL {} from offset {}", name, id, snapshot_offset);
                     let r = Self::replay_file_from(*id, path, snapshot_offset, &mut index)?;
                     fold(r, &mut max_lsn, &mut max_term);
                 } else {
                     let r = Self::replay_file_from(*id, path, 0, &mut index)?;
                     fold(r, &mut max_lsn, &mut max_term);
                 }
             }
        }

        let boot_lsn = max_lsn;
        let prev_global = db_next_lsn.fetch_max(boot_lsn, Ordering::SeqCst);
        if boot_lsn > prev_global {
            db_last_log_term.store(max_term, Ordering::SeqCst);
        }

        let current_wal_id = wal_files.last().map(|(id, _)| *id).unwrap_or(0) + 1;

        let wal_path = root_path.join(format!("wal-{:05}.log", current_wal_id));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;

        let current_wal_size = file.metadata()?.len();

        Ok(Self {
            name,
            root_path,
            data_root,
            index: RwLock::new(index),
            wal_writer: std::sync::Mutex::new(WalsState {
                current_wal: file,
                current_wal_id,
                current_wal_size,
                last_appended_lsn: boot_lsn,
                last_appended_term: max_term,
            }),
            key_locks: (0..KEY_LOCK_STRIPES).map(|_| tokio::sync::Mutex::new(())).collect(),
            commit_notifiers: Arc::new(std::sync::Mutex::new(Vec::new())),
            commit_signal: Arc::new(tokio::sync::Notify::new()),
            read_pool: std::sync::Mutex::new(HashMap::new()),
            read_pool_counter: AtomicUsize::new(0),
            released: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            db_global_commit_index,
            db_next_lsn,
            db_last_log_term,
        })
    }

    fn key_stripe(&self, key: &str) -> usize {
        xxhash_rust::xxh64::xxh64(key.as_bytes(), 0) as usize % KEY_LOCK_STRIPES
    }

    fn key_lock(&self, key: &str) -> &tokio::sync::Mutex<()> {
        &self.key_locks[self.key_stripe(key)]
    }

    fn exists(&self, key: &str) -> bool {
        self.index.read().unwrap().contains_key(key)
    }

    fn replay_file_from(wal_id: u64, path: &PathBuf, mut start_offset: u64, index: &mut BTreeMap<String, IndexEntry>) -> io::Result<(u64, u64)> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let file_len = file.metadata()?.len();

        if start_offset > file_len {
            start_offset = 0;
        }

        file.seek(SeekFrom::Start(start_offset))?;

        let mut offset = start_offset;
        let mut valid_end_offset = start_offset;
        let mut max_lsn: u64 = 0;
        let mut max_term: u64 = 0;

        loop {
            let mut header = [0u8; HEADER_LEN];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
            let term = u64::from_le_bytes(header[8..16].try_into().unwrap());
            let lsn = u64::from_le_bytes(header[16..24].try_into().unwrap());

            if len == 0 || (len as u64) > MAX_RECORD_SIZE {
                eprintln!("[{}] Invalid WAL frame length {}. Truncating.", path.display(), len);
                break;
            }

            let mut payload = vec![0u8; len as usize];
            if let Err(_) = file.read_exact(&mut payload) {
                eprintln!("[{}] Unexpected EOF while reading payload. Truncating.", path.display());
                break;
            }

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&payload);
            if hasher.finalize() != crc {
                eprintln!("[{}] CRC mismatch. Truncating file at chunk boundary.", path.display());
                break;
            }

            if let Ok(entry) = serde_json::from_slice::<LogEntry>(&payload) {
                match entry {
                    LogEntry::Put { key, .. } => {
                        index.insert(key, IndexEntry { wal_id, offset });
                    },
                    LogEntry::Del { key, .. } => {
                        index.remove(&key);
                    }
                }
                if lsn > max_lsn {
                    max_lsn = lsn;
                    max_term = term;
                }
            }
            offset += HEADER_LEN as u64 + len as u64;
            valid_end_offset = offset;
        }

        if valid_end_offset < file_len {
            file.set_len(valid_end_offset)?;
            println!("[{}] Truncated corrupted WAL file down to size {}", path.display(), valid_end_offset);
        }

        Ok((max_lsn, max_term))
    }

    fn current_timestamp() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    fn put(&self, key: String, value: serde_json::Value, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        let entry = LogEntry::Put {
            key: key.clone(),
            value,
            ts: Self::current_timestamp(),
        };
        self.append(entry, term)
    }

    fn delete(&self, key: String, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        let entry = LogEntry::Del {
            key: key.clone(),
            ts: Self::current_timestamp(),
        };
        self.append(entry, term)
    }

    fn append(&self, entry: LogEntry, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        let json_bytes = serde_json::to_vec(&entry)?;
        let len = json_bytes.len() as u64;

        if len > MAX_RECORD_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Record exceeds maximum size"));
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&json_bytes);
        let crc = hasher.finalize();

        let frame_len = HEADER_LEN as u64 + len;

        let mut wal = self.wal_writer.lock().unwrap();

        if self.released.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Collection handle is no longer active"));
        }

        if wal.current_wal_size >= WAL_ROTATION_LIMIT {
            wal.current_wal.sync_all()?;
            wal.current_wal_id += 1;
            let new_path = self.root_path.join(format!("wal-{:05}.log", wal.current_wal_id));
            wal.current_wal = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&new_path)?;
            wal.current_wal_size = 0;
        }

        let lsn = self.db_next_lsn.fetch_add(1, Ordering::SeqCst) + 1;

        let mut header = [0u8; HEADER_LEN];
        header[0..4].copy_from_slice(&(len as u32).to_le_bytes());
        header[4..8].copy_from_slice(&crc.to_le_bytes());
        header[8..16].copy_from_slice(&term.to_le_bytes());
        header[16..24].copy_from_slice(&lsn.to_le_bytes());

        let mut frame = Vec::with_capacity(frame_len as usize);
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&json_bytes);

        wal.current_wal.write_all(&header)?;
        wal.current_wal.write_all(&json_bytes)?;

        let offset = wal.current_wal_size;
        wal.current_wal_size += frame_len;

        let wal_id = wal.current_wal_id;
        wal.last_appended_lsn = lsn;
        wal.last_appended_term = term;
        self.db_last_log_term.store(term, Ordering::SeqCst);

        drop(wal);

        Ok((frame, wal_id, offset, lsn))
    }

    fn append_raw_frame(&self, frame_bytes: &[u8], prev_lsn: u64) -> io::Result<ReplicaApply> {
        if frame_bytes.len() < HEADER_LEN {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Frame too short"));
        }

        let len = u32::from_le_bytes(frame_bytes[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(frame_bytes[4..8].try_into().unwrap());
        let frame_term = u64::from_le_bytes(frame_bytes[8..16].try_into().unwrap());
        let frame_lsn = u64::from_le_bytes(frame_bytes[16..24].try_into().unwrap());

        if frame_bytes.len() < HEADER_LEN + len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Frame payload incomplete"));
        }

        let payload = &frame_bytes[HEADER_LEN..HEADER_LEN + len];

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload);
        if hasher.finalize() != crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch on replicated frame"));
        }

        let _entry: LogEntry = serde_json::from_slice(payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let frame_len = (HEADER_LEN + len) as u64;

        let mut wal = self.wal_writer.lock().unwrap();

        if self.released.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Collection handle is no longer active"));
        }

        let last = wal.last_appended_lsn;

        if frame_lsn <= last {
            return Ok(ReplicaApply::Duplicate { last_lsn: last });
        }

        if prev_lsn != last {
            return Ok(ReplicaApply::Gap { last_lsn: last });
        }

        if wal.current_wal_size >= WAL_ROTATION_LIMIT {
            wal.current_wal.sync_all()?;
            wal.current_wal_id += 1;
            let new_path = self.root_path.join(format!("wal-{:05}.log", wal.current_wal_id));
            wal.current_wal = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&new_path)?;
            wal.current_wal_size = 0;
        }

        wal.current_wal.write_all(&frame_bytes[..HEADER_LEN + len])?;

        let offset = wal.current_wal_size;
        let wal_id = wal.current_wal_id;

        wal.current_wal_size += frame_len;
        wal.last_appended_lsn = frame_lsn;
        wal.last_appended_term = frame_term;
        self.db_next_lsn.fetch_max(frame_lsn, Ordering::SeqCst);
        self.db_last_log_term.store(frame_term, Ordering::SeqCst);

        Ok(ReplicaApply::Applied { wal_id, offset, lsn: frame_lsn })
    }

    fn read_frames_after(&self, after_lsn: u64, up_to_lsn: u64) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if fname.starts_with("wal-") && fname.ends_with(".log") {
                    let id_part = &fname[4..fname.len() - 4];
                    if let Ok(id) = id_part.parse::<u64>() {
                        wal_files.push((id, path));
                    }
                }
            }
        }
        wal_files.sort_by_key(|(id, _)| *id);

        let mut out = Vec::new();
        for (_id, path) in wal_files {
            let mut file = match File::open(&path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            loop {
                let mut header = [0u8; HEADER_LEN];
                if file.read_exact(&mut header).is_err() {
                    break;
                }
                let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
                let lsn = u64::from_le_bytes(header[16..24].try_into().unwrap());

                if len == 0 || len as u64 > MAX_RECORD_SIZE {
                    break;
                }

                let mut payload = vec![0u8; len];
                if file.read_exact(&mut payload).is_err() {
                    break;
                }

                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&payload);
                if hasher.finalize() != crc {
                    break;
                }

                if lsn > after_lsn && lsn <= up_to_lsn {
                    let mut frame = Vec::with_capacity(HEADER_LEN + len);
                    frame.extend_from_slice(&header);
                    frame.extend_from_slice(&payload);
                    out.push((lsn, frame));
                }
            }
        }
        Ok(out)
    }

    pub fn enqueue_commit(&self) -> tokio::sync::oneshot::Receiver<Result<(), String>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut q = self.commit_notifiers.lock().unwrap();
        let is_empty = q.is_empty();
        q.push(tx);
        let len = q.len();

        if is_empty || len >= COMMIT_BATCH_THRESHOLD {
            self.commit_signal.notify_one();
        }
        rx
    }

    fn start_commit_task(col: Arc<Collection>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = col.commit_signal.notified() => {},
                    _ = tokio::time::sleep(Duration::from_millis(COMMIT_INTERVAL_MS)) => {},
                }

                if col.released.load(Ordering::SeqCst) {
                    let notifiers: Vec<_> = {
                        let mut q = col.commit_notifiers.lock().unwrap();
                        std::mem::take(&mut *q)
                    };
                    for tx in notifiers {
                        let _ = tx.send(Err(format!("Collection '{}' handle is no longer active", col.name)));
                    }
                    println!("[{}] Commit task stopped; handle released.", col.name);
                    return;
                }

                let has_pending = {
                    let q = col.commit_notifiers.lock().unwrap();
                    !q.is_empty()
                };

                if !has_pending {
                    continue;
                }

                let col_sync = col.clone();
                let sync_result = tokio::task::spawn_blocking(move || {
                    let wal = col_sync.wal_writer.lock().unwrap();
                    wal.current_wal.sync_data()
                }).await;

                let result = match sync_result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(format!("WAL sync failed: {}", e)),
                    Err(e) => Err(format!("Commit task panicked: {}", e)),
                };

                let notifiers: Vec<_> = {
                    let mut q = col.commit_notifiers.lock().unwrap();
                    std::mem::take(&mut *q)
                };

                let count = notifiers.len();

                for tx in notifiers {
                    let _ = tx.send(result.clone());
                }

                if count > 0 && result.is_ok() {
                    let last_lsn = { col.wal_writer.lock().unwrap().last_appended_lsn };
                    col.db_global_commit_index.fetch_max(last_lsn, Ordering::SeqCst);
                    let commit_lsn = col.db_global_commit_index.load(Ordering::SeqCst);
                    let meta = LsnMeta { commit_lsn };
                    if let Err(e) = meta.save(&col.data_root) {
                        eprintln!("[{}] Failed to persist lsn meta: {}", col.name, e);
                    }
                }
            }
        });
    }

    pub fn iter(&self) -> Vec<(String, IndexEntry)> {
        let index = self.index.read().unwrap();
        index.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    pub fn range(&self, start: Option<&str>, end: Option<&str>) -> Vec<(String, IndexEntry)> {
        let index = self.index.read().unwrap();

        let range_bound = (
            start.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included),
            end.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included),
        );

        index.range::<str, _>(range_bound).map(|(k, v)| (k.clone(), *v)).collect()
    }

    pub fn range_from(&self, after: Option<&str>, start: Option<&str>, end: Option<&str>) -> Vec<(String, IndexEntry)> {
        let index = self.index.read().unwrap();

        let start_bound = if let Some(a) = after {
            std::ops::Bound::Excluded(a)
        } else if let Some(s) = start {
            std::ops::Bound::Included(s)
        } else {
            std::ops::Bound::Unbounded
        };
        let end_bound = end.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included);

        index.range::<str, _>((start_bound, end_bound)).map(|(k, v)| (k.clone(), *v)).collect()
    }

    fn query_page(
        &self,
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        filter: &Option<Filter>,
        limit: usize,
    ) -> io::Result<(Vec<serde_json::Value>, Option<String>)> {
        let mut items = Vec::with_capacity(limit);
        let mut last_key: Option<String> = None;
        let mut has_more = false;

        for (key, _entry) in self.range_from(after, start, end).into_iter() {
            if items.len() >= limit {
                match filter {
                    None => {
                        has_more = true;
                        break;
                    },
                    Some(f) => {
                        if let Some(val) = self.get(&key)? {
                            if matches_filter(&val, f) {
                                has_more = true;
                                break;
                            }
                        }
                    }
                }
            } else if let Some(val) = self.get(&key)? {
                let matched = filter.as_ref().map_or(true, |f| matches_filter(&val, f));
                if matched {
                    items.push(val);
                    last_key = Some(key.clone());
                }
            }
        }

        let next_cursor = if has_more { last_key } else { None };
        Ok((items, next_cursor))
    }

    fn get(&self, key: &str) -> io::Result<Option<serde_json::Value>> {
        let idx_entry = {
            let index = self.index.read().unwrap();
            index.get(key).copied()
        };

        if let Some(entry) = idx_entry {
            let file_arc = {
                let mut pool = self.read_pool.lock().unwrap();
                let counter = self.read_pool_counter.fetch_add(1, Ordering::Relaxed);
                if let Some(handles) = pool.get_mut(&entry.wal_id) {
                    handles[counter % handles.len()].clone()
                } else {
                    let path = self.root_path.join(format!("wal-{:05}.log", entry.wal_id));
                    let mut handles = Vec::new();
                    for _ in 0..4 {
                         handles.push(Arc::new(std::sync::Mutex::new(File::open(&path)?)));
                    }
                    pool.insert(entry.wal_id, handles.clone());
                    handles[counter % 4].clone()
                }
            };

            let mut file = file_arc.lock().unwrap();
            file.seek(SeekFrom::Start(entry.offset))?;

            let mut header = [0u8; HEADER_LEN];
            file.read_exact(&mut header)?;
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap());

            let mut payload = vec![0u8; len as usize];
            file.read_exact(&mut payload)?;

            if let Ok(LogEntry::Put { value, .. }) = serde_json::from_slice(&payload) {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn list_all(&self) -> io::Result<Vec<serde_json::Value>> {
        let index = self.index.read().unwrap();
        let mut results = Vec::new();
        for (key, _) in index.iter() {
           if let Some(val) = self.get(key)? {
               results.push(val);
           }
        }
        Ok(results)
    }

    fn save_index(&self) -> io::Result<()> {
        let wal_writer = self.wal_writer.lock().unwrap();
        let index = self.index.read().unwrap();

        let snapshot = IndexSnapshot {
            last_wal_id: wal_writer.current_wal_id,
            last_offset: wal_writer.current_wal_size,
            last_lsn: wal_writer.last_appended_lsn,
            last_term: wal_writer.last_appended_term,
            map: index.clone(),
        };

        let temp_path = self.root_path.join("index-current.tmp");
        let path = self.root_path.join(INDEX_FILENAME);

        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        bincode::serialize_into(&mut writer, &snapshot)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        writer.flush()?;
        writer.get_mut().sync_all()?;

        fs::rename(&temp_path, &path)?;

        println!("[{}] Index saved to disk at WAL {} offset {} lsn {}.", self.name, snapshot.last_wal_id, snapshot.last_offset, snapshot.last_lsn);
        Ok(())
    }

    fn compact(&self) -> io::Result<()> {
        if self.compacting.swap(true, Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Compaction already in progress"));
        }
        let _guard = CompactionGuard { flag: &self.compacting };

        let (frozen_index, frozen_through, compact_id) = {
            let mut wal = self.wal_writer.lock().unwrap();

            wal.current_wal.sync_data()?;

            let frozen_through = wal.current_wal_id;
            let compact_id = frozen_through + 1;
            let active_id = frozen_through + 2;

            let active_path = self.root_path.join(format!("wal-{:05}.log", active_id));
            wal.current_wal = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&active_path)?;
            wal.current_wal_id = active_id;
            wal.current_wal_size = 0;

            let frozen: Vec<(String, IndexEntry)> = self.index.read().unwrap()
                .iter()
                .filter(|(_, e)| e.wal_id <= frozen_through)
                .map(|(k, e)| (k.clone(), *e))
                .collect();

            (frozen, frozen_through, compact_id)
        };

        println!("[{}] Compacting {} live keys from WAL <= {} into WAL {}; writes continue on WAL {}.",
            self.name, frozen_index.len(), frozen_through, compact_id, frozen_through + 2);

        let compact_path = self.root_path.join("wal-compacted.tmp");
        let relocated = match self.write_compacted_wal(&compact_path, &frozen_index, compact_id) {
            Ok(r) => r,
            Err(e) => {
                let _ = fs::remove_file(&compact_path);
                return Err(e);
            }
        };

        let final_path = self.root_path.join(format!("wal-{:05}.log", compact_id));
        fs::rename(&compact_path, &final_path)?;

        let mut remapped = 0usize;
        let mut superseded = 0usize;
        {
            let _wal = self.wal_writer.lock().unwrap();
            let mut index = self.index.write().unwrap();

            for (key, old_entry, new_entry) in relocated {
                match index.get(&key) {
                    Some(current) if *current == old_entry => {
                        index.insert(key, new_entry);
                        remapped += 1;
                    },
                    _ => superseded += 1,
                }
            }

            let _ = fs::remove_file(self.root_path.join(INDEX_FILENAME));

            self.read_pool.lock().unwrap().clear();

            self.remove_wals_through(frozen_through)?;
        }

        self.commit_signal.notify_one();

        println!("[{}] Compaction complete: {} keys relocated, {} superseded by concurrent writes.",
            self.name, remapped, superseded);
        Ok(())
    }

    fn write_compacted_wal(
        &self,
        compact_path: &Path,
        frozen_index: &[(String, IndexEntry)],
        compact_id: u64,
    ) -> io::Result<Vec<(String, IndexEntry, IndexEntry)>> {
        let mut compact_file = BufWriter::new(File::create(compact_path)?);
        let mut relocated = Vec::with_capacity(frozen_index.len());
        let mut current_offset = 0u64;
        let mut readers: HashMap<u64, File> = HashMap::new();

        for (key, old_entry) in frozen_index {
            let file = match readers.entry(old_entry.wal_id) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let path = self.root_path.join(format!("wal-{:05}.log", old_entry.wal_id));
                    e.insert(File::open(&path)?)
                }
            };

            file.seek(SeekFrom::Start(old_entry.offset))?;

            let mut header = [0u8; HEADER_LEN];
            file.read_exact(&mut header)?;
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;

            if len == 0 || len as u64 > MAX_RECORD_SIZE {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("Live key '{}' has an invalid frame length in WAL {}", key, old_entry.wal_id)));
            }

            let mut payload = vec![0u8; len];
            file.read_exact(&mut payload)?;

            match serde_json::from_slice::<LogEntry>(&payload) {
                Ok(LogEntry::Put { .. }) => {},
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("Live key '{}' does not resolve to a Put frame in WAL {}", key, old_entry.wal_id))),
            }

            compact_file.write_all(&header)?;
            compact_file.write_all(&payload)?;

            relocated.push((
                key.clone(),
                *old_entry,
                IndexEntry { wal_id: compact_id, offset: current_offset },
            ));
            current_offset += (HEADER_LEN + len) as u64;
        }

        compact_file.flush()?;
        compact_file.get_mut().sync_all()?;
        Ok(relocated)
    }

    fn remove_wals_through(&self, frozen_through: u64) -> io::Result<()> {
        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = match name.to_str() {
                Some(n) => n,
                None => continue,
            };
            if !name.starts_with("wal-") || !name.ends_with(".log") {
                continue;
            }
            match name[4..name.len() - 4].parse::<u64>() {
                Ok(id) if id <= frozen_through => {
                    if let Err(e) = remove_file_with_retry(&entry.path()) {
                        eprintln!("[{}] Could not remove obsolete {}: {}", self.name, name, e);
                    }
                },
                _ => {}
            }
        }
        Ok(())
    }

    fn release_handles(&self) -> io::Result<PathBuf> {
        self.released.store(true, Ordering::SeqCst);

        let tombstone = self.data_root.join(format!(".released-{}.wal", self.name));

        {
            let mut wal = self.wal_writer.lock().unwrap();
            wal.current_wal = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&tombstone)?;
            wal.current_wal_size = 0;
        }

        self.commit_signal.notify_one();
        self.read_pool.lock().unwrap().clear();
        self.index.write().unwrap().clear();

        Ok(tombstone)
    }
}

#[derive(Serialize, Deserialize)]
struct CreateDoc {
    value: serde_json::Value,
}

#[derive(Deserialize)]
struct BulkDoc {
    #[serde(default)]
    id: Option<String>,
    value: serde_json::Value,
}

#[derive(Deserialize)]
struct QueryParams {
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    filter: Option<String>,
    read: Option<String>,
    cursor: Option<String>,
    sort: Option<String>,
    fields: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct QueryPage {
    items: Vec<serde_json::Value>,
    next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, Default)]
struct ShardCursor {
    positions: BTreeMap<String, String>,
}

fn encode_cursor(c: &ShardCursor) -> String {
    let json = serde_json::to_vec(c).unwrap_or_default();
    base64_bytes::base64_encode(&json)
}

fn decode_cursor(s: &str) -> Option<ShardCursor> {
    let bytes = base64_bytes::base64_decode(s).ok()?;
    serde_json::from_slice(&bytes).ok()
}

#[derive(Deserialize)]
struct ReadParams {
    read: Option<String>,
}

#[derive(Deserialize)]
struct Filter {
    #[serde(flatten)]
    fields: HashMap<String, serde_json::Value>,
}

fn get_path_value<'a>(doc: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut cur = doc;

    for part in path.split('.') {
        cur = cur.get(part)?;
    }

    Some(cur)
}

struct SortSpec {
    field: String,
    desc: bool,
}

fn parse_sort(s: Option<&str>) -> Option<SortSpec> {
    let s = s?;
    let (field, dir) = match s.split_once(':') {
        Some((f, d)) => (f, d),
        None => (s, "asc"),
    };
    if field.is_empty() {
        return None;
    }
    Some(SortSpec { field: field.to_string(), desc: dir.eq_ignore_ascii_case("desc") })
}

fn parse_fields(s: Option<&str>) -> Vec<String> {
    match s {
        Some(s) => s.split(',').map(|f| f.trim().to_string()).filter(|f| !f.is_empty()).collect(),
        None => Vec::new(),
    }
}

fn type_rank(v: &serde_json::Value) -> u8 {
    match v {
        serde_json::Value::Null => 0,
        serde_json::Value::Bool(_) => 1,
        serde_json::Value::Number(_) => 2,
        serde_json::Value::String(_) => 3,
        serde_json::Value::Array(_) => 4,
        serde_json::Value::Object(_) => 5,
    }
}

fn json_cmp(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value;
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(_), Value::Number(_)) => {
            let x = a.as_f64().unwrap_or(f64::NAN);
            let y = b.as_f64().unwrap_or(f64::NAN);
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        },
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => {
            for (ex, ey) in x.iter().zip(y.iter()) {
                let o = json_cmp(ex, ey);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        },
        (Value::Object(x), Value::Object(y)) => x.len().cmp(&y.len()),
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

fn compare_by_sort(a: &serde_json::Value, b: &serde_json::Value, sort: &SortSpec) -> std::cmp::Ordering {
    let va = get_path_value(a, &sort.field).cloned().unwrap_or(serde_json::Value::Null);
    let vb = get_path_value(b, &sort.field).cloned().unwrap_or(serde_json::Value::Null);
    let ord = json_cmp(&va, &vb);
    if sort.desc {
        ord.reverse()
    } else {
        ord
    }
}

fn kway_merge(lists: Vec<Vec<serde_json::Value>>, sort: &SortSpec, limit: usize) -> Vec<serde_json::Value> {
    let mut heads = vec![0usize; lists.len()];
    let mut out = Vec::with_capacity(limit);

    while out.len() < limit {
        let mut best: Option<usize> = None;
        for i in 0..lists.len() {
            if heads[i] < lists[i].len() {
                match best {
                    None => best = Some(i),
                    Some(b) => {
                        if compare_by_sort(&lists[i][heads[i]], &lists[b][heads[b]], sort) == std::cmp::Ordering::Less {
                            best = Some(i);
                        }
                    }
                }
            }
        }
        match best {
            Some(i) => {
                out.push(lists[i][heads[i]].clone());
                heads[i] += 1;
            },
            None => break,
        }
    }
    out
}

fn set_path(map: &mut serde_json::Map<String, serde_json::Value>, parts: &[&str], val: serde_json::Value) {
    if parts.is_empty() {
        return;
    }
    if parts.len() == 1 {
        map.insert(parts[0].to_string(), val);
        return;
    }
    let entry = map.entry(parts[0].to_string()).or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    if !entry.is_object() {
        *entry = serde_json::Value::Object(serde_json::Map::new());
    }
    if let Some(m) = entry.as_object_mut() {
        set_path(m, &parts[1..], val);
    }
}

fn project(doc: &serde_json::Value, fields: &[String]) -> serde_json::Value {
    if fields.is_empty() {
        return doc.clone();
    }
    let mut out = serde_json::Map::new();
    for path in fields {
        if let Some(v) = get_path_value(doc, path) {
            let parts: Vec<&str> = path.split('.').collect();
            set_path(&mut out, &parts, v.clone());
        }
    }
    serde_json::Value::Object(out)
}

fn merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    match patch {
        serde_json::Value::Object(pmap) => {
            if !target.is_object() {
                *target = serde_json::Value::Object(serde_json::Map::new());
            }
            let tmap = target.as_object_mut().unwrap();
            for (k, v) in pmap {
                if v.is_null() {
                    tmap.remove(k);
                } else if let Some(existing) = tmap.get_mut(k) {
                    merge_patch(existing, v);
                } else {
                    let mut fresh = serde_json::Value::Null;
                    merge_patch(&mut fresh, v);
                    tmap.insert(k.clone(), fresh);
                }
            }
        },
        other => *target = other.clone(),
    }
}

fn matches_filter(doc: &serde_json::Value, filter: &Filter) -> bool {
    for (k, cond) in &filter.fields {
        let val = match get_path_value(doc, k) {
            Some(v) => v,
            None => return false,
        };

        if cond.is_object() {
            let cmp = cond.as_object().unwrap();

            if let Some(gt) = cmp.get("$gt") {
                if !val.as_f64().zip(gt.as_f64()).map_or(false, |(a,b)| a > b) {
                    return false;
                }
            }

            if let Some(gte) = cmp.get("$gte") {
                if !val.as_f64().zip(gte.as_f64()).map_or(false, |(a,b)| a >= b) {
                    return false;
                }
            }

            if let Some(lt) = cmp.get("$lt") {
                if !val.as_f64().zip(lt.as_f64()).map_or(false, |(a,b)| a < b) {
                    return false;
                }
            }

            if let Some(lte) = cmp.get("$lte") {
                if !val.as_f64().zip(lte.as_f64()).map_or(false, |(a,b)| a <= b) {
                    return false;
                }
            }

            if let Some(ne) = cmp.get("$ne") {
                if val == ne {
                    return false;
                }
            }

            if let Some(in_arr) = cmp.get("$in") {
                if let Some(arr) = in_arr.as_array() {
                    if !arr.contains(val) {
                        return false;
                    }
                }
            }
        } else {
            if val != cond {
                return false;
            }
        }
    }
    true
}

fn apply_demotion(repl: &mut ReplicationState, new_term: u64) -> Option<bool> {
    if new_term <= repl.term {
        return None;
    }
    repl.term = new_term;
    repl.voted_for = None;
    repl.is_leader = false;
    repl.last_heartbeat = Some(std::time::Instant::now());
    repl.last_replication = None;
    repl.was_receiving_replication = false;
    let restart = !repl.heartbeat_running;
    repl.heartbeat_running = true;
    Some(restart)
}

async fn discover_leader(state: &AppState) -> Option<String> {
    let mut peers: Vec<String> = state.config.replicas.clone();
    if let Some(r) = state.replication.as_ref() {
        if let Some(p) = r.read().unwrap().primary_addr.clone() {
            peers.push(p);
        }
    }
    peers.sort();
    peers.dedup();

    let mut best: Option<(u64, String)> = None;
    for peer in peers {
        let url = format!("{}/internal/heartbeat", peer);
        if let Ok(resp) = state.client.get(&url).send().await {
            if let Ok(hb) = resp.json::<serde_json::Value>().await {
                let role = hb.get("role").and_then(|v| v.as_str()).unwrap_or("");
                let term = hb.get("term").and_then(|v| v.as_u64()).unwrap_or(0);
                if role == "primary" && best.as_ref().map_or(true, |(t, _)| term > *t) {
                    best = Some((term, peer.clone()));
                }
            }
        }
    }
    best.map(|(_, url)| url)
}

async fn maybe_follow_new_leader(state: &AppState, current_primary: &str) {
    if state.is_leader() {
        return;
    }
    let leader = match discover_leader(state).await {
        Some(l) => l,
        None => return,
    };
    if leader == current_primary {
        return;
    }

    let changed = {
        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
        if repl.primary_addr.as_deref() == Some(leader.as_str()) {
            false
        } else {
            repl.primary_addr = Some(leader.clone());
            repl.last_heartbeat = Some(std::time::Instant::now());
            true
        }
    };

    if changed {
        println!("[failover] Following new leader {} (was {})", leader, current_primary);
        let state2 = state.clone();
        let leader2 = leader.clone();
        tokio::spawn(async move {
            resync_all_from(&state2, &leader2).await;
        });
    }
}

async fn resync_all_from(state: &AppState, leader: &str) {
    let db = match state.db.as_ref() {
        Some(d) => d.clone(),
        None => return,
    };

    let mut names: Vec<String> = { db.collections.read().unwrap().keys().cloned().collect() };
    if let Ok(entries) = fs::read_dir(&db.root_path) {
        for e in entries.flatten() {
            if e.path().is_dir() {
                if let Some(n) = e.file_name().to_str() {
                    if !n.contains('.') {
                        names.push(n.to_string());
                    }
                }
            }
        }
    }
    names.sort();
    names.dedup();

    for name in names {
        if let Err(e) = replica_sync_from_primary(&state.client, leader, &db, &name).await {
            eprintln!("[demote] resync of '{}' from {} failed: {}", name, leader, e);
        }
    }
}

async fn demote(state: &AppState, new_term: u64) {
    let restart = {
        let repl = match state.replication.as_ref() {
            Some(r) => r,
            None => return,
        };
        let mut g = repl.write().unwrap();
        match apply_demotion(&mut g, new_term) {
            Some(r) => r,
            None => return,
        }
    };

    let _ = ReplicationMeta { term: new_term, is_leader: false, voted_for: None }.save("./data");
    println!("[demote] Discovered higher term {}, stepping down to replica", new_term);

    if restart {
        heartbeat_poll_task(state.clone());
    }

    let state2 = state.clone();
    tokio::spawn(async move {
        if let Some(leader) = discover_leader(&state2).await {
            {
                let mut g = state2.replication.as_ref().unwrap().write().unwrap();
                g.primary_addr = Some(leader.clone());
            }
            println!("[demote] Following new leader {}; resyncing", leader);
            resync_all_from(&state2, &leader).await;
        } else {
            eprintln!("[demote] New leader not found yet; heartbeat poll will keep retrying");
        }
    });
}

enum ConflictKind {
    StaleTerm(u64),
    Gap(u64),
}

fn classify_conflict(body: &Option<serde_json::Value>) -> ConflictKind {
    if let Some(b) = body {
        if b.get("status").and_then(|s| s.as_str()) == Some("stale_term") {
            return ConflictKind::StaleTerm(b.get("term").and_then(|v| v.as_u64()).unwrap_or(0));
        }
        return ConflictKind::Gap(b.get("last_lsn").and_then(|v| v.as_u64()).unwrap_or(0));
    }
    ConflictKind::Gap(0)
}

fn forbidden_term(body: &Option<serde_json::Value>) -> u64 {
    body.as_ref()
        .and_then(|b| b.get("term").and_then(|v| v.as_u64()))
        .unwrap_or(0)
}

fn replicate_to_peers(
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
                    Ok(r) if r.status().is_success() => {},
                    Ok(r) if r.status() == StatusCode::CONFLICT => {
                        let body = r.json::<serde_json::Value>().await.ok();
                        match classify_conflict(&body) {
                            ConflictKind::StaleTerm(t) => {
                                eprintln!("[replication] Replica {} reports higher term {}; demoting", replica_url, t);
                                demote(&state, t).await;
                            },
                            ConflictKind::Gap(last_lsn) => {
                                eprintln!("[replication] Replica {} gap at lsn {} (its last_lsn={}), starting repair", replica_url, lsn, last_lsn);
                                let _ = repair_replica(state, replica_url.clone(), col, last_lsn).await;
                            }
                        }
                    },
                    Ok(r) if r.status() == StatusCode::FORBIDDEN => {
                        let body = r.json::<serde_json::Value>().await.ok();
                        let their_term = forbidden_term(&body);
                        if their_term > term {
                            eprintln!("[replication] Replica {} rejected us with higher term {}; demoting", replica_url, their_term);
                            demote(&state, their_term).await;
                        }
                    },
                    Ok(r) => {
                        eprintln!("[replication] Replica {} returned {} (term={}, lsn={})", replica_url, r.status(), term, lsn);
                    },
                    Err(e) => {
                        eprintln!("[replication] Replica {} failed: {} (term={}, lsn={})", replica_url, e, term, lsn);
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    });
}

fn contiguous_prefix(after_lsn: u64, mut frames: Vec<(u64, Vec<u8>)>) -> Vec<(u64, Vec<u8>)> {
    frames.sort_by_key(|(lsn, _)| *lsn);
    let mut expected = after_lsn + 1;
    let mut out = Vec::new();
    for (lsn, frame) in frames {
        if lsn == expected {
            out.push((lsn, frame));
            expected += 1;
        } else if lsn > expected {
            break;
        }
    }
    out
}

async fn trigger_resync(state: &AppState, replica_url: &str, collection: &str) {
    let url = format!("{}/internal/resync", replica_url);
    let body = ResyncRequest { collection: collection.to_string() };
    match state.client.post(&url).json(&body).send().await {
        Ok(r) if r.status().is_success() => {
            println!("[repair] Triggered snapshot resync on {} for '{}'", replica_url, collection);
        },
        Ok(r) => eprintln!("[repair] Resync trigger on {} returned {}", replica_url, r.status()),
        Err(e) => eprintln!("[repair] Resync trigger on {} failed: {}", replica_url, e),
    }
}

async fn probe_replica_lsn(state: &AppState, replica_url: &str) -> Option<u64> {
    let url = format!("{}/internal/heartbeat", replica_url);
    let r = state.client.get(&url).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    let v = r.json::<serde_json::Value>().await.ok()?;
    v.get("commit_index").and_then(|x| x.as_u64())
}

async fn repair_replica(state: AppState, replica_url: String, collection: String, reported_last_lsn: u64) -> bool {
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

    let target = db.global_commit_index.load(Ordering::SeqCst);

    let probed = probe_replica_lsn(&state, &replica_url).await.unwrap_or(0);
    let replica_last_lsn = reported_last_lsn.max(probed);

    if replica_last_lsn >= target {
        return true;
    }

    let col_scan = col.clone();
    let after = replica_last_lsn;
    let frames = match tokio::task::spawn_blocking(move || col_scan.read_frames_after(after, target)).await {
        Ok(Ok(f)) => f,
        _ => return false,
    };

    let contiguous = contiguous_prefix(replica_last_lsn, frames);
    let reaches_target = contiguous.last().map_or(false, |(lsn, _)| *lsn >= target);

    if !reaches_target {
        println!("[repair] Replica {} too far behind for '{}' (last_lsn={}, target={}), falling back to snapshot", replica_url, collection, replica_last_lsn, target);
        trigger_resync(&state, &replica_url, &collection).await;
        return false;
    }

    let term = state.current_term();
    let commit_index = db.global_commit_index.load(Ordering::SeqCst);
    let sent = contiguous.len();
    let mut prev = replica_last_lsn;

    for (lsn, frame) in contiguous {
        let req = ReplicateRequest {
            collection: collection.clone(),
            term,
            lsn,
            prev_lsn: prev,
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
                        eprintln!("[repair] Replica {} reports higher term {} during backfill; demoting", replica_url, t);
                        demote(&state, t).await;
                        return false;
                    },
                    ConflictKind::Gap(_) => {
                        eprintln!("[repair] Replica {} still gapped during backfill at lsn {}, falling back to snapshot", replica_url, lsn);
                        trigger_resync(&state, &replica_url, &collection).await;
                        return false;
                    }
                }
            },
            Ok(r) if r.status() == StatusCode::FORBIDDEN => {
                let body = r.json::<serde_json::Value>().await.ok();
                let their_term = forbidden_term(&body);
                if their_term > term {
                    eprintln!("[repair] Replica {} rejected us with higher term {}; demoting", replica_url, their_term);
                    demote(&state, their_term).await;
                }
                return false;
            },
            Ok(r) => {
                eprintln!("[repair] Replica {} returned {} during backfill", replica_url, r.status());
                return false;
            },
            Err(e) => {
                eprintln!("[repair] Replica {} unreachable during backfill: {}", replica_url, e);
                return false;
            }
        }
    }

    println!("[repair] Streamed {} frames to {}; caught up to lsn {} for '{}'", sent, replica_url, prev, collection);
    prev >= target
}

#[derive(Clone, Copy)]
enum WriteConcern {
    Local,
    Majority,
    All,
    N(usize),
}

fn parse_write_concern(w: Option<&str>) -> WriteConcern {
    match w {
        None | Some("1") => WriteConcern::Local,
        Some("majority") => WriteConcern::Majority,
        Some("all") => WriteConcern::All,
        Some(s) => s.parse::<usize>().map(WriteConcern::N).unwrap_or(WriteConcern::Local),
    }
}

fn required_acks(wc: &WriteConcern, replica_count: usize) -> usize {
    let total = 1 + replica_count;
    match wc {
        WriteConcern::Local => 1,
        WriteConcern::Majority => total / 2 + 1,
        WriteConcern::All => total,
        WriteConcern::N(n) => (*n).max(1).min(total),
    }
}

struct WriteOutcome {
    met: bool,
    acks: usize,
    required: usize,
    existed: bool,
}

struct PendingWrite {
    frame: Vec<u8>,
    term: u64,
    lsn: u64,
    existed: bool,
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
        Ok(r) if r.status().is_success() => true,
        Ok(r) if r.status() == StatusCode::CONFLICT => {
            let body = r.json::<serde_json::Value>().await.ok();
            match classify_conflict(&body) {
                ConflictKind::StaleTerm(t) => {
                    demote(state, t).await;
                    false
                },
                ConflictKind::Gap(last_lsn) => {
                    repair_replica(state.clone(), replica_url.to_string(), collection.to_string(), last_lsn).await
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

async fn replicate_and_await(
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

enum ForwardMethod {
    Put,
    Patch,
    Delete,
}

fn build_forward(client: &reqwest::Client, method: &ForwardMethod, url: &str, body: Option<&CreateDoc>) -> reqwest::RequestBuilder {
    let rb = match method {
        ForwardMethod::Put => client.put(url),
        ForwardMethod::Patch => client.patch(url),
        ForwardMethod::Delete => client.delete(url),
    };
    match body {
        Some(b) => rb.json(b),
        None => rb,
    }
}

fn authoritative_write_status(s: StatusCode) -> bool {
    s.is_success()
        || s == StatusCode::BAD_REQUEST
        || s == StatusCode::NOT_FOUND
        || s == StatusCode::CONFLICT
        || s == StatusCode::PAYLOAD_TOO_LARGE
        || s == StatusCode::UNPROCESSABLE_ENTITY
}

async fn router_forward_write(
    state: &AppState,
    col_name: &str,
    key: &str,
    method: ForwardMethod,
    body: Option<&CreateDoc>,
    wc_query: &str,
) -> Result<reqwest::Response, axum::response::Response> {
    let hash = hash_key(col_name, key);

    let (effective_url, original_url, replica_urls) = match state.get_effective_shard_url(hash) {
        Some(t) => t,
        None => return Err((StatusCode::BAD_REQUEST, "Key not owned by any shard").into_response()),
    };

    let full_url = format!("{}/collections/{}/docs/{}{}", effective_url, col_name, key, wc_query);
    if let Ok(r) = build_forward(&state.client, &method, &full_url, body).send().await {
        if authoritative_write_status(r.status()) {
            if effective_url != original_url {
                state.set_primary_override(&original_url, &effective_url);
            }
            return Ok(r);
        }
    }

    let failover_lock = {
        let mut locks = state.shard_failover_locks.lock().unwrap();
        locks.entry(original_url.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    let _guard = failover_lock.lock().await;

    if let Some((latest_url, _, _)) = state.get_effective_shard_url(hash) {
        if latest_url != effective_url {
            let retry_url = format!("{}/collections/{}/docs/{}{}", latest_url, col_name, key, wc_query);
            if let Ok(r) = build_forward(&state.client, &method, &retry_url, body).send().await {
                if authoritative_write_status(r.status()) {
                    return Ok(r);
                }
            }
        }
    }

    state.primary_overrides.lock().unwrap().remove(&original_url);
    for replica in &replica_urls {
        let fallback_url = format!("{}/collections/{}/docs/{}{}", replica, col_name, key, wc_query);
        if let Ok(r) = build_forward(&state.client, &method, &fallback_url, body).send().await {
            if authoritative_write_status(r.status()) {
                state.set_primary_override(&original_url, replica);
                println!("[router] Cached new primary: {} -> {}", original_url, replica);
                return Ok(r);
            }
        }
    }

    Err((StatusCode::BAD_GATEWAY, "All shard nodes unreachable").into_response())
}

async fn router_forward_bulk(
    state: &AppState,
    col_name: &str,
    effective_url: &str,
    original_url: &str,
    replica_urls: &[String],
    body: &[serde_json::Value],
    wc_query: &str,
) -> Result<reqwest::Response, String> {
    let full_url = format!("{}/collections/{}/docs/bulk{}", effective_url, col_name, wc_query);
    if let Ok(r) = state.client.post(&full_url).json(body).send().await {
        if authoritative_write_status(r.status()) {
            if effective_url != original_url {
                state.set_primary_override(original_url, effective_url);
            }
            return Ok(r);
        }
    }

    let failover_lock = {
        let mut locks = state.shard_failover_locks.lock().unwrap();
        locks.entry(original_url.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    let _guard = failover_lock.lock().await;

    state.primary_overrides.lock().unwrap().remove(original_url);
    for replica in replica_urls {
        let fallback_url = format!("{}/collections/{}/docs/bulk{}", replica, col_name, wc_query);
        if let Ok(r) = state.client.post(&fallback_url).json(body).send().await {
            if authoritative_write_status(r.status()) {
                state.set_primary_override(original_url, replica);
                println!("[router] Cached new primary: {} -> {}", original_url, replica);
                return Ok(r);
            }
        }
    }

    Err("All shard nodes unreachable".to_string())
}

async fn bulk_router_forward(
    state: &AppState,
    col_name: &str,
    docs: Vec<BulkDoc>,
    wc_query: &str,
) -> axum::response::Response {
    let n = docs.len();
    let mut groups: HashMap<String, (String, String, Vec<String>, Vec<(usize, String, serde_json::Value)>)> = HashMap::new();

    for (idx, d) in docs.into_iter().enumerate() {
        let id = d.id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let hash = hash_key(col_name, &id);
        let (effective_url, original_url, replica_urls) = match state.get_effective_shard_url(hash) {
            Some(t) => t,
            None => return err_json(StatusCode::BAD_REQUEST, format!("Key {} not owned by any shard", id)),
        };
        groups.entry(original_url.clone())
            .or_insert_with(|| (effective_url, original_url, replica_urls, Vec::new()))
            .3.push((idx, id, d.value));
    }

    let futures = groups.into_values().map(|(effective_url, original_url, replica_urls, items)| {
        let state = state.clone();
        let col_name = col_name.to_string();
        let wc_query = wc_query.to_string();
        async move {
            let body: Vec<serde_json::Value> = items.iter()
                .map(|(_, id, value)| serde_json::json!({"id": id, "value": value}))
                .collect();
            let resp = router_forward_bulk(&state, &col_name, &effective_url, &original_url, &replica_urls, &body, &wc_query).await;
            (items, resp)
        }
    });

    let results = futures::future::join_all(futures).await;

    let mut ordered: Vec<serde_json::Value> = vec![serde_json::Value::Null; n];
    for (items, resp) in results {
        match resp {
            Ok(r) => {
                let shard_results: Vec<serde_json::Value> = r.json::<serde_json::Value>().await.ok()
                    .and_then(|b| b.get("results").and_then(|v| v.as_array().cloned()))
                    .unwrap_or_default();
                if shard_results.len() == items.len() {
                    for ((idx, _, _), res) in items.iter().zip(shard_results.into_iter()) {
                        ordered[*idx] = res;
                    }
                } else {
                    for (idx, id, _) in &items {
                        ordered[*idx] = serde_json::json!({"id": id, "status": "error", "error": "malformed shard response"});
                    }
                }
            }
            Err(e) => {
                for (idx, id, _) in &items {
                    ordered[*idx] = serde_json::json!({"id": id, "status": "error", "error": e});
                }
            }
        }
    }

    (StatusCode::CREATED, Json(serde_json::json!({"results": ordered}))).into_response()
}

async fn passthrough_json(r: reqwest::Response) -> axum::response::Response {
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    let json_body: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body));
    (status, Json(json_body)).into_response()
}

const ROUTER_PROBE_INTERVAL_SECS: u64 = 3;

async fn probe_node(client: &reqwest::Client, url: &str) -> Option<(String, u64)> {
    let hb = format!("{}/internal/heartbeat", url);
    let r = client.get(&hb).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    let v = r.json::<serde_json::Value>().await.ok()?;
    let role = v.get("role").and_then(|x| x.as_str())?.to_string();
    let term = v.get("term").and_then(|x| x.as_u64()).unwrap_or(0);
    Some((role, term))
}

fn select_primary(probes: &[(String, Option<(String, u64)>)]) -> Option<String> {
    let mut best: Option<(u64, String)> = None;
    for (url, res) in probes {
        if let Some((role, term)) = res {
            if role == "primary" && best.as_ref().map_or(true, |(t, _)| *term > *t) {
                best = Some((*term, url.clone()));
            }
        }
    }
    best.map(|(_, u)| u)
}

fn unique_shards(state: &AppState) -> Vec<(String, Vec<String>)> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for shard in &state.config.shard_map {
        if seen.insert(shard.node_url.clone()) {
            out.push((shard.node_url.clone(), shard.replica_urls.clone()));
        }
    }
    out
}

fn router_probe_task(state: AppState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(ROUTER_PROBE_INTERVAL_SECS)).await;

            for (original, replicas) in unique_shards(&state) {
                let effective = state.effective_primary(&original);

                if let Some((role, _term)) = probe_node(&state.client, &effective).await {
                    if role == "primary" {
                        if effective != original {
                            state.set_primary_override(&original, &effective);
                        }
                        continue;
                    }
                }

                let mut candidates = vec![original.clone()];
                candidates.extend(replicas.iter().cloned());
                candidates.sort();
                candidates.dedup();

                let mut probes = Vec::new();
                for c in candidates {
                    let res = probe_node(&state.client, &c).await;
                    probes.push((c, res));
                }

                match select_primary(&probes) {
                    Some(winner) => {
                        if winner == original {
                            state.clear_primary_override(&original);
                        } else if winner != effective {
                            state.set_primary_override(&original, &winner);
                            println!("[router-probe] Shard {} primary is now {}", original, winner);
                        } else {
                            state.set_primary_override(&original, &winner);
                        }
                    },
                    None => {
                        eprintln!("[router-probe] Shard {} has no reachable primary (election in progress?)", original);
                    }
                }
            }
        }
    });
}

enum ReadPreference {
    Primary,
    Replica,
}

fn parse_read_pref(r: Option<&str>) -> ReadPreference {
    match r {
        Some("replica") => ReadPreference::Replica,
        _ => ReadPreference::Primary,
    }
}

fn read_targets(pref: &ReadPreference, effective_primary: &str, replicas: &[String], rr: usize) -> Vec<String> {
    let mut targets = Vec::new();
    match pref {
        ReadPreference::Primary => {
            targets.push(effective_primary.to_string());
            for r in replicas {
                targets.push(r.clone());
            }
        },
        ReadPreference::Replica => {
            let n = replicas.len();
            if n == 0 {
                targets.push(effective_primary.to_string());
            } else {
                for i in 0..n {
                    targets.push(replicas[(rr + i) % n].clone());
                }
                targets.push(effective_primary.to_string());
            }
        }
    }
    let mut seen = HashSet::new();
    targets.retain(|t| seen.insert(t.clone()));
    targets
}

async fn router_read_doc(state: &AppState, col_name: &str, id: &str, pref: ReadPreference) -> axum::response::Response {
    let hash = hash_key(col_name, id);
    let (effective, _original, replicas) = match state.get_effective_shard_url(hash) {
        Some(t) => t,
        None => return (StatusCode::BAD_REQUEST, "Key not owned by any shard").into_response(),
    };

    let path = format!("/collections/{}/docs/{}", col_name, id);
    let rr = state.read_rr.fetch_add(1, Ordering::Relaxed);
    let targets = read_targets(&pref, &effective, &replicas, rr);

    for target in targets {
        let url = format!("{}{}", target, path);
        if let Ok(r) = state.client.get(&url).send().await {
            let status = r.status();
            if status.is_success() || status == StatusCode::NOT_FOUND {
                return passthrough_json(r).await;
            }
        }
    }

    (StatusCode::BAD_GATEWAY, "No shard node could serve the read").into_response()
}

fn err_json(status: StatusCode, msg: String) -> axum::response::Response {
    (status, Json(serde_json::json!({"error": msg}))).into_response()
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
        let mut index = col.index.write().unwrap();
        if is_delete {
            index.remove(&key);
        } else {
            index.insert(key.clone(), IndexEntry { wal_id, offset });
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
    let prev_lsn = pending.lsn.saturating_sub(1);

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

async fn local_write(
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

async fn local_patch(
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
        for (key, _, wal_id, offset, _) in &frames {
            index.insert(key.clone(), IndexEntry { wal_id: *wal_id, offset: *offset });
        }
    }

    Ok(frames.into_iter().zip(existed.into_iter())
        .map(|((_, frame, _, _, lsn), existed)| PendingWrite { frame, term, lsn, existed })
        .collect())
}

async fn local_write_batch(
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

#[derive(Deserialize)]
struct WriteConcernParams {
    w: Option<String>,
    wtimeout: Option<u64>,
}

const DEFAULT_WTIMEOUT_MS: u64 = 5000;

fn wc_query_string(p: &WriteConcernParams) -> String {
    let mut parts = Vec::new();
    if let Some(w) = &p.w {
        parts.push(format!("w={}", w));
    }
    if let Some(t) = p.wtimeout {
        parts.push(format!("wtimeout={}", t));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

async fn create_doc(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
    Query(wcp): Query<WriteConcernParams>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    let id = Uuid::new_v4().to_string();

    if state.config.role == "router" {
        let wc_query = wc_query_string(&wcp);
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Put, Some(&payload), &wc_query).await {
            Ok(r) => passthrough_json(r).await,
            Err(resp) => resp,
        };
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    match local_write(&state, &col_name, id.clone(), Some(payload.value), wc, wtimeout).await {
        Ok(o) if o.met => (StatusCode::CREATED, Json(serde_json::json!({"id": id, "status": "created"}))).into_response(),
        Ok(o) => (StatusCode::ACCEPTED, Json(serde_json::json!({
            "id": id,
            "status": "created",
            "warning": "write concern not met",
            "acks": o.acks,
            "required": o.required,
        }))).into_response(),
        Err(resp) => resp,
    }
}

async fn put_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Query(wcp): Query<WriteConcernParams>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if state.config.role == "router" {
        let wc_query = wc_query_string(&wcp);
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Put, Some(&payload), &wc_query).await {
            Ok(r) => passthrough_json(r).await,
            Err(resp) => resp,
        };
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    match local_write(&state, &col_name, id.clone(), Some(payload.value), wc, wtimeout).await {
        Ok(o) if o.met => {
            let status = if o.existed { StatusCode::OK } else { StatusCode::CREATED };
            let label = if o.existed { "replaced" } else { "created" };
            (status, Json(serde_json::json!({"id": id, "status": label}))).into_response()
        },
        Ok(o) => (StatusCode::ACCEPTED, Json(serde_json::json!({
            "id": id,
            "status": if o.existed { "replaced" } else { "created" },
            "warning": "write concern not met",
            "acks": o.acks,
            "required": o.required,
        }))).into_response(),
        Err(resp) => resp,
    }
}

async fn bulk_create_docs(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
    Query(wcp): Query<WriteConcernParams>,
    Json(payload): Json<Vec<BulkDoc>>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if payload.is_empty() {
        return err_json(StatusCode::BAD_REQUEST, "bulk request must contain at least one document".to_string());
    }

    let wc_query = wc_query_string(&wcp);

    if state.config.role == "router" {
        return bulk_router_forward(&state, &col_name, payload, &wc_query).await;
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    let ids: Vec<String> = payload.iter()
        .map(|d| d.id.clone().unwrap_or_else(|| Uuid::new_v4().to_string()))
        .collect();
    let items: Vec<(String, serde_json::Value)> = ids.iter().cloned()
        .zip(payload.into_iter().map(|d| d.value))
        .collect();

    match local_write_batch(&state, &col_name, items, wc, wtimeout).await {
        Ok(outcomes) => {
            let results: Vec<serde_json::Value> = ids.into_iter().zip(outcomes.into_iter()).map(|(id, o)| {
                if o.met {
                    serde_json::json!({"id": id, "status": "created"})
                } else {
                    serde_json::json!({
                        "id": id,
                        "status": "created",
                        "warning": "write concern not met",
                        "acks": o.acks,
                        "required": o.required,
                    })
                }
            }).collect();
            (StatusCode::CREATED, Json(serde_json::json!({"results": results}))).into_response()
        }
        Err(resp) => resp,
    }
}

async fn get_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Query(rp): Query<ReadParams>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        let pref = parse_read_pref(rp.read.as_deref());
        return router_read_doc(&state, &col_name, &id, pref).await;
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let key = id.clone();
    let col_clone = col.clone();

    match tokio::task::spawn_blocking(move || col_clone.get(&key)).await {
        Ok(Ok(Some(val))) => (StatusCode::OK, Json(val)).into_response(),
        Ok(Ok(None)) => err_json(StatusCode::NOT_FOUND, "not found".to_string()),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn update_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Query(wcp): Query<WriteConcernParams>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if payload.value.is_null() {
        return err_json(StatusCode::BAD_REQUEST, "PATCH body must not be null; use DELETE to remove a document".to_string());
    }

    if state.config.role == "router" {
        let wc_query = wc_query_string(&wcp);
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Patch, Some(&payload), &wc_query).await {
            Ok(r) => passthrough_json(r).await,
            Err(resp) => resp,
        };
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    match local_patch(&state, &col_name, id.clone(), payload.value, wc, wtimeout).await {
        Ok(None) => err_json(StatusCode::NOT_FOUND, "not found".to_string()),
        Ok(Some(o)) if o.met => (StatusCode::OK, Json(serde_json::json!({"id": id, "status": "updated"}))).into_response(),
        Ok(Some(o)) => (StatusCode::ACCEPTED, Json(serde_json::json!({
            "id": id,
            "status": "updated",
            "warning": "write concern not met",
            "acks": o.acks,
            "required": o.required,
        }))).into_response(),
        Err(resp) => resp,
    }
}

async fn delete_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Query(wcp): Query<WriteConcernParams>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if state.config.role == "router" {
        let wc_query = wc_query_string(&wcp);
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Delete, None, &wc_query).await {
            Ok(r) => passthrough_json(r).await,
            Err(resp) => resp,
        };
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    match local_write(&state, &col_name, id.clone(), None, wc, wtimeout).await {
        Ok(o) if o.met => (StatusCode::OK, Json(serde_json::json!({"status": "deleted", "existed": o.existed}))).into_response(),
        Ok(o) => (StatusCode::ACCEPTED, Json(serde_json::json!({
            "status": "deleted",
            "existed": o.existed,
            "warning": "write concern not met",
            "acks": o.acks,
            "required": o.required,
        }))).into_response(),
        Err(resp) => resp,
    }
}

async fn list_docs(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return (StatusCode::NOT_IMPLEMENTED, "Use /query for cross-shard iteration").into_response();
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let col_clone = col.clone();
    match tokio::task::spawn_blocking(move || col_clone.list_all()).await {
        Ok(Ok(vals)) => (StatusCode::OK, Json(vals)).into_response(),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn admin_call(client: &reqwest::Client, post: bool, url: &str) -> Option<(StatusCode, serde_json::Value)> {
    let rb = if post { client.post(url) } else { client.delete(url) };
    let r = rb.send().await.ok()?;
    let status = r.status();
    let body = r.json::<serde_json::Value>().await.unwrap_or(serde_json::Value::Null);
    Some((status, body))
}

fn node_result(node: &str, outcome: Option<(StatusCode, serde_json::Value)>) -> serde_json::Value {
    match outcome {
        Some((status, body)) => serde_json::json!({
            "node": node,
            "status": status.as_u16(),
            "response": body,
        }),
        None => serde_json::json!({
            "node": node,
            "status": serde_json::Value::Null,
            "error": "unreachable",
        }),
    }
}

async fn router_fanout_maintenance(state: &AppState, col_name: &str, action: &str) -> axum::response::Response {
    let mut targets = Vec::new();
    for (original, replicas) in unique_shards(state) {
        targets.push(state.effective_primary(&original));
        for r in replicas {
            targets.push(r);
        }
    }
    targets.sort();
    targets.dedup();

    let results = futures::future::join_all(targets.into_iter().map(|node| {
        let client = state.client.clone();
        let col_name = col_name.to_string();
        let action = action.to_string();
        async move {
            let url = format!("{}/collections/{}/{}", node, col_name, action);
            let outcome = admin_call(&client, true, &url).await;
            node_result(&node, outcome)
        }
    })).await;

    let all_ok = results.iter().all(|r| r.get("status").and_then(|s| s.as_u64()).map_or(false, |s| s < 300));
    let status = if all_ok { StatusCode::OK } else { StatusCode::MULTI_STATUS };
    (status, Json(serde_json::json!({"nodes": results}))).into_response()
}

async fn router_fanout_drop(state: &AppState, col_name: &str) -> axum::response::Response {
    let results = futures::future::join_all(unique_shards(state).into_iter().map(|(original, replicas)| {
        let state = state.clone();
        let col_name = col_name.to_string();
        async move {
            let effective = state.effective_primary(&original);
            let mut candidates = vec![effective.clone()];
            if original != effective {
                candidates.push(original.clone());
            }
            candidates.extend(replicas.into_iter().filter(|r| *r != effective));

            for node in candidates {
                let url = format!("{}/collections/{}", node, col_name);
                if let Some((status, body)) = admin_call(&state.client, false, &url).await {
                    if authoritative_write_status(status) {
                        if node != original {
                            state.set_primary_override(&original, &node);
                        }
                        return node_result(&node, Some((status, body)));
                    }
                }
            }
            node_result(&original, None)
        }
    })).await;

    let all_ok = results.iter().all(|r| r.get("status").and_then(|s| s.as_u64()).map_or(false, |s| s < 300));
    let status = if all_ok { StatusCode::OK } else { StatusCode::MULTI_STATUS };
    (status, Json(serde_json::json!({"shards": results}))).into_response()
}

async fn list_collections(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        let mut targets = Vec::new();
        for (original, replicas) in unique_shards(&state) {
            targets.push((state.effective_primary(&original), replicas));
        }

        let per_shard = futures::future::join_all(targets.into_iter().map(|(primary, replicas)| {
            let client = state.client.clone();
            async move {
                let mut candidates = vec![primary];
                candidates.extend(replicas);
                for node in candidates {
                    let url = format!("{}/collections", node);
                    if let Ok(r) = client.get(&url).send().await {
                        if r.status().is_success() {
                            if let Ok(body) = r.json::<serde_json::Value>().await {
                                return body.get("collections")
                                    .and_then(|c| c.as_array().cloned())
                                    .unwrap_or_default();
                            }
                        }
                    }
                }
                Vec::new()
            }
        })).await;

        let mut names: HashSet<String> = HashSet::new();
        for list in per_shard {
            for v in list {
                if let Some(s) = v.as_str() {
                    names.insert(s.to_string());
                }
            }
        }
        let mut out: Vec<String> = names.into_iter().collect();
        out.sort();
        return (StatusCode::OK, Json(serde_json::json!({"collections": out}))).into_response();
    }

    let db = state.db.as_ref().unwrap().clone();
    match tokio::task::spawn_blocking(move || db.list_collections()).await {
        Ok(Ok(names)) => (StatusCode::OK, Json(serde_json::json!({"collections": names}))).into_response(),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn drop_collection(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return router_fanout_drop(&state, &col_name).await;
    }

    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    let term = state.current_term();
    let replicas = state.get_replicas();

    let db = state.db.as_ref().unwrap().clone();
    let name = col_name.clone();
    let existed = match tokio::task::spawn_blocking(move || db.drop_collection(&name)).await {
        Ok(Ok(e)) => e,
        Ok(Err(e)) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let acks = futures::future::join_all(replicas.iter().map(|replica| {
        let client = state.client.clone();
        let url = format!("{}/internal/drop", replica);
        let req = DropRequest { collection: col_name.clone(), term };
        async move {
            match client.post(&url).json(&req).send().await {
                Ok(r) if r.status().is_success() => true,
                _ => false,
            }
        }
    })).await;

    let replicated = acks.iter().filter(|ok| **ok).count();
    println!("[{}] Collection dropped; {}/{} replicas acked", col_name, replicated, replicas.len());

    (StatusCode::OK, Json(serde_json::json!({
        "collection": col_name,
        "status": "dropped",
        "existed": existed,
        "replicas_acked": replicated,
        "replicas": replicas.len(),
    }))).into_response()
}

async fn compact_collection(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return router_fanout_maintenance(&state, &col_name, "compact").await;
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let col_clone = col.clone();
    match tokio::task::spawn_blocking(move || col_clone.compact()).await {
        Ok(Ok(())) => {
            let wal = col.wal_writer.lock().unwrap();
            (StatusCode::OK, Json(serde_json::json!({
                "collection": col_name,
                "status": "compacted",
                "wal_id": wal.current_wal_id,
                "wal_size": wal.current_wal_size,
                "documents": col.index.read().unwrap().len(),
            }))).into_response()
        },
        Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => {
            err_json(StatusCode::CONFLICT, e.to_string())
        },
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn snapshot_collection(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return router_fanout_maintenance(&state, &col_name, "snapshot").await;
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let col_clone = col.clone();
    match tokio::task::spawn_blocking(move || col_clone.save_index()).await {
        Ok(Ok(())) => {
            let wal = col.wal_writer.lock().unwrap();
            (StatusCode::OK, Json(serde_json::json!({
                "collection": col_name,
                "status": "snapshotted",
                "last_lsn": wal.last_appended_lsn,
                "documents": col.index.read().unwrap().len(),
            }))).into_response()
        },
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Serialize, Deserialize)]
struct DropRequest {
    collection: String,
    term: u64,
}

async fn internal_drop_handler(
    State(state): State<AppState>,
    Json(req): Json<DropRequest>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() || state.is_leader() {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({
            "status": "not_a_replica",
            "term": state.current_term(),
        }))).into_response();
    }

    let our_term = state.current_term();
    if req.term < our_term {
        return (StatusCode::CONFLICT, Json(serde_json::json!({
            "status": "stale_term",
            "term": our_term,
        }))).into_response();
    }

    let db = state.db.as_ref().unwrap().clone();
    let name = req.collection.clone();
    match tokio::task::spawn_blocking(move || db.drop_collection(&name)).await {
        Ok(Ok(existed)) => {
            println!("[replica] Dropped collection '{}' on primary's instruction", req.collection);
            (StatusCode::OK, Json(serde_json::json!({"status": "dropped", "existed": existed}))).into_response()
        },
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn query_docs(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
    Query(params): Query<QueryParams>,
    _req: axum::extract::Request,
) -> impl axum::response::IntoResponse {
    let limit = params.limit.unwrap_or(100).max(1);
    let sort = parse_sort(params.sort.as_deref());
    let fields = parse_fields(params.fields.as_deref());

    if state.config.role == "router" {
        let pref = parse_read_pref(params.read.as_deref());
        let incoming = if sort.is_none() {
            params.cursor.as_deref().and_then(decode_cursor)
        } else {
            None
        };
        let shards = unique_shards(&state);
        let n = shards.len().max(1);
        let per_shard = if sort.is_some() { limit } else { ((limit + n - 1) / n).max(1) };

        let mut futures = Vec::new();
        for (original, replicas) in shards {
            let after: Option<String> = if sort.is_some() {
                None
            } else {
                match &incoming {
                    Some(c) => match c.positions.get(&original) {
                        Some(k) => Some(k.clone()),
                        None => continue,
                    },
                    None => None,
                }
            };

            let effective = state.effective_primary(&original);
            let rr = state.read_rr.fetch_add(1, Ordering::Relaxed);
            let targets = read_targets(&pref, &effective, &replicas, rr);
            let client = state.client.clone();
            let col = col_name.clone();

            let mut q: Vec<(String, String)> = vec![("limit".to_string(), per_shard.to_string())];
            if let Some(s) = &params.start { q.push(("start".to_string(), s.clone())); }
            if let Some(e) = &params.end { q.push(("end".to_string(), e.clone())); }
            if let Some(f) = &params.filter { q.push(("filter".to_string(), f.clone())); }
            if let Some(s) = &params.sort { q.push(("sort".to_string(), s.clone())); }
            if let Some(a) = &after { q.push(("cursor".to_string(), a.clone())); }

            futures.push(tokio::spawn(async move {
                for target in targets {
                    let url = format!("{}/collections/{}/query", target, col);
                    if let Ok(res) = client.get(&url).query(&q).send().await {
                        if res.status().is_success() {
                            if let Ok(page) = res.json::<QueryPage>().await {
                                return (original, Some(page));
                            }
                        }
                    }
                }
                (original, None)
            }));
        }

        let joined = futures::future::join_all(futures).await;

        if let Some(sort) = &sort {
            let mut lists = Vec::new();
            for res in joined {
                let (_original, page) = match res {
                    Ok(t) => t,
                    Err(_) => return (StatusCode::BAD_GATEWAY, "Shard query task failed").into_response(),
                };
                match page {
                    Some(p) => lists.push(p.items),
                    None => return (StatusCode::BAD_GATEWAY, "Shard query failed").into_response(),
                }
            }
            let merged = kway_merge(lists, sort, limit);
            let items: Vec<serde_json::Value> = merged.iter().map(|v| project(v, &fields)).collect();
            return (StatusCode::OK, Json(QueryPage { items, next_cursor: None })).into_response();
        }

        let mut merged = Vec::new();
        let mut positions = BTreeMap::new();
        for res in joined {
            let (original, page) = match res {
                Ok(t) => t,
                Err(_) => return (StatusCode::BAD_GATEWAY, "Shard query task failed").into_response(),
            };
            match page {
                Some(p) => {
                    for item in p.items {
                        merged.push(project(&item, &fields));
                    }
                    if let Some(k) = p.next_cursor {
                        positions.insert(original, k);
                    }
                },
                None => return (StatusCode::BAD_GATEWAY, "Shard query failed").into_response(),
            }
        }

        let next_cursor = if positions.is_empty() {
            None
        } else {
            Some(encode_cursor(&ShardCursor { positions }))
        };

        return (StatusCode::OK, Json(QueryPage { items: merged, next_cursor })).into_response();
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let col_clone = col.clone();
    let filter_obj: Option<Filter> = params.filter
        .as_ref()
        .and_then(|f| serde_json::from_str::<Filter>(f).ok());
    let after = params.cursor.clone();
    let start = params.start.clone();
    let end = params.end.clone();

    let result = tokio::task::spawn_blocking(move || -> io::Result<(Vec<serde_json::Value>, Option<String>)> {
        if let Some(sort) = &sort {
            let mut items = Vec::new();
            for (key, _entry) in col_clone.range_from(None, start.as_deref(), end.as_deref()).into_iter() {
                if let Some(val) = col_clone.get(&key)? {
                    let matched = filter_obj.as_ref().map_or(true, |f| matches_filter(&val, f));
                    if matched {
                        items.push(val);
                    }
                }
            }
            items.sort_by(|a, b| compare_by_sort(a, b, sort));
            items.truncate(limit);
            let projected = items.iter().map(|v| project(v, &fields)).collect();
            Ok((projected, None))
        } else {
            let (items, next_cursor) = col_clone.query_page(after.as_deref(), start.as_deref(), end.as_deref(), &filter_obj, limit)?;
            let projected = items.iter().map(|v| project(v, &fields)).collect();
            Ok((projected, next_cursor))
        }
    }).await;

    match result {
        Ok(Ok((items, next_cursor))) => (StatusCode::OK, Json(QueryPage { items, next_cursor })).into_response(),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Serialize, Deserialize)]
struct SnapshotFileEntry {
    filename: String,
    #[serde(with = "base64_bytes")]
    data: Vec<u8>,
}

async fn replicate_handler(
    State(state): State<AppState>,
    Json(req): Json<ReplicateRequest>,
) -> impl axum::response::IntoResponse {
    if let Some(idx) = req.commit_index {
        if let Some(ref repl) = state.replication {
            let mut r = repl.write().unwrap();
            r.last_known_primary_position = Some(idx);
        }
    }

    if !state.is_shard() || state.is_leader() {
        let our_term = state.current_term();
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({
            "status": "not_a_replica",
            "term": our_term,
        }))).into_response();
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
                let _ = ReplicationMeta { term: t, is_leader: false, voted_for: None }.save("./data");
                println!("[replicate] Adopted higher term {} from primary", t);
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

    let frame = req.wal_frame;

    if frame.len() >= HEADER_LEN {
        let frame_lsn = u64::from_le_bytes(frame[16..24].try_into().unwrap());
        if frame_lsn != req.lsn {
            return (StatusCode::BAD_REQUEST, "Frame lsn does not match request lsn").into_response();
        }
    }

    let col_clone = col.clone();
    let prev_lsn = req.prev_lsn;

    let entry_opt = if frame.len() >= HEADER_LEN {
        serde_json::from_slice::<LogEntry>(&frame[HEADER_LEN..]).ok()
    } else {
        None
    };

    match tokio::task::spawn_blocking(move || col_clone.append_raw_frame(&frame, prev_lsn)).await {
        Ok(Ok(ReplicaApply::Applied { wal_id, offset, lsn })) => {
            let commit_rx = col.enqueue_commit();
            match commit_rx.await {
                Ok(Ok(())) => {
                    if let Some(entry) = entry_opt {
                        let mut index = col.index.write().unwrap();
                        match entry {
                            LogEntry::Put { key, .. } => {
                                index.insert(key, IndexEntry { wal_id, offset });
                            },
                            LogEntry::Del { key, .. } => {
                                index.remove(&key);
                            }
                        }
                    }

                    if let Some(ref repl) = state.replication {
                        let mut r = repl.write().unwrap();
                        r.last_replication = Some(std::time::Instant::now());
                        r.was_receiving_replication = true;
                    }
                    (StatusCode::OK, Json(serde_json::json!({"status": "applied", "lsn": lsn}))).into_response()
                },
                Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        },
        Ok(Ok(ReplicaApply::Duplicate { last_lsn })) => {
            if let Some(ref repl) = state.replication {
                let mut r = repl.write().unwrap();
                r.last_replication = Some(std::time::Instant::now());
                r.was_receiving_replication = true;
            }
            (StatusCode::OK, Json(serde_json::json!({"status": "duplicate", "last_lsn": last_lsn}))).into_response()
        },
        Ok(Ok(ReplicaApply::Gap { last_lsn })) => {
            eprintln!("[replicate] Gap detected: got prev_lsn {} but replica is at lsn {}", req.prev_lsn, last_lsn);
            (StatusCode::CONFLICT, Json(serde_json::json!({"status": "gap", "last_lsn": last_lsn}))).into_response()
        },
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Serialize, Deserialize)]
struct ResyncRequest {
    collection: String,
}

async fn resync_handler(
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

    tokio::spawn(async move {
        if let Err(e) = replica_sync_from_primary(&client, &primary_addr, &db, &col).await {
            eprintln!("[resync] Failed for '{}': {}", col, e);
        } else if let Some(r) = repl {
            let mut g = r.write().unwrap();
            g.last_replication = Some(std::time::Instant::now());
            g.was_receiving_replication = true;
        }
        resyncing.lock().unwrap().remove(&col);
    });

    (StatusCode::OK, "resync started").into_response()
}

async fn heartbeat_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    let term = state.current_term();
    let role = if state.is_leader() { "primary" } else { "replica" };
    (StatusCode::OK, Json(serde_json::json!({
        "term": term,
        "role": role,
        "node_id": state.config.node_id,
        "commit_index": state.db.as_ref().map_or(0, |db| db.global_commit_index.load(Ordering::SeqCst)),
    }))).into_response()
}

fn heartbeat_poll_task(state: AppState) {
    tokio::spawn(async move {
        let timeout_secs = state.config.heartbeat_timeout_secs;
        let election_delay = state.config.election_delay_ms;

        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;

            if state.is_leader() {
                println!("[heartbeat] This node is now leader, stopping heartbeat poll");
                break;
            }

            let primary_addr = {
                let repl = state.replication.as_ref().unwrap().read().unwrap();
                if !repl.heartbeat_running {
                    break;
                }
                match repl.primary_addr.clone() {
                    Some(addr) => addr,
                    None => continue,
                }
            };

            let url = format!("{}/internal/heartbeat", primary_addr);
            match state.client.get(&url).send().await {
                Ok(r) if r.status().is_success() => {
                    if let Ok(hb) = r.json::<serde_json::Value>().await {
                        let mut adopted = None;
                        {
                            let mut repl = state.replication.as_ref().unwrap().write().unwrap();
                            repl.last_heartbeat = Some(std::time::Instant::now());
                            if let Some(idx) = hb.get("commit_index").and_then(|v| v.as_u64()) {
                                repl.last_known_primary_position = Some(idx);
                            }
                            if let Some(t) = hb.get("term").and_then(|v| v.as_u64()) {
                                if t > repl.term {
                                    repl.term = t;
                                    repl.voted_for = None;
                                    adopted = Some(t);
                                }
                            }
                        }
                        if let Some(t) = adopted {
                            let _ = ReplicationMeta { term: t, is_leader: false, voted_for: None }.save("./data");
                            println!("[heartbeat] Adopted higher term {} from primary {}", t, primary_addr);
                        }
                    }
                },
                Ok(r) => {
                    eprintln!("[heartbeat] Primary {} returned {}", primary_addr, r.status());
                    maybe_follow_new_leader(&state, &primary_addr).await;
                },
                Err(e) => {
                    eprintln!("[heartbeat] Primary {} unreachable: {}", primary_addr, e);
                    maybe_follow_new_leader(&state, &primary_addr).await;
                }
            }

            let should_elect = {
                let repl = state.replication.as_ref().unwrap().read().unwrap();
                let my_idx = state.db.as_ref().map_or(0, |db| db.global_commit_index.load(Ordering::SeqCst));
                let caught_up = repl.last_known_primary_position.map_or(false, |p| my_idx >= p);

                if let Some(last_hb) = repl.last_heartbeat {
                    let elapsed = last_hb.elapsed().as_secs();
                    let repl_eligible = repl.was_receiving_replication && caught_up &&
                        repl.last_replication.map_or(false, |lr| lr.elapsed().as_secs() < timeout_secs);
                    elapsed > timeout_secs && repl_eligible
                } else {
                    repl.was_receiving_replication && caught_up &&
                        repl.last_replication.map_or(false, |lr| lr.elapsed().as_secs() < timeout_secs)
                }
            };

            if should_elect {
                println!("[election] Heartbeat timeout detected, initiating election...");
                run_election(&state, election_delay).await;
                if state.is_leader() {
                    break;
                }
            }
        }
    });
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VoteRequest {
    term: u64,
    candidate_id: String,
    last_lsn: u64,
    last_term: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VoteResponse {
    term: u64,
    vote_granted: bool,
}

struct VoteDecision {
    granted: bool,
    term: u64,
    voted_for: Option<String>,
}

fn majority(cluster_size: usize) -> usize {
    cluster_size / 2 + 1
}

fn election_jitter(node_id: &str, max_delay_ms: u64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    if max_delay_ms == 0 {
        return 0;
    }
    let mut h = DefaultHasher::new();
    node_id.hash(&mut h);
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos().hash(&mut h);
    h.finish() % max_delay_ms
}

fn decide_vote(
    cur_term: u64,
    cur_voted_for: &Option<String>,
    my_log_term: u64,
    my_lsn: u64,
    req: &VoteRequest,
) -> VoteDecision {
    if req.term < cur_term {
        return VoteDecision { granted: false, term: cur_term, voted_for: cur_voted_for.clone() };
    }

    let mut term = cur_term;
    let mut voted_for = cur_voted_for.clone();
    if req.term > cur_term {
        term = req.term;
        voted_for = None;
    }

    let can_vote = match &voted_for {
        None => true,
        Some(v) => v == &req.candidate_id,
    };
    let up_to_date = (req.last_term, req.last_lsn) >= (my_log_term, my_lsn);

    if can_vote && up_to_date {
        VoteDecision { granted: true, term, voted_for: Some(req.candidate_id.clone()) }
    } else {
        VoteDecision { granted: false, term, voted_for }
    }
}

async fn vote_handler(
    State(state): State<AppState>,
    Json(req): Json<VoteRequest>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() {
        return (StatusCode::FORBIDDEN, "Not a voting node").into_response();
    }

    let repl = match state.replication.as_ref() {
        Some(r) => r,
        None => return (StatusCode::FORBIDDEN, "No replication state").into_response(),
    };

    let my_lsn = state.db.as_ref().map_or(0, |db| db.global_commit_index.load(Ordering::SeqCst));
    let my_log_term = state.db.as_ref().map_or(0, |db| db.last_log_term.load(Ordering::SeqCst));

    let (granted, resp_term, restart_poll, persist) = {
        let mut g = repl.write().unwrap();
        let was_leader = g.is_leader;
        let old_term = g.term;

        let d = decide_vote(g.term, &g.voted_for, my_log_term, my_lsn, &req);

        let mut restart = false;
        g.term = d.term;
        g.voted_for = d.voted_for.clone();

        if d.term > old_term && was_leader {
            g.is_leader = false;
            restart = !g.heartbeat_running;
            g.heartbeat_running = true;
        }

        if d.granted {
            g.last_heartbeat = Some(std::time::Instant::now());
        }

        let persist = ReplicationMeta { term: g.term, is_leader: g.is_leader, voted_for: g.voted_for.clone() };
        (d.granted, d.term, restart, persist)
    };

    let _ = persist.save("./data");
    if restart_poll {
        heartbeat_poll_task(state.clone());
    }
    if granted {
        println!("[vote] Granted vote to {} for term {}", req.candidate_id, req.term);
    }

    (StatusCode::OK, Json(VoteResponse { term: resp_term, vote_granted: granted })).into_response()
}

async fn run_election(state: &AppState, max_delay_ms: u64) {
    let delay_ms = election_jitter(&state.config.node_id, max_delay_ms);
    println!("[election] Waiting {}ms before requesting votes...", delay_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    if state.is_leader() {
        return;
    }

    {
        let primary_addr = state.replication.as_ref().unwrap().read().unwrap().primary_addr.clone();
        if let Some(addr) = primary_addr {
            let url = format!("{}/internal/heartbeat", addr);
            if let Ok(r) = state.client.get(&url).send().await {
                if r.status().is_success() {
                    println!("[election] Primary recovered during delay, aborting election");
                    let mut repl = state.replication.as_ref().unwrap().write().unwrap();
                    repl.last_heartbeat = Some(std::time::Instant::now());
                    return;
                }
            }
        }
    }

    let my_lsn = state.db.as_ref().map_or(0, |db| db.global_commit_index.load(Ordering::SeqCst));
    let my_log_term = state.db.as_ref().map_or(0, |db| db.last_log_term.load(Ordering::SeqCst));
    let candidate_id = state.config.node_id.clone();

    let new_term = {
        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
        repl.term += 1;
        repl.voted_for = Some(candidate_id.clone());
        repl.term
    };
    let _ = ReplicationMeta { term: new_term, is_leader: false, voted_for: Some(candidate_id.clone()) }.save("./data");

    let peers = state.config.peers.clone();
    let cluster_size = peers.len() + 1;
    let needed = majority(cluster_size);
    println!("[election] Node {} standing for term {} ({} peers, need {} votes)", candidate_id, new_term, peers.len(), needed);

    if peers.is_empty() {
        if needed <= 1 {
            become_leader(state, new_term, &candidate_id).await;
        } else {
            eprintln!("[election] No peers configured; cannot form a majority. Set 'peers' in config for automatic failover.");
        }
        return;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<(bool, u64)>(peers.len());
    for peer in peers {
        let client = state.client.clone();
        let req = VoteRequest {
            term: new_term,
            candidate_id: candidate_id.clone(),
            last_lsn: my_lsn,
            last_term: my_log_term,
        };
        let tx = tx.clone();
        tokio::spawn(async move {
            let url = format!("{}/internal/vote", peer);
            let result = match client.post(&url).json(&req).send().await {
                Ok(r) if r.status().is_success() => {
                    match r.json::<VoteResponse>().await {
                        Ok(v) => (v.vote_granted, v.term),
                        Err(_) => (false, 0),
                    }
                },
                _ => (false, 0),
            };
            let _ = tx.send(result).await;
        });
    }
    drop(tx);

    let votes = Arc::new(AtomicUsize::new(1));
    let highest_term = Arc::new(AtomicU64::new(new_term));
    let votes_inner = votes.clone();
    let ht_inner = highest_term.clone();
    let election_timeout = Duration::from_millis(max_delay_ms.max(1000) + 2000);
    let _ = tokio::time::timeout(election_timeout, async move {
        while votes_inner.load(Ordering::Relaxed) < needed {
            match rx.recv().await {
                Some((granted, term)) => {
                    if term > ht_inner.load(Ordering::Relaxed) {
                        ht_inner.store(term, Ordering::Relaxed);
                    }
                    if granted {
                        votes_inner.fetch_add(1, Ordering::Relaxed);
                    }
                },
                None => break,
            }
        }
    }).await;

    let seen_term = highest_term.load(Ordering::Relaxed);
    if seen_term > new_term {
        println!("[election] Saw higher term {} during election; stepping down", seen_term);
        demote(state, seen_term).await;
        return;
    }

    let tally = votes.load(Ordering::Relaxed);
    if tally >= needed {
        become_leader(state, new_term, &candidate_id).await;
    } else {
        println!("[election] Only {}/{} votes for term {}; election failed, will retry", tally, needed, new_term);
    }
}

async fn become_leader(state: &AppState, term: u64, candidate_id: &str) {
    {
        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
        if repl.term != term || repl.voted_for.as_deref() != Some(candidate_id) {
            println!("[election] State changed during election (term now {}); not assuming leadership", repl.term);
            return;
        }
        repl.is_leader = true;
        repl.heartbeat_running = false;
        repl.primary_addr = None;
    }
    let _ = ReplicationMeta { term, is_leader: true, voted_for: Some(candidate_id.to_string()) }.save("./data");
    println!("[election] *** WON election: PROMOTED to primary at term {} ***", term);
    println!("[election] Node {} is now accepting writes", candidate_id);
}

#[derive(Deserialize)]
struct SnapshotQuery {
    collection: String,
}

async fn snapshot_handler(
    State(state): State<AppState>,
    Query(params): Query<SnapshotQuery>,
) -> impl axum::response::IntoResponse {
    if !state.is_leader() {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Only primary nodes serve snapshots"}))).into_response();
    }

    let db = match state.db.as_ref() {
        Some(db) => db.clone(),
        None => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "No database".to_string()),
    };

    let col = match db.get_collection(&params.collection) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let col_clone = col.clone();
    let _ = tokio::task::spawn_blocking(move || col_clone.save_index()).await;

    let col_path = col.root_path.clone();
    let files = match tokio::task::spawn_blocking(move || -> io::Result<Vec<SnapshotFileEntry>> {
        let mut entries = Vec::new();
        for dir_entry in fs::read_dir(&col_path)? {
            let dir_entry = dir_entry?;
            let path = dir_entry.path();
            if path.is_file() {
                let filename = dir_entry.file_name().to_string_lossy().to_string();
                let data = fs::read(&path)?;
                entries.push(SnapshotFileEntry { filename, data });
            }
        }
        Ok(entries)
    }).await {
        Ok(Ok(entries)) => entries,
        Ok(Err(e)) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    (StatusCode::OK, Json(files)).into_response()
}

async fn replica_sync_from_primary(
    client: &reqwest::Client,
    primary_addr: &str,
    db: &Database,
    collection_name: &str,
) -> Result<(), String> {
    println!("[replica-sync] Syncing collection '{}' from primary {}", collection_name, primary_addr);

    let url = format!("{}/internal/snapshot?collection={}", primary_addr, collection_name);
    let resp = client.get(&url)
        .timeout(Duration::from_secs(30))
        .send().await
        .map_err(|e| format!("Snapshot request failed: {}", e))?;

    if !resp.status().is_success() {
        return Err(format!("Primary returned {}", resp.status()));
    }

    let files: Vec<SnapshotFileEntry> = resp.json().await
        .map_err(|e| format!("Failed to parse snapshot response: {}", e))?;

    if files.is_empty() {
        println!("[replica-sync] No files received for collection '{}', it may not exist on primary yet", collection_name);
        return Ok(());
    }

    let col_path = db.root_path.join(collection_name);
    let tmp_path = db.root_path.join(format!("{}.tmp", collection_name));

    if tmp_path.exists() {
        fs::remove_dir_all(&tmp_path).map_err(|e| format!("Failed to wipe tmp dir: {}", e))?;
    }
    fs::create_dir_all(&tmp_path).map_err(|e| format!("Failed to create tmp dir: {}", e))?;

    for entry in &files {
        let file_path = tmp_path.join(&entry.filename);
        fs::write(&file_path, &entry.data).map_err(|e| format!("Failed to write {}: {}", entry.filename, e))?;
    }

    let tombstone = db.release_collection(collection_name)
        .map_err(|e| format!("Failed to release collection handles: {}", e))?;

    let old_path = db.root_path.join(format!("{}.old", collection_name));
    if old_path.exists() {
        let _ = remove_dir_with_retry(&old_path);
    }
    if col_path.exists() {
        fs::rename(&col_path, &old_path).map_err(|e| format!("Failed to backup old col dir: {}", e))?;
    }
    fs::rename(&tmp_path, &col_path).map_err(|e| format!("Failed to finalize new col dir: {}", e))?;
    if old_path.exists() {
        let _ = remove_dir_with_retry(&old_path);
    }
    if let Some(path) = tombstone {
        let _ = fs::remove_file(path);
    }

    println!("[replica-sync] Restored {} files for collection '{}'", files.len(), collection_name);

    let _ = db.get_collection(collection_name)
        .map_err(|e| format!("Failed to reopen collection after sync: {}", e))?;

    println!("[replica-sync] Collection '{}' ready", collection_name);
    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path = "node.json".to_string();

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" && i + 1 < args.len() {
            config_path = args[i + 1].clone();
            i += 2;
        } else {
            i += 1;
        }
    }

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config file");
    let config: NodeConfig = serde_json::from_str(&config_content).expect("Invalid config JSON format");
    config.validate().expect("Invalid config map constraints");

    println!("Booting Node: {} | Role: {} | Shard Role: {:?}", config.node_id, config.role, config.shard_role);

    if config.role == "router" && Path::new("./data").exists() {
        println!("Warning: router node should not use local storage");
    }

    let db = if config.role == "shard" {
        Some(Arc::new(Database::new("./data")?))
    } else {
        None
    };

    let db_clone = db.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("\nReceived Ctrl-C. Shutting down and forcing WAL commits...");
        if let Some(d) = db_clone {
            d.force_commit_all();
        }
        std::process::exit(0);
    });

    let replication = if config.role == "shard" {
        let meta = ReplicationMeta::load("./data");
        let (term, is_leader, voted_for) = if let Some(ref m) = meta {
            println!("[boot] Restored replication state: term={}, is_leader={}", m.term, m.is_leader);
            let mut boot_term = m.term;
            let mut boot_voted = m.voted_for.clone();
            if m.is_leader {
                boot_term += 1;
                boot_voted = Some(config.node_id.clone());
                let new_meta = ReplicationMeta { term: boot_term, is_leader: true, voted_for: boot_voted.clone() };
                let _ = new_meta.save("./data");
                println!("[boot] Escalated leader term to {} to prevent split brain.", boot_term);
            }
            (boot_term, m.is_leader, boot_voted)
        } else {
            let is_primary = config.shard_role.as_deref() == Some("primary");
            (0, is_primary, None)
        };

        Some(Arc::new(RwLock::new(ReplicationState {
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
        })))
    } else {
        None
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let state = AppState {
        db: db.clone(),
        config: Arc::new(config.clone()),
        client: client.clone(),
        replication,
        primary_overrides: Arc::new(std::sync::Mutex::new(HashMap::new())),
        shard_failover_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        repair_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        resyncing: Arc::new(std::sync::Mutex::new(HashSet::new())),
        read_rr: Arc::new(AtomicUsize::new(0)),
    };

    if config.shard_role.as_deref() == Some("replica") {
        if let (Some(primary_addr), Some(db)) = (&config.primary_addr, &db) {
            println!("[replica] Performing full sync from primary: {}", primary_addr);

            if let Ok(entries) = fs::read_dir("./data") {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            if let Err(e) = replica_sync_from_primary(&client, primary_addr, db, name).await {
                                eprintln!("[replica] Sync failed for '{}': {}", name, e);
                            }
                        }
                    }
                }
            }
        }
    }

    let mut app = Router::new()
        .route("/collections", get(list_collections))
        .route("/collections/:name", delete(drop_collection))
        .route("/collections/:name/compact", post(compact_collection))
        .route("/collections/:name/snapshot", post(snapshot_collection))
        .route("/collections/:name/docs", post(create_doc).get(list_docs))
        .route("/collections/:name/docs/bulk", post(bulk_create_docs))
        .route("/collections/:name/query", get(query_docs))
        .route("/collections/:name/docs/:id", get(get_doc).put(put_doc).patch(update_doc).delete(delete_doc));

    if config.role == "shard" {
        app = app
            .route("/internal/replicate", post(replicate_handler))
            .route("/internal/snapshot", get(snapshot_handler))
            .route("/internal/resync", post(resync_handler))
            .route("/internal/vote", post(vote_handler))
            .route("/internal/drop", post(internal_drop_handler))
            .route("/internal/heartbeat", get(heartbeat_handler));
    }

    let app = app.with_state(state.clone());

    if config.role == "shard" && !state.is_leader() {
        println!("[boot] Starting heartbeat poll task (timeout={}s, delay={}ms)",
            config.heartbeat_timeout_secs, config.election_delay_ms);
        heartbeat_poll_task(state.clone());
    }

    if config.role == "router" {
        println!("[boot] Starting router primary-probe task (interval={}s)", ROUTER_PROBE_INTERVAL_SECS);
        router_probe_task(state.clone());
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    println!("Server starting on http://{}", config.listen_addr);
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> PathBuf {
        let p = std::env::temp_dir().join(format!("dewdb-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn durability_recovery_size_limit_and_corruption() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("test_durability").unwrap();

            for i in 0..100 {
                let _ = col.put(format!("key:{}", i), serde_json::json!({"n": i}), 1).unwrap();
            }
            col.enqueue_commit().await.unwrap().unwrap();
        }

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("test_durability").unwrap();

        let mut missing = 0;
        for i in 0..100 {
            if col2.get(&format!("key:{}", i)).unwrap().is_none() {
                missing += 1;
            }
        }
        assert_eq!(missing, 0, "All 100 docs should be recovered after restart");

        assert!(db2.global_commit_index.load(Ordering::SeqCst) >= 100, "Commit LSN should survive restart");

        col2.save_index().unwrap();

        let huge_str = "x".repeat((MAX_RECORD_SIZE + 10) as usize);
        let res = col2.put("huge_key".to_string(), serde_json::json!({"data": huge_str}), 1);
        assert!(res.is_err(), "Should reject a record that exceeds MAX_RECORD_SIZE");

        if let Ok((_f, wal_id, offset, _lsn)) = col2.put("key_pre_corrupt".to_string(), serde_json::json!({"valid": true}), 1) {
            col2.enqueue_commit().await.unwrap().unwrap();
            col2.index.write().unwrap().insert("key_pre_corrupt".to_string(), IndexEntry { wal_id, offset });
        }

        let active_wal_path = {
            let wal_writer = col2.wal_writer.lock().unwrap();
            col2.root_path.join(format!("wal-{:05}.log", wal_writer.current_wal_id))
        };

        {
            let mut f = OpenOptions::new().append(true).open(&active_wal_path).unwrap();

            let mut bad_header = [0u8; HEADER_LEN];
            let huge_len: u32 = (MAX_RECORD_SIZE + 5000) as u32;
            bad_header[0..4].copy_from_slice(&huge_len.to_le_bytes());
            f.write_all(&bad_header).unwrap();

            f.write_all(&[0u8; 10]).unwrap();
        }

        drop(col2);
        drop(db2);

        let db3 = Database::new(&root).unwrap();
        let col3 = db3.get_collection("test_durability").unwrap();

        assert!(col3.get("key_pre_corrupt").unwrap().is_some(), "key_pre_corrupt should survive corruption after it");

        if let Ok((_f, wal_id, offset, _lsn)) = col3.put("key_post_corrupt".to_string(), serde_json::json!({"valid": true}), 1) {
            col3.enqueue_commit().await.unwrap().unwrap();
            col3.index.write().unwrap().insert("key_post_corrupt".to_string(), IndexEntry { wal_id, offset });
        }
        assert!(col3.get("key_post_corrupt").unwrap().is_some(), "Writes should continue after recovery");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn lsn_is_monotonic_across_restarts() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("lsn_check").unwrap();
            for i in 0..10 {
                let _ = col.put(format!("a:{}", i), serde_json::json!({"i": i}), 1).unwrap();
            }
            col.enqueue_commit().await.unwrap().unwrap();
            assert_eq!(db.global_commit_index.load(Ordering::SeqCst), 10);
        }

        {
            let db = Database::new(&root).unwrap();
            assert_eq!(db.global_commit_index.load(Ordering::SeqCst), 10, "Commit LSN must be restored from lsn.meta");
            let col = db.get_collection("lsn_check").unwrap();
            for i in 0..5 {
                let _ = col.put(format!("b:{}", i), serde_json::json!({"i": i}), 1).unwrap();
            }
            col.enqueue_commit().await.unwrap().unwrap();
            assert_eq!(db.global_commit_index.load(Ordering::SeqCst), 15, "LSN must continue from restored value, not reset to zero");
        }

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn frames_carry_term_and_lsn_in_header() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("hdr").unwrap();

        let (frame, _wal_id, _offset, lsn) = col.put("k".into(), serde_json::json!({"v": 1}), 7).unwrap();
        assert_eq!(lsn, 1);

        let term_in_frame = u64::from_le_bytes(frame[8..16].try_into().unwrap());
        let lsn_in_frame = u64::from_le_bytes(frame[16..24].try_into().unwrap());
        assert_eq!(term_in_frame, 7, "term must be stamped into the frame header");
        assert_eq!(lsn_in_frame, 1, "lsn must be stamped into the frame header");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn replica_detects_gaps_and_dedupes() {
        let proot = temp_root();
        let rroot = temp_root();

        let pdb = Database::new(&proot).unwrap();
        let pcol = pdb.get_collection("c").unwrap();
        let rdb = Database::new(&rroot).unwrap();
        let rcol = rdb.get_collection("c").unwrap();

        let (f1, _, _, l1) = pcol.put("k1".into(), serde_json::json!({"v": 1}), 1).unwrap();
        let (f2, _, _, l2) = pcol.put("k2".into(), serde_json::json!({"v": 2}), 1).unwrap();
        let (f3, _, _, l3) = pcol.put("k3".into(), serde_json::json!({"v": 3}), 1).unwrap();
        assert_eq!((l1, l2, l3), (1, 2, 3));

        match rcol.append_raw_frame(&f1, 0).unwrap() {
            ReplicaApply::Applied { lsn, .. } => assert_eq!(lsn, 1),
            other => panic!("expected Applied, got {:?}", other),
        }

        match rcol.append_raw_frame(&f3, 2).unwrap() {
            ReplicaApply::Gap { last_lsn } => assert_eq!(last_lsn, 1),
            other => panic!("expected Gap, got {:?}", other),
        }

        match rcol.append_raw_frame(&f2, 1).unwrap() {
            ReplicaApply::Applied { lsn, .. } => assert_eq!(lsn, 2),
            other => panic!("expected Applied, got {:?}", other),
        }

        match rcol.append_raw_frame(&f3, 2).unwrap() {
            ReplicaApply::Applied { lsn, .. } => assert_eq!(lsn, 3),
            other => panic!("expected Applied, got {:?}", other),
        }

        match rcol.append_raw_frame(&f2, 1).unwrap() {
            ReplicaApply::Duplicate { last_lsn } => assert_eq!(last_lsn, 3),
            other => panic!("expected Duplicate, got {:?}", other),
        }

        rcol.enqueue_commit().await.unwrap().unwrap();
        drop(rcol);
        drop(rdb);

        let rdb2 = Database::new(&rroot).unwrap();
        let rcol2 = rdb2.get_collection("c").unwrap();
        assert_eq!(rcol2.get("k1").unwrap(), Some(serde_json::json!({"v": 1})));
        assert_eq!(rcol2.get("k2").unwrap(), Some(serde_json::json!({"v": 2})));
        assert_eq!(rcol2.get("k3").unwrap(), Some(serde_json::json!({"v": 3})));
        assert_eq!(rdb2.global_commit_index.load(Ordering::SeqCst), 3, "Replica LSN must match the frames it applied from the primary");

        let _ = fs::remove_dir_all(&proot);
        let _ = fs::remove_dir_all(&rroot);
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
        assert_eq!(pdb.global_commit_index.load(Ordering::SeqCst), 5);

        let all = contiguous_prefix(0, pcol.read_frames_after(0, 5).unwrap());
        let lsns: Vec<u64> = all.iter().map(|(l, _)| *l).collect();
        assert_eq!(lsns, vec![1, 2, 3, 4, 5]);

        let rdb = Database::new(&rroot).unwrap();
        let rcol = rdb.get_collection("c").unwrap();

        let mut prev = 0;
        for (lsn, fr) in &all[..2] {
            match rcol.append_raw_frame(fr, prev).unwrap() {
                ReplicaApply::Applied { lsn: a, .. } => assert_eq!(a, *lsn),
                other => panic!("expected Applied, got {:?}", other),
            }
            prev = *lsn;
        }

        let backfill = contiguous_prefix(2, pcol.read_frames_after(2, 5).unwrap());
        let bf_lsns: Vec<u64> = backfill.iter().map(|(l, _)| *l).collect();
        assert_eq!(bf_lsns, vec![3, 4, 5]);

        for (lsn, fr) in &backfill {
            match rcol.append_raw_frame(fr, prev).unwrap() {
                ReplicaApply::Applied { lsn: a, .. } => assert_eq!(a, *lsn),
                other => panic!("expected Applied during backfill, got {:?}", other),
            }
            prev = *lsn;
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
        let (_, w, o, _) = col.put("a".into(), serde_json::json!({"v": 3}), 1).unwrap();
        col.index.write().unwrap().insert("a".into(), IndexEntry { wal_id: w, offset: o });
        let (_, w2, o2, _) = col.put("b".into(), serde_json::json!({"v": 9}), 1).unwrap();
        col.index.write().unwrap().insert("b".into(), IndexEntry { wal_id: w2, offset: o2 });
        col.enqueue_commit().await.unwrap().unwrap();
        assert_eq!(db.global_commit_index.load(Ordering::SeqCst), 4);

        col.compact().unwrap();

        let frames = col.read_frames_after(0, 4).unwrap();
        let mut lsns: Vec<u64> = frames.iter().map(|(l, _)| *l).collect();
        lsns.sort();
        assert_eq!(lsns, vec![3, 4], "compaction should drop overwritten lsns 1 and 2");

        let from_zero = contiguous_prefix(0, col.read_frames_after(0, 4).unwrap());
        assert!(from_zero.is_empty(), "no contiguous run from lsn 1 exists -> repair must snapshot");

        let from_two = contiguous_prefix(2, col.read_frames_after(2, 4).unwrap());
        let two_lsns: Vec<u64> = from_two.iter().map(|(l, _)| *l).collect();
        assert_eq!(two_lsns, vec![3, 4], "a replica already at lsn 2 can still backfill");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn write_concern_resolves_required_acks() {
        assert_eq!(required_acks(&parse_write_concern(None), 2), 1);
        assert_eq!(required_acks(&parse_write_concern(Some("1")), 2), 1);

        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 2), 2);
        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 1), 2);
        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 4), 3);

        assert_eq!(required_acks(&parse_write_concern(Some("all")), 2), 3);
        assert_eq!(required_acks(&parse_write_concern(Some("all")), 0), 1);

        assert_eq!(required_acks(&parse_write_concern(Some("3")), 2), 3);
        assert_eq!(required_acks(&parse_write_concern(Some("9")), 2), 3, "N is capped at total node count");
        assert_eq!(required_acks(&parse_write_concern(Some("0")), 2), 1, "N is floored at 1");

        assert_eq!(required_acks(&parse_write_concern(Some("garbage")), 2), 1, "unparseable w falls back to local");
    }

    #[test]
    fn wc_query_string_roundtrips() {
        let p = WriteConcernParams { w: Some("majority".into()), wtimeout: Some(2000) };
        assert_eq!(wc_query_string(&p), "?w=majority&wtimeout=2000");

        let p2 = WriteConcernParams { w: None, wtimeout: None };
        assert_eq!(wc_query_string(&p2), "");

        let p3 = WriteConcernParams { w: Some("all".into()), wtimeout: None };
        assert_eq!(wc_query_string(&p3), "?w=all");
    }

    fn leader_state(term: u64) -> ReplicationState {
        ReplicationState {
            term,
            is_leader: true,
            voted_for: None,
            last_heartbeat: None,
            was_receiving_replication: true,
            last_replication: Some(std::time::Instant::now()),
            heartbeat_running: false,
            primary_addr: None,
            replicas: vec![],
            last_known_primary_position: None,
        }
    }

    fn vote_req(term: u64, candidate: &str, last_term: u64, last_lsn: u64) -> VoteRequest {
        VoteRequest { term, candidate_id: candidate.to_string(), last_term, last_lsn }
    }

    #[test]
    fn majority_math() {
        assert_eq!(majority(1), 1);
        assert_eq!(majority(2), 2);
        assert_eq!(majority(3), 2);
        assert_eq!(majority(4), 3);
        assert_eq!(majority(5), 3);
    }

    #[test]
    fn vote_granted_for_fresh_higher_term_when_up_to_date() {
        let d = decide_vote(2, &None, 2, 100, &vote_req(3, "n1", 2, 100));
        assert!(d.granted);
        assert_eq!(d.term, 3);
        assert_eq!(d.voted_for.as_deref(), Some("n1"));
    }

    #[test]
    fn vote_denied_for_stale_candidate_term() {
        let d = decide_vote(5, &None, 5, 100, &vote_req(4, "n1", 5, 100));
        assert!(!d.granted);
        assert_eq!(d.term, 5);
        assert_eq!(d.voted_for, None);
    }

    #[test]
    fn vote_at_most_once_per_term() {
        let d1 = decide_vote(3, &None, 1, 50, &vote_req(3, "n1", 1, 50));
        assert!(d1.granted);
        assert_eq!(d1.voted_for.as_deref(), Some("n1"));

        let d2 = decide_vote(3, &d1.voted_for, 1, 50, &vote_req(3, "n2", 1, 50));
        assert!(!d2.granted, "must not vote for a second candidate in the same term");
        assert_eq!(d2.voted_for.as_deref(), Some("n1"));

        let d3 = decide_vote(3, &d1.voted_for, 1, 50, &vote_req(3, "n1", 1, 50));
        assert!(d3.granted, "re-voting for the same candidate is idempotent");
    }

    #[test]
    fn vote_denied_when_candidate_log_behind() {
        let behind_lsn = decide_vote(3, &None, 2, 100, &vote_req(4, "n1", 2, 99));
        assert!(!behind_lsn.granted, "candidate with lower lsn at same log term must lose");

        let behind_term = decide_vote(3, &None, 2, 100, &vote_req(4, "n1", 1, 500));
        assert!(!behind_term.granted, "candidate with lower last log term must lose even with higher lsn");

        let ahead = decide_vote(3, &None, 2, 100, &vote_req(4, "n1", 3, 1));
        assert!(ahead.granted, "higher last log term wins regardless of lsn");
    }

    #[test]
    fn higher_term_vote_resets_prior_vote() {
        let prior = Some("n2".to_string());
        let d = decide_vote(3, &prior, 1, 50, &vote_req(4, "n1", 1, 50));
        assert!(d.granted, "a higher term clears the old vote, so n1 can win");
        assert_eq!(d.term, 4);
        assert_eq!(d.voted_for.as_deref(), Some("n1"));
    }

    #[test]
    fn demotion_transitions_leader_to_follower() {
        let mut r = leader_state(2);

        assert_eq!(apply_demotion(&mut r, 2), None, "equal term is not a demotion");
        assert!(r.is_leader);
        assert_eq!(apply_demotion(&mut r, 1), None, "lower term is not a demotion");
        assert!(r.is_leader);

        assert_eq!(apply_demotion(&mut r, 5), Some(true), "higher term demotes and needs poll restart");
        assert!(!r.is_leader);
        assert_eq!(r.term, 5);
        assert!(r.heartbeat_running);
        assert!(!r.was_receiving_replication);

        assert_eq!(apply_demotion(&mut r, 7), Some(false), "already-following node adopts term without restarting poll");
        assert_eq!(r.term, 7);
        assert!(!r.is_leader);
    }

    #[test]
    fn conflict_classification() {
        let stale = Some(serde_json::json!({"status": "stale_term", "term": 9}));
        match classify_conflict(&stale) {
            ConflictKind::StaleTerm(t) => assert_eq!(t, 9),
            _ => panic!("expected StaleTerm"),
        }

        let gap = Some(serde_json::json!({"status": "gap", "last_lsn": 42}));
        match classify_conflict(&gap) {
            ConflictKind::Gap(l) => assert_eq!(l, 42),
            _ => panic!("expected Gap"),
        }

        match classify_conflict(&None) {
            ConflictKind::Gap(l) => assert_eq!(l, 0),
            _ => panic!("expected Gap default"),
        }

        let fb = Some(serde_json::json!({"status": "not_a_replica", "term": 4}));
        assert_eq!(forbidden_term(&fb), 4);
        assert_eq!(forbidden_term(&None), 0);
    }

    fn probe(url: &str, role: &str, term: u64) -> (String, Option<(String, u64)>) {
        (url.to_string(), Some((role.to_string(), term)))
    }

    #[test]
    fn select_primary_picks_the_reachable_leader() {
        let probes = vec![
            probe("http://a", "replica", 5),
            probe("http://b", "primary", 5),
            probe("http://c", "replica", 5),
        ];
        assert_eq!(select_primary(&probes).as_deref(), Some("http://b"));
    }

    #[test]
    fn select_primary_prefers_highest_term_on_split() {
        let probes = vec![
            probe("http://old", "primary", 4),
            probe("http://new", "primary", 6),
        ];
        assert_eq!(select_primary(&probes).as_deref(), Some("http://new"));
    }

    #[test]
    fn select_primary_none_when_no_leader() {
        let probes = vec![
            probe("http://a", "replica", 5),
            ("http://b".to_string(), None),
        ];
        assert_eq!(select_primary(&probes), None);
    }

    #[test]
    fn read_targets_primary_prefers_leader() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];
        let t = read_targets(&ReadPreference::Primary, "http://p", &replicas, 0);
        assert_eq!(t, vec!["http://p", "http://r1", "http://r2"]);
    }

    #[test]
    fn read_targets_replica_prefers_replicas_and_spreads() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];

        let t0 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 0);
        assert_eq!(t0, vec!["http://r1", "http://r2", "http://p"]);

        let t1 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 1);
        assert_eq!(t1, vec!["http://r2", "http://r1", "http://p"], "round-robin rotates the starting replica");

        let t2 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 2);
        assert_eq!(t2, vec!["http://r1", "http://r2", "http://p"], "rotation wraps");
    }

    #[test]
    fn read_targets_replica_falls_back_to_primary_when_no_replicas() {
        let t = read_targets(&ReadPreference::Replica, "http://p", &[], 0);
        assert_eq!(t, vec!["http://p"]);
    }

    #[test]
    fn read_targets_dedupes_when_override_points_at_a_replica() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];
        let t = read_targets(&ReadPreference::Primary, "http://r1", &replicas, 0);
        assert_eq!(t, vec!["http://r1", "http://r2"], "promoted replica isn't tried twice");
    }

    #[test]
    fn parse_read_pref_defaults_to_primary() {
        assert!(matches!(parse_read_pref(None), ReadPreference::Primary));
        assert!(matches!(parse_read_pref(Some("primary")), ReadPreference::Primary));
        assert!(matches!(parse_read_pref(Some("garbage")), ReadPreference::Primary));
        assert!(matches!(parse_read_pref(Some("replica")), ReadPreference::Replica));
    }

    #[tokio::test]
    async fn range_from_resumes_exclusively() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for k in ["a", "b", "c", "d"] {
            let (_, w, o, _) = col.put(k.to_string(), serde_json::json!({"k": k}), 1).unwrap();
            col.index.write().unwrap().insert(k.to_string(), IndexEntry { wal_id: w, offset: o });
        }

        let inclusive: Vec<String> = col.range_from(None, Some("b"), None).into_iter().map(|(k, _)| k).collect();
        assert_eq!(inclusive, vec!["b", "c", "d"], "start is inclusive");

        let exclusive: Vec<String> = col.range_from(Some("b"), None, None).into_iter().map(|(k, _)| k).collect();
        assert_eq!(exclusive, vec!["c", "d"], "cursor resumes strictly after the key");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn query_page_paginates_without_loss_or_duplication() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for i in 1..=5 {
            let k = format!("k{}", i);
            let (_, w, o, _) = col.put(k.clone(), serde_json::json!({"i": i}), 1).unwrap();
            col.index.write().unwrap().insert(k, IndexEntry { wal_id: w, offset: o });
        }
        col.enqueue_commit().await.unwrap().unwrap();

        let (p1, c1) = col.query_page(None, None, None, &None, 2).unwrap();
        assert_eq!(p1.len(), 2);
        assert_eq!(c1.as_deref(), Some("k2"), "next_cursor is the last key when more remain");

        let (p2, c2) = col.query_page(c1.as_deref(), None, None, &None, 2).unwrap();
        assert_eq!(p2.len(), 2);
        assert_eq!(p2[0], serde_json::json!({"i": 3}));
        assert_eq!(c2.as_deref(), Some("k4"));

        let (p3, c3) = col.query_page(c2.as_deref(), None, None, &None, 2).unwrap();
        assert_eq!(p3.len(), 1, "final short page");
        assert_eq!(p3[0], serde_json::json!({"i": 5}));
        assert_eq!(c3, None, "no cursor once the range is exhausted");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn query_page_no_cursor_when_last_page_is_exactly_full() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for i in 1..=4 {
            let k = format!("k{}", i);
            let (_, w, o, _) = col.put(k.clone(), serde_json::json!({"i": i}), 1).unwrap();
            col.index.write().unwrap().insert(k, IndexEntry { wal_id: w, offset: o });
        }
        col.enqueue_commit().await.unwrap().unwrap();

        let (_p1, c1) = col.query_page(None, None, None, &None, 2).unwrap();
        assert_eq!(c1.as_deref(), Some("k2"));

        let (p2, c2) = col.query_page(c1.as_deref(), None, None, &None, 2).unwrap();
        assert_eq!(p2.len(), 2);
        assert_eq!(c2, None, "a full final page with nothing after must not emit a cursor");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shard_cursor_round_trips_through_base64() {
        let mut positions = BTreeMap::new();
        positions.insert("http://s1".to_string(), "k42".to_string());
        positions.insert("http://s2".to_string(), "k17".to_string());
        let c = ShardCursor { positions };

        let encoded = encode_cursor(&c);
        assert!(!encoded.contains('{'), "encoded cursor should be opaque, not raw JSON");

        let decoded = decode_cursor(&encoded).expect("must decode");
        assert_eq!(decoded.positions.get("http://s1").map(|s| s.as_str()), Some("k42"));
        assert_eq!(decoded.positions.get("http://s2").map(|s| s.as_str()), Some("k17"));

        assert!(decode_cursor("!!!not base64 json!!!").is_none());
    }

    #[test]
    fn parse_sort_directions() {
        let a = parse_sort(Some("age")).unwrap();
        assert_eq!(a.field, "age");
        assert!(!a.desc);

        let d = parse_sort(Some("age:desc")).unwrap();
        assert!(d.desc);

        let asc = parse_sort(Some("score:asc")).unwrap();
        assert!(!asc.desc);

        assert!(parse_sort(None).is_none());
        assert!(parse_sort(Some("")).is_none());
    }

    #[test]
    fn json_cmp_orders_across_and_within_types() {
        use std::cmp::Ordering;
        use serde_json::json;
        assert_eq!(json_cmp(&json!(1), &json!(2)), Ordering::Less);
        assert_eq!(json_cmp(&json!("b"), &json!("a")), Ordering::Greater);
        assert_eq!(json_cmp(&json!(false), &json!(true)), Ordering::Less);
        assert_eq!(json_cmp(&json!(null), &json!(0)), Ordering::Less, "null sorts before numbers");
        assert_eq!(json_cmp(&json!(5), &json!("5")), Ordering::Less, "numbers sort before strings");
    }

    #[test]
    fn kway_merge_produces_global_order_bounded_by_limit() {
        use serde_json::json;
        let sort = SortSpec { field: "n".to_string(), desc: false };
        let l1 = vec![json!({"n": 1}), json!({"n": 4}), json!({"n": 7})];
        let l2 = vec![json!({"n": 2}), json!({"n": 3}), json!({"n": 8})];
        let l3 = vec![json!({"n": 5}), json!({"n": 6})];

        let merged = kway_merge(vec![l1, l2, l3], &sort, 5);
        let ns: Vec<i64> = merged.iter().map(|v| v["n"].as_i64().unwrap()).collect();
        assert_eq!(ns, vec![1, 2, 3, 4, 5], "globally sorted, bounded to limit");
    }

    #[test]
    fn kway_merge_desc() {
        use serde_json::json;
        let sort = SortSpec { field: "n".to_string(), desc: true };
        let l1 = vec![json!({"n": 9}), json!({"n": 3})];
        let l2 = vec![json!({"n": 7}), json!({"n": 1})];
        let merged = kway_merge(vec![l1, l2], &sort, 3);
        let ns: Vec<i64> = merged.iter().map(|v| v["n"].as_i64().unwrap()).collect();
        assert_eq!(ns, vec![9, 7, 3]);
    }

    #[test]
    fn projection_keeps_only_requested_paths() {
        use serde_json::json;
        let doc = json!({"a": 1, "b": {"c": 2, "d": 3}, "e": 4});

        let flat = project(&doc, &["a".to_string(), "e".to_string()]);
        assert_eq!(flat, json!({"a": 1, "e": 4}));

        let nested = project(&doc, &["b.c".to_string()]);
        assert_eq!(nested, json!({"b": {"c": 2}}), "nested path projects into nested object");

        let missing = project(&doc, &["a".to_string(), "zzz".to_string()]);
        assert_eq!(missing, json!({"a": 1}), "missing fields are omitted");

        let empty = project(&doc, &[]);
        assert_eq!(empty, doc, "empty projection returns the full doc");
    }

    #[test]
    fn merge_patch_follows_rfc7386() {
        use serde_json::json;

        let mut basic = json!({"a": "b", "c": {"d": "e", "f": "g"}});
        merge_patch(&mut basic, &json!({"a": "z", "c": {"f": null}}));
        assert_eq!(basic, json!({"a": "z", "c": {"d": "e"}}), "null removes a member, siblings survive");

        let mut arrays = json!({"a": [1, 2, 3]});
        merge_patch(&mut arrays, &json!({"a": [4]}));
        assert_eq!(arrays, json!({"a": [4]}), "arrays are replaced wholesale, never element-merged");

        let mut scalar_to_object = json!({"a": "flat"});
        merge_patch(&mut scalar_to_object, &json!({"a": {"b": 1}}));
        assert_eq!(scalar_to_object, json!({"a": {"b": 1}}), "object patch overwrites a scalar");

        let mut created = json!({});
        merge_patch(&mut created, &json!({"a": {"b": 1, "c": null}}));
        assert_eq!(created, json!({"a": {"b": 1}}), "nulls are dropped while creating a new member");

        let mut whole = json!({"a": 1});
        merge_patch(&mut whole, &json!("replaced"));
        assert_eq!(whole, json!("replaced"), "a non-object patch replaces the whole target");

        let mut deep = json!({"x": {"y": {"z": 1, "keep": true}}});
        merge_patch(&mut deep, &json!({"x": {"y": {"z": 2}}}));
        assert_eq!(deep, json!({"x": {"y": {"z": 2, "keep": true}}}), "deep merge preserves untouched siblings");

        let mut absent_delete = json!({"a": 1});
        merge_patch(&mut absent_delete, &json!({"missing": null}));
        assert_eq!(absent_delete, json!({"a": 1}), "deleting an absent member is a no-op");

        let mut noop = json!({"a": 1});
        merge_patch(&mut noop, &json!({}));
        assert_eq!(noop, json!({"a": 1}), "an empty patch changes nothing");
    }

    #[test]
    fn merge_patch_never_leaves_null_placeholders_in_new_subtrees() {
        use serde_json::json;
        let mut doc = json!({"keep": 1});
        merge_patch(&mut doc, &json!({"new": {"deep": {"k": "v", "gone": null}}}));
        assert_eq!(doc, json!({"keep": 1, "new": {"deep": {"k": "v"}}}));
    }

    #[tokio::test]
    async fn patch_persists_the_merged_document_not_the_patch() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let (_, w, o, _) = col.put(
            "d1".into(),
            serde_json::json!({"name": "alpha", "tags": ["x", "y"], "meta": {"v": 1, "owner": "latha"}}),
            1,
        ).unwrap();
        col.index.write().unwrap().insert("d1".into(), IndexEntry { wal_id: w, offset: o });

        let mut doc = col.get("d1").unwrap().unwrap();
        merge_patch(&mut doc, &serde_json::json!({"meta": {"v": 2}, "tags": null, "status": "live"}));
        let (_, w2, o2, _) = col.put("d1".into(), doc, 1).unwrap();
        col.index.write().unwrap().insert("d1".into(), IndexEntry { wal_id: w2, offset: o2 });
        col.enqueue_commit().await.unwrap().unwrap();

        drop(col);
        drop(db);

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        assert_eq!(
            col2.get("d1").unwrap(),
            Some(serde_json::json!({"name": "alpha", "meta": {"v": 2, "owner": "latha"}, "status": "live"})),
            "the merged document survives a restart, with the patched field replaced and the sibling intact"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn key_locks_are_stable_and_striped() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        assert!(std::ptr::eq(col.key_lock("doc-1"), col.key_lock("doc-1")), "same key always maps to the same stripe");
        assert_eq!(col.key_locks.len(), KEY_LOCK_STRIPES);

        let mut distinct = HashSet::new();
        for i in 0..512 {
            distinct.insert(col.key_lock(&format!("doc-{}", i)) as *const _);
        }
        assert!(distinct.len() > 1, "keys must spread across more than one stripe");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn exists_tracks_index_membership() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        assert!(!col.exists("k"), "nothing exists before the first write");

        let (_, w, o, _) = col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();
        col.index.write().unwrap().insert("k".into(), IndexEntry { wal_id: w, offset: o });
        assert!(col.exists("k"));

        col.index.write().unwrap().remove("k");
        assert!(!col.exists("k"), "a deleted key stops existing");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn router_treats_client_errors_as_authoritative() {
        assert!(authoritative_write_status(StatusCode::OK));
        assert!(authoritative_write_status(StatusCode::CREATED));
        assert!(authoritative_write_status(StatusCode::ACCEPTED));
        assert!(authoritative_write_status(StatusCode::NOT_FOUND), "a PATCH 404 must not trigger shard failover");
        assert!(authoritative_write_status(StatusCode::BAD_REQUEST));

        assert!(!authoritative_write_status(StatusCode::FORBIDDEN), "a replica rejecting writes must trigger failover");
        assert!(!authoritative_write_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(!authoritative_write_status(StatusCode::BAD_GATEWAY));
        assert!(!authoritative_write_status(StatusCode::SERVICE_UNAVAILABLE));
    }

    #[tokio::test]
    async fn list_collections_merges_disk_and_memory_and_hides_transient_dirs() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        db.get_collection("users").unwrap();
        fs::create_dir_all(root.join("orders")).unwrap();
        fs::create_dir_all(root.join("orders.tmp")).unwrap();
        fs::create_dir_all(root.join("orders.old")).unwrap();
        fs::write(root.join("lsn.meta"), b"x").unwrap();

        let names = db.list_collections().unwrap();

        assert_eq!(names, vec!["orders".to_string(), "users".to_string()],
            "listing merges the open collection with on-disk dirs, sorted");
        assert!(!names.iter().any(|n| n.ends_with(".tmp") || n.ends_with(".old")),
            "in-flight resync scratch dirs must never surface as collections");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn drop_collection_deletes_files_despite_open_wal_handle() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let col = db.get_collection("users").unwrap();
        let (_, w, o, _) = col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();
        col.index.write().unwrap().insert("k".into(), IndexEntry { wal_id: w, offset: o });

        assert!(root.join("users").is_dir());

        let existed = db.drop_collection("users").unwrap();

        assert!(existed, "dropping a live collection reports that it existed");
        assert!(!root.join("users").exists(),
            "the collection dir must be gone even though a WAL handle was open");
        assert!(db.list_collections().unwrap().is_empty());
        assert!(!root.join(".released-users.wal").exists(), "the tombstone WAL must be cleaned up");

        assert!(col.put("k2".into(), serde_json::json!({"v": 2}), 1).is_err(),
            "a stale handle to a released collection must refuse further writes");

        assert!(!db.drop_collection("users").unwrap(), "dropping a missing collection is a no-op");

        let _ = fs::remove_dir_all(&root);
    }

    fn live_put(col: &Arc<Collection>, key: &str, v: i64) {
        let (_, w, o, _) = col.put(key.into(), serde_json::json!({"v": v}), 1).unwrap();
        col.index.write().unwrap().insert(key.into(), IndexEntry { wal_id: w, offset: o });
    }

    fn wal_ids_on_disk(root: &Path) -> Vec<u64> {
        let mut ids: Vec<u64> = fs::read_dir(root).unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str().map(|s| s.to_string()))
            .filter(|n| n.starts_with("wal-") && n.ends_with(".log"))
            .filter_map(|n| n[4..n.len() - 4].parse::<u64>().ok())
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn a_bulk_batch_larger_than_the_stripe_count_does_not_self_deadlock() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let keys: Vec<String> = (0..KEY_LOCK_STRIPES * 4).map(|i| format!("k{}", i)).collect();

        let mut stripes: Vec<usize> = keys.iter().map(|k| col.key_stripe(k)).collect();
        stripes.sort_unstable();
        let distinct = { let mut d = stripes.clone(); d.dedup(); d.len() };
        assert!(distinct < keys.len(), "this batch must contain stripe collisions for the test to be meaningful");

        stripes.dedup();
        let acquired = tokio::time::timeout(Duration::from_secs(5), async {
            let mut guards = Vec::new();
            for stripe in stripes {
                guards.push(col.key_locks[stripe].lock().await);
            }
            guards.len()
        }).await;

        assert!(acquired.is_ok(), "locking a batch must dedupe stripes; locking per key deadlocks on collision");
        assert_eq!(acquired.unwrap(), distinct);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn compaction_writes_to_a_new_wal_and_leaves_the_active_one_writable() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "a", 1);
        live_put(&col, "a", 2);
        live_put(&col, "b", 9);

        let frozen_through = col.wal_writer.lock().unwrap().current_wal_id;
        col.compact().unwrap();

        assert_eq!(col.wal_writer.lock().unwrap().current_wal_id, frozen_through + 2,
            "writes must continue on a brand new WAL, not the compaction output");
        assert_eq!(wal_ids_on_disk(&col.root_path), vec![frozen_through + 1, frozen_through + 2],
            "only the compacted WAL and the new active WAL survive");

        assert_eq!(col.index.read().unwrap().get("a").unwrap().wal_id, frozen_through + 1,
            "live keys are relocated into the compaction output");

        live_put(&col, "c", 7);
        assert_eq!(col.index.read().unwrap().get("c").unwrap().wal_id, frozen_through + 2,
            "post-compaction writes land on the active WAL");

        assert_eq!(col.get("a").unwrap(), Some(serde_json::json!({"v": 2})));
        assert_eq!(col.get("b").unwrap(), Some(serde_json::json!({"v": 9})));
        assert_eq!(col.get("c").unwrap(), Some(serde_json::json!({"v": 7})));

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn compaction_rejects_a_second_concurrent_run() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        live_put(&col, "a", 1);

        col.compacting.store(true, Ordering::SeqCst);
        let err = col.compact().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "overlapping compactions must be refused, not interleaved");

        col.compacting.store(false, Ordering::SeqCst);
        col.compact().unwrap();
        assert!(!col.compacting.load(Ordering::SeqCst), "the in-progress flag must clear when compaction finishes");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn concurrent_writes_during_compaction_are_never_lost() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 0..3000 {
            live_put(&col, &format!("k{}", i), 0);
        }

        let writer_col = col.clone();
        let writer = std::thread::spawn(move || {
            while !writer_col.compacting.load(Ordering::SeqCst) {
                std::hint::spin_loop();
            }
            for i in 0..200 {
                let _guard = writer_col.key_lock(&format!("k{}", i)).blocking_lock();
                live_put(&writer_col, &format!("k{}", i), 1);
            }
        });

        col.compact().unwrap();
        writer.join().unwrap();

        for i in 0..200 {
            assert_eq!(col.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": 1})),
                "a write racing compaction must survive it");
        }
        for i in 200..3000 {
            assert_eq!(col.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": 0})),
                "untouched keys must survive relocation");
        }

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn compacted_collection_replays_to_the_same_state_after_restart() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            live_put(&col, "a", 1);
            live_put(&col, "a", 2);
            live_put(&col, "b", 9);
            col.compact().unwrap();
            live_put(&col, "b", 10);
            live_put(&col, "c", 3);
            col.enqueue_commit().await.unwrap().unwrap();
        }

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("c").unwrap();

        assert_eq!(col2.get("a").unwrap(), Some(serde_json::json!({"v": 2})));
        assert_eq!(col2.get("b").unwrap(), Some(serde_json::json!({"v": 10})),
            "a post-compaction overwrite must win over the relocated copy on replay");
        assert_eq!(col2.get("c").unwrap(), Some(serde_json::json!({"v": 3})));
        assert_eq!(col2.index.read().unwrap().len(), 3);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn compaction_drops_deleted_keys_and_keeps_them_deleted_after_restart() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            live_put(&col, "keep", 1);
            live_put(&col, "gone", 2);

            col.delete("gone".into(), 1).unwrap();
            col.index.write().unwrap().remove("gone");

            col.compact().unwrap();
            col.enqueue_commit().await.unwrap().unwrap();

            assert!(col.get("gone").unwrap().is_none());
        }

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        assert_eq!(col2.get("keep").unwrap(), Some(serde_json::json!({"v": 1})));
        assert!(col2.get("gone").unwrap().is_none(), "a tombstoned key must not come back after compaction + replay");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn reopening_after_drop_starts_empty() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let col = db.get_collection("users").unwrap();
        let (_, w, o, _) = col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();
        col.index.write().unwrap().insert("k".into(), IndexEntry { wal_id: w, offset: o });

        db.drop_collection("users").unwrap();

        let fresh = db.get_collection("users").unwrap();
        assert!(fresh.index.read().unwrap().is_empty(), "dropped data must not resurrect on reopen");
        assert!(fresh.get("k").unwrap().is_none());

        let _ = fs::remove_dir_all(&root);
    }
}