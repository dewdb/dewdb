use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const WAL_ROTATION_LIMIT: u64 = 50 * 1024 * 1024;
const MAX_RECORD_SIZE: u64 = 10 * 1024 * 1024;
const INDEX_FILENAME: &str = "index-current.bin";

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct IndexEntry {
    wal_id: u64,
    offset: u64,
}

#[derive(Serialize, Deserialize)]
struct IndexSnapshot {
    last_wal_id: u64,
    last_offset: u64,
    last_lsn: u64,
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
    commit_index: Option<u64>,
    #[serde(with = "base64_bytes")]
    wal_frame: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ReplicationMeta {
    term: u64,
    is_leader: bool,
}

struct ReplicationState {
    term: u64,
    is_leader: bool,
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

    fn base64_encode(input: &[u8]) -> String {
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

    fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
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
    wal_pause_cv: Condvar,
    commit_notifiers: Arc<std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<Result<(), String>>>>>,
    commit_signal: Arc<tokio::sync::Notify>,
    read_pool: std::sync::Mutex<HashMap<u64, Vec<Arc<std::sync::Mutex<File>>>>>,
    read_pool_counter: AtomicUsize,
    db_global_commit_index: Arc<AtomicU64>,
    db_next_lsn: Arc<AtomicU64>,
}

struct WalsState {
    current_wal: File,
    current_wal_id: u64,
    current_wal_size: u64,
    last_appended_lsn: u64,
    commit_paused: bool,
    write_paused: bool,
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
}

struct Database {
    root_path: PathBuf,
    collections: RwLock<HashMap<String, Arc<Collection>>>,
    pub global_commit_index: Arc<AtomicU64>,
    pub next_lsn: Arc<AtomicU64>,
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
        )?);
        Collection::start_commit_task(col.clone());
        collections.insert(name.to_string(), col.clone());
        Ok(col)
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

        let mut replayed: u64 = 0;

        if !snapshot_loaded {
            println!("[{}] Replaying all WALs...", name);
             for (id, path) in &wal_files {
                replayed += Self::replay_file_from(*id, path, 0, &mut index)?;
            }
        } else {
             for (id, path) in &wal_files {
                 if *id < snapshot_wal_id {
                     continue;
                 } else if *id == snapshot_wal_id {
                     println!("[{}] Resuming WAL {} from offset {}", name, id, snapshot_offset);
                     replayed += Self::replay_file_from(*id, path, snapshot_offset, &mut index)?;
                 } else {
                     replayed += Self::replay_file_from(*id, path, 0, &mut index)?;
                 }
             }
        }

        let boot_lsn = snapshot_lsn + replayed;
        db_next_lsn.fetch_max(boot_lsn, Ordering::SeqCst);

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
                commit_paused: false,
                write_paused: false,
            }),
            wal_pause_cv: Condvar::new(),
            commit_notifiers: Arc::new(std::sync::Mutex::new(Vec::new())),
            commit_signal: Arc::new(tokio::sync::Notify::new()),
            read_pool: std::sync::Mutex::new(HashMap::new()),
            read_pool_counter: AtomicUsize::new(0),
            db_global_commit_index,
            db_next_lsn,
        })
    }

    fn replay_file_from(wal_id: u64, path: &PathBuf, mut start_offset: u64, index: &mut BTreeMap<String, IndexEntry>) -> io::Result<u64> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let file_len = file.metadata()?.len();

        if start_offset > file_len {
            start_offset = 0;
        }

        file.seek(SeekFrom::Start(start_offset))?;

        let mut offset = start_offset;
        let mut valid_end_offset = start_offset;
        let mut records: u64 = 0;

        loop {
            let mut header = [0u8; 8];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

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
                records += 1;
            }
            offset += 8 + len as u64;
            valid_end_offset = offset;
        }

        if valid_end_offset < file_len {
            file.set_len(valid_end_offset)?;
            println!("[{}] Truncated corrupted WAL file down to size {}", path.display(), valid_end_offset);
        }

        Ok(records)
    }

    fn current_timestamp() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    fn put(&self, key: String, value: serde_json::Value) -> io::Result<(Vec<u8>, u64, u64)> {
        let entry = LogEntry::Put {
            key: key.clone(),
            value,
            ts: Self::current_timestamp(),
        };
        self.append(entry)
    }

    fn delete(&self, key: String) -> io::Result<(Vec<u8>, u64, u64)> {
        let entry = LogEntry::Del {
            key: key.clone(),
            ts: Self::current_timestamp(),
        };
        self.append(entry)
    }

    fn append(&self, entry: LogEntry) -> io::Result<(Vec<u8>, u64, u64)> {
        let json_bytes = serde_json::to_vec(&entry)?;
        let len = json_bytes.len() as u64;

        if len > MAX_RECORD_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Record exceeds maximum size"));
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&json_bytes);
        let crc = hasher.finalize();

        let mut header = [0u8; 8];
        header[0..4].copy_from_slice(&(len as u32).to_le_bytes());
        header[4..8].copy_from_slice(&crc.to_le_bytes());
        let frame_len = 8 + len;

        let mut frame = Vec::with_capacity(frame_len as usize);
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&json_bytes);

        let mut wal = self.wal_writer.lock().unwrap();
        while wal.write_paused {
            wal = self.wal_pause_cv.wait(wal).unwrap();
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

        wal.current_wal.write_all(&header)?;
        wal.current_wal.write_all(&json_bytes)?;

        let offset = wal.current_wal_size;
        wal.current_wal_size += frame_len;

        let wal_id = wal.current_wal_id;

        let lsn = self.db_next_lsn.fetch_add(1, Ordering::SeqCst) + 1;
        wal.last_appended_lsn = lsn;

        drop(wal);

        Ok((frame, wal_id, offset))
    }

    fn append_raw_frame(&self, frame_bytes: &[u8]) -> io::Result<(u64, u64)> {
        if frame_bytes.len() < 8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Frame too short"));
        }

        let len = u32::from_le_bytes(frame_bytes[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(frame_bytes[4..8].try_into().unwrap());

        if frame_bytes.len() < 8 + len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Frame payload incomplete"));
        }

        let payload = &frame_bytes[8..8 + len];

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload);
        if hasher.finalize() != crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch on replicated frame"));
        }

        let _entry: LogEntry = serde_json::from_slice(payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let frame_len = (8 + len) as u64;

        let mut wal = self.wal_writer.lock().unwrap();
        while wal.write_paused {
            wal = self.wal_pause_cv.wait(wal).unwrap();
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

        wal.current_wal.write_all(&frame_bytes[..8 + len])?;

        let offset = wal.current_wal_size;
        let wal_id = wal.current_wal_id;

        wal.current_wal_size += frame_len;

        let lsn = self.db_next_lsn.fetch_add(1, Ordering::SeqCst) + 1;
        wal.last_appended_lsn = lsn;

        Ok((wal_id, offset))
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

                let has_pending = {
                    let q = col.commit_notifiers.lock().unwrap();
                    !q.is_empty()
                };

                let paused = { col.wal_writer.lock().unwrap().commit_paused };

                if !has_pending && !paused {
                    continue;
                }

                if paused {
                    tokio::time::sleep(Duration::from_millis(2)).await;
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

            let mut header = [0u8; 8];
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
        println!("[{}] Starting compaction...", self.name);

        let mut wal_guard = self.wal_writer.lock().unwrap();
        let mut index_guard = self.index.write().unwrap();

        wal_guard.current_wal.sync_data()?;
        wal_guard.commit_paused = true;
        wal_guard.write_paused = true;

        let new_wal_id = wal_guard.current_wal_id + 1;
        let compact_path = self.root_path.join("wal-compacted.tmp");
        let mut compact_file = BufWriter::new(File::create(&compact_path)?);

        let mut new_index_map = BTreeMap::new();
        let mut current_offset = 0;

        for (key, old_entry) in index_guard.iter() {
             let path = self.root_path.join(format!("wal-{:05}.log", old_entry.wal_id));
             if let Ok(mut file) = File::open(&path) {
                 file.seek(SeekFrom::Start(old_entry.offset))?;
                 let mut header = [0u8; 8];
                 if file.read_exact(&mut header).is_err() { continue; }
                 let len = u32::from_le_bytes(header[0..4].try_into().unwrap());

                 let mut payload = vec![0u8; len as usize];
                 if file.read_exact(&mut payload).is_err() { continue; }

                 if let Ok(LogEntry::Put { value, .. }) = serde_json::from_slice::<LogEntry>(&payload) {
                     let new_entry = LogEntry::Put {
                         key: key.clone(),
                         value,
                         ts: Self::current_timestamp(),
                     };

                     let json_bytes = serde_json::to_vec(&new_entry)?;
                     let new_len = json_bytes.len() as u32;
                     let mut hasher = crc32fast::Hasher::new();
                     hasher.update(&json_bytes);
                     let crc = hasher.finalize();

                     let mut new_header = [0u8; 8];
                     new_header[0..4].copy_from_slice(&new_len.to_le_bytes());
                     new_header[4..8].copy_from_slice(&crc.to_le_bytes());

                     compact_file.write_all(&new_header)?;
                     compact_file.write_all(&json_bytes)?;

                     let frame_len = 8 + new_len as u64;
                     new_index_map.insert(key.clone(), IndexEntry {
                         wal_id: new_wal_id,
                         offset: current_offset,
                     });
                     current_offset += frame_len;
                 }
             }
        }

        wal_guard.current_wal_size = current_offset;

        compact_file.flush()?;
        compact_file.get_mut().sync_all()?;

        let final_path = self.root_path.join(format!("wal-{:05}.log", new_wal_id));
        fs::rename(&compact_path, &final_path)?;

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&final_path)?;

        wal_guard.current_wal = new_file;
        wal_guard.current_wal_id = new_wal_id;
        wal_guard.current_wal_size = current_offset;

        for entry in fs::read_dir(&self.root_path)? {
             let entry = entry?;
             let path = entry.path();
             if let Some(name) = entry.file_name().to_str() {
                 if name.starts_with("wal-") && name.ends_with(".log") {
                     if name != format!("wal-{:05}.log", new_wal_id) {
                         let _ = fs::remove_file(path);
                     }
                 }
             }
        }

        self.read_pool.lock().unwrap().clear();

        *index_guard = new_index_map;

        wal_guard.commit_paused = false;
        wal_guard.write_paused = false;
        self.wal_pause_cv.notify_all();
        self.commit_signal.notify_one();

        println!("[{}] Compaction complete.", self.name);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct CreateDoc {
    value: serde_json::Value,
}

#[derive(Deserialize)]
struct QueryParams {
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    filter: Option<String>,
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

fn replicate_to_peers(
    client: reqwest::Client,
    replicas: Vec<String>,
    collection: String,
    frame: Vec<u8>,
    term: u64,
    commit_index: u64,
) {
    if replicas.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(3));
        let mut handles = Vec::new();
        for replica_url in replicas {
            let client = client.clone();
            let col = collection.clone();
            let frame = frame.clone();
            let sem = semaphore.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                let url = format!("{}/internal/replicate", replica_url);
                let req_body = ReplicateRequest {
                    collection: col,
                    term,
                    commit_index: Some(commit_index),
                    wal_frame: frame,
                };
                match client.post(&url).json(&req_body).send().await {
                    Ok(r) if r.status().is_success() => {},
                    Ok(r) => {
                        eprintln!("[replication] Replica {} returned {} (term={})", replica_url, r.status(), term);
                    },
                    Err(e) => {
                        eprintln!("[replication] Replica {} failed: {} (term={})", replica_url, e, term);
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    });
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

async fn router_forward_write(
    state: &AppState,
    col_name: &str,
    key: &str,
    method: ForwardMethod,
    body: Option<&CreateDoc>,
) -> Result<reqwest::Response, axum::response::Response> {
    let hash = hash_key(col_name, key);

    let (effective_url, original_url, replica_urls) = match state.get_effective_shard_url(hash) {
        Some(t) => t,
        None => return Err((StatusCode::BAD_REQUEST, "Key not owned by any shard").into_response()),
    };

    let full_url = format!("{}/collections/{}/docs/{}", effective_url, col_name, key);
    if let Ok(r) = build_forward(&state.client, &method, &full_url, body).send().await {
        if r.status().is_success() {
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
            let retry_url = format!("{}/collections/{}/docs/{}", latest_url, col_name, key);
            if let Ok(r) = build_forward(&state.client, &method, &retry_url, body).send().await {
                if r.status().is_success() {
                    return Ok(r);
                }
            }
        }
    }

    state.primary_overrides.lock().unwrap().remove(&original_url);
    for replica in &replica_urls {
        let fallback_url = format!("{}/collections/{}/docs/{}", replica, col_name, key);
        if let Ok(r) = build_forward(&state.client, &method, &fallback_url, body).send().await {
            if r.status().is_success() {
                state.set_primary_override(&original_url, replica);
                println!("[router] Cached new primary: {} -> {}", original_url, replica);
                return Ok(r);
            }
        }
    }

    Err((StatusCode::BAD_GATEWAY, "All shard nodes unreachable").into_response())
}

async fn passthrough_json(r: reqwest::Response) -> axum::response::Response {
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    let json_body: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body));
    (status, Json(json_body)).into_response()
}

async fn local_write(
    state: &AppState,
    col_name: &str,
    key: String,
    value: Option<serde_json::Value>,
) -> Result<(), axum::response::Response> {
    let db = state.db.as_ref().unwrap();
    let col = match db.get_collection(col_name) {
        Ok(c) => c,
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()),
    };

    let col_clone = col.clone();
    let key_clone = key.clone();
    let is_delete = value.is_none();

    let write_res = tokio::task::spawn_blocking(move || {
        match value {
            Some(v) => col_clone.put(key_clone, v),
            None => col_clone.delete(key_clone),
        }
    }).await;

    match write_res {
        Ok(Ok((frame, wal_id, offset))) => {
            let commit_rx = col.enqueue_commit();
            match commit_rx.await {
                Ok(Ok(())) => {
                    {
                        let mut index = col.index.write().unwrap();
                        if is_delete {
                            index.remove(&key);
                        } else {
                            index.insert(key.clone(), IndexEntry { wal_id, offset });
                        }
                    }
                    if state.is_leader() {
                        let commit_index = db.global_commit_index.load(Ordering::SeqCst);
                        replicate_to_peers(state.client.clone(), state.get_replicas(), col_name.to_string(), frame, state.current_term(), commit_index);
                    }
                    Ok(())
                },
                Ok(Err(e)) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))).into_response()),
                Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()),
            }
        },
        Ok(Err(e)) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()),
    }
}

async fn create_doc(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    let id = Uuid::new_v4().to_string();

    if state.config.role == "router" {
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Put, Some(&payload)).await {
            Ok(_) => (StatusCode::CREATED, Json(serde_json::json!({"id": id}))).into_response(),
            Err(resp) => resp,
        };
    }

    match local_write(&state, &col_name, id.clone(), Some(payload.value)).await {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!({"id": id}))).into_response(),
        Err(resp) => resp,
    }
}

async fn put_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if state.config.role == "router" {
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Put, Some(&payload)).await {
            Ok(r) => passthrough_json(r).await,
            Err(resp) => resp,
        };
    }

    match local_write(&state, &col_name, id.clone(), Some(payload.value)).await {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!({"id": id}))).into_response(),
        Err(resp) => resp,
    }
}

async fn get_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        let hash = hash_key(&col_name, &id);
        if let Some((target_url, _, _)) = state.get_effective_shard_url(hash) {
            let full_url = format!("{}/collections/{}/docs/{}", target_url, col_name, id);
            let res = state.client.get(&full_url).send().await;
            match res {
                Ok(r) => return passthrough_json(r).await,
                Err(e) => return (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
            }
        }
        return (StatusCode::BAD_REQUEST, "Key not owned by any shard").into_response();
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    };

    let key = id.clone();
    let col_clone = col.clone();

    match tokio::task::spawn_blocking(move || col_clone.get(&key)).await {
        Ok(Ok(Some(val))) => (StatusCode::OK, Json(val)).into_response(),
        Ok(Ok(None)) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"}))).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

async fn update_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if state.config.role == "router" {
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Patch, Some(&payload)).await {
            Ok(r) => passthrough_json(r).await,
            Err(resp) => resp,
        };
    }

    match local_write(&state, &col_name, id.clone(), Some(payload.value)).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"status": "updated"}))).into_response(),
        Err(resp) => resp,
    }
}

async fn delete_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if state.config.role == "router" {
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Delete, None).await {
            Ok(r) => passthrough_json(r).await,
            Err(resp) => resp,
        };
    }

    match local_write(&state, &col_name, id.clone(), None).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"status": "deleted"}))).into_response(),
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
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    };

    let col_clone = col.clone();
    match tokio::task::spawn_blocking(move || col_clone.list_all()).await {
        Ok(Ok(vals)) => (StatusCode::OK, Json(vals)).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

async fn query_docs(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
    Query(params): Query<QueryParams>,
    req: axum::extract::Request,
) -> impl axum::response::IntoResponse {
    let limit = params.limit.unwrap_or(100);

    if state.config.role == "router" {
        let mut unique_urls = std::collections::HashSet::new();
        for shard in &state.config.shard_map {
            unique_urls.insert(shard.node_url.clone());
        }

        let query_str = req.uri().query().map(|q| format!("?{}", q)).unwrap_or_default();

        let mut futures = Vec::new();
        for url in unique_urls {
            let full_url = format!("{}/collections/{}/query{}", url, col_name, query_str);
            let client = state.client.clone();

            futures.push(tokio::spawn(async move {
                let res = client.get(&full_url).send().await?;
                if res.status().is_success() {
                    let text = res.text().await?;
                    let json_arr: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap_or_default();
                    Ok::<_, reqwest::Error>(json_arr)
                } else {
                    Err(reqwest::Error::from(res.error_for_status().unwrap_err()))
                }
            }));
        }

        let shard_results = futures::future::try_join_all(futures).await;
        match shard_results {
            Ok(arrays) => {
                let mut merged_results = Vec::new();
                for items in arrays {
                    match items {
                        Ok(items) => {
                            for item in items {
                                merged_results.push(item);
                                if merged_results.len() >= limit {
                                    break;
                                }
                            }
                        },
                        Err(_) => {
                            return (StatusCode::BAD_GATEWAY, "Shard query failed").into_response();
                        }
                    }
                    if merged_results.len() >= limit {
                        break;
                    }
                }
                return (StatusCode::OK, Json(merged_results)).into_response();
            },
            Err(_) => {
                return (StatusCode::BAD_GATEWAY, "Shard query failed").into_response();
            }
        }
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    };

    let col_clone = col.clone();

    let filter_obj: Option<Filter> = params.filter
        .as_ref()
        .and_then(|f| serde_json::from_str::<Filter>(f).ok());

    match tokio::task::spawn_blocking(move || {
        let mut results = Vec::with_capacity(limit);

        for (key, _entry) in col_clone.range(
            params.start.as_deref(),
            params.end.as_deref()
        ).into_iter() {
            if let Ok(Some(val)) = col_clone.get(&key) {
                if let Some(ref f) = filter_obj {
                    if matches_filter(&val, f) {
                        results.push(val);
                    }
                } else {
                    results.push(val);
                }

                if results.len() >= limit {
                    break;
                }
            }
        }
        Ok::<_, io::Error>(results)
    }).await {
        Ok(Ok(vals)) => (StatusCode::OK, Json(vals)).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
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
        return (StatusCode::FORBIDDEN, "Only replica nodes accept replication").into_response();
    }

    let our_term = state.current_term();
    if req.term < our_term {
        eprintln!("[replicate] Rejecting stale frame: req term {} < our term {}", req.term, our_term);
        return (StatusCode::CONFLICT, "Stale term").into_response();
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
    let col_clone = col.clone();

    let entry_opt = serde_json::from_slice::<LogEntry>(&frame[8..]).ok();

    match tokio::task::spawn_blocking(move || col_clone.append_raw_frame(&frame)).await {
        Ok(Ok((wal_id, offset))) => {
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
                    (StatusCode::OK, "replicated").into_response()
                },
                Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        },
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
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
                        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
                        repl.last_heartbeat = Some(std::time::Instant::now());
                        if let Some(idx) = hb.get("commit_index").and_then(|v| v.as_u64()) {
                            repl.last_known_primary_position = Some(idx);
                        }
                    }
                },
                Ok(r) => {
                    eprintln!("[heartbeat] Primary {} returned {}", primary_addr, r.status());
                },
                Err(e) => {
                    eprintln!("[heartbeat] Primary {} unreachable: {}", primary_addr, e);
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
                try_promote(&state, election_delay).await;
                if state.is_leader() {
                    break;
                }
            }
        }
    });
}

async fn try_promote(state: &AppState, max_delay_ms: u64) {
    let delay_ms = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        state.config.node_id.hash(&mut h);
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos().hash(&mut h);
        h.finish() % max_delay_ms
    };
    println!("[election] Waiting {}ms before promotion attempt...", delay_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    {
        let primary_addr = state.replication.as_ref().unwrap().read().unwrap().primary_addr.clone();
        if let Some(addr) = primary_addr {
            let url = format!("{}/internal/heartbeat", addr);
            if let Ok(r) = state.client.get(&url).send().await {
                if r.status().is_success() {
                    println!("[election] Primary recovered during delay, aborting promotion");
                    let mut repl = state.replication.as_ref().unwrap().write().unwrap();
                    repl.last_heartbeat = Some(std::time::Instant::now());
                    return;
                }
            }
        }
    }

    let mut repl = state.replication.as_ref().unwrap().write().unwrap();
    repl.term += 1;
    repl.is_leader = true;
    repl.heartbeat_running = false;
    repl.primary_addr = None;

    let new_term = repl.term;
    drop(repl);

    let meta = ReplicationMeta { term: new_term, is_leader: true };
    if let Err(e) = meta.save("./data") {
        eprintln!("[election] WARNING: Failed to persist replication meta: {}", e);
    }

    println!("[election] *** PROMOTED to primary at term {} ***", new_term);
    println!("[election] Node {} is now accepting writes", state.config.node_id);
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
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "No database"}))).into_response(),
    };

    let col = match db.get_collection(&params.collection) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
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
        Ok(Err(e)) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
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

    let old_path = db.root_path.join(format!("{}.old", collection_name));
    if old_path.exists() {
        let _ = fs::remove_dir_all(&old_path);
    }
    if col_path.exists() {
        fs::rename(&col_path, &old_path).map_err(|e| format!("Failed to backup old col dir: {}", e))?;
    }
    fs::rename(&tmp_path, &col_path).map_err(|e| format!("Failed to finalize new col dir: {}", e))?;
    if old_path.exists() {
        let _ = fs::remove_dir_all(&old_path);
    }

    println!("[replica-sync] Restored {} files for collection '{}'", files.len(), collection_name);

    {
        let mut collections = db.collections.write().unwrap();
        collections.remove(collection_name);
    }
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
        let (term, is_leader) = if let Some(ref m) = meta {
            println!("[boot] Restored replication state: term={}, is_leader={}", m.term, m.is_leader);
            let mut boot_term = m.term;
            if m.is_leader {
                boot_term += 1;
                let new_meta = ReplicationMeta { term: boot_term, is_leader: true };
                let _ = new_meta.save("./data");
                println!("[boot] Escalated leader term to {} to prevent split brain.", boot_term);
            }
            (boot_term, m.is_leader)
        } else {
            let is_primary = config.shard_role.as_deref() == Some("primary");
            (0, is_primary)
        };

        Some(Arc::new(RwLock::new(ReplicationState {
            term,
            is_leader,
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
        .route("/collections/:name/docs", post(create_doc).get(list_docs))
        .route("/collections/:name/query", get(query_docs))
        .route("/collections/:name/docs/:id", get(get_doc).put(put_doc).patch(update_doc).delete(delete_doc));

    if config.role == "shard" {
        app = app
            .route("/internal/replicate", post(replicate_handler))
            .route("/internal/snapshot", get(snapshot_handler))
            .route("/internal/heartbeat", get(heartbeat_handler));
    }

    let app = app.with_state(state.clone());

    if config.role == "shard" && !state.is_leader() {
        println!("[boot] Starting heartbeat poll task (timeout={}s, delay={}ms)",
            config.heartbeat_timeout_secs, config.election_delay_ms);
        heartbeat_poll_task(state.clone());
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
                let _ = col.put(format!("key:{}", i), serde_json::json!({"n": i})).unwrap();
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
        let res = col2.put("huge_key".to_string(), serde_json::json!({"data": huge_str}));
        assert!(res.is_err(), "Should reject a record that exceeds MAX_RECORD_SIZE");

        if let Ok((_f, wal_id, offset)) = col2.put("key_pre_corrupt".to_string(), serde_json::json!({"valid": true})) {
            col2.enqueue_commit().await.unwrap().unwrap();
            col2.index.write().unwrap().insert("key_pre_corrupt".to_string(), IndexEntry { wal_id, offset });
        }

        let active_wal_path = {
            let wal_writer = col2.wal_writer.lock().unwrap();
            col2.root_path.join(format!("wal-{:05}.log", wal_writer.current_wal_id))
        };

        {
            let mut f = OpenOptions::new().append(true).open(&active_wal_path).unwrap();

            let bad_len: u32 = 100;
            let mut bad_header = [0u8; 8];
            bad_header[0..4].copy_from_slice(&bad_len.to_le_bytes());
            f.write_all(&bad_header).unwrap();

            let huge_len: u32 = (MAX_RECORD_SIZE + 5000) as u32;
            bad_header[0..4].copy_from_slice(&huge_len.to_le_bytes());
            f.write_all(&bad_header).unwrap();
        }

        drop(col2);
        drop(db2);

        let db3 = Database::new(&root).unwrap();
        let col3 = db3.get_collection("test_durability").unwrap();

        assert!(col3.get("key_pre_corrupt").unwrap().is_some(), "key_pre_corrupt should survive corruption after it");

        if let Ok((_f, wal_id, offset)) = col3.put("key_post_corrupt".to_string(), serde_json::json!({"valid": true})) {
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
                let _ = col.put(format!("a:{}", i), serde_json::json!({"i": i})).unwrap();
            }
            col.enqueue_commit().await.unwrap().unwrap();
            assert_eq!(db.global_commit_index.load(Ordering::SeqCst), 10);
        }

        {
            let db = Database::new(&root).unwrap();
            assert_eq!(db.global_commit_index.load(Ordering::SeqCst), 10, "Commit LSN must be restored from lsn.meta");
            let col = db.get_collection("lsn_check").unwrap();
            for i in 0..5 {
                let _ = col.put(format!("b:{}", i), serde_json::json!({"i": i})).unwrap();
            }
            col.enqueue_commit().await.unwrap().unwrap();
            assert_eq!(db.global_commit_index.load(Ordering::SeqCst), 15, "LSN must continue from restored value, not reset to zero");
        }

        let _ = fs::remove_dir_all(&root);
    }
}