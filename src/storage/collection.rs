//! A collection's index, key locks, group commit, and read path.

use super::frame::LogEntry;
use super::index::{AppliedMeta, IndexEntry, IndexSnapshot, LsnMeta, ReadCacheConfig, INDEX_FILENAME};
use super::wal::WalsState;
use crate::query::{matches_filter, Filter};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

const KEY_LOCK_STRIPES: usize = 64;
pub const READ_POOL_HANDLES: usize = 4;
const COMMIT_BATCH_THRESHOLD: usize = 32;
const COMMIT_INTERVAL_MS: u64 = 5;

pub struct Collection {
    pub name: String,
    pub root_path: PathBuf,
    pub data_root: PathBuf,
    pub index: RwLock<BTreeMap<String, IndexEntry>>,
    pub wal_writer: std::sync::Mutex<WalsState>,
    pub key_locks: Vec<tokio::sync::Mutex<()>>,
    pub commit_notifiers: Arc<std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<Result<(), String>>>>>,
    pub commit_signal: Arc<tokio::sync::Notify>,
    pub read_pool: std::sync::Mutex<HashMap<u64, Vec<Arc<std::sync::Mutex<File>>>>>,
    pub read_pool_counter: AtomicUsize,
    pub released: AtomicBool,
    pub compacting: AtomicBool,
    pub cache: ReadCacheConfig,
    pub inline_bytes: AtomicU64,
    // This collection's fsynced tail, distinct from the database-wide durable_lsn.
    pub durable_lsn: AtomicU64,
    // Durable but uncommitted. The index holds committed state only: a leader change can still revoke these.
    pub pending: std::sync::Mutex<BTreeMap<u64, StagedApply>>,
    pub applied_lsn: AtomicU64,
    watermark_recorded: AtomicBool,
    pub db_durable_lsn: Arc<AtomicU64>,
    pub db_next_lsn: Arc<AtomicU64>,
    pub db_last_log_term: Arc<AtomicU64>,
}

pub struct StagedApply {
    pub key: String,
    pub wal_id: u64,
    pub offset: u64,
    pub entry: Option<IndexEntry>,
}

impl Collection {
    // Recovery ordering: the snapshot sets the replay resume point; the tail LSN is the max of both.
    pub fn open(
        name: String,
        root_path: PathBuf,
        db_durable_lsn: Arc<AtomicU64>,
        db_next_lsn: Arc<AtomicU64>,
        db_last_log_term: Arc<AtomicU64>,
        cache: ReadCacheConfig,
    ) -> io::Result<Self> {
        fs::create_dir_all(&root_path)?;
        let data_root = root_path.parent().unwrap_or(Path::new(".")).to_path_buf();

        // Absent means no consensus history (fresh node, or standalone engine): replay everything.
        let applied_through = AppliedMeta::load(&root_path)
            .map(|m| m.applied_lsn)
            .unwrap_or(u64::MAX);

        let mut index = BTreeMap::new();
        let mut pending: BTreeMap<u64, StagedApply> = BTreeMap::new();
        let mut inline_used: u64 = 0;
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
                            info!(target: "storage", collection = %name, wal_id = snapshot.last_wal_id,
                                offset = snapshot.last_offset, lsn = snapshot.last_lsn,
                                entries = snapshot.map.len(), "Loaded persisted index snapshot");
                            inline_used = snapshot.map.values().map(|e| e.inline_bytes()).sum();
                            index = snapshot.map;
                            snapshot_wal_id = snapshot.last_wal_id;
                            snapshot_offset = snapshot.last_offset;
                            snapshot_lsn = snapshot.last_lsn;
                            snapshot_term = snapshot.last_term;
                            snapshot_loaded = true;
                        }
                        Err(e) => warn!(target: "storage", collection = %name, error = %e, "Snapshot unreadable (likely legacy format); rebuilding from WAL"),
                    }
                }
                Err(e) => warn!(target: "storage", collection = %name, error = %e, "Failed to open index file"),
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
            info!(target: "storage", collection = %name, "Replaying all WALs");
             for (id, path) in &wal_files {
                let r = Self::replay_file_from(*id, path, 0, &mut index, &mut pending, applied_through, &cache, &mut inline_used)?;
                fold(r, &mut max_lsn, &mut max_term);
            }
        } else {
             for (id, path) in &wal_files {
                 if *id < snapshot_wal_id {
                     continue;
                 } else if *id == snapshot_wal_id {
                     info!(target: "storage", collection = %name, wal_id = id, offset = snapshot_offset, "Resuming WAL from snapshot offset");
                     let r = Self::replay_file_from(*id, path, snapshot_offset, &mut index, &mut pending, applied_through, &cache, &mut inline_used)?;
                     fold(r, &mut max_lsn, &mut max_term);
                 } else {
                     let r = Self::replay_file_from(*id, path, 0, &mut index, &mut pending, applied_through, &cache, &mut inline_used)?;
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
        let inline_total = inline_used;

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
            cache,
            inline_bytes: AtomicU64::new(inline_total),
            durable_lsn: AtomicU64::new(boot_lsn),
            pending: std::sync::Mutex::new(pending),
            applied_lsn: AtomicU64::new(if applied_through == u64::MAX { boot_lsn } else { applied_through }),
            watermark_recorded: AtomicBool::new(applied_through != u64::MAX),
            db_durable_lsn,
            db_next_lsn,
            db_last_log_term,
        })
    }

    pub fn build_entry(&self, wal_id: u64, offset: u64, payload: &[u8]) -> IndexEntry {
        let len = payload.len() as u32;
        let inline = if len <= self.cache.inline_max_value_bytes
            && self.inline_bytes.load(Ordering::Relaxed) + len as u64 <= self.cache.inline_budget_bytes
        {
            Some(payload.to_vec().into_boxed_slice())
        } else {
            None
        };
        IndexEntry { wal_id, offset, len, inline }
    }

    pub fn apply_index_put(&self, index: &mut BTreeMap<String, IndexEntry>, key: String, entry: IndexEntry) {
        let added = entry.inline_bytes();
        let replaced = index.insert(key, entry).map_or(0, |old| old.inline_bytes());
        if added >= replaced {
            self.inline_bytes.fetch_add(added - replaced, Ordering::Relaxed);
        } else {
            self.inline_bytes.fetch_sub(replaced - added, Ordering::Relaxed);
        }
    }

    pub fn apply_index_remove(&self, index: &mut BTreeMap<String, IndexEntry>, key: &str) {
        if let Some(old) = index.remove(key) {
            self.inline_bytes.fetch_sub(old.inline_bytes(), Ordering::Relaxed);
        }
    }

    // Striped: distinct keys share locks, and batch callers must dedupe stripes or self-deadlock.
    pub fn key_stripe(&self, key: &str) -> usize {
        xxhash_rust::xxh64::xxh64(key.as_bytes(), 0) as usize % KEY_LOCK_STRIPES
    }

    pub fn key_lock(&self, key: &str) -> &tokio::sync::Mutex<()> {
        &self.key_locks[self.key_stripe(key)]
    }

    pub fn exists(&self, key: &str) -> bool {
        self.index.read().unwrap().contains_key(key)
    }

    fn current_timestamp() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    pub fn put(&self, key: String, value: serde_json::Value, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        let entry = LogEntry::Put {
            key: key.clone(),
            value,
            ts: Self::current_timestamp(),
        };
        self.append(entry, term)
    }

    pub fn delete(&self, key: String, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        let entry = LogEntry::Del {
            key: key.clone(),
            ts: Self::current_timestamp(),
        };
        self.append(entry, term)
    }

    // Group commit: one fsync serves every waiter; the tick bounds latency when the batch stays short.
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

    pub fn start_commit_task(col: Arc<Collection>) {
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
                    debug!(target: "storage", collection = %col.name, "Commit task stopped; handle released");
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
                    col.durable_lsn.fetch_max(last_lsn, Ordering::SeqCst);
                    col.db_durable_lsn.fetch_max(last_lsn, Ordering::SeqCst);
                    let commit_lsn = col.db_durable_lsn.load(Ordering::SeqCst);
                    let meta = LsnMeta { commit_lsn };
                    if let Err(e) = meta.save(&col.data_root) {
                        error!(target: "storage", collection = %col.name, error = %e, "Failed to persist lsn meta");
                    }
                }
            }
        });
    }

    pub fn range_from(&self, after: Option<&str>, start: Option<&str>, end: Option<&str>) -> Vec<String> {
        let index = self.index.read().unwrap();

        let start_bound = if let Some(a) = after {
            std::ops::Bound::Excluded(a)
        } else if let Some(s) = start {
            std::ops::Bound::Included(s)
        } else {
            std::ops::Bound::Unbounded
        };
        let end_bound = end.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included);

        index.range::<str, _>((start_bound, end_bound)).map(|(k, _)| k.clone()).collect()
    }

    pub fn query_page(
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

        for key in self.range_from(after, start, end).into_iter() {
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

    fn value_from_payload(payload: &[u8]) -> Option<serde_json::Value> {
        match serde_json::from_slice(payload) {
            Ok(LogEntry::Put { value, .. }) => Some(value),
            _ => None,
        }
    }

    fn read_entry(&self, entry: &IndexEntry) -> io::Result<Option<serde_json::Value>> {
        match &entry.inline {
            Some(payload) => Ok(Self::value_from_payload(payload)),
            None => {
                let payload = self.read_frame_payload(entry.wal_id, entry.offset)?;
                Ok(Self::value_from_payload(&payload))
            }
        }
    }

    pub fn get(&self, key: &str) -> io::Result<Option<serde_json::Value>> {
        let located = {
            let index = self.index.read().unwrap();
            match index.get(key) {
                None => return Ok(None),
                Some(entry) => match &entry.inline {
                    Some(payload) => return Ok(Self::value_from_payload(payload)),
                    None => (entry.wal_id, entry.offset),
                },
            }
        };

        let payload = self.read_frame_payload(located.0, located.1)?;
        Ok(Self::value_from_payload(&payload))
    }

    pub fn list_all(&self) -> io::Result<Vec<serde_json::Value>> {
        let mut resolved = Vec::new();
        let mut pending = Vec::new();

        {
            let index = self.index.read().unwrap();
            for (key, entry) in index.iter() {
                match &entry.inline {
                    Some(payload) => {
                        if let Some(v) = Self::value_from_payload(payload) {
                            resolved.push(v);
                        }
                    },
                    None => pending.push((key.clone(), entry.wal_id, entry.offset)),
                }
            }
        }

        for (_key, wal_id, offset) in pending {
            let payload = self.read_frame_payload(wal_id, offset)?;
            if let Some(v) = Self::value_from_payload(&payload) {
                resolved.push(v);
            }
        }

        Ok(resolved)
    }

    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::SeqCst)
    }

    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn.load(Ordering::SeqCst)
    }

    pub fn pending_len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// `entry` is None for a delete; keyed by LSN to drain in log order.
    pub fn stage(&self, lsn: u64, key: String, wal_id: u64, offset: u64, entry: Option<IndexEntry>) {
        // First stage marks this collection consensus-managed. Without the watermark now, a restart
        // before the first commit would apply-all and publish unacknowledged entries.
        if !self.watermark_recorded.swap(true, Ordering::SeqCst) {
            let applied_lsn = self.applied_lsn();
            if let Err(e) = (AppliedMeta { applied_lsn }).save(&self.root_path) {
                error!(target: "storage", collection = %self.name, error = %e,
                    "Failed to record applied watermark");
                self.watermark_recorded.store(false, Ordering::SeqCst);
            }
        }
        self.pending
            .lock()
            .unwrap()
            .insert(lsn, StagedApply { key, wal_id, offset, entry });
    }

    /// Position of the oldest frame the index does not yet reflect.
    pub fn pending_floor(&self) -> Option<(u64, u64)> {
        let pending = self.pending.lock().unwrap();
        pending.values().next().map(|s| (s.wal_id, s.offset))
    }

    /// Drains in log order; staged frames can arrive out of order.
    pub fn apply_committed(&self, committed_lsn: u64) -> usize {
        let ready = {
            let mut pending = self.pending.lock().unwrap();
            let mut ready = std::mem::take(&mut *pending);
            *pending = ready.split_off(&(committed_lsn + 1));
            ready
        };

        if !ready.is_empty() {
            let mut index = self.index.write().unwrap();
            for (_lsn, staged) in ready.iter() {
                match &staged.entry {
                    Some(entry) => self.apply_index_put(&mut index, staged.key.clone(), entry.clone()),
                    None => self.apply_index_remove(&mut index, &staged.key),
                }
            }
        }

        let previous = self.applied_lsn.fetch_max(committed_lsn, Ordering::SeqCst);
        if committed_lsn > previous {
            if let Err(e) = (AppliedMeta { applied_lsn: committed_lsn }).save(&self.root_path) {
                error!(target: "storage", collection = %self.name, error = %e,
                    "Failed to persist applied watermark; a restart will re-stage these entries");
            }
        }
        ready.len()
    }

    /// Read-modify-write must read the newest durable value; the committed one drops a racing write.
    pub fn get_including_staged(&self, key: &str) -> io::Result<Option<serde_json::Value>> {
        let staged = {
            let pending = self.pending.lock().unwrap();
            pending
                .values()
                .rev()
                .find(|s| s.key == key)
                .map(|s| s.entry.clone())
        };
        match staged {
            Some(None) => Ok(None),
            Some(Some(entry)) => self.read_entry(&entry),
            None => self.get(key),
        }
    }

    pub fn last_appended_lsn(&self) -> u64 {
        self.wal_writer.lock().unwrap().last_appended_lsn
    }

    // Temp file plus rename: a crash mid-write must leave the previous snapshot intact.
    pub fn save_index(&self) -> io::Result<u64> {
        let wal_writer = self.wal_writer.lock().unwrap();
        // Uncommitted frames are absent from the index; replay must resume at the oldest or lose them.
        let floor = self.pending_floor();
        let index = self.index.read().unwrap();

        let (resume_wal, resume_offset) = floor
            .unwrap_or((wal_writer.current_wal_id, wal_writer.current_wal_size));

        let snapshot = IndexSnapshot {
            last_wal_id: resume_wal,
            last_offset: resume_offset,
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

        let saved_lsn = snapshot.last_lsn;
        info!(target: "storage", collection = %self.name, wal_id = snapshot.last_wal_id, offset = snapshot.last_offset, lsn = saved_lsn, "Index snapshot saved");
        Ok(saved_lsn)
    }

    // Windows will not delete an open file; the writer is parked on a throwaway tombstone first.
    pub fn release_handles(&self) -> io::Result<PathBuf> {
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
        self.pending.lock().unwrap().clear();
        self.index.write().unwrap().clear();

        Ok(tombstone)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::merge_patch;
    use crate::storage::Database;
    use crate::storage::HEADER_LEN;
    use crate::test_support::{idx, live_put, stage_delete, stage_put, temp_root};
    use crate::util::remove_file_with_retry;
    use std::collections::HashSet;

    fn cache_cfg(max_value: u32, budget: u64) -> ReadCacheConfig {
        ReadCacheConfig { inline_max_value_bytes: max_value, inline_budget_bytes: budget }
    }

    fn inline_count(col: &Arc<Collection>) -> usize {
        col.index.read().unwrap().values().filter(|e| e.inline.is_some()).count()
    }

    #[tokio::test]
    async fn range_from_resumes_exclusively() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for k in ["a", "b", "c", "d"] {
            let (f, w, o, _) = col.put(k.to_string(), serde_json::json!({"k": k}), 1).unwrap();
            col.index.write().unwrap().insert(k.to_string(), idx(&f, w, o));
        }

        let inclusive: Vec<String> = col.range_from(None, Some("b"), None);
        assert_eq!(inclusive, vec!["b", "c", "d"], "start is inclusive");

        let exclusive: Vec<String> = col.range_from(Some("b"), None, None);
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
            let (f, w, o, _) = col.put(k.clone(), serde_json::json!({"i": i}), 1).unwrap();
            col.index.write().unwrap().insert(k, idx(&f, w, o));
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
            let (f, w, o, _) = col.put(k.clone(), serde_json::json!({"i": i}), 1).unwrap();
            col.index.write().unwrap().insert(k, idx(&f, w, o));
        }
        col.enqueue_commit().await.unwrap().unwrap();

        let (_p1, c1) = col.query_page(None, None, None, &None, 2).unwrap();
        assert_eq!(c1.as_deref(), Some("k2"));

        let (p2, c2) = col.query_page(c1.as_deref(), None, None, &None, 2).unwrap();
        assert_eq!(p2.len(), 2);
        assert_eq!(c2, None, "a full final page with nothing after must not emit a cursor");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn patch_persists_the_merged_document_not_the_patch() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let (f, w, o, _) = col.put(
            "d1".into(),
            serde_json::json!({"name": "alpha", "tags": ["x", "y"], "meta": {"v": 1, "owner": "latha"}}),
            1,
        ).unwrap();
        col.index.write().unwrap().insert("d1".into(), idx(&f, w, o));

        let mut doc = col.get("d1").unwrap().unwrap();
        merge_patch(&mut doc, &serde_json::json!({"meta": {"v": 2}, "tags": null, "status": "live"}));
        let (f2, w2, o2, _) = col.put("d1".into(), doc, 1).unwrap();
        col.index.write().unwrap().insert("d1".into(), idx(&f2, w2, o2));
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

        let (f, w, o, _) = col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();
        col.index.write().unwrap().insert("k".into(), idx(&f, w, o));
        assert!(col.exists("k"));

        col.index.write().unwrap().remove("k");
        assert!(!col.exists("k"), "a deleted key stops existing");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn cached_reads_do_not_touch_the_wal_at_all() {
        let root = temp_root();
        let db = Database::with_cache(&root, cache_cfg(512, 1 << 20)).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 0..20 {
            live_put(&col, &format!("k{}", i), i);
        }
        assert_eq!(inline_count(&col), 20, "small values must be inlined on write");

        col.read_pool.lock().unwrap().clear();
        for wal in fs::read_dir(&col.root_path).unwrap() {
            let path = wal.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) == Some("log") {
                remove_file_with_retry(&path).unwrap();
            }
        }

        assert_eq!(col.get("k7").unwrap(), Some(serde_json::json!({"v": 7})),
            "a cached read must be served without opening the WAL");
        assert_eq!(col.list_all().unwrap().len(), 20,
            "list_all must serve every cached doc without a single random read");
        assert_eq!(col.query_page(None, None, None, &None, 100).unwrap().0.len(), 20);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn values_over_the_threshold_stay_on_disk() {
        let root = temp_root();
        let db = Database::with_cache(&root, cache_cfg(64, 1 << 20)).unwrap();
        let col = db.get_collection("c").unwrap();

        let (f, w, o, _) = col.put("small".into(), serde_json::json!({"v": 1}), 1).unwrap();
        col.index.write().unwrap().insert("small".into(), col.build_entry(w, o, &f[HEADER_LEN..]));

        let big = "x".repeat(500);
        let (f2, w2, o2, _) = col.put("big".into(), serde_json::json!({"v": big.clone()}), 1).unwrap();
        col.index.write().unwrap().insert("big".into(), col.build_entry(w2, o2, &f2[HEADER_LEN..]));

        let index = col.index.read().unwrap();
        assert!(index.get("small").unwrap().inline.is_some(), "a value under the threshold is cached");
        assert!(index.get("big").unwrap().inline.is_none(), "a value over the threshold is not cached");
        drop(index);

        assert_eq!(col.get("big").unwrap(), Some(serde_json::json!({"v": big})),
            "an uncached value still reads correctly from the WAL");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn inline_budget_caps_memory_and_is_released_on_delete() {
        let root = temp_root();
        let db = Database::with_cache(&root, cache_cfg(512, 400)).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 0..20 {
            live_put(&col, &format!("k{}", i), i);
        }

        let cached = inline_count(&col);
        assert!(cached > 0, "some entries fit in the budget");
        assert!(cached < 20, "the budget must stop inlining once exhausted, got {}", cached);
        assert!(col.inline_bytes.load(Ordering::Relaxed) <= 400,
            "tracked inline memory must never exceed the budget");

        for i in 0..20 {
            assert_eq!(col.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": i})),
                "uncached keys still resolve from disk");
        }

        let before = col.inline_bytes.load(Ordering::Relaxed);
        {
            let mut index = col.index.write().unwrap();
            for i in 0..20 {
                col.apply_index_remove(&mut index, &format!("k{}", i));
            }
        }
        assert_eq!(col.inline_bytes.load(Ordering::Relaxed), 0,
            "removing every key must return the full budget (was {})", before);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn overwriting_a_key_refreshes_its_cached_value() {
        let root = temp_root();
        let db = Database::with_cache(&root, cache_cfg(512, 1 << 20)).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "k", 1);
        let after_first = col.inline_bytes.load(Ordering::Relaxed);

        live_put(&col, "k", 2);
        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 2})),
            "the cache must return the new value, not the stale one");
        assert_eq!(col.inline_bytes.load(Ordering::Relaxed), after_first,
            "an overwrite of equal size must not double-count budget");

        col.delete("k".into(), 1).unwrap();
        {
            let mut index = col.index.write().unwrap();
            col.apply_index_remove(&mut index, "k");
        }
        assert!(col.get("k").unwrap().is_none(), "a deleted key must not be served from cache");
        assert_eq!(col.inline_bytes.load(Ordering::Relaxed), 0);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn the_cache_survives_restart_and_compaction() {
        let root = temp_root();

        {
            let db = Database::with_cache(&root, cache_cfg(512, 1 << 20)).unwrap();
            let col = db.get_collection("c").unwrap();
            for i in 0..10 {
                live_put(&col, &format!("k{}", i), i);
            }
            col.compact().unwrap();
            assert_eq!(inline_count(&col), 10, "compaction must re-populate the cache as it relocates");
            col.save_index().unwrap();
        }

        let db2 = Database::with_cache(&root, cache_cfg(512, 1 << 20)).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        assert_eq!(inline_count(&col2), 10, "a snapshot restore must come back warm, not cold");
        assert_eq!(col2.inline_bytes.load(Ordering::Relaxed),
            col2.index.read().unwrap().values().map(|e| e.inline_bytes()).sum::<u64>(),
            "the budget counter must be rebuilt to match the restored entries");
        assert_eq!(col2.get("k3").unwrap(), Some(serde_json::json!({"v": 3})));

        let _ = fs::remove_dir_all(&root);
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
    async fn a_staged_write_is_invisible_until_it_is_committed() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let first = stage_put(&col, "a", 1);
        let second = stage_put(&col, "b", 2);
        assert_eq!(col.pending_len(), 2);
        assert!(col.get("a").unwrap().is_none(), "a durable but uncommitted write must not be readable");
        assert!(col.get("b").unwrap().is_none());

        assert_eq!(col.apply_committed(first), 1, "only the committed prefix is published");
        assert_eq!(col.get("a").unwrap(), Some(serde_json::json!({"v": 1})));
        assert!(col.get("b").unwrap().is_none(), "the entry above the watermark stays hidden");
        assert_eq!(col.pending_len(), 1);

        assert_eq!(col.apply_committed(second), 1);
        assert_eq!(col.get("b").unwrap(), Some(serde_json::json!({"v": 2})));
        assert_eq!(col.pending_len(), 0);
        assert_eq!(col.applied_lsn(), second);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn staged_frames_apply_in_log_order_not_arrival_order() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        stage_put(&col, "k", 1);
        let newer = stage_put(&col, "k", 2);
        col.apply_committed(newer);

        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 2})),
            "the later LSN must win regardless of how the staging map was walked");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn an_uncommitted_delete_does_not_hide_the_committed_value() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let put = stage_put(&col, "k", 1);
        col.apply_committed(put);

        let del = stage_delete(&col, "k");
        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 1})),
            "the delete is durable but not committed, so the old value is still the truth");

        col.apply_committed(del);
        assert!(col.get("k").unwrap().is_none());

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn read_modify_write_sees_the_staged_value() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let put = stage_put(&col, "k", 1);
        col.apply_committed(put);
        stage_put(&col, "k", 2);

        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 1})),
            "readers see committed state");
        assert_eq!(col.get_including_staged("k").unwrap(), Some(serde_json::json!({"v": 2})),
            "a patch must merge onto the newest durable value or it silently drops it");

        stage_delete(&col, "k");
        assert!(col.get_including_staged("k").unwrap().is_none(),
            "a staged delete is the newest durable state");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn compaction_waits_for_uncommitted_frames() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "a", 1);
        let staged = stage_put(&col, "b", 2);

        let err = col.compact().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock,
            "relocation skips entries missing from the index, so retiring their WAL would lose them");

        col.apply_committed(staged);
        col.compact().unwrap();
        assert_eq!(col.get("b").unwrap(), Some(serde_json::json!({"v": 2})));

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_snapshot_keeps_the_applied_prefix_and_restages_the_rest() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            live_put(&col, "applied", 1);
            stage_put(&col, "staged", 2);
            col.save_index().unwrap();
            col.enqueue_commit().await.unwrap().unwrap();
        }

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        assert_eq!(col2.get("applied").unwrap(), Some(serde_json::json!({"v": 1})),
            "the snapshot resumed replay at the oldest pending frame, so nothing before it was lost");
        assert!(col2.get("staged").unwrap().is_none(),
            "the uncommitted frame is back in the staging buffer, not published");
        assert_eq!(col2.pending_len(), 1);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn an_uncommitted_tail_stays_hidden_across_a_restart() {
        let root = temp_root();
        let committed;

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            committed = stage_put(&col, "safe", 1);
            stage_put(&col, "risky", 2);
            col.apply_committed(committed);
            col.enqueue_commit().await.unwrap().unwrap();

            assert_eq!(col.get("safe").unwrap(), Some(serde_json::json!({"v": 1})));
            assert!(col.get("risky").unwrap().is_none());
        }

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("c").unwrap();

        assert_eq!(col2.get("safe").unwrap(), Some(serde_json::json!({"v": 1})),
            "a committed entry survives the restart");
        assert!(col2.get("risky").unwrap().is_none(),
            "replaying the whole log at boot would publish an entry no quorum ever held,              and a new leader could still revoke it");
        assert_eq!(col2.applied_lsn(), committed);
        assert_eq!(col2.pending_len(), 1, "it is still durable, just not visible");

        // The restored node publishes it once the cluster commits it.
        col2.apply_committed(committed + 1);
        assert_eq!(col2.get("risky").unwrap(), Some(serde_json::json!({"v": 2})));

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_collection_with_no_consensus_history_replays_its_whole_log() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            for i in 0..5 {
                live_put(&col, &format!("k{}", i), i);
            }
            col.enqueue_commit().await.unwrap().unwrap();
        }

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        for i in 0..5 {
            assert_eq!(col2.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": i})),
                "with no watermark on disk the log is the state, so the storage engine                  stays usable on its own");
        }
        assert_eq!(col2.pending_len(), 0);

        let _ = fs::remove_dir_all(&root);
    }
}
