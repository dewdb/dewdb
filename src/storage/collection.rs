//! A collection's index, key locks, group commit, and read path.

use super::frame::{Configuration, LogEntry};
use super::index::{AppliedMeta, IndexEntry, IndexSnapshot, LsnMeta, ReadCacheConfig, INDEX_FILENAME};
use super::wal::WalsState;
use crate::model::MAX_QUERY_LIMIT;
use crate::query::{compare_rows, is_after, matches_filter, Filter, SortCursor, SortSpec, SortedRow};
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
const READ_RESOLVE_ATTEMPTS: usize = 3;
/// Keys cloned per index-lock acquisition by the chunked walk. Large enough that a scan is not
/// dominated by lock traffic, small enough that no caller pins a whole keyspace in memory.
const SCAN_CHUNK: usize = 1024;

/// A frame's `(wal_id, offset, len)` as the index records it.
type Located = (u64, u64, u32);

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
    /// Highest WAL id compaction has retired. Set once the index no longer points below it, so a
    /// location at or under this id is stale and must be re-resolved rather than read.
    pub retired_through: AtomicU64,
    pub released: AtomicBool,
    /// A committed drop emptied this collection and nothing has been written since. It still holds
    /// the log the drop lives in, which is what a lagging replica and a new leader read it from.
    pub dropped: AtomicBool,
    /// Newest committed `Config`. Rides `applied.meta` for the same reason `dropped` does.
    committed_config: std::sync::Mutex<Option<Configuration>>,
    pub compacting: AtomicBool,
    /// Prevents compaction from retiring WAL files during snapshot streaming.
    pub snapshot_boundary: std::sync::Mutex<()>,
    /// Held while this directory is being rewritten: compaction's publish-and-retire, snapshot
    /// rotation, and the handle release an install starts with. All three resolve `root_path`.
    pub rewriting: std::sync::Mutex<()>,
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
    pub wal_id: u64,
    pub offset: u64,
    /// The frame's own term. What tells a leader's retransmit from an entry of a log it replaced,
    /// which is the difference between a duplicate and a truncation.
    pub term: u64,
    pub effect: StagedEffect,
}

/// What committing a staged frame does to the index.
pub enum StagedEffect {
    Put { key: String, entry: IndexEntry },
    Remove { key: String },
    /// A barrier: it occupies an LSN and touches nothing.
    Nothing,
    /// A drop: every key goes, and the collection is a tombstone until something is written above it.
    Clear,
    /// A configuration. Like a barrier it touches no key; unlike one it leaves something behind.
    Configure(Configuration),
}

impl StagedEffect {
    /// What this frame leaves for `key`: `None` if it does not touch it, otherwise the entry it
    /// leaves behind, or `Some(None)` if the key is gone.
    fn resolve<'a>(&'a self, key: &str) -> Option<Option<&'a IndexEntry>> {
        match self {
            Self::Put { key: k, entry } if k == key => Some(Some(entry)),
            Self::Remove { key: k } if k == key => Some(None),
            Self::Clear => Some(None),
            _ => None,
        }
    }
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
        let applied = AppliedMeta::load(&root_path);
        let applied_through = applied.as_ref().map(|m| m.applied_lsn).unwrap_or(u64::MAX);
        // Seeded from the watermark because compaction retires the drop and config frames replay
        // would otherwise find them in.
        let mut dropped = applied.as_ref().is_some_and(|m| m.dropped);
        let mut committed_config = applied.and_then(|m| m.config);

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
                let r = Self::replay_file_from(*id, path, 0, &mut index, &mut pending, applied_through, &cache, &mut inline_used, &mut dropped, &mut committed_config)?;
                fold(r, &mut max_lsn, &mut max_term);
            }
        } else {
             for (id, path) in &wal_files {
                 if *id < snapshot_wal_id {
                     continue;
                 } else if *id == snapshot_wal_id {
                     info!(target: "storage", collection = %name, wal_id = id, offset = snapshot_offset, "Resuming WAL from snapshot offset");
                     let r = Self::replay_file_from(*id, path, snapshot_offset, &mut index, &mut pending, applied_through, &cache, &mut inline_used, &mut dropped, &mut committed_config)?;
                     fold(r, &mut max_lsn, &mut max_term);
                 } else {
                     let r = Self::replay_file_from(*id, path, 0, &mut index, &mut pending, applied_through, &cache, &mut inline_used, &mut dropped, &mut committed_config)?;
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
            retired_through: AtomicU64::new(0),
            released: AtomicBool::new(false),
            dropped: AtomicBool::new(dropped),
            committed_config: std::sync::Mutex::new(committed_config),
            compacting: AtomicBool::new(false),
            snapshot_boundary: std::sync::Mutex::new(()),
            rewriting: std::sync::Mutex::new(()),
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

    fn clear_index(&self, index: &mut BTreeMap<String, IndexEntry>) {
        index.clear();
        self.inline_bytes.store(0, Ordering::Relaxed);
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

    /// created/replaced has to be decided against the newest durable state, like
    /// `get_including_staged`: the committed index alone calls a replace a create.
    pub fn exists_including_staged(&self, key: &str) -> bool {
        let staged = {
            let pending = self.pending.lock().unwrap();
            pending.values().rev().find_map(|s| s.effect.resolve(key)).map(|e| e.is_some())
        };
        staged.unwrap_or_else(|| self.exists(key))
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

    /// Raft's no-op. Committing it commits everything below it, which is the only way an entry from
    /// a previous leader's term becomes visible in a collection nothing writes to.
    pub fn barrier(&self, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        self.append(LogEntry::Barrier { ts: Self::current_timestamp() }, term)
    }

    /// The drop as a log entry. Appending it deletes nothing: it takes effect where every other
    /// entry does, on commit, which is what makes a drop survive a leader change and reach a
    /// replica that was down for it.
    pub fn drop_marker(&self, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        self.append(LogEntry::Drop { ts: Self::current_timestamp() }, term)
    }

    /// A voting set. It is the one entry a node acts on before it commits, so the caller must
    /// re-read `latest_config` the moment this returns rather than waiting for the apply.
    pub fn configure(&self, config: Configuration, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        self.append(LogEntry::Config { config, ts: Self::current_timestamp() }, term)
    }

    pub fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }

    pub fn committed_config(&self) -> Option<Configuration> {
        self.committed_config.lock().unwrap().clone()
    }

    /// The configuration in force: the newest one in the log, committed or not. Raft §6 — a node
    /// uses the latest configuration it holds, because the one that replaces it may never commit
    /// and the quorum that would commit it is the one it names.
    pub fn latest_config(&self) -> Option<Configuration> {
        let staged = {
            let pending = self.pending.lock().unwrap();
            pending.values().rev().find_map(|s| match &s.effect {
                StagedEffect::Configure(c) => Some(c.clone()),
                _ => None,
            })
        };
        staged.or_else(|| self.committed_config())
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

    /// Returns the tail the fsync covers. Sampled under the append lock: a frame landing after the
    /// sync is page cache only, and counting it durable is what lets a crash lose a committed write.
    fn sync_wal(&self) -> io::Result<u64> {
        let wal = self.wal_writer.lock().unwrap();
        let synced_through = wal.last_appended_lsn;
        wal.current_wal.sync_data()?;
        // Raised under the append lock, ahead of the acks: a waiter reads durable_lsn to count its
        // own write, and a truncation holds the same lock so it cannot be re-raised past its cut.
        self.durable_lsn.fetch_max(synced_through, Ordering::SeqCst);
        self.db_durable_lsn.fetch_max(synced_through, Ordering::SeqCst);
        Ok(synced_through)
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
                let sync_result = tokio::task::spawn_blocking(move || col_sync.sync_wal()).await;

                let result = match sync_result {
                    Ok(Ok(_)) => Ok(()),
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

                // Kept off the ack path: lsn.meta is a boot hint and its write must not add latency.
                if count > 0 && result.is_ok() {
                    let commit_lsn = col.db_durable_lsn.load(Ordering::SeqCst);
                    let meta = LsnMeta { commit_lsn };
                    if let Err(e) = meta.save(&col.data_root) {
                        error!(target: "storage", collection = %col.name, error = %e, "Failed to persist lsn meta");
                    }
                }
            }
        });
    }

    /// At most `limit` keys from the range, in order. The index lock is held for that many key
    /// clones and no more, which is what lets a caller walk a large keyspace without holding one.
    pub fn range_page(
        &self,
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        limit: usize,
    ) -> Vec<String> {
        let index = self.index.read().unwrap();

        let start_bound = if let Some(a) = after {
            std::ops::Bound::Excluded(a)
        } else if let Some(s) = start {
            std::ops::Bound::Included(s)
        } else {
            std::ops::Bound::Unbounded
        };
        let end_bound = end.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included);

        index.range::<str, _>((start_bound, end_bound))
            .take(limit)
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn range_from(&self, after: Option<&str>, start: Option<&str>, end: Option<&str>) -> Vec<String> {
        self.range_page(after, start, end, usize::MAX)
    }

    /// Walks the range in `SCAN_CHUNK` slices, releasing the index lock between them. `visit`
    /// returns `false` to stop early.
    ///
    /// Deliberately not a snapshot: a key written between chunks may or may not be seen. Every
    /// caller either re-runs to convergence (handover planning, which does) or is already
    /// cursor-paginated (query), and the alternative is a clone of the whole keyspace held under
    /// the index lock while the caller does IO against it.
    pub fn try_for_each_key<E, F>(
        &self,
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        mut visit: F,
    ) -> Result<(), E>
    where
        F: FnMut(&str) -> Result<bool, E>,
    {
        let mut cursor: Option<String> = after.map(str::to_string);
        loop {
            let chunk = self.range_page(cursor.as_deref(), start, end, SCAN_CHUNK);
            if chunk.is_empty() {
                return Ok(());
            }
            cursor = chunk.last().cloned();
            for key in &chunk {
                if !visit(key)? {
                    return Ok(());
                }
            }
            if chunk.len() < SCAN_CHUNK {
                return Ok(());
            }
        }
    }

    pub fn for_each_key<F>(
        &self,
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        mut visit: F,
    ) where
        F: FnMut(&str) -> bool,
    {
        let _ = self.try_for_each_key::<(), _>(after, start, end, |key| Ok(visit(key)));
    }

    pub fn query_page(
        &self,
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        filter: &Option<Filter>,
        limit: usize,
    ) -> io::Result<(Vec<SortedRow>, Option<String>)> {
        // Capacity is capped independently of the caller: the vector still grows to `limit`.
        let mut items = Vec::with_capacity(limit.min(MAX_QUERY_LIMIT));
        let mut last_key: Option<String> = None;
        let mut has_more = false;

        self.try_for_each_key::<io::Error, _>(after, start, end, |key| {
            if items.len() >= limit {
                match filter {
                    None => {
                        has_more = true;
                        return Ok(false);
                    },
                    Some(f) => {
                        if let Some(val) = self.get(key)? {
                            if matches_filter(&val, f) {
                                has_more = true;
                                return Ok(false);
                            }
                        }
                    }
                }
            } else if let Some(value) = self.get(key)? {
                let matched = filter.as_ref().map_or(true, |f| matches_filter(&value, f));
                if matched {
                    items.push(SortedRow { key: key.to_string(), value });
                    last_key = Some(key.to_string());
                }
            }
            Ok(true)
        })?;

        let next_cursor = if has_more { last_key } else { None };
        Ok((items, next_cursor))
    }

    /// Top `limit` rows of the range in sort order, holding at most `2 * limit` of them at once.
    /// The scan itself is still the whole range: there is no index on the sort field, so every page
    /// re-reads it and pagination bounds the memory rather than the work.
    pub fn sorted_page(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        filter: &Option<Filter>,
        sort: &SortSpec,
        after: Option<&SortCursor>,
        limit: usize,
    ) -> io::Result<(Vec<SortedRow>, bool)> {
        let keep = limit.min(MAX_QUERY_LIMIT);
        let spill = keep.saturating_mul(2).max(1);
        let mut rows: Vec<SortedRow> = Vec::new();
        let mut matched = 0usize;

        self.try_for_each_key::<io::Error, _>(None, start, end, |key| {
            let value = match self.get(key)? {
                Some(v) => v,
                None => return Ok(true),
            };
            if let Some(f) = filter {
                if !matches_filter(&value, f) {
                    return Ok(true);
                }
            }

            let row = SortedRow { key: key.to_string(), value };
            if after.map_or(false, |c| !is_after(&row, c, sort)) {
                return Ok(true);
            }

            matched += 1;
            rows.push(row);
            if rows.len() >= spill {
                rows.sort_by(|a, b| compare_rows(a, b, sort));
                rows.truncate(keep);
            }
            Ok(true)
        })?;

        rows.sort_by(|a, b| compare_rows(a, b, sort));
        rows.truncate(keep);
        Ok((rows, matched > keep))
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
                let payload = self.read_frame_payload(entry.wal_id, entry.offset, entry.len)?;
                Ok(Self::value_from_payload(&payload))
            }
        }
    }

    /// Compaction remaps the index before it retires a WAL, so a location that fails validation or
    /// lost its file is stale by definition: re-resolve and retry. An unmoved location is a real error.
    fn read_located(&self, key: &str, at: Located) -> io::Result<Option<serde_json::Value>> {
        let mut at = at;
        for _ in 0..READ_RESOLVE_ATTEMPTS {
            let err = match self.read_frame_payload(at.0, at.1, at.2) {
                Ok(payload) => return Ok(Self::value_from_payload(&payload)),
                Err(e) => e,
            };
            let index = self.index.read().unwrap();
            match index.get(key) {
                None => return Ok(None),
                Some(entry) => match &entry.inline {
                    Some(payload) => return Ok(Self::value_from_payload(payload)),
                    None if (entry.wal_id, entry.offset, entry.len) != at => {
                        at = (entry.wal_id, entry.offset, entry.len);
                    },
                    None => return Err(err),
                },
            }
        }
        Err(io::Error::new(io::ErrorKind::InvalidData,
            format!("Key '{}' kept moving while being read", key)))
    }

    pub fn get(&self, key: &str) -> io::Result<Option<serde_json::Value>> {
        let at = {
            let index = self.index.read().unwrap();
            match index.get(key) {
                None => return Ok(None),
                Some(entry) => match &entry.inline {
                    Some(payload) => return Ok(Self::value_from_payload(payload)),
                    None => (entry.wal_id, entry.offset, entry.len),
                },
            }
        };

        self.read_located(key, at)
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
                    None => pending.push((key.clone(), (entry.wal_id, entry.offset, entry.len))),
                }
            }
        }

        for (key, at) in pending {
            if let Some(v) = self.read_located(&key, at)? {
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
    /// Staged under the append lock rather than by the caller: between an append and a separate
    /// stage the frame is on disk and in neither the index nor `pending`, and a compaction retiring
    /// its WAL would lose a write whose client is still waiting on the fsync. There is deliberately
    /// no public way to stage, so no append path can grow that window back.
    pub(super) fn stage_appended(&self, lsn: u64, term: u64, entry: &LogEntry, wal_id: u64, offset: u64, payload: &[u8]) {
        let effect = match entry {
            LogEntry::Put { key, .. } => StagedEffect::Put {
                key: key.clone(),
                entry: self.build_entry(wal_id, offset, payload),
            },
            LogEntry::Del { key, .. } => StagedEffect::Remove { key: key.clone() },
            LogEntry::Barrier { .. } => StagedEffect::Nothing,
            LogEntry::Drop { .. } => StagedEffect::Clear,
            LogEntry::Config { config, .. } => StagedEffect::Configure(config.clone()),
        };
        self.pending.lock().unwrap().insert(lsn, StagedApply { wal_id, offset, term, effect });
    }

    /// First stage marks this collection consensus-managed. Without the watermark, a restart before
    /// the first commit would apply-all and publish unacknowledged entries. Kept off the append lock:
    /// it writes a file, and holding the writer for that would stall every other writer.
    pub(super) fn record_watermark_once(&self) {
        if !self.watermark_recorded.swap(true, Ordering::SeqCst) {
            let applied_lsn = self.applied_lsn();
            if let Err(e) = (AppliedMeta { applied_lsn, dropped: self.is_dropped(), config: self.committed_config() })
                .save(&self.root_path)
            {
                error!(target: "storage", collection = %self.name, error = %e,
                    "Failed to record applied watermark");
                self.watermark_recorded.store(false, Ordering::SeqCst);
            }
        }
    }

    /// The term we recorded for a staged frame, or `None` if we hold no uncommitted frame there.
    pub fn staged_term(&self, lsn: u64) -> Option<u64> {
        self.pending.lock().unwrap().get(&lsn).map(|s| s.term)
    }

    /// Position of the oldest frame the index does not yet reflect.
    pub fn pending_floor(&self) -> Option<(u64, u64)> {
        let pending = self.pending.lock().unwrap();
        pending.values().next().map(|s| (s.wal_id, s.offset))
    }

    /// Drains in log order; staged frames can arrive out of order.
    pub fn apply_committed(&self, committed_lsn: u64) -> usize {
        // The pending -> index order keeps index visibility atomic with the snapshot watermark.
        let (ready_len, advanced) = {
            let mut pending = self.pending.lock().unwrap();
            let mut ready = std::mem::take(&mut *pending);
            *pending = ready.split_off(&(committed_lsn + 1));
            if !ready.is_empty() {
                let mut index = self.index.write().unwrap();
                for (_lsn, staged) in ready.iter() {
                    match &staged.effect {
                        StagedEffect::Put { key, entry } => {
                            self.apply_index_put(&mut index, key.clone(), entry.clone());
                            self.dropped.store(false, Ordering::SeqCst);
                        },
                        StagedEffect::Remove { key } => {
                            self.apply_index_remove(&mut index, key);
                            self.dropped.store(false, Ordering::SeqCst);
                        },
                        // A barrier is committed, never applied: being committed is its whole job.
                        StagedEffect::Nothing => {},
                        StagedEffect::Clear => {
                            self.clear_index(&mut index);
                            self.dropped.store(true, Ordering::SeqCst);
                        },
                        // Already in force since it was appended; committing it is what makes it
                        // survive compaction retiring its frame.
                        StagedEffect::Configure(config) => {
                            *self.committed_config.lock().unwrap() = Some(config.clone());
                        },
                    }
                }
            }

            let previous = self.applied_lsn.fetch_max(committed_lsn, Ordering::SeqCst);
            (ready.len(), committed_lsn > previous)
        };

        if advanced {
            if let Err(e) = (AppliedMeta {
                applied_lsn: committed_lsn,
                dropped: self.is_dropped(),
                config: self.committed_config(),
            })
                .save(&self.root_path)
            {
                error!(target: "storage", collection = %self.name, error = %e,
                    "Failed to persist applied watermark; a restart will re-stage these entries");
            }
        }
        ready_len
    }

    /// Read-modify-write must read the newest durable value; the committed one drops a racing write.
    pub fn get_including_staged(&self, key: &str) -> io::Result<Option<serde_json::Value>> {
        let staged = {
            let pending = self.pending.lock().unwrap();
            pending
                .values()
                .rev()
                .find_map(|s| s.effect.resolve(key))
                .map(|e| e.cloned())
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

    /// This log's tail as `(term, lsn)`. Sampled together: the pair must name one real frame.
    pub fn last_appended(&self) -> (u64, u64) {
        let wal = self.wal_writer.lock().unwrap();
        (wal.last_appended_term, wal.last_appended_lsn)
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
        // Waits out a compaction already past its own release check; it publishes and retires
        // inside this lock, so what it touches is still the directory it started on.
        let _rewriting = self.rewriting.lock().map_err(|_| {
            io::Error::other("collection rewrite lock is poisoned")
        })?;
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
        self.drain_read_pool(u64::MAX);
        self.pending.lock().unwrap().clear();
        self.index.write().unwrap().clear();

        Ok(tombstone)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::merge_patch;
    use crate::storage::frame::HEADER_LEN;
    use crate::storage::Database;
    use crate::test_support::{disk_put, idx, live_put, stage_delete, stage_put, temp_root, wait_for};
    use crate::util::remove_file_with_retry;
    use std::collections::HashSet;

    fn cache_cfg(max_value: u32, budget: u64) -> ReadCacheConfig {
        ReadCacheConfig { inline_max_value_bytes: max_value, inline_budget_bytes: budget }
    }

    #[tokio::test]
    async fn a_read_that_captured_its_location_before_a_compaction_still_finds_the_key() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let stale = disk_put(&col, "a", "a");
        disk_put(&col, "b", "b");
        col.enqueue_commit().await.unwrap().unwrap();

        col.compact().unwrap();
        assert!(col.retired_through.load(Ordering::SeqCst) >= stale.0,
            "compaction must retire the WAL the location names");

        let value = col.read_located("a", stale).unwrap()
            .expect("a location retired mid-read must be re-resolved, not dropped");
        assert_eq!(value["v"], "a".repeat(600));

        let _ = fs::remove_dir_all(&root);
    }

    fn inline_count(col: &Arc<Collection>) -> usize {
        col.index.read().unwrap().values().filter(|e| e.inline.is_some()).count()
    }

    #[tokio::test]
    async fn a_page_asked_for_more_rows_than_exist_allocates_for_what_it_holds() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("t").unwrap();
        for i in 0..3 {
            live_put(&col, &format!("k{}", i), i);
        }

        // Unfixed this reserves usize::MAX values before reading the first key.
        let (items, cursor) = col.query_page(None, None, None, &None, usize::MAX).unwrap();
        assert_eq!(items.len(), 3);
        assert!(cursor.is_none(), "the whole collection fits, so there is no next page");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_frame_appended_while_the_fsync_runs_is_not_reported_durable() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        // Enough unsynced bytes that the fsync holds the append lock long enough to append into.
        let filler = "x".repeat(64 * 1024);
        let mut tail = 0;
        for i in 0..192 {
            let (_, _, _, lsn) = col
                .put(format!("k{:04}", i), serde_json::json!({"v": filler}), 1).unwrap();
            tail = lsn;
        }

        let col2 = col.clone();
        let racer = std::thread::spawn(move || {
            let start = std::time::Instant::now();
            while col2.wal_writer.try_lock().is_ok() {
                if start.elapsed() > Duration::from_secs(5) {
                    return None;
                }
                std::hint::spin_loop();
            }
            // Blocked on the lock the fsync holds, so this frame cannot be part of that fsync.
            let (_, _, _, lsn) = col2.put("late".into(), serde_json::json!({"v": 1}), 1).unwrap();
            Some(lsn)
        });

        col.enqueue_commit().await.unwrap().unwrap();
        let late = racer.join().unwrap()
            .expect("the fsync never held the append lock; nothing was raced and the test proves nothing");

        assert!(late > tail, "the racing frame must be past the tail the fsync sampled");
        assert!(wait_for(Duration::from_secs(2), || col.durable_lsn() > 0).await,
            "the commit reported success but published no watermark at all");
        assert_eq!(col.durable_lsn(), tail,
            "durable_lsn reached {} but the fsync covered {}: lsn {} landed after it and is page \
             cache only, yet it counts toward the leader's own vote and the persisted commit_lsn",
            col.durable_lsn(), tail, late);

        let _ = fs::remove_dir_all(&root);
    }
    #[tokio::test]
    async fn durable_lsn_only_moves_for_frames_a_commit_actually_synced() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let first = stage_put(&col, "a", 1);
        col.enqueue_commit().await.unwrap().unwrap();
        assert_eq!(col.durable_lsn(), first);
        assert_eq!(db.durable_lsn.load(Ordering::SeqCst), first);

        let second = stage_put(&col, "b", 2);
        assert_eq!(col.durable_lsn(), first,
            "an append with no fsync behind it must not count toward the leader's own quorum vote");

        col.enqueue_commit().await.unwrap().unwrap();
        assert_eq!(col.durable_lsn(), second);

        let _ = fs::remove_dir_all(&root);
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
    async fn a_chunked_walk_matches_the_unbounded_one_across_its_own_boundary() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let total = SCAN_CHUNK + 7;
        for i in 0..total {
            live_put(&col, &format!("k{:06}", i), i as i64);
        }

        assert_eq!(col.range_page(None, None, None, 3).len(), 3, "a slice is bounded by its limit");
        assert_eq!(col.range_page(None, None, None, total * 2).len(), total,
            "and by the range when that is smaller");

        let mut walked = Vec::new();
        col.for_each_key(None, None, None, |key| {
            walked.push(key.to_string());
            true
        });
        let whole = col.range_from(None, None, None);
        // Length first: a boundary bug is off by a chunk, and comparing 1031 keys to say so
        // buries the number that identifies it.
        assert_eq!(walked.len(), whole.len(),
            "the walk must see every key exactly once across the chunk boundary");
        assert_eq!(walked, whole, "and in the same order as the unbounded form");

        let mut visits = 0;
        col.for_each_key(None, None, None, |_| {
            visits += 1;
            visits < 5
        });
        assert_eq!(visits, 5, "returning false stops the walk without draining the chunk");

        let after = walked[SCAN_CHUNK - 1].clone();
        assert_eq!(col.range_page(Some(&after), None, None, 2), walked[SCAN_CHUNK..SCAN_CHUNK + 2],
            "resuming from the last key of a chunk is exclusive, or the boundary key repeats");

        // Through the query path, whose page has to span two chunks to be answered at all.
        let (page, cursor) = col.query_page(None, None, None, &None, total - 2).unwrap();
        assert_eq!(page.len(), total - 2);
        let (rest, done) = col.query_page(cursor.as_deref(), None, None, &None, 10).unwrap();
        assert_eq!(rest.len(), 2, "the tail past the boundary must still be reachable");
        assert!(done.is_none());

        let _ = fs::remove_dir_all(&root);
    }

    /// H8: the sorted path used to materialize every value in the range and drop `cursor` on the
    /// floor. It now holds at most `2 * limit` rows and resumes from a position in the sort order.
    #[tokio::test]
    async fn sorted_page_walks_the_whole_order_in_bounded_pages() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        // Keys ascend while the sort field descends, so key order cannot stand in for sort order.
        for i in 1..=9i64 {
            let lsn = col.put(format!("k{}", i), serde_json::json!({"n": 10 - i}), 1).unwrap().3;
            col.apply_committed(lsn);
        }

        let sort = SortSpec { field: "n".to_string(), desc: false };
        let mut seen: Vec<i64> = Vec::new();
        let mut cursor: Option<SortCursor> = None;

        for _ in 0..10 {
            let (rows, more) = col.sorted_page(None, None, &None, &sort, cursor.as_ref(), 2).unwrap();
            assert!(rows.len() <= 2);
            seen.extend(rows.iter().map(|r| r.value["n"].as_i64().unwrap()));
            match (more, rows.last()) {
                (true, Some(last)) => cursor = Some(SortCursor {
                    value: last.value["n"].clone(),
                    key: last.key.clone(),
                }),
                _ => break,
            }
        }

        assert_eq!(seen, (1..=9).collect::<Vec<i64>>(), "every row once, in sort order");

        let (desc_rows, _) = col.sorted_page(
            None, None, &None, &SortSpec { field: "n".to_string(), desc: true }, None, 3).unwrap();
        assert_eq!(desc_rows.iter().map(|r| r.value["n"].as_i64().unwrap()).collect::<Vec<_>>(),
            vec![9, 8, 7]);

        let (bounded, more) = col.sorted_page(None, None, &None, &sort, None, 4).unwrap();
        assert_eq!(bounded.len(), 4);
        assert!(more, "five rows are left, so the page has to say so");

        let (all, more) = col.sorted_page(None, None, &None, &sort, None, 9).unwrap();
        assert_eq!(all.len(), 9);
        assert!(!more, "a page holding the whole collection has nothing after it");

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
        assert_eq!(p2[0].value, serde_json::json!({"i": 3}));
        assert_eq!(c2.as_deref(), Some("k4"));

        let (p3, c3) = col.query_page(c2.as_deref(), None, None, &None, 2).unwrap();
        assert_eq!(p3.len(), 1, "final short page");
        assert_eq!(p3[0].value, serde_json::json!({"i": 5}));
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

        let lsn = col.put(
            "d1".into(),
            serde_json::json!({"name": "alpha", "tags": ["x", "y"], "meta": {"v": 1, "owner": "latha"}}),
            1,
        ).unwrap().3;
        col.apply_committed(lsn);

        let mut doc = col.get("d1").unwrap().unwrap();
        merge_patch(&mut doc, &serde_json::json!({"meta": {"v": 2}, "tags": null, "status": "live"}));
        let lsn2 = col.put("d1".into(), doc, 1).unwrap().3;
        col.enqueue_commit().await.unwrap().unwrap();
        col.apply_committed(lsn2);

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

    /// M9: an uncommitted drop is durable and revocable, exactly like an uncommitted delete. It
    /// hides nothing until it commits, and a restart must leave it staged rather than apply it.
    #[tokio::test]
    async fn an_uncommitted_drop_hides_nothing_and_stays_staged_across_a_restart() {
        let root = temp_root();

        let dropped = {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            live_put(&col, "k", 1);

            let dropped = col.drop_marker(1).unwrap().3;
            col.enqueue_commit().await.unwrap().unwrap();
            assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 1})),
                "a durable drop no quorum has confirmed is not yet an answer");
            assert!(!col.is_dropped());
            col.save_index().unwrap();
            dropped
        };

        let reopened = Database::new(&root).unwrap().get_collection("c").unwrap();
        assert_eq!(reopened.pending_len(), 1, "the drop was never committed, so it is still staged");
        assert!(!reopened.is_dropped());
        assert_eq!(reopened.get("k").unwrap(), Some(serde_json::json!({"v": 1})));

        reopened.apply_committed(dropped);
        assert!(reopened.is_dropped());
        assert!(reopened.get("k").unwrap().is_none());
        assert_eq!(reopened.pending_len(), 0);

        let _ = fs::remove_dir_all(&root);
    }

    /// C17: a barrier occupies an LSN and applies nothing. It has to survive replay, drain from
    /// `pending` on commit like any other frame, and never reach the index.
    #[tokio::test]
    async fn a_barrier_commits_the_tail_below_it_without_touching_the_index() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let stranded = stage_put(&col, "k", 1);
        assert!(!col.exists("k"), "the tail is durable and unpublished, as after a promotion");

        let (_frame, _wal, _off, barrier) = col.barrier(7).unwrap();
        assert!(barrier > stranded, "a barrier is appended above the tail it publishes");
        assert_eq!(col.pending_len(), 2, "and is itself staged until it commits");

        col.apply_committed(barrier);
        assert!(col.exists("k"), "committing the barrier commits everything below it");
        assert_eq!(col.pending_len(), 0, "including the barrier, which drains and applies nothing");
        assert_eq!(col.index.read().unwrap().len(), 1, "the barrier is not a key");
        assert_eq!(col.last_appended(), (7, barrier), "the tail is the barrier's own term");

        // A reopen replays it: still one key, and the LSN it occupied is still accounted for.
        col.save_index().unwrap();
        drop(col);
        let reopened = Database::new(&root).unwrap().get_collection("c").unwrap();
        assert!(reopened.exists("k"));
        assert_eq!(reopened.index.read().unwrap().len(), 1, "replay must not invent a key for it");
        assert_eq!(reopened.pending_len(), 0);

        let _ = fs::remove_dir_all(&root);
    }

    fn config_of(names: &[&str]) -> Configuration {
        Configuration::simple(names.iter().map(|n| format!("http://{}", n)).collect())
    }

    /// A configuration entry is in force where it is appended, not where it commits, and it has to
    /// survive both a restart and a compaction that retires the frame carrying it.
    #[tokio::test]
    async fn a_configuration_is_in_force_while_staged_and_durable_once_committed() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        assert_eq!(col.latest_config(), None, "a log with no configuration entry names no voters");

        let first = config_of(&["a", "b", "c"]);
        let (_f, _w, _o, lsn) = col.configure(first.clone(), 3).unwrap();
        assert_eq!(col.latest_config(), Some(first.clone()),
            "the entry decides from the append; waiting for the commit is the split-brain window");
        assert_eq!(col.committed_config(), None);
        assert_eq!(col.index.read().unwrap().len(), 0, "a configuration is not a key");

        col.apply_committed(lsn);
        assert_eq!(col.committed_config(), Some(first.clone()));
        assert_eq!(col.pending_len(), 0);

        // The newest entry wins, staged or not.
        let joint = Configuration::joint(first.voters.clone(), config_of(&["b", "c", "d"]).voters);
        col.configure(joint.clone(), 3).unwrap();
        assert_eq!(col.latest_config(), Some(joint));
        assert_eq!(col.committed_config(), Some(first.clone()), "still the newest committed one");

        // Compaction relocates index keys and a configuration is never in one, so the frame goes.
        let tail = col.last_appended_lsn();
        col.enqueue_commit().await.unwrap().unwrap();
        col.apply_committed(tail);
        col.compact().unwrap();
        col.save_index().unwrap();
        drop(col);

        let reopened = Database::new(&root).unwrap().get_collection("c").unwrap();
        assert!(reopened.latest_config().is_some_and(|c| c.is_joint()),
            "the watermark is what carries it past a compaction that dropped its frame");

        let _ = fs::remove_dir_all(&root);
    }

    /// An uncommitted configuration comes back staged and still in force, the way Raft requires:
    /// the quorum that would commit it is the one it names.
    #[tokio::test]
    async fn an_uncommitted_configuration_replays_still_in_force() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let committed = config_of(&["a", "b", "c"]);
        let (_f, _w, _o, lsn) = col.configure(committed.clone(), 3).unwrap();
        col.apply_committed(lsn);

        let staged = config_of(&["a", "b", "c", "d"]);
        col.configure(staged.clone(), 3).unwrap();
        col.enqueue_commit().await.unwrap().unwrap();
        col.save_index().unwrap();
        drop(col);

        let reopened = Database::new(&root).unwrap().get_collection("c").unwrap();
        assert_eq!(reopened.pending_len(), 1, "it was durable and never committed");
        assert_eq!(reopened.committed_config(), Some(committed));
        assert_eq!(reopened.latest_config(), Some(staged),
            "a restart must come back deciding against the entry it last appended");

        let _ = fs::remove_dir_all(&root);
    }

    /// An uncommitted barrier comes back staged, the way an uncommitted put does.
    #[tokio::test]
    async fn an_uncommitted_barrier_replays_into_pending() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        stage_put(&col, "k", 1);
        col.barrier(7).unwrap();
        col.enqueue_commit().await.unwrap().unwrap();
        col.save_index().unwrap();
        drop(col);

        let reopened = Database::new(&root).unwrap().get_collection("c").unwrap();
        assert_eq!(reopened.pending_len(), 2, "neither frame was committed before the restart");
        assert!(!reopened.exists("k"));

        let tail = reopened.last_appended_lsn();
        reopened.apply_committed(tail);
        assert!(reopened.exists("k"), "and the barrier still publishes the tail after a replay");
        assert_eq!(reopened.pending_len(), 0);

        let _ = fs::remove_dir_all(&root);
    }

    /// M5: created/replaced is decided before the write lands, so the check has to see the staged
    /// tail the committed index does not.
    #[tokio::test]
    async fn the_existence_check_behind_created_or_replaced_sees_the_staged_tail() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let put = stage_put(&col, "k", 1);
        assert!(!col.exists("k"), "committed reads must not see an uncommitted write");
        assert!(col.exists_including_staged("k"), "unfixed this called a replace a create");

        col.apply_committed(put);
        assert!(col.exists_including_staged("k"));

        let del = stage_delete(&col, "k");
        assert!(col.exists("k"), "the delete has not committed");
        assert!(!col.exists_including_staged("k"), "a staged delete is the newest durable state");

        col.apply_committed(del);
        assert!(!col.exists_including_staged("k"));

        let _ = std::fs::remove_dir_all(&root);
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
