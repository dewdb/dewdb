//! A collection's index, key locks, group commit, and read path.

use super::frame::{Configuration, HandoverRecord, LogEntry};
use super::index::{AppliedMeta, AppliedPos, IndexEntry, IndexSnapshot, LsnMeta, ReadCacheConfig, INDEX_FILENAME};
use super::secondary::{index_values, IndexChange, IndexKey, IndexSpec, IndexStatus, Indexes, Selection, BUILD_CHUNK};
use super::wal::{WalCut, WalsState};
use crate::model::MAX_QUERY_LIMIT;
use crate::aggregate::{AggregateResult, AggregateSpec, Aggregator};
use crate::changefeed::{ChangeEvent, ChangeOp, Changefeed, ChangefeedConfig};
use crate::query::{compare_rows, is_after, matches_filter, Filter, SortCursor, SortOrder, SortedRow};
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
/// Ceiling on how long a newly defined index waits before its build starts. The apply path notifies
/// as well, so this only covers a notification the builder was mid-run for.
const INDEX_BUILD_POLL_MS: u64 = 250;
/// Keys cloned per index-lock acquisition by the chunked walk. Large enough that a scan is not
/// dominated by lock traffic, small enough that no caller pins a whole keyspace in memory.
const SCAN_CHUNK: usize = 1024;

/// A frame's `(wal_id, offset, len)` as the index records it.
type Located = (u64, u64, u32);

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitSyncPhase { Before, After }

#[cfg(test)]
type CommitSyncHook = Box<dyn FnMut(CommitSyncPhase) -> io::Result<()> + Send>;

pub struct Collection {
    pub name: String,
    pub root_path: PathBuf,
    pub data_root: PathBuf,
    pub index: RwLock<BTreeMap<String, IndexEntry>>,
    pub wal_writer: std::sync::Mutex<WalsState>,
    pub key_locks: Vec<tokio::sync::Mutex<()>>,
    pub commit_notifiers: Arc<std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<Result<(), String>>>>>,
    pub commit_signal: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    commit_sync_hook: std::sync::Mutex<Option<CommitSyncHook>>,
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
    committed_handover: std::sync::Mutex<Option<HandoverRecord>>,
    /// Committed secondary index definitions, and the postings derived from them. The definitions
    /// ride `applied.meta`; the postings are rebuilt by `start_index_task`, never persisted.
    committed_indexes: std::sync::Mutex<Vec<IndexSpec>>,
    pub indexes: RwLock<Indexes>,
    index_signal: Arc<tokio::sync::Notify>,
    /// Fed from `apply_committed` and nowhere else: a staged entry can still be truncated, and a
    /// subscriber cannot un-see an event the way a reader can re-read.
    pub changefeed: Arc<Changefeed>,
    pub compacting: AtomicBool,
    /// Prevents compaction from retiring WAL files during snapshot streaming.
    pub snapshot_boundary: std::sync::Mutex<()>,
    /// Held while this directory is being rewritten: compaction's publish-and-retire, snapshot
    /// rotation, and the handle release an install starts with. All three resolve `root_path`.
    pub rewriting: std::sync::Mutex<()>,
    pub cache: ReadCacheConfig,
    pub inline_bytes: AtomicU64,
    /// Inline bytes held by staged frames, which the committed-only index cannot count. The same
    /// budget, since it is the same memory; released when the frame commits or is truncated away.
    pub staged_inline: AtomicU64,
    // This collection's fsynced tail, distinct from the database-wide durable_lsn.
    pub durable_lsn: AtomicU64,
    // Durable but uncommitted. The index holds committed state only: a leader change can still revoke these.
    pub pending: std::sync::Mutex<BTreeMap<u64, StagedApply>>,
    pub applied_lsn: AtomicU64,
    /// A cut whose WAL truncation failed part-way. Set, nothing may be appended until the recorded
    /// cut is re-applied: the doomed bytes are still on disk and the pending buffer no longer has them.
    pub(super) truncation_fence: std::sync::Mutex<Option<WalCut>>,
    watermark_recorded: AtomicBool,
    /// Highest `applied_lsn` `applied.meta` is known to cover, and the lock that serializes writers
    /// to it. Together they collapse concurrent commits into one fsync -- see `persist_watermark`.
    watermark_saved: AtomicU64,
    watermark_write: std::sync::Mutex<()>,
    /// The position alone, so an ordinary commit does not rewrite `applied.meta`. Absent only if
    /// the file could not be opened, which falls the hot path back to the full write.
    applied_pos: std::sync::Mutex<Option<AppliedPos>>,
    /// Bumped when `dropped`, `config` or `handover` actually changes, and compared against
    /// `rich_saved` to decide whether a commit needs the full record or only its position.
    rich_gen: AtomicU64,
    rich_saved: AtomicU64,
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
    /// `indexed` is what the secondary indexes in force at *append* time asked of this document,
    /// computed here because the document is in hand and committing must cost no read. An index
    /// defined above this frame is not in it, and does not need to be: its own build walks
    /// everything committed below it, which by then includes this key.
    Put { key: String, entry: IndexEntry, indexed: Vec<(String, IndexKey)> },
    Remove { key: String },
    /// A barrier: it occupies an LSN and touches nothing.
    Nothing,
    /// A drop: every key goes, and the collection is a tombstone until something is written above it.
    Clear,
    /// A configuration. Like a barrier it touches no key; unlike one it leaves something behind.
    Configure(Configuration),
    /// A completed handover. Same shape as a configuration: no key, but state that outlives it.
    RecordHandover(HandoverRecord),
    /// A secondary index definition. Registered on commit, which is where its build starts.
    DefineIndex(IndexChange),
}

/// A committed effect the changefeed still owes an event for. Collected under the index lock,
/// where insert and update are distinguishable, and resolved into events outside it.
enum PendingChange {
    Wrote { lsn: u64, op: ChangeOp, key: String, entry: IndexEntry },
    Gone { lsn: u64, key: String },
    Dropped { lsn: u64 },
}

impl StagedEffect {
    /// What this frame leaves for `key`: `None` if it does not touch it, otherwise the entry it
    /// leaves behind, or `Some(None)` if the key is gone.
    fn resolve<'a>(&'a self, key: &str) -> Option<Option<&'a IndexEntry>> {
        match self {
            Self::Put { key: k, entry, .. } if k == key => Some(Some(entry)),
            Self::Remove { key: k } if k == key => Some(None),
            Self::Clear => Some(None),
            _ => None,
        }
    }
}

impl Collection {
    // The snapshot supplies the prefix; surviving replayed frames determine the tail.
    pub fn open(
        name: String,
        root_path: PathBuf,
        db_durable_lsn: Arc<AtomicU64>,
        db_next_lsn: Arc<AtomicU64>,
        db_last_log_term: Arc<AtomicU64>,
        cache: ReadCacheConfig,
        feed: ChangefeedConfig,
    ) -> io::Result<Self> {
        fs::create_dir_all(&root_path)?;
        let data_root = root_path.parent().unwrap_or(Path::new(".")).to_path_buf();

        // Absent means no consensus history (fresh node, or standalone engine): replay everything.
        let applied = AppliedMeta::load(&root_path)?;
        // The position file is normally ahead: `applied.meta` is rewritten only when the drop,
        // config or handover beside the position changes, and replay re-derives those three from
        // the frames above it, which compaction cannot have retired for exactly that reason.
        let applied_through = Self::recorded_watermark(&root_path)?.unwrap_or(u64::MAX);
        // Seeded from the watermark because compaction retires the drop and config frames replay
        // would otherwise find them in.
        let mut dropped = applied.as_ref().is_some_and(|m| m.dropped);
        let mut committed_config = applied.as_ref().and_then(|m| m.config.clone())
            .map(Configuration::canonicalized);
        let mut committed_indexes = applied.as_ref().map(|m| m.indexes.clone()).unwrap_or_default();
        let mut committed_handover = applied.and_then(|m| m.handover);

        let mut index = BTreeMap::new();
        let mut pending: BTreeMap<u64, StagedApply> = BTreeMap::new();
        let mut inline_used: u64 = 0;
        let mut staged_inline: u64 = 0;
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

        // Before the scan: a cut recorded but not finished left doomed frames on disk, and replay
        // would take them for a log that continues.
        WalCut::complete_recorded(&root_path)?;

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
            if lsn != 0 {
                *max_lsn = lsn;
                *max_term = term;
            }
        };

        if !snapshot_loaded {
            info!(target: "storage", collection = %name, "Replaying all WALs");
             for (id, path) in &wal_files {
                let r = Self::replay_file_from(*id, path, 0, &mut index, &mut pending, applied_through, &cache, &mut inline_used, &mut staged_inline, &mut dropped, &mut committed_config, &mut committed_handover, &mut committed_indexes)?;
                fold(r, &mut max_lsn, &mut max_term);
            }
        } else {
             for (id, path) in &wal_files {
                 if *id < snapshot_wal_id {
                     continue;
                 } else if *id == snapshot_wal_id {
                     info!(target: "storage", collection = %name, wal_id = id, offset = snapshot_offset, "Resuming WAL from snapshot offset");
                     let r = Self::replay_file_from(*id, path, snapshot_offset, &mut index, &mut pending, applied_through, &cache, &mut inline_used, &mut staged_inline, &mut dropped, &mut committed_config, &mut committed_handover, &mut committed_indexes)?;
                     fold(r, &mut max_lsn, &mut max_term);
                 } else {
                     let r = Self::replay_file_from(*id, path, 0, &mut index, &mut pending, applied_through, &cache, &mut inline_used, &mut staged_inline, &mut dropped, &mut committed_config, &mut committed_handover, &mut committed_indexes)?;
                     fold(r, &mut max_lsn, &mut max_term);
                 }
             }
        }

        // An uncommitted snapshot tail must still exist in the replayed staging buffer.
        if max_lsn > applied_through && pending.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                "Index snapshot tail has no surviving uncommitted WAL frame"));
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

        // Also where the changefeed opens: a subscriber resuming from below the committed log is
        // refused, rather than joined to a stream that starts in the middle.
        let applied_at = if applied_through == u64::MAX { boot_lsn } else { applied_through };

        // Failing to open it is not fatal: the hot path falls back to rewriting `applied.meta`,
        // which is what it did before this file existed.
        let applied_pos = AppliedPos::open(&root_path)
            .inspect_err(|e| warn!(target: "storage", collection = %name, error = %e,
                "Cannot open the applied position file; every commit will rewrite applied.meta"))
            .ok();

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
            #[cfg(test)]
            commit_sync_hook: std::sync::Mutex::new(None),
            read_pool: std::sync::Mutex::new(HashMap::new()),
            read_pool_counter: AtomicUsize::new(0),
            retired_through: AtomicU64::new(0),
            released: AtomicBool::new(false),
            dropped: AtomicBool::new(dropped),
            // Seeded as building, always: the postings are derived, so an open rebuilds them from
            // the keys this replay just published rather than trusting a file beside them.
            indexes: RwLock::new(Indexes::seed(&committed_indexes)),
            committed_indexes: std::sync::Mutex::new(committed_indexes),
            index_signal: Arc::new(tokio::sync::Notify::new()),
            changefeed: Arc::new(Changefeed::new(feed, applied_at)),
            committed_config: std::sync::Mutex::new(committed_config),
            committed_handover: std::sync::Mutex::new(committed_handover),
            compacting: AtomicBool::new(false),
            snapshot_boundary: std::sync::Mutex::new(()),
            rewriting: std::sync::Mutex::new(()),
            cache,
            inline_bytes: AtomicU64::new(inline_total),
            staged_inline: AtomicU64::new(staged_inline),
            durable_lsn: AtomicU64::new(boot_lsn),
            pending: std::sync::Mutex::new(pending),
            applied_lsn: AtomicU64::new(applied_at),
            truncation_fence: std::sync::Mutex::new(None),
            watermark_recorded: AtomicBool::new(applied_through != u64::MAX),
            watermark_saved: AtomicU64::new(if applied_through == u64::MAX { 0 } else { applied_through }),
            watermark_write: std::sync::Mutex::new(()),
            applied_pos: std::sync::Mutex::new(applied_pos),
            rich_gen: AtomicU64::new(0),
            rich_saved: AtomicU64::new(0),
            db_durable_lsn,
            db_next_lsn,
            db_last_log_term,
        })
    }

    /// A cut this collection could not finish. Its WAL still holds frames the log no longer has, and
    /// the writer's tail still names them, so nothing may append or ship the log until it completes.
    pub fn truncation_fenced(&self) -> bool {
        self.truncation_fence.lock().unwrap().is_some()
    }

    pub fn build_entry(&self, wal_id: u64, offset: u64, payload: &[u8]) -> IndexEntry {
        let len = payload.len() as u32;
        let inline = if len <= self.cache.inline_max_value_bytes
            && self.inline_resident() + len as u64 <= self.cache.inline_budget_bytes
        {
            Some(payload.to_vec().into_boxed_slice())
        } else {
            None
        };
        IndexEntry { wal_id, offset, len, inline }
    }

    /// Committed plus staged inline bytes -- the budget bounds resident memory, and a staged frame's
    /// copy is as resident as a committed one.
    pub fn inline_resident(&self) -> u64 {
        self.inline_bytes.load(Ordering::Relaxed) + self.staged_inline.load(Ordering::Relaxed)
    }

    /// `build_entry` plus the reservation the frame holds until it commits or is truncated. Without
    /// it a burst of uncommitted writes each sizes itself against the same unused budget (M11).
    fn reserve_staged_entry(&self, wal_id: u64, offset: u64, payload: &[u8]) -> IndexEntry {
        let entry = self.build_entry(wal_id, offset, payload);
        self.staged_inline.fetch_add(entry.inline_bytes(), Ordering::Relaxed);
        entry
    }

    /// Called for every staged frame that leaves `pending`, by commit or by truncation.
    pub(super) fn release_staged(&self, effect: &StagedEffect) {
        if let StagedEffect::Put { entry, .. } = effect {
            self.staged_inline.fetch_sub(entry.inline_bytes(), Ordering::Relaxed);
        }
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

    /// A completed handover. Unlike a configuration this one waits for its commit like an ordinary
    /// entry: nothing acts on it until cleanup, which runs after the ring has already flipped.
    pub fn record_handover(&self, handover: HandoverRecord, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        self.append(LogEntry::Handover { handover, ts: Self::current_timestamp() }, term)
    }

    /// A secondary index definition. Appending it is what puts it in force for later entries, so
    /// every frame above this one stages the values the new index asks for; the build that fills
    /// in everything below runs when it commits.
    pub fn define_index(&self, change: IndexChange, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        self.append(LogEntry::Index { change, ts: Self::current_timestamp() }, term)
    }

    pub fn committed_indexes(&self) -> Vec<IndexSpec> {
        self.committed_indexes.lock().unwrap().clone()
    }

    /// The definitions in force: committed, with every staged change laid over them in log order.
    /// Same rule `latest_config` follows, and it gets the same thing from it -- a truncation that
    /// drops a staged `Index` entry drops the definition with it, without a second bookkeeping path.
    pub fn active_index_specs(&self) -> Vec<IndexSpec> {
        let pending = self.pending.lock().unwrap();
        Self::overlay_index_specs(&self.committed_indexes(), &pending)
    }

    fn overlay_index_specs(
        committed: &[IndexSpec],
        pending: &BTreeMap<u64, StagedApply>,
    ) -> Vec<IndexSpec> {
        let mut specs = committed.to_vec();
        for staged in pending.values() {
            match &staged.effect {
                StagedEffect::DefineIndex(change) => change.apply_to(&mut specs),
                // An uncommitted drop takes the definitions with the documents, in the same order
                // the apply would: a create above it survives, one below it does not.
                StagedEffect::Clear => specs.clear(),
                _ => {},
            }
        }
        specs
    }

    pub fn index_status(&self) -> Vec<IndexStatus> {
        self.indexes.read().unwrap().status()
    }

    pub fn committed_handover(&self) -> Option<HandoverRecord> {
        self.committed_handover.lock().unwrap().clone()
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

    // Enqueue only after appending: the next detached batch's fsync must cover this write.
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
    pub(super) fn sync_wal(&self) -> io::Result<u64> {
        #[cfg(test)]
        self.commit_sync_checkpoint(CommitSyncPhase::Before)?;
        let wal = self.wal_writer.lock().unwrap();
        let synced_through = wal.last_appended_lsn;
        wal.current_wal.sync_data()?;
        // Raised under the append lock, ahead of the acks: a waiter reads durable_lsn to count its
        // own write, and a truncation holds the same lock so it cannot be re-raised past its cut.
        self.durable_lsn.fetch_max(synced_through, Ordering::SeqCst);
        self.db_durable_lsn.fetch_max(synced_through, Ordering::SeqCst);
        drop(wal);
        #[cfg(test)]
        self.commit_sync_checkpoint(CommitSyncPhase::After)?;
        Ok(synced_through)
    }

    #[cfg(test)]
    fn commit_sync_checkpoint(&self, phase: CommitSyncPhase) -> io::Result<()> {
        if let Some(hook) = self.commit_sync_hook.lock().unwrap().as_mut() {
            hook(phase)?;
        }
        Ok(())
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

                // Only waiters detached before the fsync may receive its result.
                let notifiers = {
                    let mut q = col.commit_notifiers.lock().unwrap();
                    std::mem::take(&mut *q)
                };

                if notifiers.is_empty() {
                    continue;
                }

                let col_sync = col.clone();
                let sync_result = tokio::task::spawn_blocking(move || col_sync.sync_wal()).await;

                let result = match sync_result {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err(format!("WAL sync failed: {}", e)),
                    Err(e) => Err(format!("Commit task panicked: {}", e)),
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
        let lower = after.or(start);
        // `BTreeMap::range` panics on a reversed pair, and a cursor can carry `after` above a
        // narrower `end` on the next request; an empty range is the answer (IB-018).
        if lower.zip(end).is_some_and(|(l, e)| l > e) {
            return Vec::new();
        }

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

    /// The keys a secondary index offers for `filter`, or `None` when no index is eligible or the
    /// candidate set is not narrow enough to be worth materialising. Public so a test can assert
    /// the plan rather than infer it from how fast the answer came back.
    pub fn index_plan(&self, filter: &Option<Filter>) -> Option<Selection> {
        let filter = filter.as_ref()?;
        let total = self.index.read().unwrap().len();
        let chosen = self.indexes.read().unwrap().select(filter, total)?;
        debug!(target: "query", collection = %self.name, index = %chosen.index, field = %chosen.field,
            candidates = chosen.keys.len(), documents = total, "Query planned on a secondary index");
        Some(chosen)
    }

    /// `try_for_each_key` over an index's candidates instead of the whole range. The candidates
    /// arrive key-ordered and deduplicated, so the bounds apply the same way and a caller's cursor
    /// resumes exactly where the full walk would have left it.
    ///
    /// The candidates are a snapshot and the walk is not, which is the same guarantee
    /// `try_for_each_key` gives: neither is a consistent view of the collection, and a key written
    /// during either may or may not be seen.
    fn scan_candidates<E, F>(
        &self,
        candidates: &[String],
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        mut visit: F,
    ) -> Result<(), E>
    where
        F: FnMut(&str) -> Result<bool, E>,
    {
        for key in candidates {
            // Ascending, so the upper bound ends the walk where the lower ones only skip.
            if end.is_some_and(|e| key.as_str() > e) {
                return Ok(());
            }
            if after.is_some_and(|a| key.as_str() <= a) || start.is_some_and(|s| key.as_str() < s) {
                continue;
            }
            if !visit(key)? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Whichever of the two walks the planner chose. Every query path goes through here so an
    /// index can only ever change which keys are *read*, never which rows are returned.
    fn scan_for<E, F>(
        &self,
        plan: Option<&Selection>,
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        visit: F,
    ) -> Result<(), E>
    where
        F: FnMut(&str) -> Result<bool, E>,
    {
        match plan {
            Some(chosen) => self.scan_candidates(&chosen.keys, after, start, end, visit),
            None => self.try_for_each_key(after, start, end, visit),
        }
    }

    /// Fills in the postings for every index the log has defined and this node has not built yet.
    /// Documents are read without the index lock and filed under it in `BUILD_CHUNK` batches; a
    /// write landing in between wins, because it holds the value that is current and this walk
    /// holds the one it replaced.
    pub fn build_pending_indexes(&self) -> io::Result<()> {
        loop {
            let Some((name, field)) = self.indexes.read().unwrap().next_building() else {
                return Ok(());
            };

            let mut cursor: Option<String> = None;
            let mut walked = true;
            loop {
                if self.released.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let chunk = self.range_page(cursor.as_deref(), None, None, BUILD_CHUNK);
                if chunk.is_empty() {
                    break;
                }
                cursor = chunk.last().cloned();

                let mut rows = Vec::with_capacity(chunk.len());
                for key in &chunk {
                    match self.get(key) {
                        Ok(Some(doc)) => rows.push((key.clone(), doc)),
                        // Deleted between the range page and the read, or the handle went away.
                        Ok(None) => {},
                        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                        Err(e) => return Err(e),
                    }
                }

                if !self.indexes.write().unwrap().absorb_build(&name, &field, &rows) {
                    walked = false;
                    break;
                }
                if chunk.len() < BUILD_CHUNK {
                    break;
                }
            }

            if walked {
                self.indexes.write().unwrap().finish_build(&name, &field);
                info!(target: "storage", collection = %self.name, index = %name, field = %field,
                    "Secondary index built");
            }
        }
    }

    /// One builder per collection. Separate from the commit task because a build is a full walk of
    /// the keyspace and the commit task's tick bounds write latency.
    pub fn start_index_task(col: Arc<Collection>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = col.index_signal.notified() => {},
                    _ = tokio::time::sleep(Duration::from_millis(INDEX_BUILD_POLL_MS)) => {},
                }
                if col.released.load(Ordering::SeqCst) {
                    return;
                }
                if !col.indexes.read().unwrap().has_building() {
                    continue;
                }
                let building = col.clone();
                match tokio::task::spawn_blocking(move || building.build_pending_indexes()).await {
                    Ok(Ok(())) => {},
                    Ok(Err(e)) => error!(target: "storage", collection = %col.name, error = %e,
                        "Secondary index build failed; queries fall back to a scan and it retries"),
                    Err(e) => error!(target: "storage", collection = %col.name, error = %e,
                        "Secondary index build panicked"),
                }
            }
        });
    }

    #[cfg(test)]
    pub fn query_page(
        &self,
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        filter: &Option<Filter>,
        limit: usize,
    ) -> io::Result<(Vec<SortedRow>, Option<String>)> {
        self.query_page_owned(after, start, end, filter, limit,
            crate::aggregate::DEFAULT_AGGREGATE_SCAN, &|_| true)
    }

    /// One page of rows in key order, and the position the next page resumes from.
    ///
    /// `budget` is documents read, not rows returned: a filter matching nothing still reads what
    /// the plan offered, and nothing bounded that before (IB-054). Spending it ends the page
    /// instead of refusing it -- the cursor is a keyspace position and a rejected candidate is
    /// decided, so the page resumes past everything it read and a client following the cursor
    /// still sees every match. A sorted page refuses instead, having to read a whole range before
    /// it can order any of it.
    pub(crate) fn query_page_owned(
        &self,
        after: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        filter: &Option<Filter>,
        limit: usize,
        budget: usize,
        owned: &dyn Fn(&str) -> bool,
    ) -> io::Result<(Vec<SortedRow>, Option<String>)> {
        // Ahead of the walk, not inside it: a cleared index yields no keys, so a scan of a released
        // handle never reaches the check inside `get` and answers an empty page.
        self.check_live()?;
        // Capacity is capped independently of the caller: the vector still grows to `limit`.
        let mut items = Vec::with_capacity(limit.min(MAX_QUERY_LIMIT));
        let mut last_key: Option<String> = None;
        let mut has_more = false;
        let mut scanned = 0usize;
        let plan = self.index_plan(filter);

        self.scan_for::<io::Error, _>(plan.as_ref(), after, start, end, |key| {
            if !owned(key) {
                return Ok(true);
            }
            // Charged after the ownership test and before the read, as an aggregation charges it.
            if scanned >= budget {
                has_more = true;
                return Ok(false);
            }
            scanned += 1;
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
                }
            }
            // Every key walked past is decided -- emitted, rejected, or gone -- so the cursor
            // passes it. Advancing on matches alone left a page stopped by its budget resuming
            // where it started.
            last_key = Some(key.to_string());
            Ok(true)
        })?;

        let next_cursor = if has_more { last_key } else { None };
        Ok((items, next_cursor))
    }

    /// Top `limit` rows within the default read budget, holding at most `2 * limit` rows.
    #[cfg(test)]
    pub fn sorted_page(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        filter: &Option<Filter>,
        sort: &SortOrder,
        after: Option<&SortCursor>,
        limit: usize,
    ) -> io::Result<(Vec<SortedRow>, bool)> {
        self.sorted_page_owned(start, end, filter, sort, after, limit, crate::aggregate::DEFAULT_AGGREGATE_SCAN, &|_| true)
    }

    pub(crate) fn sorted_page_owned(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        filter: &Option<Filter>,
        sort: &SortOrder,
        after: Option<&SortCursor>,
        limit: usize,
        budget: usize,
        owned: &dyn Fn(&str) -> bool,
    ) -> io::Result<(Vec<SortedRow>, bool)> {
        self.check_live()?;
        let keep = limit.min(MAX_QUERY_LIMIT);
        let spill = keep.saturating_mul(2).max(1);
        let mut rows: Vec<SortedRow> = Vec::new();
        let mut matched = 0usize;
        let mut scanned = 0usize;
        // The filter's index, not the sort field's: a sorted page re-sorts whatever it reads, so
        // an index can narrow what that is but cannot supply the order.
        let plan = self.index_plan(filter);

        self.scan_for::<io::Error, _>(plan.as_ref(), None, start, end, |key| {
            if !owned(key) {
                return Ok(true);
            }
            if scanned >= budget {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,
                    format!("sorted query exceeded max_docs {}; narrow the range or filter", budget)));
            }
            scanned += 1;
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

    /// Every matching document folded into `spec`, over the whole range rather than a page: an
    /// aggregate is not resumable, so a partial one merged across shards would be wrong.
    ///
    /// `budget` is documents read, not groups produced or rows matched, and it is the only thing
    /// bounding the walk -- a filter that matches nothing still reads what the plan offered. The
    /// result says how many were read and whether the walk stopped short; refusing a short answer
    /// is the caller's decision, not this one's.
    pub fn aggregate(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        filter: &Option<Filter>,
        spec: AggregateSpec,
        budget: usize,
        owned: &dyn Fn(&str) -> bool,
    ) -> io::Result<AggregateResult> {
        self.check_live()?;
        let mut agg = Aggregator::new(spec);
        let plan = self.index_plan(filter);
        let mut scanned = 0u64;
        let mut partial = false;

        self.scan_for::<io::Error, _>(plan.as_ref(), None, start, end, |key| {
            if !owned(key) {
                return Ok(true);
            }
            // Charged before the read and after the ownership test, so the flag means "a key this
            // node owns was left unread" rather than "the budget happened to land on the last key".
            if scanned as usize >= budget {
                partial = true;
                return Ok(false);
            }
            scanned += 1;
            let Some(value) = self.get(key)? else { return Ok(true) };
            if filter.as_ref().is_some_and(|f| !matches_filter(&value, f)) {
                return Ok(true);
            }
            // `InvalidInput` is what the handler answers `400`: the remedy is a narrower request.
            agg.add(&value).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            Ok(true)
        })?;

        Ok(agg.finish(scanned, partial))
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

    /// A handle whose directory a snapshot install has replaced. `release_handles` clears the index
    /// and parks the writer, and the append path already refuses; without the same check here a
    /// caller that took the handle before the install reads the cleared index and is told the
    /// collection is empty (M14). A silent wrong answer here reads as data loss somewhere else.
    fn check_live(&self) -> io::Result<()> {
        if self.released.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                "collection handle is no longer active"));
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> io::Result<Option<serde_json::Value>> {
        self.check_live()?;
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
        self.check_live()?;
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
        let committed_specs = self.committed_indexes();
        let mut pending = self.pending.lock().unwrap();
        let effect = match entry {
            // Specs read inside the lock the frame stages under, and both appends hold
            // `wal_writer`: a definition just below this frame is already in force for it.
            LogEntry::Put { key, value, .. } => StagedEffect::Put {
                key: key.clone(),
                entry: self.reserve_staged_entry(wal_id, offset, payload),
                indexed: index_values(&Self::overlay_index_specs(&committed_specs, &pending), value),
            },
            LogEntry::Del { key, .. } => StagedEffect::Remove { key: key.clone() },
            LogEntry::Barrier { .. } => StagedEffect::Nothing,
            LogEntry::Drop { .. } => StagedEffect::Clear,
            LogEntry::Config { config, .. } => StagedEffect::Configure(config.clone().canonicalized()),
            LogEntry::Handover { handover, .. } => StagedEffect::RecordHandover(handover.clone()),
            LogEntry::Index { change, .. } => StagedEffect::DefineIndex(change.clone()),
        };
        // A frame landing on an LSN we already hold displaces it; its reservation goes with it.
        if let Some(old) = pending.insert(lsn, StagedApply { wal_id, offset, term, effect }) {
            self.release_staged(&old.effect);
        }
    }

    /// No managed frame may reach the WAL before recovery can distinguish it from committed history.
    pub(super) fn record_watermark_once(&self) -> io::Result<()> {
        if self.watermark_recorded.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _one_writer = self.watermark_write.lock().unwrap();
        if self.watermark_recorded.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.persist_watermark_full(self.rich_gen.load(Ordering::SeqCst))?;
        self.watermark_recorded.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// A durable commit position covering at least `through`, before this returns. `dropped`,
    /// `config`, `handover` and the index definitions ride `applied.meta` because compaction
    /// retires the frames they came from, so a torn or unsynced copy loses state no replay can
    /// rebuild (bugs.md C27).
    ///
    /// Concurrent commits coalesce onto one fsync: the first writer covers the rest. The check is
    /// "is my position covered", never "did someone else just run" — a writer whose entry landed
    /// after the running save read the watermark is not covered by it and takes its own turn. That
    /// distinction is what H11 and H12 were both about.
    ///
    /// Two writes, not one, because the two have different costs and different rates. The position
    /// moves on every commit and goes to `AppliedPos`, which is one `sync_data` into blocks that
    /// already exist. The three fields beside it change rarely, and only that takes the full
    /// `applied.meta` rewrite -- a create, an `fsync` on a new file and a rename (bugs.md H17).
    fn persist_watermark(&self, through: u64) -> io::Result<()> {
        if self.watermark_saved.load(Ordering::SeqCst) >= through
            && self.rich_gen.load(Ordering::SeqCst) == self.rich_saved.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _one_writer = self.watermark_write.lock().unwrap();
        if self.watermark_saved.load(Ordering::SeqCst) >= through
            && self.rich_gen.load(Ordering::SeqCst) == self.rich_saved.load(Ordering::SeqCst) {
            return Ok(());
        }

        let rich = self.rich_gen.load(Ordering::SeqCst);
        if rich != self.rich_saved.load(Ordering::SeqCst) {
            return self.persist_watermark_full(rich);
        }

        // Sampled under the index lock for the same reason the full write is: `apply_committed`
        // moves the index and this position together under its write lock.
        let covered = {
            let _consistent = self.index.read().unwrap();
            self.applied_lsn()
        };
        let saved = self.applied_pos.lock().unwrap().as_mut().map(|pos| pos.save(covered));
        match saved {
            Some(Ok(())) => {
                self.watermark_saved.fetch_max(covered, Ordering::SeqCst);
                Ok(())
            },
            // No position file, or it would not take the write. The full record carries the
            // position too, so this is slower rather than a failure to be durable.
            other => {
                if let Some(Err(e)) = other {
                    warn!(target: "storage", collection = %self.name, error = %e,
                        "Position write failed; falling back to the full applied.meta");
                }
                self.persist_watermark_full(rich)
            },
        }
    }

    /// The full record. Caller holds `watermark_write`.
    fn persist_watermark_full(&self, rich: u64) -> io::Result<()> {
        let meta = {
            let _consistent = self.index.read().unwrap();
            AppliedMeta {
                applied_lsn: self.applied_lsn(),
                dropped: self.is_dropped(),
                config: self.committed_config(),
                handover: self.committed_handover(),
                indexes: self.committed_indexes(),
            }
        };
        let covered = meta.applied_lsn;
        meta.save(&self.root_path)?;
        // After the write, so a failure retries rather than being remembered as done.
        self.rich_saved.store(rich, Ordering::SeqCst);
        self.watermark_saved.fetch_max(covered, Ordering::SeqCst);
        Ok(())
    }

    /// The position a restart recovers, over both files. `open` reads it through here so a test can
    /// assert the durability invariant without depending on which of the two is carrying it.
    ///
    /// `None` only when there is no `applied.meta`: that is "never consensus-managed, replay
    /// everything", and a position with no full record beside it is a first write that tore.
    pub fn recorded_watermark(col_dir: &Path) -> io::Result<Option<u64>> {
        let meta = AppliedMeta::load(col_dir)?.map(|m| m.applied_lsn);
        let pos = AppliedPos::read(col_dir)?;
        Ok(meta.map(|m| m.max(pos.unwrap_or(0))))
    }

    /// `applied.meta` covering the position on its own, whatever the position file says.
    /// Compaction needs it: once a drop, config or handover frame is retired, that file is the only
    /// copy, and replay can only re-derive one from a frame that is still there.
    pub fn flush_watermark_full(&self) -> io::Result<()> {
        let _one_writer = self.watermark_write.lock().unwrap();
        self.persist_watermark_full(self.rich_gen.load(Ordering::SeqCst))
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
    pub fn apply_committed(&self, committed_lsn: u64) -> io::Result<usize> {
        let mut build_wanted = false;
        // Decided once for the batch and outside the locks: whether a document has to be resolved
        // is not a per-entry question, and the answer is almost always no.
        let watched = self.changefeed.active();
        let mut changes: Vec<PendingChange> = Vec::new();
        // The pending -> index order keeps index visibility atomic with the snapshot watermark.
        let ready_len = {
            let mut pending = self.pending.lock().unwrap();
            let mut ready = std::mem::take(&mut *pending);
            *pending = ready.split_off(&(committed_lsn + 1));
            if !ready.is_empty() {
                let mut index = self.index.write().unwrap();
                // Under the index write lock with the key it indexes, so no reader ever sees a
                // key published without its postings or a posting without its key.
                let mut secondary = self.indexes.write().unwrap();
                for (lsn, staged) in ready.iter() {
                    match &staged.effect {
                        // `swap` rather than `store`: only a real flip is a change `applied.meta`
                        // has to be rewritten for, and a keyed write is the common case.
                        StagedEffect::Put { key, entry, indexed } => {
                            // Judged before the insert: afterwards nothing separates a new key
                            // from one this frame replaced.
                            if watched {
                                let op = if index.contains_key(key) { ChangeOp::Update } else { ChangeOp::Insert };
                                changes.push(PendingChange::Wrote {
                                    lsn: *lsn, op, key: key.clone(), entry: entry.clone() });
                            }
                            self.apply_index_put(&mut index, key.clone(), entry.clone());
                            secondary.put(key, indexed);
                            if self.dropped.swap(false, Ordering::SeqCst) {
                                self.rich_gen.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                        StagedEffect::Remove { key } => {
                            // Deleting a key that was not there changes nothing, so there is
                            // nothing to publish; the frame still applies and still commits.
                            if watched && index.contains_key(key) {
                                changes.push(PendingChange::Gone { lsn: *lsn, key: key.clone() });
                            }
                            self.apply_index_remove(&mut index, key);
                            secondary.remove(key);
                            if self.dropped.swap(false, Ordering::SeqCst) {
                                self.rich_gen.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                        // A barrier is committed, never applied: being committed is its whole job.
                        StagedEffect::Nothing => {},
                        StagedEffect::Clear => {
                            if watched {
                                changes.push(PendingChange::Dropped { lsn: *lsn });
                            }
                            self.clear_index(&mut index);
                            // The definitions go with the documents: the collection is gone as far
                            // as a client is concerned, and a schema outliving it is invisible state.
                            secondary.clear_all();
                            if !self.committed_indexes.lock().unwrap().is_empty() {
                                self.committed_indexes.lock().unwrap().clear();
                                self.rich_gen.fetch_add(1, Ordering::SeqCst);
                            }
                            if !self.dropped.swap(true, Ordering::SeqCst) {
                                self.rich_gen.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                        // Registered here rather than at the append, unlike the definition itself:
                        // what commits is the build, and it walks the keys committed below it.
                        StagedEffect::DefineIndex(change) => {
                            secondary.apply_change(change);
                            change.apply_to(&mut self.committed_indexes.lock().unwrap());
                            self.rich_gen.fetch_add(1, Ordering::SeqCst);
                            build_wanted = true;
                        },
                        // Already in force since it was appended; committing it is what makes it
                        // survive compaction retiring its frame.
                        StagedEffect::Configure(config) => {
                            *self.committed_config.lock().unwrap() = Some(config.clone());
                            self.rich_gen.fetch_add(1, Ordering::SeqCst);
                        },
                        // Replaces rather than accumulates: one handover is in flight at a time,
                        // and an older one is inert as soon as the ring it names is not the live one.
                        StagedEffect::RecordHandover(handover) => {
                            *self.committed_handover.lock().unwrap() = Some(handover.clone());
                            self.rich_gen.fetch_add(1, Ordering::SeqCst);
                        },
                    }
                    // After the index has been charged, never before: the overlap over-reports for
                    // an instant, where the gap would let a concurrent append inline past the budget.
                    self.release_staged(&staged.effect);
                }
            }

            self.applied_lsn.fetch_max(committed_lsn, Ordering::SeqCst);
            ready.len()
        };

        // Retry against durable state even when an earlier failed save already drained the batch.
        let persisted = self.persist_watermark(committed_lsn);
        if build_wanted {
            self.index_signal.notify_one();
        }
        if watched {
            self.publish_changes(changes, committed_lsn);
        } else if self.changefeed.active() {
            // A subscriber or pin attached mid-batch, so these changes were never built.
            self.changefeed.note_gap(committed_lsn);
        }
        persisted.map(|()| ready_len)
    }

    /// Off the index lock, because a document that was not inlined is a WAL read. Resolved from the
    /// frame the entry names, so two writes to one key in one batch do not both report the later one.
    fn publish_changes(&self, changes: Vec<PendingChange>, through: u64) {
        let mut events = Vec::with_capacity(changes.len());
        let mut gap = 0;
        for change in changes {
            match change {
                PendingChange::Wrote { lsn, op, key, entry } => match self.read_entry(&entry) {
                    Ok(Some(value)) => events.push(ChangeEvent { lsn, op, key, value: Some(value) }),
                    // A gap costs the subscriber a resubscribe; an event without the document it
                    // says it carries would be a wrong answer.
                    unresolved => {
                        warn!(target: "changefeed", collection = %self.name, key = %key, lsn,
                            cause = ?unresolved.err(), "Could not resolve a changed document; \
                            breaking the feed here");
                        gap = gap.max(lsn);
                    },
                },
                PendingChange::Gone { lsn, key } =>
                    events.push(ChangeEvent { lsn, op: ChangeOp::Delete, key, value: None }),
                PendingChange::Dropped { lsn } =>
                    events.push(ChangeEvent { lsn, op: ChangeOp::Drop, key: String::new(), value: None }),
            }
        }
        self.changefeed.publish(events, through);
        // After the publish, so the floor lands on the unresolved frame and the events above it
        // that did resolve stay servable from there.
        if gap > 0 {
            self.changefeed.note_gap(gap);
        }
    }

    /// Read-modify-write must read the newest durable value; the committed one drops a racing write.
    pub fn get_including_staged(&self, key: &str) -> io::Result<Option<serde_json::Value>> {
        self.check_live()?;
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

    // Called under the append lock before truncation; a crash may replay either surviving tail.
    pub(super) fn rewind_index_tail(&self, lsn: u64, term: u64) -> io::Result<()> {
        let path = self.root_path.join(INDEX_FILENAME);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let mut snapshot = match bincode::deserialize_from::<_, IndexSnapshot>(BufReader::new(file)) {
            Ok(snapshot) => snapshot,
            // An unreadable snapshot is already ignored by recovery.
            Err(_) => return Ok(()),
        };
        if snapshot.last_lsn <= lsn {
            return Ok(());
        }
        snapshot.last_lsn = lsn;
        snapshot.last_term = term;
        let bytes = bincode::serialize(&snapshot).map_err(io::Error::other)?;
        crate::util::write_atomic(&self.root_path, INDEX_FILENAME, &bytes)
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
        self.index_signal.notify_one();
        // No local position continues into the directory that replaces this one, so subscribers
        // are ended rather than left waiting on a feed nothing will ever publish to again.
        self.changefeed.close();
        self.drain_read_pool(u64::MAX);
        {
            let mut pending = self.pending.lock().unwrap();
            pending.clear();
            self.staged_inline.store(0, Ordering::Relaxed);
        }
        self.index.write().unwrap().clear();
        // The definitions stay: the log still says they exist, and the collection reopened over
        // the installed directory rebuilds their postings from whatever it now holds.
        self.indexes.write().unwrap().clear_entries();
        // For the same reason the WAL handle moves to the tombstone: Windows refuses to replace a
        // directory anything still holds a handle in, and an install renames this one away.
        *self.applied_pos.lock().unwrap() = None;

        Ok(tombstone)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Retention;
    use crate::json::merge_patch;
    use crate::storage::frame::HEADER_LEN;
    use crate::storage::{Database, FrameHeader};
    use crate::test_support::{disk_put, idx, live_put, stage_delete, stage_put, temp_root, wait_for};
    use crate::util::remove_file_with_retry;
    use std::collections::HashSet;

    fn cache_cfg(max_value: u32, budget: u64) -> ReadCacheConfig {
        ReadCacheConfig { inline_max_value_bytes: max_value, inline_budget_bytes: budget }
    }

    fn ib004_open(path: &Path) -> Arc<Collection> {
        Arc::new(Collection::open("c".into(), path.to_path_buf(),
            Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)), ReadCacheConfig::default(),
            ChangefeedConfig::default()).unwrap())
    }

    #[tokio::test]
    async fn ib005_failed_position_save_retries_without_new_entries() {
        for recover_before_retry in [false, true] {
            let root = temp_root();
            let col = ib004_open(&root);
            let first = stage_put(&col, "a", 1);
            col.sync_wal().unwrap();
            col.apply_committed(first).unwrap();
            let next = stage_put(&col, "a", 2);
            col.sync_wal().unwrap();
            // Exercise the full-record fallback when no position handle is available.
            col.applied_pos.lock().unwrap().take();
            let blocked = root.join("applied.meta.tmp");
            fs::create_dir(&blocked).unwrap();

            for _ in 0..2 {
                assert!(col.apply_committed(next).is_err());
                assert_eq!(col.pending_len(), 0);
                assert_eq!(col.applied_lsn(), next);
                assert_eq!(Collection::recorded_watermark(&root).unwrap(), Some(first));
            }
            let col = if recover_before_retry {
                drop(col);
                let reopened = ib004_open(&root);
                assert_eq!(reopened.get("a").unwrap(), Some(serde_json::json!({"v": 1})));
                assert_eq!(reopened.pending_len(), 1);
                reopened
            } else { col };
            fs::remove_dir(&blocked).unwrap();
            col.apply_committed(next).unwrap();
            assert_eq!(Collection::recorded_watermark(&root).unwrap(), Some(next));
            drop(col);
            let reopened = ib004_open(&root);
            assert_eq!(reopened.get("a").unwrap(), Some(serde_json::json!({"v": 2})));
            assert_eq!(reopened.pending_len(), 0);
        }
    }

    #[tokio::test]
    async fn ib005_failed_drop_save_retries_and_survives_recovery() {
        for recover_before_retry in [false, true] {
            let root = temp_root();
            let col = ib004_open(&root);
            let first = stage_put(&col, "a", 1);
            col.sync_wal().unwrap();
            col.apply_committed(first).unwrap();
            let dropped = col.drop_marker(1).unwrap().3;
            col.sync_wal().unwrap();
            let blocked = root.join("applied.meta.tmp");
            fs::create_dir(&blocked).unwrap();
            for _ in 0..2 {
                assert!(col.apply_committed(dropped).is_err());
                assert!(col.is_dropped());
                assert_eq!(Collection::recorded_watermark(&root).unwrap(), Some(first));
            }
            let col = if recover_before_retry {
                drop(col);
                let reopened = ib004_open(&root);
                assert!(!reopened.is_dropped());
                assert_eq!(reopened.get("a").unwrap(), Some(serde_json::json!({"v": 1})));
                reopened
            } else { col };
            fs::remove_dir(&blocked).unwrap();
            col.apply_committed(dropped).unwrap();
            assert_eq!(Collection::recorded_watermark(&root).unwrap(), Some(dropped));
            drop(col);
            let reopened = ib004_open(&root);
            assert!(reopened.is_dropped());
            assert!(reopened.get("a").unwrap().is_none());
            assert_eq!(reopened.pending_len(), 0);
        }
    }

    fn ib004_append(col: &Collection, replica: bool, key: &str, lsn: u64) -> io::Result<()> {
        if replica {
            let frame = crate::test_support::make_frame(1, lsn, lsn - 1,
                if lsn == 1 { 0 } else { 1 }, key, 2);
            assert!(matches!(col.append_raw_frame(&frame)?,
                crate::storage::frame::ReplicaApply::Applied { lsn: applied } if applied == lsn));
        } else {
            assert_eq!(col.put(key.into(), serde_json::json!({"v": 2}), 1)?.3, lsn);
        }
        Ok(())
    }

    #[test]
    fn ib004_failed_initial_watermark_rejects_appends_and_retries_safely() {
        for replica in [false, true] {
            for legacy in [false, true] {
                for retry in [false, true] {
                    let root = temp_root();
                    let path = root.join("c");
                    fs::create_dir_all(&path).unwrap();
                    let baseline = u64::from(legacy);
                    if legacy {
                        fs::write(path.join("wal-00001.log"),
                            crate::test_support::make_frame(1, 1, 0, 0, "legacy", 1)).unwrap();
                    }
                    let col = ib004_open(&path);
                    let blocked = path.join("applied.meta.tmp");
                    fs::create_dir(&blocked).unwrap();

                    for _ in 0..2 {
                        assert!(ib004_append(&col, replica, "pending", baseline + 1).is_err());
                        assert!(!col.watermark_recorded.load(Ordering::SeqCst));
                        assert_eq!(col.last_appended_lsn(), baseline);
                        assert_eq!(col.db_next_lsn.load(Ordering::SeqCst), baseline);
                        assert_eq!(col.pending_len(), 0);
                        let wal = col.wal_writer.lock().unwrap();
                        assert_eq!(wal.current_wal_size, 0);
                        assert_eq!(wal.current_wal.metadata().unwrap().len(), 0);
                    }
                    assert_eq!(Collection::recorded_watermark(&path).unwrap(), None);
                    fs::remove_dir(&blocked).unwrap();
                    if retry {
                        ib004_append(&col, replica, "pending", baseline + 1).unwrap();
                        assert_eq!(Collection::recorded_watermark(&path).unwrap(), Some(baseline));
                    }
                    col.sync_wal().unwrap();
                    drop(col);

                    let reopened = ib004_open(&path);
                    assert_eq!(reopened.applied_lsn(), baseline);
                    assert_eq!(reopened.last_appended_lsn(), baseline + u64::from(retry));
                    assert_eq!(reopened.pending_len(), usize::from(retry));
                    assert!(reopened.get("pending").unwrap().is_none());
                    assert_eq!(reopened.get("legacy").unwrap(),
                        legacy.then(|| serde_json::json!({"v": 1})));
                    if retry {
                        reopened.apply_committed(baseline + 1).unwrap();
                        drop(reopened);
                        let committed = ib004_open(&path);
                        assert_eq!(committed.get("pending").unwrap(), Some(serde_json::json!({"v": 2})));
                        assert_eq!(committed.pending_len(), 0);
                    }
                }
            }
        }
    }

    #[test]
    fn ib004_concurrent_appends_wait_for_initial_watermark_persistence() {
        for fail in [false, true] {
            let root = temp_root();
            let path = root.join("c");
            let col = ib004_open(&path);
            if fail {
                fs::create_dir(path.join("applied.meta.tmp")).unwrap();
            }
            let saving = col.watermark_write.lock().unwrap();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let writers: Vec<_> = [false, true].into_iter().map(|replica| {
                let col = col.clone();
                let started = started_tx.clone();
                let done = done_tx.clone();
                std::thread::spawn(move || {
                    started.send(()).unwrap();
                    let result = if replica {
                        let frame = crate::test_support::make_frame(1, 100, 0, 0, "replica", 2);
                        col.append_raw_frame(&frame).map(|_| ())
                    } else {
                        col.put("local".into(), serde_json::json!({"v": 2}), 1).map(|_| ())
                    };
                    done.send(result).unwrap();
                })
            }).collect();
            for _ in 0..2 {
                started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            let premature = done_rx.recv_timeout(Duration::from_millis(100));
            let recorded_early = col.watermark_recorded.load(Ordering::SeqCst);
            let appended_early = col.last_appended_lsn();
            drop(saving);
            for writer in writers {
                writer.join().unwrap();
            }
            assert!(matches!(premature, Err(std::sync::mpsc::RecvTimeoutError::Timeout)),
                "an append bypassed the watermark writer");
            assert!(!recorded_early, "an unfinished watermark was advertised as persisted");
            assert_eq!(appended_early, 0);
            for _ in 0..2 {
                assert_eq!(done_rx.recv_timeout(Duration::from_secs(5)).unwrap().is_err(), fail);
            }
            assert_eq!(Collection::recorded_watermark(&path).unwrap(), if fail { None } else { Some(0) });
            col.sync_wal().unwrap();
            drop(col);
            let reopened = ib004_open(&path);
            assert!(reopened.get("local").unwrap().is_none());
            assert!(reopened.get("replica").unwrap().is_none());
            assert_eq!(reopened.applied_lsn(), 0);
            assert_eq!(reopened.last_appended_lsn() > 0, !fail);
        }
    }

    fn create_spec(name: &str, field: &str) -> IndexChange {
        IndexChange::Create { spec: IndexSpec { name: name.to_string(), field: field.to_string() } }
    }

    /// Defines an index the way a committed log entry does, and waits out the build. Every test
    /// below asserts against a *ready* index: a building one is deliberately not selected, so
    /// without the wait they would all pass on the fallback scan.
    async fn ready_index(col: &Arc<Collection>, name: &str, field: &str) {
        let lsn = col.define_index(create_spec(name, field), 1).unwrap().3;
        col.apply_committed(lsn).unwrap();
        let name = name.to_string();
        assert!(wait_for(Duration::from_secs(20), || {
            col.index_status().iter().any(|s| s.name == name && s.state == "ready")
        }).await, "the build did not finish");
    }

    fn put_json(col: &Arc<Collection>, key: &str, value: serde_json::Value) {
        let lsn = col.put(key.to_string(), value, 1).unwrap().3;
        col.apply_committed(lsn).unwrap();
    }

    fn filter_of(s: &str) -> Option<crate::query::Filter> {
        Some(crate::query::parse_filter(s).unwrap())
    }

    fn page_keys(col: &Arc<Collection>, filter: &Option<Filter>, limit: usize) -> Vec<String> {
        col.query_page(None, None, None, filter, limit).unwrap().0
            .into_iter().map(|r| r.key).collect()
    }

    #[tokio::test]
    async fn a_read_that_captured_its_location_before_a_compaction_still_finds_the_key() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let stale = disk_put(&col, "a", "a");
        disk_put(&col, "b", "b");
        col.enqueue_commit().await.unwrap().unwrap();

        col.compact(Retention::none()).unwrap();
        assert!(col.retired_through.load(Ordering::SeqCst) >= stale.0,
            "compaction must retire the WAL the location names");

        let value = col.read_located("a", stale).unwrap()
            .expect("a location retired mid-read must be re-resolved, not dropped");
        assert_eq!(value["v"], "a".repeat(600));
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
    }

    struct CommitGate {
        reached: tokio::sync::mpsc::UnboundedReceiver<CommitSyncPhase>,
        resume: std::sync::mpsc::Sender<io::Result<()>>,
    }

    impl CommitGate {
        fn install(col: &Collection) -> Self {
            let (notify, reached) = tokio::sync::mpsc::unbounded_channel();
            let (resume, proceed) = std::sync::mpsc::channel();
            *col.commit_sync_hook.lock().unwrap() = Some(Box::new(move |phase| {
                notify.send(phase).map_err(io::Error::other)?;
                proceed.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?
            }));
            Self { reached, resume }
        }

        async fn at(&mut self, phase: CommitSyncPhase) {
            assert_eq!(tokio::time::timeout(Duration::from_secs(5), self.reached.recv())
                .await.expect("commit worker did not reach the checkpoint"), Some(phase));
        }

        fn proceed(&self) {
            self.resume.send(Ok(())).unwrap();
        }
    }

    #[tokio::test]
    async fn ib002_late_waiters_need_their_own_sync_for_local_and_replica_appends() {
        for replica in [false, true] {
            let root = temp_root();
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            let mut gate = CommitGate::install(&col);

            stage_put(&col, "first", 1);
            let first = col.enqueue_commit();
            let covered = stage_put(&col, "second", 2);
            let second = col.enqueue_commit();
            drop(col.enqueue_commit());
            gate.at(CommitSyncPhase::Before).await;
            gate.proceed();
            gate.at(CommitSyncPhase::After).await;
            assert_eq!(col.durable_lsn(), covered);
            assert_eq!(db.durable_lsn.load(Ordering::SeqCst), covered);

            let late_lsn = if replica {
                let lsn = covered + 1;
                let frame = crate::test_support::make_frame(1, lsn, covered, 1, "late", 3);
                assert!(matches!(col.append_raw_frame(&frame).unwrap(),
                    crate::storage::ReplicaApply::Applied { .. }));
                lsn
            } else {
                stage_put(&col, "late", 3)
            };
            let mut late = col.enqueue_commit();
            gate.proceed();
            first.await.unwrap().unwrap();
            second.await.unwrap().unwrap();

            gate.at(CommitSyncPhase::Before).await;
            assert!(matches!(late.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)),
                "a waiter appended after the first sync must still be pending");
            assert_eq!(col.durable_lsn(), covered);
            gate.proceed();
            gate.at(CommitSyncPhase::After).await;
            assert_eq!(col.durable_lsn(), late_lsn);
            assert_eq!(db.durable_lsn.load(Ordering::SeqCst), late_lsn);
            gate.proceed();
            late.await.unwrap().unwrap();
            assert!(col.commit_notifiers.lock().unwrap().is_empty());
            db.release_collection("c").unwrap();
        }
    }

    #[tokio::test]
    async fn ib002_sync_failure_only_fails_the_detached_batch() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        let mut gate = CommitGate::install(&col);

        stage_put(&col, "first", 1);
        let first = col.enqueue_commit();
        let other = col.enqueue_commit();
        gate.at(CommitSyncPhase::Before).await;
        let late_lsn = stage_put(&col, "late", 2);
        let mut late = col.enqueue_commit();
        gate.resume.send(Err(io::Error::other("injected sync failure"))).unwrap();
        for waiter in [first, other] {
            assert!(waiter.await.unwrap().unwrap_err().contains("injected sync failure"));
        }

        gate.at(CommitSyncPhase::Before).await;
        assert!(matches!(late.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
        assert_eq!(col.durable_lsn(), 0);
        assert_eq!(db.durable_lsn.load(Ordering::SeqCst), 0);
        gate.proceed();
        gate.at(CommitSyncPhase::After).await;
        assert_eq!(col.durable_lsn(), late_lsn);
        gate.proceed();
        late.await.unwrap().unwrap();
        db.release_collection("c").unwrap();
    }

    #[tokio::test]
    async fn ib002_forced_flush_keeps_late_waiters_and_propagates_sync_failures() {
        for fail in [false, true] {
            let root = temp_root();
            let db = Arc::new(Database::new(&root).unwrap());
            // No background worker: each round below must be covered by the forced flush alone.
            let col = Arc::new(Collection::open("c".into(), root.join("c"),
                db.durable_lsn.clone(), db.next_lsn.clone(), db.last_log_term.clone(),
                db.cache.clone(), db.changefeed.clone()).unwrap());
            db.collections.write().unwrap().insert("c".into(), col.clone());
            let mut gate = CommitGate::install(&col);

            let covered = stage_put(&col, "first", 1);
            let first = col.enqueue_commit();
            let flushing_db = db.clone();
            let flush = tokio::task::spawn_blocking(move || flushing_db.force_commit_all());
            gate.at(CommitSyncPhase::Before).await;
            if !fail {
                gate.proceed();
                gate.at(CommitSyncPhase::After).await;
            }
            let late_lsn = stage_put(&col, "late", 2);
            let mut late = col.enqueue_commit();
            gate.resume.send(if fail { Err(io::Error::other("injected sync failure")) }
                else { Ok(()) }).unwrap();
            flush.await.unwrap();
            let result = first.await.unwrap();
            if fail {
                assert!(result.unwrap_err().contains("injected sync failure"));
            } else {
                result.unwrap();
            }
            assert!(matches!(late.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
            assert_eq!(col.durable_lsn(), if fail { 0 } else { covered });
            assert_eq!(db.durable_lsn.load(Ordering::SeqCst), col.durable_lsn());

            let flushing_db = db.clone();
            let flush = tokio::task::spawn_blocking(move || flushing_db.force_commit_all());
            gate.at(CommitSyncPhase::Before).await;
            gate.proceed();
            gate.at(CommitSyncPhase::After).await;
            assert_eq!(col.durable_lsn(), late_lsn);
            gate.proceed();
            flush.await.unwrap();
            late.await.unwrap().unwrap();
        }
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
    }

    /// IB-018: a reversed pair went straight into `BTreeMap::range`, which panics on one.
    #[tokio::test]
    async fn a_reversed_range_is_empty_rather_than_a_panic() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for k in ["a", "b", "c", "d"] {
            live_put(&col, k, 1);
        }

        assert!(col.range_from(None, Some("z"), Some("a")).is_empty(), "start above end");
        assert!(col.range_from(Some("z"), None, Some("a")).is_empty(),
            "a cursor above a narrower end on the next request");
        assert_eq!(col.range_from(None, Some("b"), Some("b")), vec!["b"],
            "an equal pair is one key, not a rejected range");
        let (rows, next) = col.query_page(None, Some("z"), Some("a"), &None, 10).unwrap();
        assert!(rows.is_empty() && next.is_none(), "and the query path returns the empty page");
    }

    /// IB-025: the walk had no request-level bound, so `MAX_AGGREGATE_GROUPS` was the only ceiling
    /// and it bounded the answer rather than the reads that produced it.
    #[tokio::test]
    async fn an_aggregation_stops_at_its_read_budget_and_says_so() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for i in 0..40 {
            live_put(&col, &format!("k{:03}", i), i as i64);
        }

        let spec = || AggregateSpec {
            group: Vec::new(),
            metrics: crate::aggregate::parse_metrics(Some("count,sum:v")).unwrap(),
        };

        let whole = col.aggregate(None, None, &None, spec(), 1000, &|_| true).unwrap();
        assert_eq!((whole.matched, whole.scanned), (40, 40));
        assert!(!whole.partial, "a budget the range fits inside leaves nothing unread");

        let capped = col.aggregate(None, None, &None, spec(), 10, &|_| true).unwrap();
        assert_eq!(capped.scanned, 10, "the bound is on documents read");
        assert_eq!(capped.matched, 10);
        assert!(capped.partial, "and the shortfall is reported rather than passed off as the total");
        assert_eq!(capped.groups[0].metrics["sum:v"].sum, Some(45.0), "0..10, not 0..40");

        // The budget is spent on reads, not on matches: a filter selecting nothing still reads.
        let filter = Some(crate::query::parse_filter(r#"{"v": {"$gte": 900}}"#).unwrap());
        let filtered = col.aggregate(None, None, &filter, spec(), 10, &|_| true).unwrap();
        assert_eq!((filtered.matched, filtered.scanned), (0, 10));
        assert!(filtered.partial);

        // Exactly the range is complete: the charge is taken before a read, so the last key is
        // read rather than being the one the flag is raised over.
        let exact = col.aggregate(None, None, &None, spec(), 40, &|_| true).unwrap();
        assert_eq!(exact.scanned, 40);
        assert!(!exact.partial);

        // A key this node does not own is not a read, so it does not spend the budget.
        let odd = col.aggregate(None, None, &None, spec(), 20,
            &|key| key[1..].parse::<u32>().unwrap() % 2 == 1).unwrap();
        assert_eq!((odd.matched, odd.scanned), (20, 20));
        assert!(!odd.partial, "twenty owned keys under a budget of twenty is the whole range");
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
            col.apply_committed(lsn).unwrap();
        }

        let sort = crate::query::parse_sort(Some("n:asc")).unwrap().unwrap();
        let mut seen: Vec<i64> = Vec::new();
        let mut cursor: Option<SortCursor> = None;

        for _ in 0..10 {
            let (rows, more) = col.sorted_page(None, None, &None, &sort, cursor.as_ref(), 2).unwrap();
            assert!(rows.len() <= 2);
            seen.extend(rows.iter().map(|r| r.value["n"].as_i64().unwrap()));
            match (more, rows.last()) {
                (true, Some(last)) => cursor = Some(SortCursor::at(
                    crate::query::sort_position(&last.value, &sort), last.key.clone())),
                _ => break,
            }
        }

        assert_eq!(seen, (1..=9).collect::<Vec<i64>>(), "every row once, in sort order");

        let (desc_rows, _) = col.sorted_page(
            None, None, &None, &crate::query::parse_sort(Some("n:desc")).unwrap().unwrap(), None, 3).unwrap();
        assert_eq!(desc_rows.iter().map(|r| r.value["n"].as_i64().unwrap()).collect::<Vec<_>>(),
            vec![9, 8, 7]);

        let (bounded, more) = col.sorted_page(None, None, &None, &sort, None, 4).unwrap();
        assert_eq!(bounded.len(), 4);
        assert!(more, "five rows are left, so the page has to say so");

        let (all, more) = col.sorted_page(None, None, &None, &sort, None, 9).unwrap();
        assert_eq!(all.len(), 9);
        assert!(!more, "a page holding the whole collection has nothing after it");
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
    }

    /// C27: `applied.meta` was a bare `fs::write` — no temp file, no rename, no fsync — and
    /// `load` read anything unparseable as "no consensus history", which means replay everything.
    /// A crash inside that write therefore published entries no client was ever promised.
    #[tokio::test]
    async fn a_damaged_watermark_is_refused_rather_than_read_as_a_fresh_collection() {
        let root = temp_root();
        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();
            col.enqueue_commit().await.unwrap().unwrap();
            assert_eq!(col.applied_lsn(), 0, "durable, but never committed");
            drop(col);
            drop(db);
        }

        let meta = root.join("c").join("applied.meta");
        assert!(meta.exists(), "staging marks the collection consensus-managed");
        assert!(!root.join("c").join("applied.meta.tmp").exists(),
            "the atomic write leaves no staging file behind");
        fs::write(&meta, "{\"applied_l").unwrap();

        let err = match Database::new(&root) {
            Err(e) => e,
            Ok(_) => panic!("a torn watermark read as absent publishes every staged entry"),
        };
        assert!(err.to_string().contains("applied.meta"), "{}", err);

        // Intact again, and the entry is still staged rather than published.
        fs::write(&meta, "{\"applied_lsn\":0}").unwrap();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        assert_eq!(col.get("k").unwrap(), None, "an uncommitted entry stays staged across a restart");
        drop(col);
        drop(db);
    }

    /// The watermark is the one file whose absence and whose damage mean opposite things.
    #[test]
    fn an_absent_watermark_is_a_fresh_collection_and_a_damaged_one_is_an_error() {
        let root = temp_root();
        fs::create_dir_all(&root).unwrap();

        assert!(AppliedMeta::load(&root).unwrap().is_none(), "absent is 'no consensus history'");

        AppliedMeta { applied_lsn: 7, dropped: true, config: None, handover: None, indexes: Vec::new() }
            .save(&root).unwrap();
        let back = AppliedMeta::load(&root).unwrap().unwrap();
        assert_eq!((back.applied_lsn, back.dropped), (7, true));

        fs::write(root.join("applied.meta"), "{\"applied_l").unwrap();
        assert!(AppliedMeta::load(&root).is_err(), "damage is not absence");
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
        col.apply_committed(lsn).unwrap();

        let mut doc = col.get("d1").unwrap().unwrap();
        merge_patch(&mut doc, &serde_json::json!({"meta": {"v": 2}, "tags": null, "status": "live"}));
        let lsn2 = col.put("d1".into(), doc, 1).unwrap().3;
        col.enqueue_commit().await.unwrap().unwrap();
        col.apply_committed(lsn2).unwrap();

        drop(col);
        drop(db);

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        assert_eq!(
            col2.get("d1").unwrap(),
            Some(serde_json::json!({"name": "alpha", "meta": {"v": 2, "owner": "latha"}, "status": "live"})),
            "the merged document survives a restart, with the patched field replaced and the sibling intact"
        );
    }

    /// H17 was an attempt to take this fsync off the ack path, and a soak run lost 41 acknowledged
    /// entries to it: `rewind_to`'s floor is this file's position, so a watermark behind a crash
    /// returns published entries as staged and a leader is then allowed to truncate them.
    ///
    /// No `await` between the apply and the read: this must hold at the instant `apply_committed`
    /// returns, not once some background task catches up.
    #[tokio::test]
    async fn the_applied_watermark_is_durable_before_a_commit_is_reported() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for v in 1..=3 {
            let lsn = stage_put(&col, "a", v);
            col.apply_committed(lsn).unwrap();
            assert_eq!(Collection::recorded_watermark(&col.root_path).unwrap().unwrap(), col.applied_lsn(),
                "an entry reported as applied must already be on disk as applied");
        }
    }

    /// H17: an ordinary commit records only its position, so a restart has to take the higher of
    /// the two files. Reading `applied.meta` alone would re-stage everything above it -- which is
    /// the truncation the entry is about, arriving by a different route.
    #[tokio::test]
    async fn a_restart_recovers_the_position_the_commits_recorded_not_the_full_records() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let first = stage_put(&col, "a", 1);
        col.apply_committed(first).unwrap();
        let mut last = first;
        for v in 2..=6 {
            last = stage_put(&col, "a", v);
            col.apply_committed(last).unwrap();
        }
        assert!(last > first);

        // The point of the split: the full record stayed where the last rich change left it.
        assert!(AppliedMeta::load(&col.root_path).unwrap().unwrap().applied_lsn < last,
            "a keyed commit must not have rewritten applied.meta, or nothing was saved");
        assert_eq!(AppliedPos::read(&col.root_path).unwrap(), Some(last));
        assert_eq!(Collection::recorded_watermark(&col.root_path).unwrap(), Some(last));

        drop(col);
        db.release_collection("c").unwrap();
        let fresh = db.get_collection("c").unwrap();
        assert_eq!(fresh.applied_lsn(), last,
            "the watermark has to come back at the position the last commit recorded");
        assert_eq!(fresh.pending_len(), 0, "nothing above the watermark, so nothing to re-stage");
    }

    /// The other half of the split: `dropped`, `config` and `handover` are never in the position
    /// file, so a commit that changes one has to take the full write. Compaction retires the frame
    /// they came from and then that file is the only copy (bugs.md C27).
    #[tokio::test]
    async fn a_committed_drop_still_takes_the_full_record() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "a", 1);
        let dropped_at = col.drop_marker(1).unwrap().3;
        col.apply_committed(dropped_at).unwrap();

        let meta = AppliedMeta::load(&col.root_path).unwrap().unwrap();
        assert!(meta.dropped, "the drop has to be in the record that survives compaction");
        assert_eq!(meta.applied_lsn, dropped_at,
            "and the record's own position has to cover the frame it describes");

        // And the reverse transition, which is what makes a keyed write after a drop expensive
        // exactly once rather than never.
        let revived = stage_put(&col, "b", 2);
        col.apply_committed(revived).unwrap();
        let after = AppliedMeta::load(&col.root_path).unwrap().unwrap();
        assert!(!after.dropped, "a put clears the tombstone, and that is a full-record change");
        assert_eq!(after.applied_lsn, revived);
    }

    /// Two slots, so a torn write costs the newer position and not the file. Simulated by
    /// corrupting whichever slot the last save used.
    #[tokio::test]
    async fn a_torn_position_slot_falls_back_to_the_other_one() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let older = stage_put(&col, "a", 1);
        col.apply_committed(older).unwrap();
        let newer = stage_put(&col, "a", 2);
        col.apply_committed(newer).unwrap();
        assert_eq!(AppliedPos::read(&col.root_path).unwrap(), Some(newer));

        drop(col);
        db.release_collection("c").unwrap();

        // Slots alternate on the sequence, not the lsn: two commits are seq 1 then 2, so the
        // newer position is in slot 0 and the older is still in slot 1. Corrupting each in turn
        // pins both the alternation and the fallback -- guessing one slot would pass either way.
        let dir = root.join("c");
        let intact = std::fs::read(dir.join("applied.pos")).unwrap();
        for (slot, survivor, which) in [(0usize, older, "newer"), (512usize, newer, "older")] {
            let mut bytes = intact.clone();
            // Inside `applied_lsn`, so the record's own CRC is what refuses it.
            bytes[slot + 13] ^= 0xFF;
            std::fs::write(dir.join("applied.pos"), &bytes).unwrap();
            assert_eq!(AppliedPos::read(&dir).unwrap(), Some(survivor),
                "a torn {} slot must leave the other one readable", which);
        }

        let mut both = intact.clone();
        both[13] ^= 0xFF;
        both[512 + 13] ^= 0xFF;
        std::fs::write(dir.join("applied.pos"), &both).unwrap();
        assert!(AppliedPos::read(&dir).is_err(),
            "with neither slot readable this is damage, and reading it as absence would retract              the position both slots were holding");
        assert!(db.get_collection("c").is_err(), "which has to fail the open, not lower the floor");

        std::fs::write(dir.join("applied.pos"), &intact).unwrap();
        assert_eq!(AppliedPos::read(&dir).unwrap(), Some(newer), "and it opens again once readable");
    }

    /// A position file that is absent, or created and never written, is "nothing recorded here"
    /// and not damage: `AppliedPos::open` creates it before the first commit writes to it.
    #[test]
    fn an_unwritten_position_file_reads_as_absent() {
        let root = temp_root();
        let dir = root.join("c");
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(AppliedPos::read(&dir).unwrap(), None, "absent");

        let _pos = AppliedPos::open(&dir).unwrap();
        assert_eq!(AppliedPos::read(&dir).unwrap(), None,
            "pre-allocated and unwritten is still nothing recorded, not a position of 0");
    }

    #[tokio::test]
    async fn ib006_corrupt_position_cannot_retract_the_recovery_watermark() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for value in 1..=2 {
            let lsn = stage_put(&col, "a", value);
            col.sync_wal().unwrap();
            col.apply_committed(lsn).unwrap();
        }
        let dir = root.join("c");
        assert_eq!(AppliedMeta::load(&dir).unwrap().unwrap().applied_lsn, 0);
        assert_eq!(Collection::recorded_watermark(&dir).unwrap(), Some(2));
        drop(col);
        db.release_collection("c").unwrap();
        let path = dir.join("applied.pos");
        let intact = fs::read(&path).unwrap();
        let mut bad_magic = intact.clone();
        bad_magic[..4].copy_from_slice(b"BAD!");
        bad_magic[512..516].copy_from_slice(b"BAD!");
        for damaged in [bad_magic, intact[..23].to_vec(), Vec::new()] {
            fs::write(&path, &damaged).unwrap();
            assert_eq!(Collection::recorded_watermark(&dir).unwrap_err().kind(),
                io::ErrorKind::InvalidData);
            assert!(db.get_collection("c").is_err());
            assert_eq!(fs::read(&path).unwrap(), damaged);
        }
        fs::write(&path, intact).unwrap();
        let recovered = db.get_collection("c").unwrap();
        assert_eq!(recovered.applied_lsn(), 2);
        assert_eq!(recovered.pending_len(), 0);
        assert_eq!(recovered.get("a").unwrap(), Some(serde_json::json!({"v": 2})));
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
    }

    #[tokio::test]
    async fn cached_reads_do_not_touch_the_wal_at_all() {
        let root = temp_root();
        let db = Database::with_config(&root, cache_cfg(512, 1 << 20), Default::default()).unwrap();
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
    }

    #[tokio::test]
    async fn values_over_the_threshold_stay_on_disk() {
        let root = temp_root();
        let db = Database::with_config(&root, cache_cfg(64, 1 << 20), Default::default()).unwrap();
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
    }

    #[tokio::test]
    async fn inline_budget_caps_memory_and_is_released_on_delete() {
        let root = temp_root();
        let db = Database::with_config(&root, cache_cfg(512, 400), Default::default()).unwrap();
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
    }

    #[tokio::test]
    async fn overwriting_a_key_refreshes_its_cached_value() {
        let root = temp_root();
        let db = Database::with_config(&root, cache_cfg(512, 1 << 20), Default::default()).unwrap();
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
    }

    fn staged_inline_sum(col: &Arc<Collection>) -> u64 {
        col.pending.lock().unwrap().values()
            .filter_map(|s| match &s.effect {
                StagedEffect::Put { entry, .. } => Some(entry.inline_bytes()),
                _ => None,
            })
            .sum()
    }

    /// IB-013: every staged value was measured against committed `inline_bytes` alone, so a burst
    /// of uncommitted writes each inlined against the same unused budget. M11 covered replay only.
    #[tokio::test]
    async fn ib013_staged_values_are_charged_to_the_inline_budget() {
        let root = temp_root();
        const BUDGET: u64 = 100;
        let db = Database::with_config(&root, cache_cfg(512, BUDGET), Default::default()).unwrap();
        let col = db.get_collection("c").unwrap();

        let mut tail = 0;
        for i in 0..10 {
            tail = stage_put(&col, &format!("k{}", i), i);
        }

        let staged = staged_inline_sum(&col);
        assert!(staged > 0, "some staged frame must be inlined, or this tests nothing");
        assert!(staged <= BUDGET,
            "staging inlined {} bytes against a {} byte budget", staged, BUDGET);
        assert_eq!(col.staged_inline.load(Ordering::Relaxed), staged,
            "the reservation must match what the staged frames actually hold");

        col.apply_committed(tail).unwrap();
        assert_eq!(col.staged_inline.load(Ordering::Relaxed), 0,
            "committing must hand every reservation back");
        assert!(col.inline_bytes.load(Ordering::Relaxed) <= BUDGET,
            "committing the batch left {} inline bytes against a {} byte budget",
            col.inline_bytes.load(Ordering::Relaxed), BUDGET);

        for i in 0..10 {
            assert_eq!(col.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": i})),
                "a value the budget refused to inline still resolves from the WAL");
        }
    }

    /// The staged reservation is only correct if it is released by every path a staged frame leaves
    /// `pending` on. Here that is an overwrite of the same key, then a delete on top of it.
    #[tokio::test]
    async fn ib013_a_staged_reservation_survives_replacement_and_release() {
        let root = temp_root();
        let db = Database::with_config(&root, cache_cfg(512, 1 << 20), Default::default()).unwrap();
        let col = db.get_collection("c").unwrap();

        stage_put(&col, "k", 1);
        let one = col.staged_inline.load(Ordering::Relaxed);
        assert!(one > 0, "a small value under an ample budget must inline");

        stage_put(&col, "k", 2);
        assert_eq!(col.staged_inline.load(Ordering::Relaxed), one * 2,
            "two staged versions of a key hold two copies, and both are resident");

        let tail = stage_delete(&col, "k");
        col.apply_committed(tail).unwrap();
        assert_eq!(col.staged_inline.load(Ordering::Relaxed), 0);
        assert_eq!(col.inline_bytes.load(Ordering::Relaxed), 0,
            "the key is gone, so neither counter may still hold its bytes");
    }

    /// bugs.md M11: replay inlined a staged frame on value size alone, so a node restarting on a
    /// large uncommitted tail came back over its configured budget, and committing those frames
    /// pushed the tracked total over it too.
    #[tokio::test]
    async fn a_restart_on_an_uncommitted_tail_stays_inside_the_inline_budget() {
        let root = temp_root();
        const BUDGET: u64 = 400;

        {
            let db = Database::with_config(&root, cache_cfg(512, BUDGET), Default::default()).unwrap();
            let col = db.get_collection("c").unwrap();
            // Durable and never committed, which is what a leader that lost its quorum leaves.
            for i in 0..40 {
                stage_put(&col, &format!("k{}", i), i);
            }
            col.enqueue_commit().await.unwrap().unwrap();
        }

        let db2 = Database::with_config(&root, cache_cfg(512, BUDGET), Default::default()).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        assert_eq!(col2.pending_len(), 40, "the tail must come back staged, not applied");

        let staged: u64 = col2.pending.lock().unwrap().values()
            .filter_map(|s| match &s.effect {
                StagedEffect::Put { entry, .. } => Some(entry.inline_bytes()),
                _ => None,
            })
            .sum();
        assert!(staged > 0, "some staged frames must still be inlined, or this tests nothing");
        assert!(staged <= BUDGET,
            "replay inlined {} bytes of staged frames against a {} byte budget", staged, BUDGET);

        col2.apply_committed(col2.last_appended_lsn()).unwrap();
        assert!(col2.inline_bytes.load(Ordering::Relaxed) <= BUDGET,
            "committing the tail pushed tracked inline memory to {} over a {} byte budget",
            col2.inline_bytes.load(Ordering::Relaxed), BUDGET);
    }

    #[tokio::test]
    async fn the_cache_survives_restart_and_compaction() {
        let root = temp_root();

        {
            let db = Database::with_config(&root, cache_cfg(512, 1 << 20), Default::default()).unwrap();
            let col = db.get_collection("c").unwrap();
            for i in 0..10 {
                live_put(&col, &format!("k{}", i), i);
            }
            col.compact(Retention::none()).unwrap();
            assert_eq!(inline_count(&col), 10, "compaction must re-populate the cache as it relocates");
            col.save_index().unwrap();
        }

        let db2 = Database::with_config(&root, cache_cfg(512, 1 << 20), Default::default()).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        assert_eq!(inline_count(&col2), 10, "a snapshot restore must come back warm, not cold");
        assert_eq!(col2.inline_bytes.load(Ordering::Relaxed),
            col2.index.read().unwrap().values().map(|e| e.inline_bytes()).sum::<u64>(),
            "the budget counter must be rebuilt to match the restored entries");
        assert_eq!(col2.get("k3").unwrap(), Some(serde_json::json!({"v": 3})));
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

        assert_eq!(col.apply_committed(first).unwrap(), 1, "only the committed prefix is published");
        assert_eq!(col.get("a").unwrap(), Some(serde_json::json!({"v": 1})));
        assert!(col.get("b").unwrap().is_none(), "the entry above the watermark stays hidden");
        assert_eq!(col.pending_len(), 1);

        assert_eq!(col.apply_committed(second).unwrap(), 1);
        assert_eq!(col.get("b").unwrap(), Some(serde_json::json!({"v": 2})));
        assert_eq!(col.pending_len(), 0);
        assert_eq!(col.applied_lsn(), second);
    }

    #[tokio::test]
    async fn staged_frames_apply_in_log_order_not_arrival_order() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        stage_put(&col, "k", 1);
        let newer = stage_put(&col, "k", 2);
        col.apply_committed(newer).unwrap();

        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 2})),
            "the later LSN must win regardless of how the staging map was walked");
    }

    #[tokio::test]
    async fn an_uncommitted_delete_does_not_hide_the_committed_value() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let put = stage_put(&col, "k", 1);
        col.apply_committed(put).unwrap();

        let del = stage_delete(&col, "k");
        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 1})),
            "the delete is durable but not committed, so the old value is still the truth");

        col.apply_committed(del).unwrap();
        assert!(col.get("k").unwrap().is_none());
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

        reopened.apply_committed(dropped).unwrap();
        assert!(reopened.is_dropped());
        assert!(reopened.get("k").unwrap().is_none());
        assert_eq!(reopened.pending_len(), 0);
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

        col.apply_committed(barrier).unwrap();
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

        col.apply_committed(lsn).unwrap();
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
        col.apply_committed(tail).unwrap();
        col.compact(Retention::none()).unwrap();
        col.save_index().unwrap();
        drop(col);

        let reopened = Database::new(&root).unwrap().get_collection("c").unwrap();
        assert!(reopened.latest_config().is_some_and(|c| c.is_joint()),
            "the watermark is what carries it past a compaction that dropped its frame");
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
        col.apply_committed(lsn).unwrap();

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
        reopened.apply_committed(tail).unwrap();
        assert!(reopened.exists("k"), "and the barrier still publishes the tail after a replay");
        assert_eq!(reopened.pending_len(), 0);
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

        col.apply_committed(put).unwrap();
        assert!(col.exists_including_staged("k"));

        let del = stage_delete(&col, "k");
        assert!(col.exists("k"), "the delete has not committed");
        assert!(!col.exists_including_staged("k"), "a staged delete is the newest durable state");

        col.apply_committed(del).unwrap();
        assert!(!col.exists_including_staged("k"));
    }

    #[tokio::test]
    async fn read_modify_write_sees_the_staged_value() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let put = stage_put(&col, "k", 1);
        col.apply_committed(put).unwrap();
        stage_put(&col, "k", 2);

        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 1})),
            "readers see committed state");
        assert_eq!(col.get_including_staged("k").unwrap(), Some(serde_json::json!({"v": 2})),
            "a patch must merge onto the newest durable value or it silently drops it");

        stage_delete(&col, "k");
        assert!(col.get_including_staged("k").unwrap().is_none(),
            "a staged delete is the newest durable state");
    }

    #[tokio::test]
    async fn compaction_waits_for_uncommitted_frames() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "a", 1);
        let staged = stage_put(&col, "b", 2);

        let err = col.compact(Retention::none()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock,
            "relocation skips entries missing from the index, so retiring their WAL would lose them");

        col.apply_committed(staged).unwrap();
        col.compact(Retention::none()).unwrap();
        assert_eq!(col.get("b").unwrap(), Some(serde_json::json!({"v": 2})));
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
            col.apply_committed(committed).unwrap();
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
        col2.apply_committed(committed + 1).unwrap();
        assert_eq!(col2.get("risky").unwrap(), Some(serde_json::json!({"v": 2})));
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
    }

    /// M14: `release_handles` clears the index, and only the append path checked `released`. A
    /// caller that took the handle before a snapshot install then read the cleared index and was
    /// told the collection was empty -- which reads as data loss somewhere else, and cost three
    /// debugging rounds looking like a consensus bug when it turned up in the C17 test.
    #[tokio::test]
    async fn a_released_handle_fails_reads_rather_than_answering_empty() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        live_put(&col, "k", 1);
        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 1})));

        let stale = col.clone();
        db.release_collection("c").unwrap();

        for (what, err) in [
            ("get", stale.get("k").err()),
            ("list_all", stale.list_all().err()),
            ("query_page", stale.query_page(None, None, None, &None, 10).err()),
            ("get_including_staged", stale.get_including_staged("k").err()),
        ] {
            let err = err.unwrap_or_else(|| panic!("{} answered for a released handle", what));
            assert_eq!(err.kind(), io::ErrorKind::NotFound, "{}", what);
        }

        drop(stale);
        drop(col);
    }

    /// The whole contract in one test: an index changes which keys a query reads, never which
    /// rows it returns. Every filter is run against a second collection that has no index, and
    /// the two answers have to agree.
    #[tokio::test]
    async fn an_indexed_query_answers_exactly_what_the_scan_would() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let indexed = db.get_collection("indexed").unwrap();
        let plain = db.get_collection("plain").unwrap();

        let docs: Vec<(String, serde_json::Value)> = (0..40).map(|i| {
            let mut doc = serde_json::json!({
                "age": i % 7,
                "tier": if i % 3 == 0 { "gold" } else { "silver" },
                "name": format!("n{:02}", i % 11),
                "tags": ["a", if i % 2 == 0 { "even" } else { "odd" }],
                "meta": {"rank": i},
            });
            // Present on some documents only, so `$exists` has both answers to give.
            if i % 5 == 0 {
                doc["bonus"] = serde_json::json!(i);
            }
            (format!("k{:03}", i), doc)
        }).collect();
        for (key, value) in &docs {
            put_json(&indexed, key, value.clone());
            put_json(&plain, key, value.clone());
        }
        ready_index(&indexed, "by_age", "age").await;
        ready_index(&indexed, "by_rank", "meta.rank").await;
        ready_index(&indexed, "by_name", "name").await;
        ready_index(&indexed, "by_bonus", "bonus").await;

        for expr in [
            r#"{"age": 3}"#,
            r#"{"age": 999}"#,
            r#"{"age": {"$in": [1, 2]}}"#,
            r#"{"age": {"$gte": 5}}"#,
            r#"{"meta.rank": {"$gt": 30, "$lte": 35}}"#,
            r#"{"meta.rank": {"$gt": 35, "$lt": 30}}"#,
            r#"{"age": 3, "tier": "gold"}"#,
            r#"{"age": {"$ne": 3}}"#,
            r#"{"tier": "gold"}"#,
            r#"{"name": {"$prefix": "n0"}}"#,
            r#"{"name": {"$gte": "n03", "$lt": "n07"}}"#,
            r#"{"name": {"$suffix": "5"}}"#,
            r#"{"name": {"$contains": "0"}}"#,
            r#"{"bonus": {"$exists": true}}"#,
            r#"{"bonus": {"$exists": false}}"#,
            r#"{"bonus": {"$type": "number"}}"#,
            r#"{"age": {"$not": {"$gte": 5}}}"#,
            r#"{"tags": {"$all": ["a", "even"]}}"#,
            r#"{"tags": {"$size": 2}}"#,
            r#"{"tags": {"$elemMatch": {"$prefix": "ev"}}}"#,
            r#"{"$or": [{"age": 1}, {"name": {"$prefix": "n1"}}]}"#,
            r#"{"age": {"$gte": 5}, "$or": [{"tier": "gold"}, {"bonus": {"$exists": true}}]}"#,
            r#"{"$nor": [{"age": 1}, {"age": 2}]}"#,
        ] {
            let filter = filter_of(expr);
            assert_eq!(page_keys(&indexed, &filter, 100), page_keys(&plain, &filter, 100),
                "the index changed the answer to {}", expr);
        }

        assert!(indexed.index_plan(&filter_of(r#"{"age": 3}"#)).is_some(), "and it was used");
        assert!(indexed.index_plan(&filter_of(r#"{"tier": "gold"}"#)).is_none(),
            "with no index on the field there is nothing to use");
        assert!(indexed.index_plan(&filter_of(r#"{"name": {"$prefix": "n0"}}"#)).is_some(),
            "a prefix is a band of the string order");
        assert!(indexed.index_plan(&filter_of(r#"{"bonus": {"$exists": true}}"#)).is_some(),
            "few enough documents hold the field for its postings to be the answer");
        assert!(indexed.index_plan(&filter_of(r#"{"$or": [{"age": 1}, {"age": 2}]}"#)).is_none(),
            "nothing inside an $or constrains every matching row");
    }

    /// IB-054: the page bounded its matches and not its reads, so a selective filter read every
    /// owned candidate in the range -- and a page that stops on a budget has to resume past the
    /// candidates it rejected, or it comes back to the same ones and never reaches a match.
    #[tokio::test]
    async fn an_unsorted_filtered_page_stops_at_its_budget_and_resumes_past_what_it_read() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for i in 0..30 {
            put_json(&col, &format!("k{:02}", i), serde_json::json!({"n": i}));
        }

        // One match, at the far end: every earlier candidate is a read and none of them is a row.
        let filter = filter_of(r#"{"n": 29}"#);
        let page = |after: Option<&str>| col
            .query_page_owned(after, None, None, &filter, 10, 4, &|_| true).unwrap();

        let (rows, next) = page(None);
        assert!(rows.is_empty(), "four reads reach no match, and the budget stops the fifth");
        assert_eq!(next.as_deref(), Some("k03"),
            "an empty page still carries the position it read to");

        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..30 {
            let (rows, next) = page(cursor.as_deref());
            seen.extend(rows.into_iter().map(|r| r.key));
            match next {
                Some(k) => cursor = Some(k),
                None => break,
            }
        }
        assert_eq!(cursor.as_deref(), Some("k27"),
            "thirty pages of four must have finished the range, not stalled inside it");
        assert_eq!(seen, vec!["k29".to_string()],
            "a bounded page is short, not lossy: following the cursor still finds every match");
    }

    /// A page resuming by key has to see the candidates in key order, or its cursor either repeats
    /// rows or steps over them.
    #[tokio::test]
    async fn paging_an_indexed_query_visits_every_row_once() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 0..30 {
            put_json(&col, &format!("k{:03}", i), serde_json::json!({"age": i % 2}));
        }
        ready_index(&col, "by_age", "age").await;

        let filter = filter_of(r#"{"age": 1}"#);
        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let (rows, next) = col.query_page(cursor.as_deref(), None, None, &filter, 4).unwrap();
            seen.extend(rows.into_iter().map(|r| r.key));
            match next {
                Some(k) => cursor = Some(k),
                None => break,
            }
        }

        let expected: Vec<String> = (0..30).filter(|i| i % 2 == 1)
            .map(|i| format!("k{:03}", i)).collect();
        assert_eq!(seen, expected, "an indexed page must resume where the scan would have");
    }

    /// Bounds are applied on top of the candidates, not instead of them.
    #[tokio::test]
    async fn key_bounds_still_apply_to_an_indexed_query() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 0..10 {
            put_json(&col, &format!("k{}", i), serde_json::json!({"age": 1}));
        }
        ready_index(&col, "by_age", "age").await;

        let filter = filter_of(r#"{"age": 1}"#);
        let (rows, _) = col.query_page(None, Some("k3"), Some("k5"), &filter, 100).unwrap();
        assert_eq!(rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
            vec!["k3".to_string(), "k4".to_string(), "k5".to_string()]);
    }

    /// Every mutation path, against the one structure that decides whether a row is a candidate.
    #[tokio::test]
    async fn a_replace_a_delete_and_a_vanished_field_all_reach_the_index() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        put_json(&col, "a", serde_json::json!({"age": 30}));
        put_json(&col, "b", serde_json::json!({"age": 30}));
        put_json(&col, "c", serde_json::json!({"age": 30}));
        ready_index(&col, "by_age", "age").await;

        put_json(&col, "a", serde_json::json!({"age": 40}));
        col.apply_committed(stage_delete(&col, "b")).unwrap();
        put_json(&col, "c", serde_json::json!({"other": 1}));

        assert_eq!(page_keys(&col, &filter_of(r#"{"age": 30}"#), 100), Vec::<String>::new(),
            "nothing holds 30 any more");
        assert_eq!(page_keys(&col, &filter_of(r#"{"age": 40}"#), 100), vec!["a".to_string()]);
        assert_eq!(col.index_status()[0].documents, 1, "and the reverse map moved with them");
    }

    /// The definitions are durable and the postings are not, so a restart has to rebuild them from
    /// the keys it replayed. A collection that came back with an empty index would answer `200`
    /// with no rows.
    #[tokio::test]
    async fn a_restart_rebuilds_the_postings_from_a_definition_that_survived() {
        let root = temp_root();
        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            for i in 0..5 {
                put_json(&col, &format!("k{}", i), serde_json::json!({"age": i}));
            }
            ready_index(&col, "by_age", "age").await;
            col.save_index().unwrap();
        }

        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        assert_eq!(col.committed_indexes().len(), 1, "the definition rides applied.meta");
        assert!(wait_for(Duration::from_secs(20), || {
            col.index_status().iter().all(|s| s.state == "ready")
        }).await, "and the postings are rebuilt rather than restored");
        assert_eq!(page_keys(&col, &filter_of(r#"{"age": 3}"#), 100), vec!["k3".to_string()]);
        assert!(col.index_plan(&filter_of(r#"{"age": 3}"#)).is_some());
    }

    /// Compaction retires the frame the definition arrived in, exactly as it does for a drop or a
    /// configuration. `applied.meta` is the only copy after that.
    #[tokio::test]
    async fn compaction_retires_the_index_frame_without_losing_the_definition() {
        let root = temp_root();
        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            ready_index(&col, "by_age", "age").await;
            for i in 0..5 {
                put_json(&col, &format!("k{}", i), serde_json::json!({"age": i}));
            }
            col.compact(Retention::none()).unwrap();
            assert!(col.read_frames_after(0, u64::MAX).unwrap().iter().all(|(_, frame)| {
                let payload = &frame[HEADER_LEN..];
                !matches!(serde_json::from_slice::<LogEntry>(payload), Ok(LogEntry::Index { .. }))
            }), "the frame has to be gone or the test proves nothing");
        }

        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        assert_eq!(col.committed_indexes(),
            vec![IndexSpec { name: "by_age".to_string(), field: "age".to_string() }]);
        assert!(wait_for(Duration::from_secs(20), || {
            col.index_status().iter().all(|s| s.state == "ready")
        }).await);
        assert_eq!(page_keys(&col, &filter_of(r#"{"age": 2}"#), 100), vec!["k2".to_string()]);
    }

    /// The definition is in force from the append, so a write appended above an uncommitted
    /// definition indexes for it -- and committing the definition must not need a second pass over
    /// keys that already staged their values.
    #[tokio::test]
    async fn a_write_above_an_uncommitted_definition_is_indexed_when_both_commit() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        put_json(&col, "below", serde_json::json!({"age": 7}));
        let define = col.define_index(create_spec("by_age", "age"), 1).unwrap().3;
        assert_eq!(col.active_index_specs().len(), 1, "in force from the append, like a Config");
        assert!(col.committed_indexes().is_empty(), "and not committed yet");
        let above = col.put("above".to_string(), serde_json::json!({"age": 7}), 1).unwrap().3;

        col.apply_committed(above.max(define)).unwrap();
        assert!(wait_for(Duration::from_secs(20), || {
            col.index_status().iter().all(|s| s.state == "ready")
        }).await);

        assert_eq!(page_keys(&col, &filter_of(r#"{"age": 7}"#), 100),
            vec!["above".to_string(), "below".to_string()],
            "the build covers what was committed below it and the staged values cover the rest");
    }

    /// A truncation drops staged entries, and a definition is one of them. Nothing separate
    /// unregisters it: `active_index_specs` reads pending, so the cut takes it.
    #[tokio::test]
    async fn a_staged_definition_a_truncation_removes_stops_being_in_force() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let seed = stage_put(&col, "k", 1);
        col.apply_committed(seed).unwrap();
        let (frame, ..) = col.define_index(create_spec("by_v", "v"), 1).unwrap();
        assert_eq!(col.active_index_specs().len(), 1);

        // A frame of a later term at the definition's position: the leader we follow does not have
        // it, so it goes, and the definition goes with it.
        let prev = FrameHeader::parse(&frame).unwrap().prev_lsn;
        let replacement = crate::test_support::make_frame(2, prev + 1, prev, 1, "k", 2);
        col.append_raw_frame(&replacement).unwrap();

        assert!(col.active_index_specs().is_empty(),
            "unfixed the definition outlives the entry that carried it");
    }

    #[tokio::test]
    async fn dropping_the_collection_drops_its_indexes() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        put_json(&col, "a", serde_json::json!({"age": 1}));
        ready_index(&col, "by_age", "age").await;

        let lsn = col.drop_marker(1).unwrap().3;
        col.apply_committed(lsn).unwrap();
        assert!(col.committed_indexes().is_empty(),
            "a schema outliving its collection is invisible state");
        assert!(col.index_status().is_empty());
        assert!(col.active_index_specs().is_empty());
    }

    /// A replica applies frames it never appended, so the whole definition path has to work from
    /// `append_raw_frame` alone.
    #[tokio::test]
    async fn a_replica_picks_up_a_definition_and_indexes_the_writes_that_follow_it() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let leader = db.get_collection("leader").unwrap();
        let replica = db.get_collection("replica").unwrap();

        let mut frames = Vec::new();
        frames.push(leader.put("a".to_string(), serde_json::json!({"age": 5}), 1).unwrap().0);
        frames.push(leader.define_index(create_spec("by_age", "age"), 1).unwrap().0);
        frames.push(leader.put("b".to_string(), serde_json::json!({"age": 5}), 1).unwrap().0);

        for frame in &frames {
            replica.append_raw_frame(frame).unwrap();
        }
        replica.apply_committed(replica.last_appended_lsn()).unwrap();

        assert_eq!(replica.committed_indexes().len(), 1);
        assert!(wait_for(Duration::from_secs(20), || {
            replica.index_status().iter().all(|s| s.state == "ready")
        }).await);
        assert_eq!(page_keys(&replica, &filter_of(r#"{"age": 5}"#), 100),
            vec!["a".to_string(), "b".to_string()]);
    }

    /// The build walks the keyspace while writes continue, and both file into the same postings.
    #[tokio::test]
    async fn writes_during_a_build_are_not_lost_to_it() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 0..600 {
            put_json(&col, &format!("k{:04}", i), serde_json::json!({"age": 1}));
        }
        let lsn = col.define_index(create_spec("by_age", "age"), 1).unwrap().3;
        col.apply_committed(lsn).unwrap();

        // Interleaved with the walk rather than before or after it: more than one `BUILD_CHUNK`
        // of keys, so the build is still running when these land.
        for i in 0..600 {
            if i % 3 == 0 {
                put_json(&col, &format!("k{:04}", i), serde_json::json!({"age": 2}));
            }
        }

        assert!(wait_for(Duration::from_secs(30), || {
            col.index_status().iter().all(|s| s.state == "ready")
        }).await);

        let twos = page_keys(&col, &filter_of(r#"{"age": 2}"#), 1000);
        let expected: Vec<String> = (0..600).filter(|i| i % 3 == 0)
            .map(|i| format!("k{:04}", i)).collect();
        assert_eq!(twos, expected, "a write during the build must beat what the build read");
        assert_eq!(page_keys(&col, &filter_of(r#"{"age": 1}"#), 1000).len(), 600 - expected.len());
    }
}
