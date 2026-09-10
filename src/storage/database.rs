//! Open collections under one data directory and the LSN counters they share.

use super::collection::Collection;
use crate::consensus::config::{is_system_collection, valid_collection_name};
use super::index::{AppliedMeta, INDEX_FILENAME, LsnMeta, ReadCacheConfig};
use crate::changefeed::ChangefeedConfig;
use crate::util::{remove_dir_with_retry, rename_with_retry, write_atomic};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{error, info, warn};

const INSTALL_MARKER: &str = ".install";

fn sync_dir(dir: &Path) {
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
}

fn install_paths(root: &Path, name: &str) -> (PathBuf, PathBuf, PathBuf) {
    (
        root.join(name),
        root.join(format!("{}.old", name)),
        root.join(format!("{}.tmp", name)),
    )
}

fn install_base_name(dir_name: &str) -> Option<&str> {
    for suffix in [".tmp", ".old"] {
        if let Some(base) = dir_name.strip_suffix(suffix) {
            if valid_collection_name(base) {
                return Some(base);
            }
        }
    }
    None
}

/// The marker, never the file set: `<name>.tmp` is also a snapshot download target, and a
/// complete-looking download is not an install. Without it, `<name>.old` is restored instead.
fn staged_install_ready(dir: &Path) -> bool {
    dir.is_dir() && dir.join(INSTALL_MARKER).is_file()
}

fn holds_collection_data(dir: &Path) -> bool {
    dir.join("applied.meta").is_file()
        || dir.join(INDEX_FILENAME).is_file()
        || fs::read_dir(dir).ok().is_some_and(|entries| {
            entries.flatten().any(|entry| {
                entry.file_name().to_str().is_some_and(|n| {
                    n.starts_with("wal-") && n.ends_with(".log")
                })
            })
        })
}

// durable_lsn is the highest LSN fsynced in any collection, not a prefix: a lower LSN in another
// collection can still be unsynced. Replication watermarks live in consensus::Progress.
pub struct Database {
    pub root_path: PathBuf,
    pub cache: ReadCacheConfig,
    pub changefeed: ChangefeedConfig,
    pub collections: RwLock<HashMap<String, Arc<Collection>>>,
    pub durable_lsn: Arc<AtomicU64>,
    pub next_lsn: Arc<AtomicU64>,
    pub last_log_term: Arc<AtomicU64>,
}

impl Database {
    /// The last check before a name becomes a path. Every name that reaches here has come off a
    /// URL or off the wire, and only the API layer's gate stands in front of the HTTP half.
    fn collection_dir(&self, name: &str) -> io::Result<PathBuf> {
        if !valid_collection_name(name) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("invalid collection name '{}'", name)));
        }
        Ok(self.root_path.join(name))
    }

    fn open_collection(&self, name: &str) -> io::Result<Arc<Collection>> {
        let col = Arc::new(Collection::open(
            name.to_string(),
            self.collection_dir(name)?,
            self.durable_lsn.clone(),
            self.next_lsn.clone(),
            self.last_log_term.clone(),
            self.cache.clone(),
            self.changefeed.clone(),
        )?);
        Collection::start_commit_task(col.clone());
        Collection::start_index_task(col.clone());
        Ok(col)
    }

    pub fn new(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        Self::with_config(path, ReadCacheConfig::default(), ChangefeedConfig::default())
    }

    pub fn with_config(
        path: impl AsRef<std::path::Path>,
        cache: ReadCacheConfig,
        changefeed: ChangefeedConfig,
    ) -> io::Result<Self> {
        let root_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&root_path)?;
        let boot_lsn = LsnMeta::load(&root_path).map(|m| m.commit_lsn).unwrap_or(0);
        if boot_lsn > 0 {
            info!(target: "db", "Restored durable LSN {} from lsn.meta", boot_lsn);
        }
        let db = Self {
            root_path,
            cache,
            changefeed,
            collections: RwLock::new(HashMap::new()),
            durable_lsn: Arc::new(AtomicU64::new(boot_lsn)),
            next_lsn: Arc::new(AtomicU64::new(boot_lsn)),
            last_log_term: Arc::new(AtomicU64::new(0)),
        };
        db.refuse_uncanonical_collections()?;
        db.reconcile_collection_installs()?;
        db.adopt_collection_tails()?;
        Ok(db)
    }

    /// A name the old rule allowed and this one cannot address: `list_collections` hides it, so its
    /// tail is never adopted and the LSN it holds is handed out a second time. Refused, not skipped.
    fn refuse_uncanonical_collections(&self) -> io::Result<()> {
        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let fname = entry.file_name();
            let Some(name) = fname.to_str() else { continue };
            if valid_collection_name(name) || install_base_name(name).is_some() {
                continue;
            }
            if holds_collection_data(&entry.path()) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, format!(
                    "collection directory '{}' holds data under a name this version cannot \
                     address; rename it to [a-z0-9._-], no trailing '.', no Windows device name",
                    name)));
            }
        }
        Ok(())
    }

    /// Crash between `live→.old` and `.tmp→live` hides both suffixes from listing.
    fn reconcile_collection_installs(&self) -> io::Result<()> {
        let mut names = HashSet::new();
        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let fname = entry.file_name();
            let Some(name) = fname.to_str() else { continue };
            if valid_collection_name(name) {
                names.insert(name.to_string());
            } else if let Some(base) = install_base_name(name) {
                names.insert(base.to_string());
            }
        }
        for name in names {
            self.recover_collection_install(&name, true)?;
        }
        Ok(())
    }

    fn recover_collection_install(&self, name: &str, boot: bool) -> io::Result<()> {
        if !valid_collection_name(name) {
            return Ok(());
        }
        let (live, old, tmp) = install_paths(&self.root_path, name);
        if live.is_dir() {
            let _ = fs::remove_file(live.join(INSTALL_MARKER));
            if boot && tmp.is_dir() {
                if let Err(e) = remove_dir_with_retry(&tmp) {
                    warn!(target: "storage", collection = %name, error = %e,
                        "Could not remove leftover snapshot staging directory");
                }
            }
            return Ok(());
        }

        if staged_install_ready(&tmp) {
            rename_with_retry(&tmp, &live)?;
            sync_dir(&self.root_path);
            let _ = fs::remove_file(live.join(INSTALL_MARKER));
            return Ok(());
        }

        if tmp.is_dir() {
            if let Err(e) = remove_dir_with_retry(&tmp) {
                warn!(target: "storage", collection = %name, error = %e,
                    "Could not remove incomplete snapshot staging directory");
            }
        }
        if old.is_dir() {
            rename_with_retry(&old, &live)?;
            sync_dir(&self.root_path);
        }
        Ok(())
    }

    fn drop_install_backup(&self, name: &str) {
        let old = self.root_path.join(format!("{}.old", name));
        if old.is_dir() {
            if let Err(e) = remove_dir_with_retry(&old) {
                warn!(target: "storage", collection = %name, error = %e,
                    "Could not remove leftover collection backup after snapshot install");
            }
        }
    }

    /// `lsn.meta` records only the committed prefix, so every collection is opened before the first
    /// LSN is handed out: an unopened tail above that watermark would be allocated a second time.
    fn adopt_collection_tails(&self) -> io::Result<()> {
        let names = self.list_collections()?;
        for name in &names {
            self.get_collection(name)?;
        }
        if !names.is_empty() {
            info!(target: "db", collections = names.len(),
                next_lsn = self.next_lsn.load(Ordering::SeqCst),
                last_log_term = self.last_log_term.load(Ordering::SeqCst),
                "Adopted collection tails at boot");
        }
        Ok(())
    }

    pub fn get_collection(&self, name: &str) -> io::Result<Arc<Collection>> {
        if !valid_collection_name(name) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("invalid collection name '{}'", name)));
        }
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

        self.recover_collection_install(name, false)?;
        let col = self.open_collection(name)?;
        collections.insert(name.to_string(), col.clone());
        self.drop_install_backup(name);
        Ok(col)
    }

    /// Opens a collection only if it is already there; `get_collection` would create the directory.
    /// `Ok(None)` is absent, but a directory that will not open stays an error.
    pub fn lookup_collection(&self, name: &str) -> io::Result<Option<Arc<Collection>>> {
        if !valid_collection_name(name) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("invalid collection name '{}'", name)));
        }
        if let Some(col) = self.collections.read().unwrap().get(name) {
            return Ok(Some(col.clone()));
        }
        let live = self.collection_dir(name)?;
        let (_, old, tmp) = install_paths(&self.root_path, name);
        if !live.is_dir() && !old.is_dir() && !staged_install_ready(&tmp) {
            return Ok(None);
        }
        self.get_collection(name).map(Some)
    }

    /// `lookup_collection` for callers with one fallback for absent and broken alike.
    pub fn existing_collection(&self, name: &str) -> Option<Arc<Collection>> {
        self.lookup_collection(name).ok().flatten()
    }

    /// Holds the collection map write lock across release, directory swap, and reopen.
    pub fn install_staged_collection(&self, name: &str, staged_path: &std::path::Path) -> io::Result<()> {
        let col_path = self.collection_dir(name)?;
        let expected_staging = self.root_path.join(format!("{}.tmp", name));
        if staged_path != expected_staging || !staged_path.is_dir() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "staged snapshot is not the expected collection temporary directory"));
        }

        let old_path = self.root_path.join(format!("{}.old", name));
        let mut collections = self.collections.write().unwrap();

        let tombstone = match collections.remove(name) {
            Some(col) => match col.release_handles() {
                Ok(path) => Some(path),
                Err(e) => {
                    // Reopen the untouched old directory so installation failure leaves it live.
                    if let Ok(reopened) = self.open_collection(name) {
                        collections.insert(name.to_string(), reopened);
                    }
                    return Err(e);
                },
            },
            None => None,
        };

        if old_path.exists() {
            if let Err(e) = remove_dir_with_retry(&old_path) {
                if col_path.is_dir() {
                    if let Ok(reopened) = self.open_collection(name) {
                        collections.insert(name.to_string(), reopened);
                    }
                }
                if let Some(path) = tombstone {
                    let _ = fs::remove_file(path);
                }
                return Err(e);
            }
        }

        let had_old = col_path.is_dir();
        if let Err(e) = write_atomic(staged_path, INSTALL_MARKER, b"1") {
            if col_path.is_dir() {
                if let Ok(reopened) = self.open_collection(name) {
                    collections.insert(name.to_string(), reopened);
                }
            }
            if let Some(path) = tombstone {
                let _ = fs::remove_file(path);
            }
            return Err(e);
        }

        if had_old {
            if let Err(e) = rename_with_retry(&col_path, &old_path) {
                if let Ok(reopened) = self.open_collection(name) {
                    collections.insert(name.to_string(), reopened);
                }
                if let Some(path) = tombstone {
                    let _ = fs::remove_file(path);
                }
                return Err(e);
            }
            sync_dir(&self.root_path);
        }

        if let Err(e) = rename_with_retry(staged_path, &col_path) {
            let restore = if had_old {
                rename_with_retry(&old_path, &col_path)
                    .and_then(|_| {
                        sync_dir(&self.root_path);
                        self.open_collection(name)
                    })
                    .map(|col| collections.insert(name.to_string(), col))
            } else { Ok(None) };
            if let Some(path) = tombstone {
                let _ = fs::remove_file(path);
            }
            return match restore {
                Ok(_) => Err(e),
                Err(restore_error) => Err(io::Error::other(format!(
                    "snapshot rename failed: {}; restoring old collection also failed: {}",
                    e, restore_error))),
            };
        }
        sync_dir(&self.root_path);

        match self.open_collection(name) {
            Ok(col) => {
                collections.insert(name.to_string(), col);
                let _ = fs::remove_file(col_path.join(INSTALL_MARKER));
                if old_path.exists() {
                    if let Err(e) = remove_dir_with_retry(&old_path) {
                        warn!(target: "replica_sync", collection = %name, error = %e,
                            "Installed snapshot but could not remove the previous collection directory");
                    }
                }
                if let Some(path) = tombstone {
                    let _ = fs::remove_file(path);
                }
                Ok(())
            },
            Err(install_error) => {
                // Restore and reopen the last known-good directory if installation fails.
                let moved_bad = rename_with_retry(&col_path, staged_path).is_ok();
                if !moved_bad && col_path.exists() {
                    let _ = remove_dir_with_retry(&col_path);
                }
                let restore = if had_old {
                    rename_with_retry(&old_path, &col_path)
                        .and_then(|_| {
                            sync_dir(&self.root_path);
                            self.open_collection(name)
                        })
                        .map(|col| collections.insert(name.to_string(), col))
                } else {
                    Ok(None)
                };
                if let Some(path) = tombstone {
                    let _ = fs::remove_file(path);
                }
                match restore {
                    Ok(_) => Err(install_error),
                    Err(restore_error) => Err(io::Error::other(format!(
                        "snapshot open failed: {}; restoring old collection also failed: {}",
                        install_error, restore_error))),
                }
            },
        }
    }

    pub fn list_collections(&self) -> io::Result<Vec<String>> {
        let mut names: HashSet<String> = self.collections.read().unwrap().keys().cloned().collect();

        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                if !valid_collection_name(name) {
                    continue;
                }
                names.insert(name.to_string());
            }
        }

        let mut out: Vec<String> = names.into_iter().collect();
        out.sort();
        Ok(out)
    }

    /// What a client sees. A tombstone is still a collection on disk -- it holds the log the drop lives
    /// in, which replication and compaction still work on -- but it is gone as far as the API goes.
    pub fn live_collections(&self) -> io::Result<Vec<String>> {
        let mut names = self.list_collections()?;
        names.retain(|name| !is_system_collection(name) && !self.is_dropped(name));
        Ok(names)
    }

    pub fn is_dropped(&self, name: &str) -> bool {
        if !valid_collection_name(name) {
            return false;
        }
        if let Some(col) = self.collections.read().unwrap().get(name) {
            return col.is_dropped();
        }
        let Some(dir) = self.collection_dir(name).ok() else { return false };
        match AppliedMeta::load(&dir) {
            Ok(meta) => meta.is_some_and(|m| m.dropped),
            // Unopenable either way, since `Collection::open` propagates the same error. Hidden
            // rather than listed, because the other reading resurrects a dropped collection.
            Err(e) => {
                warn!(target: "storage", collection = %name, error = %e,
                    "Cannot read the applied watermark; treating the collection as dropped");
                true
            },
        }
    }

    pub fn release_collection(&self, name: &str) -> io::Result<Option<PathBuf>> {
        if !valid_collection_name(name) {
            return Ok(None);
        }
        let existing = self.collections.write().unwrap().remove(name);
        match existing {
            Some(col) => Ok(Some(col.release_handles()?)),
            None => Ok(None),
        }
    }

    /// Whether anything has ever been written here, answered from directory entries: an unopened
    /// collection still holds data. Tombstones count -- this gates a change that can hide keys.
    pub fn has_any_data(&self) -> io::Result<bool> {
        for name in self.list_collections()? {
            let dir = self.root_path.join(&name);
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let is_wal = path.file_name().and_then(|s| s.to_str())
                    .is_some_and(|n| n.starts_with("wal-") && n.ends_with(".log"));
                if is_wal && fs::metadata(&path).map(|m| m.len() > 0).unwrap_or(true) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub fn recompute_durable_lsn(&self) -> io::Result<u64> {
        let mut highest = 0u64;
        for name in self.list_collections()? {
            highest = highest.max(self.get_collection(&name)?.last_appended_lsn());
        }

        let previous = self.durable_lsn.swap(highest, Ordering::SeqCst);
        if highest < previous {
            info!(target: "db", from = previous, to = highest,
                "Lowered durable LSN to match on-disk log after resync");
            let _ = LsnMeta { commit_lsn: highest }.save(&self.root_path);
        }
        Ok(highest)
    }

    pub fn force_commit_all(&self) {
        let collections = self.collections.read().unwrap();
        for (name, col) in collections.iter() {
            // Forced flushes obey the same pre-sync batch boundary as the commit worker.
            let notifiers: Vec<_> = {
                let mut q = col.commit_notifiers.lock().unwrap();
                std::mem::take(&mut *q)
            };
            let result = col.sync_wal().map(|_| ()).map_err(|e| {
                error!(target: "storage", collection = %name, error = %e, "Failed to force sync WAL on shutdown");
                format!("WAL sync failed: {}", e)
            });
            let count = notifiers.len();
            for tx in notifiers {
                let _ = tx.send(result.clone());
            }
            if result.is_ok() {
                info!(target: "storage", collection = %name, pending = count, "Flushed pending writes on shutdown");
            }
        }
        let meta = LsnMeta { commit_lsn: self.durable_lsn.load(Ordering::SeqCst) };
        if let Err(e) = meta.save(&self.root_path) {
            error!(target: "db", "Failed to persist lsn meta on shutdown: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{live_put, temp_root};

    #[tokio::test]
    async fn a_restart_does_not_reissue_lsns_an_unopened_collection_holds() {
        let root = temp_root();
        let tail_of_a;

        {
            let db = Database::new(&root).unwrap();
            let a = db.get_collection("a").unwrap();
            let b = db.get_collection("b").unwrap();
            live_put(&b, "seed", 0);
            b.enqueue_commit().await.unwrap().unwrap();

            // Never committed, so lsn.meta stops at b's write and a's tail sits above it. That is
            // the ordinary state of a log, not a corner case.
            for i in 0..5 {
                live_put(&a, &format!("k{}", i), i);
            }
            tail_of_a = a.last_appended_lsn();
            assert_eq!(tail_of_a, 6);
            assert_eq!(LsnMeta::load(&root).map(|m| m.commit_lsn), Some(1),
                "the persisted watermark must be behind a's tail or the test proves nothing");
        }

        let db = Database::new(&root).unwrap();
        assert_eq!(db.next_lsn.load(Ordering::SeqCst), tail_of_a,
            "boot must adopt every collection's tail, not just the committed watermark");
        assert_eq!(db.last_log_term.load(Ordering::SeqCst), 1,
            "the term of the highest LSN must come back with it; run_election and vote_handler \
             read it directly and a 0 makes this node look like it holds no log at all");

        // b, the collection that was written last time and is opened first here.
        let b = db.get_collection("b").unwrap();
        let (_, _, _, lsn) = b.put("x".to_string(), serde_json::json!({"v": 1}), 1).unwrap();

        assert!(lsn > tail_of_a,
            "lsn {} was handed to collection b while collection a already holds it: two frames \
             share an LSN, and the chain, the commit index and every cursor keyed on it disagree",
            lsn);
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
            assert_eq!(db.durable_lsn.load(Ordering::SeqCst), 10);
        }

        {
            let db = Database::new(&root).unwrap();
            assert_eq!(db.durable_lsn.load(Ordering::SeqCst), 10, "Commit LSN must be restored from lsn.meta");
            let col = db.get_collection("lsn_check").unwrap();
            for i in 0..5 {
                let _ = col.put(format!("b:{}", i), serde_json::json!({"i": i}), 1).unwrap();
            }
            col.enqueue_commit().await.unwrap().unwrap();
            assert_eq!(db.durable_lsn.load(Ordering::SeqCst), 15, "LSN must continue from restored value, not reset to zero");
        }
    }

    #[tokio::test]
    async fn recompute_durable_lsn_follows_the_log_back_down() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let a = db.get_collection("a").unwrap();
        for i in 0..3 {
            live_put(&a, &format!("k{}", i), i);
        }
        let b = db.get_collection("b").unwrap();
        live_put(&b, "k", 1);
        a.enqueue_commit().await.unwrap().unwrap();
        b.enqueue_commit().await.unwrap().unwrap();

        let real_end = db.durable_lsn.load(Ordering::SeqCst);
        assert_eq!(real_end, 4);

        db.durable_lsn.store(999, Ordering::SeqCst);
        assert_eq!(db.recompute_durable_lsn().unwrap(), real_end);
        assert_eq!(db.durable_lsn.load(Ordering::SeqCst), real_end,
            "an inflated watermark must come back down to the real end of the log");
        assert_eq!(LsnMeta::load(&root).map(|m| m.commit_lsn), Some(real_end),
            "the persisted watermark must be corrected too, or a restart re-inflates it");

        assert_eq!(db.recompute_durable_lsn().unwrap(), real_end, "recomputing is idempotent");
    }

    #[tokio::test]
    async fn last_appended_reports_this_collections_own_tail() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let users = db.get_collection("users").unwrap();
        let orders = db.get_collection("orders").unwrap();
        for i in 0..3 {
            live_put(&users, &format!("k{}", i), i);
        }
        live_put(&orders, "k", 9);

        assert_eq!(users.last_appended(), (1, 3));
        assert_eq!(orders.last_appended(), (1, 4),
            "LSNs are database-wide, so a collection's tail is not its own write count");

        live_put(&users, "k9", 9);

        assert_eq!(users.last_appended(), (1, 5));
        assert_eq!(orders.last_appended(), (1, 4),
            "a write to one collection must not move another's tail; election freshness reads these");
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
    }

    /// M9: a drop that only unlinks files is invisible to every node but the one that ran it. The
    /// collection survives its own drop as a tombstone holding the log entry.
    #[tokio::test]
    async fn a_committed_drop_empties_the_collection_and_hides_it() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let col = db.get_collection("users").unwrap();
        live_put(&col, "k", 1);
        assert_eq!(db.live_collections().unwrap(), vec!["users".to_string()]);

        let lsn = col.drop_marker(1).unwrap().3;
        assert!(!db.is_dropped("users"), "appending the drop must not apply it");
        assert_eq!(col.get("k").unwrap(), Some(serde_json::json!({"v": 1})));

        col.apply_committed(lsn).unwrap();

        assert!(col.is_dropped());
        assert!(col.get("k").unwrap().is_none());
        assert!(db.live_collections().unwrap().is_empty(), "a tombstone is gone to a client");
        assert_eq!(db.list_collections().unwrap(), vec!["users".to_string()],
            "but still present to replication and compaction, which own its log");
    }

    #[tokio::test]
    async fn reopening_after_drop_starts_empty() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let col = db.get_collection("users").unwrap();
        live_put(&col, "k", 1);
        let lsn = col.drop_marker(1).unwrap().3;
        col.enqueue_commit().await.unwrap().unwrap();
        col.apply_committed(lsn).unwrap();
        drop(col);
        db.release_collection("users").unwrap();

        let fresh = db.get_collection("users").unwrap();
        assert!(fresh.is_dropped(), "the drop must survive a reopen");
        assert!(fresh.index.read().unwrap().is_empty(), "dropped data must not resurrect on reopen");
        assert!(fresh.get("k").unwrap().is_none());
    }

    #[tokio::test]
    async fn writing_above_a_drop_brings_the_collection_back() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let col = db.get_collection("users").unwrap();
        live_put(&col, "old", 1);
        let dropped = col.drop_marker(1).unwrap().3;
        col.apply_committed(dropped).unwrap();

        live_put(&col, "new", 2);

        assert!(!col.is_dropped());
        assert_eq!(db.live_collections().unwrap(), vec!["users".to_string()]);
        assert!(col.get("old").unwrap().is_none(), "the drop still took the keys below it");
        assert_eq!(col.get("new").unwrap(), Some(serde_json::json!({"v": 2})));
    }

    #[tokio::test]
    async fn a_snapshot_that_cannot_open_rolls_back_to_the_live_collection() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let original = db.get_collection("users").unwrap();
        live_put(&original, "safe", 7);
        original.enqueue_commit().await.unwrap().unwrap();

        let staged = root.join("users.tmp");
        fs::create_dir_all(staged.join("wal-00001.log")).unwrap();

        assert!(db.install_staged_collection("users", &staged).is_err(),
            "a directory masquerading as a WAL must make the staged collection fail to open");
        let restored = db.get_collection("users").unwrap();
        assert_eq!(restored.get("safe").unwrap(), Some(serde_json::json!({"v": 7})),
            "installation failure must reopen the previous directory and preserve its data");
        assert!(root.join("users").is_dir());
        assert!(staged.is_dir(), "the rejected snapshot goes back to staging for cleanup");
        assert!(!root.join("users.old").exists());
    }

    async fn committed_collection(root: &std::path::Path, name: &str, key: &str, v: i64) {
        let db = Database::new(root).unwrap();
        let col = db.get_collection(name).unwrap();
        live_put(&col, key, v);
        col.enqueue_commit().await.unwrap().unwrap();
        drop(col);
        for existing in db.list_collections().unwrap() {
            let _ = db.release_collection(&existing);
        }
        drop(db);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn ib008_swap_crash_after_backup_rename_restores_the_collection() {
        let root = temp_root();
        committed_collection(&root, "t", "k", 1).await;
        crate::util::rename_with_retry(&root.join("t"), &root.join("t.old")).unwrap();

        let db = Database::new(&root).unwrap();
        assert_eq!(db.list_collections().unwrap(), vec!["t".to_string()]);
        assert_eq!(db.get_collection("t").unwrap().get("k").unwrap(),
            Some(serde_json::json!({"v": 1})));
        assert!(root.join("t").is_dir());
        assert!(!root.join("t.old").exists());

        live_put(&db.get_collection("t").unwrap(), "k2", 2);
        assert_eq!(db.get_collection("t").unwrap().get("k").unwrap(),
            Some(serde_json::json!({"v": 1})),
            "a later write must use the restored directory, not a freshly created one");
    }

    #[tokio::test]
    async fn ib008_swap_crash_promotes_a_complete_staged_snapshot() {
        let root = temp_root();
        committed_collection(&root, "snap", "k", 1).await;
        committed_collection(&root, "t", "k", 2).await;

        crate::util::rename_with_retry(&root.join("t"), &root.join("t.old")).unwrap();
        crate::util::write_atomic(&root.join("snap"), INSTALL_MARKER, b"1").unwrap();
        crate::util::rename_with_retry(&root.join("snap"), &root.join("t.tmp")).unwrap();

        let db = Database::new(&root).unwrap();
        assert_eq!(db.get_collection("t").unwrap().get("k").unwrap(),
            Some(serde_json::json!({"v": 1})),
            "a crash after live→old must finish the install from the staged snapshot");
        assert!(root.join("t").is_dir());
        assert!(!root.join("t.tmp").exists());
        assert!(!root.join("t.old").exists());
        assert!(!root.join("snap").exists());
    }

    #[tokio::test]
    async fn ib008_incomplete_staging_restores_the_backed_up_collection() {
        let root = temp_root();
        committed_collection(&root, "app.events", "k", 7).await;
        crate::util::rename_with_retry(&root.join("app.events"), &root.join("app.events.old")).unwrap();
        fs::create_dir_all(root.join("app.events.tmp")).unwrap();

        let db = Database::new(&root).unwrap();
        assert_eq!(db.list_collections().unwrap(), vec!["app.events".to_string()]);
        assert_eq!(db.get_collection("app.events").unwrap().get("k").unwrap(),
            Some(serde_json::json!({"v": 7})));
        assert!(!root.join("app.events.tmp").exists());
        assert!(!root.join("app.events.old").exists());
    }

    #[tokio::test]
    async fn ib008_boot_drops_leftover_backup_once_the_live_directory_opens() {
        let root = temp_root();
        committed_collection(&root, "t", "k", 1).await;
        fs::create_dir_all(root.join("t.old")).unwrap();

        let db = Database::new(&root).unwrap();
        assert_eq!(db.get_collection("t").unwrap().get("k").unwrap(),
            Some(serde_json::json!({"v": 1})));
        assert!(!root.join("t.old").exists());
    }

    #[tokio::test]
    async fn ib009_case_and_trailing_dot_aliases_are_refused_by_database() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        for alias in ["Orders", "orders.", "orders..", "users_A", "nul", "con"] {
            match db.get_collection(alias) {
                Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
                Ok(_) => panic!("expected get_collection to fail for alias {}", alias),
            }
            match db.lookup_collection(alias) {
                Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
                Ok(_) => panic!("expected lookup_collection to fail for alias {}", alias),
            }
        }

        let orders = db.get_collection("orders").unwrap();
        assert_eq!(db.collections.read().unwrap().len(), 1);

        for alias in ["Orders", "orders.", "orders.."] {
            assert!(db.get_collection(alias).is_err());
            assert!(db.lookup_collection(alias).is_err());
        }
        assert_eq!(db.collections.read().unwrap().len(), 1);
        assert_eq!(orders.name, "orders");
        assert_eq!(db.list_collections().unwrap(), vec!["orders".to_string()]);
    }

    #[tokio::test]
    async fn ib009_a_legacy_alias_directory_refuses_boot_rather_than_hiding_its_tail() {
        let root = temp_root();
        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("orders").unwrap();
            live_put(&col, "k", 1);
            col.enqueue_commit().await.unwrap().unwrap();
            live_put(&col, "tail", 2);
            assert_eq!(col.last_appended(), (1, 2), "an appended tail above the commit watermark");
            drop(col);
            db.release_collection("orders").unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        crate::util::rename_with_retry(&root.join("orders"), &root.join("Orders")).unwrap();

        let err = match Database::new(&root) {
            Err(e) => e,
            Ok(_) => panic!("a collection this version cannot address must not boot as if absent"),
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Orders"),
            "the operator needs the directory named: {}", err);
    }

    #[tokio::test]
    async fn ib009_an_alias_directory_holding_no_collection_data_does_not_block_boot() {
        let root = temp_root();
        fs::create_dir_all(root.join("Orders")).unwrap();
        fs::create_dir_all(root.join("users_A")).unwrap();

        let db = Database::new(&root).unwrap();
        assert!(db.list_collections().unwrap().is_empty());
        assert!(db.get_collection("Orders").is_err());
        assert!(db.get_collection("users_A").is_err());
    }
}
