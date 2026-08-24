//! Open collections under one data directory and the LSN counters they share.

use super::collection::Collection;
use super::index::{LsnMeta, ReadCacheConfig};
use crate::util::remove_dir_with_retry;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{error, info, warn};

// durable_lsn is fsync progress, not replication; the quorum watermark lives in consensus::Progress.
pub struct Database {
    pub root_path: PathBuf,
    pub cache: ReadCacheConfig,
    pub collections: RwLock<HashMap<String, Arc<Collection>>>,
    pub durable_lsn: Arc<AtomicU64>,
    pub next_lsn: Arc<AtomicU64>,
    pub last_log_term: Arc<AtomicU64>,
}

impl Database {
    fn open_collection(&self, name: &str) -> io::Result<Arc<Collection>> {
        let col = Arc::new(Collection::open(
            name.to_string(),
            self.root_path.join(name),
            self.durable_lsn.clone(),
            self.next_lsn.clone(),
            self.last_log_term.clone(),
            self.cache.clone(),
        )?);
        Collection::start_commit_task(col.clone());
        Ok(col)
    }

    pub fn new(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        Self::with_cache(path, ReadCacheConfig::default())
    }

    pub fn with_cache(path: impl AsRef<std::path::Path>, cache: ReadCacheConfig) -> io::Result<Self> {
        let root_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&root_path)?;
        let boot_lsn = LsnMeta::load(&root_path).map(|m| m.commit_lsn).unwrap_or(0);
        if boot_lsn > 0 {
            info!(target: "db", "Restored durable LSN {} from lsn.meta", boot_lsn);
        }
        let db = Self {
            root_path,
            cache,
            collections: RwLock::new(HashMap::new()),
            durable_lsn: Arc::new(AtomicU64::new(boot_lsn)),
            next_lsn: Arc::new(AtomicU64::new(boot_lsn)),
            last_log_term: Arc::new(AtomicU64::new(0)),
        };
        db.adopt_collection_tails()?;
        Ok(db)
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

        let col = self.open_collection(name)?;
        collections.insert(name.to_string(), col.clone());
        Ok(col)
    }

    /// Holds the collection map write lock across release, directory swap, and reopen.
    pub fn install_staged_collection(&self, name: &str, staged_path: &std::path::Path) -> io::Result<()> {
        let expected_staging = self.root_path.join(format!("{}.tmp", name));
        if staged_path != expected_staging || !staged_path.is_dir() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "staged snapshot is not the expected collection temporary directory"));
        }

        let col_path = self.root_path.join(name);
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
        if had_old {
            if let Err(e) = fs::rename(&col_path, &old_path) {
                if let Ok(reopened) = self.open_collection(name) {
                    collections.insert(name.to_string(), reopened);
                }
                if let Some(path) = tombstone {
                    let _ = fs::remove_file(path);
                }
                return Err(e);
            }
        }

        if let Err(e) = fs::rename(staged_path, &col_path) {
            let restore = if had_old {
                fs::rename(&old_path, &col_path)
                    .and_then(|_| self.open_collection(name))
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

        match self.open_collection(name) {
            Ok(col) => {
                collections.insert(name.to_string(), col);
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
                let moved_bad = fs::rename(&col_path, staged_path).is_ok();
                if !moved_bad && col_path.exists() {
                    let _ = remove_dir_with_retry(&col_path);
                }
                let restore = if had_old {
                    fs::rename(&old_path, &col_path)
                        .and_then(|_| self.open_collection(name))
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

    pub fn release_collection(&self, name: &str) -> io::Result<Option<PathBuf>> {
        let existing = self.collections.write().unwrap().remove(name);
        match existing {
            Some(col) => Ok(Some(col.release_handles()?)),
            None => Ok(None),
        }
    }

    pub fn drop_collection(&self, name: &str) -> io::Result<bool> {
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

    // The one place the watermark may fall: a snapshot can shrink the log.
    /// Whether anything has ever been written here. Answered from directory entries rather than
    /// from open collections: a collection that exists on disk but has not been opened yet still
    /// holds data, and a check that missed it would report an empty node.
    ///
    /// A log containing only tombstones counts as data. Deliberately conservative -- this gates a
    /// change that can make keys unreadable, so the cheap error is a false "populated".
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
            let wal = col.wal_writer.lock().unwrap();
            if let Err(e) = wal.current_wal.sync_data() {
                error!(target: "storage", collection = %name, error = %e, "Failed to force sync WAL on shutdown");
            }
            self.durable_lsn.fetch_max(wal.last_appended_lsn, Ordering::SeqCst);
            drop(wal);
            let notifiers: Vec<_> = {
                let mut q = col.commit_notifiers.lock().unwrap();
                std::mem::take(&mut *q)
            };
            let count = notifiers.len();
            for tx in notifiers {
                let _ = tx.send(Ok(()));
            }
            info!(target: "storage", collection = %name, pending = count, "Flushed pending writes on shutdown");
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
    use crate::test_support::{idx, live_put, temp_root};

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

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
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
        let (f, w, o, _) = col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();
        col.index.write().unwrap().insert("k".into(), idx(&f, w, o));

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

    #[tokio::test]
    async fn reopening_after_drop_starts_empty() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let col = db.get_collection("users").unwrap();
        let (f, w, o, _) = col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();
        col.index.write().unwrap().insert("k".into(), idx(&f, w, o));

        db.drop_collection("users").unwrap();

        let fresh = db.get_collection("users").unwrap();
        assert!(fresh.index.read().unwrap().is_empty(), "dropped data must not resurrect on reopen");
        assert!(fresh.get("k").unwrap().is_none());

        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
    }
}
