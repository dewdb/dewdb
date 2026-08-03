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
use tracing::{error, info};

// durable_lsn is what this node has fsynced. It says nothing about replication;
// the quorum-committed watermark lives in consensus::Progress.
pub struct Database {
    pub root_path: PathBuf,
    pub cache: ReadCacheConfig,
    pub collections: RwLock<HashMap<String, Arc<Collection>>>,
    pub durable_lsn: Arc<AtomicU64>,
    pub next_lsn: Arc<AtomicU64>,
    pub last_log_term: Arc<AtomicU64>,
}

impl Database {
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
        Ok(Self {
            root_path,
            cache,
            collections: RwLock::new(HashMap::new()),
            durable_lsn: Arc::new(AtomicU64::new(boot_lsn)),
            next_lsn: Arc::new(AtomicU64::new(boot_lsn)),
            last_log_term: Arc::new(AtomicU64::new(0)),
        })
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

        let col_path = self.root_path.join(name);
        let col = Arc::new(Collection::open(
            name.to_string(),
            col_path,
            self.durable_lsn.clone(),
            self.next_lsn.clone(),
            self.last_log_term.clone(),
            self.cache.clone(),
        )?);
        Collection::start_commit_task(col.clone());
        collections.insert(name.to_string(), col.clone());
        Ok(col)
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

    // The watermark normally only rises. A snapshot can shrink the log, so this is
    // the one place it may go down.
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
}
