//! Compaction: relocate live keys into a fresh WAL, retire the frozen ones.

use super::collection::Collection;
use super::frame::{LogEntry, HEADER_LEN, MAX_RECORD_SIZE};
use super::index::IndexEntry;
use crate::util::remove_file_with_retry;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, warn};

pub struct SpaceUsage {
    pub total_bytes: u64,
    pub live_bytes: u64,
    pub live_keys: usize,
}

impl SpaceUsage {
    pub fn dead_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.live_bytes)
    }

    pub fn dead_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        self.dead_bytes() as f64 / self.total_bytes as f64
    }
}

struct CompactionGuard<'a> {
    pub flag: &'a AtomicBool,
}

impl<'a> Drop for CompactionGuard<'a> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

impl Collection {
    pub fn space_usage(&self) -> io::Result<SpaceUsage> {
        let mut total_bytes = 0u64;
        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = match name.to_str() {
                Some(n) => n,
                None => continue,
            };
            if name.starts_with("wal-") && name.ends_with(".log") {
                total_bytes += fs::metadata(entry.path())?.len();
            }
        }

        let index = self.index.read().unwrap();
        let live_bytes = index.values().map(|e| e.frame_bytes()).sum();

        Ok(SpaceUsage { total_bytes, live_bytes, live_keys: index.len() })
    }

    // Frozen WALs are <= N, rewritten output N+1, new active N+2; only the writer swap takes the lock.
    // Dropping superseded frames breaks the per-collection chain: replicas behind this must snapshot.
    pub fn compact(&self) -> io::Result<()> {
        if self.compacting.swap(true, Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Compaction already in progress"));
        }
        let _guard = CompactionGuard { flag: &self.compacting };
        let _snapshot_boundary = self.snapshot_boundary.try_lock()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "Snapshot transfer in progress"))?;
        // Uncommitted frames are absent from the index; retiring their WAL would lose them.
        if self.pending_len() > 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Uncommitted frames pending"));
        }

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

            // Key-order iteration leaves the compacted WAL not LSN-ordered.
            let frozen: Vec<(String, u64, u64)> = self.index.read().unwrap()
                .iter()
                .filter(|(_, e)| e.wal_id <= frozen_through)
                .map(|(k, e)| (k.clone(), e.wal_id, e.offset))
                .collect();

            (frozen, frozen_through, compact_id)
        };

        info!(target: "compaction", collection = %self.name, live_keys = frozen_index.len(),
            frozen_through, compact_wal = compact_id, active_wal = frozen_through + 2, "Compaction started");

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

        // Sampled before the index lock: `apply_committed` takes pending first, and inverting that
        // here deadlocks. A frame staged into a frozen WAL is uncommitted, so its WAL must survive.
        let retire = self.pending_floor().map_or(true, |(wal_id, _)| wal_id > frozen_through);

        let mut remapped = 0usize;
        let mut superseded = 0usize;
        {
            let _wal = self.wal_writer.lock().unwrap();
            let mut index = self.index.write().unwrap();

            // Remap only entries still pointing at the location we read; a concurrent overwrite wins.
            for (key, old_wal_id, old_offset, new_entry) in relocated {
                let unchanged = index.get(&key)
                    .map_or(false, |cur| cur.wal_id == old_wal_id && cur.offset == old_offset);
                if unchanged {
                    self.apply_index_put(&mut index, key, new_entry);
                    remapped += 1;
                } else {
                    superseded += 1;
                }
            }

            // Published under the index lock: past this point no reader can resolve a key into a
            // frozen WAL, so only handles taken before it are still outstanding.
            if retire {
                self.retired_through.fetch_max(frozen_through, Ordering::SeqCst);
            }
        }

        // The pivot. Until it lands the previous snapshot plus the intact frozen WALs still describe
        // the collection; once it names a resume point above them, boot skips them by id and a
        // failed unlink is wasted disk rather than a key coming back from the dead.
        self.save_index()?;

        let mut orphaned = 0usize;
        if retire {
            self.drain_read_pool(frozen_through);
            orphaned = self.remove_wals_through(frozen_through)?;
        }

        self.commit_signal.notify_one();

        info!(target: "compaction", collection = %self.name, relocated = remapped, superseded,
            retired = retire, orphaned, "Compaction complete");
        Ok(())
    }

    fn write_compacted_wal(
        &self,
        compact_path: &Path,
        frozen_index: &[(String, u64, u64)],
        compact_id: u64,
    ) -> io::Result<Vec<(String, u64, u64, IndexEntry)>> {
        let mut compact_file = BufWriter::new(File::create(compact_path)?);
        let mut relocated = Vec::with_capacity(frozen_index.len());
        let mut current_offset = 0u64;
        let mut readers: HashMap<u64, File> = HashMap::new();

        for (key, old_wal_id, old_offset) in frozen_index {
            let file = match readers.entry(*old_wal_id) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let path = self.root_path.join(format!("wal-{:05}.log", old_wal_id));
                    e.insert(File::open(&path)?)
                }
            };

            file.seek(SeekFrom::Start(*old_offset))?;

            let mut header = [0u8; HEADER_LEN];
            file.read_exact(&mut header)?;
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;

            if len == 0 || len as u64 > MAX_RECORD_SIZE {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("Live key '{}' has an invalid frame length in WAL {}", key, old_wal_id)));
            }

            let mut payload = vec![0u8; len];
            file.read_exact(&mut payload)?;

            match serde_json::from_slice::<LogEntry>(&payload) {
                Ok(LogEntry::Put { .. }) => {},
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("Live key '{}' does not resolve to a Put frame in WAL {}", key, old_wal_id))),
            }

            compact_file.write_all(&header)?;
            compact_file.write_all(&payload)?;

            relocated.push((
                key.clone(),
                *old_wal_id,
                *old_offset,
                self.build_entry(compact_id, current_offset, &payload),
            ));
            current_offset += (HEADER_LEN + len) as u64;
        }

        compact_file.flush()?;
        compact_file.get_mut().sync_all()?;
        Ok(relocated)
    }

    /// Returns how many obsolete WALs could not be unlinked. Safe to leave behind: the snapshot
    /// written before this call already puts boot's resume point past them.
    fn remove_wals_through(&self, frozen_through: u64) -> io::Result<usize> {
        let mut orphaned = 0usize;
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
                        orphaned += 1;
                        warn!(target: "compaction", collection = %self.name, file = %name, error = %e, "Could not remove obsolete WAL");
                    }
                },
                _ => {}
            }
        }
        Ok(orphaned)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::index::{IndexSnapshot, INDEX_FILENAME};
    use crate::storage::Database;
    use crate::test_support::{live_put, temp_root};
    use std::path::Path;

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
    async fn space_usage_tracks_dead_bytes_from_overwrites() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "a", 1);
        let fresh = col.space_usage().unwrap();
        assert_eq!(fresh.live_keys, 1);
        assert_eq!(fresh.dead_bytes(), 0, "a log with no overwrites has no dead bytes");

        for v in 2..=10 {
            live_put(&col, "a", v);
        }

        let churned = col.space_usage().unwrap();
        assert_eq!(churned.live_keys, 1, "ten writes to one key leave one live key");
        assert!(churned.dead_bytes() > 0);
        assert!(churned.dead_ratio() > 0.8,
            "nine superseded versions should dominate the log, got {:.2}", churned.dead_ratio());

        col.compact().unwrap();

        let reclaimed = col.space_usage().unwrap();
        assert_eq!(reclaimed.live_keys, 1);
        assert_eq!(reclaimed.dead_bytes(), 0, "compaction must reclaim every dead byte");
        assert!(reclaimed.total_bytes < churned.total_bytes);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn space_usage_counts_only_this_collections_wals() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let a = db.get_collection("a").unwrap();
        let b = db.get_collection("b").unwrap();

        live_put(&a, "k", 1);
        for v in 0..20 {
            live_put(&b, &format!("k{}", v), v);
        }

        let ua = a.space_usage().unwrap();
        let ub = b.space_usage().unwrap();

        assert_eq!(ua.live_keys, 1);
        assert_eq!(ub.live_keys, 20);
        assert!(ua.total_bytes < ub.total_bytes, "each collection measures its own WAL directory only");

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
    async fn a_frozen_wal_that_outlives_compaction_cannot_resurrect_a_deleted_key() {
        let root = temp_root();
        let col_dir = root.join("c");

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();

            live_put(&col, "keep", 1);
            live_put(&col, "gone", 2);
            col.enqueue_commit().await.unwrap().unwrap();
            col.compact().unwrap();

            // The surviving copy of the Put, as a failed unlink would leave it behind.
            let put_wal = col_dir.join(format!("wal-{:05}.log", wal_ids_on_disk(&col_dir)[0]));
            let orphan = (put_wal.clone(), fs::read(&put_wal).unwrap());

            col.delete("gone".into(), 1).unwrap();
            col.index.write().unwrap().remove("gone");
            col.enqueue_commit().await.unwrap().unwrap();
            col.compact().unwrap();

            assert!(!orphan.0.exists(), "the second compaction must have retired the first output");
            fs::write(&orphan.0, &orphan.1).unwrap();
        }

        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        assert_eq!(col.get("keep").unwrap(), Some(serde_json::json!({"v": 1})));
        assert!(col.get("gone").unwrap().is_none(),
            "a WAL compaction failed to unlink must not put a tombstoned key back");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn compaction_leaves_a_snapshot_that_resumes_past_the_wals_it_retired() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "k", 1);
        col.enqueue_commit().await.unwrap().unwrap();

        let frozen_through = col.wal_writer.lock().unwrap().current_wal_id;
        col.compact().unwrap();

        let file = File::open(col.root_path.join(INDEX_FILENAME))
            .expect("compaction must leave a snapshot, not unlink the one it had");
        let snapshot: IndexSnapshot = bincode::deserialize_from(file).unwrap();
        assert!(snapshot.last_wal_id > frozen_through,
            "boot must resume above the retired WALs, or it replays whatever survived them");

        let _ = fs::remove_dir_all(&root);
    }
}
