//! WAL append, replicated-frame apply, boot replay, and frame read-back.

use super::collection::{Collection, StagedApply, READ_POOL_HANDLES};
use super::frame::{FrameHeader, LogEntry, ReplicaApply, HEADER_LEN, MAX_RECORD_SIZE};
use super::index::{IndexEntry, ReadCacheConfig};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::warn;

const WAL_ROTATION_LIMIT: u64 = 50 * 1024 * 1024;
const DRAIN_ATTEMPTS: usize = 5;

pub struct WalsState {
    pub current_wal: File,
    pub current_wal_id: u64,
    pub current_wal_size: u64,
    pub last_appended_lsn: u64,
    pub last_appended_term: u64,
}

impl Collection {
    pub fn replay_file_from(
        wal_id: u64,
        path: &PathBuf,
        mut start_offset: u64,
        index: &mut BTreeMap<String, IndexEntry>,
        pending: &mut BTreeMap<u64, StagedApply>,
        applied_through: u64,
        cache: &ReadCacheConfig,
        inline_used: &mut u64,
    ) -> io::Result<(u64, u64)> {
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

            let parsed = match FrameHeader::parse(&header) {
                Some(h) => h,
                None => break,
            };
            let (len, term, lsn) = (parsed.len, parsed.term, parsed.lsn);

            if len == 0 || (len as u64) > MAX_RECORD_SIZE {
                warn!(target: "wal", file = %path.display(), frame_len = len, "Invalid WAL frame length; truncating");
                break;
            }

            let mut payload = vec![0u8; len as usize];
            if let Err(_) = file.read_exact(&mut payload) {
                warn!(target: "wal", file = %path.display(), "Unexpected EOF while reading payload; truncating");
                break;
            }

            if !parsed.payload_valid(&payload) {
                warn!(target: "wal", file = %path.display(), "CRC mismatch; truncating at chunk boundary");
                break;
            }

            if let Ok(entry) = serde_json::from_slice::<LogEntry>(&payload) {
                if lsn > applied_through {
                    // Uncommitted at the last shutdown: keep it durable but unpublished.
                    let staged = match entry {
                        LogEntry::Put { key, .. } => {
                            let inline = if len <= cache.inline_max_value_bytes {
                                Some(payload.clone().into_boxed_slice())
                            } else {
                                None
                            };
                            StagedApply {
                                key,
                                wal_id,
                                offset,
                                entry: Some(IndexEntry { wal_id, offset, len, inline }),
                            }
                        },
                        LogEntry::Del { key, .. } => StagedApply { key, wal_id, offset, entry: None },
                    };
                    pending.insert(lsn, staged);
                    if lsn > max_lsn {
                        max_lsn = lsn;
                        max_term = term;
                    }
                    offset += HEADER_LEN as u64 + len as u64;
                    valid_end_offset = offset;
                    continue;
                }
                match entry {
                    LogEntry::Put { key, .. } => {
                        let inline = if len as u32 <= cache.inline_max_value_bytes
                            && *inline_used + len as u64 <= cache.inline_budget_bytes
                        {
                            *inline_used += len as u64;
                            Some(payload.clone().into_boxed_slice())
                        } else {
                            None
                        };
                        if let Some(old) = index.insert(key, IndexEntry { wal_id, offset, len: len as u32, inline }) {
                            *inline_used -= old.inline_bytes();
                        }
                    },
                    LogEntry::Del { key, .. } => {
                        if let Some(old) = index.remove(&key) {
                            *inline_used -= old.inline_bytes();
                        }
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

        // Crash recovery: a torn tail is expected, so stop at the first bad frame and truncate.
        if valid_end_offset < file_len {
            file.set_len(valid_end_offset)?;
            warn!(target: "wal", file = %path.display(), size = valid_end_offset, "Truncated corrupted WAL file");
        }

        Ok((max_lsn, max_term))
    }

    pub fn append(&self, entry: LogEntry, term: u64) -> io::Result<(Vec<u8>, u64, u64, u64)> {
        let json_bytes = serde_json::to_vec(&entry)?;
        let len = json_bytes.len() as u64;

        if len > MAX_RECORD_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Record exceeds maximum size"));
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&json_bytes);
        let crc = hasher.finalize();

        let frame_len = HEADER_LEN as u64 + len;

        self.record_watermark_once();
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

        let header = FrameHeader {
            len: len as u32,
            crc,
            term,
            lsn,
            // prev_* must be sampled under the append lock; outside it, two writers interleave their links.
            prev_lsn: wal.last_appended_lsn,
            prev_term: wal.last_appended_term,
        }.encode();

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

        self.stage_appended(lsn, &entry, wal_id, offset, &json_bytes);
        drop(wal);

        Ok((frame, wal_id, offset, lsn))
    }

    pub fn append_raw_frame(&self, frame_bytes: &[u8]) -> io::Result<ReplicaApply> {
        let header = FrameHeader::parse(frame_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Frame too short"))?;
        let len = header.len as usize;

        if frame_bytes.len() < HEADER_LEN + len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Frame payload incomplete"));
        }

        let payload = &frame_bytes[HEADER_LEN..HEADER_LEN + len];

        if !header.payload_valid(payload) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch on replicated frame"));
        }

        let entry: LogEntry = serde_json::from_slice(payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let frame_len = (HEADER_LEN + len) as u64;

        self.record_watermark_once();
        let mut wal = self.wal_writer.lock().unwrap();

        if self.released.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Collection handle is no longer active"));
        }

        let last = wal.last_appended_lsn;
        let last_term = wal.last_appended_term;

        // Ordering invariant: divergence before duplicate. A newer term reusing an LSN we hold is a
        // conflicting log, not a retransmit, and checking duplicate first keeps a deposed leader's tail.
        if header.lsn <= last && header.term > last_term {
            return Ok(ReplicaApply::Divergent { last_lsn: last, last_term });
        }

        if header.lsn <= last {
            return Ok(ReplicaApply::Duplicate { last_lsn: last });
        }

        if header.prev_lsn != last {
            return Ok(ReplicaApply::Gap { last_lsn: last, last_term });
        }

        // Raft log matching: same position, different history.
        if header.prev_term != last_term {
            return Ok(ReplicaApply::Divergent { last_lsn: last, last_term });
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
        wal.last_appended_lsn = header.lsn;
        wal.last_appended_term = header.term;
        self.db_next_lsn.fetch_max(header.lsn, Ordering::SeqCst);
        self.db_last_log_term.store(header.term, Ordering::SeqCst);

        self.stage_appended(header.lsn, &entry, wal_id, offset, payload);

        Ok(ReplicaApply::Applied { lsn: header.lsn })
    }

    /// Frames in `(after_lsn, up_to_lsn]`, LSN-ordered and deduplicated. Refuses to return a set with
    /// a hole in it: the caller cannot tell a compaction race from a genuine loss, and `chain_prefix`
    /// turns either into a full snapshot resync.
    pub fn read_frames_after(&self, after_lsn: u64, up_to_lsn: u64) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let retired = self.retired_through.load(Ordering::SeqCst);

        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if fname.starts_with("wal-") && fname.ends_with(".log") {
                    let id_part = &fname[4..fname.len() - 4];
                    match id_part.parse::<u64>() {
                        // A retired WAL holds only frames the compacted one already carries, so
                        // scanning one an unlink left behind emits every LSN in it twice.
                        Ok(id) if id > retired => wal_files.push((id, path)),
                        _ => {},
                    }
                }
            }
        }
        wal_files.sort_by_key(|(id, _)| *id);

        // Only the WAL being appended to may end mid-frame; anywhere else that is a hole.
        let active = wal_files.last().map_or(0, |(id, _)| *id);

        let mut out: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        for (id, path) in wal_files {
            let mut file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => return Err(gapped_scan(&self.name, format!("WAL {} vanished mid-scan: {}", id, e))),
            };
            let file_len = file.metadata()?.len();
            let mut offset = 0u64;

            loop {
                let mut header = [0u8; HEADER_LEN];
                if file.read_exact(&mut header).is_err() {
                    break;
                }
                let parsed = match FrameHeader::parse(&header) {
                    Some(h) => h,
                    None => break,
                };
                let len = parsed.len as usize;

                if len == 0 || len as u64 > MAX_RECORD_SIZE {
                    break;
                }

                let mut payload = vec![0u8; len];
                if file.read_exact(&mut payload).is_err() {
                    break;
                }

                if !parsed.payload_valid(&payload) {
                    break;
                }

                offset += (HEADER_LEN + len) as u64;

                if parsed.lsn > after_lsn && parsed.lsn <= up_to_lsn {
                    let mut frame = Vec::with_capacity(HEADER_LEN + len);
                    frame.extend_from_slice(&header);
                    frame.extend_from_slice(&payload);
                    // Highest WAL id wins: a relocated copy supersedes the frozen original.
                    out.insert(parsed.lsn, frame);
                }
            }

            if offset < file_len && id != active {
                return Err(gapped_scan(&self.name,
                    format!("WAL {} stops at {} of {} bytes", id, offset, file_len)));
            }
        }

        // Cheaper than holding `snapshot_boundary` for the whole scan, which would serialise every
        // repair on a collection behind one another.
        if self.retired_through.load(Ordering::SeqCst) != retired {
            return Err(gapped_scan(&self.name, "a compaction retired WALs mid-scan".to_string()));
        }

        Ok(out.into_iter().collect())
    }

    fn wal_reader(&self, wal_id: u64) -> io::Result<Arc<std::sync::Mutex<File>>> {
        if wal_id <= self.retired_through.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("WAL {} was retired by compaction", wal_id)));
        }
        let mut pool = self.read_pool.lock().unwrap();
        let counter = self.read_pool_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(handles) = pool.get_mut(&wal_id) {
            return Ok(handles[counter % handles.len()].clone());
        }
        let path = self.root_path.join(format!("wal-{:05}.log", wal_id));
        let mut handles = Vec::new();
        for _ in 0..READ_POOL_HANDLES {
            handles.push(Arc::new(std::sync::Mutex::new(File::open(&path)?)));
        }
        pool.insert(wal_id, handles.clone());
        Ok(handles[counter % READ_POOL_HANDLES].clone())
    }

    /// Retires every pooled handle for a WAL id at or below `through` and waits for in-flight
    /// readers to drop theirs. Windows will not unlink a file that any handle still holds open.
    pub fn drain_read_pool(&self, through: u64) {
        let retired: Vec<Arc<std::sync::Mutex<File>>> = {
            let mut pool = self.read_pool.lock().unwrap();
            let ids: Vec<u64> = pool.keys().copied().filter(|id| *id <= through).collect();
            ids.iter().filter_map(|id| pool.remove(id)).flatten().collect()
        };
        for attempt in 0..DRAIN_ATTEMPTS {
            if retired.iter().all(|h| Arc::strong_count(h) == 1) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(20 * (attempt + 1) as u64));
        }
        warn!(target: "storage", collection = %self.name, through,
            "Read handles still in flight after drain; retired WALs may not unlink");
    }

    /// `expected_len` is what the index recorded for this frame. A disagreeing length or a failed
    /// CRC means the offset no longer names that frame, so the bytes are refused, not returned.
    pub fn read_frame_payload(&self, wal_id: u64, offset: u64, expected_len: u32) -> io::Result<Vec<u8>> {
        let file_arc = self.wal_reader(wal_id)?;
        let mut file = file_arc.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;

        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header)?;
        let parsed = FrameHeader::parse(&header)
            .ok_or_else(|| bad_frame(wal_id, offset, "unparseable header"))?;

        if parsed.len != expected_len || parsed.len == 0 || parsed.len as u64 > MAX_RECORD_SIZE {
            return Err(bad_frame(wal_id, offset, "frame length disagrees with the index"));
        }

        let mut payload = vec![0u8; parsed.len as usize];
        file.read_exact(&mut payload)?;

        if !parsed.payload_valid(&payload) {
            return Err(bad_frame(wal_id, offset, "payload failed its CRC"));
        }
        Ok(payload)
    }

}

fn gapped_scan(collection: &str, why: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData,
        format!("Frame scan of '{}' would be gapped: {}", collection, why))
}

fn bad_frame(wal_id: u64, offset: u64, why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData,
        format!("WAL {} offset {}: {}", wal_id, offset, why))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::frame::MAX_RECORD_SIZE;
    use crate::storage::Database;
    use crate::test_support::{disk_put, live_put, make_frame, temp_root};
    use std::io::Write;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn durability_recovery_size_limit_and_corruption() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("test_durability").unwrap();

            let mut last = 0;
            for i in 0..100 {
                last = col.put(format!("key:{}", i), serde_json::json!({"n": i}), 1).unwrap().3;
            }
            col.enqueue_commit().await.unwrap().unwrap();
            col.apply_committed(last);
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

        assert!(db2.durable_lsn.load(Ordering::SeqCst) >= 100, "Commit LSN should survive restart");

        col2.save_index().unwrap();

        let huge_str = "x".repeat((MAX_RECORD_SIZE + 10) as usize);
        let res = col2.put("huge_key".to_string(), serde_json::json!({"data": huge_str}), 1);
        assert!(res.is_err(), "Should reject a record that exceeds MAX_RECORD_SIZE");

        if let Ok((_, _, _, lsn)) = col2.put("key_pre_corrupt".to_string(), serde_json::json!({"valid": true}), 1) {
            col2.enqueue_commit().await.unwrap().unwrap();
            col2.apply_committed(lsn);
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

        if let Ok((_, _, _, lsn)) = col3.put("key_post_corrupt".to_string(), serde_json::json!({"valid": true}), 1) {
            col3.enqueue_commit().await.unwrap().unwrap();
            col3.apply_committed(lsn);
        }
        assert!(col3.get("key_post_corrupt").unwrap().is_some(), "Writes should continue after recovery");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_read_refuses_a_frame_that_is_not_the_one_the_index_recorded() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let (wal_id, off_a, len) = disk_put(&col, "a", "a");
        let (_, off_b, _) = disk_put(&col, "b", "b");
        col.enqueue_commit().await.unwrap().unwrap();

        // b's payload spliced over a's: same length, still valid JSON, wrong document.
        let path = col.root_path.join(format!("wal-{:05}.log", wal_id));
        let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let mut payload_b = vec![0u8; len as usize];
        f.seek(SeekFrom::Start(off_b + HEADER_LEN as u64)).unwrap();
        f.read_exact(&mut payload_b).unwrap();
        f.seek(SeekFrom::Start(off_a + HEADER_LEN as u64)).unwrap();
        f.write_all(&payload_b).unwrap();
        drop(f);

        col.drain_read_pool(u64::MAX);

        let err = col.get("a").expect_err("a frame failing its CRC must not be served as a value");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_drain_waits_for_a_reader_that_still_holds_a_handle() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        disk_put(&col, "a", "a");
        assert!(col.get("a").unwrap().is_some(), "the read must warm the pool");

        let held = col.read_pool.lock().unwrap().values().next().unwrap()[0].clone();

        let started = std::time::Instant::now();
        col.drain_read_pool(u64::MAX);
        assert!(started.elapsed() >= std::time::Duration::from_millis(100),
            "a drain that returns while a reader holds a handle has drained nothing");
        assert!(col.read_pool.lock().unwrap().is_empty());

        drop(held);
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_wal_a_failed_unlink_left_behind_does_not_duplicate_relocated_frames() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 1..=3 {
            live_put(&col, &format!("k{}", i), i);
        }
        col.enqueue_commit().await.unwrap().unwrap();

        let frozen = col.root_path.join(format!("wal-{:05}.log",
            col.wal_writer.lock().unwrap().current_wal_id));
        let orphan = fs::read(&frozen).unwrap();

        col.compact().unwrap();
        assert!(!frozen.exists());
        fs::write(&frozen, &orphan).unwrap();

        let frames = col.read_frames_after(0, 3).unwrap();
        let lsns: Vec<u64> = frames.iter().map(|(lsn, _)| *lsn).collect();
        assert_eq!(lsns, vec![1, 2, 3],
            "a frame relocated by compaction must be reported once, not once per surviving copy");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_scan_refuses_to_report_a_wal_that_stops_mid_frame() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 1..=3 {
            live_put(&col, &format!("k{}", i), i);
        }
        col.enqueue_commit().await.unwrap().unwrap();
        col.compact().unwrap();

        // The compacted WAL, which is frozen: nothing appends to it, so a short read is a hole.
        let compacted = col.root_path.join(format!("wal-{:05}.log", col.retired_through
            .load(Ordering::SeqCst) + 1));
        let len = compacted.metadata().unwrap().len();
        OpenOptions::new().write(true).open(&compacted).unwrap().set_len(len - 5).unwrap();

        let err = col.read_frames_after(0, 3)
            .expect_err("a scan that cannot read a frozen WAL out must not report a short set");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_partial_frame_at_the_tail_of_the_active_wal_is_still_tolerated() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for i in 1..=3 {
            live_put(&col, &format!("k{}", i), i);
        }
        col.enqueue_commit().await.unwrap().unwrap();

        let active = col.root_path.join(format!("wal-{:05}.log",
            col.wal_writer.lock().unwrap().current_wal_id));
        OpenOptions::new().append(true).open(&active).unwrap().write_all(&[0u8; 7]).unwrap();

        let frames = col.read_frames_after(0, 3).unwrap();
        assert_eq!(frames.len(), 3, "an append caught in flight must not fail the scan");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn an_appended_frame_is_accounted_for_before_the_append_returns() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();

        assert_eq!(col.pending_len(), 1,
            "a frame the caller has not staged yet is in neither the index nor pending, and              nothing compaction checks can see it");
        assert!(col.compact().is_err(),
            "so compaction must refuse rather than retire the WAL holding an in-flight write");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_replicated_frame_is_accounted_for_before_the_apply_returns() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        col.append_raw_frame(&make_frame(1, 1, 0, 0, "k", 1)).unwrap();

        assert_eq!(col.pending_len(), 1, "the replica path closes the same window");
        assert!(col.compact().is_err());

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

        match rcol.append_raw_frame(&f1).unwrap() {
            ReplicaApply::Applied { lsn, .. } => assert_eq!(lsn, 1),
            other => panic!("expected Applied, got {:?}", other),
        }

        match rcol.append_raw_frame(&f3).unwrap() {
            ReplicaApply::Gap { last_lsn, .. } => assert_eq!(last_lsn, 1),
            other => panic!("expected Gap, got {:?}", other),
        }

        match rcol.append_raw_frame(&f2).unwrap() {
            ReplicaApply::Applied { lsn, .. } => assert_eq!(lsn, 2),
            other => panic!("expected Applied, got {:?}", other),
        }

        match rcol.append_raw_frame(&f3).unwrap() {
            ReplicaApply::Applied { lsn, .. } => assert_eq!(lsn, 3),
            other => panic!("expected Applied, got {:?}", other),
        }

        match rcol.append_raw_frame(&f2).unwrap() {
            ReplicaApply::Duplicate { last_lsn } => assert_eq!(last_lsn, 3),
            other => panic!("expected Duplicate, got {:?}", other),
        }

        rcol.enqueue_commit().await.unwrap().unwrap();
        rcol.apply_committed(3);
        drop(rcol);
        drop(rdb);

        let rdb2 = Database::new(&rroot).unwrap();
        let rcol2 = rdb2.get_collection("c").unwrap();
        assert_eq!(rcol2.get("k1").unwrap(), Some(serde_json::json!({"v": 1})));
        assert_eq!(rcol2.get("k2").unwrap(), Some(serde_json::json!({"v": 2})));
        assert_eq!(rcol2.get("k3").unwrap(), Some(serde_json::json!({"v": 3})));
        assert_eq!(rdb2.durable_lsn.load(Ordering::SeqCst), 3, "Replica LSN must match the frames it applied from the primary");

        let _ = fs::remove_dir_all(&proot);
        let _ = fs::remove_dir_all(&rroot);
    }

    #[tokio::test]
    async fn frames_chain_to_their_collection_predecessor_not_to_lsn_minus_one() {
        let proot = temp_root();
        let rroot = temp_root();

        let pdb = Database::new(&proot).unwrap();
        let pa = pdb.get_collection("alpha").unwrap();
        let pb = pdb.get_collection("beta").unwrap();

        let (fa1, _, _, la1) = pa.put("k1".into(), serde_json::json!({"v": 1}), 1).unwrap();
        let (fb1, _, _, lb1) = pb.put("k1".into(), serde_json::json!({"v": 2}), 1).unwrap();
        let (fa2, _, _, la2) = pa.put("k2".into(), serde_json::json!({"v": 3}), 1).unwrap();
        let (fb2, _, _, lb2) = pb.put("k2".into(), serde_json::json!({"v": 4}), 1).unwrap();

        assert_eq!((la1, lb1, la2, lb2), (1, 2, 3, 4),
            "LSNs are handed out by one database-wide counter, so collections interleave");

        assert_eq!(FrameHeader::parse(&fa1).unwrap().prev_lsn, 0);
        assert_eq!(FrameHeader::parse(&fb1).unwrap().prev_lsn, 0,
            "beta's first frame has no predecessor in beta, even though alpha already used lsn 1");
        assert_eq!(FrameHeader::parse(&fa2).unwrap().prev_lsn, 1,
            "alpha's second frame follows alpha's first, not the globally previous lsn 2");
        assert_eq!(FrameHeader::parse(&fb2).unwrap().prev_lsn, 2);

        let rdb = Database::new(&rroot).unwrap();
        let ra = rdb.get_collection("alpha").unwrap();
        let rb = rdb.get_collection("beta").unwrap();

        for (col, frame, expected) in [(&ra, &fa1, 1u64), (&rb, &fb1, 2), (&ra, &fa2, 3), (&rb, &fb2, 4)] {
            match col.append_raw_frame(frame).unwrap() {
                ReplicaApply::Applied { lsn, .. } => assert_eq!(lsn, expected),
                other => panic!(
                    "a write to a second collection must not look like a gap (lsn {}), got {:?}",
                    expected, other),
            }
        }

        ra.enqueue_commit().await.unwrap().unwrap();
        rb.enqueue_commit().await.unwrap().unwrap();
        ra.apply_committed(3);
        rb.apply_committed(4);
        drop(ra);
        drop(rb);
        drop(rdb);

        let rdb2 = Database::new(&rroot).unwrap();
        assert_eq!(rdb2.get_collection("alpha").unwrap().get("k2").unwrap(), Some(serde_json::json!({"v": 3})));
        assert_eq!(rdb2.get_collection("beta").unwrap().get("k2").unwrap(), Some(serde_json::json!({"v": 4})));

        let _ = fs::remove_dir_all(&proot);
        let _ = fs::remove_dir_all(&rroot);
    }

    #[tokio::test]
    async fn a_replica_holding_a_superseded_leaders_tail_reports_divergence() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let mut prev = (0u64, 0u64);
        for lsn in 1..=3u64 {
            let frame = make_frame(1, lsn, prev.0, prev.1, &format!("k{}", lsn), lsn as i64);
            match col.append_raw_frame(&frame).unwrap() {
                ReplicaApply::Applied { .. } => {},
                other => panic!("term-1 replication should apply cleanly, got {:?}", other),
            }
            prev = (lsn, 1);
        }

        let contested = make_frame(2, 3, 2, 1, "k3", 99);
        match col.append_raw_frame(&contested).unwrap() {
            ReplicaApply::Divergent { last_lsn, last_term } => assert_eq!((last_lsn, last_term), (3, 1),
                "the replica must report where its own log ends so the primary can replace it"),
            other => panic!("a newer term re-using an occupied lsn is divergence, not a duplicate: {:?}", other),
        }

        let retransmit = make_frame(1, 2, 1, 1, "k2", 2);
        match col.append_raw_frame(&retransmit).unwrap() {
            ReplicaApply::Duplicate { last_lsn } => assert_eq!(last_lsn, 3),
            other => panic!("expected Duplicate, got {:?}", other),
        }

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_frame_whose_predecessor_sits_in_another_term_is_refused() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        let first = make_frame(1, 1, 0, 0, "k1", 1);
        assert!(matches!(col.append_raw_frame(&first).unwrap(), ReplicaApply::Applied { .. }));

        let mismatched = make_frame(2, 2, 1, 2, "k2", 2);
        match col.append_raw_frame(&mismatched).unwrap() {
            ReplicaApply::Divergent { last_lsn, last_term } => assert_eq!((last_lsn, last_term), (1, 1)),
            other => panic!("log matching must reject a predecessor from another term, got {:?}", other),
        }

        let agreed = make_frame(2, 2, 1, 1, "k2", 2);
        match col.append_raw_frame(&agreed).unwrap() {
            ReplicaApply::Applied { lsn, .. } => assert_eq!(lsn, 2),
            other => panic!("a new term appending onto agreed history must apply, got {:?}", other),
        }

        let _ = fs::remove_dir_all(&root);
    }
}
