//! WAL append, replicated-frame apply, boot replay, and frame read-back.

use super::collection::{Collection, StagedApply, StagedEffect, READ_POOL_HANDLES};
use super::frame::{Configuration, FrameHeader, HandoverRecord, LogEntry, ReplicaApply, HEADER_LEN, MAX_RECORD_SIZE};
use super::index::{IndexEntry, ReadCacheConfig};
use super::secondary::{index_values, IndexSpec};
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
        // Inline bytes held by staged frames: the same budget, since it is the same memory, but
        // kept out of `inline_used` because `apply_index_put` charges them when they commit (M11).
        staged_inline: &mut u64,
        dropped: &mut bool,
        config: &mut Option<Configuration>,
        handover: &mut Option<HandoverRecord>,
        // Committed below the watermark and in force above it, both in one list: a staged `Put`
        // has to index for every definition the log put in force before it, committed or not.
        indexes: &mut Vec<IndexSpec>,
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
                    let effect = match entry {
                        LogEntry::Put { key, value, .. } => {
                            let inline = if len <= cache.inline_max_value_bytes
                                && *inline_used + *staged_inline + len as u64 <= cache.inline_budget_bytes
                            {
                                *staged_inline += len as u64;
                                Some(payload.clone().into_boxed_slice())
                            } else {
                                None
                            };
                            StagedEffect::Put {
                                key,
                                entry: IndexEntry { wal_id, offset, len, inline },
                                indexed: index_values(indexes, &value),
                            }
                        },
                        LogEntry::Del { key, .. } => StagedEffect::Remove { key },
                        LogEntry::Barrier { .. } => StagedEffect::Nothing,
                        LogEntry::Drop { .. } => {
                            indexes.clear();
                            StagedEffect::Clear
                        },
                        LogEntry::Config { config, .. } => StagedEffect::Configure(config),
                        LogEntry::Handover { handover, .. } => StagedEffect::RecordHandover(handover),
                        // In force from here on, the way the append path puts it in force: every
                        // staged frame above this one indexes for it.
                        LogEntry::Index { change, .. } => {
                            change.apply_to(indexes);
                            StagedEffect::DefineIndex(change)
                        },
                    };
                    pending.insert(lsn, StagedApply { wal_id, offset, term, effect });
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
                        *dropped = false;
                    },
                    LogEntry::Del { key, .. } => {
                        if let Some(old) = index.remove(&key) {
                            *inline_used -= old.inline_bytes();
                        }
                        *dropped = false;
                    },
                    // Already committed and it applies nothing; it still carries the tail LSN below.
                    LogEntry::Barrier { .. } => {},
                    // Replay is in append order, so everything below it is already in `index`.
                    LogEntry::Drop { .. } => {
                        index.clear();
                        *inline_used = 0;
                        *dropped = true;
                        indexes.clear();
                    },
                    // Append order again: the last one below the watermark is the committed one.
                    LogEntry::Config { config: c, .. } => *config = Some(c),
                    LogEntry::Handover { handover: h, .. } => *handover = Some(h),
                    LogEntry::Index { change, .. } => change.apply_to(indexes),
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

        self.record_watermark_once()?;
        let mut wal = self.wal_writer.lock().unwrap();

        if self.released.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Collection handle is no longer active"));
        }

        if wal.current_wal_size >= WAL_ROTATION_LIMIT {
            wal.current_wal.sync_all()?;
            self.open_next_wal(&mut wal)?;
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

        if let Err(e) = wal.current_wal.write_all(&header)
            .and_then(|()| wal.current_wal.write_all(&json_bytes))
        {
            return Err(self.rotate_past_torn_write(&mut wal, e));
        }

        let offset = wal.current_wal_size;
        wal.current_wal_size += frame_len;

        let wal_id = wal.current_wal_id;
        wal.last_appended_lsn = lsn;
        wal.last_appended_term = term;
        self.db_last_log_term.store(term, Ordering::SeqCst);

        self.stage_appended(lsn, term, &entry, wal_id, offset, &json_bytes);
        drop(wal);

        Ok((frame, wal_id, offset, lsn))
    }

    fn open_next_wal(&self, wal: &mut WalsState) -> io::Result<()> {
        wal.current_wal_id += 1;
        let new_path = self.root_path.join(format!("wal-{:05}.log", wal.current_wal_id));
        wal.current_wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&new_path)?;
        wal.current_wal_size = 0;
        Ok(())
    }

    /// `write_all` can write some bytes and then fail, leaving a frame on disk that the size counter
    /// is not past. Carrying that counter forward records every later frame at an offset that is not
    /// its own, and replay stops at the tear and truncates everything above it -- including frames
    /// whose clients were told `200` (M21). Rotating leaves the tear last in its WAL, where replay
    /// takes it and nothing else, and the writer continues on a clean file.
    fn rotate_past_torn_write(&self, wal: &mut WalsState, cause: io::Error) -> io::Error {
        match self.open_next_wal(wal) {
            Ok(()) => cause,
            // Nowhere left to write, and the counter is deliberately still behind the tear: the
            // next append fails the same way rather than landing where nothing can read it back.
            Err(rotate) => io::Error::other(format!(
                "{}; the WAL could not be rotated past the partial frame either: {}", cause, rotate)),
        }
    }

    /// Raft §5.3: entries past the leader's position came from a log it does not have, so they are
    /// deleted. Only above the committed watermark -- below it they are agreed, and a leader asking
    /// us to drop one is not a leader we can follow, so the caller answers `Divergent` and the
    /// snapshot path takes over.
    ///
    /// `Some(())` means the log now ends at `prev_lsn` and the caller may set the tail to
    /// `(prev_lsn, prev_term)`. The frame at `prev_lsn` is checked against `prev_term` when we hold
    /// it staged; at the watermark itself it is committed, so both logs carry the same one.
    fn rewind_to(&self, wal: &mut WalsState, prev_lsn: u64, prev_term: u64) -> io::Result<Option<()>> {
        let cut = {
            let mut pending = self.pending.lock().unwrap();
            // Read under `pending`, which `apply_committed` also holds while it publishes and moves
            // the watermark. Outside it an entry can commit between this check and the cut.
            let applied = self.applied_lsn();
            if prev_lsn < applied {
                return Ok(None);
            }
            match pending.get(&prev_lsn) {
                Some(staged) if staged.term != prev_term => return Ok(None),
                // Sparse LSNs: no staged frame here and not the watermark means we never held it.
                None if prev_lsn != applied => return Ok(None),
                _ => {},
            }

            let doomed: Vec<u64> = pending.range((prev_lsn + 1)..).map(|(lsn, _)| *lsn).collect();
            // Our tail is above the cut, yet nothing uncommitted is: they were published while we
            // decided, and a published entry is not ours to drop.
            if doomed.is_empty() {
                return Ok(None);
            }
            let cut = pending.get(&doomed[0]).map(|s| (s.wal_id, s.offset));
            self.rewind_index_tail(prev_lsn, prev_term)?;
            for lsn in doomed {
                if let Some(dropped) = pending.remove(&lsn) {
                    self.release_staged(&dropped.effect);
                }
            }
            cut
        };

        if let Some((wal_id, offset)) = cut {
            self.shrink_wal_to(wal, wal_id, offset)?;
            // The fsynced tail is now the cut. Left high, a promotion would count durability this
            // node no longer has toward the quorum that decides a commit index.
            self.durable_lsn.store(prev_lsn, Ordering::SeqCst);
        }
        Ok(Some(()))
    }

    /// Later WALs are emptied before the cut file shrinks. The reverse order leaves frames above a
    /// truncation point after a crash, and replay cannot tell those from a log that continues.
    fn shrink_wal_to(&self, wal: &mut WalsState, cut_wal_id: u64, cut_offset: u64) -> io::Result<()> {
        for id in (cut_wal_id + 1)..=wal.current_wal_id {
            let path = self.root_path.join(format!("wal-{:05}.log", id));
            if path.exists() {
                let emptied = OpenOptions::new().write(true).open(&path)?;
                emptied.set_len(0)?;
                emptied.sync_all()?;
            }
        }

        let path = self.root_path.join(format!("wal-{:05}.log", cut_wal_id));
        // Through a write handle, not the writer's: an append-mode handle carries no write access
        // on Windows and `set_len` on one is refused outright.
        let shrinking = OpenOptions::new().write(true).open(&path)?;
        shrinking.set_len(cut_offset)?;
        shrinking.sync_all()?;

        wal.current_wal = OpenOptions::new().create(true).append(true).read(true).open(&path)?;
        wal.current_wal_id = cut_wal_id;
        wal.current_wal_size = cut_offset;
        Ok(())
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

        self.record_watermark_once()?;
        let mut wal = self.wal_writer.lock().unwrap();

        if self.released.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Collection handle is no longer active"));
        }

        let last = wal.last_appended_lsn;
        let last_term = wal.last_appended_term;

        // A retransmit: committed entries are agreed, staged ones match only at the same term.
        // Ahead of the truncation, which would read an ordinary resend as a conflicting log.
        if header.lsn <= last
            && (header.lsn <= self.applied_lsn() || self.staged_term(header.lsn) == Some(header.term))
        {
            return Ok(ReplicaApply::Duplicate { last_lsn: last });
        }

        if header.prev_lsn > last {
            return Ok(ReplicaApply::Gap { last_lsn: last, last_term });
        }

        if header.prev_lsn < last {
            if self.rewind_to(&mut wal, header.prev_lsn, header.prev_term)?.is_none() {
                return Ok(ReplicaApply::Divergent { last_lsn: last, last_term });
            }
            wal.last_appended_lsn = header.prev_lsn;
            wal.last_appended_term = header.prev_term;
        } else if header.prev_term != last_term {
            // Raft log matching: same position, different history. The conflict is at or below our
            // tail, so there is nothing here to truncate to -- the leader has to back up further.
            return Ok(ReplicaApply::Divergent { last_lsn: last, last_term });
        }

        if wal.current_wal_size >= WAL_ROTATION_LIMIT {
            wal.current_wal.sync_all()?;
            self.open_next_wal(&mut wal)?;
        }

        if let Err(e) = wal.current_wal.write_all(&frame_bytes[..HEADER_LEN + len]) {
            return Err(self.rotate_past_torn_write(&mut wal, e));
        }

        let offset = wal.current_wal_size;
        let wal_id = wal.current_wal_id;

        wal.current_wal_size += frame_len;
        wal.last_appended_lsn = header.lsn;
        wal.last_appended_term = header.term;
        self.db_next_lsn.fetch_max(header.lsn, Ordering::SeqCst);
        self.db_last_log_term.store(header.term, Ordering::SeqCst);

        self.stage_appended(header.lsn, header.term, &entry, wal_id, offset, payload);

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
    use crate::storage::Retention;
    use crate::storage::frame::MAX_RECORD_SIZE;
    use crate::storage::Database;
    use crate::test_support::{disk_put, live_put, make_frame, temp_root};
    use std::io::Write;
    use std::sync::atomic::Ordering;

    fn ib007_open(path: &std::path::Path) -> Collection {
        use std::sync::atomic::AtomicU64;
        Collection::open("c".into(), path.to_path_buf(),
            Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)), ReadCacheConfig::default(),
            crate::changefeed::ChangefeedConfig::default()).unwrap()
    }

    #[test]
    fn ib007_replaced_snapshot_tail_recovers_and_chains_the_next_append() {
        for replacement_lsn in [2, 100] {
            for rotate in [false, true] {
                for legacy_snapshot in [false, true] {
                    let root = temp_root();
                    let col = ib007_open(&root);
                    col.append_raw_frame(&make_frame(1, 1, 0, 0, "safe", 1)).unwrap();
                    col.sync_wal().unwrap();
                    col.apply_committed(1).unwrap();
                    col.append_raw_frame(&make_frame(1, 100, 1, 1, "old", 100)).unwrap();
                    col.sync_wal().unwrap();
                    col.save_index().unwrap();
                    let snapshot_path = root.join(super::super::index::INDEX_FILENAME);
                    let old_snapshot = fs::read(&snapshot_path).unwrap();
                    if rotate {
                        col.open_next_wal(&mut col.wal_writer.lock().unwrap()).unwrap();
                        col.append_raw_frame(&make_frame(1, 200, 100, 1, "later", 200)).unwrap();
                        col.sync_wal().unwrap();
                    }
                    assert!(matches!(col.append_raw_frame(
                        &make_frame(2, replacement_lsn, 1, 1, "new", 2)).unwrap(),
                        ReplicaApply::Applied { .. }));
                    col.sync_wal().unwrap();
                    assert_eq!(col.last_appended(), (2, replacement_lsn));
                    if legacy_snapshot {
                        // Reconstruct the stale snapshot left by a pre-fix replacement.
                        fs::write(&snapshot_path, &old_snapshot).unwrap();
                    }
                    drop(col);

                    let col = ib007_open(&root);
                    assert_eq!(col.last_appended(), (2, replacement_lsn));
                    assert_eq!(col.durable_lsn.load(Ordering::SeqCst), replacement_lsn);
                    assert_eq!(col.db_next_lsn.load(Ordering::SeqCst), replacement_lsn);
                    assert_eq!(col.db_last_log_term.load(Ordering::SeqCst), 2);
                    assert_eq!(col.applied_lsn(), 1);
                    assert_eq!(col.pending_len(), 1);
                    assert_eq!(col.get("safe").unwrap(), Some(serde_json::json!({"v": 1})));
                    assert!(col.get("new").unwrap().is_none());
                    assert!(!col.exists_including_staged("old"));
                    assert!(!col.exists_including_staged("later"));
                    let (frame, _, _, next) = col.put("next".into(), serde_json::json!({"v": 3}), 3).unwrap();
                    let header = FrameHeader::parse(&frame).unwrap();
                    assert_eq!((header.prev_term, header.prev_lsn), (2, replacement_lsn));
                    assert_eq!(next, replacement_lsn + 1);
                    col.sync_wal().unwrap();
                    col.apply_committed(next).unwrap();
                    drop(col);
                    let col = ib007_open(&root);
                    assert_eq!(col.last_appended(), (3, next));
                    assert_eq!(col.get("new").unwrap(), Some(serde_json::json!({"v": 2})));
                    assert_eq!(col.pending_len(), 0);
                }
            }
        }
    }

    #[test]
    fn ib007_rewind_crash_boundaries_preserve_compacted_and_staged_prefixes() {
        for retain_staged in [false, true] {
            for after_truncation in [false, true] {
                let root = temp_root();
                let col = ib007_open(&root);
                col.put("safe".into(), serde_json::json!({"v": 1}), 1).unwrap();
                let barrier = col.barrier(3).unwrap().3;
                col.sync_wal().unwrap();
                col.apply_committed(barrier).unwrap();
                col.compact(Retention::none()).unwrap();
                col.append_raw_frame(&make_frame(4, 10, barrier, 3, "retained", 10)).unwrap();
                col.sync_wal().unwrap();
                col.open_next_wal(&mut col.wal_writer.lock().unwrap()).unwrap();
                col.append_raw_frame(&make_frame(4, 100, 10, 4, "old", 100)).unwrap();
                col.sync_wal().unwrap();
                col.save_index().unwrap();
                let (term, lsn) = if retain_staged { (4, 10) } else { (3, barrier) };
                if after_truncation {
                    assert!(col.rewind_to(&mut col.wal_writer.lock().unwrap(), lsn, term).unwrap().is_some());
                } else {
                    col.rewind_index_tail(lsn, term).unwrap();
                }
                drop(col);

                let col = ib007_open(&root);
                let expected = if after_truncation { (term, lsn) } else { (4, 100) };
                assert_eq!(col.last_appended(), expected);
                assert_eq!(col.applied_lsn(), barrier);
                assert_eq!(col.get("safe").unwrap(), Some(serde_json::json!({"v": 1})));
                assert_eq!(col.pending_len(), if after_truncation { usize::from(retain_staged) } else { 2 });
                assert_eq!(col.exists_including_staged("old"), !after_truncation);
                assert!(!col.exists("retained"));
            }
        }
    }

    #[test]
    fn ib007_snapshot_reconcile_failure_leaves_the_tail_retryable() {
        let root = temp_root();
        let col = ib007_open(&root);
        col.append_raw_frame(&make_frame(1, 1, 0, 0, "safe", 1)).unwrap();
        col.sync_wal().unwrap();
        col.apply_committed(1).unwrap();
        col.append_raw_frame(&make_frame(1, 100, 1, 1, "old", 100)).unwrap();
        col.sync_wal().unwrap();
        col.save_index().unwrap();
        let snapshot_path = root.join(super::super::index::INDEX_FILENAME);
        let snapshot = fs::read(&snapshot_path).unwrap();
        let wal_path = root.join(format!("wal-{:05}.log", col.wal_writer.lock().unwrap().current_wal_id));
        let wal_bytes = fs::read(&wal_path).unwrap();
        let blocked = root.join(format!("{}.tmp", super::super::index::INDEX_FILENAME));
        fs::create_dir(&blocked).unwrap();
        let replacement = make_frame(2, 2, 1, 1, "new", 2);
        for _ in 0..2 {
            assert!(col.append_raw_frame(&replacement).is_err());
            assert_eq!(col.last_appended(), (1, 100));
            assert_eq!(col.staged_term(100), Some(1));
            assert_eq!(col.pending_len(), 1);
            assert_eq!(col.durable_lsn.load(Ordering::SeqCst), 100);
            assert_eq!(fs::read(&snapshot_path).unwrap(), snapshot);
            assert_eq!(fs::read(&wal_path).unwrap(), wal_bytes);
        }
        fs::remove_dir(&blocked).unwrap();
        assert!(matches!(col.append_raw_frame(&replacement).unwrap(), ReplicaApply::Applied { lsn: 2 }));
        col.sync_wal().unwrap();
        drop(col);
        let col = ib007_open(&root);
        assert_eq!(col.last_appended(), (2, 2));
        assert_eq!(col.pending_len(), 1);
        assert!(!col.exists_including_staged("old"));
    }

    #[test]
    fn ib007_legacy_snapshot_without_a_surviving_tail_is_refused() {
        use std::sync::atomic::AtomicU64;
        let root = temp_root();
        let col = ib007_open(&root);
        col.append_raw_frame(&make_frame(1, 1, 0, 0, "safe", 1)).unwrap();
        col.sync_wal().unwrap();
        col.apply_committed(1).unwrap();
        col.append_raw_frame(&make_frame(1, 100, 1, 1, "old", 100)).unwrap();
        col.sync_wal().unwrap();
        col.save_index().unwrap();
        let snapshot_path = root.join(super::super::index::INDEX_FILENAME);
        let stale = fs::read(&snapshot_path).unwrap();
        col.rewind_to(&mut col.wal_writer.lock().unwrap(), 1, 1).unwrap().unwrap();
        let reconciled = fs::read(&snapshot_path).unwrap();
        drop(col);
        // A pre-fix crash after truncation left only the stale snapshot's claim to LSN 100.
        fs::write(&snapshot_path, &stale).unwrap();
        let error = Collection::open("c".into(), root.to_path_buf(),
            Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)), ReadCacheConfig::default(),
            crate::changefeed::ChangefeedConfig::default()).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(&snapshot_path).unwrap(), stale);
        fs::write(&snapshot_path, &reconciled).unwrap();
        let col = ib007_open(&root);
        assert_eq!(col.last_appended(), (1, 1));
        assert_eq!(col.get("safe").unwrap(), Some(serde_json::json!({"v": 1})));
    }

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
            col.apply_committed(last).unwrap();
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
            col2.apply_committed(lsn).unwrap();
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
            col3.apply_committed(lsn).unwrap();
        }
        assert!(col3.get("key_post_corrupt").unwrap().is_some(), "Writes should continue after recovery");
    }

    /// The half a process crash cannot reach: a machine losing power drops what the page cache
    /// still held, so recovery meets a WAL cut off mid-record. Everything the cut did not reach
    /// has to survive it, and the collection has to go on accepting writes.
    #[tokio::test]
    async fn a_wal_cut_off_mid_frame_keeps_every_record_before_the_cut() {
        let root = temp_root();
        let wal_path;

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("torn").unwrap();
            let mut last = 0;
            for i in 0..50 {
                last = col.put(format!("key:{}", i), serde_json::json!({"n": i}), 1).unwrap().3;
            }
            col.enqueue_commit().await.unwrap().unwrap();
            col.apply_committed(last).unwrap();
            let writer = col.wal_writer.lock().unwrap();
            wal_path = col.root_path.join(format!("wal-{:05}.log", writer.current_wal_id));
        }

        let full = fs::metadata(&wal_path).unwrap().len();
        let cut = OpenOptions::new().write(true).open(&wal_path).unwrap();
        // Mid-record rather than on a boundary: a clean boundary is the easy case and the one a
        // length check already handles.
        cut.set_len(full - 37).unwrap();
        cut.sync_all().unwrap();
        drop(cut);

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("torn").unwrap();

        let survivors = (0..50)
            .filter(|i| col2.get(&format!("key:{}", i)).unwrap().is_some())
            .collect::<Vec<_>>();
        assert!(!survivors.is_empty(), "a cut tail took the whole log with it");
        // Records here are about 100 bytes, so 37 lands inside the last one. Asserted so a change
        // in record size cannot quietly turn this into a test that cuts nothing.
        assert!(survivors.len() < 50, "the cut fell on a record boundary and tested nothing");
        assert_eq!(survivors, (0..survivors.len() as i32).collect::<Vec<_>>(),
            "recovery kept a record written after one it dropped, so the loss was not a tail");

        let lsn = col2.put("after".to_string(), serde_json::json!({"n": -1}), 1).unwrap().3;
        col2.enqueue_commit().await.unwrap().unwrap();
        col2.apply_committed(lsn).unwrap();
        assert!(col2.get("after").unwrap().is_some(), "writes did not resume past a cut tail");
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

        col.compact(Retention::none()).unwrap();
        assert!(!frozen.exists());
        fs::write(&frozen, &orphan).unwrap();

        let frames = col.read_frames_after(0, 3).unwrap();
        let lsns: Vec<u64> = frames.iter().map(|(lsn, _)| *lsn).collect();
        assert_eq!(lsns, vec![1, 2, 3],
            "a frame relocated by compaction must be reported once, not once per surviving copy");
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
        col.compact(Retention::none()).unwrap();

        // The compacted WAL, which is frozen: nothing appends to it, so a short read is a hole.
        let compacted = col.root_path.join(format!("wal-{:05}.log", col.retired_through
            .load(Ordering::SeqCst) + 1));
        let len = compacted.metadata().unwrap().len();
        OpenOptions::new().write(true).open(&compacted).unwrap().set_len(len - 5).unwrap();

        let err = col.read_frames_after(0, 3)
            .expect_err("a scan that cannot read a frozen WAL out must not report a short set");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
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
    }

    #[tokio::test]
    async fn an_appended_frame_is_accounted_for_before_the_append_returns() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        col.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();

        assert_eq!(col.pending_len(), 1,
            "a frame the caller has not staged yet is in neither the index nor pending, and              nothing compaction checks can see it");
        assert!(col.compact(Retention::none()).is_err(),
            "so compaction must refuse rather than retire the WAL holding an in-flight write");
    }

    #[tokio::test]
    async fn a_replicated_frame_is_accounted_for_before_the_apply_returns() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        col.append_raw_frame(&make_frame(1, 1, 0, 0, "k", 1)).unwrap();

        assert_eq!(col.pending_len(), 1, "the replica path closes the same window");
        assert!(col.compact(Retention::none()).is_err());
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
        rcol.apply_committed(3).unwrap();
        drop(rcol);
        drop(rdb);

        let rdb2 = Database::new(&rroot).unwrap();
        let rcol2 = rdb2.get_collection("c").unwrap();
        assert_eq!(rcol2.get("k1").unwrap(), Some(serde_json::json!({"v": 1})));
        assert_eq!(rcol2.get("k2").unwrap(), Some(serde_json::json!({"v": 2})));
        assert_eq!(rcol2.get("k3").unwrap(), Some(serde_json::json!({"v": 3})));
        assert_eq!(rdb2.durable_lsn.load(Ordering::SeqCst), 3, "Replica LSN must match the frames it applied from the primary");
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
        ra.apply_committed(3).unwrap();
        rb.apply_committed(4).unwrap();
        drop(ra);
        drop(rb);
        drop(rdb);

        let rdb2 = Database::new(&rroot).unwrap();
        assert_eq!(rdb2.get_collection("alpha").unwrap().get("k2").unwrap(), Some(serde_json::json!({"v": 3})));
        assert_eq!(rdb2.get_collection("beta").unwrap().get("k2").unwrap(), Some(serde_json::json!({"v": 4})));
    }

    /// Replicates `lsn` frames of term 1, chained, and returns the tail as `(lsn, term)`.
    fn replicate_term_one(col: &Arc<Collection>, through: u64) -> (u64, u64) {
        let mut prev = (0u64, 0u64);
        for lsn in 1..=through {
            let frame = make_frame(1, lsn, prev.0, prev.1, &format!("k{}", lsn), lsn as i64);
            match col.append_raw_frame(&frame).unwrap() {
                ReplicaApply::Applied { .. } => {},
                other => panic!("term-1 replication should apply cleanly, got {:?}", other),
            }
            prev = (lsn, 1);
        }
        prev
    }

    #[tokio::test]
    async fn a_replica_holding_a_superseded_leaders_tail_truncates_it_and_applies() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        replicate_term_one(&col, 2);
        let through_two = col.wal_writer.lock().unwrap().current_wal_size;
        col.append_raw_frame(&make_frame(1, 3, 2, 1, "k3", 3)).unwrap();

        let contested = make_frame(2, 3, 2, 1, "k3", 99);
        match col.append_raw_frame(&contested).unwrap() {
            ReplicaApply::Applied { lsn } => assert_eq!(lsn, 3),
            other => panic!("an uncommitted tail is the leader's to replace, not a snapshot: {:?}", other),
        }

        assert_eq!(col.last_appended(), (2, 3), "the tail is now the entry the leader sent");
        assert_eq!(col.pending_len(), 3, "the superseded frame at lsn 3 must be gone, not shadowed");
        assert_eq!(col.wal_writer.lock().unwrap().current_wal_size, through_two + contested.len() as u64,
            "the frame it replaced must leave the WAL, not sit under the one that replaced it");

        col.enqueue_commit().await.unwrap().unwrap();
        col.apply_committed(3).unwrap();
        assert_eq!(col.get("k3").unwrap(), Some(serde_json::json!({"v": 99})));

        drop(col);
        drop(db);
        let reopened = Database::new(&root).unwrap();
        let col2 = reopened.get_collection("c").unwrap();
        assert_eq!(col2.last_appended(), (2, 3), "and the truncation must survive a restart");
        assert_eq!(col2.get("k3").unwrap(), Some(serde_json::json!({"v": 99})));
    }

    /// IB-013: a truncated frame's inline copy is freed with it, so the reservation must go too --
    /// otherwise a replica that repeatedly loses its tail leaks the budget until nothing inlines.
    #[tokio::test]
    async fn ib013_truncation_returns_the_staged_inline_reservation() {
        let root = temp_root();
        let cache = ReadCacheConfig { inline_max_value_bytes: 512, inline_budget_bytes: 1 << 20 };
        let db = Database::with_config(&root, cache, Default::default()).unwrap();
        let col = db.get_collection("c").unwrap();

        replicate_term_one(&col, 3);
        let three_staged = col.staged_inline.load(Ordering::Relaxed);
        assert!(three_staged > 0, "the replicated frames must be inlined, or this tests nothing");

        // A new leader replaces lsn 2 and everything above it.
        match col.append_raw_frame(&make_frame(2, 2, 1, 1, "k2", 99)).unwrap() {
            ReplicaApply::Applied { lsn } => assert_eq!(lsn, 2),
            other => panic!("an uncommitted tail is the leader's to replace: {:?}", other),
        }
        assert_eq!(col.pending_len(), 2);
        let held: u64 = col.pending.lock().unwrap().values()
            .filter_map(|s| match &s.effect {
                StagedEffect::Put { entry, .. } => Some(entry.inline_bytes()),
                _ => None,
            })
            .sum();
        assert!(held < three_staged, "the truncation must have dropped inlined frames");
        assert_eq!(col.staged_inline.load(Ordering::Relaxed), held,
            "the frames the truncation dropped must give their bytes back");

        col.apply_committed(2).unwrap();
        assert_eq!(col.staged_inline.load(Ordering::Relaxed), 0);
        assert_eq!(col.inline_bytes.load(Ordering::Relaxed),
            col.index.read().unwrap().values().map(|e| e.inline_bytes()).sum::<u64>());
    }

    #[tokio::test]
    async fn a_retransmit_is_a_duplicate_and_never_truncates_what_follows_it() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        replicate_term_one(&col, 3);

        let retransmit = make_frame(1, 2, 1, 1, "k2", 2);
        match col.append_raw_frame(&retransmit).unwrap() {
            ReplicaApply::Duplicate { last_lsn } => assert_eq!(last_lsn, 3),
            other => panic!("expected Duplicate, got {:?}", other),
        }
        assert_eq!(col.last_appended(), (1, 3),
            "reading an ordinary resend as a conflicting log would drop lsn 3 on every retry");
        assert_eq!(col.pending_len(), 3);
    }

    /// The line truncation may not cross. Below the watermark the entries are agreed, and a leader
    /// asking us to drop one is not a leader we can follow -- the snapshot path takes over.
    #[tokio::test]
    async fn a_leader_backing_up_past_the_committed_watermark_is_refused() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        replicate_term_one(&col, 5);
        col.enqueue_commit().await.unwrap().unwrap();
        col.apply_committed(3).unwrap();
        assert_eq!(col.applied_lsn(), 3);

        // A leader whose log runs 1, 2, 4 -- it never held the lsn 3 we have committed.
        let backed_up = make_frame(2, 4, 2, 1, "k4", 99);
        match col.append_raw_frame(&backed_up).unwrap() {
            ReplicaApply::Divergent { last_lsn, last_term } => assert_eq!((last_lsn, last_term), (5, 1),
                "the replica must report its own tail so the primary can snapshot it"),
            other => panic!("committed entries are not the leader's to withdraw: {:?}", other),
        }
        assert_eq!(col.last_appended(), (1, 5), "and nothing may be dropped on the way to refusing");
        assert_eq!(col.pending_len(), 2);
        assert_eq!(col.get("k3").unwrap(), Some(serde_json::json!({"v": 3})));
    }

    /// The conflict is at our tail rather than above it, so there is nothing to truncate to.
    #[tokio::test]
    async fn a_conflict_at_the_tail_itself_asks_the_leader_to_back_up() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        replicate_term_one(&col, 3);

        let next = make_frame(2, 4, 3, 2, "k4", 4);
        match col.append_raw_frame(&next).unwrap() {
            ReplicaApply::Divergent { last_lsn, last_term } => assert_eq!((last_lsn, last_term), (3, 1),
                "our lsn 3 is from a term the leader does not have there"),
            other => panic!("expected Divergent, got {:?}", other),
        }
        assert_eq!(col.last_appended(), (1, 3));
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
    }
}
