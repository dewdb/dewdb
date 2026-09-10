//! Compaction: relocate live keys into a fresh WAL, carry forward the tail a replica still needs,
//! retire the rest. Rests on: every WAL is LSN-ordered, and LSNs rise across WAL ids.

use super::collection::Collection;
use super::frame::{FrameHeader, LogEntry, HEADER_LEN, MAX_RECORD_SIZE};
use super::index::IndexEntry;
use crate::util::remove_file_with_retry;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
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

/// What a compaction run may not destroy, and when it is not worth running. Dropping superseded
/// frames breaks the chain a replica repairs over, so a retention floor keeps what a target needs.
#[derive(Clone, Copy, Debug)]
pub struct Retention {
    /// The lowest position any replication target still needs frames above. 0 protects nothing,
    /// which is what a node with no target to protect wants.
    pub above_lsn: u64,
    /// Ceiling on the retained tail, and so on what one run copies forward. A target below what fits
    /// here is further behind than the tail is worth and snapshots instead.
    pub max_bytes: u64,
    /// A run reclaiming less than this is refused. A pinned tail of superseded frames holds
    /// `dead_ratio` above the scheduler's threshold, so a stuck replica would be rewritten forever.
    pub min_reclaim_bytes: u64,
}

impl Retention {
    /// Nothing downstream to protect: every frozen frame may go, which is what compaction did
    /// before the floor existed.
    pub fn none() -> Self {
        Self { above_lsn: 0, max_bytes: 0, min_reclaim_bytes: 0 }
    }
}

/// A frozen WAL as planning sees it. `first_lsn` is the first frame's, which is the file's lowest
/// under the LSN-order invariant -- and where it is not, it only over-estimates what to keep.
struct FrozenWal {
    id: u64,
    size: u64,
    first_lsn: u64,
}

/// Where the tail a replication target still needs begins, in (wal id, offset) order. Everything
/// from here to the end of the frozen set is copied into the compacted output instead of dropped.
#[derive(Clone, Copy, Debug, PartialEq)]
struct TailStart {
    wal_id: u64,
    offset: u64,
    bytes: u64,
}

impl TailStart {
    /// Whether a frame at this position is inside the tail, and so moves as bytes rather than
    /// being relocated by key.
    fn covers(&self, wal_id: u64, offset: u64) -> bool {
        (wal_id, offset) >= (self.wal_id, self.offset)
    }
}

struct Plan {
    /// `None` retains nothing, which is the whole of what compaction did before M15.
    tail: Option<TailStart>,
    /// Frozen bytes this run would actually free. The scheduler's threshold is on dead bytes,
    /// which counts the pinned tail's superseded frames, so it cannot answer this on its own.
    reclaimable: u64,
}

/// What one compacted WAL turned out to hold: keys rewritten by key, and tail frames that moved
/// by position. Both have to reach the index, and they reach it differently.
struct CompactedWal {
    relocated: Vec<(String, u64, u64, IndexEntry)>,
    moved: HashMap<(u64, u64), u64>,
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

    /// `(offset, lsn)` for every frame in a WAL, in file order, plus where the last valid one ends.
    /// Stops at the first frame that does not parse. Headers only -- the payload is seeked over.
    fn frame_offsets(&self, wal_id: u64) -> io::Result<(Vec<(u64, u64)>, u64)> {
        let path = self.root_path.join(format!("wal-{:05}.log", wal_id));
        let mut file = BufReader::new(File::open(&path)?);
        let mut frames = Vec::new();
        let mut offset = 0u64;

        loop {
            let mut header = [0u8; HEADER_LEN];
            if file.read_exact(&mut header).is_err() {
                break;
            }
            let parsed = match FrameHeader::parse(&header) {
                Some(h) if h.len > 0 && h.len as u64 <= MAX_RECORD_SIZE => h,
                _ => break,
            };
            if file.seek_relative(parsed.len as i64).is_err() {
                break;
            }
            frames.push((offset, parsed.lsn));
            offset += HEADER_LEN as u64 + parsed.len as u64;
        }

        Ok((frames, offset))
    }

    /// Frozen WALs in id order: non-empty, above `retired_through`, each with its first frame's LSN.
    /// `None` if one does not start with a readable frame, where the only safe plan is to keep nothing.
    fn frozen_wals(&self, frozen_through: u64) -> io::Result<Option<Vec<FrozenWal>>> {
        let retired = self.retired_through.load(Ordering::SeqCst);
        let mut wals = Vec::new();

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
            let id = match name[4..name.len() - 4].parse::<u64>() {
                Ok(id) if id > retired && id <= frozen_through => id,
                _ => continue,
            };
            let size = entry.metadata()?.len();
            // An empty WAL holds nothing to keep and nothing to reclaim, and would otherwise stand
            // between two files as a first LSN nobody can read.
            if size == 0 {
                continue;
            }

            let mut header = [0u8; HEADER_LEN];
            let mut file = File::open(entry.path())?;
            match file.read_exact(&mut header).ok().and_then(|_| FrameHeader::parse(&header)) {
                Some(h) => wals.push(FrozenWal { id, size, first_lsn: h.lsn }),
                None => {
                    warn!(target: "compaction", collection = %self.name, file = %name,
                        "WAL does not start with a readable frame; retaining nothing this run");
                    return Ok(None);
                }
            }
        }

        wals.sort_by_key(|w| w.id);
        Ok(Some(wals))
    }

    /// What to keep, and whether keeping it leaves enough to be worth the rewrite. Runs before the
    /// rotation, so a refusal costs nothing; bailing after would leak a WAL id per interval.
    fn plan_compaction(
        &self,
        frozen_through: u64,
        last_lsn: u64,
        retention: &Retention,
    ) -> io::Result<Plan> {
        let wals = match self.frozen_wals(frozen_through)? {
            Some(w) if !w.is_empty() => w,
            _ => return Ok(Plan { tail: None, reclaimable: 0 }),
        };
        let frozen_bytes: u64 = wals.iter().map(|w| w.size).sum();

        let tail = self.plan_tail(&wals, last_lsn, retention)?;
        let retained = |e: &&IndexEntry| tail.is_some_and(|t| t.covers(e.wal_id, e.offset));

        // Live frames outside the tail are rewritten rather than freed, and the tail moves whole.
        let kept: u64 = self.index.read().unwrap().values()
            .filter(|e| e.wal_id <= frozen_through && !retained(e))
            .map(|e| e.frame_bytes())
            .sum();
        let reclaimable = frozen_bytes
            .saturating_sub(kept)
            .saturating_sub(tail.map_or(0, |t| t.bytes));

        Ok(Plan { tail, reclaimable })
    }

    /// The start of the frames above `retention.above_lsn`, bounded by `retention.max_bytes`. Taken as
    /// a minimum offset, not a first match, so an unordered WAL costs retention, not a dropped frame.
    fn plan_tail(
        &self,
        wals: &[FrozenWal],
        last_lsn: u64,
        retention: &Retention,
    ) -> io::Result<Option<TailStart>> {
        if retention.max_bytes == 0 {
            return Ok(None);
        }

        // Anchored past the last frozen byte rather than given up on: planning runs before the
        // rotation, so a frame committed after it still needs the tail to reach the frozen end.
        let last = wals.last().expect("planning returns early on an empty frozen set");
        let end = TailStart { wal_id: last.id, offset: last.size, bytes: 0 };
        if retention.above_lsn >= last_lsn {
            return Ok(Some(end));
        }

        let keep = match (0..wals.len()).find(|i| {
            wals.get(i + 1).map_or(last_lsn + 1, |next| next.first_lsn) > retention.above_lsn
        }) {
            Some(k) => k,
            None => return Ok(Some(end)),
        };

        let after: u64 = wals[keep + 1..].iter().map(|w| w.size).sum();
        let (frames, valid_end) = self.frame_offsets(wals[keep].id)?;
        let offset = frames.iter()
            .filter(|(_, lsn)| *lsn > retention.above_lsn)
            .map(|(off, _)| *off)
            .min()
            .unwrap_or(valid_end);

        let mut start = TailStart {
            wal_id: wals[keep].id,
            offset,
            bytes: wals[keep].size.saturating_sub(offset) + after,
        };

        // Over budget: give up the oldest of it, whole files first and then frames, until what is
        // left fits. The last WAL always fits on its own, since nothing follows it to count.
        if start.bytes > retention.max_bytes {
            for (i, wal) in wals.iter().enumerate().skip(keep) {
                let after: u64 = wals[i + 1..].iter().map(|w| w.size).sum();
                if after > retention.max_bytes {
                    continue;
                }
                let want = wal.size.saturating_sub(retention.max_bytes - after);
                let (frames, valid_end) = self.frame_offsets(wal.id)?;
                let offset = frames.iter()
                    .map(|(off, _)| *off)
                    .find(|off| *off >= want)
                    .unwrap_or(valid_end);
                start = TailStart {
                    wal_id: wal.id,
                    offset,
                    bytes: wal.size.saturating_sub(offset) + after,
                };
                break;
            }
        }

        Ok(Some(start))
    }

    // Frozen WALs are <= N, rewritten output N+1, new active N+2; only the writer swap takes the lock.
    // The output carries the tail `retention` pins; everything below it is dropped and breaks the chain.
    pub fn compact(&self, retention: Retention) -> io::Result<()> {
        if self.compacting.swap(true, Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Compaction already in progress"));
        }
        let _guard = CompactionGuard { flag: &self.compacting };
        if self.released.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Collection handle is no longer active"));
        }
        let _snapshot_boundary = self.snapshot_boundary.try_lock()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "Snapshot transfer in progress"))?;
        // Uncommitted frames are absent from the index; retiring their WAL would lose them.
        if self.pending_len() > 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Uncommitted frames pending"));
        }

        // Before anything is retired: ordinary commits record only their position, so a drop, config
        // or handover frame this run removes must already be in `applied.meta` to be re-derivable.
        self.flush_watermark_full()?;

        let (planned_through, last_lsn) = {
            let wal = self.wal_writer.lock().unwrap();
            (wal.current_wal_id, wal.last_appended_lsn)
        };
        let plan = self.plan_compaction(planned_through, last_lsn, &retention)?;
        if plan.reclaimable < retention.min_reclaim_bytes {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, format!(
                "{} reclaimable bytes is below the {} a rewrite has to pay back",
                plan.reclaimable, retention.min_reclaim_bytes)));
        }

        let (frozen_index, frozen_through, compact_id) = {
            let mut wal = self.wal_writer.lock().unwrap();

            // Re-checked under the append lock, which is what makes it authoritative: appends stage
            // while holding it, so nothing can land in the WAL about to be frozen after this point.
            if self.pending_len() > 0 {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "Uncommitted frames pending"));
            }
            // Re-checked here too so a release cannot land between the entry check and the
            // rotation, which would leave a WAL id behind in a directory about to be replaced.
            if self.released.load(Ordering::SeqCst) {
                return Err(io::Error::new(io::ErrorKind::NotFound, "Collection handle is no longer active"));
            }

            // The plan named the WALs it may destroy and where the retained tail starts in them.
            // A rotation since then means it named the wrong set.
            if wal.current_wal_id != planned_through {
                return Err(io::Error::new(io::ErrorKind::WouldBlock,
                    "The WAL rotated while this compaction was being planned"));
            }

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

            // Keys inside the retained tail are left out: their frames move with it, as bytes,
            // and relocating them as well would write each of them twice.
            let mut frozen: Vec<(String, u64, u64)> = self.index.read().unwrap()
                .iter()
                .filter(|(_, e)| e.wal_id <= frozen_through
                    && !plan.tail.is_some_and(|t| t.covers(e.wal_id, e.offset)))
                .map(|(k, e)| (k.clone(), e.wal_id, e.offset))
                .collect();
            // Source order, which is LSN order under this file's two invariants, so the output is
            // LSN-ordered too and the next run finds its retention boundary in one walk.
            frozen.sort_by_key(|(_, wal_id, offset)| (*wal_id, *offset));

            (frozen, frozen_through, compact_id)
        };

        info!(target: "compaction", collection = %self.name, live_keys = frozen_index.len(),
            frozen_through, compact_wal = compact_id, active_wal = frozen_through + 2,
            retained_bytes = plan.tail.map_or(0, |t| t.bytes), reclaimable = plan.reclaimable,
            "Compaction started");

        let compact_path = self.root_path.join("wal-compacted.tmp");
        let written = match self.write_compacted_wal(
            &compact_path, &frozen_index, compact_id, plan.tail, frozen_through)
        {
            Ok(w) => w,
            Err(e) => {
                let _ = fs::remove_file(&compact_path);
                return Err(e);
            }
        };

        // Every step below changes what is on disk, and an install can swap the directory out from
        // under them: `rewriting` holds it still, `released` says it is already gone.
        let _rewriting = self.rewriting.lock()
            .map_err(|_| io::Error::other("collection rewrite lock is poisoned"))?;
        if self.released.load(Ordering::SeqCst) {
            let _ = fs::remove_file(&compact_path);
            return Err(io::Error::new(io::ErrorKind::NotFound, "Collection handle is no longer active"));
        }

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
            for (key, old_wal_id, old_offset, new_entry) in written.relocated {
                let unchanged = index.get(&key)
                    .map_or(false, |cur| cur.wal_id == old_wal_id && cur.offset == old_offset);
                if unchanged {
                    self.apply_index_put(&mut index, key, new_entry);
                    remapped += 1;
                } else {
                    superseded += 1;
                }
            }

            // The tail moved as bytes, so its live keys move by position rather than by key. No entry
            // is caught by both loops: relocated ones point at `compact_id`, which no source does.
            for entry in index.values_mut() {
                if let Some(offset) = written.moved.get(&(entry.wal_id, entry.offset)) {
                    entry.wal_id = compact_id;
                    entry.offset = *offset;
                    remapped += 1;
                }
            }

            // Published under the index lock: past this point no reader can resolve a key into a
            // frozen WAL, so only handles taken before it are still outstanding.
            if retire {
                self.retired_through.fetch_max(frozen_through, Ordering::SeqCst);
            }
        }

        // The pivot. Until it lands the previous snapshot plus the intact frozen WALs still describe
        // the collection; after it, a failed unlink is wasted disk rather than a resurrected key.
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

    /// A tail frame that moved, keyed by where it was so the index can be remapped without
    /// parsing it: only its position changed, and the index already holds its length.
    fn copy_retained_tail(
        &self,
        out: &mut BufWriter<File>,
        readers: &mut HashMap<u64, File>,
        tail: TailStart,
        frozen_through: u64,
        start_offset: u64,
    ) -> io::Result<(HashMap<(u64, u64), u64>, u64)> {
        let mut moved = HashMap::new();
        let mut out_offset = start_offset;

        // A frame that does not parse ends the tail rather than the file: what a replica can use
        // is a contiguous run of LSNs, and carrying the frames past a hole forward buys it nothing.
        'files: for wal_id in tail.wal_id..=frozen_through {
            let from = if wal_id == tail.wal_id { tail.offset } else { 0 };
            let path = self.root_path.join(format!("wal-{:05}.log", wal_id));
            let file = match readers.entry(wal_id) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => match File::open(&path) {
                    Ok(f) => e.insert(f),
                    // A gap in the ids is ordinary: a rotation can skip one, and an id below the
                    // tail start is not in this range at all.
                    Err(ref err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                },
            };
            file.seek(SeekFrom::Start(from))?;

            let mut offset = from;
            loop {
                let mut header = [0u8; HEADER_LEN];
                if file.read_exact(&mut header).is_err() {
                    break;
                }
                let parsed = match FrameHeader::parse(&header) {
                    Some(h) if h.len > 0 && h.len as u64 <= MAX_RECORD_SIZE => h,
                    _ => break 'files,
                };
                let mut payload = vec![0u8; parsed.len as usize];
                if file.read_exact(&mut payload).is_err() || !parsed.payload_valid(&payload) {
                    break 'files;
                }

                out.write_all(&header)?;
                out.write_all(&payload)?;
                moved.insert((wal_id, offset), out_offset);
                let frame = HEADER_LEN as u64 + parsed.len as u64;
                offset += frame;
                out_offset += frame;
            }
        }

        Ok((moved, out_offset))
    }

    fn write_compacted_wal(
        &self,
        compact_path: &Path,
        frozen_index: &[(String, u64, u64)],
        compact_id: u64,
        tail: Option<TailStart>,
        frozen_through: u64,
    ) -> io::Result<CompactedWal> {
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

        // After the relocated frames, which are all below the tail: the file stays LSN-ordered,
        // and a replay in offset order still ends on the newest version of every key.
        let moved = match tail {
            Some(t) => {
                let (moved, _) = self.copy_retained_tail(
                    &mut compact_file, &mut readers, t, frozen_through, current_offset)?;
                moved
            },
            None => HashMap::new(),
        };

        compact_file.flush()?;
        compact_file.get_mut().sync_all()?;
        Ok(CompactedWal { relocated, moved })
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
    use super::super::index::{AppliedMeta, IndexSnapshot, INDEX_FILENAME};
    use crate::storage::Database;
    use crate::test_support::{disk_put, live_put, stage_delete, temp_root};
    use std::path::Path;
    use std::sync::Arc;

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

    fn keep_all(above_lsn: u64) -> Retention {
        Retention { above_lsn, max_bytes: u64::MAX, min_reclaim_bytes: 0 }
    }

    /// Surviving LSNs, lowest first. What a repair can chain over after a run.
    fn surviving_lsns(col: &Arc<Collection>, up_to: u64) -> Vec<u64> {
        col.read_frames_after(0, up_to).unwrap().iter().map(|(l, _)| *l).collect()
    }

    /// The invariant retention rests on: the boundary between "below the floor" and "above it" is
    /// one offset in the output, found by a single walk. Key-order output would scatter it.
    #[tokio::test]
    async fn the_compacted_wal_comes_out_lsn_ordered() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        // Keys whose alphabetical order is the reverse of the order they were written in.
        for (i, key) in ["e", "d", "c", "b", "a"].iter().enumerate() {
            live_put(&col, key, i as i64);
        }
        col.enqueue_commit().await.unwrap().unwrap();
        let tip = db.durable_lsn.load(Ordering::SeqCst);

        col.compact(Retention::none()).unwrap();

        let compacted = col.retired_through.load(Ordering::SeqCst) + 1;
        let (frames, _) = col.frame_offsets(compacted).unwrap();
        let lsns: Vec<u64> = frames.iter().map(|(_, lsn)| *lsn).collect();
        let mut sorted = lsns.clone();
        sorted.sort();
        assert_eq!(lsns, sorted, "the output must rise in LSN whatever order the keys are in");
        assert_eq!(lsns.len() as u64, tip);
    }

    /// The tail moves as bytes, so the keys inside it move by position rather than by key. A
    /// `disk_put` value is too large to inline, so the read has to resolve through that remap.
    #[tokio::test]
    async fn a_key_whose_frame_moved_with_the_tail_is_still_readable() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        disk_put(&col, "old", "o");
        col.enqueue_commit().await.unwrap().unwrap();
        let below = db.durable_lsn.load(Ordering::SeqCst);
        disk_put(&col, "new", "n");
        col.enqueue_commit().await.unwrap().unwrap();

        let before = col.index.read().unwrap()["new"].clone();
        col.compact(keep_all(below)).unwrap();
        let after = col.index.read().unwrap()["new"].clone();

        assert_ne!((before.wal_id, before.offset), (after.wal_id, after.offset),
            "the frame moved into the compacted WAL, so the index must have moved with it");
        assert_eq!(after.len, before.len, "and only its position changed");
        assert!(col.get("new").unwrap().unwrap()["v"].as_str().unwrap().starts_with("n"),
            "a value too large to inline is read back through the remapped position or not at all");
        assert!(col.get("old").unwrap().unwrap()["v"].as_str().unwrap().starts_with("o"),
            "and a key relocated by key still reads too");
    }

    /// Relocation only carries live keys, which are all `Put`s. Everything else a replica needs to
    /// replay -- a delete, a barrier, a config -- survives only because the tail is copied whole.
    #[tokio::test]
    async fn a_delete_inside_the_retained_tail_survives_the_rewrite() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "a", 1);
        live_put(&col, "b", 2);
        col.enqueue_commit().await.unwrap().unwrap();
        let below = db.durable_lsn.load(Ordering::SeqCst);

        let del = stage_delete(&col, "a");
        col.apply_committed(del).unwrap();
        col.enqueue_commit().await.unwrap().unwrap();

        col.compact(keep_all(below)).unwrap();

        assert!(surviving_lsns(&col, del).contains(&del),
            "the delete is in no index, so nothing but the tail copy can carry it");
        assert!(col.get("a").unwrap().is_none(), "and it still reads as deleted");

        col.compact(Retention::none()).unwrap();
        assert!(!surviving_lsns(&col, del).contains(&del),
            "with nothing to protect it goes, which is what it did before retention");
    }

    /// The bound the entry calls for: past it a replica is further behind than the tail is worth
    /// and snapshots, which it would have had to do anyway.
    #[tokio::test]
    async fn the_retained_tail_is_bounded_by_its_budget() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();

        let churn = |col: &Arc<Collection>| {
            for v in 1..=20 {
                live_put(col, "a", v);
            }
        };

        let unbounded = db.get_collection("unbounded").unwrap();
        churn(&unbounded);
        unbounded.enqueue_commit().await.unwrap().unwrap();
        let tip = unbounded.last_appended_lsn();
        let total = unbounded.space_usage().unwrap().total_bytes;
        unbounded.compact(keep_all(0)).unwrap();
        assert_eq!(surviving_lsns(&unbounded, tip).len(), 20,
            "an unbounded floor at 0 pins the whole log");

        let bounded = db.get_collection("bounded").unwrap();
        churn(&bounded);
        bounded.enqueue_commit().await.unwrap().unwrap();
        let tip = bounded.last_appended_lsn();
        bounded.compact(Retention { above_lsn: 0, max_bytes: total / 4, min_reclaim_bytes: 0 })
            .unwrap();

        let kept = surviving_lsns(&bounded, tip);
        assert!(kept.len() > 1 && kept.len() < 20,
            "a quarter of the log is neither none of it nor all of it, got {:?}", kept);
        assert_eq!(*kept.last().unwrap(), tip, "and what it keeps is the newest end");
        assert_eq!(kept, (kept[0]..=tip).collect::<Vec<_>>(), "contiguously, or it chains nothing");
    }

    /// Without this the tail is its own reason to be rewritten: its superseded frames are dead
    /// bytes, so a replica stuck at one position holds `dead_ratio` over the threshold forever.
    #[tokio::test]
    async fn a_run_that_cannot_pay_for_itself_is_refused_before_it_rotates() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for key in ["a", "b", "c"] {
            live_put(&col, key, 1);
        }
        col.enqueue_commit().await.unwrap().unwrap();

        let wal_before = col.wal_writer.lock().unwrap().current_wal_id;
        let err = col.compact(Retention { above_lsn: 0, max_bytes: u64::MAX,
            min_reclaim_bytes: 1 << 20 }).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(col.wal_writer.lock().unwrap().current_wal_id, wal_before,
            "a refusal must not rotate: one extra WAL id per scheduler interval is a leak");
        assert_eq!(wal_ids_on_disk(&col.root_path).len(), 1);
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

        col.compact(Retention::none()).unwrap();

        let reclaimed = col.space_usage().unwrap();
        assert_eq!(reclaimed.live_keys, 1);
        assert_eq!(reclaimed.dead_bytes(), 0, "compaction must reclaim every dead byte");
        assert!(reclaimed.total_bytes < churned.total_bytes);
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
        col.compact(Retention::none()).unwrap();

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
    }

    #[tokio::test]
    async fn compaction_rejects_a_second_concurrent_run() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        live_put(&col, "a", 1);

        col.compacting.store(true, Ordering::SeqCst);
        let err = col.compact(Retention::none()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "overlapping compactions must be refused, not interleaved");

        col.compacting.store(false, Ordering::SeqCst);
        col.compact(Retention::none()).unwrap();
        assert!(!col.compacting.load(Ordering::SeqCst), "the in-progress flag must clear when compaction finishes");
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

        col.compact(Retention::none()).unwrap();
        writer.join().unwrap();

        for i in 0..200 {
            assert_eq!(col.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": 1})),
                "a write racing compaction must survive it");
        }
        for i in 200..3000 {
            assert_eq!(col.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": 0})),
                "untouched keys must survive relocation");
        }
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
            col.compact(Retention::none()).unwrap();
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
    }

    /// The retained tail sits above the relocated keys in one file, so replay in offset order still
    /// ends on the newest version of everything. Asserted from the index snapshot and without it.
    #[tokio::test]
    async fn a_compaction_that_retained_a_tail_replays_to_the_same_state() {
        let root = temp_root();

        let expected = {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();

            live_put(&col, "a", 1);
            live_put(&col, "gone", 7);
            live_put(&col, "a", 2);
            col.enqueue_commit().await.unwrap().unwrap();
            let below = col.last_appended_lsn();

            live_put(&col, "a", 3);
            let del = stage_delete(&col, "gone");
            col.apply_committed(del).unwrap();
            live_put(&col, "b", 9);
            col.enqueue_commit().await.unwrap().unwrap();

            col.compact(keep_all(below)).unwrap();
            assert!(surviving_lsns(&col, col.last_appended_lsn()).contains(&del),
                "the delete has to be in the tail for this test to be about anything");

            live_put(&col, "b", 10);
            col.enqueue_commit().await.unwrap().unwrap();
            col.index.read().unwrap().len()
        };

        let state = |db: &Database| {
            let col = db.get_collection("c").unwrap();
            (col.get("a").unwrap(), col.get("b").unwrap(), col.get("gone").unwrap(),
             col.index.read().unwrap().len())
        };
        let want = (Some(serde_json::json!({"v": 3})), Some(serde_json::json!({"v": 10})),
            None, expected);

        assert_eq!(state(&Database::new(&root).unwrap()), want);

        fs::remove_file(root.join("c").join(INDEX_FILENAME)).unwrap();
        assert_eq!(state(&Database::new(&root).unwrap()), want,
            "and with no snapshot to lean on, replaying the WALs in id order gives the same answer");
    }

    #[tokio::test]
    async fn compaction_drops_deleted_keys_and_keeps_them_deleted_after_restart() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            live_put(&col, "keep", 1);
            live_put(&col, "gone", 2);

            let lsn = col.delete("gone".into(), 1).unwrap().3;
            col.enqueue_commit().await.unwrap().unwrap();
            col.apply_committed(lsn).unwrap();

            col.compact(Retention::none()).unwrap();

            assert!(col.get("gone").unwrap().is_none());
        }

        let db2 = Database::new(&root).unwrap();
        let col2 = db2.get_collection("c").unwrap();
        assert_eq!(col2.get("keep").unwrap(), Some(serde_json::json!({"v": 1})));
        assert!(col2.get("gone").unwrap().is_none(), "a tombstoned key must not come back after compaction + replay");
    }

    /// M9: the drop frame is not in the index, so compaction retires it with everything else the
    /// empty index no longer points at. The applied watermark is what carries the drop past that.
    #[tokio::test]
    async fn a_drop_survives_the_compaction_that_retires_its_own_frame() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            live_put(&col, "a", 1);
            live_put(&col, "b", 2);

            let lsn = col.drop_marker(1).unwrap().3;
            col.enqueue_commit().await.unwrap().unwrap();
            col.apply_committed(lsn).unwrap();

            col.compact(Retention::none()).unwrap();
            assert!(col.is_dropped());
        }

        let db2 = Database::new(&root).unwrap();
        assert!(db2.is_dropped("c"), "a compacted tombstone must not come back as a live collection");
        let col2 = db2.get_collection("c").unwrap();
        assert!(col2.is_dropped());
        assert!(col2.get("a").unwrap().is_none());
        assert!(db2.live_collections().unwrap().is_empty());
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
            col.compact(Retention::none()).unwrap();

            // The surviving copy of the Put, as a failed unlink would leave it behind.
            let put_wal = col_dir.join(format!("wal-{:05}.log", wal_ids_on_disk(&col_dir)[0]));
            let orphan = (put_wal.clone(), fs::read(&put_wal).unwrap());

            let lsn = col.delete("gone".into(), 1).unwrap().3;
            col.enqueue_commit().await.unwrap().unwrap();
            col.apply_committed(lsn).unwrap();
            col.compact(Retention::none()).unwrap();

            assert!(!orphan.0.exists(), "the second compaction must have retired the first output");
            fs::write(&orphan.0, &orphan.1).unwrap();
        }

        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        assert_eq!(col.get("keep").unwrap(), Some(serde_json::json!({"v": 1})));
        assert!(col.get("gone").unwrap().is_none(),
            "a WAL compaction failed to unlink must not put a tombstoned key back");
    }

    /// The flag only covers a release landing before compaction's last check. The lock covers one
    /// landing after it, while the rename, the remap and the index snapshot are still in flight.
    #[tokio::test]
    async fn a_release_waits_for_a_compaction_that_is_already_publishing() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        live_put(&col, "a", 1);

        let publishing = col.rewriting.lock().unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let releasing = col.clone();
        let thread = std::thread::spawn(move || {
            let _ = tx.send(releasing.release_handles().is_ok());
        });

        assert!(rx.recv_timeout(std::time::Duration::from_millis(300)).is_err(),
            "a release that proceeds here installs over files compaction is still writing");
        assert!(!col.released.load(Ordering::SeqCst), "and it must not have flagged the handle yet");

        drop(publishing);
        assert!(rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),
            "once the publish is done the release must go through");
        thread.join().unwrap();
    }

    #[tokio::test]
    async fn compaction_leaves_a_snapshot_that_resumes_past_the_wals_it_retired() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "k", 1);
        col.enqueue_commit().await.unwrap().unwrap();

        let frozen_through = col.wal_writer.lock().unwrap().current_wal_id;
        col.compact(Retention::none()).unwrap();

        let file = File::open(col.root_path.join(INDEX_FILENAME))
            .expect("compaction must leave a snapshot, not unlink the one it had");
        let snapshot: IndexSnapshot = bincode::deserialize_from(file).unwrap();
        assert!(snapshot.last_wal_id > frozen_through,
            "boot must resume above the retired WALs, or it replays whatever survived them");
    }

    /// The two recovery inputs meeting: compaction retires the WALs a crash would have replayed, so
    /// boot reads the snapshot plus what came after. Both halves present, newer value wins.
    #[tokio::test]
    async fn a_value_survives_a_compaction_and_the_crash_that_follows_it() {
        let root = temp_root();

        {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("c").unwrap();
            for i in 0..40 {
                live_put(&col, &format!("k{}", i % 10), i);
            }
            col.enqueue_commit().await.unwrap().unwrap();
            col.compact(Retention::none()).unwrap();

            // Written after the compaction, so it lives only in the WAL the snapshot does not cover.
            live_put(&col, "after", 99);
            live_put(&col, "k3", 500);
            col.enqueue_commit().await.unwrap().unwrap();
        }

        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();
        for i in 0..10 {
            assert!(col.get(&format!("k{}", i)).unwrap().is_some(),
                "k{} was compacted away rather than into the snapshot", i);
        }
        assert_eq!(col.get("after").unwrap().and_then(|v| v.get("v").and_then(|n| n.as_i64())),
            Some(99), "a write after the compaction did not survive the crash");
        assert_eq!(col.get("k3").unwrap().and_then(|v| v.get("v").and_then(|n| n.as_i64())),
            Some(500), "recovery preferred the snapshot's value over the newer WAL frame");
    }

    /// C27's state: once the drop's frame is retired, `applied.meta` is the only record that this
    /// collection is a tombstone, so the watermark must cover the drop before compaction retires it.
    #[tokio::test]
    async fn compaction_flushes_the_watermark_before_retiring_the_frames_behind_it() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "a", 1);
        let dropped_at = col.drop_marker(1).unwrap().3;
        col.apply_committed(dropped_at).unwrap();
        assert!(col.is_dropped());

        col.compact(Retention::none()).unwrap();
        assert_eq!(AppliedMeta::load(&col.root_path).unwrap().unwrap().applied_lsn, dropped_at,
            "the watermark must cover the drop before its frame is retired");

        drop(col);
        db.release_collection("c").unwrap();
        let fresh = db.get_collection("c").unwrap();
        assert!(fresh.is_dropped(), "compaction retired the drop frame without persisting the drop");
        assert!(fresh.index.read().unwrap().is_empty());
    }
}
