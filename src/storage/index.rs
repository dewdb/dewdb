//! Index entries, their persisted snapshot, and the commit watermark file.

use super::frame::{Configuration, HandoverRecord, HEADER_LEN};
use super::secondary::IndexSpec;
use crate::util::write_atomic;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

pub const INDEX_FILENAME: &str = "index-current.bin";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    pub wal_id: u64,
    pub offset: u64,
    pub len: u32,
    #[serde(default)]
    pub inline: Option<Box<[u8]>>,
}

impl IndexEntry {
    pub fn frame_bytes(&self) -> u64 {
        HEADER_LEN as u64 + self.len as u64
    }

    pub fn inline_bytes(&self) -> u64 {
        self.inline.as_ref().map_or(0, |b| b.len() as u64)
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ReadCacheConfig {
    #[serde(default = "default_inline_max_bytes")]
    pub inline_max_value_bytes: u32,
    #[serde(default = "default_inline_budget")]
    pub inline_budget_bytes: u64,
}

fn default_inline_max_bytes() -> u32 { 512 }
fn default_inline_budget() -> u64 { 64 * 1024 * 1024 }

impl Default for ReadCacheConfig {
    fn default() -> Self {
        Self {
            inline_max_value_bytes: default_inline_max_bytes(),
            inline_budget_bytes: default_inline_budget(),
        }
    }
}

#[derive(Serialize, Deserialize)]
// Compatibility: bincode carries no version tag, so a failed decode means "no snapshot" and a full replay.
pub struct IndexSnapshot {
    pub last_wal_id: u64,
    pub last_offset: u64,
    pub last_lsn: u64,
    #[serde(default)]
    pub last_term: u64,
    pub map: BTreeMap<String, IndexEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct LsnMeta {
    pub commit_lsn: u64,
}

// How far the index reflects the log. Boot replays everything but publishes only to here;
// anything above was never committed and must stay staged across the restart.
#[derive(Serialize, Deserialize)]
pub struct AppliedMeta {
    pub applied_lsn: u64,
    /// Set by a committed `Drop`, cleared by any keyed entry above it. Survives compaction, which
    /// retires the drop frame along with everything else the empty index no longer points at.
    #[serde(default)]
    pub dropped: bool,
    /// The newest committed `Config`. Here for the same reason `dropped` is: a configuration entry
    /// is never in the index, so compaction retires its frame and replay cannot find it again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Configuration>,
    /// The newest committed `Handover`, kept for the same reason as `config`. One at a time: a
    /// migration is cluster-wide, so a later plan replaces an earlier one rather than joining it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handover: Option<HandoverRecord>,
    /// The committed secondary index definitions, here because an `Index` entry is never in the key
    /// index and compaction retires its frame. Definitions only -- postings rebuild on open.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<IndexSpec>,
}

pub const APPLIED_FILENAME: &str = "applied.meta";

impl AppliedMeta {
    /// `Ok(None)` is "no consensus history", which the caller replays in full. A file that exists but
    /// does not parse is an error, never `None`: reading damage as absence republishes the whole log.
    pub fn load(col_dir: &Path) -> io::Result<Option<Self>> {
        let path = col_dir.join(APPLIED_FILENAME);
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        serde_json::from_str(&content)
            .map(Some)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!(
                "{} is unreadable ({}); it records which of this collection's durable frames are committed", path.display(), e)))
    }

    pub fn save(&self, col_dir: &Path) -> io::Result<()> {
        let content = serde_json::to_string(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        write_atomic(col_dir, APPLIED_FILENAME, content.as_bytes())
    }
}

/// The commit position alone, updated in place: rewriting `AppliedMeta` per commit costs a create plus
/// an fsync. Two sector-sized slots written alternately, newest sequence wins, so a tear is local.
pub struct AppliedPos {
    file: fs::File,
    seq: u64,
}

const POS_FILENAME: &str = "applied.pos";
const POS_MAGIC: u32 = 0x4450_4F53;
const POS_SLOT: u64 = 512;
const POS_RECORD: usize = 24;

fn pos_encode(seq: u64, applied_lsn: u64) -> [u8; POS_RECORD] {
    let mut out = [0u8; POS_RECORD];
    out[0..4].copy_from_slice(&POS_MAGIC.to_le_bytes());
    out[4..12].copy_from_slice(&seq.to_le_bytes());
    out[12..20].copy_from_slice(&applied_lsn.to_le_bytes());
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&out[0..20]);
    out[20..24].copy_from_slice(&hasher.finalize().to_le_bytes());
    out
}

// Reading damage as absence can lower the recovered watermark.
enum Slot {
    Empty,
    Damaged,
    Written { seq: u64, applied_lsn: u64 },
}

fn pos_decode(bytes: &[u8]) -> Slot {
    if bytes.len() != POS_SLOT as usize {
        return Slot::Damaged;
    }
    if bytes.iter().all(|byte| *byte == 0) {
        return Slot::Empty;
    }
    if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != POS_MAGIC {
        return Slot::Damaged;
    }
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[0..20]);
    if hasher.finalize() != u32::from_le_bytes(bytes[20..24].try_into().unwrap()) {
        return Slot::Damaged;
    }
    Slot::Written {
        seq: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
        applied_lsn: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
    }
}

impl AppliedPos {
    /// Pre-allocated here, once, so no later `save` changes the file's size and every one of them
    /// is a data flush into blocks that already exist.
    pub fn open(col_dir: &Path) -> io::Result<Self> {
        let path = col_dir.join(POS_FILENAME);
        let file = match fs::OpenOptions::new().create_new(true).read(true).write(true).open(&path) {
            Ok(file) => {
                file.set_len(POS_SLOT * 2)?;
                file.sync_all()?;
                file
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists =>
                fs::OpenOptions::new().read(true).write(true).open(&path)?,
            Err(e) => return Err(e),
        };
        let seq = Self::newest(&file)?.map_or(0, |(seq, _)| seq);
        Ok(Self { file, seq })
    }

    // A torn record can leave a valid peer slot; an unexpected file size cannot prove that.
    fn newest(file: &fs::File) -> io::Result<Option<(u64, u64)>> {
        use std::io::{Read, Seek};
        let len = file.metadata()?.len();
        if len != POS_SLOT * 2 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("{} has unexpected length {}; expected {} bytes", POS_FILENAME, len, POS_SLOT * 2)));
        }
        let mut handle = file.try_clone()?;
        handle.seek(io::SeekFrom::Start(0))?;
        let mut buf = vec![0u8; (POS_SLOT * 2) as usize];
        let mut filled = 0;
        while filled < buf.len() {
            match handle.read(&mut buf[filled..]) {
                Ok(0) => return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("{} was truncated while reading its slots", POS_FILENAME))),
                Ok(n) => filled += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {},
                Err(e) => return Err(e),
            }
        }

        let slots: Vec<Slot> = buf.chunks_exact(POS_SLOT as usize)
            .map(pos_decode)
            .collect();

        let newest = slots.iter()
            .filter_map(|s| match s {
                Slot::Written { seq, applied_lsn } => Some((*seq, *applied_lsn)),
                _ => None,
            })
            .max_by_key(|(seq, _)| *seq);

        match newest {
            Some(found) => Ok(Some(found)),
            None if slots.iter().any(|s| matches!(s, Slot::Damaged)) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} has no readable slot; it records which of this collection's durable frames are committed", POS_FILENAME))),
            None => Ok(None),
        }
    }

    /// The position this collection last recorded. `Ok(None)` is "nothing recorded here yet",
    /// which includes the file being absent and a freshly pre-allocated one.
    pub fn read(col_dir: &Path) -> io::Result<Option<u64>> {
        let path = col_dir.join(POS_FILENAME);
        let file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(Self::newest(&file)?.map(|(_, lsn)| lsn))
    }

    /// One `sync_data` on an open handle, and no metadata change: this is the whole point of the
    /// file. Durable before it returns, because `apply_committed` runs ahead of the client reply.
    pub fn save(&mut self, applied_lsn: u64) -> io::Result<()> {
        use std::io::{Seek, Write};
        self.seq += 1;
        let record = pos_encode(self.seq, applied_lsn);
        self.file.seek(io::SeekFrom::Start((self.seq % 2) * POS_SLOT))?;
        self.file.write_all(&record)?;
        self.file.sync_data()
    }
}

impl LsnMeta {
    pub fn load(dir: &Path) -> Option<Self> {
        let content = fs::read_to_string(dir.join("lsn.meta")).ok()?;
        serde_json::from_str(&content).ok()
    }

    pub fn save(&self, dir: &Path) -> io::Result<()> {
        let content = serde_json::to_string(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(dir.join("lsn.meta"), content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::temp_root;

    #[test]
    fn ib006_only_complete_zeroed_slots_are_unwritten() {
        let dir = temp_root();
        fs::create_dir_all(&dir).unwrap();
        assert_eq!(AppliedPos::read(&dir).unwrap(), None);
        drop(AppliedPos::open(&dir).unwrap());
        let path = dir.join(POS_FILENAME);
        let empty = fs::read(&path).unwrap();
        assert_eq!(empty, vec![0; (POS_SLOT * 2) as usize]);
        assert_eq!(AppliedPos::read(&dir).unwrap(), None);
        drop(AppliedPos::open(&dir).unwrap());

        for offset in [0, 4, 20, 24, 511, 512, 1023] {
            let mut bytes = empty.clone();
            bytes[offset] = 1;
            fs::write(&path, &bytes).unwrap();
            assert_eq!(AppliedPos::read(&dir).unwrap_err().kind(), io::ErrorKind::InvalidData,
                "nonzero byte at {offset} is not an unwritten slot");
            assert!(AppliedPos::open(&dir).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn ib006_unexpected_position_lengths_are_refused_without_resizing() {
        let dir = temp_root();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(POS_FILENAME);
        for len in [0, 1, 23, 24, 511, 512, 513, 535, 536, 1023, 1025] {
            for with_record in [false, true] {
                let mut bytes = vec![0; len];
                if with_record {
                    let record = pos_encode(2, 42);
                    let copied = len.min(POS_RECORD);
                    bytes[..copied].copy_from_slice(&record[..copied]);
                }
                fs::write(&path, &bytes).unwrap();
                assert_eq!(AppliedPos::read(&dir).unwrap_err().kind(), io::ErrorKind::InvalidData,
                    "length {len}, with_record={with_record}");
                assert!(AppliedPos::open(&dir).is_err());
                assert_eq!(fs::read(&path).unwrap(), bytes);
            }
        }
    }

    #[test]
    fn ib006_bad_magic_preserves_the_valid_slot_and_next_save_sequence() {
        let dir = temp_root();
        fs::create_dir_all(&dir).unwrap();
        let mut pos = AppliedPos::open(&dir).unwrap();
        pos.save(10).unwrap();
        pos.save(20).unwrap();
        drop(pos);
        let path = dir.join(POS_FILENAME);
        let intact = fs::read(&path).unwrap();
        for (offset, survivor) in [(0, 10), (POS_SLOT as usize, 20)] {
            let mut bytes = intact.clone();
            bytes[offset..offset + 4].copy_from_slice(b"BAD!");
            fs::write(&path, bytes).unwrap();
            assert_eq!(AppliedPos::read(&dir).unwrap(), Some(survivor));
            let mut pos = AppliedPos::open(&dir).unwrap();
            pos.save(30).unwrap();
            drop(pos);
            assert_eq!(AppliedPos::read(&dir).unwrap(), Some(30));
            let mut pos = AppliedPos::open(&dir).unwrap();
            pos.save(40).unwrap();
            drop(pos);
            assert_eq!(AppliedPos::read(&dir).unwrap(), Some(40));
        }
    }
}
