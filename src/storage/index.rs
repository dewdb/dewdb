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
    /// The committed secondary index definitions, here for the same reason the three above are:
    /// an `Index` entry is never in the key index, so compaction retires its frame. Only the
    /// definitions -- the postings are derived and rebuilt when the collection opens.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<IndexSpec>,
}

pub const APPLIED_FILENAME: &str = "applied.meta";

impl AppliedMeta {
    /// `Ok(None)` is "this collection has no consensus history", which the caller replays in full.
    /// A file that exists but does not parse is an error, never `None`: the two answers differ by
    /// the whole log, and reading damage as absence publishes every entry above the watermark.
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

/// The commit position alone, updated in place, because rewriting `AppliedMeta` per commit costs a
/// create plus an `fsync` on a *new* file -- a metadata transaction, not a data flush (bugs.md H17).
///
/// Two sector-sized slots written alternately, newest sequence wins: a torn write damages only the
/// slot it was writing, which is what replaces the temp file and rename.
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

/// Never written and damaged are different answers, the way they are for `AppliedMeta`: absence
/// means replay from the full record, and reading damage as absence lowers the watermark, which is
/// the truncation `H17` is about.
enum Slot {
    Empty,
    Damaged,
    Written { seq: u64, applied_lsn: u64 },
}

fn pos_decode(bytes: &[u8]) -> Slot {
    if bytes.len() < POS_RECORD || u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != POS_MAGIC {
        return Slot::Empty;
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
        let file = fs::OpenOptions::new().create(true).read(true).write(true).open(&path)?;
        let seq = Self::newest(&file)?.map_or(0, |(seq, _)| seq);
        if file.metadata()?.len() != POS_SLOT * 2 {
            file.set_len(POS_SLOT * 2)?;
            file.sync_all()?;
        }
        Ok(Self { file, seq })
    }

    /// The higher-sequence slot that verifies. A short read is a file that was created and never
    /// written, which is `Empty` rather than an error. Both slots damaged is an error: one of them
    /// held a position, and answering "nothing recorded" would retract it.
    fn newest(file: &fs::File) -> io::Result<Option<(u64, u64)>> {
        use std::io::{Read, Seek};
        let mut handle = file.try_clone()?;
        handle.seek(io::SeekFrom::Start(0))?;
        let mut buf = vec![0u8; (POS_SLOT * 2) as usize];
        let mut filled = 0;
        while filled < buf.len() {
            match handle.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {},
                Err(e) => return Err(e),
            }
        }

        let slots: Vec<Slot> = [0usize, POS_SLOT as usize].iter()
            .map(|at| match buf.get(*at..at + POS_RECORD) {
                Some(bytes) if at + POS_RECORD <= filled => pos_decode(bytes),
                _ => Slot::Empty,
            })
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
