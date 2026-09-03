//! Index entries, their persisted snapshot, and the commit watermark file.

use super::frame::{Configuration, HandoverRecord, HEADER_LEN};
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
