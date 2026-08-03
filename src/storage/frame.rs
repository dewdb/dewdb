//! WAL frame format and the replica's per-frame apply decision.

use serde::{Deserialize, Serialize};

pub const MAX_RECORD_SIZE: u64 = 10 * 1024 * 1024;
// Compatibility: frames carry no version field, so changing this length or the
// field order invalidates every existing WAL.
pub const HEADER_LEN: usize = 40;

// Format invariant:
// prev_lsn/prev_term name the predecessor in THIS collection, not lsn - 1.
// LSNs come from one database-wide counter, so a collection's frames are
// sparse in it; chaining globally makes every interleaved write look like a gap.
#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    pub len: u32,
    pub crc: u32,
    pub term: u64,
    pub lsn: u64,
    pub prev_lsn: u64,
    pub prev_term: u64,
}

impl FrameHeader {
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN {
            return None;
        }
        Some(Self {
            len: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            crc: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            term: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            lsn: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            prev_lsn: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            prev_term: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        })
    }

    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[0..4].copy_from_slice(&self.len.to_le_bytes());
        out[4..8].copy_from_slice(&self.crc.to_le_bytes());
        out[8..16].copy_from_slice(&self.term.to_le_bytes());
        out[16..24].copy_from_slice(&self.lsn.to_le_bytes());
        out[24..32].copy_from_slice(&self.prev_lsn.to_le_bytes());
        out[32..40].copy_from_slice(&self.prev_term.to_le_bytes());
        out
    }

    pub fn payload_valid(&self, payload: &[u8]) -> bool {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload);
        hasher.finalize() == self.crc
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum LogEntry {
    Put {
        key: String,
        value: serde_json::Value,
        ts: u64,
    },
    Del {
        key: String,
        ts: u64,
    },
}

#[derive(Debug)]
pub enum ReplicaApply {
    Applied { wal_id: u64, offset: u64, lsn: u64 },
    Duplicate { last_lsn: u64 },
    Gap { last_lsn: u64, last_term: u64 },
    Divergent { last_lsn: u64, last_term: u64 },
}
