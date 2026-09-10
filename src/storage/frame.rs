//! WAL frame format and the replica's per-frame apply decision.

use serde::{Deserialize, Serialize};

pub const MAX_RECORD_SIZE: u64 = 10 * 1024 * 1024;
// Compatibility: no version field, so changing this length or the field order invalidates every WAL.
pub const HEADER_LEN: usize = 40;

/// A record at the limit with its header: what a transport carrying one frame has to fit.
pub const MAX_FRAME_SIZE: u64 = HEADER_LEN as u64 + MAX_RECORD_SIZE;

/// Padded base64, which is how `wal_frame` travels. The alphabet needs no JSON escaping, so this
/// is also the encoded length inside the request body.
pub const fn base64_len(bytes: u64) -> u64 {
    (bytes + 2) / 3 * 4
}

/// One widening chain with `MAX_RECORD_SIZE` and `MAX_INTERNAL_BODY`: a body the public API takes
/// fits a frame, and any frame the log holds fits one internal request. Disagreeing is `H13`.
pub const MAX_PUBLIC_BODY: usize = 2 * 1024 * 1024;
/// One max-size frame encoded, plus the envelope around it. Batches are bounded separately, by
/// bytes, so raising this does not turn a 64-frame batch into a 900 MB request.
pub const MAX_INTERNAL_BODY: usize = base64_len(MAX_FRAME_SIZE) as usize + 64 * 1024;

/// A key, a timestamp and the field names around the body. Keys arrive on the URL, so hyper's
/// header limit is what bounds them; this is that with room to spare.
const ENVELOPE_HEADROOM: u64 = 64 * 1024;

// Asserted rather than commented: drift here is H13 again, and it went unnoticed for eight phases.
const _: () = assert!(MAX_PUBLIC_BODY as u64 + ENVELOPE_HEADROOM <= MAX_RECORD_SIZE);
const _: () = assert!(base64_len(MAX_FRAME_SIZE) <= MAX_INTERNAL_BODY as u64);

// Format invariant: prev_lsn/prev_term name the predecessor in THIS collection, not lsn - 1.
// LSNs are database-wide, so a collection's frames are sparse and global chaining fakes gaps.
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
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        migration: bool,
        key: String,
        value: serde_json::Value,
        ts: u64,
    },
    Del {
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        migration: bool,
        key: String,
        ts: u64,
    },
    /// A no-op occupying an LSN, appended per collection on promotion so the inherited tail has a
    /// current-term entry to be committed by. Applies nothing and is never in the index.
    Barrier {
        ts: u64,
    },
    /// Removes every key and leaves the collection a tombstone until something is written above it.
    /// Committing it, not appending it, is what makes the collection gone.
    Drop {
        ts: u64,
    },
    /// A voting set. Unlike every other entry it takes effect where it is *appended*, not where it
    /// commits, so a leader cannot decide a change using the membership the change replaces.
    Config {
        config: Configuration,
        ts: u64,
    },
    /// What this shard group handed over, so cleanup survives the group electing someone else.
    Handover {
        handover: HandoverRecord,
        ts: u64,
    },
    /// A secondary index definition. In force from the append, so every entry above it stages the
    /// values it asks for; what commits is the build, since the postings derive from committed keys.
    Index {
        change: crate::storage::secondary::IndexChange,
        ts: u64,
    },
}

impl LogEntry {
    pub fn is_migration(&self) -> bool {
        matches!(self, Self::Put { migration: true, .. } | Self::Del { migration: true, .. })
    }
}

/// A completed handover as it travels in the log: the plan's id and the ring it moved keys for. The
/// keys are absent -- unbounded, and cleanup derives them from this ring instead.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct HandoverRecord {
    pub id: String,
    pub target: crate::ring::HashRing,
}

/// A quorum membership, as it travels in the log. `outgoing` is present only between the two entries
/// of a change, and while it is, a decision needs a majority of each half separately.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Configuration {
    pub voters: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outgoing: Option<Vec<String>>,
}

#[derive(Debug)]
pub enum ReplicaApply {
    Applied { lsn: u64 },
    Duplicate { last_lsn: u64 },
    Gap { last_lsn: u64, last_term: u64 },
    Divergent { last_lsn: u64, last_term: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::base64_bytes::base64_encode;

    #[test]
    fn ib028_legacy_entries_remain_client_changes() {
        for value in [serde_json::json!({"op": "put", "key": "k", "value": {}, "ts": 0}),
            serde_json::json!({"op": "del", "key": "k", "ts": 0})] {
            let entry: LogEntry = serde_json::from_value(value.clone()).unwrap();
            assert!(!entry.is_migration());
            assert_eq!(serde_json::to_value(entry).unwrap(), value);
            let mut marked = value;
            marked["migration"] = serde_json::json!(true);
            let entry: LogEntry = serde_json::from_value(marked.clone()).unwrap();
            assert!(entry.is_migration());
            assert_eq!(serde_json::to_value(entry).unwrap(), marked);
        }
    }

    /// The chain's assertions are only as good as this arithmetic, and `base64_len` is a closed
    /// form for a loop that pads.
    #[test]
    fn base64_len_matches_the_encoder_it_predicts() {
        for n in [0usize, 1, 2, 3, 4, 5, 62, 63, 64, 1000, HEADER_LEN, HEADER_LEN + 1] {
            assert_eq!(base64_len(n as u64) as usize, base64_encode(&vec![0u8; n]).len(),
                "predicted length is wrong at {} bytes", n);
        }
    }
}
