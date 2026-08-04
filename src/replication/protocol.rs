//! Primary/replica RPC types and decoding of a replica's refusal.

use crate::util::base64_bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicateRequest {
    pub collection: String,
    pub term: u64,
    pub lsn: u64,
    pub prev_lsn: u64,
    pub commit_index: Option<u64>,
    #[serde(with = "base64_bytes")]
    pub wal_frame: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct DropRequest {
    pub collection: String,
    pub term: u64,
}

#[derive(Serialize, Deserialize)]
pub struct ResyncRequest {
    pub collection: String,
}

// Gap streams back; divergence does not, since the replica holds entries we lack.
pub enum ConflictKind {
    StaleTerm(u64),
    Gap(u64, u64),
    Divergent(u64),
}

pub fn classify_conflict(body: &Option<serde_json::Value>) -> ConflictKind {
    let b = match body {
        Some(b) => b,
        None => return ConflictKind::Gap(0, 0),
    };

    let last_lsn = b.get("last_lsn").and_then(|v| v.as_u64()).unwrap_or(0);
    let last_term = b.get("last_term").and_then(|v| v.as_u64()).unwrap_or(0);

    match b.get("status").and_then(|s| s.as_str()) {
        Some("stale_term") => ConflictKind::StaleTerm(b.get("term").and_then(|v| v.as_u64()).unwrap_or(0)),
        Some("divergent") => ConflictKind::Divergent(last_lsn),
        _ => ConflictKind::Gap(last_lsn, last_term),
    }
}

pub fn forbidden_term(body: &Option<serde_json::Value>) -> u64 {
    body.as_ref()
        .and_then(|b| b.get("term").and_then(|v| v.as_u64()))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_classification() {
        let stale = Some(serde_json::json!({"status": "stale_term", "term": 9}));
        match classify_conflict(&stale) {
            ConflictKind::StaleTerm(t) => assert_eq!(t, 9),
            _ => panic!("expected StaleTerm"),
        }

        let gap = Some(serde_json::json!({"status": "gap", "last_lsn": 42, "last_term": 7}));
        match classify_conflict(&gap) {
            ConflictKind::Gap(l, t) => assert_eq!((l, t), (42, 7)),
            _ => panic!("expected Gap"),
        }

        let divergent = Some(serde_json::json!({"status": "divergent", "last_lsn": 11, "last_term": 3}));
        match classify_conflict(&divergent) {
            ConflictKind::Divergent(l) => assert_eq!(l, 11),
            _ => panic!("expected Divergent"),
        }

        match classify_conflict(&None) {
            ConflictKind::Gap(l, t) => assert_eq!((l, t), (0, 0)),
            _ => panic!("expected Gap default"),
        }

        let fb = Some(serde_json::json!({"status": "not_a_replica", "term": 4}));
        assert_eq!(forbidden_term(&fb), 4);
        assert_eq!(forbidden_term(&None), 0);
    }
}
