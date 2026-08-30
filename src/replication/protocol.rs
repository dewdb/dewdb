//! Primary/replica RPC types and decoding of a replica's refusal.

use crate::util::base64_bytes;
use serde::{Deserialize, Serialize};

/// `wal_frame` is the first entry and `frames` the rest, ascending by LSN. Splitting them this way
/// keeps a peer that predates batching correct: it applies the first and reports that LSN, so the
/// leader advances one frame per round trip instead of misreading the batch as delivered.
///
/// `lsn`/`prev_lsn` describe `wal_frame` only. Later entries carry their own chain in their headers.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicateRequest {
    pub collection: String,
    pub term: u64,
    pub lsn: u64,
    pub prev_lsn: u64,
    pub commit_index: Option<u64>,
    #[serde(with = "base64_bytes")]
    pub wal_frame: Vec<u8>,
    #[serde(default, with = "base64_frames", skip_serializing_if = "Vec::is_empty")]
    pub frames: Vec<Vec<u8>>,
}

// serialize/deserialize are reached only via #[serde(with = "base64_frames")]; sweeps flag them.
mod base64_frames {
    use crate::util::base64_bytes::{base64_decode, base64_encode};
    use serde::{Deserialize, Deserializer, Serializer};
    use serde::de;

    pub fn serialize<S: Serializer>(frames: &[Vec<u8>], s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Serialize;
        let encoded: Vec<String> = frames.iter().map(|f| base64_encode(f)).collect();
        encoded.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Vec<u8>>, D::Error> {
        let raw = Vec::<String>::deserialize(d)?;
        raw.iter().map(|s| base64_decode(s).map_err(de::Error::custom)).collect()
    }
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

/// How far a replica got through a batch. `lsn` on an accepted run, `last_lsn` when it already held
/// everything sent; a peer that applied only the head reports the head, which bounds the next send.
pub fn applied_through(body: &Option<serde_json::Value>) -> Option<u64> {
    let b = body.as_ref()?;
    b.get("lsn").or_else(|| b.get("last_lsn")).and_then(|v| v.as_u64())
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
    fn a_request_without_frames_decodes_as_a_single_entry() {
        let old_wire = format!(
            r#"{{"collection":"t","term":1,"lsn":5,"prev_lsn":4,"commit_index":4,"wal_frame":"{}"}}"#,
            crate::util::base64_bytes::base64_encode(b"frame-bytes"));
        let req: ReplicateRequest = serde_json::from_str(&old_wire).unwrap();

        assert!(req.frames.is_empty(), "a pre-batching peer must still decode");
        assert_eq!(req.wal_frame, b"frame-bytes");
    }

    #[test]
    fn a_batch_survives_the_wire_in_order() {
        let req = ReplicateRequest {
            collection: "t".into(),
            term: 2,
            lsn: 7,
            prev_lsn: 6,
            commit_index: Some(6),
            wal_frame: vec![1, 2, 3],
            frames: vec![vec![4, 5], vec![6, 7, 8, 9]],
        };
        let back: ReplicateRequest = serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();

        assert_eq!(back.wal_frame, vec![1, 2, 3]);
        assert_eq!(back.frames, vec![vec![4, 5], vec![6, 7, 8, 9]],
            "order carries the chain, so a reordered batch would read as divergence");
    }

    #[test]
    fn an_empty_batch_is_omitted_from_the_wire() {
        let req = ReplicateRequest {
            collection: "t".into(), term: 1, lsn: 1, prev_lsn: 0,
            commit_index: None, wal_frame: vec![9], frames: Vec::new(),
        };
        let json = serde_json::to_string(&req).unwrap();

        assert!(!json.contains("frames"),
            "single-frame sends must stay byte-identical on the wire for peers that predate batching");
    }

    #[test]
    fn applied_through_reads_either_reply_shape() {
        assert_eq!(applied_through(&Some(serde_json::json!({"status": "applied", "lsn": 12}))), Some(12));
        assert_eq!(applied_through(&Some(serde_json::json!({"status": "duplicate", "last_lsn": 9}))), Some(9),
            "a replica that already held the batch still bounds where we resume");
        assert_eq!(applied_through(&None), None);
    }

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
