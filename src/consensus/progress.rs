//! Leader-side replication progress and the quorum-committed watermark.

use crate::consensus::election::majority;
use crate::util::write_atomic;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

const PROGRESS_FILE: &str = "progress.meta";

/// Send cursors carried across a leader restart, purely to avoid re-probing every collection.
/// Never quorum evidence: a stale entry only costs a rejected frame and a rewind, so this is
/// flushed lazily and an absent or unreadable file is not an error.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ProgressMeta {
    pub sent_through: HashMap<String, HashMap<String, u64>>,
}

/// Flushed on an interval rather than per ack: the cursor moves on every replicated frame, and an
/// fsync there would put a disk write on the write path for a value that is only ever a hint.
pub const PROGRESS_FLUSH_INTERVAL_SECS: u64 = 2;

impl ProgressMeta {
    pub fn load(data_dir: &str) -> Self {
        let path = Path::new(data_dir).join(PROGRESS_FILE);
        fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default()
    }

    pub fn save(&self, data_dir: &str) -> io::Result<()> {
        let bytes = serde_json::to_vec(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        write_atomic(Path::new(data_dir), PROGRESS_FILE, &bytes)
    }
}

// LSNs come from one database-wide counter but replication is per collection.
// An ack of lsn 7 for "users" says nothing about "orders"; a global watermark over-claims.
//
// `matched` is quorum evidence and is term-scoped. `sent_through` is only a send cursor:
// it may be restored from disk, and must never be read as evidence a replica holds anything.
#[derive(Default)]
pub struct Progress {
    matched: HashMap<String, HashMap<String, u64>>,
    committed: HashMap<String, u64>,
    sent_through: HashMap<String, HashMap<String, u64>>,
}

impl Progress {
    pub fn new() -> Self {
        Self::default()
    }

    // Match state from a previous term is stale evidence for a quorum.
    pub fn reset(&mut self) {
        self.matched.clear();
        self.committed.clear();
        self.sent_through.clear();
    }

    /// Exclusive lower bound for the next send: frames after this LSN are what the replica still
    /// needs. Raft's `nextIndex` minus one, since sparse per-collection LSNs make `+ 1` meaningless.
    pub fn sent_through(&self, replica: &str, collection: &str) -> Option<u64> {
        self.sent_through.get(replica).and_then(|m| m.get(collection)).copied()
    }

    pub fn note_sent(&mut self, replica: &str, collection: &str, lsn: u64) {
        let slot = self.sent_through
            .entry(replica.to_string()).or_default()
            .entry(collection.to_string()).or_insert(0);
        if lsn > *slot {
            *slot = lsn;
        }
    }

    /// Rewinds the cursor after a replica refuses a frame, using the tail it reported.
    /// Unconditional, unlike `note_sent`: backing off is the whole point.
    pub fn rewind_to(&mut self, replica: &str, collection: &str, lsn: u64) {
        self.sent_through
            .entry(replica.to_string()).or_default()
            .insert(collection.to_string(), lsn);
    }

    /// On winning an election: forget all quorum evidence, and seed each send cursor at our own
    /// tail the way Raft does. A persisted hint may only lower it -- raising it would skip frames
    /// the replica lacks, and the chain check would then have to catch what we should not have sent.
    pub fn reinit_as_leader(
        &mut self,
        replicas: &[String],
        own_tails: &HashMap<String, u64>,
        hints: &HashMap<String, HashMap<String, u64>>,
    ) {
        self.matched.clear();
        self.committed.clear();
        self.sent_through.clear();

        for replica in replicas {
            let per_collection = self.sent_through.entry(replica.clone()).or_default();
            for (collection, tail) in own_tails {
                let hinted = hints.get(replica).and_then(|m| m.get(collection)).copied();
                per_collection.insert(collection.clone(), hinted.unwrap_or(*tail).min(*tail));
            }
        }
    }

    pub fn cursor_snapshot(&self) -> HashMap<String, HashMap<String, u64>> {
        self.sent_through.clone()
    }

    pub fn observe_ack(&mut self, replica: &str, collection: &str, lsn: u64) {
        let per_collection = self.matched.entry(replica.to_string()).or_default();
        let slot = per_collection.entry(collection.to_string()).or_insert(0);
        if lsn > *slot {
            *slot = lsn;
        }
    }

    pub fn matched(&self, replica: &str, collection: &str) -> u64 {
        self.matched
            .get(replica)
            .and_then(|m| m.get(collection))
            .copied()
            .unwrap_or(0)
    }

    pub fn committed(&self, collection: &str) -> u64 {
        self.committed.get(collection).copied().unwrap_or(0)
    }

    pub fn all_committed(&self) -> Vec<(String, u64)> {
        self.committed.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    pub fn max_committed(&self) -> u64 {
        self.committed.values().copied().max().unwrap_or(0)
    }

    /// Highest LSN a majority holds, counting the leader. Never moves backwards:
    /// a resynced replica can report a lower match than before.
    pub fn advance(&mut self, collection: &str, leader_durable: u64, replicas: &[String]) -> u64 {
        let mut held: Vec<u64> = Vec::with_capacity(replicas.len() + 1);
        held.push(leader_durable);
        for replica in replicas {
            held.push(self.matched(replica, collection));
        }
        held.sort_unstable_by(|a, b| b.cmp(a));

        let quorum = held[majority(held.len()) - 1];
        let slot = self.committed.entry(collection.to_string()).or_insert(0);
        if quorum > *slot {
            *slot = quorum;
        }
        *slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("http://r{}", i)).collect()
    }

    #[test]
    fn a_lone_leader_commits_its_own_writes() {
        let mut p = Progress::new();
        assert_eq!(p.advance("c", 7, &[]), 7, "majority of one is itself");
    }

    #[test]
    fn two_of_three_is_a_quorum() {
        let r = urls(2);
        let mut p = Progress::new();

        assert_eq!(p.advance("c", 10, &r), 0, "leader alone is not a majority of three");

        p.observe_ack(&r[0], "c", 10);
        assert_eq!(p.advance("c", 10, &r), 10, "leader plus one replica is two of three");

        p.observe_ack(&r[1], "c", 10);
        assert_eq!(p.advance("c", 10, &r), 10);
    }

    #[test]
    fn commits_the_highest_lsn_a_majority_holds_not_the_highest_seen() {
        let r = urls(4);
        let mut p = Progress::new();
        p.observe_ack(&r[0], "c", 9);
        p.observe_ack(&r[1], "c", 5);
        p.observe_ack(&r[2], "c", 5);
        p.observe_ack(&r[3], "c", 1);

        // sorted: 20, 9, 5, 5, 1 -> majority of five is the 3rd
        assert_eq!(p.advance("c", 20, &r), 5,
            "one fast replica must not carry an entry the majority lacks");
    }

    #[test]
    fn the_watermark_never_moves_backwards() {
        let r = urls(2);
        let mut p = Progress::new();
        p.observe_ack(&r[0], "c", 10);
        assert_eq!(p.advance("c", 10, &r), 10);

        // a replica wiped and resynced reports less than it did before
        p.matched.clear();
        assert_eq!(p.advance("c", 10, &r), 10, "already-committed entries stay committed");
    }

    #[test]
    fn collections_commit_independently() {
        let r = urls(2);
        let mut p = Progress::new();
        p.observe_ack(&r[0], "alpha", 4);
        p.observe_ack(&r[1], "alpha", 4);

        assert_eq!(p.advance("alpha", 4, &r), 4);
        assert_eq!(p.advance("beta", 6, &r), 0,
            "an ack for alpha must not commit beta, whose frames nobody has");
        assert_eq!(p.committed("alpha"), 4);
        assert_eq!(p.max_committed(), 4);
    }

    #[test]
    fn acks_only_ratchet_up() {
        let mut p = Progress::new();
        p.observe_ack("http://a", "c", 9);
        p.observe_ack("http://a", "c", 4);
        assert_eq!(p.matched("http://a", "c"), 9, "a late lower ack must not rewind");
    }

    fn owned(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
        pairs.iter().map(|(n, l)| (n.to_string(), *l)).collect()
    }

    fn hint(replica: &str, pairs: &[(&str, u64)]) -> HashMap<String, HashMap<String, u64>> {
        let mut h = HashMap::new();
        h.insert(replica.to_string(), owned(pairs));
        h
    }

    #[test]
    fn a_fresh_leader_seeds_each_cursor_at_its_own_tail() {
        let r = urls(2);
        let mut p = Progress::new();
        p.reinit_as_leader(&r, &owned(&[("users", 10), ("orders", 4)]), &HashMap::new());

        assert_eq!(p.sent_through(&r[0], "users"), Some(10));
        assert_eq!(p.sent_through(&r[1], "orders"), Some(4));
        assert_eq!(p.sent_through(&r[0], "absent"), None, "collections we do not hold get no cursor");
    }

    #[test]
    fn a_persisted_hint_may_only_lower_the_cursor() {
        let r = urls(1);

        let mut p = Progress::new();
        p.reinit_as_leader(&r, &owned(&[("users", 10)]), &hint(&r[0], &[("users", 6)]));
        assert_eq!(p.sent_through(&r[0], "users"), Some(6),
            "a lower hint saves probing back down to where the replica actually is");

        let mut p = Progress::new();
        p.reinit_as_leader(&r, &owned(&[("users", 10)]), &hint(&r[0], &[("users", 99)]));
        assert_eq!(p.sent_through(&r[0], "users"), Some(10),
            "a hint above our own tail must be clamped; trusting it would skip frames \
             the replica lacks and leave the chain check to catch what we should not have sent");
    }

    #[test]
    fn a_restored_cursor_is_never_quorum_evidence() {
        let r = urls(2);
        let mut p = Progress::new();

        p.observe_ack(&r[0], "users", 10);
        p.observe_ack(&r[1], "users", 10);
        assert_eq!(p.advance("users", 10, &r), 10);

        p.reinit_as_leader(&r, &owned(&[("users", 10)]), &hint(&r[0], &[("users", 10)]));

        assert_eq!(p.matched(&r[0], "users"), 0, "a new term starts with no match evidence");
        assert_eq!(p.committed("users"), 0);
        assert_eq!(p.advance("users", 10, &r), 0,
            "a cursor restored from disk says where to resume sending, never that a replica holds it");
    }

    #[test]
    fn cursors_ratchet_up_but_rewind_on_demand() {
        let mut p = Progress::new();
        p.note_sent("http://a", "users", 7);
        p.note_sent("http://a", "users", 4);
        assert_eq!(p.sent_through("http://a", "users"), Some(7), "a late lower send must not rewind");

        p.rewind_to("http://a", "users", 2);
        assert_eq!(p.sent_through("http://a", "users"), Some(2),
            "a refusal must rewind, or the leader keeps sending past what the replica has");
    }

    #[test]
    fn reset_clears_the_cursors_too() {
        let mut p = Progress::new();
        p.note_sent("http://a", "users", 7);
        p.reset();
        assert_eq!(p.sent_through("http://a", "users"), None);
    }

    #[test]
    fn progress_meta_round_trips_and_tolerates_absence() {
        let root = crate::test_support::temp_root();
        let dir = root.to_string_lossy().to_string();

        assert!(ProgressMeta::load(&dir).sent_through.is_empty(),
            "no file is a normal cold start, not an error");

        let meta = ProgressMeta { sent_through: hint("http://a", &[("users", 12)]) };
        meta.save(&dir).unwrap();
        assert_eq!(ProgressMeta::load(&dir).sent_through, meta.sent_through);

        fs::write(root.join(PROGRESS_FILE), b"{ truncated").unwrap();
        assert!(ProgressMeta::load(&dir).sent_through.is_empty(),
            "an unreadable hint costs a probe, so it must not block startup");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn reset_forgets_everything_from_the_previous_term() {
        let r = urls(2);
        let mut p = Progress::new();
        p.observe_ack(&r[0], "c", 10);
        p.advance("c", 10, &r);

        p.reset();
        assert_eq!(p.matched(&r[0], "c"), 0);
        assert_eq!(p.committed("c"), 0);
        assert_eq!(p.advance("c", 10, &r), 0, "a new leader must re-earn its quorum");
    }
}
