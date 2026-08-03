//! Leader-side replication progress and the quorum-committed watermark.

use crate::consensus::election::majority;
use std::collections::HashMap;

// Per-collection because LSNs come from one database-wide counter but replication
// is per collection: a replica acking lsn 7 for "users" says nothing about
// "orders", so one global watermark per replica would over-claim.
#[derive(Default)]
pub struct Progress {
    matched: HashMap<String, HashMap<String, u64>>,
    committed: HashMap<String, u64>,
}

impl Progress {
    pub fn new() -> Self {
        Self::default()
    }

    // A new term starts with no knowledge of what anyone holds. Carrying match
    // state across a leader change would let us commit on stale evidence.
    pub fn reset(&mut self) {
        self.matched.clear();
        self.committed.clear();
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

    /// Highest LSN a majority of the cluster holds, counting the leader itself.
    ///
    /// Never moves backwards: a replica that is replaced by a snapshot can report
    /// a lower match than before, and an entry stays committed regardless.
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
