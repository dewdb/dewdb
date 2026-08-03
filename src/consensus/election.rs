//! The vote round: eligibility, tallying, and assuming leadership.

use super::failover::{adopt_existing_leader, demote};
use crate::consensus::state::ReplicationMeta;
use crate::state::AppState;
use crate::util::same_endpoint;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const VOTE_REQUEST_TIMEOUT_MS: u64 = 1500;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_lsn: u64,
    pub last_term: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

pub struct VoteDecision {
    pub granted: bool,
    pub term: u64,
    pub voted_for: Option<String>,
}

// A promoted follower with no configured replicas must adopt its peers, or it
// accepts writes and replicates them nowhere.
fn leader_replica_set(configured: &[String], peers: &[String], listen_addr: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for candidate in configured.iter().chain(peers.iter()) {
        if same_endpoint(candidate, listen_addr) {
            continue;
        }
        if !out.iter().any(|existing| same_endpoint(existing, candidate)) {
            out.push(candidate.clone());
        }
    }
    out
}

// Cluster size is peers + self, so peers must exclude this node or the threshold
// is computed against an inflated cluster.
pub fn majority(cluster_size: usize) -> usize {
    cluster_size / 2 + 1
}

// Staggers candidates so a shared timeout does not split the vote every round.
fn election_jitter(node_id: &str, max_delay_ms: u64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    if max_delay_ms == 0 {
        return 0;
    }
    let mut h = DefaultHasher::new();
    node_id.hash(&mut h);
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos().hash(&mut h);
    h.finish() % max_delay_ms
}

// Election invariants: at most one vote per term, and never for a candidate
// whose log is behind. (last_term, last_lsn) compares as a tuple, so a higher
// last term wins regardless of LSN.
pub fn decide_vote(
    cur_term: u64,
    cur_voted_for: &Option<String>,
    my_log_term: u64,
    my_lsn: u64,
    req: &VoteRequest,
) -> VoteDecision {
    if req.term < cur_term {
        return VoteDecision { granted: false, term: cur_term, voted_for: cur_voted_for.clone() };
    }

    let mut term = cur_term;
    let mut voted_for = cur_voted_for.clone();
    if req.term > cur_term {
        term = req.term;
        voted_for = None;
    }

    let can_vote = match &voted_for {
        None => true,
        Some(v) => v == &req.candidate_id,
    };
    let up_to_date = (req.last_term, req.last_lsn) >= (my_log_term, my_lsn);

    if can_vote && up_to_date {
        VoteDecision { granted: true, term, voted_for: Some(req.candidate_id.clone()) }
    } else {
        VoteDecision { granted: false, term, voted_for }
    }
}

pub async fn run_election(state: &AppState, max_delay_ms: u64) {
    let delay_ms = election_jitter(&state.config.node_id, max_delay_ms);
    info!(target: "election", "Waiting {}ms before requesting votes...", delay_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    if state.is_leader() {
        return;
    }

    if adopt_existing_leader(state).await {
        info!(target: "election", "A leader is already serving; aborting election and following it");
        return;
    }

    let my_lsn = state.db.as_ref().map_or(0, |db| db.global_commit_index.load(Ordering::SeqCst));
    let my_log_term = state.db.as_ref().map_or(0, |db| db.last_log_term.load(Ordering::SeqCst));
    let candidate_id = state.config.node_id.clone();

    let new_term = {
        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
        repl.term += 1;
        repl.voted_for = Some(candidate_id.clone());
        repl.term
    };
    let _ = ReplicationMeta { term: new_term, is_leader: false, voted_for: Some(candidate_id.clone()) }.save(&state.config.data_dir);

    let peers = state.config.peers.clone();
    let cluster_size = peers.len() + 1;
    let needed = majority(cluster_size);
    info!(target: "election", "Node {} standing for term {} ({} peers, need {} votes)", candidate_id, new_term, peers.len(), needed);

    if peers.is_empty() {
        if needed <= 1 {
            become_leader(state, new_term, &candidate_id).await;
        } else {
            warn!(target: "election", "No peers configured; cannot form a majority. Set 'peers' in config for automatic failover.");
        }
        return;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<(bool, u64)>(peers.len());
    for peer in peers {
        let client = state.client.clone();
        let req = VoteRequest {
            term: new_term,
            candidate_id: candidate_id.clone(),
            last_lsn: my_lsn,
            last_term: my_log_term,
        };
        let tx = tx.clone();
        tokio::spawn(async move {
            let url = format!("{}/internal/vote", peer);
            let sent = client.post(&url)
                .timeout(Duration::from_millis(VOTE_REQUEST_TIMEOUT_MS))
                .json(&req).send().await;
            let result = match sent {
                Ok(r) if r.status().is_success() => {
                    match r.json::<VoteResponse>().await {
                        Ok(v) => (v.vote_granted, v.term),
                        Err(_) => (false, 0),
                    }
                },
                _ => (false, 0),
            };
            let _ = tx.send(result).await;
        });
    }
    drop(tx);

    let votes = Arc::new(AtomicUsize::new(1));
    let highest_term = Arc::new(AtomicU64::new(new_term));
    let votes_inner = votes.clone();
    let ht_inner = highest_term.clone();
    let election_timeout = Duration::from_millis(max_delay_ms.max(1000) + 2000);
    let _ = tokio::time::timeout(election_timeout, async move {
        while votes_inner.load(Ordering::Relaxed) < needed {
            match rx.recv().await {
                Some((granted, term)) => {
                    if term > ht_inner.load(Ordering::Relaxed) {
                        ht_inner.store(term, Ordering::Relaxed);
                    }
                    if granted {
                        votes_inner.fetch_add(1, Ordering::Relaxed);
                    }
                },
                None => break,
            }
        }
    }).await;

    let seen_term = highest_term.load(Ordering::Relaxed);
    if seen_term > new_term {
        info!(target: "election", "Saw higher term {} during election; stepping down", seen_term);
        demote(state, seen_term).await;
        return;
    }

    let tally = votes.load(Ordering::Relaxed);
    if tally >= needed {
        become_leader(state, new_term, &candidate_id).await;
    } else {
        info!(target: "election", "Only {}/{} votes for term {}; election failed, will retry", tally, needed, new_term);
    }
}

// The vote round is async, so re-check that term and vote still match before
// claiming leadership; another node may have moved us on meanwhile.
async fn become_leader(state: &AppState, term: u64, candidate_id: &str) {
    {
        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
        if repl.term != term || repl.voted_for.as_deref() != Some(candidate_id) {
            info!(target: "election", "State changed during election (term now {}); not assuming leadership", repl.term);
            return;
        }
        repl.is_leader = true;
        repl.heartbeat_running = false;
        repl.primary_addr = None;
        repl.replicas = leader_replica_set(
            &state.config.replicas,
            &state.config.peers,
            &state.config.listen_addr,
        );
    }
    let _ = ReplicationMeta { term, is_leader: true, voted_for: Some(candidate_id.to_string()) }.save(&state.config.data_dir);
    info!(target: "election", "*** WON election: PROMOTED to primary at term {} ***", term);
    info!(target: "election", "Node {} is now accepting writes", candidate_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vote_req(term: u64, candidate: &str, last_term: u64, last_lsn: u64) -> VoteRequest {
        VoteRequest { term, candidate_id: candidate.to_string(), last_term, last_lsn }
    }

    #[test]
    fn majority_math() {
        assert_eq!(majority(1), 1);
        assert_eq!(majority(2), 2);
        assert_eq!(majority(3), 2);
        assert_eq!(majority(4), 3);
        assert_eq!(majority(5), 3);
    }

    #[test]
    fn vote_granted_for_fresh_higher_term_when_up_to_date() {
        let d = decide_vote(2, &None, 2, 100, &vote_req(3, "n1", 2, 100));
        assert!(d.granted);
        assert_eq!(d.term, 3);
        assert_eq!(d.voted_for.as_deref(), Some("n1"));
    }

    #[test]
    fn vote_denied_for_stale_candidate_term() {
        let d = decide_vote(5, &None, 5, 100, &vote_req(4, "n1", 5, 100));
        assert!(!d.granted);
        assert_eq!(d.term, 5);
        assert_eq!(d.voted_for, None);
    }

    #[test]
    fn vote_at_most_once_per_term() {
        let d1 = decide_vote(3, &None, 1, 50, &vote_req(3, "n1", 1, 50));
        assert!(d1.granted);
        assert_eq!(d1.voted_for.as_deref(), Some("n1"));

        let d2 = decide_vote(3, &d1.voted_for, 1, 50, &vote_req(3, "n2", 1, 50));
        assert!(!d2.granted, "must not vote for a second candidate in the same term");
        assert_eq!(d2.voted_for.as_deref(), Some("n1"));

        let d3 = decide_vote(3, &d1.voted_for, 1, 50, &vote_req(3, "n1", 1, 50));
        assert!(d3.granted, "re-voting for the same candidate is idempotent");
    }

    #[test]
    fn vote_denied_when_candidate_log_behind() {
        let behind_lsn = decide_vote(3, &None, 2, 100, &vote_req(4, "n1", 2, 99));
        assert!(!behind_lsn.granted, "candidate with lower lsn at same log term must lose");

        let behind_term = decide_vote(3, &None, 2, 100, &vote_req(4, "n1", 1, 500));
        assert!(!behind_term.granted, "candidate with lower last log term must lose even with higher lsn");

        let ahead = decide_vote(3, &None, 2, 100, &vote_req(4, "n1", 3, 1));
        assert!(ahead.granted, "higher last log term wins regardless of lsn");
    }

    #[test]
    fn higher_term_vote_resets_prior_vote() {
        let prior = Some("n2".to_string());
        let d = decide_vote(3, &prior, 1, 50, &vote_req(4, "n1", 1, 50));
        assert!(d.granted, "a higher term clears the old vote, so n1 can win");
        assert_eq!(d.term, 4);
        assert_eq!(d.voted_for.as_deref(), Some("n1"));
    }

    #[test]
    fn a_promoted_follower_inherits_the_rest_of_the_cluster_as_replicas() {
        let peers = vec!["http://127.0.0.1:2".to_string(), "http://127.0.0.1:3".to_string()];

        let promoted = leader_replica_set(&[], &peers, "127.0.0.1:2");
        assert_eq!(promoted, vec!["http://127.0.0.1:3".to_string()],
            "a follower with no configured replicas must adopt its peers, minus itself, \
             or it would accept writes and replicate them nowhere");

        let configured = vec!["http://127.0.0.1:9".to_string()];
        let merged = leader_replica_set(&configured, &peers, "127.0.0.1:1");
        assert_eq!(merged, vec![
            "http://127.0.0.1:9".to_string(),
            "http://127.0.0.1:2".to_string(),
            "http://127.0.0.1:3".to_string(),
        ], "configured replicas come first, peers fill in the rest");

        let deduped = leader_replica_set(&["http://127.0.0.1:2/".to_string()], &peers, "127.0.0.1:1");
        assert_eq!(deduped.len(), 2, "the same endpoint written differently must not be duplicated");

        assert!(leader_replica_set(&[], &[], "127.0.0.1:1").is_empty());
    }
}
