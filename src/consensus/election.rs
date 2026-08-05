//! The vote round: eligibility, tallying, and assuming leadership.

use super::failover::{adopt_existing_leader, demote};
use super::progress::ProgressMeta;
use crate::consensus::state::ReplicationMeta;
use crate::state::AppState;
use crate::util::same_endpoint;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const VOTE_REQUEST_TIMEOUT_MS: u64 = 1500;

// Field order is the comparison order: derived Ord gives Raft's (term, index) freshness test.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogTail {
    pub last_term: u64,
    pub last_lsn: u64,
}

// last_lsn/last_term are the database-wide summary, kept for peers that predate `logs`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_lsn: u64,
    pub last_term: u64,
    #[serde(default)]
    pub logs: HashMap<String, LogTail>,
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

// A promoted follower with no configured replicas would accept writes and replicate them nowhere.
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

// Opens every collection on disk: one we have not opened yet still holds entries we could lose.
// Takes the collections lock, so never call this while holding the replication lock.
pub fn local_log_tails(state: &AppState) -> HashMap<String, LogTail> {
    let db = match state.db.as_ref() {
        Some(d) => d,
        None => return HashMap::new(),
    };
    let mut tails = HashMap::new();
    for name in db.list_collections().unwrap_or_default() {
        if let Ok(col) = db.get_collection(&name) {
            let (last_term, last_lsn) = col.last_appended();
            tails.insert(name, LogTail { last_term, last_lsn });
        }
    }
    tails
}

// Cluster size is peers + self; peers must exclude this node or the threshold inflates.
pub fn majority(cluster_size: usize) -> usize {
    cluster_size / 2 + 1
}

// Staggered: a shared timeout splits the vote every round.
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

/// Every collection is an independent log and one leader serves all of them, so a candidate
/// behind on any single collection could lose that collection's committed entries once elected.
fn candidate_is_current(my_logs: &HashMap<String, LogTail>, my_summary: LogTail, req: &VoteRequest) -> bool {
    if req.logs.is_empty() {
        return LogTail { last_term: req.last_term, last_lsn: req.last_lsn } >= my_summary;
    }
    // A collection absent from the candidate's map is a log it holds nothing of.
    my_logs.iter().all(|(name, mine)| req.logs.get(name).copied().unwrap_or_default() >= *mine)
}

// Election invariant: at most one vote per term, never for a candidate whose log is behind.
pub fn decide_vote(
    cur_term: u64,
    cur_voted_for: &Option<String>,
    my_logs: &HashMap<String, LogTail>,
    my_summary: LogTail,
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
    let up_to_date = candidate_is_current(my_logs, my_summary, req);

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

    let my_lsn = state.db.as_ref().map_or(0, |db| db.durable_lsn.load(Ordering::SeqCst));
    let my_log_term = state.db.as_ref().map_or(0, |db| db.last_log_term.load(Ordering::SeqCst));
    let my_logs = local_log_tails(state);
    let candidate_id = state.config.node_id.clone();

    let new_term = {
        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
        repl.term += 1;
        repl.voted_for = Some(candidate_id.clone());
        repl.term
    };
    // Standing is a self-vote: forget it across a restart and this node can grant the same term twice.
    if let Err(e) = (ReplicationMeta { term: new_term, is_leader: false, voted_for: Some(candidate_id.clone()) })
        .save(&state.config.data_dir)
    {
        warn!(target: "election", error = %e,
            "Could not persist candidacy for term {}; abandoning this election", new_term);
        return;
    }

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
            logs: my_logs.clone(),
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

// The vote round is async; another node may have moved our term while it ran.
/// Seeds every replica's send cursor from our own log, lowered by any persisted hint.
/// Also used at boot by a node configured as primary, which never runs an election.
pub fn seed_leader_progress(state: &AppState) {
    // Both reach the collections lock, so they are gathered before the replication lock.
    let own_tails: HashMap<String, u64> = local_log_tails(state)
        .into_iter()
        .map(|(name, tail)| (name, tail.last_lsn))
        .collect();
    let hints = ProgressMeta::load(&state.config.data_dir).sent_through;

    if let Some(repl) = state.replication.as_ref() {
        let mut g = repl.write().unwrap();
        let replicas = g.replicas.clone();
        g.progress.reinit_as_leader(&replicas, &own_tails, &hints);
    }
}

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
    seed_leader_progress(state);
    // Term and self-vote are already durable; losing only is_leader rejoins as a follower.
    if let Err(e) = (ReplicationMeta { term, is_leader: true, voted_for: Some(candidate_id.to_string()) })
        .save(&state.config.data_dir)
    {
        warn!(target: "election", error = %e, "Won term {} but could not record leadership", term);
    }
    info!(target: "election", "*** WON election: PROMOTED to primary at term {} ***", term);
    info!(target: "election", "Node {} is now accepting writes", candidate_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    // No `logs`, so these exercise the scalar fallback a pre-`logs` peer still sends.
    fn vote_req(term: u64, candidate: &str, last_term: u64, last_lsn: u64) -> VoteRequest {
        VoteRequest {
            term,
            candidate_id: candidate.to_string(),
            last_term,
            last_lsn,
            logs: HashMap::new(),
        }
    }

    fn tails(pairs: &[(&str, u64, u64)]) -> HashMap<String, LogTail> {
        pairs.iter()
            .map(|(n, t, l)| (n.to_string(), LogTail { last_term: *t, last_lsn: *l }))
            .collect()
    }

    fn per_log_req(term: u64, candidate: &str, pairs: &[(&str, u64, u64)]) -> VoteRequest {
        let logs = tails(pairs);
        let summary = logs.values().copied().max().unwrap_or_default();
        VoteRequest {
            term,
            candidate_id: candidate.to_string(),
            last_term: summary.last_term,
            last_lsn: logs.values().map(|t| t.last_lsn).max().unwrap_or(0),
            logs,
        }
    }

    fn scalar(last_term: u64, last_lsn: u64) -> LogTail {
        LogTail { last_term, last_lsn }
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
        let d = decide_vote(2, &None, &HashMap::new(), scalar(2, 100), &vote_req(3, "n1", 2, 100));
        assert!(d.granted);
        assert_eq!(d.term, 3);
        assert_eq!(d.voted_for.as_deref(), Some("n1"));
    }

    #[test]
    fn vote_denied_for_stale_candidate_term() {
        let d = decide_vote(5, &None, &HashMap::new(), scalar(5, 100), &vote_req(4, "n1", 5, 100));
        assert!(!d.granted);
        assert_eq!(d.term, 5);
        assert_eq!(d.voted_for, None);
    }

    #[test]
    fn vote_at_most_once_per_term() {
        let d1 = decide_vote(3, &None, &HashMap::new(), scalar(1, 50), &vote_req(3, "n1", 1, 50));
        assert!(d1.granted);
        assert_eq!(d1.voted_for.as_deref(), Some("n1"));

        let d2 = decide_vote(3, &d1.voted_for, &HashMap::new(), scalar(1, 50), &vote_req(3, "n2", 1, 50));
        assert!(!d2.granted, "must not vote for a second candidate in the same term");
        assert_eq!(d2.voted_for.as_deref(), Some("n1"));

        let d3 = decide_vote(3, &d1.voted_for, &HashMap::new(), scalar(1, 50), &vote_req(3, "n1", 1, 50));
        assert!(d3.granted, "re-voting for the same candidate is idempotent");
    }

    #[test]
    fn vote_denied_when_candidate_log_behind() {
        let behind_lsn = decide_vote(3, &None, &HashMap::new(), scalar(2, 100), &vote_req(4, "n1", 2, 99));
        assert!(!behind_lsn.granted, "candidate with lower lsn at same log term must lose");

        let behind_term = decide_vote(3, &None, &HashMap::new(), scalar(2, 100), &vote_req(4, "n1", 1, 500));
        assert!(!behind_term.granted, "candidate with lower last log term must lose even with higher lsn");

        let ahead = decide_vote(3, &None, &HashMap::new(), scalar(2, 100), &vote_req(4, "n1", 3, 1));
        assert!(ahead.granted, "higher last log term wins regardless of lsn");
    }

    #[test]
    fn higher_term_vote_resets_prior_vote() {
        let prior = Some("n2".to_string());
        let d = decide_vote(3, &prior, &HashMap::new(), scalar(1, 50), &vote_req(4, "n1", 1, 50));
        assert!(d.granted, "a higher term clears the old vote, so n1 can win");
        assert_eq!(d.term, 4);
        assert_eq!(d.voted_for.as_deref(), Some("n1"));
    }

    #[test]
    fn a_candidate_behind_on_one_collection_is_refused() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 3)]);
        let summary = scalar(4, 10);

        // Same global max LSN and same term, but the two logs are swapped.
        let swapped = per_log_req(5, "n1", &[("users", 4, 3), ("orders", 4, 10)]);
        assert_eq!(swapped.last_lsn, 10, "the global summary cannot tell these two apart");

        let d = decide_vote(4, &None, &mine, summary, &swapped);
        assert!(!d.granted,
            "the candidate is missing users 4..10; electing it would drop entries a quorum may hold");
        assert_eq!(d.term, 5, "the term still advances even though the vote is refused");
    }

    #[test]
    fn a_higher_term_on_one_collection_does_not_excuse_a_stale_other() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 11)]);

        let d = decide_vote(4, &None, &mine, scalar(4, 11),
            &per_log_req(5, "n1", &[("users", 4, 5), ("orders", 5, 12)]));
        assert!(!d.granted,
            "a newer term on orders says nothing about users, where this candidate is behind");
    }

    #[test]
    fn a_candidate_current_on_every_collection_wins() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 11)]);

        let equal = decide_vote(4, &None, &mine, scalar(4, 11),
            &per_log_req(5, "n1", &[("users", 4, 10), ("orders", 4, 11)]));
        assert!(equal.granted, "matching every log is up to date");

        let ahead = decide_vote(4, &None, &mine, scalar(4, 11),
            &per_log_req(5, "n2", &[("users", 5, 20), ("orders", 4, 11)]));
        assert!(ahead.granted, "ahead on one log and level on the rest is up to date");
    }

    #[test]
    fn a_collection_the_candidate_has_never_seen_makes_it_stale() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 3)]);

        let d = decide_vote(4, &None, &mine, scalar(4, 10),
            &per_log_req(5, "n1", &[("users", 4, 10)]));
        assert!(!d.granted, "a log absent from the candidate is one it holds nothing of");

        let empty_too = tails(&[("users", 4, 10), ("orders", 0, 0)]);
        let d = decide_vote(4, &None, &empty_too, scalar(4, 10),
            &per_log_req(5, "n2", &[("users", 4, 10)]));
        assert!(d.granted, "but an empty collection dir costs the candidate nothing");
    }

    #[test]
    fn collections_the_candidate_alone_holds_do_not_block_it() {
        let mine = tails(&[("users", 4, 10)]);

        let d = decide_vote(4, &None, &mine, scalar(4, 10),
            &per_log_req(5, "n1", &[("users", 4, 10), ("audit", 4, 99)]));
        assert!(d.granted, "only logs this voter holds can be lost, so extras are irrelevant");
    }

    #[test]
    fn a_voter_holding_no_log_grants_freely() {
        let d = decide_vote(0, &None, &HashMap::new(), LogTail::default(),
            &per_log_req(1, "n1", &[("users", 3, 40)]));
        assert!(d.granted, "a node with nothing to lose has no grounds to refuse");
    }

    #[test]
    fn a_peer_that_sends_no_summaries_falls_back_to_the_scalar_compare() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 3)]);

        let current = decide_vote(4, &None, &mine, scalar(4, 10), &vote_req(5, "n1", 4, 10));
        assert!(current.granted, "a pre-`logs` peer is still judged on the summary it does send");

        let behind = decide_vote(4, &None, &mine, scalar(4, 10), &vote_req(5, "n2", 4, 9));
        assert!(!behind.granted);

        let nothing = decide_vote(4, &None, &mine, scalar(4, 10), &vote_req(5, "n3", 0, 0));
        assert!(!nothing.granted, "an empty log must lose to a voter that holds entries");
    }

    #[test]
    fn a_vote_request_without_logs_decodes_as_an_empty_map() {
        let old_wire = r#"{"term":3,"candidate_id":"n1","last_lsn":10,"last_term":2}"#;
        let req: VoteRequest = serde_json::from_str(old_wire).unwrap();

        assert!(req.logs.is_empty(),
            "a peer that predates `logs` must still decode, or no vote succeeds mid-upgrade");
        assert_eq!(req.last_lsn, 10, "and its scalar summary must survive to drive the fallback");
    }

    #[test]
    fn per_log_summaries_survive_the_wire() {
        let req = per_log_req(5, "n1", &[("users", 4, 10), ("orders", 3, 7)]);
        let back: VoteRequest = serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();

        assert_eq!(back.logs, req.logs);
    }

    #[test]
    fn log_tails_order_by_term_before_lsn() {
        assert!(LogTail { last_term: 5, last_lsn: 1 } > LogTail { last_term: 4, last_lsn: 900 },
            "field order in LogTail is the comparison order; reordering it silently inverts this");
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
