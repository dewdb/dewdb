//! The vote round: eligibility, tallying, and assuming leadership.

use super::failover::{adopt_existing_leader, demote};
use super::progress::ProgressMeta;
use crate::consensus::state::ReplicationMeta;
use crate::replication::stream::replicate_to_peers;
use crate::state::AppState;
use crate::storage::frame::Configuration;
use crate::storage::FrameHeader;
use crate::util::same_endpoint;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// How a voter recognises the candidate in its own configuration, which is keyed by endpoint
    /// and not by `candidate_id`. Absent from a peer that predates configuration entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_url: Option<String>,
    /// The leader that told this candidate to stand, on a transfer. A voter following that leader
    /// votes despite fresh contact from it -- see `AppState::honours_transfer`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_from: Option<String>,
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
// Opens every collection on disk; takes the collections lock, so not under the replication lock.
pub fn local_log_tails(state: &AppState) -> HashMap<String, LogTail> {
    try_local_log_tails(state).unwrap_or_default()
}

pub fn try_local_log_tails(state: &AppState) -> std::io::Result<HashMap<String, LogTail>> {
    let db = match state.db.as_ref() {
        Some(d) => d,
        None => return Ok(HashMap::new()),
    };
    let mut tails = HashMap::new();
    for name in db.list_collections()? {
        let col = db.get_collection(&name)?;
        let (last_term, last_lsn) = col.last_appended();
        tails.insert(name, LogTail { last_term, last_lsn });
    }
    Ok(tails)
}

/// The database-wide `(term, lsn)` pair for peers that predate per-collection `logs`. Derived from
/// `tails`, never `durable_lsn`: fsync lags the tail, and a stale advertisement can win an election.
pub fn log_summary(state: &AppState, tails: &HashMap<String, LogTail>) -> LogTail {
    if let Some(max) = tails.values().copied().max() {
        return max;
    }
    // No tails means no collection could be opened; the shared counters are the only tail left.
    state.db.as_ref().map_or(LogTail::default(), |db| LogTail {
        last_term: db.last_log_term.load(Ordering::SeqCst),
        last_lsn: db.next_lsn.load(Ordering::SeqCst),
    })
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

/// Whether this voter's own configuration still counts the candidate: a demoted node goes on
/// campaigning, and its majorities are over members it no longer has. `None` on either side grants.
fn candidate_is_a_member(my_config: Option<&Configuration>, req: &VoteRequest) -> bool {
    match (my_config, req.candidate_url.as_deref()) {
        (Some(config), Some(url)) => config.contains(url),
        _ => true,
    }
}

/// The grant itself, given the vote this node would hold once `req`'s term is applied. Shared with
/// the pre-vote. `must_withhold` is the lease's other half: silence owed refuses any offer.
fn grants(
    voted_for: &Option<String>,
    my_logs: &HashMap<String, LogTail>,
    my_summary: LogTail,
    my_config: Option<&Configuration>,
    req: &VoteRequest,
    must_withhold: bool,
) -> bool {
    let can_vote = match voted_for {
        None => true,
        Some(v) => v == &req.candidate_id,
    };
    can_vote
        && candidate_is_current(my_logs, my_summary, req)
        && candidate_is_a_member(my_config, req)
        && !must_withhold
}

/// The question the real vote answers, decided against nothing and changing nothing: a candidate
/// that could not win never raises anyone's term, so a removed node stops costing an election.
pub fn decide_pre_vote(
    cur_term: u64,
    my_logs: &HashMap<String, LogTail>,
    my_summary: LogTail,
    my_config: Option<&Configuration>,
    req: &VoteRequest,
    must_withhold: bool,
) -> bool {
    // The real request would raise us to `req.term` first, so at or below our own term it either
    // loses on the term or lands in one this node may already have voted in.
    if req.term <= cur_term {
        return false;
    }
    grants(&None, my_logs, my_summary, my_config, req, must_withhold)
}

// Election invariant: at most one vote per term, never for a candidate whose log is behind or that
// this voter's configuration does not name. A refusal still adopts the term: withholding is not following.
pub fn decide_vote(
    cur_term: u64,
    cur_voted_for: &Option<String>,
    my_logs: &HashMap<String, LogTail>,
    my_summary: LogTail,
    my_config: Option<&Configuration>,
    req: &VoteRequest,
    must_withhold: bool,
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

    if grants(&voted_for, my_logs, my_summary, my_config, req, must_withhold) {
        VoteDecision { granted: true, term, voted_for: Some(req.candidate_id.clone()) }
    } else {
        VoteDecision { granted: false, term, voted_for }
    }
}

fn vote_request(
    term: u64,
    candidate_id: &str,
    logs: &HashMap<String, LogTail>,
    tail: LogTail,
    own: &str,
    transfer_from: Option<String>,
) -> VoteRequest {
    VoteRequest {
        term,
        candidate_id: candidate_id.to_string(),
        last_lsn: tail.last_lsn,
        last_term: tail.last_term,
        logs: logs.clone(),
        candidate_url: Some(own.to_string()),
        transfer_from,
    }
}

/// One round of asking every peer, stopping once the answers carry a quorum. Returns who granted --
/// this node included -- and the highest peer term, or 0. Who granted, not how many: joint halves.
async fn ask_peers(
    state: &AppState,
    req: &VoteRequest,
    peers: Vec<String>,
    quorum: &Configuration,
    endpoint: &'static str,
    absent_grants: bool,
    deadline: Duration,
) -> (Vec<String>, u64) {
    let own = state.own_url();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, bool, u64)>(peers.len());
    for peer in peers {
        let client = state.client.clone();
        let req = req.clone();
        let tx = tx.clone();
        let voter = peer.clone();
        let url = format!("{}{}", peer, endpoint);
        tokio::spawn(async move {
            let sent = client.post(&url)
                .timeout(Duration::from_millis(VOTE_REQUEST_TIMEOUT_MS))
                .json(&req).send().await;
            let (granted, term) = match sent {
                Ok(r) if r.status().is_success() => {
                    match r.json::<VoteResponse>().await {
                        Ok(v) => (v.vote_granted, v.term),
                        Err(_) => (false, 0),
                    }
                },
                // A peer that predates the endpoint. Counted as willing where the caller says so,
                // which leaves a mixed-version cluster exactly where it was without this round.
                Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => (absent_grants, 0),
                _ => (false, 0),
            };
            let _ = tx.send((voter, granted, term)).await;
        });
    }
    drop(tx);

    let granted = Arc::new(std::sync::Mutex::new(vec![own]));
    let highest_term = Arc::new(AtomicU64::new(0));
    let granted_inner = granted.clone();
    let ht_inner = highest_term.clone();
    let quorum_inner = quorum.clone();
    let _ = tokio::time::timeout(deadline, async move {
        loop {
            if quorum_inner.has_quorum(&granted_inner.lock().unwrap()) {
                return;
            }
            match rx.recv().await {
                Some((voter, vote_granted, term)) => {
                    if term > ht_inner.load(Ordering::Relaxed) {
                        ht_inner.store(term, Ordering::Relaxed);
                    }
                    if vote_granted {
                        granted_inner.lock().unwrap().push(voter);
                    }
                },
                None => break,
            }
        }
    }).await;

    (granted.lock().unwrap().clone(), highest_term.load(Ordering::Relaxed))
}

/// `forced_by` is the leader handing office over. Everything a timeout election does to avoid
/// disturbing a live leader is what a transfer skips: jitter, adoption, and the pre-vote round.
pub async fn run_election(state: &AppState, max_delay_ms: u64, forced_by: Option<String>) {
    let Ok(_campaign) = state.campaign.try_lock() else { return };
    let forced = forced_by.is_some();
    if !forced {
        let delay_ms = election_jitter(&state.config.node_id, max_delay_ms);
        info!(target: "election", "Waiting {}ms before requesting votes...", delay_ms);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }

    if state.is_leader() {
        return;
    }

    // Checked here rather than at the poll, so a node told mid-flight still stops. Covers both a
    // node booted as a learner and one the view has since named non-voting.
    if !state.in_quorum() {
        info!(target: "election",
            "This node is a non-voting member; waiting for the leader rather than standing");
        if !forced {
            adopt_existing_leader(state).await;
        }
        return;
    }

    if !forced && adopt_existing_leader(state).await {
        info!(target: "election", "A leader is already serving; aborting election and following it");
        return;
    }

    // Order matters: the summary is only as fresh as the collections local_log_tails has opened.
    let my_logs = match try_local_log_tails(state) {
        Ok(logs) => logs,
        Err(e) => {
            warn!(target: "election", error = %e, "Cannot read election histories");
            return;
        },
    };
    let my_tail = log_summary(state, &my_logs);
    let candidate_id = state.config.node_id.clone();

    let quorum = state.quorum_config();
    let own = state.own_url();
    let peers: Vec<String> = quorum.members().into_iter().filter(|v| !same_endpoint(v, &own)).collect();
    // The self-vote alone can carry a lone voter; while joint it cannot carry a half it is not in.
    let lone_voter = quorum.has_quorum(std::slice::from_ref(&own));
    let round_deadline = Duration::from_millis(max_delay_ms.max(1000) + 2000);

    // A term raised and lost with deposes a leader that was serving fine, so ask whether it could win.
    // A transfer skips it: every voter inside the standing leader's refusal window would answer no.
    if !forced && !lone_voter && !peers.is_empty() {
        let asking = state.current_term() + 1;
        let probe = vote_request(asking, &candidate_id, &my_logs, my_tail, &own, None);
        let (willing, seen_term) = ask_peers(
            state, &probe, peers.clone(), &quorum, "/internal/pre-vote", true, round_deadline).await;

        if seen_term > state.current_term() {
            info!(target: "election", "Pre-vote saw term {}; adopting it rather than standing", seen_term);
            demote(state, seen_term).await;
            return;
        }
        if !quorum.has_quorum(&willing) {
            info!(target: "election", "Pre-vote {:?} is short of a quorum for term {}; not standing",
                willing, asking);
            if let Err(e) = super::recovery::recover_histories(state, &quorum).await {
                warn!(target: "election", error = %e, "Election history recovery failed; will retry");
            }
            return;
        }
    }

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
    info!(target: "election", "Node {} standing for term {} (voters {:?}, outgoing {:?}, handover {:?})",
        candidate_id, new_term, quorum.voters, quorum.outgoing, forced_by);

    if lone_voter {
        become_leader(state, new_term, &candidate_id).await;
        return;
    }
    if peers.is_empty() {
        info!(target: "election", "No peers to ask and the self-vote is short of a quorum");
        return;
    }

    let req = vote_request(new_term, &candidate_id, &my_logs, my_tail, &own, forced_by);
    let (granted, seen_term) = ask_peers(
        state, &req, peers, &quorum, "/internal/vote", false, round_deadline).await;

    // The vote round is async; another node may have moved our term while it ran.
    if seen_term > new_term {
        info!(target: "election", "Saw higher term {} during election; stepping down", seen_term);
        demote(state, seen_term).await;
        return;
    }
    if quorum.has_quorum(&granted) {
        become_leader(state, new_term, &candidate_id).await;
    } else {
        info!(target: "election", "Votes {:?} are short of a quorum for term {}; election failed, will retry",
            granted, new_term);
    }
}

/// Seeds every replica's send cursor from our own log, lowered by any persisted hint.
/// Also used at boot by a node configured as primary, which never runs an election.
pub fn seed_leader_progress(state: &AppState) {
    // All three reach the collections lock, so they are gathered before the replication lock.
    let own_tails: HashMap<String, u64> = local_log_tails(state)
        .into_iter()
        .map(|(name, tail)| (name, tail.last_lsn))
        .collect();
    let applied: HashMap<String, u64> = state.db.as_ref()
        .map(|db| own_tails.keys()
            .filter_map(|name| db.get_collection(name).ok().map(|c| (name.clone(), c.applied_lsn())))
            .collect())
        .unwrap_or_default();
    let hints = ProgressMeta::load(&state.config.data_dir).sent_through;

    if let Some(repl) = state.replication.as_ref() {
        let mut g = repl.write().unwrap();
        let replicas = g.replicas.clone();
        g.progress.reinit_as_leader(&replicas, &own_tails, &applied, &hints);
        // A promise was made to the node this one was a moment ago; the first read pays a round.
        g.leases.clear();
    }
}

/// Raft's no-op, appended once per collection that needs one: a durable-but-uncommitted tail from a
/// previous leader has no current-term entry, so `advance` has no floor. Re-run on every drive tick.
pub fn publish_inherited_tails(state: &AppState) {
    let db = match state.db.as_ref() {
        Some(db) => db.clone(),
        None => return,
    };
    let term = state.current_term();
    let state = state.clone();

    tokio::spawn(async move {
        for name in db.list_collections().unwrap_or_default() {
            // The invariant, not a sample of it: `pending_len` was whatever promotion happened to
            // see, and a tail over the commit index with no floor is what cannot resolve itself.
            let col = match db.get_collection(&name) {
                Ok(c) if c.last_appended_lsn() > state.committed_lsn(&name)
                    && !state.has_term_floor(&name) => c,
                _ => continue,
            };
            // Leadership is re-checked per collection: this loop outlives a demotion otherwise.
            if !state.is_leader() || state.current_term() != term {
                return;
            }

            let appended = {
                let col = col.clone();
                tokio::task::spawn_blocking(move || col.barrier(term)).await
            };
            let (frame, _wal_id, _offset, lsn) = match appended {
                Ok(Ok(t)) => t,
                Ok(Err(e)) => {
                    warn!(target: "election", collection = %name, error = %e,
                        "Could not append the promotion barrier; the inherited tail stays staged");
                    continue;
                },
                Err(e) => {
                    warn!(target: "election", collection = %name, error = %e, "Barrier append panicked");
                    continue;
                },
            };

            state.note_leader_append(&name, lsn);
            let commit = col.enqueue_commit();
            let prev_lsn = FrameHeader::parse(&frame).map_or(0, |h| h.prev_lsn);
            let commit_index = state.committed_lsn(&name);
            replicate_to_peers(state.clone(), name.clone(), frame, term, commit_index, lsn, prev_lsn);

            // Own durability counts toward the quorum only after the fsync, as on the write path.
            if matches!(commit.await, Ok(Ok(()))) {
                if let Err(e) = state.advance_own_commit(&name, col.durable_lsn()) {
                    warn!(target: "election", collection = %name, error = %e,
                        "Failed to persist promotion commit watermark");
                }
            }
            info!(target: "election", collection = %name, lsn,
                "Appended a promotion barrier to publish an inherited tail");
        }
    });
}

async fn become_leader(state: &AppState, term: u64, candidate_id: &str) {
    // The caller already refused to campaign, so reaching here means a new path grew that skips
    // that check. Cheaper to re-test than to discover it as a split brain.
    if !state.in_quorum() {
        warn!(target: "election", "Refusing leadership at term {}: this node is non-voting", term);
        return;
    }
    // A configuration this node holds but never installed -- a snapshot arriving between boot and
    // now -- would otherwise seed the commit quorum from the view instead of from the log.
    state.refresh_configuration();
    // Sampled before the replication lock, which `voting_peers` must never be called under. Same
    // set the election counted, so the commit quorum cannot end up narrower than the vote was.
    let quorum_peers = state.voting_peers();
    {
        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
        if repl.term != term || repl.voted_for.as_deref() != Some(candidate_id) {
            info!(target: "election", "State changed during election (term now {}); not assuming leadership", repl.term);
            return;
        }
        repl.is_leader = true;
        repl.heartbeat_running = false;
        repl.primary_addr = None;
        repl.replicas = quorum_peers;
    }
    seed_leader_progress(state);
    publish_inherited_tails(state);
    // The leader that would have ended a change in flight is the one this node replaced.
    super::reconfigure::resume_change(state);
    // Term and self-vote are already durable; losing only is_leader rejoins as a follower.
    if let Err(e) = (ReplicationMeta { term, is_leader: true, voted_for: Some(candidate_id.to_string()) })
        .save(&state.config.data_dir)
    {
        warn!(target: "election", error = %e, "Won term {} but could not record leadership", term);
    }
    info!(target: "election", "*** WON election: PROMOTED to primary at term {} ***", term);
    info!(target: "election", "Node {} is now accepting writes", candidate_id);

    // A handover in flight is the new leader's to run: the deposed node's copy stopped with it, and
    // the coordinator polls this group, not that node.
    state.react_to_migration();
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
            candidate_url: None,
            transfer_from: None,
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
            candidate_url: None,
            transfer_from: None,
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
        let d = decide_vote(2, &None, &HashMap::new(), scalar(2, 100), None, &vote_req(3, "n1", 2, 100), false);
        assert!(d.granted);
        assert_eq!(d.term, 3);
        assert_eq!(d.voted_for.as_deref(), Some("n1"));
    }

    #[test]
    fn vote_denied_for_stale_candidate_term() {
        let d = decide_vote(5, &None, &HashMap::new(), scalar(5, 100), None, &vote_req(4, "n1", 5, 100), false);
        assert!(!d.granted);
        assert_eq!(d.term, 5);
        assert_eq!(d.voted_for, None);
    }

    #[test]
    fn vote_at_most_once_per_term() {
        let d1 = decide_vote(3, &None, &HashMap::new(), scalar(1, 50), None, &vote_req(3, "n1", 1, 50), false);
        assert!(d1.granted);
        assert_eq!(d1.voted_for.as_deref(), Some("n1"));

        let d2 = decide_vote(3, &d1.voted_for, &HashMap::new(), scalar(1, 50), None, &vote_req(3, "n2", 1, 50), false);
        assert!(!d2.granted, "must not vote for a second candidate in the same term");
        assert_eq!(d2.voted_for.as_deref(), Some("n1"));

        let d3 = decide_vote(3, &d1.voted_for, &HashMap::new(), scalar(1, 50), None, &vote_req(3, "n1", 1, 50), false);
        assert!(d3.granted, "re-voting for the same candidate is idempotent");
    }

    #[test]
    fn vote_denied_when_candidate_log_behind() {
        let behind_lsn = decide_vote(3, &None, &HashMap::new(), scalar(2, 100), None, &vote_req(4, "n1", 2, 99), false);
        assert!(!behind_lsn.granted, "candidate with lower lsn at same log term must lose");

        let behind_term = decide_vote(3, &None, &HashMap::new(), scalar(2, 100), None, &vote_req(4, "n1", 1, 500), false);
        assert!(!behind_term.granted, "candidate with lower last log term must lose even with higher lsn");

        let ahead = decide_vote(3, &None, &HashMap::new(), scalar(2, 100), None, &vote_req(4, "n1", 3, 1), false);
        assert!(ahead.granted, "higher last log term wins regardless of lsn");
    }

    #[test]
    fn higher_term_vote_resets_prior_vote() {
        let prior = Some("n2".to_string());
        let d = decide_vote(3, &prior, &HashMap::new(), scalar(1, 50), None, &vote_req(4, "n1", 1, 50), false);
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

        let d = decide_vote(4, &None, &mine, summary, None, &swapped, false);
        assert!(!d.granted,
            "the candidate is missing users 4..10; electing it would drop entries a quorum may hold");
        assert_eq!(d.term, 5, "the term still advances even though the vote is refused");
    }

    #[test]
    fn a_higher_term_on_one_collection_does_not_excuse_a_stale_other() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 11)]);

        let d = decide_vote(4, &None, &mine, scalar(4, 11), None,
            &per_log_req(5, "n1", &[("users", 4, 5), ("orders", 5, 12)]), false);
        assert!(!d.granted,
            "a newer term on orders says nothing about users, where this candidate is behind");
    }

    #[test]
    fn a_candidate_current_on_every_collection_wins() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 11)]);

        let equal = decide_vote(4, &None, &mine, scalar(4, 11), None,
            &per_log_req(5, "n1", &[("users", 4, 10), ("orders", 4, 11)]), false);
        assert!(equal.granted, "matching every log is up to date");

        let ahead = decide_vote(4, &None, &mine, scalar(4, 11), None,
            &per_log_req(5, "n2", &[("users", 5, 20), ("orders", 4, 11)]), false);
        assert!(ahead.granted, "ahead on one log and level on the rest is up to date");
    }

    #[test]
    fn a_collection_the_candidate_has_never_seen_makes_it_stale() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 3)]);

        let d = decide_vote(4, &None, &mine, scalar(4, 10), None,
            &per_log_req(5, "n1", &[("users", 4, 10)]), false);
        assert!(!d.granted, "a log absent from the candidate is one it holds nothing of");

        let empty_too = tails(&[("users", 4, 10), ("orders", 0, 0)]);
        let d = decide_vote(4, &None, &empty_too, scalar(4, 10), None,
            &per_log_req(5, "n2", &[("users", 4, 10)]), false);
        assert!(d.granted, "but an empty collection dir costs the candidate nothing");
    }

    #[test]
    fn collections_the_candidate_alone_holds_do_not_block_it() {
        let mine = tails(&[("users", 4, 10)]);

        let d = decide_vote(4, &None, &mine, scalar(4, 10), None,
            &per_log_req(5, "n1", &[("users", 4, 10), ("audit", 4, 99)]), false);
        assert!(d.granted, "only logs this voter holds can be lost, so extras are irrelevant");
    }

    #[test]
    fn a_voter_holding_no_log_grants_freely() {
        let d = decide_vote(0, &None, &HashMap::new(), LogTail::default(), None,
            &per_log_req(1, "n1", &[("users", 3, 40)]), false);
        assert!(d.granted, "a node with nothing to lose has no grounds to refuse");
    }

    #[test]
    fn a_peer_that_sends_no_summaries_falls_back_to_the_scalar_compare() {
        let mine = tails(&[("users", 4, 10), ("orders", 4, 3)]);

        let current = decide_vote(4, &None, &mine, scalar(4, 10), None, &vote_req(5, "n1", 4, 10), false);
        assert!(current.granted, "a pre-`logs` peer is still judged on the summary it does send");

        let behind = decide_vote(4, &None, &mine, scalar(4, 10), None, &vote_req(5, "n2", 4, 9), false);
        assert!(!behind.granted);

        let nothing = decide_vote(4, &None, &mine, scalar(4, 10), None, &vote_req(5, "n3", 0, 0), false);
        assert!(!nothing.granted, "an empty log must lose to a voter that holds entries");
    }

    #[test]
    fn a_candidate_the_voters_configuration_does_not_name_is_refused() {
        let config = Configuration::simple(vec!["http://a".into(), "http://b".into()]);
        let mut req = vote_req(5, "n3", 4, 10);

        req.candidate_url = Some("http://c".to_string());
        let d = decide_vote(4, &None, &HashMap::new(), LogTail::default(), Some(&config), &req, false);
        assert!(!d.granted,
            "a demoted node that has not heard about its demotion goes on campaigning, and this \
             vote would hand it a leadership over a set the cluster has left");
        assert_eq!(d.term, 5, "the term still advances; only the grant is withheld");

        req.candidate_url = Some("http://b".to_string());
        assert!(decide_vote(4, &None, &HashMap::new(), LogTail::default(), Some(&config), &req, false).granted);

        // Both directions of not knowing: no url from the peer, or no configuration here.
        req.candidate_url = None;
        assert!(decide_vote(4, &None, &HashMap::new(), LogTail::default(), Some(&config), &req, false).granted,
            "a peer that predates configuration entries must still be able to win an election");
        req.candidate_url = Some("http://c".to_string());
        assert!(decide_vote(4, &None, &HashMap::new(), LogTail::default(), None, &req, false).granted,
            "and a voter holding no configuration has nothing to judge the candidate against");
    }

    /// The promise a leader's lease is built on: withholding survives a higher term, a longer log,
    /// and an unspent vote, or the leases handed out on it are not leases.
    #[test]
    fn a_voter_with_fresh_leader_contact_withholds_its_vote_but_still_takes_the_term() {
        let req = vote_req(9, "n1", 4, 500);
        let mine: HashMap<String, LogTail> = HashMap::new();

        let withheld = decide_vote(4, &None, &mine, scalar(4, 10), None, &req, true);
        assert!(!withheld.granted,
            "a read is being answered on this node's promise not to vote for the next candidate");
        assert_eq!(withheld.term, 9, "refusing is not following: the term still advances");
        assert_eq!(withheld.voted_for, None, "and the term it advances to has its vote unspent");

        assert!(decide_vote(4, &None, &mine, scalar(4, 10), None, &req, false).granted,
            "the same request wins once the leader has gone quiet, which is the whole liveness story");
    }

    #[test]
    fn a_pre_vote_answers_the_same_question_the_real_vote_would() {
        let mine: HashMap<String, LogTail> = HashMap::new();

        assert!(decide_pre_vote(4, &mine, scalar(4, 100), None, &vote_req(5, "n1", 4, 100), false));
        assert!(!decide_pre_vote(4, &mine, scalar(4, 100), None, &vote_req(5, "n1", 4, 99), false),
            "a candidate that would lose the real vote on its log is not worth encouraging to stand");

        let config = Configuration::simple(vec!["http://a".into(), "http://b".into()]);
        let mut stranger = vote_req(5, "n3", 4, 100);
        stranger.candidate_url = Some("http://c".to_string());
        assert!(!decide_pre_vote(4, &mine, scalar(4, 100), Some(&config), &stranger, false),
            "nor one this voter's configuration does not name");
    }

    #[test]
    fn a_pre_vote_at_or_below_our_own_term_is_refused() {
        let mine: HashMap<String, LogTail> = HashMap::new();

        assert!(!decide_pre_vote(5, &mine, scalar(4, 100), None, &vote_req(5, "n1", 4, 100), false),
            "our own term may already hold this node's vote, so there is nothing to pre-approve");
        assert!(!decide_pre_vote(5, &mine, scalar(4, 100), None, &vote_req(4, "n1", 4, 100), false));
        assert!(decide_pre_vote(5, &mine, scalar(4, 100), None, &vote_req(6, "n1", 4, 100), false),
            "and a term above ours is the one a candidate would actually stand at");
    }

    /// C20's fix. The refusal a leader's lease rests on now happens before any term moves, so a
    /// candidate that cannot win stops costing the group an election every cycle.
    #[test]
    fn a_voter_that_owes_a_leader_silence_refuses_the_pre_vote() {
        let mine: HashMap<String, LogTail> = HashMap::new();
        let req = vote_req(9, "n1", 9, 500);

        assert!(decide_pre_vote(4, &mine, scalar(4, 10), None, &req, false),
            "a fresher candidate takes this outright once the leader has gone quiet");
        assert!(!decide_pre_vote(4, &mine, scalar(4, 10), None, &req, true));
    }

    /// A node isolated from its voters used to raise its term on every cycle, so by the time it
    /// rejoined it deposed whatever leader had been elected without it. Nothing rate-limited that.
    #[tokio::test]
    async fn an_isolated_candidate_stops_inflating_its_term() {
        let root = crate::test_support::temp_root();
        let port = crate::test_support::next_test_port();
        let mut node = crate::test_support::TestNode::new("lonely", port, &root, "replica");
        node.primary_addr = Some("http://127.0.0.1:1".to_string());
        node.start();
        let state = node.state.clone().unwrap();

        // Two voters that do not exist: a majority of three this node can never reach.
        state.install_configuration(Configuration::simple(vec![
            state.own_url(),
            format!("http://127.0.0.1:{}", crate::test_support::next_test_port()),
            format!("http://127.0.0.1:{}", crate::test_support::next_test_port()),
        ]));
        assert!(state.in_quorum(), "vacuous unless this node would really stand");

        let before = state.current_term();
        for _ in 0..3 {
            run_election(&state, 0, None).await;
        }

        assert_eq!(state.current_term(), before,
            "three rounds of standing used to be three terms, each one deposing a live leader");
        assert!(!state.is_leader(), "and it must not be able to elect itself either");

        node.kill();
    }

    /// A peer with no route for the endpoint, which is what a node predating pre-vote answers with.
    async fn serve_nothing(port: u16) {
        let app = axum::Router::new()
            .route("/elsewhere", axum::routing::get(|| async { "" }));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
    }

    /// Pre-vote is an optimization, so a peer that has never heard of it counts as willing. The real
    /// vote gets the opposite treatment, because there nobody answered.
    #[tokio::test]
    async fn a_peer_that_does_not_know_the_pre_vote_endpoint_is_counted_as_willing() {
        let root = crate::test_support::temp_root();
        let mut node = crate::test_support::TestNode::new(
            "candidate", crate::test_support::next_test_port(), &root, "replica");
        node.start();
        let state = node.state.clone().unwrap();

        let old_port = crate::test_support::next_test_port();
        serve_nothing(old_port).await;
        let old_peer = format!("http://127.0.0.1:{}", old_port);
        let quorum = Configuration::simple(vec![state.own_url(), old_peer.clone()]);
        let req = vote_request(9, "candidate", &HashMap::new(), LogTail::default(), &state.own_url(), None);
        let deadline = Duration::from_secs(3);

        let (willing, _) = ask_peers(&state, &req, vec![old_peer.clone()], &quorum,
            "/internal/pre-vote", true, deadline).await;
        assert!(quorum.has_quorum(&willing),
            "failing open on a 404 is what keeps a rolling upgrade able to elect anything");

        let (granted, _) = ask_peers(&state, &req, vec![old_peer], &quorum,
            "/internal/vote", false, deadline).await;
        assert!(!quorum.has_quorum(&granted), "a real vote nobody answered is not a vote");

        node.kill();
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

    fn shard_config(root: &std::path::Path) -> crate::config::NodeConfig {
        serde_json::from_value(serde_json::json!({
            "node_id": "n1", "role": "shard", "shard_role": "primary",
            "listen_addr": "127.0.0.1:1",
            "data_dir": root.to_string_lossy(),
        })).unwrap()
    }

    #[tokio::test]
    async fn the_summary_is_the_log_tail_not_fsync_progress() {
        use crate::storage::Database;
        use std::sync::Arc;

        let root = crate::test_support::temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let users = db.get_collection("users").unwrap();
        let orders = db.get_collection("orders").unwrap();
        users.put("a".into(), serde_json::json!({"v": 1}), 4).unwrap();
        let tail_lsn = orders.put("b".into(), serde_json::json!({"v": 2}), 7).unwrap().3;

        let state = AppState::for_admission_test(shard_config(&root), db.clone(), true);
        // What the pre-fix code sampled: fsync progress, which trails every one of those appends.
        db.durable_lsn.store(0, Ordering::SeqCst);

        let my_logs = local_log_tails(&state);
        assert_eq!(log_summary(&state, &my_logs), LogTail { last_term: 7, last_lsn: tail_lsn },
            "the summary must name the newest log tail, not the fsynced prefix, or a candidate              advertises itself as behind and a staler peer wins the vote");
    }

    #[tokio::test]
    async fn a_summary_taken_before_the_tails_misses_a_collection() {
        use crate::storage::Database;
        use std::sync::Arc;

        let root = crate::test_support::temp_root();
        let db = Arc::new(Database::new(&root).unwrap());
        let state = AppState::for_admission_test(shard_config(&root), db.clone(), true);

        // A collection nothing has opened yet: only local_log_tails reaches it.
        std::fs::create_dir_all(root.join("late")).unwrap();
        let stale = log_summary(&state, &HashMap::new());

        let my_logs = local_log_tails(&state);
        db.get_collection("late").unwrap().put("k".into(), serde_json::json!({"v": 1}), 3).unwrap();
        let after = log_summary(&state, &local_log_tails(&state));

        assert_eq!(stale, LogTail::default());
        assert!(my_logs.contains_key("late"), "local_log_tails must open collections on disk");
        assert!(after > stale, "sampling the summary after the tails is what makes it complete");
    }

}
