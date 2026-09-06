//! ReadIndex: what a leader has to establish before it may answer a read from local state.

use crate::state::AppState;
use crate::storage::Collection;
use futures::future::join_all;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

const CONFIRM_TIMEOUT: Duration = Duration::from_secs(2);
const APPLY_TIMEOUT: Duration = Duration::from_secs(2);
const APPLY_POLL_MS: u64 = 2;

#[derive(Debug, PartialEq, Eq)]
pub enum ReadRefusal {
    /// Either never the leader, or deposed while establishing the index.
    NotLeader,
    /// Leadership too new: entries this node inherited are not yet committed in its own term, so
    /// its commit index is a lower bound rather than the answer.
    TermTooNew,
    /// No majority answered, so nothing rules out a leader at a higher term.
    Unconfirmed,
    /// Confirmed, but the index did not become visible in time.
    ApplyTimeout,
}

impl ReadRefusal {
    pub fn message(&self) -> &'static str {
        match self {
            ReadRefusal::NotLeader => "this node is not the primary; retry, or ask for read=replica",
            ReadRefusal::TermTooNew => "leadership is too new to answer a quorum read; retry",
            ReadRefusal::Unconfirmed => "could not confirm leadership with a quorum; retry",
            ReadRefusal::ApplyTimeout => "leadership confirmed but the read index is not yet visible; retry",
        }
    }
}

/// Whether the leader has committed an entry of its own term for this collection. Raft §6.4: until
/// it has, `commitIndex` can sit below entries a previous leader committed and this node holds
/// staged, so a read at that index would miss them. An empty staging buffer says the same thing
/// another way -- everything durable here is already applied.
fn index_is_complete(state: &AppState, collection: &str, col: &Collection) -> bool {
    col.pending_len() == 0 || state.has_current_term_commit(collection)
}

/// One round of heartbeats, counted by identity. A voter reporting a term at or below ours has not
/// moved to a higher one, and a leader elected above ours would have needed a majority to do so --
/// two majorities intersect, so a majority answering at our term rules that out.
async fn confirm_with_quorum(state: &AppState) -> Result<(), ReadRefusal> {
    let config = state.quorum_config();
    let own = state.own_url();
    let term = state.current_term();

    let mut granted = vec![own.clone()];
    if config.has_quorum(&granted) {
        return Ok(());
    }

    let peers: Vec<String> = state.voting_peers();
    let probes = peers.iter().map(|url| {
        let client = state.client.clone();
        let endpoint = format!("{}/internal/heartbeat", url);
        async move {
            let body = client.get(&endpoint).timeout(CONFIRM_TIMEOUT).send().await.ok()?
                .json::<serde_json::Value>().await.ok()?;
            Some((body.get("term").and_then(|t| t.as_u64())?,
                  body.get("role").and_then(|r| r.as_str()) == Some("primary")))
        }
    });

    let mut highest = term;
    for (url, answer) in peers.iter().zip(join_all(probes).await) {
        let (their_term, claims_primary) = match answer {
            Some(a) => a,
            None => continue,
        };
        highest = highest.max(their_term);
        // A peer claiming leadership at our own term is a split brain, not a confirmation.
        if their_term <= term && !(their_term == term && claims_primary) {
            granted.push(url.clone());
        }
    }

    if highest > term {
        warn!(target: "read_index", term, highest,
            "A voter reports a higher term; stepping down instead of answering the read");
        super::failover::demote(state, highest).await;
        return Err(ReadRefusal::NotLeader);
    }

    if config.has_quorum(&granted) {
        Ok(())
    } else {
        Err(ReadRefusal::Unconfirmed)
    }
}

/// The lease first: it establishes exactly what the round would -- that no voter can have elected
/// anyone else -- and the polls that paid for it were being sent anyway.
async fn confirm_leadership(state: &AppState) -> Result<(), ReadRefusal> {
    if state.holds_read_lease() {
        return Ok(());
    }
    confirm_with_quorum(state).await?;
    // A demotion during the round means the confirmation was for a term this node no longer holds.
    if state.is_leader() {
        Ok(())
    } else {
        Err(ReadRefusal::NotLeader)
    }
}

async fn wait_for_applied(col: &Arc<Collection>, index: u64) -> Result<(), ReadRefusal> {
    let deadline = Instant::now() + APPLY_TIMEOUT;
    while col.applied_lsn() < index {
        if Instant::now() >= deadline {
            return Err(ReadRefusal::ApplyTimeout);
        }
        tokio::time::sleep(Duration::from_millis(APPLY_POLL_MS)).await;
    }
    Ok(())
}

/// Establishes the point a linearizable read may be answered at, and returns it once local state
/// has caught up to it. The order is Raft's and none of it is optional: the index is sampled before
/// leadership is confirmed so a write landing during a round cannot be required, the confirmation
/// rules out a newer leader, and the wait makes the index readable and not merely known.
pub async fn read_index(state: &AppState, collection: &str) -> Result<u64, ReadRefusal> {
    if !state.is_leader() {
        return Err(ReadRefusal::NotLeader);
    }

    let index = match state.db.as_ref().and_then(|db| db.existing_collection(collection)) {
        // Nothing has ever been written here, so there is no entry a read could be behind on.
        None => {
            confirm_leadership(state).await?;
            0
        },
        Some(col) => {
            if !index_is_complete(state, collection, &col) {
                return Err(ReadRefusal::TermTooNew);
            }
            let index = state.committed_lsn(collection);
            confirm_leadership(state).await?;
            wait_for_applied(&col, index).await?;
            index
        },
    };

    // Re-read at the end: every check above is in the past, and both of the steps that await can
    // span a vote this node grants, which deposes it.
    if !state.is_leader() {
        return Err(ReadRefusal::NotLeader);
    }
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::frame::Configuration;
    use crate::test_support::{next_test_port, put_doc_http, temp_root, wait_for, TestNode};

    /// A stub voter, answering the one field the confirmation reads, plus whatever silence it is
    /// told to grant a probing leader.
    async fn serve_voter(term: u64, role: &'static str, novote_ms: u64, port: u16) {
        let app = axum::Router::new().route(
            "/internal/heartbeat",
            axum::routing::get(move || async move {
                axum::Json(serde_json::json!({
                    "term": term, "role": role, "novote_ms": novote_ms }))
            }),
        );
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
    }

    async fn serve_term(term: u64, role: &'static str, port: u16) {
        serve_voter(term, role, 0, port).await;
    }

    #[tokio::test]
    async fn a_lone_leader_is_its_own_quorum_and_answers_without_a_round_trip() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        assert!(put_doc_http(&client, &leader.url(), "k1", 1).await.is_success());

        let index = read_index(&state, "t").await.expect("a majority of one is itself");
        assert!(index > 0, "the write is committed, so the read index has to cover it");

        leader.kill();
    }

    #[tokio::test]
    async fn a_read_on_a_collection_that_was_never_written_still_confirms_leadership() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.start();
        let state = leader.state.clone().unwrap();

        assert_eq!(read_index(&state, "never").await, Ok(0),
            "there is no entry to be behind on, but the answer still comes from a confirmed leader");
        assert!(!leader.data_dir.join("never").exists(),
            "and probing must not leave an empty collection behind on every node");

        leader.kill();
    }

    /// Raft §6.4's first step. Staged entries with nothing of this term committed means the commit
    /// index is a floor, not the answer -- a read there would miss what a previous leader committed
    /// and this node has not published yet. C17's promotion barrier is what clears it.
    #[tokio::test]
    async fn a_leader_with_an_unpublished_tail_refuses_until_its_own_term_commits() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.start();
        let state = leader.state.clone().unwrap();
        let col = state.db.as_ref().unwrap().get_collection("t").unwrap();

        // Staged directly, so no `note_leader_append` and no term floor -- the shape a promotion
        // leaves behind before the barrier lands.
        let staged = col.put("k".into(), serde_json::json!({"v": 1}), state.current_term()).unwrap().3;
        assert!(staged > 0);
        assert!(!state.has_current_term_commit("t"));

        assert_eq!(read_index(&state, "t").await, Err(ReadRefusal::TermTooNew),
            "answering here would read a commit index that has not absorbed the inherited tail");

        col.enqueue_commit().await.unwrap().unwrap();
        state.note_leader_append("t", staged);
        state.advance_own_commit("t", col.durable_lsn()).unwrap();
        assert!(state.has_current_term_commit("t"));

        assert!(read_index(&state, "t").await.is_ok(),
            "and once an entry of this term is committed the refusal has to clear on its own");

        leader.kill();
    }

    #[tokio::test]
    async fn a_follower_never_establishes_a_read_index() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.primary_addr = Some("http://127.0.0.1:1".to_string());
        replica.start();

        assert_eq!(read_index(replica.state.as_ref().unwrap(), "t").await, Err(ReadRefusal::NotLeader));

        replica.kill();
    }

    #[tokio::test]
    async fn a_leader_whose_voters_are_gone_refuses_rather_than_answering() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        assert!(put_doc_http(&client, &leader.url(), "k1", 1).await.is_success());

        // Two peers that do not exist: a majority of three cannot be reached.
        state.install_configuration(Configuration::simple(vec![
            state.own_url(),
            format!("http://127.0.0.1:{}", next_test_port()),
            format!("http://127.0.0.1:{}", next_test_port()),
        ]));

        assert_eq!(read_index(&state, "t").await, Err(ReadRefusal::Unconfirmed),
            "a partitioned leader that answers is exactly the stale read this exists to stop");

        leader.kill();
    }

    /// Records what a probe of `voter` would have brought back, from a peer that does not have to
    /// exist for it: the grant is the whole of the evidence.
    fn grant(state: &AppState, voter: &str, term: u64, ms: u64) {
        state.note_lease_grant(voter, term, Instant::now(), std::time::SystemTime::now(),
            Duration::from_millis(ms));
    }

    /// A solo leader whose two voters are unreachable ports: the same setup that has to refuse a
    /// read while it is asking them, and may answer it once they have answered in advance.
    async fn leader_with_absent_voters(root: &std::path::Path) -> (TestNode, Vec<String>) {
        let mut leader = TestNode::new("solo", next_test_port(), root, "primary");
        leader.heartbeat_timeout_secs = 30;
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        assert!(put_doc_http(&client, &leader.url(), "k1", 1).await.is_success());

        let voters: Vec<String> = (0..2).map(|_| format!("http://127.0.0.1:{}", next_test_port())).collect();
        let mut all = vec![state.own_url()];
        all.extend(voters.iter().cloned());
        state.install_configuration(Configuration::simple(all));
        (leader, voters)
    }

    #[tokio::test]
    async fn a_promised_majority_answers_the_read_with_no_round_at_all() {
        let root = temp_root();
        let (mut leader, voters) = leader_with_absent_voters(&root).await;
        let state = leader.state.clone().unwrap();
        let term = state.current_term();

        assert_eq!(read_index(&state, "t").await, Err(ReadRefusal::Unconfirmed),
            "nothing is reachable, so a read that has to ask cannot be answered at all");

        for voter in &voters {
            grant(&state, voter, term, 10_000);
        }

        let index = read_index(&state, "t").await
            .expect("a majority that has promised not to vote is a majority that cannot elect");
        assert!(index > 0);

        leader.kill();
    }

    /// The plumbing end to end: the leader's own probes collect the grants and its reads stop
    /// asking. Killing the follower is what tells a lease from a round -- a round would fail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_real_leaders_probes_are_the_lease_and_it_outlives_the_voter() {
        let root = temp_root();
        let (p1, p2) = (next_test_port(), next_test_port());
        let (u1, u2) = (format!("http://127.0.0.1:{}", p1), format!("http://127.0.0.1:{}", p2));

        let mut leader = TestNode::new("n1", p1, &root, "primary");
        leader.heartbeat_timeout_secs = 30;
        leader.peers = vec![u2.clone()];
        leader.replicas = vec![u2.clone()];
        let mut follower = TestNode::new("n2", p2, &root, "replica");
        follower.heartbeat_timeout_secs = 30;
        follower.peers = vec![u1.clone()];
        follower.primary_addr = Some(u1.clone());
        leader.start();
        follower.start();

        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        assert!(put_doc_http(&client, &leader.url(), "k1", 1).await.is_success());

        assert!(wait_for(Duration::from_secs(10), || state.holds_read_lease()).await,
            "a leader probing twice a second and being granted each time has to add up to a lease");

        follower.kill();
        assert!(read_index(&state, "t").await.is_ok(),
            "the promise was for a window, not for as long as the node making it stays up");

        leader.kill();
    }

    /// The lease is a claim that no election can complete, and a handover is this node arranging
    /// for exactly that. So it surrenders the lease first, and reads go back to asking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_leader_handing_office_over_stops_resting_reads_on_its_leases() {
        let root = temp_root();
        let (p1, p2) = (next_test_port(), next_test_port());
        let (u1, u2) = (format!("http://127.0.0.1:{}", p1), format!("http://127.0.0.1:{}", p2));

        let mut leader = TestNode::new("n1", p1, &root, "primary");
        leader.heartbeat_timeout_secs = 30;
        leader.peers = vec![u2.clone()];
        leader.replicas = vec![u2.clone()];
        let mut follower = TestNode::new("n2", p2, &root, "replica");
        follower.heartbeat_timeout_secs = 30;
        follower.peers = vec![u1.clone()];
        follower.primary_addr = Some(u1.clone());
        leader.start();
        follower.start();

        let state = leader.state.clone().unwrap();
        assert!(wait_for(Duration::from_secs(10), || state.holds_read_lease()).await,
            "vacuous unless there is a lease to give up");

        state.set_handing_over(true);
        assert!(!state.holds_read_lease(),
            "the voter this node is about to tell to stand will vote past the promise the lease              is made of, and nothing reports that back before the vote arrives");

        // And the probes running underneath must not put one back while it is in flight.
        assert!(!wait_for(Duration::from_secs(3), || state.holds_read_lease()).await,
            "a grant recorded during the handover restores exactly the lease that was surrendered");

        state.set_handing_over(false);
        assert!(wait_for(Duration::from_secs(10), || state.holds_read_lease()).await,
            "an abandoned handover leaves this node leading, so its reads have to get cheap again");

        follower.kill();
        leader.kill();
    }

    /// The leader's half of the round over the wire: it has to ask for the window, and read the
    /// grant back out of the reply, without a real follower's contact clock in the way.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_leaders_own_probe_collects_the_grant_and_the_read_stops_asking() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.heartbeat_timeout_secs = 30;
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        assert!(put_doc_http(&client, &leader.url(), "k1", 1).await.is_success());

        let term = state.current_term();
        let mut voters = vec![state.own_url()];
        for _ in 0..2 {
            let port = next_test_port();
            serve_voter(term, "replica", 20_000, port).await;
            voters.push(format!("http://127.0.0.1:{}", port));
        }
        state.install_configuration(Configuration::simple(voters));

        assert!(wait_for(Duration::from_secs(10), || state.holds_read_lease()).await,
            "the probe carries the ask and the reply carries the grant, or neither name is right");
        assert!(read_index(&state, "t").await.is_ok());

        leader.kill();
    }

    /// A probe outliving the term it went out at. The transition that ended that term dropped
    /// every lease with it, and a reply landing afterwards must not put one back.
    #[tokio::test]
    async fn a_grant_answering_a_probe_from_an_older_term_is_not_a_lease() {
        let root = temp_root();
        let (mut leader, voters) = leader_with_absent_voters(&root).await;
        let state = leader.state.clone().unwrap();
        state.replication.as_ref().unwrap().write().unwrap().term = 5;

        for voter in &voters {
            grant(&state, voter, 4, 10_000);
        }
        assert_eq!(read_index(&state, "t").await, Err(ReadRefusal::Unconfirmed));

        for voter in &voters {
            grant(&state, voter, 5, 10_000);
        }
        assert!(read_index(&state, "t").await.is_ok(), "and the same grant at our own term is one");

        leader.kill();
    }

    #[tokio::test]
    async fn a_lease_expires_rather_than_standing_until_something_contradicts_it() {
        let root = temp_root();
        let (mut leader, voters) = leader_with_absent_voters(&root).await;
        let state = leader.state.clone().unwrap();
        let term = state.current_term();

        for voter in &voters {
            grant(&state, voter, term, 300);
        }
        assert!(read_index(&state, "t").await.is_ok());

        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(read_index(&state, "t").await, Err(ReadRefusal::Unconfirmed),
            "a voter is free again the moment its promise runs out, and so is the leader");

        leader.kill();
    }

    #[tokio::test]
    async fn a_voter_reporting_a_higher_term_deposes_the_reader_instead_of_answering_it() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.heartbeat_timeout_secs = 30;
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        assert!(put_doc_http(&client, &leader.url(), "k1", 1).await.is_success());

        let ahead = next_test_port();
        serve_term(state.current_term() + 5, "primary", ahead).await;
        state.install_configuration(Configuration::simple(vec![
            state.own_url(),
            format!("http://127.0.0.1:{}", ahead),
        ]));

        assert_eq!(read_index(&state, "t").await, Err(ReadRefusal::NotLeader));
        assert!(!state.is_leader(), "learning of a higher term is a demotion, not just a refusal");

        leader.kill();
    }

    #[tokio::test]
    async fn a_voter_claiming_our_own_term_is_not_a_confirmation() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.heartbeat_timeout_secs = 30;
        leader.start();
        let state = leader.state.clone().unwrap();
        let client = reqwest::Client::new();
        assert!(put_doc_http(&client, &leader.url(), "k1", 1).await.is_success());

        let twin = next_test_port();
        serve_term(state.current_term(), "primary", twin).await;
        state.install_configuration(Configuration::simple(vec![
            state.own_url(),
            format!("http://127.0.0.1:{}", twin),
            format!("http://127.0.0.1:{}", next_test_port()),
        ]));

        assert_eq!(read_index(&state, "t").await, Err(ReadRefusal::Unconfirmed),
            "two leaders in one term is a broken invariant; counting one as a voter hides it");
        assert!(state.is_leader(), "and it is not a higher term, so there is nothing to step down to");

        leader.kill();
    }
}
