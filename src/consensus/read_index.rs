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
/// the confirmation so a write that lands during the round cannot be required, the confirmation
/// rules out a newer leader, and the wait is what makes the index readable rather than merely known.
pub async fn read_index(state: &AppState, collection: &str) -> Result<u64, ReadRefusal> {
    if !state.is_leader() {
        return Err(ReadRefusal::NotLeader);
    }

    let col = match state.db.as_ref().and_then(|db| db.existing_collection(collection)) {
        Some(col) => col,
        // Nothing has ever been written here, so there is no entry a read could be behind on.
        None => return confirm_with_quorum(state).await.map(|_| 0),
    };

    if !index_is_complete(state, collection, &col) {
        return Err(ReadRefusal::TermTooNew);
    }

    let index = state.committed_lsn(collection);
    confirm_with_quorum(state).await?;

    // Re-checked after the round: a demotion during it means the confirmation was for a term this
    // node no longer holds.
    if !state.is_leader() {
        return Err(ReadRefusal::NotLeader);
    }

    wait_for_applied(&col, index).await?;
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::frame::Configuration;
    use crate::test_support::{next_test_port, put_doc_http, temp_root, TestNode};
    use std::fs;

    /// A stub voter, answering the one field the confirmation reads.
    async fn serve_term(term: u64, role: &'static str, port: u16) {
        let app = axum::Router::new().route(
            "/internal/heartbeat",
            axum::routing::get(move || async move {
                axum::Json(serde_json::json!({"term": term, "role": role}))
            }),
        );
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
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
        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
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
        state.advance_own_commit("t", col.durable_lsn());
        assert!(state.has_current_term_commit("t"));

        assert!(read_index(&state, "t").await.is_ok(),
            "and once an entry of this term is committed the refusal has to clear on its own");

        leader.kill();
        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
    }
}
