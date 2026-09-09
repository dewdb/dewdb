//! Membership changes as two log entries: the joint configuration, then the target.
//!
//! Raft §6. The joint entry names both voting sets and is in force from the moment it is appended,
//! so between it and the target every decision needs a majority of each half. That overlap is what
//! makes the change safe: there is no instant at which the leaving half and the joining half can
//! each reach a majority on their own, which is what a one-shot swap of the voter list allows.

use crate::consensus::config::CONFIG_LOG;
use crate::consensus::transfer;
use crate::replication::stream::replicate_and_await;
use crate::replication::write_concern::WriteQuorum;
use crate::state::AppState;
use crate::storage::frame::Configuration;
use crate::storage::FrameHeader;
use crate::util::same_endpoint;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Per entry, not per change: a change is two of these and pays it twice in the worst case.
const CONFIG_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);
const COMMIT_POLL_MS: u64 = 20;

#[derive(Default)]
pub struct MembershipChanges {
    pub gate: tokio::sync::Mutex<()>,
    #[cfg(test)]
    before_append: std::sync::Mutex<Option<std::sync::Arc<AppendPause>>>,
}

#[cfg(test)]
#[derive(Default)]
struct AppendPause {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

fn require_leader(state: &AppState, term: u64) -> Result<(), ChangeError> {
    if !state.is_leader() || state.current_term() != term || !state.in_quorum() {
        return Err(ChangeError::Refused("leadership changed; send the change to the current leader".into()));
    }
    Ok(())
}

impl ChangeError {
    pub fn why(&self) -> &str {
        match self {
            ChangeError::Refused(why) | ChangeError::Stalled(why) | ChangeError::Redirect(why) => why,
        }
    }
}

#[derive(Debug)]
pub enum ChangeError {
    /// The caller can retry elsewhere or later; nothing was appended.
    Refused(String),
    /// An entry is in the log and did not commit. The change is neither applied nor undone, and
    /// the next leader finishes or replaces it from the log.
    Stalled(String),
    /// The change removes this node, so leadership moved to the named voter and the change belongs
    /// there now. Nothing was appended here; the same request against that node applies it.
    Redirect(String),
}

/// The newest configuration entry when it has not committed yet, which is what makes a second
/// change unsafe to start: the quorum deciding it is not yet the quorum a third one would name.
pub fn pending_change(state: &AppState) -> Option<Configuration> {
    let col = state.db.as_ref()?.existing_collection(CONFIG_LOG)?;
    let latest = col.latest_config()?;
    (Some(&latest) != col.committed_config().as_ref()).then_some(latest)
}

/// Whether `next` is the change already in flight, which a retry is allowed to finish rather than
/// being refused as a second concurrent change.
fn resumes(current: &Configuration, next: &[String]) -> bool {
    current.is_joint()
        && current.voters.len() == next.len()
        && next.iter().all(|v| current.voters.iter().any(|w| same_endpoint(w, v)))
}

fn validate(state: &AppState, current: &Configuration, next: &[String]) -> Result<(), ChangeError> {
    if next.is_empty() {
        return Err(ChangeError::Refused("a configuration needs at least one voter".to_string()));
    }
    // Refused rather than deduped, so the caller learns the set it asked for is not the set it
    // named. `node_key` is the rule `ClusterMetadata::validate` already applies to the member list.
    let mut seen = std::collections::HashSet::new();
    for voter in next {
        if !seen.insert(crate::util::node_key(voter)) {
            return Err(ChangeError::Refused(format!(
                "{} appears more than once; each node counts once toward a majority", voter)));
        }
    }
    if current.is_joint() && !resumes(current, next) {
        return Err(ChangeError::Refused(format!(
            "a change to {:?} is already in flight; finish or retry that one first", current.voters)));
    }
    if pending_change(state).is_some() {
        return Err(ChangeError::Refused(
            "the previous configuration entry has not committed yet; retry".to_string()));
    }
    let view = state.cluster_view();
    for voter in next {
        if current.contains(voter) {
            continue;
        }
        // A node nobody has been shipping frames to holds nothing, so admitting it to the quorum
        // narrows every majority to the nodes that do hold entries until it catches up.
        match view.member(voter) {
            Some(m) if m.role == "shard" => {},
            Some(_) => return Err(ChangeError::Refused(format!("{} is not a shard node", voter))),
            None => return Err(ChangeError::Refused(format!(
                "{} is not a member; admit it as a learner first so it can catch up", voter))),
        }
    }
    Ok(())
}

/// Appends one configuration entry and waits for it to commit. The entry is in force before this
/// returns either way: `refresh_configuration` runs on the append, not on the commit.
async fn append_and_commit(state: &AppState, config: Configuration, term: u64) -> Result<u64, ChangeError> {
    require_leader(state, term)?;
    let db = state.db.as_ref()
        .ok_or_else(|| ChangeError::Refused("this node has no storage".to_string()))?;
    let col = db.get_collection(CONFIG_LOG)
        .map_err(|e| ChangeError::Refused(e.to_string()))?;

    // Sampled before the append, which is what makes them new: the entry is in force the moment it
    // lands, and a cursor seeded at our tail would leave a node that holds nothing looking current.
    let previous = state.quorum_config();
    let joining: Vec<String> = config.members().into_iter()
        .filter(|m| !previous.contains(m))
        .collect();

    #[cfg(test)]
    {
        let pause = state.membership_changes.before_append.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
    }
    require_leader(state, term)?;
    let appended = {
        let col = col.clone();
        let config = config.clone();
        tokio::task::spawn_blocking(move || col.configure(config, term)).await
    };
    let (frame, _wal_id, _offset, lsn) = match appended {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => return Err(ChangeError::Refused(e.to_string())),
        Err(e) => return Err(ChangeError::Refused(e.to_string())),
    };

    // Before anything decides anything: from here the new membership is what counts votes and acks.
    state.refresh_configuration();
    state.note_leader_append(CONFIG_LOG, lsn);
    for member in &joining {
        state.begin_tracking_learner(member);
    }

    let commit = col.enqueue_commit();
    let prev_lsn = FrameHeader::parse(&frame).map_or(0, |h| h.prev_lsn);
    let commit_index = state.committed_lsn(CONFIG_LOG);
    let quorum = WriteQuorum::Majority(state.quorum_config());

    let replicating = replicate_and_await(
        state.clone(), CONFIG_LOG.to_string(), frame, term, commit_index, lsn, prev_lsn,
        quorum, CONFIG_COMMIT_TIMEOUT,
    );
    let (_holders, committed) = tokio::join!(replicating, commit);
    match committed {
        Ok(Ok(())) => {},
        Ok(Err(e)) => return Err(ChangeError::Stalled(e)),
        Err(e) => return Err(ChangeError::Stalled(e.to_string())),
    }
    state.advance_own_commit(CONFIG_LOG, col.durable_lsn())
        .map_err(|e| ChangeError::Stalled(e.to_string()))?;

    let deadline = Instant::now() + CONFIG_COMMIT_TIMEOUT;
    while state.committed_lsn(CONFIG_LOG) < lsn {
        if Instant::now() >= deadline || !state.is_leader() || state.current_term() != term {
            return Err(ChangeError::Stalled(format!(
                "configuration entry {} is durable but did not reach a quorum", lsn)));
        }
        tokio::time::sleep(Duration::from_millis(COMMIT_POLL_MS)).await;
    }
    state.apply_committed(CONFIG_LOG, state.committed_lsn(CONFIG_LOG))
        .map_err(|e| ChangeError::Stalled(e.to_string()))?;
    require_leader(state, term).map_err(|e| ChangeError::Stalled(e.why().to_string()))?;
    Ok(lsn)
}

/// A change that removes this node. Raft allows the leader to append it and requires it to step
/// down once the target commits -- on a log this node would by then have no standing to replicate.
/// So leadership moves first, to a voter the change keeps, and the change follows it there.
///
/// Nothing is appended either way, so a failure leaves the caller exactly where the refusal used to:
/// still leading, with the same request to send somewhere else (bugs.md C21).
async fn hand_over_first(state: &AppState, next: &[String]) -> Result<Configuration, ChangeError> {
    let eligible = transfer::eligible_targets(state, next);
    let Some(target) = transfer::best_target(state, &eligible) else {
        return Err(ChangeError::Refused(
            "this change removes the leader and names no other current voter to hand office to; add one as a voter first".to_string()));
    };

    info!(target: "membership", to = %target, voters = ?next,
        "The change removes this node; handing leadership over so it can be made there");
    match transfer::transfer_leadership(state, Some(target)).await {
        Ok(leader) => Err(ChangeError::Redirect(leader)),
        Err(e) => Err(ChangeError::Refused(format!(
            "this change removes the leader and leadership could not be handed over: {}", e.why()))),
    }
}

/// Moves the voting set to `next`. On `Stalled` the joint entry may be in the log and in force;
/// that is a legal state to be in, and the next leader completes it from the log rather than
/// rolling it back.
pub async fn change_membership(
    state: &AppState,
    next: Vec<String>,
) -> Result<Configuration, ChangeError> {
    let state = state.clone();
    // Client cancellation must not release the gate while a blocking append can still finish.
    tokio::spawn(async move {
        let _change = state.membership_changes.gate.lock().await;
        let term = state.current_term();
        change_membership_locked(&state, next, term).await
    }).await.map_err(|e| ChangeError::Stalled(e.to_string()))?
}

async fn change_membership_locked(
    state: &AppState,
    next: Vec<String>,
    term: u64,
) -> Result<Configuration, ChangeError> {
    require_leader(state, term)?;
    let current = state.quorum_config();
    validate(state, &current, &next)?;
    if !next.iter().any(|v| same_endpoint(v, &state.own_url())) {
        return hand_over_first(state, &next).await;
    }

    let target = Configuration::simple(next);
    if !current.is_joint()
        && target.voters.len() == current.voters.len()
        && target.voters.iter().all(|v| current.contains(v))
    {
        return Ok(current);
    }

    if !resumes(&current, &target.voters) {
        let joint = Configuration::joint(current.voters.clone(), target.voters.clone());
        info!(target: "membership", from = ?current.voters, to = ?target.voters,
            "Entering joint consensus");
        append_and_commit(state, joint, term).await?;
    }

    info!(target: "membership", voters = ?target.voters, "Joint configuration committed; leaving it");
    match append_and_commit(state, target.clone(), term).await {
        Ok(_) => Ok(target),
        // The joint entry is committed, so both halves are still deciding together. Availability is
        // unchanged and correctness is not at risk; only the second entry is owed.
        Err(ChangeError::Stalled(why)) => {
            warn!(target: "membership", error = %why,
                "Left the cluster in joint consensus; retry the same change to finish it");
            Err(ChangeError::Stalled(why))
        },
        Err(other) => Err(other),
    }
}

/// A leader that inherits a committed joint configuration finishes the change. Left alone the group
/// goes on needing a majority of each half indefinitely, which is availability the change was only
/// ever meant to cost for the length of one round trip -- and the leader that would have ended it
/// is the one that died.
///
/// Gated on the joint entry being *committed*: appending the target above an uncommitted joint entry
/// would let a majority of the incoming half alone commit both, which is the hole joint consensus
/// exists to close.
pub fn resume_change(state: &AppState) {
    let state = state.clone();
    let term = state.current_term();
    tokio::spawn(async move { resume_change_serialized(&state, term).await });
}

async fn resume_change_serialized(state: &AppState, term: u64) {
    let _change = state.membership_changes.gate.lock().await;
    if !state.is_leader() || state.current_term() != term {
        return;
    }
    let committed = state.db.as_ref()
        .and_then(|db| db.existing_collection(CONFIG_LOG))
        .and_then(|col| col.committed_config());
    let Some(joint) = committed.filter(|c| c.is_joint()) else { return };
    if state.quorum_config() != joint || pending_change(state).is_some() {
        return;
    }
    let target = joint.target();
    info!(target: "membership", voters = ?target.voters,
        "Inherited a joint configuration; appending the target it was heading for");
    match change_membership_locked(state, target.voters.clone(), term).await {
        Ok(_) => info!(target: "membership", voters = ?target.voters, "Configuration change completed"),
        Err(e) => warn!(target: "membership", error = %e.why(),
            "Could not leave joint consensus; the next promotion will try again"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{single_node, temp_root, wait_for};

    fn urls(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| format!("http://{}", n)).collect()
    }

    fn pause_append(state: &AppState) -> std::sync::Arc<AppendPause> {
        let pause = std::sync::Arc::new(AppendPause::default());
        *state.membership_changes.before_append.lock().unwrap() = Some(pause.clone());
        pause
    }

    fn configurations(state: &AppState) -> Vec<Configuration> {
        use crate::storage::frame::{LogEntry, HEADER_LEN};
        state.db.as_ref().unwrap().get_collection(CONFIG_LOG).unwrap()
            .read_frames_after(0, u64::MAX).unwrap().into_iter()
            .filter_map(|(_, frame)| match serde_json::from_slice::<LogEntry>(&frame[HEADER_LEN..]).unwrap() {
                LogEntry::Config { config, .. } => Some(config),
                _ => None,
            }).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib011_concurrent_requests_validate_against_the_completed_predecessor() {
        let root = temp_root();
        let (mut leader, mut a, mut b) = crate::test_support::three_node_cluster_with_timeout(&root, 30).await;
        let state = leader.state.clone().unwrap();
        let original = state.quorum_config();
        let first_target = vec![leader.url(), a.url()];
        let second_target = vec![leader.url()];
        let pause = pause_append(&state);
        let endpoint = format!("{}/cluster/configuration", leader.url());
        let request = |target: Vec<String>| {
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                reqwest::Client::new().post(endpoint).json(&serde_json::json!({"voters": target}))
                    .send().await.unwrap().status()
            })
        };
        let first = request(first_target.clone());
        tokio::time::timeout(Duration::from_secs(5), pause.entered.notified()).await.unwrap();
        let protected = state.membership_changes.gate.try_lock().is_err();
        let mut second = request(second_target.clone());
        let early = tokio::time::timeout(Duration::from_millis(150), &mut second).await;
        let waited = early.is_err();
        pause.release.notify_one();
        assert_eq!(first.await.unwrap(), axum::http::StatusCode::OK);
        let second_status = match early { Ok(result) => result.unwrap(), Err(_) => second.await.unwrap() };
        assert_eq!(second_status, axum::http::StatusCode::OK);
        assert!(protected && waited, "another request entered before the first validated append finished");
        assert_eq!(configurations(&state), vec![
            Configuration::joint(original.voters, first_target.clone()),
            Configuration::simple(first_target.clone()),
            Configuration::joint(first_target, second_target.clone()),
            Configuration::simple(second_target.clone()),
        ]);
        assert_eq!(state.quorum_config(), Configuration::simple(second_target));
        assert!(pending_change(&state).is_none());
        drop(state);
        leader.kill();
        a.kill();
        b.kill();
        let db = crate::storage::Database::new(&leader.data_dir).unwrap();
        assert_eq!(db.get_collection(CONFIG_LOG).unwrap().committed_config().unwrap().voters, vec![leader.url()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib011_cancelling_a_request_keeps_its_transition_serialized() {
        let root = temp_root();
        let (mut leader, mut a, mut b) = crate::test_support::three_node_cluster_with_timeout(&root, 30).await;
        let state = leader.state.clone().unwrap();
        let target = vec![leader.url(), a.url()];
        let pause = pause_append(&state);
        let changing = state.clone();
        let next = target.clone();
        let request = tokio::spawn(async move { change_membership(&changing, next).await });
        tokio::time::timeout(Duration::from_secs(5), pause.entered.notified()).await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        let protected = state.membership_changes.gate.try_lock().is_err();
        let retrying = state.clone();
        let next = target.clone();
        let retry = tokio::spawn(async move { change_membership(&retrying, next).await });
        pause.release.notify_one();
        assert_eq!(retry.await.unwrap().unwrap(), Configuration::simple(target));
        assert!(protected, "client cancellation released an append still in flight");
        assert_eq!(configurations(&state).len(), 2, "retry must not append a second transition");
        drop(state);
        leader.kill();
        a.kill();
        b.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib011_resume_and_retries_share_one_transition_and_recheck_pending_entries() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let state = node.state.clone().unwrap();
        let target = vec![node.url()];
        let joint = Configuration::joint(vec![node.url(), "http://gone".into()], target.clone());
        let col = state.db.as_ref().unwrap().get_collection(CONFIG_LOG).unwrap();
        let lsn = col.configure(joint.clone(), state.current_term()).unwrap().3;
        col.enqueue_commit().await.unwrap().unwrap();
        col.apply_committed(lsn).unwrap();
        state.refresh_configuration();
        let pause = pause_append(&state);
        let resuming = state.clone();
        let term = state.current_term();
        let resume = tokio::spawn(async move { resume_change_serialized(&resuming, term).await });
        tokio::time::timeout(Duration::from_secs(5), pause.entered.notified()).await.unwrap();
        let protected = state.membership_changes.gate.try_lock().is_err();
        let retrying = state.clone();
        let next = target.clone();
        let retry = tokio::spawn(async move { change_membership(&retrying, next).await });
        pause.release.notify_one();
        resume.await.unwrap();
        retry.await.unwrap().unwrap();
        resume_change_serialized(&state, term).await;
        assert!(protected);
        assert_eq!(configurations(&state), vec![joint.clone(), Configuration::simple(target.clone())]);

        let joint_lsn = col.configure(joint, term).unwrap().3;
        col.apply_committed(joint_lsn).unwrap();
        col.configure(Configuration::simple(target), term).unwrap();
        state.refresh_configuration();
        let tail = col.last_appended();
        resume_change_serialized(&state, term).await;
        assert_eq!(col.last_appended(), tail, "resume must not append over a pending target");
        assert!(pending_change(&state).is_some());
        assert!(matches!(change_membership(&state, vec![node.url()]).await, Err(ChangeError::Refused(_))));
        assert_eq!(col.last_appended(), tail);
        col.apply_committed(tail.1).unwrap();
        col.configure(Configuration::joint(vec![node.url()], vec![node.url(), "http://gone".into()]), term).unwrap();
        state.refresh_configuration();
        let tail = col.last_appended();
        resume_change_serialized(&state, term).await;
        assert_eq!(col.last_appended(), tail, "an uncommitted joint entry cannot be finalized");
        drop(col);
        drop(state);
        node.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib011_waiting_changes_recheck_leadership_and_failed_appends_release_the_gate() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let state = node.state.clone().unwrap();
        let gate = state.membership_changes.gate.lock().await;
        let changing = state.clone();
        let target = vec![node.url()];
        let queued = tokio::spawn(async move { change_membership(&changing, target).await });
        state.replication.as_ref().unwrap().write().unwrap().is_leader = false;
        drop(gate);
        assert!(matches!(queued.await.unwrap(), Err(ChangeError::Refused(_))));
        assert!(configurations(&state).is_empty());
        state.replication.as_ref().unwrap().write().unwrap().is_leader = true;
        let col = state.db.as_ref().unwrap().get_collection(CONFIG_LOG).unwrap();
        let target = vec![node.url()];
        let joint = Configuration::joint(vec![node.url(), "http://gone".into()], target.clone());
        let lsn = col.configure(joint.clone(), state.current_term()).unwrap().3;
        col.enqueue_commit().await.unwrap().unwrap();
        col.apply_committed(lsn).unwrap();
        state.refresh_configuration();
        let tombstone = col.release_handles().unwrap();
        assert!(matches!(change_membership(&state, target.clone()).await, Err(ChangeError::Refused(_))));
        assert!(state.membership_changes.gate.try_lock().is_ok());
        state.db.as_ref().unwrap().collections.write().unwrap().remove(CONFIG_LOG);
        drop(col);
        let _ = std::fs::remove_file(tombstone);
        assert_eq!(change_membership(&state, target.clone()).await.unwrap(), Configuration::simple(target));
        drop(state);
        node.kill();
    }

    /// The membership API is the way an inflated voter set gets into `CONFIG_LOG`; `validate`
    /// refused an empty set, a concurrent change and a non-member, but not a node named twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib043_a_voter_named_twice_is_refused_and_a_stored_one_reads_back_deduped() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let state = node.state.clone().unwrap();
        let own = node.url();

        for duplicated in [vec![own.clone(), own.clone()],
                           vec![own.clone(), own.to_uppercase(), format!("{}/", own)]] {
            let refused = change_membership(&state, duplicated).await;
            assert!(matches!(refused, Err(ChangeError::Refused(_))), "{:?}", refused);
        }
        assert!(configurations(&state).is_empty(), "a refused change must append nothing");

        // What a pre-fix leader left behind: an entry naming one node twice in each half.
        let col = state.db.as_ref().unwrap().get_collection(CONFIG_LOG).unwrap();
        let inflated = Configuration {
            voters: vec![own.clone(), own.to_uppercase()],
            outgoing: Some(vec![own.clone(), own.clone(), "http://gone".into()]),
        };
        let lsn = col.configure(inflated, state.current_term()).unwrap().3;
        col.apply_committed(lsn).unwrap();
        state.refresh_configuration();

        let in_force = state.quorum_config();
        assert_eq!(in_force.voters, vec![own.clone()]);
        assert_eq!(in_force.outgoing, Some(vec![own.clone(), "http://gone".to_string()]));
        assert!(!in_force.has_quorum(&[own.clone()]),
            "the outgoing half is two nodes, and this one alone is not a majority of it");

        drop(col);
        drop(state);
        node.kill();
    }

    #[test]
    fn a_retry_of_the_change_in_flight_is_not_a_second_change() {
        let joint = Configuration::joint(urls(&["a", "b"]), urls(&["a", "b", "c"]));

        assert!(resumes(&joint, &urls(&["a", "b", "c"])),
            "the same target has to be allowed through, or the retry that finishes it is refused");
        assert!(resumes(&joint, &urls(&["c", "a", "b"])), "order is not identity");
        assert!(!resumes(&joint, &urls(&["a", "b", "d"])),
            "a different target while joint is a second concurrent change");
        assert!(!resumes(&Configuration::simple(urls(&["a", "b"])), &urls(&["a", "b"])),
            "nothing to resume when no change is in flight");
    }

    /// A leader that dies between the two entries leaves the group needing both halves forever.
    /// Whoever leads next owns finishing it, so promotion has to look.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_leader_finishes_a_joint_configuration_it_inherits() {
        let root = temp_root();
        let node = single_node(&root).await;
        let state = node.state.as_ref().unwrap();
        let own = node.url();

        // What the dead leader left behind: a committed joint entry and no target above it.
        let joint = Configuration::joint(vec![own.clone(), "http://gone".to_string()], vec![own.clone()]);
        let col = state.db.as_ref().unwrap().get_collection(CONFIG_LOG).unwrap();
        let (_f, _w, _o, lsn) = col.configure(joint.clone(), state.current_term()).unwrap();
        col.apply_committed(lsn).unwrap();
        state.refresh_configuration();
        assert!(state.quorum_config().is_joint(), "the setup itself has to leave it joint");

        resume_change(state);

        assert!(wait_for(Duration::from_secs(15), || !state.quorum_config().is_joint()).await,
            "the group is stuck needing a majority of a half whose other node is gone");
        assert_eq!(state.quorum_config().voters, vec![own],
            "and it must land on the target the joint entry named, not back on the old set");
        assert_eq!(col.committed_config().map(|c| c.is_joint()), Some(false),
            "the target has to commit, not just be appended");
    }
}
