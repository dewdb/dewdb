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

impl ChangeError {
    pub fn why(&self) -> &str {
        match self {
            ChangeError::Refused(why) | ChangeError::Stalled(why) | ChangeError::Redirect(why) => why,
        }
    }
}

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
async fn append_and_commit(state: &AppState, config: Configuration) -> Result<u64, ChangeError> {
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

    let term = state.current_term();
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
    state.advance_own_commit(CONFIG_LOG, col.durable_lsn());

    let deadline = Instant::now() + CONFIG_COMMIT_TIMEOUT;
    while state.committed_lsn(CONFIG_LOG) < lsn {
        if Instant::now() >= deadline || !state.is_leader() || state.current_term() != term {
            return Err(ChangeError::Stalled(format!(
                "configuration entry {} is durable but did not reach a quorum", lsn)));
        }
        tokio::time::sleep(Duration::from_millis(COMMIT_POLL_MS)).await;
    }
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
        append_and_commit(state, joint).await?;
    }

    info!(target: "membership", voters = ?target.voters, "Joint configuration committed; leaving it");
    match append_and_commit(state, target.clone()).await {
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
    let committed = state.db.as_ref()
        .and_then(|db| db.existing_collection(CONFIG_LOG))
        .and_then(|col| col.committed_config());
    let Some(joint) = committed.filter(|c| c.is_joint()) else { return };

    let state = state.clone();
    let term = state.current_term();
    tokio::spawn(async move {
        if !state.is_leader() || state.current_term() != term {
            return;
        }
        let target = joint.target();
        info!(target: "membership", voters = ?target.voters,
            "Inherited a joint configuration; appending the target it was heading for");
        match append_and_commit(&state, target.clone()).await {
            Ok(_) => info!(target: "membership", voters = ?target.voters, "Configuration change completed"),
            // A redirect cannot reach here: the target of an inherited joint entry is the half
            // this node is finishing from, and it is a member of it or it would not be leading.
            Err(e) => warn!(target: "membership", error = %e.why(),
                "Could not leave joint consensus; the next promotion will try again"),
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{single_node, temp_root, wait_for};

    fn urls(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| format!("http://{}", n)).collect()
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
        col.apply_committed(lsn);
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
