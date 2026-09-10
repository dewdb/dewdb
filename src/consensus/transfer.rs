//! Leadership transfer: a leader hands office to a voter that can take it, Raft 3.10. Hold writes,
//! catch the target up, then tell it to stand now -- past the pre-vote and past every voter's lease.

use super::reconfigure::pending_change;
use crate::replication::stream::catch_up_replica;
use crate::state::AppState;
use crate::util::same_endpoint;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// The whole handover, catch-up included. Bounded because writes are held for its length.
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(5);
/// How long in-flight writes get to finish before the barrier gives up. Separate from the transfer
/// budget: this one is spent before anything has been decided, so a refusal here costs nothing.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const CATCH_UP_POLL_MS: u64 = 20;
const HANDOVER_POLL_MS: u64 = 20;
const TIMEOUT_NOW_TIMEOUT_MS: u64 = 1000;

#[derive(Serialize, Deserialize)]
pub struct TimeoutNowRequest {
    pub term: u64,
    /// The leader standing aside. A voter checks it against the leader it is following, so the
    /// request carries no authority a node could not already spend.
    pub leader: String,
}

/// Clears the handover mark on every exit, including the ones that leave this node still leading.
struct HandoverGuard<'a> {
    state: &'a AppState,
}

impl Drop for HandoverGuard<'_> {
    fn drop(&mut self) {
        self.state.set_handing_over(false);
    }
}

pub enum TransferError {
    /// Nothing was attempted and this node still leads.
    Refused(String),
    /// The handover was attempted and did not complete. This node still leads, unless it lost
    /// office for an unrelated reason while trying.
    Failed(String),
}

impl TransferError {
    pub fn why(&self) -> &str {
        match self {
            TransferError::Refused(why) | TransferError::Failed(why) => why,
        }
    }
}

/// Who could take over, out of `among`: the voters of the configuration in force, this node
/// excluded. While joint that means both halves -- a target in one half cannot win.
pub fn eligible_targets(state: &AppState, among: &[String]) -> Vec<String> {
    let quorum = state.quorum_config();
    let own = state.own_url();
    quorum.members().into_iter()
        .filter(|m| !same_endpoint(m, &own))
        .filter(|m| quorum.voters.iter().any(|v| same_endpoint(v, m)))
        .filter(|m| quorum.outgoing.as_ref().map_or(true, |old| old.iter().any(|v| same_endpoint(v, m))))
        .filter(|m| among.iter().any(|a| same_endpoint(a, m)))
        .collect()
}

/// The readiest of them, measured by the collection each is furthest *behind* on: a target is only
/// as ready as its worst log, since one voter holding an entry it lacks is enough to refuse it.
pub fn best_target(state: &AppState, among: &[String]) -> Option<String> {
    let Some(db) = state.db.as_ref() else { return among.first().cloned() };
    let tails: Vec<(String, u64)> = db.list_collections().unwrap_or_default().into_iter()
        .filter_map(|name| db.get_collection(&name).ok().map(|col| (name, col.last_appended_lsn())))
        .filter(|(_, tail)| *tail > 0)
        .collect();

    among.iter()
        .max_by_key(|target| {
            let worst = tails.iter()
                .map(|(name, tail)| tail.saturating_sub(state.matched_lsn(target, name)))
                .max()
                .unwrap_or(0);
            // Ranked ascending by `max_by_key`, and lag is the wrong way round for that.
            u64::MAX - worst
        })
        .cloned()
}

/// Hands leadership to `to`, or to the readiest voter when `None`. Returns the node that took over.
/// On any error this node still leads and writes resume: nothing here appends, the target is asked.
pub async fn transfer_leadership(state: &AppState, to: Option<String>) -> Result<String, TransferError> {
    if !state.is_shard() {
        return Err(TransferError::Refused("a router has no leadership to transfer".to_string()));
    }
    if !state.is_leader() {
        return Err(TransferError::Refused(
            "not the leader; there is nothing here to hand over".to_string()));
    }
    // The successor would inherit an entry that is in force and uncommitted, and finish it under a
    // configuration that is not the one this transfer picked its target from.
    if pending_change(state).is_some() {
        return Err(TransferError::Refused(
            "a configuration entry has not committed yet; retry once it has".to_string()));
    }

    let eligible = eligible_targets(state, &state.quorum_config().members());
    let target = match to {
        Some(url) => match eligible.iter().find(|e| same_endpoint(e, &url)) {
            Some(found) => found.clone(),
            None => return Err(TransferError::Refused(format!(
                "{} is not a voter that could take over; eligible: {:?}", url, eligible))),
        },
        None => match best_target(state, &eligible) {
            Some(found) => found,
            None => return Err(TransferError::Refused(
                "no other voter to hand to; this node is the whole quorum".to_string())),
        },
    };

    let term = state.current_term();
    let own = state.own_url();
    info!(target: "transfer", to = %target, term, "Handing over leadership");

    // Raft §3.10's first step, held rather than drained: a write admitted behind the catch-up puts
    // the tail back out of the target's reach, and the loop below would chase it to the deadline.
    let _gate = match tokio::time::timeout(DRAIN_TIMEOUT, state.write_gate.write()).await {
        Ok(gate) => gate,
        Err(_) => return Err(TransferError::Failed(format!(
            "writes did not drain within {:?}; nothing was handed over", DRAIN_TIMEOUT))),
    };

    // Re-checked under the barrier, which is what makes them authoritative: nothing above it
    // excludes a second transfer or a configuration change that started while this one queued.
    if !state.is_leader() || state.current_term() != term {
        return Err(TransferError::Refused(
            "leadership moved while this handover waited for writes to drain".to_string()));
    }
    if pending_change(state).is_some() {
        return Err(TransferError::Refused(
            "a configuration entry landed while this handover waited; retry once it commits".to_string()));
    }

    let deadline = Instant::now() + TRANSFER_TIMEOUT;
    while !catch_up_replica(state, &target).await {
        if !state.is_leader() || state.current_term() != term {
            return Err(TransferError::Failed(
                "lost leadership while catching the target up".to_string()));
        }
        if Instant::now() >= deadline {
            return Err(TransferError::Failed(format!(
                "{} did not reach this node's tail within {:?}", target, TRANSFER_TIMEOUT)));
        }
        tokio::time::sleep(Duration::from_millis(CATCH_UP_POLL_MS)).await;
    }

    // Before the ask, not after: from here a voter may grant past the promises this node's leases
    // rest on. The guard clears it on every way out, failures included.
    state.set_handing_over(true);
    let _handover = HandoverGuard { state };

    let body = TimeoutNowRequest { term, leader: own };
    let sent = state.client.post(format!("{}/internal/timeout-now", target))
        .timeout(Duration::from_millis(TIMEOUT_NOW_TIMEOUT_MS))
        .json(&body).send().await;
    match sent {
        Ok(r) if r.status().is_success() => {},
        Ok(r) => return Err(TransferError::Failed(format!(
            "{} would not stand: {}", target, r.status()))),
        Err(e) => return Err(TransferError::Failed(format!("{} unreachable: {}", target, e))),
    }

    // The target raises the term, and the vote it asks for is what demotes us. Relinquishing first
    // would leave the group leaderless for a full election timeout if that election then failed.
    while state.is_leader() && state.current_term() == term {
        if Instant::now() >= deadline {
            warn!(target: "transfer", to = %target, term,
                "Target was told to stand and has not taken over; still leading");
            return Err(TransferError::Failed(format!(
                "{} did not take office within {:?}; this node still leads", target, TRANSFER_TIMEOUT)));
        }
        tokio::time::sleep(Duration::from_millis(HANDOVER_POLL_MS)).await;
    }

    // The vote that deposed us left no primary behind. The poll would find one, but only after the
    // target has won and only by resyncing from it; we know exactly who took over.
    super::failover::follow_handover(state, &target);

    info!(target: "transfer", to = %target, from_term = term, "Leadership handed over");
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::frame::Configuration;
    use crate::test_support::{live_put, single_node, temp_root};

    fn urls(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| format!("http://{}", n)).collect()
    }

    /// The three ways a node fails to be somewhere office can go: it is us, it is not a voter, or
    /// it is a voter of only one half of a change in flight.
    #[tokio::test]
    async fn only_a_voter_of_every_half_can_be_handed_to() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let state = node.state.clone().unwrap();
        let own = state.own_url();

        let all = |c: &Configuration| c.members();

        let simple = Configuration::simple([vec![own.clone()], urls(&["b", "c"])].concat());
        state.install_configuration(simple.clone());
        assert_eq!(eligible_targets(&state, &all(&simple)), urls(&["b", "c"]),
            "a leader cannot hand office to itself");
        assert_eq!(eligible_targets(&state, &urls(&["b"])), urls(&["b"]),
            "and the caller's own restriction narrows it further");
        assert!(eligible_targets(&state, &urls(&["d"])).is_empty(),
            "a node outside the configuration is not a voter, whatever the caller asks for");

        let joint = Configuration::joint(
            [vec![own.clone()], urls(&["b"])].concat(),
            [vec![own.clone()], urls(&["c"])].concat());
        state.install_configuration(joint.clone());
        assert_eq!(eligible_targets(&state, &all(&joint)), Vec::<String>::new(),
            "b and c each decide in one half only, so neither could win the election it is told \
             to run");

        node.kill();
    }

    /// A target is only as ready as its worst log: one voter holding an entry it lacks refuses it.
    #[tokio::test]
    async fn the_readiest_target_is_the_one_least_behind_on_its_worst_collection() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let state = node.state.clone().unwrap();
        let db = state.db.as_ref().unwrap().clone();

        for name in ["a", "b"] {
            let col = db.get_collection(name).unwrap();
            for v in 1..=4 {
                live_put(&col, "k", v);
            }
            col.enqueue_commit().await.unwrap().unwrap();
        }
        let (tail_a, tail_b) = (
            db.get_collection("a").unwrap().last_appended_lsn(),
            db.get_collection("b").unwrap().last_appended_lsn(),
        );

        let (ahead, uneven) = (urls(&["ahead"])[0].clone(), urls(&["uneven"])[0].clone());
        {
            let repl = state.replication.as_ref().unwrap();
            let mut g = repl.write().unwrap();
            g.progress.observe_ack(&ahead, "a", tail_a - 1);
            g.progress.observe_ack(&ahead, "b", tail_b - 1);
            // Level with the tail on one log and far behind on the other, which is the case a
            // per-collection maximum catches and a total or an average does not.
            g.progress.observe_ack(&uneven, "a", tail_a);
            g.progress.observe_ack(&uneven, "b", 0);
        }

        let among = vec![ahead.clone(), uneven.clone()];
        assert_eq!(best_target(&state, &among), Some(ahead),
            "the one behind by a frame everywhere is readier than the one caught up on half of it");
        assert_eq!(best_target(&state, &[]), None, "nowhere to hand to is not a choice");

        node.kill();
    }
}
