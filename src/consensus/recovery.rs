//! Monotonic per-collection history recovery before retrying a failed pre-vote.

use super::election::{try_local_log_tails, LogTail};
use crate::replication::snapshot::recover_from_peer;
use crate::state::AppState;
use crate::storage::frame::Configuration;
use crate::util::same_endpoint;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

#[derive(Serialize, Deserialize)]
pub struct ElectionHistories {
    pub term: u64,
    pub logs: HashMap<String, LogTail>,
}

pub async fn recover_histories(state: &AppState, quorum: &Configuration) -> Result<(), String> {
    if state.is_leader() || !state.in_quorum() {
        return Ok(());
    }
    let term = state.current_term();
    let own = state.own_url();
    let peers = quorum.members().into_iter().filter(|p| !same_endpoint(p, &own));
    let responses = futures::future::join_all(peers.map(|peer| async move {
        let response = state.client.get(format!("{peer}/internal/election-histories"))
            .timeout(Duration::from_millis(1500)).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        let histories = response.json::<ElectionHistories>().await.ok()?;
        Some((peer, histories))
    })).await;

    let mut sources: HashMap<String, (LogTail, String)> = HashMap::new();
    let local = try_local_log_tails(state).map_err(|e| e.to_string())?;
    for (peer, histories) in responses.into_iter().flatten() {
        if histories.term > term {
            super::failover::demote(state, histories.term).await;
            return Ok(());
        }
        for (name, tail) in histories.logs {
            if tail.last_term > term || tail <= local.get(&name).copied().unwrap_or_default() {
                continue;
            }
            if sources.get(&name).is_none_or(|(best, _)| tail > *best) {
                sources.insert(name, (tail, peer.clone()));
            }
        }
    }

    // Configuration recovery changes the electorate; the next campaign must gather peers again.
    let mut sources: Vec<_> = sources.into_iter().collect();
    sources.sort_by(|a, b| (a.0 != super::config::CONFIG_LOG)
        .cmp(&(b.0 != super::config::CONFIG_LOG)).then_with(|| a.0.cmp(&b.0)));
    for (name, (tail, peer)) in sources {
        if state.is_leader() || state.current_term() != term || state.quorum_config() != *quorum {
            return Ok(());
        }
        recover_from_peer(state, &peer, &name, tail, term).await?;
        info!(target: "election", collection = %name, source = %peer,
            "Recovered a newer collection history before retrying pre-vote");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::election::{decide_pre_vote, log_summary, run_election, VoteRequest};
    use crate::consensus::ReplicationMeta;
    use crate::storage::{Database, Retention};
    use crate::test_support::{make_frame, next_test_port, temp_root, wait_for, TestNode};

    fn prepare(node: &TestNode, quorum: &Configuration) -> AppState {
        let state = node.state.clone().unwrap();
        {
            let mut r = state.replication.as_ref().unwrap().write().unwrap();
            r.term = 1;
            r.configuration = Some(quorum.clone());
            r.booted_at = std::time::Instant::now() - Duration::from_secs(60);
        }
        ReplicationMeta { term: 1, is_leader: false, voted_for: None }
            .save(&state.config.data_dir).unwrap();
        state
    }

    fn request(state: &AppState) -> VoteRequest {
        let logs = try_local_log_tails(state).unwrap();
        let tail = log_summary(state, &logs);
        VoteRequest { term: 2, candidate_id: state.config.node_id.clone(),
            last_lsn: tail.last_lsn, last_term: tail.last_term, logs,
            candidate_url: Some(state.own_url()), transfer_from: None }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ib010_complementary_histories_recover_elect_and_survive_restart() {
        for compact in [false, true] {
            let root = temp_root();
            let mut a = TestNode::new("a", next_test_port(), &root, "replica");
            let mut b = TestNode::new("b", next_test_port(), &root, "replica");
            let dead = format!("http://127.0.0.1:{}", next_test_port());
            let quorum = Configuration::simple(vec![a.url(), b.url(), dead.clone()]);
            a.peers = vec![b.url(), dead.clone()];
            b.peers = vec![a.url(), dead];
            a.heartbeat_timeout_secs = 60;
            b.heartbeat_timeout_secs = 60;
            a.start();
            b.start();
            let sa = prepare(&a, &quorum);
            let sb = prepare(&b, &quorum);
            let c = Database::new(root.join("c")).unwrap();
            for (name, lsn, survivor) in [("x", 1, &sa), ("y", 2, &sb)] {
                let frame = make_frame(1, lsn, 0, 0, "committed", lsn as i64);
                for db in [&c, survivor.db.as_ref().unwrap().as_ref()] {
                    let col = db.get_collection(name).unwrap();
                    col.append_raw_frame(&frame).unwrap();
                    col.enqueue_commit().await.unwrap().unwrap();
                    col.apply_committed(lsn).unwrap();
                    if compact { col.compact(Retention::none()).unwrap(); }
                }
            }
            for (voter, candidate) in [(&sa, &sb), (&sb, &sa)] {
                let logs = try_local_log_tails(voter).unwrap();
                assert!(!decide_pre_vote(1, &logs, log_summary(voter, &logs),
                    Some(&quorum), &request(candidate), false));
            }

            run_election(&sa, 0, None).await;
            assert!(!sa.is_leader());
            assert_eq!(sa.current_term(), 1, "repair must not spend a term or vote");
            assert_eq!(sa.replication.as_ref().unwrap().read().unwrap().voted_for, None);
            assert_eq!(sa.db.as_ref().unwrap().get_collection("y").unwrap()
                .get("committed").unwrap().unwrap()["v"], 2);

            run_election(&sa, 0, None).await;
            assert!(sa.is_leader(), "the repaired candidate must win with the other survivor");
            let client = reqwest::Client::new();
            for name in ["x", "y"] {
                let response = client.put(format!("{}/collections/{name}/docs/after?w=majority&wtimeout=5000", a.url()))
                    .json(&serde_json::json!({"value": {"v": 3}})).send().await.unwrap();
                let status = response.status();
                let body = response.text().await.unwrap();
                assert!(status.is_success(), "{status}: {body}");
                assert_ne!(status, reqwest::StatusCode::ACCEPTED, "{body}");
            }
            assert!(wait_for(Duration::from_secs(10), || {
                sb.db.as_ref().unwrap().get_collection("y").unwrap()
                    .get("after").unwrap().is_some()
            }).await);
            drop(sa);
            drop(sb);
            a.kill();
            b.kill();
            for node in [&a, &b] {
                let db = Database::new(&node.data_dir).unwrap();
                for (name, value) in [("x", 1), ("y", 2)] {
                    assert_eq!(db.get_collection(name).unwrap().get("committed").unwrap()
                        .unwrap()["v"], value);
                }
                assert_eq!(db.get_collection("y").unwrap().get("after").unwrap().unwrap()["v"], 3);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ib010_simultaneous_recovery_preserves_staged_entries_and_membership() {
        let root = temp_root();
        let mut a = TestNode::new("a", next_test_port(), &root, "replica");
        let mut b = TestNode::new("b", next_test_port(), &root, "replica");
        let dead = format!("http://127.0.0.1:{}", next_test_port());
        let quorum = Configuration::joint(vec![a.url(), b.url(), dead.clone()],
            vec![a.url(), b.url(), format!("http://127.0.0.1:{}", next_test_port())]);
        a.heartbeat_timeout_secs = 60;
        b.heartbeat_timeout_secs = 60;
        a.start();
        b.start();
        let sa = prepare(&a, &quorum);
        let sb = prepare(&b, &quorum);
        for (state, name, lsn) in [(&sa, "x", 1), (&sb, "y", 2)] {
            let col = state.db.as_ref().unwrap().get_collection(name).unwrap();
            col.append_raw_frame(&make_frame(1, lsn, 0, 0, "pending", lsn as i64)).unwrap();
            col.enqueue_commit().await.unwrap().unwrap();
        }
        let (ra, rb) = tokio::join!(recover_histories(&sa, &quorum), recover_histories(&sb, &quorum));
        ra.unwrap();
        rb.unwrap();
        for state in [&sa, &sb] {
            for (name, lsn) in [("x", 1), ("y", 2)] {
                let col = state.db.as_ref().unwrap().get_collection(name).unwrap();
                assert_eq!(col.last_appended(), (1, lsn));
                assert_eq!(col.applied_lsn(), 0);
                assert!(col.get("pending").unwrap().is_none());
            }
            assert_eq!(state.current_term(), 1);
        }
        let logs = try_local_log_tails(&sb).unwrap();
        assert!(decide_pre_vote(1, &logs, log_summary(&sb, &logs), Some(&quorum), &request(&sa), false));
        assert!(!quorum.has_quorum(&[sa.own_url(), dead]));
        drop(sa);
        drop(sb);
        a.kill();
        b.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ib010_recovery_uses_only_voters_and_rechecks_a_recovered_configuration() {
        let root = temp_root();
        let mut a = TestNode::new("a", next_test_port(), &root, "replica");
        let mut b = TestNode::new("b", next_test_port(), &root, "replica");
        let mut foreign = TestNode::new("foreign", next_test_port(), &root, "replica");
        let dead = format!("http://127.0.0.1:{}", next_test_port());
        let quorum = Configuration::simple(vec![a.url(), b.url(), dead.clone()]);
        for node in [&mut a, &mut b, &mut foreign] {
            node.heartbeat_timeout_secs = 60;
            node.start();
        }
        let sa = prepare(&a, &quorum);
        let sb = prepare(&b, &quorum);
        let sf = prepare(&foreign, &Configuration::simple(vec![foreign.url()]));
        let alien = sf.db.as_ref().unwrap().get_collection("foreign").unwrap();
        alien.append_raw_frame(&make_frame(1, 100, 0, 0, "alien", 100)).unwrap();
        alien.enqueue_commit().await.unwrap().unwrap();
        sa.cluster.write().unwrap().members.extend(sf.cluster.read().unwrap().members.clone());
        recover_histories(&sa, &quorum).await.unwrap();
        assert!(sa.db.as_ref().unwrap().lookup_collection("foreign").unwrap().is_none());

        let incoming = Configuration::simple(vec![b.url(), dead,
            format!("http://127.0.0.1:{}", next_test_port())]);
        let config_log = sb.db.as_ref().unwrap().get_collection(super::super::config::CONFIG_LOG).unwrap();
        config_log.configure(incoming.clone(), 1).unwrap();
        config_log.enqueue_commit().await.unwrap().unwrap();
        sb.refresh_configuration();
        let later = sb.db.as_ref().unwrap().get_collection("later").unwrap();
        later.put("k".into(), serde_json::json!({"v": 1}), 1).unwrap();
        later.enqueue_commit().await.unwrap().unwrap();
        recover_histories(&sa, &quorum).await.unwrap();
        assert_eq!(sa.quorum_config(), incoming);
        assert!(!sa.in_quorum());
        assert!(sa.db.as_ref().unwrap().lookup_collection("later").unwrap().is_none());
        run_election(&sa, 0, None).await;
        assert!(!sa.is_leader());
        assert_eq!(sa.current_term(), 1);
        drop(sa);
        drop(sb);
        drop(sf);
        drop(alien);
        drop(config_log);
        drop(later);
        a.kill();
        b.kill();
        foreign.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ib010_failed_transfer_and_a_term_change_leave_local_history_intact() {
        let root = temp_root();
        let mut a = TestNode::new("a", next_test_port(), &root, "replica");
        let mut b = TestNode::new("b", next_test_port(), &root, "replica");
        let quorum = Configuration::simple(vec![a.url(), b.url()]);
        a.heartbeat_timeout_secs = 60;
        b.heartbeat_timeout_secs = 60;
        a.start();
        b.start();
        let sa = prepare(&a, &quorum);
        let sb = prepare(&b, &quorum);
        for state in [&sa, &sb] {
            let col = state.db.as_ref().unwrap().get_collection("x").unwrap();
            col.append_raw_frame(&make_frame(1, 1, 0, 0, "committed", 1)).unwrap();
            col.enqueue_commit().await.unwrap().unwrap();
            col.apply_committed(1).unwrap();
        }
        let donor = sb.db.as_ref().unwrap().get_collection("x").unwrap();
        donor.append_raw_frame(&make_frame(1, 2, 1, 1, "pending", 2)).unwrap();
        donor.enqueue_commit().await.unwrap().unwrap();
        let offered = LogTail { last_term: 1, last_lsn: 2 };
        assert!(recover_from_peer(&sa, &b.url(), "x",
            LogTail { last_term: 1, last_lsn: 3 }, 1).await.is_err());
        let history = sa.election_history.read().await;
        let candidate = sa.clone();
        let peer = b.url();
        let transfer = tokio::spawn(async move {
            recover_from_peer(&candidate, &peer, "x", offered, 1).await
        });
        assert!(wait_for(Duration::from_secs(5), || {
            sa.db.as_ref().unwrap().root_path.join("x.tmp/applied.meta").is_file()
        }).await);
        sa.replication.as_ref().unwrap().write().unwrap().term = 2;
        drop(history);
        assert!(transfer.await.unwrap().is_err());
        let col = sa.db.as_ref().unwrap().get_collection("x").unwrap();
        assert_eq!(col.last_appended(), (1, 1));
        assert_eq!(col.applied_lsn(), 1);
        assert_eq!(col.get("committed").unwrap().unwrap()["v"], 1);
        assert!(col.get("pending").unwrap().is_none());
        assert!(!sa.db.as_ref().unwrap().root_path.join("x.tmp").exists());
        let history = sa.election_history.read().await;
        let candidate = sa.clone();
        let peer = b.url();
        let transfer = tokio::spawn(async move {
            recover_from_peer(&candidate, &peer, "x", offered, 2).await
        });
        assert!(wait_for(Duration::from_secs(5), || {
            sa.db.as_ref().unwrap().root_path.join("x.tmp/applied.meta").is_file()
        }).await);
        sa.install_configuration(Configuration::simple(vec![a.url()]));
        drop(history);
        assert!(transfer.await.unwrap().is_err());
        assert_eq!(col.last_appended(), (1, 1));
        assert!(recover_from_peer(&sa, &b.url(), "x", offered, 2).await.is_err());
        sa.install_configuration(quorum);
        recover_from_peer(&sa, &b.url(), "x", offered, 2).await.unwrap();
        let repaired = sa.db.as_ref().unwrap().get_collection("x").unwrap();
        assert_eq!(repaired.last_appended(), (1, 2));
        assert_eq!(repaired.applied_lsn(), 1);
        assert!(repaired.get("pending").unwrap().is_none());
        drop(repaired);
        drop(col);
        drop(donor);
        drop(sa);
        drop(sb);
        a.kill();
        b.kill();
    }
}
