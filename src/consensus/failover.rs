//! Follower watchdog: detect leader silence, find the new leader, step down.

use super::election::run_election;
use super::progress::{ProgressMeta, PROGRESS_FLUSH_INTERVAL_SECS};
use super::state::{apply_demotion, ReplicationMeta};
use crate::replication::snapshot::replica_sync_from_primary;
use crate::state::AppState;
use crate::util::same_endpoint;
use std::collections::HashMap;
use std::fs;
use std::time::Duration;
use tracing::{info, warn};

const HEARTBEAT_POLL_INTERVAL_MS: u64 = 500;
const PEER_PROBE_TIMEOUT_MS: u64 = 400;

async fn discover_leader(state: &AppState) -> Option<String> {
    let mut peers: Vec<String> = state.config.replicas.clone();
    peers.extend(state.config.peers.iter().cloned());
    if let Some(r) = state.replication.as_ref() {
        let g = r.read().unwrap();
        if let Some(p) = g.primary_addr.clone() {
            peers.push(p);
        }
        peers.extend(g.replicas.iter().cloned());
    }
    peers.retain(|p| !same_endpoint(p, &state.config.listen_addr));
    peers.sort();
    peers.dedup();

    let probes = futures::future::join_all(peers.into_iter().map(|peer| {
        let client = state.client.clone();
        async move {
            let url = format!("{}/internal/heartbeat", peer);
            let resp = client.get(&url)
                .timeout(Duration::from_millis(PEER_PROBE_TIMEOUT_MS))
                .send().await.ok()?;
            let hb = resp.json::<serde_json::Value>().await.ok()?;
            let role = hb.get("role").and_then(|v| v.as_str()).unwrap_or("");
            let term = hb.get("term").and_then(|v| v.as_u64()).unwrap_or(0);
            if role == "primary" { Some((term, peer)) } else { None }
        }
    })).await;

    probes.into_iter().flatten().max_by_key(|(term, _)| *term).map(|(_, url)| url)
}

async fn maybe_follow_new_leader(state: &AppState, current_primary: &str) {
    if state.is_leader() {
        return;
    }
    let leader = match discover_leader(state).await {
        Some(l) => l,
        None => return,
    };
    if leader == current_primary {
        return;
    }

    let changed = {
        let mut repl = state.replication.as_ref().unwrap().write().unwrap();
        if repl.primary_addr.as_deref() == Some(leader.as_str()) {
            false
        } else {
            repl.primary_addr = Some(leader.clone());
            repl.last_heartbeat = Some(std::time::Instant::now());
            true
        }
    };

    if changed {
        info!(target: "failover", "Following new leader {} (was {})", leader, current_primary);
        let state2 = state.clone();
        let leader2 = leader.clone();
        tokio::spawn(async move {
            resync_all_from(&state2, &leader2).await;
        });
    }
}

async fn resync_all_from(state: &AppState, leader: &str) {
    let db = match state.db.as_ref() {
        Some(d) => d.clone(),
        None => return,
    };

    let mut names: Vec<String> = { db.collections.read().unwrap().keys().cloned().collect() };
    if let Ok(entries) = fs::read_dir(&db.root_path) {
        for e in entries.flatten() {
            if e.path().is_dir() {
                if let Some(n) = e.file_name().to_str() {
                    if !n.contains('.') {
                        names.push(n.to_string());
                    }
                }
            }
        }
    }
    names.sort();
    names.dedup();

    for name in names {
        if let Err(e) = replica_sync_from_primary(&state.client, leader, &db, &name).await {
            warn!(target: "demote", "resync of '{}' from {} failed: {}", name, leader, e);
        }
    }

    if let Err(e) = db.recompute_durable_lsn() {
        warn!(target: "demote", "could not recompute durable LSN after resync: {}", e);
    }
}

pub async fn demote(state: &AppState, new_term: u64) {
    let restart = {
        let repl = match state.replication.as_ref() {
            Some(r) => r,
            None => return,
        };
        let mut g = repl.write().unwrap();
        match apply_demotion(&mut g, new_term) {
            Some(r) => r,
            None => return,
        }
    };

    if let Err(e) = (ReplicationMeta { term: new_term, is_leader: false, voted_for: None }).save(&state.config.data_dir) {
        warn!(target: "demote", error = %e, "Stepped down in memory but could not persist term {}", new_term);
    }
    info!(target: "demote", "Discovered higher term {}, stepping down to replica", new_term);

    if restart {
        heartbeat_poll_task(state.clone());
    }

    let state2 = state.clone();
    tokio::spawn(async move {
        if let Some(leader) = discover_leader(&state2).await {
            {
                let mut g = state2.replication.as_ref().unwrap().write().unwrap();
                g.primary_addr = Some(leader.clone());
            }
            info!(target: "demote", "Following new leader {}; resyncing", leader);
            resync_all_from(&state2, &leader).await;
        } else {
            warn!(target: "demote", "New leader not found yet; heartbeat poll will keep retrying");
        }
    });
}

pub async fn adopt_existing_leader(state: &AppState) -> bool {
    let leader = match discover_leader(state).await {
        Some(l) => l,
        None => return false,
    };

    let changed = {
        let mut repl = match state.replication.as_ref() {
            Some(r) => r.write().unwrap(),
            None => return false,
        };
        let changed = repl.primary_addr.as_deref() != Some(leader.as_str());
        repl.primary_addr = Some(leader.clone());
        repl.last_heartbeat = Some(std::time::Instant::now());
        changed
    };

    if changed {
        info!(target: "failover", "Found active leader {}; following it instead of standing for election", leader);
        let state2 = state.clone();
        let leader2 = leader.clone();
        tokio::spawn(async move {
            resync_all_from(&state2, &leader2).await;
        });
    }
    true
}

pub fn progress_flush_task(state: AppState) {
    tokio::spawn(async move {
        let mut last: HashMap<String, HashMap<String, u64>> = HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_secs(PROGRESS_FLUSH_INTERVAL_SECS)).await;

            let snapshot = match state.replication.as_ref() {
                Some(r) => r.read().unwrap().progress.cursor_snapshot(),
                None => return,
            };
            if snapshot == last {
                continue;
            }
            if let Err(e) = (ProgressMeta { sent_through: snapshot.clone() }).save(&state.config.data_dir) {
                warn!(target: "replication", error = %e, "Could not persist replication cursors");
                continue;
            }
            last = snapshot;
        }
    });
}

pub fn heartbeat_poll_task(state: AppState) {
    tokio::spawn(async move {
        let timeout = Duration::from_secs(state.config.heartbeat_timeout_secs);
        let election_delay = state.config.election_delay_ms;
        let task_started = std::time::Instant::now();

        loop {
            tokio::time::sleep(Duration::from_millis(HEARTBEAT_POLL_INTERVAL_MS)).await;

            if state.is_leader() {
                info!(target: "heartbeat", "This node is now leader, stopping heartbeat poll");
                break;
            }

            let primary_addr = {
                let repl = state.replication.as_ref().unwrap().read().unwrap();
                if !repl.heartbeat_running {
                    break;
                }
                repl.primary_addr.clone()
            };

            if let Some(primary_addr) = primary_addr {
                let url = format!("{}/internal/heartbeat", primary_addr);
                match state.client.get(&url).send().await {
                    Ok(r) if r.status().is_success() => {
                        if let Ok(hb) = r.json::<serde_json::Value>().await {
                            let mut adopted = None;
                            {
                                let mut repl = state.replication.as_ref().unwrap().write().unwrap();
                                repl.last_heartbeat = Some(std::time::Instant::now());
                                if let Some(idx) = hb.get("commit_index").and_then(|v| v.as_u64()) {
                                    repl.last_known_primary_position = Some(idx);
                                }
                                if let Some(t) = hb.get("term").and_then(|v| v.as_u64()) {
                                    if t > repl.term {
                                        repl.term = t;
                                        repl.voted_for = None;
                                        adopted = Some(t);
                                    }
                                }
                            }
                            for (col, lsn) in hb
                                .get("committed")
                                .and_then(|c| c.as_object())
                                .map(|m| {
                                    m.iter()
                                        .filter_map(|(k, v)| v.as_u64().map(|l| (k.clone(), l)))
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                            {
                                state.note_leader_committed(&col, lsn);
                            }

                            if let Some(t) = adopted {
                                match (ReplicationMeta { term: t, is_leader: false, voted_for: None })
                                    .save(&state.config.data_dir)
                                {
                                    Ok(()) => info!(target: "heartbeat",
                                        "Adopted higher term {} from primary {}", t, primary_addr),
                                    Err(e) => warn!(target: "heartbeat", error = %e,
                                        "Adopted term {} in memory but could not persist it", t),
                                }
                            }
                        }
                    },
                    Ok(r) => {
                        warn!(target: "heartbeat", "Primary {} returned {}", primary_addr, r.status());
                        maybe_follow_new_leader(&state, &primary_addr).await;
                    },
                    Err(e) => {
                        warn!(target: "heartbeat", "Primary {} unreachable: {}", primary_addr, e);
                    }
                }
            }

            let quiet = {
                let repl = state.replication.as_ref().unwrap().read().unwrap();
                contact_lost(repl.last_heartbeat, repl.last_replication, task_started.elapsed(), timeout)
            };

            if !quiet {
                continue;
            }

            if adopt_existing_leader(&state).await {
                continue;
            }

            info!(target: "election", "No leader contact for {}s; standing for election", timeout.as_secs());
            run_election(&state, election_delay).await;

            if state.is_leader() {
                break;
            }
        }
    });
}

// Newer of the two signals: requiring fresh replication under a stale heartbeat never holds.
fn contact_lost(
    last_heartbeat: Option<std::time::Instant>,
    last_replication: Option<std::time::Instant>,
    since_start: Duration,
    timeout: Duration,
) -> bool {
    match [last_heartbeat, last_replication].into_iter().flatten().max() {
        Some(latest) => latest.elapsed() >= timeout,
        None => since_start >= timeout,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        leaders, node_by_id, put_doc_at, put_doc_http, read_doc_http, settle_leader, temp_root,
        three_node_cluster, TestNode,
    };
    use axum::http::StatusCode;

    fn ago(ms: u64) -> std::time::Instant {
        std::time::Instant::now() - Duration::from_millis(ms)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_follower_takes_over_when_the_leader_stops_heartbeating() {
        let root = temp_root();
        let (mut n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();

        assert_eq!(leaders(&[&n1, &n2, &n3]), vec!["n1".to_string()], "n1 starts as the only leader");

        assert_eq!(put_doc_http(&client, &n1.url(), "k1", 1).await, StatusCode::CREATED);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(read_doc_http(&client, &n2.url(), "k1").await, Some(1), "write replicated to n2");
        assert_eq!(read_doc_http(&client, &n3.url(), "k1").await, Some(1), "write replicated to n3");

        let term_before = n1.term();
        n1.kill();

        let winner = settle_leader(&[&n2, &n3], Duration::from_secs(20)).await
            .expect("a follower must stand for election once the leader stops heartbeating");

        let new_leader = node_by_id(&[&n2, &n3], &winner);
        assert!(new_leader.term() > term_before,
            "the new leader must run at a higher term ({} vs {})", new_leader.term(), term_before);

        assert_eq!(
            put_doc_at(&client, &new_leader.url(), "t", "k2", 2, "?w=majority&wtimeout=4000").await,
            StatusCode::CREATED, "the new leader must accept writes");
        assert_eq!(read_doc_http(&client, &new_leader.url(), "k2").await, Some(2),
            "a committed write is readable straight away");
        assert_eq!(read_doc_http(&client, &new_leader.url(), "k1").await, Some(1),
            "data written before the failover must survive it");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failover_elects_a_single_leader_every_time() {
        for attempt in 0..3 {
            let root = temp_root();
            let (mut n1, n2, n3) = three_node_cluster(&root).await;
            let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();

            assert_eq!(put_doc_http(&client, &n1.url(), "k1", 1).await, StatusCode::CREATED);
            tokio::time::sleep(Duration::from_millis(400)).await;
            n1.kill();

            let winner = settle_leader(&[&n2, &n3], Duration::from_secs(20)).await;
            assert!(winner.is_some(), "attempt {}: failover must be repeatable, not a fluke", attempt);

            let holder = node_by_id(&[&n2, &n3], winner.as_ref().unwrap());
            assert_eq!(put_doc_http(&client, &holder.url(), "k2", 2).await, StatusCode::CREATED,
                "attempt {}: the settled leader must accept writes", attempt);

            drop(n2);
            drop(n3);
            let _ = fs::remove_dir_all(&root);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_surviving_follower_follows_the_new_leader() {
        let root = temp_root();
        let (mut n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();

        assert_eq!(put_doc_http(&client, &n1.url(), "k1", 1).await, StatusCode::CREATED);
        tokio::time::sleep(Duration::from_millis(500)).await;
        n1.kill();

        let winner = settle_leader(&[&n2, &n3], Duration::from_secs(20)).await
            .expect("no leader was elected");

        let leader = node_by_id(&[&n2, &n3], &winner);
        let follower = if winner == "n2" { &n3 } else { &n2 };

        assert_eq!(put_doc_http(&client, &leader.url(), "k2", 2).await, StatusCode::CREATED);

        let follower_url = follower.url();
        let start = std::time::Instant::now();
        let mut converged = false;
        while start.elapsed() < Duration::from_secs(20) {
            if read_doc_http(&client, &follower_url, "k2").await == Some(2) {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(converged, "the surviving follower must discover the new leader and receive its writes");
        assert!(!follower.is_leader(), "the follower must not also claim leadership");
        assert_eq!(leaders(&[&n2, &n3]).len(), 1, "exactly one leader must remain");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_recovered_leader_rejoins_as_a_follower_without_stealing_back() {
        let root = temp_root();
        let (mut n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();

        assert_eq!(put_doc_http(&client, &n1.url(), "k1", 1).await, StatusCode::CREATED);
        tokio::time::sleep(Duration::from_millis(500)).await;

        n1.kill();
        let winner = settle_leader(&[&n2, &n3], Duration::from_secs(20)).await
            .expect("no leader was elected");
        let new_leader_term = node_by_id(&[&n2, &n3], &winner).term();

        let leader_url = node_by_id(&[&n2, &n3], &winner).url();
        assert_eq!(put_doc_http(&client, &leader_url, "k2", 2).await, StatusCode::CREATED);

        n1.start();
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert!(!n1.is_leader(),
            "a restarted leader must rejoin as a follower; resuming leadership from disk causes split brain");
        assert_eq!(leaders(&[&n1, &n2, &n3]), vec![winner.clone()],
            "the cluster must still have exactly the leader it elected");
        assert!(n1.term() >= new_leader_term,
            "the rejoining node must adopt the cluster's term, got {} vs {}", n1.term(), new_leader_term);

        assert_eq!(put_doc_http(&client, &n1.url(), "k3", 3).await, StatusCode::FORBIDDEN,
            "the rejoined node must refuse direct writes now that it is a follower");

        let start = std::time::Instant::now();
        let mut caught_up = false;
        while start.elapsed() < Duration::from_secs(20) {
            if read_doc_http(&client, &n1.url(), "k2").await == Some(2) {
                caught_up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(caught_up, "the rejoined node must receive the writes it missed while down");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_election_records_the_term_and_vote_on_disk() {
        let root = temp_root();
        let (mut n1, n2, n3) = three_node_cluster(&root).await;

        n1.kill();
        let winner = settle_leader(&[&n2, &n3], Duration::from_secs(20)).await
            .expect("no leader was elected");
        let leader = node_by_id(&[&n2, &n3], &winner);
        let elected_term = leader.term();

        let on_disk = |n: &TestNode| ReplicationMeta::load(&n.data_dir.to_string_lossy()).unwrap().unwrap();

        let leader_meta = on_disk(leader);
        assert_eq!(leader_meta.term, elected_term, "the term it leads at must be the term on disk");
        assert_eq!(leader_meta.voted_for.as_deref(), Some(winner.as_str()),
            "a leader must have durably voted for itself before soliciting votes");
        assert!(leader_meta.is_leader);

        let follower = if winner == "n2" { &n3 } else { &n2 };
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(10) && on_disk(follower).term < elected_term {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let follower_meta = on_disk(follower);
        assert_eq!(follower_meta.term, elected_term,
            "the voter must record the term it voted in, or a restart would let it vote again");
        assert!(follower_meta.voted_for.is_some(), "the vote itself must be recorded, not just the term");
        assert!(!follower_meta.is_leader);

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restarted_node_comes_back_at_the_term_it_recorded() {
        let root = temp_root();
        let (mut n1, n2, n3) = three_node_cluster(&root).await;

        n1.kill();
        let winner = settle_leader(&[&n2, &n3], Duration::from_secs(20)).await
            .expect("no leader was elected");
        let elected_term = node_by_id(&[&n2, &n3], &winner).term();

        let mut restarting = if winner == "n2" { n3 } else { n2 };
        let recorded = ReplicationMeta::load(&restarting.data_dir.to_string_lossy()).unwrap().unwrap();
        assert!(recorded.term >= elected_term);

        restarting.kill();
        restarting.start();

        assert_eq!(restarting.term(), recorded.term,
            "a node must resume at the term it last recorded; starting lower would let it \
             grant a second vote in a term it has already voted in");
        assert!(!restarting.is_leader());

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_healthy_leader_is_never_displaced() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();

        let term_before = n1.term();
        for i in 0..3 {
            assert_eq!(put_doc_http(&client, &n1.url(), &format!("k{}", i), i).await, StatusCode::CREATED);
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert_eq!(leaders(&[&n1, &n2, &n3]), vec!["n1".to_string()],
            "followers must not depose a leader that is still answering heartbeats");
        assert_eq!(n1.term(), term_before, "a stable cluster must not churn terms");

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_minority_cannot_elect_itself() {
        let root = temp_root();
        let (mut n1, n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();

        assert_eq!(put_doc_http(&client, &n1.url(), "k1", 1).await, StatusCode::CREATED);
        tokio::time::sleep(Duration::from_millis(500)).await;

        n1.kill();
        n3.kill();

        tokio::time::sleep(Duration::from_secs(8)).await;

        assert!(!n2.is_leader(),
            "a single survivor out of three must not promote itself; that would allow split brain");
        assert_eq!(put_doc_http(&client, &n2.url(), "k2", 2).await, StatusCode::FORBIDDEN,
            "a node that lost quorum must keep refusing writes");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn contact_lost_fires_once_the_leader_goes_quiet() {
        let timeout = Duration::from_secs(3);

        assert!(!contact_lost(Some(ago(500)), None, Duration::from_secs(60), timeout),
            "a fresh heartbeat means the leader is alive");
        assert!(contact_lost(Some(ago(4000)), None, Duration::from_secs(60), timeout),
            "a stale heartbeat must trigger an election");

        assert!(!contact_lost(None, Some(ago(500)), Duration::from_secs(60), timeout),
            "recent replication counts as leader contact even with no heartbeat");
        assert!(contact_lost(None, Some(ago(4000)), Duration::from_secs(60), timeout),
            "stale replication must not hold off an election");
    }

    #[test]
    fn a_dead_leader_silences_heartbeat_and_replication_together() {
        let timeout = Duration::from_secs(3);

        assert!(contact_lost(Some(ago(4000)), Some(ago(4000)), Duration::from_secs(60), timeout),
            "when a leader dies both signals go stale at once; this must still elect. \
             The original bug required replication to be FRESH while the heartbeat was STALE, \
             which can never hold, so failover never happened.");

        assert!(!contact_lost(Some(ago(4000)), Some(ago(100)), Duration::from_secs(60), timeout),
            "replication still arriving means the leader lives, whatever the heartbeat poll saw");
        assert!(!contact_lost(Some(ago(100)), Some(ago(4000)), Duration::from_secs(60), timeout),
            "the most recent of the two signals wins");
    }

    #[test]
    fn a_node_that_never_heard_from_anyone_still_elects() {
        let timeout = Duration::from_secs(3);

        assert!(!contact_lost(None, None, Duration::from_millis(500), timeout),
            "a freshly started node waits out the timeout before standing");
        assert!(contact_lost(None, None, Duration::from_secs(4), timeout),
            "a node with no contact at all must eventually stand, or a cluster that \
             never replicated could never elect a leader");
    }
}
