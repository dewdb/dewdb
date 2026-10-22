//! Directional link faults for cluster tests, test-only by design. Nodes name themselves in
//! `auth::NODE_HEADER` and a cut link hangs, so the sender sees the `Err` a partition gives it.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Fault {
    Cut,
    Delay(Duration),
}

/// Process-wide, because every node in a cluster test is a thread of one process. Keyed on
/// `host:port` pairs, so tests reach for it with whatever URL form they already hold.
fn table() -> &'static Mutex<HashMap<(String, String), Fault>> {
    static TABLE: OnceLock<Mutex<HashMap<(String, String), Fault>>> = OnceLock::new();
    TABLE.get_or_init(Default::default)
}

fn key(from: &str, to: &str) -> (String, String) {
    (crate::util::node_key(from), crate::util::node_key(to))
}

fn set(from: &str, to: &str, fault: Fault) {
    table().lock().unwrap().insert(key(from, to), fault);
}

/// One direction only: `from` can no longer reach `to`, while `to` reaches `from` as before. The
/// asymmetric case is the interesting one, and the symmetric case is two of these.
pub fn cut(from: &str, to: &str) {
    set(from, to, Fault::Cut);
}

pub fn delay(from: &str, to: &str, by: Duration) {
    set(from, to, Fault::Delay(by));
}

pub fn heal(from: &str, to: &str) {
    table().lock().unwrap().remove(&key(from, to));
}

/// Both directions, for every peer: `node` is off the network but still running, which is what
/// separates a partition from a crash.
pub fn isolate(node: &str, peers: &[&str]) {
    for peer in peers {
        cut(node, peer);
        cut(peer, node);
    }
}

pub fn rejoin(node: &str, peers: &[&str]) {
    for peer in peers {
        heal(node, peer);
        heal(peer, node);
    }
}

pub fn lookup(from: &str, to: &str) -> Option<Fault> {
    table().lock().unwrap().get(&key(from, to)).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ports are unique per test, so entries never collide across the suite's parallel runs.
    const A: &str = "http://127.0.0.1:59001";
    const B: &str = "http://127.0.0.1:59002";

    #[test]
    fn a_cut_link_is_one_way() {
        cut(A, B);
        assert_eq!(lookup(A, B), Some(Fault::Cut));
        assert_eq!(lookup(B, A), None);
        heal(A, B);
    }

    #[test]
    fn a_fault_is_found_whatever_url_form_names_the_node() {
        let (a, b) = ("http://127.0.0.1:59003", "http://127.0.0.1:59004");
        cut(a, b);
        assert_eq!(lookup("127.0.0.1:59003", &format!("{}/internal/heartbeat", b)), Some(Fault::Cut));
        heal(a, b);
    }

    #[test]
    fn isolating_a_node_cuts_it_in_both_directions() {
        let (a, b) = ("http://127.0.0.1:59005", "http://127.0.0.1:59006");
        isolate(a, &[b]);
        assert_eq!(lookup(a, b), Some(Fault::Cut));
        assert_eq!(lookup(b, a), Some(Fault::Cut));
        rejoin(a, &[b]);
        assert_eq!(lookup(a, b), None);
        assert_eq!(lookup(b, a), None);
    }
}

/// Chaos tests: faults injected into a live cluster, asserting what consensus is supposed to
/// survive. Kept here rather than beside each subsystem because the fault is the subject.
#[cfg(test)]
mod cluster {
    use super::*;
    use crate::test_support::{
        leaders, node_by_id, put_doc_at, put_doc_http, settle_leader, temp_root, three_node_cluster,
        voter_group, voter_group_starting, wait_for, DEFAULT_HEARTBEAT_TIMEOUT_SECS,
    };
    use crate::util::same_endpoint;

    const SETTLE: Duration = Duration::from_secs(20);

    fn others<'a>(all: &[&'a str], one: &str) -> Vec<&'a str> {
        all.iter().copied().filter(|u| !same_endpoint(u, one)).collect()
    }

    /// H10: the leader can still be reached -- every follower poll succeeds and every lease promise
    /// lands -- so nothing inbound tells it anything is wrong. Only its own probes failing can.
    #[tokio::test]
    async fn an_asymmetric_partition_of_the_leaders_outbound_path_still_replaces_it() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let nodes = [&n1, &n2, &n3];
        let leader = settle_leader(&nodes, SETTLE).await.expect("no initial leader");
        let leader_url = node_by_id(&nodes, &leader).url();
        let urls: Vec<String> = nodes.iter().map(|n| n.url()).collect();
        let refs: Vec<&str> = urls.iter().map(String::as_str).collect();

        for peer in others(&refs, &leader_url) {
            cut(&leader_url, peer);
        }

        // Waited on before settling: the old leader is still the one stable leader for as long as
        // it takes to notice, and `settle_leader` would report it and call the cluster healthy.
        let stepped_down = wait_for(SETTLE, || !node_by_id(&nodes, &leader).is_leader()).await;
        let replaced = settle_leader(&nodes, SETTLE).await;
        for peer in others(&refs, &leader_url) {
            heal(&leader_url, peer);
        }
        assert!(stepped_down, "leader {} never noticed it could reach nobody", leader);
        assert_eq!(leaders(&nodes).len(), 1, "expected exactly one leader");
        assert!(matches!(replaced, Some(ref id) if *id != leader),
            "leader {} was not replaced, got {:?}", leader, replaced);
    }

    /// The other half of the same rule: a leader that has lost one voter has lost nothing, and
    /// stepping down there would trade an availability bug for an election on every blip.
    #[tokio::test]
    async fn a_leader_that_still_reaches_a_majority_is_left_alone() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let nodes = [&n1, &n2, &n3];
        let leader = settle_leader(&nodes, SETTLE).await.expect("no initial leader");
        let leader_url = node_by_id(&nodes, &leader).url();
        let urls: Vec<String> = nodes.iter().map(|n| n.url()).collect();
        let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
        let severed = others(&refs, &leader_url)[0].to_string();

        cut(&leader_url, &severed);
        tokio::time::sleep(Duration::from_secs(4)).await;
        let held = node_by_id(&nodes, &leader).is_leader();

        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
        let wrote = put_doc_http(&client, &leader_url, "k", 1).await;
        heal(&leader_url, &severed);

        assert!(held, "leader {} stepped down over one unreachable voter", leader);
        assert!(wrote.is_success(), "a majority was still reachable, but the write got {}", wrote);
    }

    /// No split brain while the partition stands: the minority side gives up leadership, and the
    /// majority side elects someone else.
    #[tokio::test]
    async fn an_isolated_leader_steps_down_and_the_majority_elects_one_that_is_not_it() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let nodes = [&n1, &n2, &n3];
        let leader = settle_leader(&nodes, SETTLE).await.expect("no initial leader");
        let leader_url = node_by_id(&nodes, &leader).url();
        let urls: Vec<String> = nodes.iter().map(|n| n.url()).collect();
        let refs: Vec<&str> = urls.iter().map(String::as_str).collect();

        isolate(&leader_url, &others(&refs, &leader_url));
        let stepped_down = wait_for(SETTLE, || !node_by_id(&nodes, &leader).is_leader()).await;
        let elected = settle_leader(&nodes, SETTLE).await;
        rejoin(&leader_url, &others(&refs, &leader_url));

        assert!(stepped_down, "isolated leader {} kept claiming leadership", leader);
        assert!(matches!(elected, Some(ref id) if *id != leader),
            "majority did not elect a replacement, got {:?}", elected);
    }

    /// Healing must not leave the two sides disagreeing. The isolated node comes back at a term the
    /// cluster has moved past, so it follows rather than contests.
    #[tokio::test]
    async fn a_healed_partition_leaves_the_old_leader_following_the_new_one() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let nodes = [&n1, &n2, &n3];
        let leader = settle_leader(&nodes, SETTLE).await.expect("no initial leader");
        let leader_url = node_by_id(&nodes, &leader).url();
        let urls: Vec<String> = nodes.iter().map(|n| n.url()).collect();
        let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
        let peers = others(&refs, &leader_url);

        isolate(&leader_url, &peers);
        assert!(wait_for(SETTLE, || !node_by_id(&nodes, &leader).is_leader()).await,
            "isolated leader {} never stepped down", leader);
        let successor = settle_leader(&nodes, SETTLE).await.expect("majority elected nobody");
        rejoin(&leader_url, &peers);

        let settled = settle_leader(&nodes, SETTLE).await;
        let converged = wait_for(SETTLE, || {
            node_by_id(&nodes, &leader).term() >= node_by_id(&nodes, &successor).term()
        }).await;
        assert_eq!(settled.as_deref(), Some(successor.as_str()),
            "healing changed the leader from {}", successor);
        assert!(!node_by_id(&nodes, &leader).is_leader(),
            "old leader {} resumed leadership after healing", leader);
        assert!(converged, "rejoined node never adopted the term it missed");
    }

    /// A voter that is slow rather than gone must cost the write path nothing: the quorum is the
    /// other two, and the leader's own probes to it timing out is not a loss of quorum either.
    #[tokio::test]
    async fn a_slow_link_to_one_voter_stalls_neither_writes_nor_leadership() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let nodes = [&n1, &n2, &n3];
        let leader = settle_leader(&nodes, SETTLE).await.expect("no initial leader");
        let leader_url = node_by_id(&nodes, &leader).url();
        let urls: Vec<String> = nodes.iter().map(|n| n.url()).collect();
        let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
        let slow = others(&refs, &leader_url)[0].to_string();

        // Wide on purpose: the assertion below separates "did not wait" from "waited", and a threshold
        // at the delay itself leaves no room for a scheduler stall (L8b).
        delay(&leader_url, &slow, Duration::from_secs(6));
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
        let started = std::time::Instant::now();
        let wrote = put_doc_http(&client, &leader_url, "k", 1).await;
        let took = started.elapsed();
        let held = node_by_id(&nodes, &leader).is_leader();
        heal(&leader_url, &slow);

        assert!(wrote.is_success(), "write to a leader with one slow voter got {}", wrote);
        assert!(took < Duration::from_millis(2500),
            "the write waited on the slow voter: {:?}", took);
        assert!(held, "leader {} stepped down over one slow voter", leader);
    }

    /// Acknowledged `w=majority` writes per second from `clients` writers over `window`. A `202`
    /// missed its concern, so it is not counted: that is how a starved quorum shows up.
    async fn quorum_writes_per_sec(leader: &str, clients: usize, window: Duration, tag: &str) -> f64 {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(clients * 2)
            .timeout(Duration::from_secs(10))
            .build().unwrap();
        let until = std::time::Instant::now() + window;
        let writers = (0..clients).map(|w| {
            let (client, leader, tag) = (client.clone(), leader.to_string(), tag.to_string());
            tokio::spawn(async move {
                let mut acked = 0usize;
                let mut i = 0i64;
                while std::time::Instant::now() < until {
                    let key = format!("{}-{}-{}", tag, w, i);
                    let status = put_doc_at(&client, &leader, "t", &key, i, "?w=majority&wtimeout=5000").await;
                    if status.is_success() && status != axum::http::StatusCode::ACCEPTED {
                        acked += 1;
                    }
                    i += 1;
                }
                acked
            })
        }).collect::<Vec<_>>();
        let mut acked = 0;
        for w in writers {
            acked += w.await.unwrap();
        }
        acked as f64 / window.as_secs_f64()
    }

    const STARVATION_CLIENTS: usize = 8;
    const STARVATION_WINDOW: Duration = Duration::from_secs(4);

    /// With one of three voters gone, `w=majority` is the leader and the one that is left: the same
    /// work as a two-voter group, which is the rate this has to stay near.
    async fn two_voter_quorum_rate() -> f64 {
        let root = temp_root();
        let pair = voter_group(&root, 2, DEFAULT_HEARTBEAT_TIMEOUT_SECS).await;
        let leader = pair[0].url();
        quorum_writes_per_sec(&leader, STARVATION_CLIENTS, Duration::from_secs(1), "warm").await;
        quorum_writes_per_sec(&leader, STARVATION_CLIENTS, STARVATION_WINDOW, "pair").await
    }

    /// `compare_couchdb.md` diagnostic 2. Every replica's sends shared one node-wide budget of
    /// `max_inflight_requests`, so requests to a voter that never answers held all of it and the
    /// live voter's waited behind their timeouts: 8 writes a second for 250. The link is cut rather
    /// than the node stopped because a hung request is what a powered-off host gives on any OS.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn an_unreachable_voter_does_not_starve_the_quorum_that_is_left() {
        let baseline = two_voter_quorum_rate().await;

        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let nodes = [&n1, &n2, &n3];
        let leader = settle_leader(&nodes, SETTLE).await.expect("no initial leader");
        let leader_node = node_by_id(&nodes, &leader);
        let leader_url = leader_node.url();
        let gone = nodes.iter().find(|n| n.node_id != leader).unwrap().url();

        // Before the first write, so the leader holds no send cursor for it: every frame then goes
        // out as its own request, which is what a member missing since boot or a failed-over-from
        // leader gets. A peer with a cursor is sent its backlog by one repair at a time instead.
        cut(&leader_url, &gone);
        quorum_writes_per_sec(&leader_url, STARVATION_CLIENTS, Duration::from_secs(1), "warm").await;
        let degraded = quorum_writes_per_sec(&leader_url, STARVATION_CLIENTS, STARVATION_WINDOW, "cut").await;
        heal(&leader_url, &gone);

        // And the peer cannot be shut out for good: a probe finds it back and the cursor resends.
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
        assert!(put_doc_at(&client, &leader_url, "t", "after-heal", 1, "?w=all&wtimeout=15000").await
                == axum::http::StatusCode::CREATED,
            "the healed voter never rejoined the quorum");

        assert!(degraded >= baseline * 0.5,
            "one unreachable voter cut quorum writes to {:.0}/s against {:.0}/s for a two-voter group",
            degraded, baseline);
    }

    /// The same defect from boot: a group restarted with one member missing. On Windows a connect
    /// to a closed port takes about a second to be refused, which held the shared budget as surely
    /// as a hung request.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_voter_that_never_started_does_not_starve_the_quorum_that_is_left() {
        let baseline = two_voter_quorum_rate().await;

        let root = temp_root();
        let group = voter_group_starting(&root, 3, DEFAULT_HEARTBEAT_TIMEOUT_SECS, 2).await;
        let leader = group[0].url();
        quorum_writes_per_sec(&leader, STARVATION_CLIENTS, Duration::from_secs(1), "warm").await;
        let degraded = quorum_writes_per_sec(&leader, STARVATION_CLIENTS, STARVATION_WINDOW, "absent").await;

        assert!(degraded >= baseline * 0.5,
            "a member that never started cut quorum writes to {:.0}/s against {:.0}/s for a \
             two-voter group", degraded, baseline);
    }

    /// Both followers of a dead leader go quiet together, and the one with the longer jitter votes for
    /// the other while it waits. It used to stand anyway, and the leader it had just elected granted it
    /// a pre-vote and a vote, so leadership moved twice in a second and writes pinned to the first
    /// winner got 403 (dbbench diag 3 at ~700 writes/s, one run in five).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_follower_that_votes_while_waiting_to_stand_leaves_the_winner_leading() {
        use crate::consensus::election::Pause;
        let root = temp_root();
        let mut group = voter_group(&root, 3, 2).await;
        let (n2, n3) = (group[1].url(), group[2].url());
        let late = group[2].state.clone().unwrap();

        // n2's campaign reaches n3 a second late, so n3 is past its own search for a leader, and
        // holding there, before n2 can win. The hold is where n3's jitter and search used to end.
        let wait = Pause::arm(&late.append_order.after_campaign_wait);
        delay(&n2, &n3, Duration::from_secs(1));
        group[0].kill();
        tokio::time::timeout(SETTLE, wait.entered.notified()).await
            .expect("n3 never went quiet and began a campaign");
        let won = wait_for(SETTLE, || group[1].is_leader()).await;
        heal(&n2, &n3);
        let term = group[1].term();
        wait.release.notify_one();

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(won, "premise: n2 wins with n3's vote while n3 waits to stand");
        assert!(group[1].is_leader() && group[1].term() == term,
            "the winner of term {} was deposed: n2 at term {} leading {}, n3 at term {} leading {}",
            term, group[1].term(), group[1].is_leader(), group[2].term(), group[2].is_leader());
        assert!(!group[2].is_leader(), "n3 stood after voting for n2");
    }

    /// The injector itself: a cut has to look like a partition to the sender -- an error on its own
    /// timeout -- and not like a peer that is up and answering.
    #[tokio::test]
    async fn a_cut_link_fails_the_sender_and_leaves_the_reverse_direction_open() {
        let root = temp_root();
        let (n1, n2, _n3) = three_node_cluster(&root).await;
        cut(&n1.url(), &n2.url());

        let speak_as = |from: &str| {
            reqwest::Client::builder()
                .timeout(Duration::from_millis(600))
                .default_headers({
                    let mut h = reqwest::header::HeaderMap::new();
                    h.insert(crate::auth::NODE_HEADER,
                        reqwest::header::HeaderValue::from_str(from).unwrap());
                    h
                })
                .build().unwrap()
        };

        let blocked = speak_as(&n1.url())
            .get(format!("{}/internal/heartbeat", n2.url())).send().await;
        let reverse = speak_as(&n2.url())
            .get(format!("{}/internal/heartbeat", n1.url())).send().await;
        heal(&n1.url(), &n2.url());

        assert!(blocked.is_err(), "a cut link answered instead of timing out");
        assert!(reverse.is_ok_and(|r| r.status().is_success()), "the reverse direction was cut too");
    }
}
