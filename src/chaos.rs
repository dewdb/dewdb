//! Directional link faults for cluster tests.
//!
//! Test-only, and deliberately so: a live switch that black-holes traffic between two nodes has no
//! place in a shipped database. Nodes name themselves in `auth::NODE_HEADER`, the receiving node's
//! middleware looks the pair up here, and a cut link hangs until the sender's own timeout
//! fires -- a partition the sender sees as `Err`, which is what distinguishes it from a node that
//! is up and refusing.

use crate::util::endpoint_of;
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
    (endpoint_of(from).to_string(), endpoint_of(to).to_string())
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
        leaders, node_by_id, put_doc_http, settle_leader, temp_root, three_node_cluster, wait_for,
    };
    use crate::util::same_endpoint;

    const SETTLE: Duration = Duration::from_secs(20);

    fn others<'a>(all: &[&'a str], one: &str) -> Vec<&'a str> {
        all.iter().copied().filter(|u| !same_endpoint(u, one)).collect()
    }

    /// bugs.md H10. The leader can still be *reached* -- every follower poll succeeds and every
    /// lease promise lands -- so nothing inbound tells it anything is wrong. Only its own probes
    /// failing can.
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

        delay(&leader_url, &slow, Duration::from_millis(1500));
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
        let started = std::time::Instant::now();
        let wrote = put_doc_http(&client, &leader_url, "k", 1).await;
        let took = started.elapsed();
        let held = node_by_id(&nodes, &leader).is_leader();
        heal(&leader_url, &slow);

        assert!(wrote.is_success(), "write to a leader with one slow voter got {}", wrote);
        assert!(took < Duration::from_millis(1500),
            "the write waited on the slow voter: {:?}", took);
        assert!(held, "leader {} stepped down over one slow voter", leader);
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
