//! Background probing of which node answers as primary for each shard.

use crate::cluster::metadata::{Adoption, ClusterMetadata, ViewId};
use crate::metrics::NodeLoad;
use crate::state::AppState;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{info, warn};

pub const ROUTER_PROBE_INTERVAL_SECS: u64 = 3;

#[derive(Clone)]
pub struct Probe {
    pub role: String,
    pub term: u64,
    pub cluster: ViewId,
    pub load: Option<NodeLoad>,
}

pub(crate) async fn probe_node(client: &reqwest::Client, url: &str) -> Option<Probe> {
    let hb = format!("{}/internal/heartbeat", url);
    let r = client.get(&hb).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    let v = r.json::<serde_json::Value>().await.ok()?;
    parse_probe(&v)
}

fn parse_probe(v: &serde_json::Value) -> Option<Probe> {
    let load = v.get("load").and_then(|load| Some(NodeLoad {
        inflight: load.get("inflight")?.as_u64()?,
        latency_ewma_us: load.get("latency_ewma_us")?.as_u64()?,
    }));
    Some(Probe {
        role: v.get("role").and_then(|x| x.as_str())?.to_string(),
        term: v.get("term").and_then(|x| x.as_u64()).unwrap_or(0),
        cluster: parse_view_id(v),
        load,
    })
}

/// Pulls the view a peer advertised. Gated on the advertised identity by the caller, so a peer
/// that lies about being ahead costs one request; the adopt path still validates what comes back.
pub async fn fetch_cluster_view(client: &reqwest::Client, url: &str) -> Option<ClusterMetadata> {
    let r = client.get(&format!("{}/internal/cluster", url)).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    r.json::<ClusterMetadata>().await.ok()
}

// Highest term wins: a partitioned old primary must not reclaim traffic.
pub(crate) fn select_primary(probes: &[(String, Option<Probe>)]) -> Option<String> {
    let mut best: Option<(u64, String)> = None;
    for (url, res) in probes {
        if let Some(p) = res {
            if p.role == "primary" && best.as_ref().map_or(true, |(t, _)| p.term > *t) {
                best = Some((p.term, url.clone()));
            }
        }
    }
    best.map(|(_, u)| u)
}

/// The advertised ordering identity. `updated_by` and `seeded` are absent from a peer that
/// predates them, which `ViewId` treats as an unorderable equal version rather than a win.
pub(crate) fn parse_view_id(v: &serde_json::Value) -> ViewId {
    ViewId {
        version: v.get("cluster_version").and_then(|x| x.as_u64()).unwrap_or(0),
        updated_by: v.get("cluster_updated_by").and_then(|x| x.as_str()).map(str::to_string),
        seeded: v.get("cluster_seeded").and_then(|x| x.as_bool()).unwrap_or(false),
    }
}

/// Whoever is furthest ahead, so one pass converges even when only a replica heard the update. Ordered
/// by adoption's total order, not by version: a `version >` gate never fetches a tiebreak winner.
fn best_cluster_source(probes: &[(String, Option<Probe>)], ours: &ViewId) -> Option<String> {
    probes.iter()
        .filter_map(|(url, res)| res.as_ref().map(|p| (&p.cluster, url)))
        .filter(|(theirs, _)| theirs.supersedes(ours))
        .max_by(|(a, _), (b, _)| order_key(a).cmp(&order_key(b)))
        .map(|(_, url)| url.clone())
}

// Only ever compared between peers that already beat this node's view, so an unorderable
// `updated_by` sorting lowest picks a different source, never a losing one.
fn order_key(id: &ViewId) -> (u64, &str) {
    (id.version, id.updated_by.as_deref().unwrap_or(""))
}

pub fn unique_shards(state: &AppState) -> Vec<(String, Vec<String>)> {
    state.shard_owners()
}

fn probe_targets(original: &str, replicas: &[String]) -> Vec<String> {
    let mut candidates = vec![original.to_string()];
    candidates.extend(replicas.iter().cloned());
    let mut seen = HashSet::new();
    candidates.retain(|c| seen.insert(c.clone()));
    candidates
}

pub fn router_probe_task(state: AppState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(ROUTER_PROBE_INTERVAL_SECS)).await;

            let groups: Vec<(String, Vec<String>, String)> = unique_shards(&state)
                .into_iter()
                .map(|(original, replicas)| {
                    let effective = state.effective_primary(&original);
                    (original, replicas, effective)
                })
                .collect();
            let mut targets = Vec::new();
            let mut target_endpoints = HashSet::new();
            for (original, replicas, effective) in &groups {
                for url in probe_targets(original, replicas).into_iter().chain([effective.clone()]) {
                    if target_endpoints.insert(crate::util::node_key(&url)) {
                        targets.push(url);
                    }
                }
            }

            let client = state.client.clone();
            let probes = futures::future::join_all(targets.into_iter().map(|url| {
                let client = client.clone();
                async move {
                    let result = probe_node(&client, &url).await;
                    (url, result)
                }
            })).await;

            let mut by_endpoint = HashMap::new();
            for (url, probe) in &probes {
                match probe.as_ref().and_then(|p| p.load) {
                    Some(load) => state.note_node_load(url, load),
                    None => state.clear_node_load(url),
                }
                by_endpoint.insert(crate::util::node_key(url), probe.clone());
            }

            for (original, replicas, effective) in groups {
                let mut candidates = probe_targets(&original, &replicas);
                if !candidates.iter().any(|url| crate::util::same_endpoint(url, &effective)) {
                    candidates.push(effective.clone());
                }
                let group_probes: Vec<(String, Option<Probe>)> = candidates.into_iter()
                    .map(|url| {
                        let probe = by_endpoint.get(&crate::util::node_key(&url)).cloned().flatten();
                        (url, probe)
                    })
                    .collect();

                match select_primary(&group_probes) {
                    Some(winner) => {
                        if winner == original {
                            state.clear_primary_override(&original);
                        } else if winner != effective {
                            state.set_primary_override(&original, &winner);
                            info!(target: "router_probe", "Shard {} primary is now {}", original, winner);
                        } else {
                            state.set_primary_override(&original, &winner);
                        }
                    },
                    None => {
                        warn!(target: "router_probe", "Shard {} has no reachable primary (election in progress?)", original);
                    }
                }

            }

            if let Some(source) = best_cluster_source(&probes, &state.cluster_view_id()) {
                if let Some(view) = fetch_cluster_view(&state.client, &source).await {
                    match state.adopt_cluster(view) {
                        Adoption::Adopted { from, to } => info!(target: "router_probe",
                            "Adopted cluster view v{} from {} (was v{})", to, source, from),
                        Adoption::Rejected(why) => warn!(target: "router_probe",
                            "Refused cluster view from {}: {}", source, why),
                        Adoption::Stale { .. } => {},
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(url: &str, role: &str, term: u64) -> (String, Option<Probe>) {
        (url.to_string(), Some(Probe {
            role: role.to_string(), term, cluster: ViewId::default(), load: None,
        }))
    }

    fn id(version: u64, updated_by: &str) -> ViewId {
        ViewId { version, updated_by: Some(updated_by.to_string()), seeded: false }
    }

    fn at_version(url: &str, cluster_version: u64) -> (String, Option<Probe>) {
        published(url, cluster_version, "op")
    }

    fn published(url: &str, version: u64, updated_by: &str) -> (String, Option<Probe>) {
        (url.to_string(), Some(Probe {
            role: "replica".to_string(), term: 1, cluster: id(version, updated_by), load: None,
        }))
    }

    #[test]
    fn select_primary_picks_the_reachable_leader() {
        let probes = vec![
            probe("http://a", "replica", 5),
            probe("http://b", "primary", 5),
            probe("http://c", "replica", 5),
        ];
        assert_eq!(select_primary(&probes).as_deref(), Some("http://b"));
    }

    #[test]
    fn select_primary_prefers_highest_term_on_split() {
        let probes = vec![
            probe("http://old", "primary", 4),
            probe("http://new", "primary", 6),
        ];
        assert_eq!(select_primary(&probes).as_deref(), Some("http://new"));
    }

    #[test]
    fn select_primary_none_when_no_leader() {
        let probes = vec![
            probe("http://a", "replica", 5),
            ("http://b".to_string(), None),
        ];
        assert_eq!(select_primary(&probes), None);
    }

    #[test]
    fn the_cluster_view_is_pulled_from_whoever_is_furthest_ahead() {
        let probes = vec![
            at_version("http://a", 4),
            at_version("http://b", 9),
            at_version("http://c", 7),
            ("http://down".to_string(), None),
        ];
        assert_eq!(best_cluster_source(&probes, &id(4, "op")).as_deref(), Some("http://b"),
            "a replica may hear an update before the primary this router is talking to");

        assert_eq!(best_cluster_source(&probes, &id(9, "op")), None,
            "already current: probing must not turn into a fetch every three seconds");
        assert_eq!(best_cluster_source(&probes, &id(20, "op")), None, "ahead of every peer");
        assert_eq!(best_cluster_source(&[], &id(0, "op")), None);
    }

    /// IB-023: adoption orders views by `(version, updated_by)` but this gate compared `version` alone,
    /// so two nodes holding conflicting equal-version views had no poll that would fetch the winner.
    #[test]
    fn an_equal_version_conflict_is_fetched_and_converges_one_way() {
        let peers = vec![published("http://bravo", 5, "bravo")];
        assert_eq!(best_cluster_source(&peers, &id(5, "alpha")).as_deref(), Some("http://bravo"),
            "the tiebreak winner at the same version is exactly the view this node is missing");

        // The loser is not fetched back, so the two do not trade views forever.
        let winner = vec![published("http://alpha", 5, "alpha")];
        assert_eq!(best_cluster_source(&winner, &id(5, "bravo")), None);
        assert_eq!(best_cluster_source(&peers, &id(5, "bravo")), None, "already converged");

        // A higher version still outranks a tiebreak; the order is lexicographic on the pair.
        let mixed = vec![published("http://old", 6, "zulu"), published("http://new", 7, "alpha")];
        assert_eq!(best_cluster_source(&mixed, &id(5, "alpha")).as_deref(), Some("http://new"));
    }

    /// A seed loses to everything, so a node still on its config seed must fetch a real view
    /// published at the same version rather than sit on the seed forever.
    #[test]
    fn a_seeded_view_is_replaced_by_a_published_one_at_the_same_version() {
        let seeded = ViewId { version: 1, updated_by: Some("n1".into()), seeded: true };
        let peers = vec![published("http://router", 1, "op")];
        assert_eq!(best_cluster_source(&peers, &seeded).as_deref(), Some("http://router"));

        let peer_seeded = vec![(
            "http://shard".to_string(),
            Some(Probe { role: "replica".into(), term: 1, cluster: seeded.clone(), load: None }),
        )];
        assert_eq!(best_cluster_source(&peer_seeded, &id(1, "op")), None,
            "a seed wins against nothing; adopting one would route every key nowhere");
    }

    /// The tiebreak rides in the heartbeat, so a peer that predates it must not be read as
    /// advertising an empty `updated_by` that loses every comparison it should not enter.
    #[test]
    fn a_peer_that_does_not_advertise_the_tiebreak_falls_back_to_the_version() {
        let old = serde_json::json!({"role": "replica", "term": 2, "cluster_version": 7});
        let advertised = parse_view_id(&old);
        assert_eq!(advertised.updated_by, None);
        assert!(!advertised.seeded);

        assert!(advertised.supersedes(&id(6, "zulu")), "a higher version still moves this node");
        assert!(!advertised.supersedes(&id(7, "alpha")),
            "an unorderable equal version is not a win: half-upgraded behaves as before");

        let current = serde_json::json!({
            "role": "replica", "term": 2,
            "cluster_version": 7, "cluster_updated_by": "bravo", "cluster_seeded": false,
        });
        assert!(parse_view_id(&current).supersedes(&id(7, "alpha")));
    }

    #[test]
    fn heartbeat_load_is_optional_during_a_rolling_upgrade() {
        let old = serde_json::json!({"role": "replica", "term": 2, "cluster_version": 7});
        assert!(parse_probe(&old).unwrap().load.is_none());

        let current = serde_json::json!({
            "role": "replica",
            "term": 2,
            "cluster_version": 7,
            "load": {"inflight": 3, "latency_ewma_us": 4500},
        });
        assert_eq!(
            parse_probe(&current).unwrap().load,
            Some(NodeLoad { inflight: 3, latency_ewma_us: 4_500 }),
        );
    }
}
