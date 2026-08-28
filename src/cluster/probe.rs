//! Background probing of which node answers as primary for each shard.

use crate::cluster::metadata::{Adoption, ClusterMetadata};
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
    pub cluster_version: u64,
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
        cluster_version: v.get("cluster_version").and_then(|x| x.as_u64()).unwrap_or(0),
        load,
    })
}

/// Pulls the view a peer advertised. Version-gated by the caller, so a peer that lies about being
/// ahead costs one request; the adopt path still validates whatever comes back.
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

// Whoever is furthest ahead, so one pass converges even when only a replica has heard the update.
fn best_cluster_source(probes: &[(String, Option<Probe>)], ours: u64) -> Option<String> {
    probes.iter()
        .filter_map(|(url, res)| res.as_ref().map(|p| (p.cluster_version, url)))
        .filter(|(version, _)| *version > ours)
        .max_by_key(|(version, _)| *version)
        .map(|(_, url)| url.clone())
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
                    if target_endpoints.insert(crate::util::endpoint_of(&url).to_string()) {
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
                by_endpoint.insert(crate::util::endpoint_of(url).to_string(), probe.clone());
            }

            for (original, replicas, effective) in groups {
                let mut candidates = probe_targets(&original, &replicas);
                if !candidates.iter().any(|url| crate::util::same_endpoint(url, &effective)) {
                    candidates.push(effective.clone());
                }
                let group_probes: Vec<(String, Option<Probe>)> = candidates.into_iter()
                    .map(|url| {
                        let probe = by_endpoint.get(crate::util::endpoint_of(&url)).cloned().flatten();
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

            if let Some(source) = best_cluster_source(&probes, state.cluster_version()) {
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
            role: role.to_string(), term, cluster_version: 0, load: None,
        }))
    }

    fn at_version(url: &str, cluster_version: u64) -> (String, Option<Probe>) {
        (url.to_string(), Some(Probe {
            role: "replica".to_string(), term: 1, cluster_version, load: None,
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
        assert_eq!(best_cluster_source(&probes, 4).as_deref(), Some("http://b"),
            "a replica may hear an update before the primary this router is talking to");

        assert_eq!(best_cluster_source(&probes, 9), None,
            "already current: probing must not turn into a fetch every three seconds");
        assert_eq!(best_cluster_source(&probes, 20), None, "ahead of every peer");
        assert_eq!(best_cluster_source(&[], 0), None);
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
