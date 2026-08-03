//! Background probing of which node answers as primary for each shard.

use crate::state::AppState;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{info, warn};

pub const ROUTER_PROBE_INTERVAL_SECS: u64 = 3;

async fn probe_node(client: &reqwest::Client, url: &str) -> Option<(String, u64)> {
    let hb = format!("{}/internal/heartbeat", url);
    let r = client.get(&hb).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    let v = r.json::<serde_json::Value>().await.ok()?;
    let role = v.get("role").and_then(|x| x.as_str())?.to_string();
    let term = v.get("term").and_then(|x| x.as_u64()).unwrap_or(0);
    Some((role, term))
}

// Highest term wins so a partitioned old primary cannot reclaim traffic.
fn select_primary(probes: &[(String, Option<(String, u64)>)]) -> Option<String> {
    let mut best: Option<(u64, String)> = None;
    for (url, res) in probes {
        if let Some((role, term)) = res {
            if role == "primary" && best.as_ref().map_or(true, |(t, _)| *term > *t) {
                best = Some((*term, url.clone()));
            }
        }
    }
    best.map(|(_, u)| u)
}

pub fn unique_shards(state: &AppState) -> Vec<(String, Vec<String>)> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for shard in &state.config.shard_map {
        if seen.insert(shard.node_url.clone()) {
            out.push((shard.node_url.clone(), shard.replica_urls.clone()));
        }
    }
    out
}

pub fn router_probe_task(state: AppState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(ROUTER_PROBE_INTERVAL_SECS)).await;

            for (original, replicas) in unique_shards(&state) {
                let effective = state.effective_primary(&original);

                if let Some((role, _term)) = probe_node(&state.client, &effective).await {
                    if role == "primary" {
                        if effective != original {
                            state.set_primary_override(&original, &effective);
                        }
                        continue;
                    }
                }

                let mut candidates = vec![original.clone()];
                candidates.extend(replicas.iter().cloned());
                candidates.sort();
                candidates.dedup();

                let mut probes = Vec::new();
                for c in candidates {
                    let res = probe_node(&state.client, &c).await;
                    probes.push((c, res));
                }

                match select_primary(&probes) {
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
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(url: &str, role: &str, term: u64) -> (String, Option<(String, u64)>) {
        (url.to_string(), Some((role.to_string(), term)))
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
}
