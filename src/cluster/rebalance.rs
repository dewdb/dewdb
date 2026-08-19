//! Reconciles primary-shard membership with consistent-hash ownership via safe migrations.

use crate::api::migrate::{begin_migration, resume_migration_coordination, MigrationLaunch};
use crate::cluster::metadata::ClusterMetadata;
use crate::ring::{keyspace_movement, HashRing, RingShard};
use crate::state::AppState;
use crate::util::{endpoint_of, same_endpoint};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[derive(Deserialize, Clone, Debug)]
pub struct RebalanceConfig {
    /// Opt-in because legacy membership may omit current shards.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    /// Debounces membership churn into one migration.
    #[serde(default = "default_stabilization")]
    pub stabilization_secs: u64,
}

fn default_interval() -> u64 {
    10
}
fn default_stabilization() -> u64 {
    30
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_interval(),
            stabilization_secs: default_stabilization(),
        }
    }
}

impl RebalanceConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.interval_secs == 0 {
            return Err("rebalance.interval_secs must be greater than zero".to_string());
        }
        Ok(())
    }
}

fn replica_placements(
    view: &ClusterMetadata,
    current: &HashRing,
    primaries: &[String],
) -> BTreeMap<String, Vec<String>> {
    let primary_endpoints: HashSet<&str> = primaries
        .iter()
        .map(|url| endpoint_of(url))
        .collect();
    let replicas: BTreeMap<&str, _> = view
        .members
        .iter()
        .filter(|member| {
            member.role == "shard" && member.shard_role.as_deref() == Some("replica")
        })
        .map(|member| (endpoint_of(&member.url), member))
        .collect();
    let mut placements: BTreeMap<String, Vec<String>> = primaries
        .iter()
        .map(|primary| (endpoint_of(primary).to_string(), Vec::new()))
        .collect();

    for shard in &current.shards {
        let primary = endpoint_of(&shard.node_url);
        if !primary_endpoints.contains(primary) {
            continue;
        }
        for replica_url in &shard.replica_urls {
            let replica = match replicas.get(endpoint_of(replica_url)) {
                Some(replica) => replica,
                None => continue,
            };
            if replica
                .follows
                .as_deref()
                .is_some_and(|follows| endpoint_of(follows) != primary)
            {
                continue;
            }
            placements
                .get_mut(primary)
                .unwrap()
                .push(replica.url.clone());
        }
    }

    for replica in replicas.values() {
        let primary = match replica.follows.as_deref() {
            Some(primary) if primary_endpoints.contains(endpoint_of(primary)) => {
                endpoint_of(primary)
            }
            _ => continue,
        };
        placements
            .get_mut(primary)
            .unwrap()
            .push(replica.url.clone());
    }

    for replicas in placements.values_mut() {
        replicas.sort_by(|a, b| endpoint_of(a).cmp(endpoint_of(b)));
        replicas.dedup_by(|a, b| same_endpoint(a, b));
    }
    placements
}

/// Derives stable primary and replica placement from live membership.
pub fn desired_ring(view: &ClusterMetadata) -> Result<Option<HashRing>, String> {
    let current = match &view.ring {
        Some(ring) => ring,
        None => return Ok(None),
    };

    let mut seen = HashSet::new();
    let mut primaries: Vec<String> = view
        .members
        .iter()
        .filter(|m| m.role == "shard" && m.shard_role.as_deref() == Some("primary"))
        .map(|m| m.url.clone())
        .filter(|url| seen.insert(endpoint_of(url).to_string()))
        .collect();

    if primaries.is_empty() {
        return Err(
            "automatic rebalancing needs at least one member with shard_role 'primary'".to_string(),
        );
    }
    primaries.sort_by(|a, b| endpoint_of(a).cmp(endpoint_of(b)));
    let replicas = replica_placements(view, current, &primaries);

    let target = HashRing {
        vnodes: current.vnodes,
        shards: primaries
            .into_iter()
            .map(|node_url| {
                let replica_urls = replicas
                    .get(endpoint_of(&node_url))
                    .cloned()
                    .unwrap_or_default();
                RingShard {
                    node_url,
                    replica_urls,
                }
            })
            .collect(),
    };
    target.validate()?;
    Ok(Some(target))
}

fn target_signature(target: &HashRing) -> Vec<(String, Vec<String>)> {
    target
        .shards
        .iter()
        .map(|shard| {
            (
                endpoint_of(&shard.node_url).to_string(),
                shard
                    .replica_urls
                    .iter()
                    .map(|url| endpoint_of(url).to_string())
                    .collect(),
            )
        })
        .collect()
}

/// The lowest shard group coordinates; its elected replica may take over.
fn is_coordinator(state: &AppState, view: &ClusterMetadata) -> bool {
    let first = match view
        .ring
        .as_ref()
        .and_then(|r| r.shards.iter().min_by_key(|s| endpoint_of(&s.node_url)))
    {
        Some(first) => first,
        None => return false,
    };
    let own = state.own_url();
    same_endpoint(&own, &first.node_url)
        || first.replica_urls.iter().any(|r| same_endpoint(r, &own))
}

pub fn rebalance_task(state: AppState, cfg: RebalanceConfig) {
    tokio::spawn(async move {
        info!(target: "rebalance", interval_secs = cfg.interval_secs,
            stabilization_secs = cfg.stabilization_secs, "Automatic rebalancer active");
        let mut observed: Option<(Vec<(String, Vec<String>)>, Instant)> = None;

        loop {
            tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;
            if !state.is_leader() {
                continue;
            }

            let view = state.cluster_view();
            if view.migration.is_some() {
                // Idempotent recovery restarts source copying and coordinator polling.
                state.react_to_migration();
                if is_coordinator(&state, &view) {
                    resume_migration_coordination(&state);
                }
                continue;
            }
            let target = match desired_ring(&view) {
                Ok(Some(target)) => target,
                Ok(None) => continue,
                Err(why) => {
                    warn!(target: "rebalance", reason = %why, "Cannot derive a target ring");
                    continue;
                }
            };
            if view.ring.as_ref() == Some(&target) {
                observed = None;
                continue;
            }

            let signature = target_signature(&target);
            match &observed {
                Some((previous, since))
                    if previous == &signature
                        && since.elapsed() >= Duration::from_secs(cfg.stabilization_secs) => {}
                Some((previous, _)) if previous == &signature => continue,
                _ => {
                    observed = Some((signature, Instant::now()));
                    continue;
                }
            }

            if !is_coordinator(&state, &view) {
                continue;
            }
            match begin_migration(&state, target).await {
                Ok(MigrationLaunch::Started { id, movement, .. }) => {
                    info!(target: "rebalance", migration = %id,
                        moved_fraction = movement.moved_fraction,
                        "Membership changed; automatic handover started");
                    observed = None;
                }
                Ok(MigrationLaunch::Applied { version }) => {
                    info!(target: "rebalance", version,
                        "Reconciled ring metadata without moving keyspace");
                    observed = None;
                }
                Err(response) => warn!(target: "rebalance", status = %response.status(),
                    "Could not start automatic handover; will retry"),
            }
        }
    });
}

pub async fn rebalance_status_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    let view = state.cluster_view();
    let desired = match desired_ring(&view) {
        Ok(ring) => ring,
        Err(why) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "enabled": state.config.rebalance.enabled,
                    "error": why,
                })),
            )
                .into_response()
        }
    };
    let movement = match (&view.ring, &desired) {
        (Some(current), Some(target)) => Some(keyspace_movement(&current.build(), &target.build())),
        _ => None,
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "enabled": state.config.rebalance.enabled,
            "interval_secs": state.config.rebalance.interval_secs,
            "stabilization_secs": state.config.rebalance.stabilization_secs,
            "coordinator": is_coordinator(&state, &view),
            "in_progress": view.migration.is_some(),
            "balanced": desired.as_ref() == view.ring.as_ref(),
            "current_shards": view.ring.as_ref().map(|r| r.shards.iter()
                .map(|s| &s.node_url).collect::<Vec<_>>()),
            "desired_shards": desired.as_ref().map(|r| r.shards.iter()
                .map(|s| &s.node_url).collect::<Vec<_>>()),
            "current_replica_placements": view.ring.as_ref().map(|r| r.shards.iter()
                .map(|s| serde_json::json!({
                    "primary": s.node_url,
                    "replicas": s.replica_urls,
                })).collect::<Vec<_>>()),
            "desired_replica_placements": desired.as_ref().map(|r| r.shards.iter()
                .map(|s| serde_json::json!({
                    "primary": s.node_url,
                    "replicas": s.replica_urls,
                })).collect::<Vec<_>>()),
            "moved_fraction": movement.as_ref().map(|m| m.moved_fraction),
            "transfers": movement.as_ref().map(|m| &m.transfers),
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::metadata::{Adoption, ClusterMetadata, Member};
    use crate::ring::hash_key;
    use crate::test_support::{
        next_test_port, put_doc_at, read_doc_http, temp_root, wait_for, TestNode,
    };

    fn view() -> ClusterMetadata {
        ClusterMetadata {
            version: 4,
            updated_by: "n1".into(),
            seeded: false,
            members: vec![
                Member {
                    url: "http://c".into(),
                    node_id: None,
                    role: "shard".into(),
                    shard_role: Some("primary".into()),
                    voting: false,
                    follows: None,
                },
                Member {
                    url: "http://a".into(),
                    node_id: None,
                    role: "shard".into(),
                    shard_role: Some("primary".into()),
                    voting: true,
                    follows: None,
                },
                Member {
                    url: "http://a2".into(),
                    node_id: None,
                    role: "shard".into(),
                    shard_role: Some("replica".into()),
                    voting: true,
                    follows: None,
                },
                Member {
                    url: "http://router".into(),
                    node_id: None,
                    role: "router".into(),
                    shard_role: None,
                    voting: false,
                    follows: None,
                },
            ],
            shards: Vec::new(),
            ring: Some(HashRing {
                vnodes: 64,
                shards: vec![RingShard {
                    node_url: "http://a".into(),
                    replica_urls: vec!["http://a2".into()],
                }],
            }),
            migration: None,
        }
    }

    #[test]
    fn membership_produces_a_stable_target_and_preserves_existing_replicas() {
        let target = desired_ring(&view()).unwrap().unwrap();
        assert_eq!(target.vnodes, 64);
        assert_eq!(
            target
                .shards
                .iter()
                .map(|s| s.node_url.as_str())
                .collect::<Vec<_>>(),
            vec!["http://a", "http://c"],
            "member order must not alter the derived ring"
        );
        assert_eq!(target.shards[0].replica_urls, vec!["http://a2"]);
        assert!(
            target.shards[1].replica_urls.is_empty(),
            "commit 40 must not invent the placement policy planned for commit 41"
        );
    }

    #[test]
    fn follower_affinity_moves_live_replicas_and_drops_departed_ones() {
        let mut view = view();
        view.ring.as_mut().unwrap().shards[0].replica_urls = vec![
            "http://a2".into(),
            "http://gone".into(),
        ];
        view.members
            .iter_mut()
            .find(|member| same_endpoint(&member.url, "http://a2"))
            .unwrap()
            .follows = Some("http://c".into());
        view.members.push(Member {
            url: "http://a3".into(),
            node_id: None,
            role: "shard".into(),
            shard_role: Some("replica".into()),
            voting: false,
            follows: Some("http://a".into()),
        });

        let target = desired_ring(&view).unwrap().unwrap();
        assert_eq!(target.shards[0].node_url, "http://a");
        assert_eq!(target.shards[0].replica_urls, vec!["http://a3"]);
        assert_eq!(target.shards[1].node_url, "http://c");
        assert_eq!(target.shards[1].replica_urls, vec!["http://a2"]);
    }

    #[test]
    fn no_ring_is_a_noop_and_no_primary_is_refused() {
        let mut v = view();
        v.ring = None;
        assert!(desired_ring(&v).unwrap().is_none());

        v.ring = view().ring;
        v.members
            .retain(|m| m.shard_role.as_deref() != Some("primary"));
        assert!(
            desired_ring(&v).unwrap_err().contains("at least one"),
            "an incomplete membership list must not automatically empty the ring"
        );
    }

    #[test]
    fn config_is_opt_in_and_rejects_a_spinning_scheduler() {
        let default = RebalanceConfig::default();
        assert!(!default.enabled);
        assert!(default.validate().is_ok());

        let bad = RebalanceConfig {
            enabled: true,
            interval_secs: 0,
            stabilization_secs: 0,
        };
        assert!(bad.validate().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stable_primary_join_starts_and_finishes_a_handover_without_an_operator_call() {
        let root = temp_root();
        let mut a = TestNode::new("a", next_test_port(), &root, "primary");
        let mut b = TestNode::new("b", next_test_port(), &root, "primary");
        let mut c = TestNode::new("c", next_test_port(), &root, "primary");
        a.start();
        b.start();
        c.start();

        let current = ClusterMetadata {
            version: 2,
            updated_by: "a".into(),
            seeded: false,
            members: vec![
                Member {
                    url: a.url(),
                    node_id: Some("a".into()),
                    role: "shard".into(),
                    shard_role: Some("primary".into()),
                    voting: true,
                    follows: None,
                },
                Member {
                    url: b.url(),
                    node_id: Some("b".into()),
                    role: "shard".into(),
                    shard_role: Some("primary".into()),
                    voting: true,
                    follows: None,
                },
            ],
            shards: Vec::new(),
            ring: Some(HashRing {
                vnodes: 32,
                shards: vec![
                    RingShard {
                        node_url: a.url(),
                        replica_urls: Vec::new(),
                    },
                    RingShard {
                        node_url: b.url(),
                        replica_urls: Vec::new(),
                    },
                ],
            }),
            migration: None,
        };
        assert!(matches!(
            a.state.as_ref().unwrap().adopt_cluster(current.clone()),
            Adoption::Adopted { .. }
        ));
        assert!(matches!(
            b.state.as_ref().unwrap().adopt_cluster(current),
            Adoption::Adopted { .. }
        ));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let initial_ring = a
            .state
            .as_ref()
            .unwrap()
            .cluster_view()
            .ring
            .unwrap()
            .build();
        for i in 0..40 {
            let key = format!("k{}", i);
            let owner = initial_ring
                .owner(hash_key("t", &key))
                .unwrap()
                .node_url
                .clone();
            assert_eq!(
                put_doc_at(&client, &owner, "t", &key, i, "").await,
                StatusCode::CREATED
            );
        }

        rebalance_task(
            a.state.as_ref().unwrap().clone(),
            RebalanceConfig {
                enabled: true,
                interval_secs: 1,
                stabilization_secs: 0,
            },
        );

        let joined = client
            .post(format!("{}/cluster/members", b.url()))
            .json(&serde_json::json!({
                "url": c.url(), "node_id": "c", "role": "shard", "shard_role": "primary",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(joined.status(), StatusCode::OK);

        assert!(
            wait_for(Duration::from_secs(20), || {
                [&a, &b, &c].iter().all(|node| {
                    let view = node.state.as_ref().unwrap().cluster_view();
                    view.migration.is_none()
                        && view.ring.as_ref().is_some_and(|r| r.shards.len() == 3)
                })
            })
            .await,
            "membership never converged into a three-shard ring"
        );

        let final_ring = a.state.as_ref().unwrap().cluster_view().ring.unwrap();
        assert!(
            final_ring
                .shards
                .iter()
                .any(|s| same_endpoint(&s.node_url, &c.url())),
            "the joined primary was not assigned any tokens"
        );
        let built = final_ring.build();
        for i in 0..40 {
            let key = format!("k{}", i);
            let owner = built.owner(hash_key("t", &key)).unwrap();
            assert_eq!(
                read_doc_http(&client, &owner.node_url, &key).await,
                Some(i),
                "{} was lost while scaling out",
                key
            );
        }

        let left = client
            .delete(format!("{}/cluster/members?url={}", b.url(), c.url()))
            .send()
            .await
            .unwrap();
        assert_eq!(left.status(), StatusCode::OK);
        assert!(
            wait_for(Duration::from_secs(20), || {
                [&a, &b, &c].iter().all(|node| {
                    let view = node.state.as_ref().unwrap().cluster_view();
                    view.migration.is_none()
                        && view.ring.as_ref().is_some_and(|r| {
                            r.shards.len() == 2
                                && !r
                                    .shards
                                    .iter()
                                    .any(|s| same_endpoint(&s.node_url, &c.url()))
                        })
                })
            })
            .await,
            "removing the primary did not automatically hand its tokens back"
        );
        let built = a
            .state
            .as_ref()
            .unwrap()
            .cluster_view()
            .ring
            .unwrap()
            .build();
        for i in 0..40 {
            let key = format!("k{}", i);
            let owner = built.owner(hash_key("t", &key)).unwrap();
            assert_eq!(
                read_doc_http(&client, &owner.node_url, &key).await,
                Some(i),
                "{} was lost while scaling in",
                key
            );
        }

        a.kill();
        b.kill();
        c.kill();
        let _ = std::fs::remove_dir_all(&root);
    }
}
