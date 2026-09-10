//! Starting, watching, aborting and completing a handover. The coordinator moves no data -- each shard
//! pushes its own keys -- and if it dies the plan is in the view, so another call can finish it.

use crate::api::members::{publish, writable};
use crate::api::ring::RingRequest;
use crate::cluster::metadata::{Migration, MigrationPhase};
use crate::model::err_json;
use crate::ring::{keyspace_movement, HashRing, DEFAULT_VNODES};
use crate::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

/// How often the coordinator asks every source whether it is done.
const POLL_INTERVAL_MS: u64 = 300;

pub(crate) enum MigrationLaunch {
    Applied { version: u64 },
    Started { id: String, version: u64, movement: crate::ring::Movement },
}

pub async fn start_migration_handler(
    State(state): State<AppState>,
    Json(req): Json<RingRequest>,
) -> impl axum::response::IntoResponse {
    if let Err(resp) = writable(&state) {
        return resp;
    }

    let current = state.cluster_view();
    let vnodes = req.vnodes
        .or_else(|| current.ring.as_ref().map(|r| r.vnodes))
        .unwrap_or(DEFAULT_VNODES);
    let target = HashRing { vnodes, shards: req.shards };

    match begin_migration(&state, target).await {
        Ok(MigrationLaunch::Applied { version }) => (StatusCode::OK, Json(serde_json::json!({
            "status": "applied", "version": version, "moved_fraction": 0.0,
            "note": "the target ring owns the same keys, so no data had to move",
        }))).into_response(),
        Ok(MigrationLaunch::Started { id, version, movement }) =>
            (StatusCode::ACCEPTED, Json(serde_json::json!({
                "status": "migrating",
                "migration_id": id,
                "version": version,
                "moved_fraction": movement.moved_fraction,
                "transfers": movement.transfers,
                "note": "bulk copying in the background; writes pause only during finalization; \
                         watch GET /cluster/migrate",
            }))).into_response(),
        Err(resp) => resp,
    }
}

/// Manual and automatic rebalancing share this copy-before-flip safety boundary.
pub(crate) async fn begin_migration(
    state: &AppState,
    target: HashRing,
) -> Result<MigrationLaunch, axum::response::Response> {
    let current = state.cluster_view();
    if current.migration.is_some() {
        return Err(err_json(StatusCode::CONFLICT,
            "a handover is already in progress; wait for it or DELETE /cluster/migrate".to_string()));
    }
    if let Err(why) = target.validate() {
        return Err(err_json(StatusCode::UNPROCESSABLE_ENTITY, why));
    }

    // Nothing to move means nothing to coordinate: publish the ring and be done.
    let before = match state.built_ring() {
        Some(before) => before,
        None => return Err(err_json(StatusCode::CONFLICT,
            "this node holds no ring to migrate from; publish one with POST /cluster/ring first"
                .to_string())),
    };
    // Same reasoning as set_ring_handler: bounded, but not cheap enough for a request task.
    let (target, movement) = match tokio::task::spawn_blocking(move || {
        let movement = keyspace_movement(&before, &target.build());
        (target, movement)
    }).await {
        Ok(pair) => pair,
        Err(e) => return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not evaluate the target ring: {}", e))),
    };
    if movement.moved_fraction == 0.0 {
        let next = current.with_ring(&state.config.node_id, target);
        broadcast(state, &next, next.ring.as_ref());
        return match publish(state, next).await {
            Ok(version) => Ok(MigrationLaunch::Applied { version }),
            Err(resp) => Err(resp),
        };
    }

    let id = Uuid::new_v4().to_string();
    let migration = Migration {
        id: id.clone(),
        target: target.clone(),
        started_by: state.config.node_id.clone(),
        phase: MigrationPhase::Copy,
    };
    let next = current.with_migration(&state.config.node_id, Some(migration));

    // Handed out before this node adopts it: adopting starts our own push at once, and a destination
    // that has not seen the plan refuses the batch, costing a backoff on every handover.
    broadcast(state, &next, Some(&target));

    let version = match publish(state, next).await {
        Ok(v) => v,
        Err(resp) => return Err(resp),
    };

    info!(target: "migration", id = %id, moved = movement.moved_fraction,
        "Background handover started");
    // Captured now: a shard being removed disappears from the owner list the moment the ring
    // lands, and cleanup would then never reach the one node most likely to be holding copies.
    let sources: Vec<String> = state.cluster_view().shard_owners()
        .into_iter().map(|(url, _)| url).collect();
    coordinate(state.clone(), id.clone(), target, sources);

    Ok(MigrationLaunch::Started { id, version, movement })
}

/// Hands the current view to every node the plan touches. `extra` names a ring whose shards are
/// not owners yet, which is exactly the case for a shard being added.
fn tell_everyone(state: &AppState, extra: Option<&HashRing>) {
    let view = state.cluster_view();
    broadcast(state, &view, extra);
}

fn broadcast(state: &AppState, view: &crate::cluster::metadata::ClusterMetadata, extra: Option<&HashRing>) {
    let own = state.own_url();
    let mut seen = std::collections::HashSet::new();

    let targets: Vec<String> = view.members.iter().map(|member| member.url.clone())
        .chain(view.shard_owners().into_iter().flat_map(|(url, replicas)| {
            std::iter::once(url).chain(replicas)
        }))
        .chain(extra.into_iter().flat_map(|ring| ring.shards.iter().flat_map(|shard| {
            std::iter::once(shard.node_url.clone()).chain(shard.replica_urls.clone())
        })))
        .filter(|url| !crate::util::same_endpoint(url, &own))
        .filter(|url| seen.insert(crate::util::node_key(url)))
        .collect();

    for url in targets {
        crate::api::members::nudge(state, &url, view);
    }
}

/// Waits for every source to finish, then publishes the target ring. Runs detached because a
/// handover outlives any one request, and the plan in the view is what makes that safe to do.
struct CoordinationGuard {
    state: AppState,
    id: String,
}

impl Drop for CoordinationGuard {
    fn drop(&mut self) {
        let mut runs = self.state.migrations.lock().unwrap();
        if runs.coordinating.as_deref() == Some(&self.id) {
            runs.coordinating = None;
        }
    }
}

/// Logs only when the outstanding set changes: the poll interval is sub-second, so a handover
/// waiting on a node that is down would otherwise fill the log with one line every poll.
fn note_waiting(state: &AppState, id: &str, pending: Vec<String>, last: &mut Option<Vec<String>>) {
    if last.as_ref() == Some(&pending) {
        return;
    }
    if pending.is_empty() {
        info!(target: "migration", id = %id, "Every source has finished this phase");
    } else {
        warn!(target: "migration", id = %id, waiting_on = ?pending,
            "Handover is open until these sources answer, or until DELETE /cluster/migrate");
    }
    state.migrations.lock().unwrap().waiting_on = pending.clone();
    *last = Some(pending);
}

fn coordinate(state: AppState, id: String, target: HashRing, sources: Vec<String>) {
    {
        let mut runs = state.migrations.lock().unwrap();
        if runs.coordinating.as_deref() == Some(&id) {
            return;
        }
        runs.coordinating = Some(id.clone());
    }
    tokio::spawn(async move {
        let _guard = CoordinationGuard { state: state.clone(), id: id.clone() };
        let mut last_report: Option<Vec<String>> = None;
        loop {
            tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

            let migration = match state.migration() {
                Some(m) if m.id == id => m,
                // Abandoned, or replaced by something newer. Either way this is not ours to finish.
                _ => {
                    info!(target: "migration", id = %id, "Handover no longer in the view; stopping");
                    return;
                },
            };

            // Keep waiting rather than abandon: a source that is briefly unreachable is the normal
            // case, and completing without it would strand its keys.
            match sources_pending(&state, &id, migration.phase, &sources).await {
                Ok(pending) if pending.is_empty() => {
                    note_waiting(&state, &id, Vec::new(), &mut last_report);
                },
                Ok(pending) => {
                    note_waiting(&state, &id, pending, &mut last_report);
                    continue;
                },
                Err(why) => {
                    note_waiting(&state, &id, vec![why], &mut last_report);
                    continue;
                },
            }

            if migration.phase == MigrationPhase::Copy {
                let mut finalizing = migration;
                finalizing.phase = MigrationPhase::Finalizing;
                let next = state.cluster_view()
                    .with_migration(&state.config.node_id, Some(finalizing));
                match publish(&state, next).await {
                    Ok(version) => {
                        info!(target: "migration", id = %id, version,
                            "Bulk copy complete; finalizing handover");
                        tell_everyone(&state, Some(&target));
                    },
                    Err(_) => warn!(target: "migration", id = %id,
                        "Could not publish finalization phase; the bulk copy remains active"),
                }
                continue;
            }

            let next = state.cluster_view().with_ring(&state.config.node_id, target.clone());
            match publish(&state, next).await {
                Ok(version) => {
                    info!(target: "migration", id = %id, version, "Ownership handed over");
                    tell_everyone(&state, None);
                    cleanup(&state, &id, &sources).await;
                },
                Err(_) => warn!(target: "migration", id = %id,
                    "Could not publish the completed ring; the handover stays in the view"),
            }
            return;
        }
    });
}

/// Restarts coordinator polling for a durable in-flight migration.
pub(crate) fn resume_migration_coordination(state: &AppState) {
    let plan = match state.migration() {
        Some(plan) => plan,
        None => return,
    };
    let sources = state.cluster_view().shard_owners()
        .into_iter().map(|(url, _)| url).collect();
    coordinate(state.clone(), plan.id, plan.target, sources);
}

/// Who answers as primary for a source group now: the ring names whoever led it when the plan was made.
/// Cached per group, so a healthy handover costs one probe round per override TTL, not one per poll.
async fn source_leader(state: &AppState, source: &str) -> Option<String> {
    if let Some(cached) = state.cached_primary(source) {
        return Some(cached);
    }

    let mut group = vec![source.to_string()];
    group.extend(state.shard_owners().into_iter()
        .find(|(url, _)| crate::util::same_endpoint(url, source))
        .map(|(_, replicas)| replicas)
        .unwrap_or_default());

    let probes = futures::future::join_all(group.into_iter().map(|url| {
        let client = state.client.clone();
        async move {
            let probe = crate::cluster::probe::probe_node(&client, &url).await;
            (url, probe)
        }
    })).await;

    let winner = crate::cluster::probe::select_primary(&probes)?;
    if !crate::util::same_endpoint(&winner, source) {
        info!(target: "migration", "Source group {} is now led by {}", source, winner);
    }
    state.set_primary_override(source, &winner);
    Some(winner)
}

/// Which sources are not finished yet, this node included. A source that cannot be asked is not
/// finished, so an unreachable group keeps the handover open rather than completing without it.
async fn sources_pending(
    state: &AppState,
    id: &str,
    phase: MigrationPhase,
    sources: &[String],
) -> Result<Vec<String>, String> {
    let own = state.own_url();
    let mut pending = Vec::new();

    for owner in sources {
        if crate::util::same_endpoint(owner, &own) {
            let local = crate::cluster::migration::progress(state);
            if !local.is_some_and(|p| p.id == id && p.phase == phase && p.done) {
                pending.push(own.clone());
            }
            continue;
        }

        let leader = source_leader(state, owner).await
            .ok_or_else(|| format!("{}: no reachable primary in that shard group", owner))?;

        let url = format!("{}/internal/migration-status", leader);
        let body = match state.client.get(&url).send().await {
            Ok(r) => r.json::<serde_json::Value>().await
                .map_err(|e| format!("{}: {}", leader, e))?,
            Err(e) => {
                // The cached leader may itself have been deposed; the next poll re-probes.
                state.clear_primary_override(owner);
                return Err(format!("{}: {}", leader, e));
            },
        };

        let progress = body.get("progress");
        let matches = progress.and_then(|p| p.get("id")).and_then(|v| v.as_str()) == Some(id)
            && progress.and_then(|p| p.get("phase")).and_then(|v| v.as_str())
                == Some(match phase {
                    MigrationPhase::Copy => "copy",
                    MigrationPhase::Finalizing => "finalizing",
                })
            && progress.and_then(|p| p.get("done")).and_then(|v| v.as_bool()) == Some(true);
        if !matches {
            pending.push(owner.clone());
        }
    }
    Ok(pending)
}

/// Asks everyone who might hold a handed-over key to drop it, the previous owners included -- a shard
/// dropped from the ring holds the most. Its leftovers would shadow live values if it came back.
async fn cleanup(state: &AppState, id: &str, sources: &[String]) {
    let view = state.cluster_view();
    let body = serde_json::json!({ "migration_id": id });
    let mut seen = std::collections::HashSet::new();

    let nodes: Vec<String> = sources.iter().cloned()
        .chain(view.shard_owners().into_iter().map(|(url, _)| url))
        .filter(|url| seen.insert(crate::util::node_key(url)))
        .collect();

    for node in nodes {
        // The ring names a group by its configured primary, which need not be the node leading it now,
        // and only the leader can commit the deletions (H16). Resolved as `sources_pending` does it.
        let node = match source_leader(state, &node).await {
            Some(leader) => leader,
            None => {
                warn!(target: "migration", node = %node,
                    "No reachable primary in that shard group; handed-over keys are still on disk");
                continue;
            },
        };

        // The final view first, and awaited: a node still holding the plan refuses to clean up, so
        // relying on ordinary propagation would make cleanup a race it usually loses.
        if !crate::util::same_endpoint(&node, &state.own_url()) {
            let handover = format!("{}/internal/cluster", node);
            if let Err(e) = state.client.post(&handover).json(&view).send().await {
                warn!(target: "migration", node = %node, error = %e,
                    "Could not hand over the completed view; skipping cleanup there");
                continue;
            }
        }

        let url = format!("{}/internal/migrate-cleanup", node);
        match state.client.post(&url).json(&body).send().await {
            Ok(r) if r.status().is_success() => {},
            Ok(r) => warn!(target: "migration", node = %node, status = %r.status(),
                "Cleanup did not finish there; handed-over keys are still on disk"),
            Err(e) => warn!(target: "migration", node = %node, error = %e,
                "Could not clean up handed-over keys; they are unreachable but still on disk"),
        }
    }
}

pub async fn migration_status(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    let plan = state.migration();
    (StatusCode::OK, Json(serde_json::json!({
        "in_progress": plan.is_some(),
        "waiting_on": state.migrations.lock().unwrap().waiting_on.clone(),
        "migration": plan.map(|m| serde_json::json!({
            "id": m.id,
            "started_by": m.started_by,
            "phase": m.phase,
            "target": m.target.shards.iter().map(|s| &s.node_url).collect::<Vec<_>>(),
        })),
        "local_progress": crate::cluster::migration::progress(&state),
    }))).into_response()
}

/// Abandons a handover. Safe at any point before the flip: ownership has not moved, so the sources
/// are still the owners and the copies already pushed are simply unreferenced.
pub async fn abort_migration_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    if let Err(resp) = writable(&state) {
        return resp;
    }
    let current = state.cluster_view();
    let id = match &current.migration {
        Some(m) => m.id.clone(),
        None => return err_json(StatusCode::NOT_FOUND, "no handover is in progress".to_string()),
    };

    let next = current.with_migration(&state.config.node_id, None);
    match publish(&state, next).await {
        Ok(version) => {
            tell_everyone(&state, None);
            info!(target: "migration", id = %id, "Handover abandoned; ownership never moved");
            (StatusCode::OK, Json(serde_json::json!({
                "status": "aborted", "migration_id": id, "version": version,
                "note": "ownership never moved, so nothing was lost; copies already pushed to the \
                         destination are unreferenced and will be overwritten by a later handover",
            }))).into_response()
        },
        Err(resp) => resp,
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::metadata::{Adoption, Migration, MigrationPhase};
    use crate::cluster::migration::{
        CompletedHandover, MigrateBatch, MigrateDoc, MigrateReset, MigrationMeta,
        MigrationProgress, MigrationRuns,
    };
    use crate::ring::{hash_key, HashRing, RingShard};
    use crate::storage::frame::HandoverRecord;
    use std::collections::HashSet;
    use crate::test_support::{next_test_port, temp_root, wait_for, wait_for_doc, TestNode};
    use axum::http::StatusCode;
    use std::time::Duration;

    fn shards(urls: &[&str]) -> Vec<RingShard> {
        urls.iter().map(|u| RingShard { node_url: u.to_string(), replica_urls: vec![] }).collect()
    }

    fn ring_body(urls: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "shards": urls.iter().map(|u| serde_json::json!({"node_url": u})).collect::<Vec<_>>(),
        })
    }

    fn owner_of(urls: &[&str], key: &str) -> String {
        HashRing { vnodes: 128, shards: shards(urls) }.build()
            .owner(hash_key("t", key)).unwrap().node_url.clone()
    }

    struct Cluster {
        a: TestNode,
        b: TestNode,
        c: TestNode,
        client: reqwest::Client,
    }

    impl Cluster {
        /// Three independent single-node shards, which is what a sharded deployment is: each is
        /// the leader of its own group, and the ring is what ties them together.
        async fn start(root: &std::path::Path) -> Self {
            Self::start_with_movement(root, 64, 5).await
        }

        async fn start_with_movement(
            root: &std::path::Path,
            batch_size: usize,
            batch_delay_ms: u64,
        ) -> Self {
            let mut nodes: Vec<TestNode> = ["a", "b", "c"].iter()
                .map(|id| {
                    let mut n = TestNode::new(id, next_test_port(), root, "primary");
                    n.data_movement_batch_size = batch_size;
                    n.data_movement_batch_delay_ms = batch_delay_ms;
                    n.start();
                    n
                })
                .collect();
            let c = nodes.pop().unwrap();
            let b = nodes.pop().unwrap();
            let a = nodes.pop().unwrap();
            let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
            Self { a, b, c, client }
        }

        async fn post(&self, url: String, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
            let r = self.client.post(&url).json(&body).send().await.unwrap();
            (r.status(), r.json().await.unwrap_or_default())
        }

        async fn put(&self, node: &str, key: &str, v: i64) -> StatusCode {
            self.client.put(&format!("{}/collections/t/docs/{}", node, key))
                .json(&serde_json::json!({"value": {"v": v}}))
                .send().await.unwrap().status()
        }

        /// Which shard physically holds the key, asked of each directly.
        async fn holder(&self, key: &str) -> Option<String> {
            for node in [&self.a, &self.b, &self.c] {
                let r = self.client.get(&format!("{}/collections/t/docs/{}", node.url(), key))
                    .send().await.unwrap();
                if r.status() == StatusCode::OK {
                    return Some(node.url());
                }
            }
            None
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_handover_moves_the_data_with_the_ownership() {
        let root = temp_root();
        let cl = Cluster::start(&root).await;
        let (a, b, c) = (cl.a.url(), cl.b.url(), cl.c.url());
        let two: Vec<&str> = vec![&a, &b];
        let three: Vec<&str> = vec![&a, &b, &c];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&two)).await.0, StatusCode::OK);
        // Everyone must share the ring, or a shard would not know which keys are its own.
        for node in [&b, &c] {
            cl.post(format!("{}/internal/cluster", node),
                serde_json::to_value(cl.a.state.as_ref().unwrap().cluster_view()).unwrap()).await;
        }

        // Write through whichever shard owns each key, so the starting state is correct.
        let keys: Vec<String> = (0..60).map(|i| format!("k{}", i)).collect();
        for key in &keys {
            let owner = owner_of(&two, key);
            assert_eq!(cl.put(&owner, key, 1).await, StatusCode::CREATED, "seeding {}", key);
        }

        // Exactly the keys that shard c will take over once the ring grows.
        let moving: Vec<&String> = keys.iter()
            .filter(|k| owner_of(&three, k) == c && owner_of(&two, k) != c)
            .collect();
        assert!(!moving.is_empty(), "adding a shard must take some keys");

        let (code, body) = cl.post(format!("{}/cluster/migrate", a), ring_body(&three)).await;
        assert_eq!(code, StatusCode::ACCEPTED, "handover was refused: {}", body);
        let id = body["migration_id"].as_str().unwrap().to_string();
        assert!(body["moved_fraction"].as_f64().unwrap() > 0.0);

        let landed = wait_for(Duration::from_secs(30), || {
            cl.c.state.as_ref().unwrap().cluster_view().ring
                .is_some_and(|r| r.shards.len() == 3)
                && cl.c.state.as_ref().unwrap().migration().is_none()
        }).await;
        assert!(landed, "the handover never completed; id={}", id);

        // The whole point: every moved key is readable at its new owner.
        for key in &moving {
            let holder = cl.holder(key).await;
            assert_eq!(holder.as_deref(), Some(c.as_str()),
                "key {} was reassigned to {} but is held by {:?}", key, c, holder);
        }
        // And nothing that was not moving went anywhere.
        for key in &keys {
            if moving.contains(&key) {
                continue;
            }
            assert_eq!(cl.holder(key).await.as_deref(), Some(owner_of(&two, key).as_str()),
                "key {} moved without being part of the plan", key);
        }

        // Ownership moved, so the old owner must refuse it and say where it went.
        let stale = cl.client
            .put(&format!("{}/collections/t/docs/{}", owner_of(&two, moving[0]), moving[0]))
            .json(&serde_json::json!({"value": {"v": 9}}))
            .send().await.unwrap();
        assert_eq!(stale.status(), StatusCode::CONFLICT,
            "the old owner still accepts writes for a key it handed over");
        assert_eq!(stale.json::<serde_json::Value>().await.unwrap()["owner"].as_str(), Some(c.as_str()));

        // Writes to the new owner work.
        assert_eq!(cl.put(&c, moving[0], 7).await, StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn finalization_carries_updates_and_deletes_written_during_bulk_copy() {
        let root = temp_root();
        let cl = Cluster::start_with_movement(&root, 1, 25).await;
        let (a, b, c) = (cl.a.url(), cl.b.url(), cl.c.url());
        let two: Vec<&str> = vec![&a, &b];
        let three: Vec<&str> = vec![&a, &b, &c];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&two)).await.0,
            StatusCode::OK);
        for node in [&b, &c] {
            cl.post(format!("{}/internal/cluster", node),
                serde_json::to_value(cl.a.state.as_ref().unwrap().cluster_view()).unwrap()).await;
        }

        let keys: Vec<String> = (0..120).map(|i| format!("k{:03}", i)).collect();
        for key in &keys {
            assert_eq!(cl.put(&owner_of(&two, key), key, 1).await, StatusCode::CREATED);
        }

        let mut from_a: Vec<String> = keys.iter()
            .filter(|key| owner_of(&two, key) == a && owner_of(&three, key) == c)
            .cloned().collect();
        let mut from_b: Vec<String> = keys.iter()
            .filter(|key| owner_of(&two, key) == b && owner_of(&three, key) == c)
            .cloned().collect();
        from_a.sort();
        from_b.sort();
        let (source, moving, source_state) = if from_a.len() >= from_b.len() {
            (&a, from_a, cl.a.state.as_ref().unwrap())
        } else {
            (&b, from_b, cl.b.state.as_ref().unwrap())
        };
        assert!(moving.len() >= 2, "the test ring must move at least two keys from one source");

        assert_eq!(cl.post(format!("{}/cluster/migrate", a), ring_body(&three)).await.0,
            StatusCode::ACCEPTED);
        assert!(wait_for(Duration::from_secs(10), || {
            crate::cluster::migration::progress(source_state).is_some_and(|progress| {
                progress.phase == MigrationPhase::Copy && progress.pushed > 0 && !progress.done
            })
        }).await, "the bulk copy completed before the test could write concurrently");

        assert_eq!(cl.put(source, &moving[1], 99).await, StatusCode::OK);
        let deleted = cl.client.delete(&format!("{}/collections/t/docs/{}", source, moving[0]))
            .send().await.unwrap();
        assert_eq!(deleted.status(), StatusCode::OK);

        assert!(wait_for(Duration::from_secs(30), || {
            cl.c.state.as_ref().unwrap().cluster_view().ring
                .is_some_and(|ring| ring.shards.len() == 3)
                && cl.c.state.as_ref().unwrap().migration().is_none()
        }).await, "the handover never completed");

        let updated = cl.client.get(&format!("{}/collections/t/docs/{}", c, moving[1]))
            .send().await.unwrap();
        assert_eq!(updated.status(), StatusCode::OK);
        assert_eq!(updated.json::<serde_json::Value>().await.unwrap()["v"], 99);

        let deleted = cl.client.get(&format!("{}/collections/t/docs/{}", c, moving[0]))
            .send().await.unwrap();
        assert_eq!(deleted.status(), StatusCode::NOT_FOUND,
            "a value deleted after the bulk copy must not reappear at cutover");
    }

    /// Finalization has to freeze the keys that are moving, not the node: the barrier drains writes that
    /// decided ownership under the previous view, which is an instant, not a whole keyspace scan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writes_the_handover_is_not_moving_keep_going_through_finalization() {
        let root = temp_root();
        let cl = Cluster::start_with_movement(&root, 1, 40).await;
        let (a, b, c) = (cl.a.url(), cl.b.url(), cl.c.url());
        let two: Vec<&str> = vec![&a, &b];
        let three: Vec<&str> = vec![&a, &b, &c];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&two)).await.0, StatusCode::OK);
        for node in [&b, &c] {
            cl.post(format!("{}/internal/cluster", node),
                serde_json::to_value(cl.a.state.as_ref().unwrap().cluster_view()).unwrap()).await;
        }

        let keys: Vec<String> = (0..300).map(|i| format!("k{:03}", i)).collect();
        for key in &keys {
            assert_eq!(cl.put(&owner_of(&two, key), key, 1).await, StatusCode::CREATED);
        }

        // The source with more to hand over, so its finalize pass is the long one.
        let moving_from = |shard: &str| keys.iter()
            .filter(|key| owner_of(&two, key) == shard && owner_of(&three, key) == c).count();
        let (source, source_state) = if moving_from(&a) >= moving_from(&b) {
            (&a, cl.a.state.as_ref().unwrap())
        } else {
            (&b, cl.b.state.as_ref().unwrap())
        };
        assert!(moving_from(source) >= 20, "the pass has to be long enough to write during");
        let kept = keys.iter()
            .find(|key| owner_of(&two, key) == *source && owner_of(&three, key) == *source)
            .expect("the source must keep at least one key to write to")
            .clone();

        assert_eq!(cl.post(format!("{}/cluster/migrate", a), ring_body(&three)).await.0,
            StatusCode::ACCEPTED);
        assert!(wait_for(Duration::from_secs(30), || {
            crate::cluster::migration::progress(source_state)
                .is_some_and(|p| p.phase == MigrationPhase::Finalizing)
        }).await, "finalization never started");

        // Counted rather than timed: a barrier held across the pass lets exactly one write through,
        // the one that was already blocked on it when the pass ended.
        let mut wrote = 0usize;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while crate::cluster::migration::progress(source_state)
            .is_some_and(|p| p.phase == MigrationPhase::Finalizing && !p.done)
        {
            if std::time::Instant::now() >= deadline {
                break;
            }
            if cl.client.put(format!("{}/collections/t/docs/{}", source, kept))
                .json(&serde_json::json!({"value": {"v": 2}}))
                .send().await.is_ok_and(|r| r.status() == StatusCode::OK)
            {
                wrote += 1;
            }
        }

        // ~180 in practice; a held barrier lets through only the couple already waiting on it
        assert!(wrote >= 25,
            "only {} write(s) landed during finalization; the write barrier is being held across \
             the whole pass, so every client on this node is blocked for its duration", wrote);

        assert!(wait_for(Duration::from_secs(60), || {
            cl.a.state.as_ref().unwrap().migration().is_none()
                && cl.a.state.as_ref().unwrap().cluster_view().ring
                    .is_some_and(|ring| ring.shards.len() == 3)
        }).await, "the handover never completed");

        assert_eq!(cl.holder(&kept).await.as_deref(), Some(source.as_str()),
            "a key the plan never moved must still be where it was");
        // Polled, not sampled once: the ring flip is what ends the handover, and the destination
        // applying the last copied batch can trail it. Sampling here is bugs.md L10.
        for key in keys.iter().filter(|key| owner_of(&three, key) == c) {
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while cl.holder(key).await.as_deref() != Some(c.as_str()) {
                assert!(std::time::Instant::now() < deadline,
                    "key {} was reassigned but never arrived", key);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_replayed_final_reset_cannot_erase_the_completed_copy() {
        let root = temp_root();
        let mut node = TestNode::new("c", next_test_port(), &root, "primary");
        node.start();
        let c = node.url();
        let source = "http://source-a";
        let other = "http://source-b";
        let current = HashRing { vnodes: 128, shards: shards(&[source, other]) };
        let target = HashRing { vnodes: 128, shards: shards(&[source, other, &c]) };
        let key = (0..10_000).map(|i| format!("k{}", i)).find(|key| {
            let hash = hash_key("t", key);
            current.build().owner(hash).unwrap().node_url == source
                && target.build().owner(hash).unwrap().node_url == c
        }).unwrap();

        let state = node.state.as_ref().unwrap();
        let mut view = state.cluster_view();
        view.version += 1;
        view.seeded = false;
        view.updated_by = "operator".into();
        view.ring = Some(current);
        view.migration = Some(Migration {
            id: "m1".into(), target, started_by: "operator".into(),
            phase: MigrationPhase::Finalizing,
        });
        assert!(matches!(state.adopt_cluster(view), Adoption::Adopted { .. }));

        let client = reqwest::Client::new();
        let drop_during_cutover = client.delete(format!("{}/collections/t", c))
            .send().await.unwrap();
        assert_eq!(drop_during_cutover.status(), StatusCode::SERVICE_UNAVAILABLE);

        let reset = MigrateReset {
            migration_id: "m1".into(), phase: MigrationPhase::Finalizing,
            source: source.into(),
        };
        assert_eq!(client.post(format!("{}/internal/migrate-reset", c)).json(&reset)
            .send().await.unwrap().status(), StatusCode::OK);

        let batch = MigrateBatch {
            migration_id: "m1".into(), phase: MigrationPhase::Finalizing,
            collection: "t".into(),
            docs: vec![MigrateDoc { key: key.clone(), value: serde_json::json!({"v": 7}) }],
        };
        assert_eq!(client.post(format!("{}/internal/migrate", c)).json(&batch)
            .send().await.unwrap().status(), StatusCode::OK);

        let replay = client.post(format!("{}/internal/migrate-reset", c)).json(&reset)
            .send().await.unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(replay.json::<serde_json::Value>().await.unwrap()["repeated"], true);
        assert_eq!(state.db.as_ref().unwrap().get_collection("t").unwrap().get(&key).unwrap(),
            Some(serde_json::json!({"v": 7})));

        node.kill();
    }

    /// A destination whose group is one node wide accepts a handover no quorum holds. The source
    /// deletes what it hands over, so that ack is the whole of C13.
    struct Handover {
        node: TestNode,
        key: String,
        source: &'static str,
    }

    impl Handover {
        /// A node standing as the destination of a finalizing plan, with `replicas` as its group.
        async fn stage(root: &std::path::Path, replicas: Vec<String>) -> Self {
            let mut node = TestNode::new("c", next_test_port(), root, "primary");
            node.replicas = replicas;
            node.start();
            let c = node.url();
            let source = "http://source-a";
            let current = HashRing { vnodes: 128, shards: shards(&[source, "http://source-b"]) };
            let target = HashRing {
                vnodes: 128, shards: shards(&[source, "http://source-b", &c]),
            };
            let key = (0..10_000).map(|i| format!("k{}", i)).find(|key| {
                let hash = hash_key("t", key);
                current.build().owner(hash).unwrap().node_url == source
                    && target.build().owner(hash).unwrap().node_url == c
            }).unwrap();

            let state = node.state.as_ref().unwrap();
            let mut view = state.cluster_view();
            view.version += 1;
            view.seeded = false;
            view.updated_by = "operator".into();
            view.ring = Some(current);
            view.migration = Some(Migration {
                id: "m1".into(), target, started_by: "operator".into(),
                phase: MigrationPhase::Finalizing,
            });
            assert!(matches!(state.adopt_cluster(view), Adoption::Adopted { .. }));

            Self { node, key, source }
        }

        fn batch(&self, v: i64) -> MigrateBatch {
            MigrateBatch {
                migration_id: "m1".into(), phase: MigrationPhase::Finalizing,
                collection: "t".into(),
                docs: vec![MigrateDoc { key: self.key.clone(), value: serde_json::json!({"v": v}) }],
            }
        }

        fn reset(&self) -> MigrateReset {
            MigrateReset {
                migration_id: "m1".into(), phase: MigrationPhase::Finalizing,
                source: self.source.into(),
            }
        }

        fn held(&self) -> Option<serde_json::Value> {
            self.node.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").unwrap().get(&self.key).unwrap()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handover_batch_is_refused_when_the_destination_cannot_reach_its_quorum() {
        let root = temp_root();
        // Bound once to prove it is free, then never listened on: the replica is simply absent.
        let absent = format!("http://127.0.0.1:{}", next_test_port());
        let mut h = Handover::stage(&root, vec![absent]).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();

        let response = client.post(format!("{}/internal/migrate", h.node.url()))
            .json(&h.batch(7)).send().await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE,
            "a batch only the destination leader holds must be refused; the source deletes what it              hands over, so acknowledging it loses the keys to one destination failover");
        assert_eq!(h.held(), None, "a refused batch must not be published either");

        h.node.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_final_reset_that_loses_its_quorum_neither_reports_success_nor_drops_the_copy() {
        let root = temp_root();
        let mut replica = TestNode::new("c-replica", next_test_port(), &root, "replica");
        let mut h = Handover::stage(&root, vec![replica.url()]).await;
        replica.primary_addr = Some(h.node.url());
        replica.start();
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();

        // The copy lands while the group is whole, so the reset below has something to remove.
        assert_eq!(client.post(format!("{}/internal/migrate", h.node.url()))
            .json(&h.batch(7)).send().await.unwrap().status(), StatusCode::OK);
        assert!(wait_for(Duration::from_secs(10), || h.held().is_some()).await,
            "the copy never became visible at the destination");

        replica.kill();

        let response = client.post(format!("{}/internal/migrate-reset", h.node.url()))
            .json(&h.reset()).send().await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE,
            "a reset whose tombstones reached one node must not be reported complete");
        assert_eq!(h.held(), Some(serde_json::json!({"v": 7})),
            "and the copy it could not remove must still be here, not half-deleted");
        assert!(!crate::cluster::migration::reset_completed(
            h.node.state.as_ref().unwrap(), "m1", h.source),
            "an unfinished reset must stay retryable");

        h.node.kill();
    }

    /// A group can lose leadership and win it back inside one phase. The push task stops when it
    /// does, so the run record it leaves behind must not read as "already running" (bugs.md L21).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_re_elected_source_leader_restarts_the_push_its_step_down_stopped() {
        let root = temp_root();
        let cl = Cluster::start_with_movement(&root, 1, 100).await;
        let (a, b, c) = (cl.a.url(), cl.b.url(), cl.c.url());
        let two: Vec<&str> = vec![&a, &b];
        let three: Vec<&str> = vec![&a, &b, &c];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&two)).await.0, StatusCode::OK);
        for node in [&b, &c] {
            cl.post(format!("{}/internal/cluster", node),
                serde_json::to_value(cl.a.state.as_ref().unwrap().cluster_view()).unwrap()).await;
        }

        let keys: Vec<String> = (0..120).map(|i| format!("k{:03}", i)).collect();
        for key in &keys {
            assert_eq!(cl.put(&owner_of(&two, key), key, 1).await, StatusCode::CREATED);
        }
        assert!(keys.iter().filter(|key| owner_of(&two, key) == a && owner_of(&three, key) == c)
            .count() >= 5, "the test ring must move a workable number of keys off a");

        assert_eq!(cl.post(format!("{}/cluster/migrate", a), ring_body(&three)).await.0,
            StatusCode::ACCEPTED);
        let state = cl.a.state.as_ref().unwrap().clone();
        assert!(wait_for(Duration::from_secs(20), || {
            crate::cluster::migration::progress(&state).is_some_and(|p| p.pushed > 0)
        }).await, "a never started handing over");

        // What a deposition does to the push task, without the churn that usually causes one.
        {
            let mut repl = state.replication.as_ref().unwrap().write().unwrap();
            crate::consensus::state::relinquish_leadership(&mut repl);
        }
        assert!(wait_for(Duration::from_secs(20), || {
            state.migrations.lock().unwrap().pushing.is_none()
        }).await, "the push task never noticed the step-down");
        assert!(crate::cluster::migration::progress(&state).is_some_and(|p| !p.done),
            "the step-down landed after the copy finished, so there is nothing left to resume");

        state.replication.as_ref().unwrap().write().unwrap().is_leader = true;
        crate::consensus::seed_leader_progress(&state);
        state.react_to_migration();

        assert!(wait_for(Duration::from_secs(30), || {
            state.migration().is_none()
                && state.cluster_view().ring.is_some_and(|ring| ring.shards.len() == 3)
        }).await, "the handover never resumed after a was re-elected: {:?}",
            crate::cluster::migration::progress(&state));

        drop(cl);
    }

    fn shard_json(node: &str, replicas: &[String]) -> serde_json::Value {
        serde_json::json!({"node_url": node, "replica_urls": replicas})
    }

    /// Why a survivor is not finishing a handover: what it believes about the plan, whether it is
    /// leading, and what its own push and coordinator records say (bugs.md L21).
    fn handover_diagnostic(node: &crate::test_support::TestNode) -> String {
        let state = node.state.as_ref().unwrap();
        let runs = state.migrations.lock().unwrap();
        format!(
            "{} leader={} term={} view_migration={:?} pushing={:?} progress={:?} coordinating={:?} waiting_on={:?}",
            node.node_id, node.is_leader(), node.term(),
            state.migration().map(|m| (m.id, m.phase)),
            runs.pushing,
            runs.current.as_ref().map(|p| (p.id.clone(), p.phase, p.pushed, p.total, p.done, p.error.clone())),
            runs.coordinating, runs.waiting_on,
        )
    }

    /// The ring names one node per shard, but a shard is a group: when its leader dies mid-handover that
    /// name belongs to a node that is not leading, and answers for the group it no longer speaks for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_handover_finishes_when_the_source_group_elects_a_new_leader() {
        let root = temp_root();
        // b1 takes the lowest port so its group stays the coordinating one after it dies.
        let ports: Vec<u16> = (0..4).map(|_| next_test_port()).collect();
        let urls: Vec<String> = ports.iter()
            .map(|port| format!("http://127.0.0.1:{}", port)).collect();
        let (b1, b2, b3, dest) = (urls[0].clone(), urls[1].clone(), urls[2].clone(), urls[3].clone());

        let mut group: Vec<TestNode> = ["b1", "b2", "b3"].iter().enumerate()
            .map(|(i, id)| {
                let role = if i == 0 { "primary" } else { "replica" };
                let mut node = TestNode::new(id, ports[i], &root, role);
                node.peers = urls[..3].iter().filter(|u| *u != &urls[i]).cloned().collect();
                // Slow enough that the kill below lands while the handover is still running.
                node.data_movement_batch_size = 1;
                node.data_movement_batch_delay_ms = 25;
                if i == 0 {
                    node.replicas = vec![b2.clone(), b3.clone()];
                } else {
                    node.primary_addr = Some(b1.clone());
                }
                node.start();
                node
            })
            .collect();
        let mut destination = TestNode::new("dest", ports[3], &root, "primary");
        destination.start();
        crate::test_support::await_converged(&group.iter().collect::<Vec<_>>()).await;

        let b3_node = group.pop().unwrap();
        let b2_node = group.pop().unwrap();
        let mut b1_node = group.pop().unwrap();

        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
        let one_shard = serde_json::json!({
            "shards": [shard_json(&b1, &[b2.clone(), b3.clone()])],
        });
        assert_eq!(client.post(format!("{}/cluster/ring", b1)).json(&one_shard)
            .send().await.unwrap().status(), StatusCode::OK);
        let view = serde_json::to_value(b1_node.state.as_ref().unwrap().cluster_view()).unwrap();
        client.post(format!("{}/internal/cluster", dest)).json(&view).send().await.unwrap();

        // Named without replicas: vnode tokens hash node_url only, so ownership is unaffected.
        let after: Vec<&str> = vec![&b1, &dest];
        let keys: Vec<String> = (0..100).map(|i| format!("k{:03}", i)).collect();
        for key in &keys {
            // At majority, so whichever member outlives b1 already holds what it will have to push.
            assert_eq!(client.put(format!("{}/collections/t/docs/{}?w=majority&wtimeout=4000",
                b1, key)).json(&serde_json::json!({"value": {"v": 1}}))
                .send().await.unwrap().status(), StatusCode::CREATED, "seeding {}", key);
        }
        for member in [&b2, &b3] {
            assert!(wait_for_doc(&client, member, "t", keys.last().unwrap(), 1,
                Duration::from_secs(15)).await,
                "{} never caught up, so a promotion there would plan from a short log", member);
        }
        let moving: Vec<&String> = keys.iter()
            .filter(|key| owner_of(&after, key) == dest).collect();
        assert!(moving.len() >= 10, "the test ring must move a workable number of keys off b1");

        let two_shards = serde_json::json!({
            "shards": [shard_json(&b1, &[b2.clone(), b3.clone()]), shard_json(&dest, &[])],
        });
        assert_eq!(client.post(format!("{}/cluster/migrate", b1)).json(&two_shards)
            .send().await.unwrap().status(), StatusCode::ACCEPTED);

        assert!(wait_for(Duration::from_secs(20), || {
            crate::cluster::migration::progress(b1_node.state.as_ref().unwrap())
                .is_some_and(|p| p.pushed > 0)
        }).await, "b1 never started handing over");
        assert!(b1_node.state.as_ref().unwrap().migration().is_some(),
            "the handover finished before the test could interrupt it");

        b1_node.kill();

        let landed = wait_for(Duration::from_secs(60), || {
            [&b2_node, &b3_node].iter().any(|node| {
                let state = node.state.as_ref().unwrap();
                state.migration().is_none()
                    && state.cluster_view().ring.is_some_and(|ring| ring.shards.len() == 2)
            })
        }).await;

        assert!(landed, "the handover never completed after b1 died; survivors were {}",
            [&b2_node, &b3_node].map(handover_diagnostic).join(" | "));

        // The reads below go through the destination's own ownership check, so it has to have
        // adopted the flipped ring too.
        assert!(wait_for(Duration::from_secs(20), || {
            let state = destination.state.as_ref().unwrap();
            state.migration().is_none()
                && state.cluster_view().ring.is_some_and(|ring| ring.shards.len() == 2)
        }).await, "the destination never saw the completed ring");

        for key in moving {
            assert_eq!(client.get(format!("{}/collections/t/docs/{}", dest, key))
                .send().await.unwrap().status(), StatusCode::OK,
                "key {} was reassigned to the new shard but never arrived", key);
        }

        drop((b2_node, b3_node, destination));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_source_records_what_it_handed_over_where_a_restart_can_find_it() {
        let root = temp_root();
        let cl = Cluster::start_with_movement(&root, 1, 25).await;
        let (a, b, c) = (cl.a.url(), cl.b.url(), cl.c.url());
        let two: Vec<&str> = vec![&a, &b];
        let three: Vec<&str> = vec![&a, &b, &c];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&two)).await.0, StatusCode::OK);
        for node in [&b, &c] {
            cl.post(format!("{}/internal/cluster", node),
                serde_json::to_value(cl.a.state.as_ref().unwrap().cluster_view()).unwrap()).await;
        }

        let keys: Vec<String> = (0..120).map(|i| format!("k{:03}", i)).collect();
        for key in &keys {
            assert_eq!(cl.put(&owner_of(&two, key), key, 1).await, StatusCode::CREATED);
        }
        let moving_from_a = keys.iter()
            .filter(|key| owner_of(&two, key) == a && owner_of(&three, key) == c).count();
        assert!(moving_from_a > 0, "the test ring must move keys off a");
        let source_dir = cl.a.data_dir.to_string_lossy().to_string();

        assert_eq!(cl.post(format!("{}/cluster/migrate", a), ring_body(&three)).await.0,
            StatusCode::ACCEPTED);

        let recorded = wait_for(Duration::from_secs(30), || {
            MigrationRuns::restored(&source_dir).current
                .is_some_and(|p| p.done && !p.handed_over.is_empty())
        }).await;

        assert!(recorded,
            "the source never wrote down what it handed over, so a restart between the flip and              cleanup leaves the copies on both nodes with nothing able to tell them apart");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cleanup_after_a_restart_still_deletes_what_the_record_names() {
        let root = temp_root();
        let mut source = TestNode::new("s", next_test_port(), &root, "primary");
        source.start();
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();

        assert_eq!(client.put(format!("{}/collections/t/docs/k1", source.url()))
            .json(&serde_json::json!({"value": {"v": 1}}))
            .send().await.unwrap().status(), StatusCode::CREATED);

        // The ring this node has just been dropped from, adopted before the restart so the view
        // on disk is the one cleanup compares the record against.
        let gone = HashRing { vnodes: 128, shards: shards(&["http://other-a", "http://other-b"]) };
        let mut view = source.state.as_ref().unwrap().cluster_view();
        view.version += 1;
        view.seeded = false;
        view.updated_by = "operator".into();
        view.ring = Some(gone.clone());
        assert!(matches!(source.state.as_ref().unwrap().adopt_cluster(view), Adoption::Adopted { .. }));

        source.kill();
        MigrationMeta {
            completed: Some(CompletedHandover {
                id: "m1".into(),
                phase: MigrationPhase::Finalizing,
                target: gone,
                handed_over: HashSet::from([("t".to_string(), "k1".to_string())]),
            }),
            completed_resets: HashSet::new(),
        }.save(&source.data_dir.to_string_lossy()).unwrap();
        source.start();

        let response = client.post(format!("{}/internal/migrate-cleanup", source.url()))
            .json(&serde_json::json!({"migration_id": "m1"})).send().await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.json::<serde_json::Value>().await.unwrap()["removed"], 1,
            "a restart lost the handover record, so the stale copy stays on disk here and shadows              the live one the moment this node is put back in the ring");
        assert_eq!(source.state.as_ref().unwrap().db.as_ref().unwrap()
            .get_collection("t").unwrap().get("k1").unwrap(), None);

        source.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reset_already_completed_is_not_replayed_after_a_restart() {
        let root = temp_root();
        let mut h = Handover::stage(&root, Vec::new()).await;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();

        // The ordinary finalize sequence: clear whatever a previous pass left, then take the copy.
        assert_eq!(client.post(format!("{}/internal/migrate-reset", h.node.url()))
            .json(&h.reset()).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(client.post(format!("{}/internal/migrate", h.node.url()))
            .json(&h.batch(7)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(h.held(), Some(serde_json::json!({"v": 7})));

        h.node.kill();
        h.node.start();

        let replay = client.post(format!("{}/internal/migrate-reset", h.node.url()))
            .json(&h.reset()).send().await.unwrap();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(replay.json::<serde_json::Value>().await.unwrap()["repeated"], true,
            "a reset this node already ran must still be recognised as a replay");
        assert_eq!(h.held(), Some(serde_json::json!({"v": 7})),
            "a replay after a restart erased the copy the reset was meant to precede");

        h.node.kill();
    }


    /// C18: the handover record went to the node-local `migration.meta`, so a peer elected in this
    /// node's place had nothing to re-derive it from and its group kept the stale copies.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_handover_record_reaches_a_node_that_never_pushed_anything() {
        let root = temp_root();
        let mut replica = TestNode::new("h-replica", next_test_port(), &root, "replica");
        let mut source = TestNode::new("h", next_test_port(), &root, "primary");
        source.replicas = vec![replica.url()];
        source.start();
        replica.primary_addr = Some(source.url());
        replica.start();
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();

        assert_eq!(client.put(format!("{}/collections/t/docs/k1?w=majority&wtimeout=4000",
            source.url())).json(&serde_json::json!({"value": {"v": 1}}))
            .send().await.unwrap().status(), StatusCode::CREATED);
        assert!(wait_for_doc(&client, &replica.url(), "t", "k1", 1, Duration::from_secs(5)).await);

        // The ring this group has just been dropped from, adopted by both nodes.
        let gone = HashRing { vnodes: 128, shards: shards(&["http://other-a", "http://other-b"]) };
        for node in [&source, &replica] {
            let state = node.state.as_ref().unwrap();
            // Re-read per attempt: the group is live and publishes views of its own, so a version
            // sampled once can be stale by the time it is offered back.
            let adopted = wait_for(Duration::from_secs(10), || {
                let mut view = state.cluster_view();
                view.version += 1;
                view.seeded = false;
                view.updated_by = "operator".into();
                view.ring = Some(gone.clone());
                matches!(state.adopt_cluster(view), Adoption::Adopted { .. })
            }).await;
            assert!(adopted, "{} never adopted the ring its group was dropped from", node.node_id);
        }

        let source_state = source.state.as_ref().unwrap();
        crate::cluster::migration::replicate_handover(source_state, HandoverRecord {
            id: "m1".into(), target: gone.clone(),
        }).await;

        let replica_state = replica.state.as_ref().unwrap();
        assert!(wait_for(Duration::from_secs(10), || {
            !crate::cluster::migration::handed_over_after_flip(replica_state, "m1").is_empty()
        }).await, "the record never reached the node that would have to act on it");

        assert!(replica_state.migrations.lock().unwrap().current.is_none(),
            "the premise: this node pushed nothing, so nothing local could have told it");
        assert!(crate::cluster::migration::handed_over_after_flip(replica_state, "m1")
            .contains(&("t".to_string(), "k1".to_string())),
            "the ring it recorded is what says which of the keys it holds are no longer its own");

        // An id nobody recorded stays inert, so the derivation is not a licence to delete.
        assert!(crate::cluster::migration::handed_over_after_flip(replica_state, "m2").is_empty());

        source.kill();
        replica.kill();
    }

    /// H16: the only migration handler with no leadership check, so cleanup addressed to the ring's
    /// nominal primary could be accepted by a follower and answered `200 cleaned, removed: 0`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_follower_refuses_cleanup_rather_than_reporting_it_done() {
        let root = temp_root();
        let mut replica = TestNode::new("f-replica", next_test_port(), &root, "replica");
        let mut source = TestNode::new("f", next_test_port(), &root, "primary");
        source.replicas = vec![replica.url()];
        source.start();
        replica.primary_addr = Some(source.url());
        replica.start();
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();

        let response = client.post(format!("{}/internal/migrate-cleanup", replica.url()))
            .json(&serde_json::json!({"migration_id": "m1"})).send().await.unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT,
            "a follower holds no record and cannot commit a tombstone, so reporting the cleanup \
             done is how the coordinator stops asking the node that could");

        source.kill();
        replica.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_source_keeps_what_it_could_not_tombstone_on_a_quorum() {
        let root = temp_root();
        let mut replica = TestNode::new("s-replica", next_test_port(), &root, "replica");
        let mut source = TestNode::new("s", next_test_port(), &root, "primary");
        source.replicas = vec![replica.url()];
        source.start();
        replica.primary_addr = Some(source.url());
        replica.start();
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();

        assert_eq!(client.put(format!("{}/collections/t/docs/k1?w=majority&wtimeout=4000",
            source.url())).json(&serde_json::json!({"value": {"v": 1}}))
            .send().await.unwrap().status(), StatusCode::CREATED);

        // The ring this node has just been dropped from, so the key is no longer its own.
        let gone = HashRing { vnodes: 128, shards: shards(&["http://other-a", "http://other-b"]) };
        let state = source.state.as_ref().unwrap();
        let mut view = state.cluster_view();
        view.version += 1;
        view.seeded = false;
        view.updated_by = "operator".into();
        view.ring = Some(gone.clone());
        assert!(matches!(state.adopt_cluster(view), Adoption::Adopted { .. }));
        state.migrations.lock().unwrap().current = Some(MigrationProgress {
            id: "m1".into(), phase: MigrationPhase::Finalizing, target: gone,
            pushed: 1, total: 1, done: true, error: None,
            handed_over: HashSet::from([("t".to_string(), "k1".to_string())]),
        });

        replica.kill();

        let response = client.post(format!("{}/internal/migrate-cleanup", source.url()))
            .json(&serde_json::json!({"migration_id": "m1"})).send().await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE,
            "a cleanup whose tombstones reached one node must not report itself done");
        assert!(!crate::cluster::migration::handed_over_after_flip(state, "m1").is_empty(),
            "the handover record must survive a partial cleanup, or nothing can finish it later");

        source.kill();
    }

    /// Shrinking is where the tidy-up is easy to get wrong: the departing shard leaves the owner list the
    /// moment the ring lands, so anything driven off the new owners misses its copies entirely.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_shard_removed_from_the_ring_does_not_keep_the_keys_it_gave_back() {
        let root = temp_root();
        let cl = Cluster::start(&root).await;
        let (a, b, c) = (cl.a.url(), cl.b.url(), cl.c.url());
        let two: Vec<&str> = vec![&a, &b];
        let three: Vec<&str> = vec![&a, &b, &c];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&three)).await.0, StatusCode::OK);
        let keys: Vec<String> = (0..60).map(|i| format!("k{}", i)).collect();
        for key in &keys {
            assert_eq!(cl.put(&owner_of(&three, key), key, 1).await, StatusCode::CREATED);
        }
        let started_on_c: Vec<&String> = keys.iter().filter(|k| owner_of(&three, k) == c).collect();
        assert!(!started_on_c.is_empty(), "c must start with some keys to give back");

        // Shrink back to two shards: everything c holds has to go somewhere else.
        assert_eq!(cl.post(format!("{}/cluster/migrate", a), ring_body(&two)).await.0,
            StatusCode::ACCEPTED);
        assert!(wait_for(Duration::from_secs(30), || {
            cl.a.state.as_ref().unwrap().migration().is_none()
                && cl.a.state.as_ref().unwrap().cluster_view().ring
                    .is_some_and(|r| r.shards.len() == 2)
        }).await, "the shrink never completed");

        // Give cleanup a moment; it runs after the flip is published.
        let cleaned = wait_for(Duration::from_secs(15), || {
            cl.c.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.range_from(None, None, None).is_empty())
                .unwrap_or(true)
        }).await;
        assert!(cleaned, "the departed shard is still holding {} keys it handed back",
            cl.c.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.range_from(None, None, None).len()).unwrap_or(0));

        // And every key is still there, on one of the two remaining shards.
        for key in &keys {
            let holder = cl.holder(key).await;
            assert_eq!(holder.as_deref(), Some(owner_of(&two, key).as_str()),
                "key {} is not at its new owner", key);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_abandoned_handover_leaves_ownership_and_data_where_they_were() {
        let root = temp_root();
        let cl = Cluster::start(&root).await;
        let (a, b) = (cl.a.url(), cl.b.url());
        let two: Vec<&str> = vec![&a, &b];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&two)).await.0, StatusCode::OK);
        let keys: Vec<String> = (0..40).map(|i| format!("k{}", i)).collect();
        for key in &keys {
            assert_eq!(cl.put(&owner_of(&two, key), key, 1).await, StatusCode::CREATED);
        }

        // Point the handover at a destination that does not answer, so it cannot complete.
        let dead = "http://127.0.0.1:9";
        let stuck: Vec<&str> = vec![&a, &b, dead];
        let (code, body) = cl.post(format!("{}/cluster/migrate", a), ring_body(&stuck)).await;
        assert_eq!(code, StatusCode::ACCEPTED, "{}", body);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cl.a.state.as_ref().unwrap().migration().is_some(),
            "a handover that cannot reach its destination must not complete");
        assert_eq!(cl.a.state.as_ref().unwrap().cluster_view().ring.unwrap().shards.len(), 2,
            "ownership must not move while the copy is stuck");

        let r = cl.client.delete(&format!("{}/cluster/migrate", a)).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert!(cl.a.state.as_ref().unwrap().migration().is_none());

        // Ownership and data stay at the source after an abort.
        for key in &keys {
            let owner = owner_of(&two, key);
            assert_eq!(cl.holder(key).await.as_deref(), Some(owner.as_str()),
                "abandoning a handover must not move data");
            assert_eq!(cl.put(&owner, key, 2).await, StatusCode::OK,
                "the source must remain writable after an abort");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bulk_copy_keeps_moving_keys_writable() {
        let root = temp_root();
        let cl = Cluster::start(&root).await;
        let (a, b) = (cl.a.url(), cl.b.url());
        let two: Vec<&str> = vec![&a, &b];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&two)).await.0, StatusCode::OK);
        let keys: Vec<String> = (0..60).map(|i| format!("k{}", i)).collect();
        for key in &keys {
            assert_eq!(cl.put(&owner_of(&two, key), key, 1).await, StatusCode::CREATED);
        }

        // An unreachable destination keeps the migration in its bulk-copy phase.
        let dead = "http://127.0.0.1:9";
        let stalled: Vec<&str> = vec![&a, &b, dead];
        assert_eq!(cl.post(format!("{}/cluster/migrate", a), ring_body(&stalled)).await.0,
            StatusCode::ACCEPTED);
        tokio::time::sleep(Duration::from_millis(700)).await;

        let moving = keys.iter()
            .find(|k| owner_of(&two, k) == a && owner_of(&stalled, k) == dead)
            .expect("something must be moving off a");
        let staying = keys.iter()
            .find(|k| owner_of(&two, k) == a && owner_of(&stalled, k) == a)
            .expect("a must keep most of its keys");

        let moving_write = cl.client.put(&format!("{}/collections/t/docs/{}", a, moving))
            .json(&serde_json::json!({"value": {"v": 2}})).send().await.unwrap();
        assert_eq!(moving_write.status(), StatusCode::OK,
            "the bulk copy must not make moving keys read-only");

        assert_eq!(cl.put(&a, staying, 2).await, StatusCode::OK,
            "a handover must not freeze keys it does not touch");

        // Reads stay available throughout, including for the frozen key.
        let read = cl.client.get(&format!("{}/collections/t/docs/{}", a, moving))
            .send().await.unwrap();
        assert_eq!(read.status(), StatusCode::OK, "the source still owns it, so it still serves it");
    }
}
