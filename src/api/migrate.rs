//! Starting, watching, aborting and completing a handover.
//!
//! The coordinator is whichever leader was asked to start it. It owns no data movement -- each
//! shard pushes its own keys -- it only waits for every source to finish and then publishes the
//! ring that makes the move real. If the coordinator dies mid-move nothing is lost: the plan is in
//! the view, sources keep pushing, and another operator call can finish or abandon it.

use crate::api::members::{publish, writable};
use crate::api::ring::RingRequest;
use crate::cluster::metadata::Migration;
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
                "note": "keys being moved are read-only until the handover completes; \
                         watch GET /cluster/migrate",
            }))).into_response(),
        Err(resp) => resp,
    }
}

/// Starts the same copy-before-flip workflow for both an operator request and the automatic
/// reconciler. Keeping one entry point prevents automatic movement from acquiring weaker safety
/// rules than an explicit `/cluster/migrate` call.
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
    let movement = keyspace_movement(&before, &target.build());
    if movement.moved_fraction == 0.0 {
        let next = current.with_ring(&state.config.node_id, target);
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
    };
    let next = current.with_migration(&state.config.node_id, Some(migration));

    // Handed out before this node adopts it. Adopting starts our own push immediately, and a
    // destination that has not seen the plan yet refuses the batch -- survivable, since pushes
    // retry, but it costs a backoff on every handover for no reason.
    broadcast(state, &next, Some(&target));

    let version = match publish(state, next).await {
        Ok(v) => v,
        Err(resp) => return Err(resp),
    };

    info!(target: "migration", id = %id, moved = movement.moved_fraction,
        "Handover started; keys that are moving are read-only until it completes");
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

    let targets: Vec<String> = view.shard_owners().into_iter().map(|(url, _)| url)
        .chain(extra.into_iter().flat_map(|r| r.shards.iter().map(|s| s.node_url.clone())))
        .filter(|url| !crate::util::same_endpoint(url, &own))
        .filter(|url| seen.insert(crate::util::endpoint_of(url).to_string()))
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
        loop {
            tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

            match state.migration() {
                Some(m) if m.id == id => {},
                // Abandoned, or replaced by something newer. Either way this is not ours to finish.
                _ => {
                    info!(target: "migration", id = %id, "Handover no longer in the view; stopping");
                    return;
                },
            }

            match sources_finished(&state, &id).await {
                Ok(false) => continue,
                Err(why) => {
                    // Keep waiting rather than abandon: a source that is briefly unreachable is
                    // the normal case, and completing without it would strand its keys.
                    warn!(target: "migration", id = %id, reason = %why, "Still waiting on sources");
                    continue;
                },
                Ok(true) => {},
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

/// Recreates the coordinator loop after its process restarts. Sources already restart their own
/// idempotent copy from the durable plan; this restores the missing "wait, flip, clean up" half.
pub(crate) fn resume_migration_coordination(state: &AppState) {
    let plan = match state.migration() {
        Some(plan) => plan,
        None => return,
    };
    let sources = state.cluster_view().shard_owners()
        .into_iter().map(|(url, _)| url).collect();
    coordinate(state.clone(), plan.id, plan.target, sources);
}

/// Every source, including this node. A source that cannot be asked is not finished.
async fn sources_finished(state: &AppState, id: &str) -> Result<bool, String> {
    let own = state.own_url();
    if let Some(p) = crate::cluster::migration::progress(state) {
        if p.id == id && !p.done {
            return Ok(false);
        }
    }

    for (owner, _) in state.cluster_view().shard_owners() {
        if crate::util::same_endpoint(&owner, &own) {
            continue;
        }
        let url = format!("{}/internal/migration-status", owner);
        let body = state.client.get(&url).send().await
            .map_err(|e| format!("{}: {}", owner, e))?
            .json::<serde_json::Value>().await
            .map_err(|e| format!("{}: {}", owner, e))?;

        match body.get("progress").and_then(|p| p.get("done")).and_then(|d| d.as_bool()) {
            Some(true) => {},
            // No progress at all means the node has not started, which is not the same as finished.
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Asks everyone who might be holding a handed-over key to drop it: the shards that owned the
/// keyspace before, plus the ones that own it now. A shard dropped from the ring is in the first
/// list only, and it is the one most likely to be sitting on a whole shard's worth of copies.
///
/// Leftovers are unreachable while the node stays out of the ring, but they are not harmless: put
/// that node back later and its stale values would shadow the current ones.
async fn cleanup(state: &AppState, id: &str, sources: &[String]) {
    let view = state.cluster_view();
    let body = serde_json::json!({ "migration_id": id });
    let mut seen = std::collections::HashSet::new();

    let nodes: Vec<String> = sources.iter().cloned()
        .chain(view.shard_owners().into_iter().map(|(url, _)| url))
        .filter(|url| seen.insert(crate::util::endpoint_of(url).to_string()))
        .collect();

    for node in nodes {
        // The final view first, and awaited. A node still holding the plan refuses to clean up --
        // correctly, since from where it stands ownership has not moved yet. Relying on ordinary
        // propagation to get there first would make cleanup a race it usually loses.
        if !crate::util::same_endpoint(&node, &state.own_url()) {
            let handover = format!("{}/internal/cluster", node);
            if let Err(e) = state.client.post(&handover).json(&view).send().await {
                warn!(target: "migration", node = %node, error = %e,
                    "Could not hand over the completed view; skipping cleanup there");
                continue;
            }
        }

        let url = format!("{}/internal/migrate-cleanup", node);
        if let Err(e) = state.client.post(&url).json(&body).send().await {
            warn!(target: "migration", node = %node, error = %e,
                "Could not clean up handed-over keys; they are unreachable but still on disk");
        }
    }
}

pub async fn migration_status(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    let plan = state.migration();
    (StatusCode::OK, Json(serde_json::json!({
        "in_progress": plan.is_some(),
        "migration": plan.map(|m| serde_json::json!({
            "id": m.id,
            "started_by": m.started_by,
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
    use crate::ring::{hash_key, HashRing, RingShard};
    use crate::test_support::{next_test_port, temp_root, wait_for, TestNode};
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
            let mut nodes: Vec<TestNode> = ["a", "b", "c"].iter()
                .map(|id| {
                    let mut n = TestNode::new(id, next_test_port(), root, "primary");
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

        let _ = std::fs::remove_dir_all(&root);
    }

    /// Shrinking is where the tidy-up is easy to get wrong: the departing shard leaves the owner
    /// list the moment the ring lands, so anything driven off the new owners misses it entirely.
    /// Its copies are unreachable while it stays out, but adding it back would resurrect them.
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

        let _ = std::fs::remove_dir_all(&root);
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

        // Everything is still where it was, and writable again.
        for key in &keys {
            let owner = owner_of(&two, key);
            assert_eq!(cl.holder(key).await.as_deref(), Some(owner.as_str()),
                "abandoning a handover must not move data");
            assert_eq!(cl.put(&owner, key, 2).await, StatusCode::OK,
                "the freeze must lift when the plan is abandoned");
        }

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn keys_being_handed_over_are_read_only_and_the_rest_are_not() {
        let root = temp_root();
        let cl = Cluster::start(&root).await;
        let (a, b) = (cl.a.url(), cl.b.url());
        let two: Vec<&str> = vec![&a, &b];

        assert_eq!(cl.post(format!("{}/cluster/ring", a), ring_body(&two)).await.0, StatusCode::OK);
        let keys: Vec<String> = (0..60).map(|i| format!("k{}", i)).collect();
        for key in &keys {
            assert_eq!(cl.put(&owner_of(&two, key), key, 1).await, StatusCode::CREATED);
        }

        // Stall the handover on an unreachable third shard so the frozen state can be observed.
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

        let frozen = cl.client.put(&format!("{}/collections/t/docs/{}", a, moving))
            .json(&serde_json::json!({"value": {"v": 2}})).send().await.unwrap();
        assert_eq!(frozen.status(), StatusCode::SERVICE_UNAVAILABLE,
            "a key mid-handover must not accept writes at the node that is losing it");
        assert_eq!(frozen.headers().get("retry-after").map(|v| v.to_str().unwrap()), Some("1"),
            "the client is being asked to wait, not told it failed");

        assert_eq!(cl.put(&a, staying, 2).await, StatusCode::OK,
            "a handover must not freeze keys it does not touch");

        // Reads stay available throughout, including for the frozen key.
        let read = cl.client.get(&format!("{}/collections/t/docs/{}", a, moving))
            .send().await.unwrap();
        assert_eq!(read.status(), StatusCode::OK, "the source still owns it, so it still serves it");

        let _ = std::fs::remove_dir_all(&root);
    }
}
