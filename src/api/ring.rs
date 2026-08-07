//! Publishing the consistent-hash ring, and previewing what a change would cost.

use crate::api::members::{nudge, publish, writable};
use crate::model::err_json;
use crate::ring::{keyspace_movement, HashRing, RingShard, DEFAULT_VNODES};
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use tracing::info;

#[derive(Deserialize)]
pub struct RingParams {
    /// Report what the change would cost without applying it.
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Deserialize)]
pub struct RingRequest {
    pub shards: Vec<RingShard>,
    /// Omitted keeps the current count. Changing it re-derives every token, so it moves far more
    /// of the keyspace than adding a node does -- the response says how much.
    #[serde(default)]
    pub vnodes: Option<u32>,
}

enum HasData {
    No,
    Yes(String),
    Unknown(String),
}

/// Fails closed. Every answer other than a clear "nothing here" from every current owner blocks the
/// change, because the caller is deciding whether keys can safely be made unreachable.
async fn cluster_holds_data(state: &AppState) -> HasData {
    if let Some(db) = state.db.as_ref() {
        match db.has_any_data() {
            Ok(true) => return HasData::Yes("this node".to_string()),
            Ok(false) => {},
            Err(e) => return HasData::Unknown(format!("cannot read local storage: {}", e)),
        }
    }

    let own = state.own_url();
    for (owner, _) in state.cluster_view().shard_owners() {
        if crate::util::same_endpoint(&owner, &own) {
            continue;
        }
        let url = format!("{}/internal/data-summary", owner);
        match state.client.get(&url).send().await {
            Ok(r) if r.status().is_success() => {
                let body = r.json::<serde_json::Value>().await.unwrap_or_default();
                match body.get("has_data").and_then(|b| b.as_bool()) {
                    Some(true) => return HasData::Yes(owner),
                    Some(false) => {},
                    None => return HasData::Unknown(format!("{} gave no answer", owner)),
                }
            },
            Ok(r) => return HasData::Unknown(format!("{} returned {}", owner, r.status())),
            Err(e) => return HasData::Unknown(format!("{} is unreachable: {}", owner, e)),
        }
    }
    HasData::No
}

fn refuse(summary: &serde_json::Value, because: &str) -> axum::response::Response {
    (StatusCode::CONFLICT, Json(serde_json::json!({
        "error": format!(
            "refusing to reassign ownership: {}. This endpoint moves ownership without moving \
             the data, which would strand those keys. POST /cluster/migrate copies the data \
             first and flips ownership only once it has landed", because),
        "moved_fraction": summary.get("moved_fraction"),
        "movement_known": summary.get("movement_known"),
        "transfers": summary.get("transfers"),
        "hint": "POST /cluster/migrate applies the same target ring safely; ?dry_run=true here \
                 still inspects it, and allow_unsafe_ring_changes forces it through (development \
                 only, and leaves the moved keys unreadable)",
    }))).into_response()
}

pub async fn set_ring_handler(
    State(state): State<AppState>,
    Query(params): Query<RingParams>,
    Json(req): Json<RingRequest>,
) -> impl axum::response::IntoResponse {
    if let Err(resp) = writable(&state) {
        return resp;
    }

    let current = state.cluster_view();
    let vnodes = req.vnodes
        .or_else(|| current.ring.as_ref().map(|r| r.vnodes))
        .unwrap_or(DEFAULT_VNODES);

    let proposed = HashRing { vnodes, shards: req.shards };
    if let Err(why) = proposed.validate() {
        return err_json(StatusCode::UNPROCESSABLE_ENTITY, why);
    }

    // Measured before publishing, so a dry run and the real thing report the same number.
    let after = proposed.build();
    let movement = state.built_ring().map(|before| keyspace_movement(&before, &after));

    let previous_model = if current.ring.is_some() { "ring" }
                         else if current.shards.is_empty() { "none" }
                         else { "ranges" };

    let owners: Vec<&str> = proposed.shards.iter().map(|s| s.node_url.as_str()).collect();
    let summary = serde_json::json!({
        "vnodes": vnodes,
        "shards": owners,
        "moved_fraction": movement.as_ref().map(|m| m.moved_fraction),
        "transfers": movement.as_ref().map(|m| &m.transfers),
        "previous_model": previous_model,
        // A null fraction is "not comparable", which a client would otherwise read as "nothing
        // moves". This node has no ring to diff against, usually because the cluster's ring was
        // never published to it -- config rings are per-node seeds and do not propagate.
        "movement_known": movement.is_some(),
        "note": movement.is_none().then_some(
            "this node holds no ring to compare against, so the cost of the change is unknown \
             from here; publish the current ring first to get a movement estimate"),
    });

    if params.dry_run {
        return (StatusCode::OK, Json(serde_json::json!({
            "status": "dry_run", "version": current.version, "would_apply": summary,
        }))).into_response();
    }

    // Ownership moves the instant each node adopts, and this endpoint moves nothing else. That is
    // a deliberate outage of the moved keys, so it is refused rather than warned about and the
    // caller is pointed at /cluster/migrate. A change proven to move nothing is always safe.
    let proven_no_op = movement.as_ref().is_some_and(|m| m.moved_fraction == 0.0);
    if !proven_no_op && !state.config.allow_unsafe_ring_changes {
        match cluster_holds_data(&state).await {
            HasData::No => {},
            HasData::Yes(where_) => return refuse(&summary, &format!(
                "{} holds data, and this change reassigns ownership", where_)),
            HasData::Unknown(why) => return refuse(&summary, &format!(
                "cannot confirm the cluster is empty ({})", why)),
        }
    }

    let next = current.with_ring(&state.config.node_id, proposed);
    let version = match publish(&state, next).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    // Routing changes the moment each node adopts, and nothing moves the data with it. Say so
    // where an operator will see it, not only in the docs.
    info!(target: "ring", version, moved = ?movement.as_ref().map(|m| m.moved_fraction),
        "Published a new hash ring; keys that changed owner are unreachable until their data moves");

    let view = state.cluster_view();
    for owner in view.shard_owners().into_iter().map(|(url, _)| url) {
        if !crate::util::same_endpoint(&owner, &state.own_url()) {
            nudge(&state, &owner, &view);
        }
    }

    (StatusCode::OK, Json(serde_json::json!({
        "status": "applied",
        "version": version,
        "applied": summary,
        "warning": "ownership moved without moving data; reassigned keys read as missing at their \
                    new owner. POST /cluster/migrate is the path that moves the data too",
    }))).into_response()
}

#[cfg(test)]
mod tests {
    use crate::cluster::metadata::ClusterMetadata;
    use crate::ring::{hash_key, HashRing, RingShard};
    use crate::state::AppState;
    use crate::test_support::temp_root;

    fn shards(urls: &[&str]) -> Vec<RingShard> {
        urls.iter().map(|u| RingShard { node_url: u.to_string(), replica_urls: vec![] }).collect()
    }

    fn router_on_ring(root: &std::path::Path, urls: &[&str]) -> AppState {
        let ring = serde_json::to_value(HashRing { vnodes: 128, shards: shards(urls) }).unwrap();
        AppState::for_routing_test(serde_json::from_value(serde_json::json!({
            "node_id": "r1", "role": "router", "listen_addr": "127.0.0.1:1",
            "data_dir": root.to_string_lossy(),
            "ring": ring,
        })).unwrap())
    }

    fn route(state: &AppState, key: &str) -> String {
        state.get_effective_shard_url(hash_key("docs", key)).unwrap().0
    }

    #[test]
    fn a_router_routes_by_the_ring_and_follows_a_published_change() {
        let root = temp_root();
        let state = router_on_ring(&root, &["http://a", "http://b", "http://c"]);
        let keys: Vec<String> = (0..400).map(|i| format!("key-{}", i)).collect();

        let before: Vec<String> = keys.iter().map(|k| route(&state, k)).collect();
        assert!(before.iter().any(|o| o == "http://a"), "the ring must spread across every shard");
        assert!(before.iter().any(|o| o == "http://c"));
        assert_eq!(state.shard_owners().len(), 3, "fan-out must see the same owners as routing");

        let next = state.cluster_view().with_ring("operator", HashRing {
            vnodes: 128, shards: shards(&["http://a", "http://b", "http://c", "http://d"]),
        });
        assert!(matches!(state.adopt_cluster(next), crate::cluster::metadata::Adoption::Adopted { .. }));

        let after: Vec<String> = keys.iter().map(|k| route(&state, k)).collect();
        let moved: Vec<usize> = (0..keys.len()).filter(|i| before[*i] != after[*i]).collect();
        assert!(!moved.is_empty(), "the router kept routing by the old ring after adopting a new one");
        for i in moved {
            assert_eq!(after[i], "http://d",
                "key {} went from {} to {}; only the added shard may gain keys",
                keys[i], before[i], after[i]);
        }
        assert_eq!(state.shard_owners().len(), 4);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_cached_ring_is_rebuilt_when_the_view_moves() {
        let root = temp_root();
        let state = router_on_ring(&root, &["http://a", "http://b"]);

        let first = state.built_ring().expect("a ring view must build a ring");
        let again = state.built_ring().expect("still there");
        assert!(std::sync::Arc::ptr_eq(&first, &again),
            "an unchanged view must reuse the built ring rather than rebuild it per request");

        let next = state.cluster_view().with_ring("operator", HashRing {
            vnodes: 128, shards: shards(&["http://a", "http://b", "http://z"]),
        });
        state.adopt_cluster(next);

        let rebuilt = state.built_ring().unwrap();
        assert!(!std::sync::Arc::ptr_eq(&first, &rebuilt), "a new version must invalidate the cache");
        let reaches_new_shard = (0..500)
            .any(|i| rebuilt.owner(hash_key("docs", &format!("k{}", i)))
                .is_some_and(|s| s.node_url == "http://z"));
        assert!(reaches_new_shard, "the rebuilt ring must actually route to the added shard");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_ring_supersedes_ranges_without_discarding_them() {
        let root = temp_root();
        const HALF: u64 = 9223372036854775808;
        let state = AppState::for_routing_test(serde_json::from_value(serde_json::json!({
            "node_id": "r1", "role": "router", "listen_addr": "127.0.0.1:1",
            "data_dir": root.to_string_lossy(),
            "shard_map": [
                {"start_hash": 0, "end_hash": HALF, "node_url": "http://old-a", "replica_urls": []},
                {"start_hash": HALF, "end_hash": 0, "node_url": "http://old-b", "replica_urls": []}],
        })).unwrap());

        assert!(state.built_ring().is_none(), "a range view has no token ring");
        let on_ranges: Vec<String> = (0..50).map(|i| route(&state, &format!("k{}", i))).collect();
        assert!(on_ranges.iter().all(|o| o.starts_with("http://old-")));

        let next = state.cluster_view().with_ring("operator", HashRing {
            vnodes: 128, shards: shards(&["http://new-a", "http://new-b"]),
        });
        state.adopt_cluster(next);

        let on_ring: Vec<String> = (0..50).map(|i| route(&state, &format!("k{}", i))).collect();
        assert!(on_ring.iter().all(|o| o.starts_with("http://new-")),
            "the ring must win wherever a key is routed: {:?}", on_ring);
        assert_eq!(state.shard_owners().len(), 2);
        assert!(state.shard_owners().iter().all(|(u, _)| u.starts_with("http://new-")));

        // Kept, not cleared, so a rollback publish can put the cluster back on ranges.
        assert_eq!(state.cluster_view().shards.len(), 2,
            "the ranges must survive so the change can be undone");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn only_a_leader_publishes_a_ring_and_the_cost_is_reported_before_it_lands() {
        use crate::test_support::three_node_cluster;
        let root = temp_root();
        let (n1, n2, _n3) = three_node_cluster(&root).await;
        let c = reqwest::Client::builder().timeout(std::time::Duration::from_secs(3))
            .build().unwrap();

        // Real, reachable, empty nodes: the safety gate asks every current owner whether it holds
        // data, and an owner it cannot reach blocks the change.
        let base = serde_json::json!({
            "shards": [{"node_url": n1.url()}, {"node_url": n2.url()}],
        });

        let r = c.post(&format!("{}/cluster/ring", n2.url())).json(&base).send().await.unwrap();
        assert_eq!(r.status(), axum::http::StatusCode::CONFLICT,
            "a follower publishing would be a second writer to a view that only converges");

        // First ring on a cluster that had none: the cost is not knowable, and must not read as 0.
        let r = c.post(&format!("{}/cluster/ring?dry_run=true", n1.url()))
            .json(&base).send().await.unwrap();
        let body = r.json::<serde_json::Value>().await.unwrap();
        assert_eq!(body["status"], "dry_run");
        assert_eq!(body["would_apply"]["movement_known"], false);
        assert!(body["would_apply"]["moved_fraction"].is_null());
        assert!(body["would_apply"]["note"].as_str().unwrap().contains("publish the current ring"));
        assert!(n1.state.as_ref().unwrap().built_ring().is_none(), "a dry run must not apply");

        assert_eq!(c.post(&format!("{}/cluster/ring", n1.url())).json(&base)
            .send().await.unwrap().status(), axum::http::StatusCode::OK);

        // Now there is a ring to diff against, so adding a shard reports a real number.
        let grown = serde_json::json!({
            "shards": [{"node_url": n1.url()}, {"node_url": n2.url()}, {"node_url": "http://s3"}],
        });
        let r = c.post(&format!("{}/cluster/ring?dry_run=true", n1.url()))
            .json(&grown).send().await.unwrap();
        let body = r.json::<serde_json::Value>().await.unwrap();
        let moved = body["would_apply"]["moved_fraction"].as_f64().unwrap();

        assert_eq!(body["would_apply"]["movement_known"], true);
        assert!((0.10..0.60).contains(&moved), "moved_fraction {} is not a third-ish", moved);
        let transfers = body["would_apply"]["transfers"].as_array().unwrap();
        assert!(!transfers.is_empty());
        assert!(transfers.iter().all(|t| t["to"] == "http://s3"),
            "every transfer must be inbound to the added shard: {:?}", transfers);

        // The applied number must be the number that was previewed.
        let r = c.post(&format!("{}/cluster/ring", n1.url())).json(&grown).send().await.unwrap();
        let applied = r.json::<serde_json::Value>().await.unwrap();
        assert_eq!(applied["status"], "applied");
        assert_eq!(applied["applied"]["moved_fraction"].as_f64().unwrap(), moved,
            "a dry run that disagrees with the real thing is worse than no dry run");
        assert!(applied["warning"].as_str().unwrap().contains("/cluster/migrate"),
            "the warning must point at the endpoint that moves the data too");

        let _ = std::fs::remove_dir_all(&root);
    }

    /// Ownership moves on publish; this endpoint moves no data. So the whole safety question is
    /// "is there anything to strand", and the gate is built around that one fact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ownership_cannot_be_reassigned_while_the_cluster_holds_data() {
        use crate::test_support::{put_doc_at, three_node_cluster};
        use axum::http::StatusCode;

        let root = temp_root();
        let (n1, n2, _n3) = three_node_cluster(&root).await;
        let c = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5))
            .build().unwrap();

        let two = serde_json::json!({"shards": [{"node_url": n1.url()}, {"node_url": n2.url()}]});
        let three = serde_json::json!({
            "shards": [{"node_url": n1.url()}, {"node_url": n2.url()}, {"node_url": "http://s3"}],
        });
        let post = |url: String, body: serde_json::Value| {
            let c = c.clone();
            async move {
                let r = c.post(&url).json(&body).send().await.unwrap();
                (r.status(), r.json::<serde_json::Value>().await.unwrap_or_default())
            }
        };

        // 1. The first ring on a cluster with nothing in it. Nothing can be stranded.
        let (code, body) = post(format!("{}/cluster/ring", n1.url()), two.clone()).await;
        assert_eq!(code, StatusCode::OK, "an empty cluster must accept its first ring: {}", body);

        // A key n1 actually owns: writes to a shard are now checked against the ring.
        let built = HashRing { vnodes: 128, shards: shards(&[&n1.url(), &n2.url()]) }.build();
        let mine = (0..5000).map(|i| format!("k{}", i))
            .find(|k| built.owner(hash_key("t", k)).unwrap().node_url == n1.url())
            .expect("n1 must own some keys");
        assert_eq!(put_doc_at(&c, &n1.url(), "t", &mine, 1, "?w=1").await, StatusCode::CREATED);

        // 2. Inspecting a change is always allowed; it changes nothing.
        let (code, body) = post(format!("{}/cluster/ring?dry_run=true", n1.url()), three.clone()).await;
        assert_eq!(code, StatusCode::OK, "a dry run must survive the gate: {}", body);
        assert_eq!(body["status"], "dry_run");
        let predicted = body["would_apply"]["moved_fraction"].as_f64().unwrap();
        assert!(predicted > 0.0);

        // 3. The same change for real, now that a key exists that it would strand.
        let (code, body) = post(format!("{}/cluster/ring", n1.url()), three.clone()).await;
        assert_eq!(code, StatusCode::CONFLICT,
            "reassigning ownership over live data must be refused, not warned about: {}", body);
        assert_eq!(body["moved_fraction"].as_f64().unwrap(), predicted,
            "the refusal must carry the number the operator was deciding on");
        let err = body["error"].as_str().unwrap();
        assert!(err.contains("/cluster/migrate"), "the refusal must name the safe path: {}", err);
        assert!(err.contains("holds data"), "and what triggered it: {}", err);
        assert!(body["hint"].as_str().unwrap().contains("allow_unsafe_ring_changes"));

        assert_eq!(n1.state.as_ref().unwrap().built_ring().unwrap().owner(0).is_some(), true);
        assert_eq!(n1.state.as_ref().unwrap().cluster_view().ring.unwrap().shards.len(), 2,
            "the refused ring must not have been applied");

        // 4. Republishing what is already in force moves nothing, so data is irrelevant.
        let (code, body) = post(format!("{}/cluster/ring", n1.url()), two.clone()).await;
        assert_eq!(code, StatusCode::OK, "a no-op republish must stay allowed on live data: {}", body);
        assert_eq!(body["applied"]["moved_fraction"].as_f64().unwrap(), 0.0);

        let _ = std::fs::remove_dir_all(&root);
    }

    /// The override, on a solo primary so the node keeps leadership across the restart that turns
    /// the flag on. Same node, same data, same ring change: only the flag differs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_development_override_is_the_only_way_through() {
        use crate::test_support::{next_test_port, put_doc_at, TestNode};
        use axum::http::StatusCode;

        let root = temp_root();
        let mut solo = TestNode::new("solo", next_test_port(), &root, "primary");
        solo.start();
        let c = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5))
            .build().unwrap();

        let one = serde_json::json!({"shards": [{"node_url": solo.url()}]});
        let two = serde_json::json!({
            "shards": [{"node_url": solo.url()}, {"node_url": "http://s2"}],
        });
        let apply = |body: serde_json::Value, url: String| {
            let c = c.clone();
            async move {
                let r = c.post(&url).json(&body).send().await.unwrap();
                (r.status(), r.json::<serde_json::Value>().await.unwrap_or_default())
            }
        };

        assert_eq!(apply(one, format!("{}/cluster/ring", solo.url())).await.0, StatusCode::OK);
        // A one-shard ring means solo owns everything, so any key works here.
        assert_eq!(put_doc_at(&c, &solo.url(), "t", "k1", 1, "?w=1").await, StatusCode::CREATED);

        let (code, refused) = apply(two.clone(), format!("{}/cluster/ring", solo.url())).await;
        assert_eq!(code, StatusCode::CONFLICT, "the flag is off, so this must be refused: {}", refused);
        let predicted = refused["moved_fraction"].as_f64().unwrap();
        assert!(predicted > 0.0);

        solo.kill();
        solo.allow_unsafe_ring_changes = true;
        solo.start();
        assert!(solo.is_leader(), "a solo primary keeps leadership across a restart");
        assert_eq!(solo.state.as_ref().unwrap().cluster_view().ring.unwrap().shards.len(), 1,
            "the refused change must not have survived the restart either");

        let (code, applied) = apply(two, format!("{}/cluster/ring", solo.url())).await;
        assert_eq!(code, StatusCode::OK,
            "the explicit override must permit exactly the change it exists to permit: {}", applied);
        assert_eq!(applied["status"], "applied");
        assert_eq!(applied["applied"]["moved_fraction"].as_f64().unwrap(), predicted,
            "the override must not change the cost, only whether it is allowed");
        assert_eq!(solo.state.as_ref().unwrap().cluster_view().ring.unwrap().shards.len(), 2);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_owner_that_cannot_be_asked_blocks_the_change() {
        use crate::test_support::three_node_cluster;
        use axum::http::StatusCode;

        let root = temp_root();
        let (n1, _n2, _n3) = three_node_cluster(&root).await;
        let c = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5))
            .build().unwrap();

        // A ring whose other owner does not exist. n1 itself is empty, so a local-only check would
        // wave this through while a whole shard's worth of data might be sitting behind that URL.
        let with_ghost = serde_json::json!({
            "shards": [{"node_url": n1.url()}, {"node_url": "http://127.0.0.1:9/"}],
        });
        assert_eq!(c.post(&format!("{}/cluster/ring", n1.url())).json(&with_ghost)
            .send().await.unwrap().status(), StatusCode::OK, "empty cluster, first ring");

        let moved = serde_json::json!({
            "shards": [{"node_url": n1.url()}, {"node_url": "http://127.0.0.1:9/"},
                       {"node_url": "http://s3"}],
        });
        let r = c.post(&format!("{}/cluster/ring", n1.url())).json(&moved).send().await.unwrap();
        let code = r.status();
        let body = r.json::<serde_json::Value>().await.unwrap_or_default();
        assert_eq!(code, StatusCode::CONFLICT, "an unanswerable owner must fail closed: {}", body);
        assert!(body["error"].as_str().unwrap().contains("cannot confirm"),
            "the refusal must distinguish 'unknown' from 'populated': {}", body);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_ring_that_cannot_route_never_reaches_the_view() {
        let root = temp_root();
        let state = router_on_ring(&root, &["http://a", "http://b"]);
        let baseline = route(&state, "key-1");

        let mut broken = ClusterMetadata { ring: None, ..state.cluster_view() };
        broken.ring = Some(HashRing { vnodes: 0, shards: shards(&["http://x"]) });
        broken.version = 99;
        broken.seeded = false;
        assert!(matches!(state.adopt_cluster(broken),
            crate::cluster::metadata::Adoption::Rejected(_)),
            "a ring with no tokens owns nothing and must not be adopted");

        assert_eq!(route(&state, "key-1"), baseline, "routing must be untouched by the refusal");

        let _ = std::fs::remove_dir_all(&root);
    }
}
