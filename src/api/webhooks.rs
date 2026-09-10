//! Webhook subscription administration. Registrations commit through the group log; pushes only
//! accelerate local reconciliation.

use crate::api::middleware::{client_collection, CollectionPath};
use crate::auth::Credential;
use crate::model::err_json;
use crate::state::AppState;
use crate::util::encode_path_segment;
use crate::webhook::{
    valid_webhook_id, valid_webhook_url, WebhookKey, WebhookSpec, MAX_WEBHOOK_ID_LEN,
    MAX_WEBHOOK_URL_LEN,
};
use axum::extract::{Extension, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use tracing::info;

#[derive(Deserialize)]
pub struct CreateWebhook {
    pub id: String,
    pub url: String,
    pub secret: Option<String>,
    pub filter: Option<String>,
    pub ops: Option<String>,
    #[serde(default)]
    pub position: Option<u64>,
    /// Set on the copy the leader pushes: the whole group holds one registration, so it holds one
    /// creating credential, and each node judges that same digest against its own key set.
    #[serde(default)]
    pub creator: Option<String>,
}

#[derive(Deserialize)]
pub struct WebhookParams {
    /// Set on the copy the leader pushes to its own group, so a peer stores it without pushing it
    /// on again.
    #[serde(default)]
    pub local: bool,
}

/// A webhook is delivered by the group that holds the keys, so it is registered there. A router
/// names the groups rather than guessing which one the operator meant.
fn on_a_router(state: &AppState) -> axum::response::Response {
    let groups: Vec<String> = state.partitioning().1.into_iter().map(|(owner, _)| owner).collect();
    (StatusCode::NOT_IMPLEMENTED, Json(serde_json::json!({
        "error": "webhook subscriptions are registered on a shard group, not on a router",
        "shards": groups,
    }))).into_response()
}

fn no_webhooks() -> axum::response::Response {
    err_json(StatusCode::NOT_IMPLEMENTED,
        "webhooks.enabled is false on this node; a subscription here would never be delivered"
            .to_string())
}

fn missing(key: &WebhookKey) -> axum::response::Response {
    err_json(StatusCode::NOT_FOUND,
        format!("collection '{}' has no webhook subscription '{}'", key.0, key.1))
}

/// Best-effort wakeup; the committed catalogue repairs a missed push.
async fn push_to_group(
    state: &AppState,
    collection: &str,
    body: Option<&serde_json::Value>,
    id: &str,
) -> (Vec<String>, Vec<String>) {
    let mut peers = state.voting_replicas();
    peers.extend(state.learner_replicas());
    let (mut took, mut missed) = (Vec::new(), Vec::new());

    for peer in peers {
        let path = match body {
            Some(_) => format!("{}/collections/{}/webhooks?local=true",
                peer, encode_path_segment(collection)),
            None => format!("{}/collections/{}/webhooks/{}?local=true",
                peer, encode_path_segment(collection), encode_path_segment(id)),
        };
        let request = match body {
            Some(body) => state.client.post(&path).json(body),
            None => state.client.delete(&path),
        };
        match request.send().await {
            // A `404` on a delete is the peer already agreeing there is nothing there.
            Ok(r) if r.status().is_success() || (body.is_none() && r.status() == StatusCode::NOT_FOUND) =>
                took.push(peer),
            _ => missed.push(peer),
        }
    }
    (took, missed)
}

pub async fn create_webhook(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<WebhookParams>,
    credential: Option<Extension<Credential>>,
    Json(payload): Json<CreateWebhook>,
) -> axum::response::Response {
    if state.config.role == "router" {
        return on_a_router(&state);
    }
    if !state.config.webhooks.enabled {
        return no_webhooks();
    }
    // A replica takes the leader's copy so it can deliver if it is elected, and refuses one an
    // operator sends it directly: the group has one node that delivers, and this is not it.
    if !params.local && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }
    if !valid_webhook_id(&payload.id) {
        return err_json(StatusCode::BAD_REQUEST, format!(
            "invalid webhook id: expected 1-{} of [A-Za-z0-9_-]", MAX_WEBHOOK_ID_LEN));
    }
    if !valid_webhook_url(&payload.url) {
        return err_json(StatusCode::BAD_REQUEST, format!(
            "invalid webhook url: expected an absolute http(s) URL of up to {} bytes",
            MAX_WEBHOOK_URL_LEN));
    }
    // Parsed here rather than at the first delivery: a filter the engine cannot evaluate is the
    // operator's error, and finding out about it from a subscription that never fires is worse.
    if let Err(e) = crate::cdc::CdcFilter::parse(payload.filter.as_deref(), payload.ops.as_deref()) {
        return err_json(StatusCode::BAD_REQUEST, e);
    }

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    // A pushed copy carries the client's digest, not the pushing leader's: the group's nodes hold
    // one registration between them, and it is the client's credential that created it.
    let creator = match params.local {
        true => payload.creator.clone(),
        false => credential.and_then(|Extension(c)| c.digest()),
    };
    let spec = WebhookSpec {
        id: payload.id.clone(),
        collection: col_name.clone(),
        url: payload.url.clone(),
        secret: payload.secret.clone(),
        filter: payload.filter.clone(),
        ops: payload.ops.clone(),
        creator: creator.clone(),
    };
    let feed_position = col.applied_lsn();
    // Covers commits between sampling the initial cursor and transferring ownership to the store.
    let registration_pin = col.changefeed.pin_from(feed_position);
    // From here rather than from the start of the log: a new subscription is "from now on", the
    // way a change stream that names no position is.
    let initial_position = if params.local {
        payload.position.unwrap_or(feed_position)
    } else {
        feed_position
    };
    let key = (col_name.clone(), payload.id.clone());
    if params.local {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if let Err(e) = crate::webhook::reconcile_registrations(&state) {
                return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
            if state.webhooks.get(&key).is_some_and(|s| s.spec == spec) { break; }
            if tokio::time::Instant::now() >= deadline {
                return err_json(StatusCode::SERVICE_UNAVAILABLE, "registration is still catching up".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    } else if let Err(response) = crate::webhook::commit_registration(
        &state, &key, Some((spec, initial_position)),
    ).await {
        return response;
    }
    let Some(subscription) = state.webhooks.get(&key) else {
        return err_json(StatusCode::SERVICE_UNAVAILABLE,
            "registration has not replicated to this node yet".into());
    };
    state.webhooks.sync_pins(state.db.as_ref().unwrap());
    drop(registration_pin);

    let mut body = subscription.public();
    if !params.local {
        let forwarded = serde_json::json!({
            "id": payload.id, "url": payload.url, "secret": payload.secret,
            "filter": payload.filter, "ops": payload.ops, "position": subscription.delivery.position,
            "creator": creator,
        });
        let (took, missed) = push_to_group(&state, &col_name, Some(&forwarded), &payload.id).await;
        body["replicated_to"] = serde_json::json!(took);
        if !missed.is_empty() {
            body["unreachable"] = serde_json::json!(missed);
        }
        info!(target: "webhook", collection = %col_name, subscription = %payload.id,
            url = %payload.url, "Registered a webhook subscription");
    }
    (StatusCode::CREATED, Json(body)).into_response()
}

pub async fn list_webhooks(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
) -> axum::response::Response {
    if state.config.role == "router" {
        return on_a_router(&state);
    }
    let subscriptions: Vec<serde_json::Value> = state.webhooks.list(&col_name)
        .iter().map(|s| s.public()).collect();
    (StatusCode::OK, Json(serde_json::json!({
        "collection": col_name,
        "delivering": state.is_leader(),
        "webhooks": subscriptions,
    }))).into_response()
}

pub async fn get_webhook(
    State(state): State<AppState>,
    CollectionPath((col_name, id)): CollectionPath<(String, String)>,
) -> axum::response::Response {
    if state.config.role == "router" {
        return on_a_router(&state);
    }
    let key = (col_name, id);
    match state.webhooks.get(&key) {
        Some(subscription) => (StatusCode::OK, Json(subscription.public())).into_response(),
        None => missing(&key),
    }
}

pub async fn delete_webhook(
    State(state): State<AppState>,
    CollectionPath((col_name, id)): CollectionPath<(String, String)>,
    Query(params): Query<WebhookParams>,
) -> axum::response::Response {
    if state.config.role == "router" {
        return on_a_router(&state);
    }
    if !params.local && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    let key = (col_name.clone(), id.clone());
    let removed = if params.local {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if let Err(e) = crate::webhook::reconcile_registrations(&state) {
                return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
            if state.webhooks.get(&key).is_none() { break; }
            if tokio::time::Instant::now() >= deadline {
                return err_json(StatusCode::SERVICE_UNAVAILABLE, "removal is still catching up".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        true
    } else {
        match crate::webhook::commit_registration(&state, &key, None).await {
            Ok(removed) => removed,
            Err(response) => return response,
        }
    };
    state.webhooks.sync_pins(state.db.as_ref().unwrap());
    if params.local {
        return match removed {
            true => StatusCode::NO_CONTENT.into_response(),
            false => missing(&key),
        };
    }

    // The peers are told either way: this node not holding it says nothing about whether they do.
    let (took, missed) = push_to_group(&state, &col_name, None, &id).await;
    if !removed && took.is_empty() && missed.is_empty() {
        return missing(&key);
    }
    info!(target: "webhook", collection = %col_name, subscription = %id,
        "Removed a webhook subscription");
    (StatusCode::OK, Json(serde_json::json!({
        "collection": col_name,
        "id": id,
        "removed": removed,
        "replicated_to": took,
        "unreachable": missed,
    }))).into_response()
}

#[cfg(test)]
mod tests {
    use crate::test_support::{
        get_raw, leaders, next_test_port, node_by_id, put_value, router_for, settle_leader,
        single_node, temp_root, three_node_cluster, three_node_cluster_with_timeout, wait_for,
        TestNode, WebhookSink,
    };
    use crate::webhook::sign;
    use axum::http::StatusCode;
    use std::time::Duration;

    const SETTLE: Duration = Duration::from_secs(10);

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib031_offline_replica_recovers_registration_and_removal() {
        let root = temp_root();
        let (n1, n2, mut n3) = three_node_cluster(&root).await;
        let sink = WebhookSink::start().await;
        let c = reqwest::Client::new();
        assert!(wait_for(SETTLE, || n1.is_leader()).await);
        assert!(put_value(&c, &n1.url(), "c", "seed", serde_json::json!({"v": 0}),
            "?w=all").await.is_success());
        n3.kill();
        let response = register(&c, &n1.url(), serde_json::json!({
            "id": "offline", "url": sink.url, "secret": "kept",
        })).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let key = ("c".to_string(), "offline".to_string());
        n3.start();
        assert!(wait_for(SETTLE, || n3.state.as_ref().unwrap().webhooks.get(&key)
            .is_some_and(|s| s.spec.secret.as_deref() == Some("kept"))).await,
            "catch-up must restore the registration without another POST");
        n3.kill();
        let holder = settle_leader(&[&n1, &n2], SETTLE).await.unwrap();
        let leader = node_by_id(&[&n1, &n2], &holder);
        assert_eq!(c.delete(format!("{}/collections/c/webhooks/offline", leader.url()))
            .send().await.unwrap().status(), StatusCode::OK);
        n3.start();
        assert!(wait_for(SETTLE, || {
            let state = n3.state.as_ref().unwrap();
            state.webhooks.get(&key).is_none() && state.db.as_ref().unwrap()
                .existing_collection(crate::webhook::WEBHOOK_PROGRESS_LOG)
                .and_then(|log| log.get("registrations").ok().flatten())
                .is_some_and(|v| v["subscriptions"].as_array().is_some_and(|s| s.is_empty()))
        }).await,
            "catch-up must remove a stale destination without another DELETE");
    }

    async fn put(client: &reqwest::Client, base: &str, key: &str, v: i64) {
        assert!(put_value(client, base, "c", key, serde_json::json!({"v": v}), "").await
            .is_success(), "write of {} failed", key);
    }

    async fn register(
        client: &reqwest::Client,
        base: &str,
        body: serde_json::Value,
    ) -> reqwest::Response {
        client.post(format!("{}/collections/c/webhooks", base)).json(&body).send().await.unwrap()
    }

    async fn state_of(client: &reqwest::Client, base: &str, id: &str) -> serde_json::Value {
        client.get(format!("{}/collections/c/webhooks/{}", base, id))
            .send().await.unwrap().json().await.unwrap()
    }

    fn ops(events: &[serde_json::Value]) -> Vec<String> {
        events.iter().map(|e| e["op"].as_str().unwrap_or("?").to_string()).collect()
    }

    /// The delivery itself, and the proof that it came from here: a signature the endpoint can
    /// recompute from the timestamp and the exact bytes it was sent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_webhook_delivers_committed_changes_under_a_verifiable_signature() {
        let root = temp_root();
        let n = single_node(&root).await;
        let sink = WebhookSink::start().await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "seed", 0).await;
        let created = register(&c, &n.url(), serde_json::json!({
            "id": "orders", "url": sink.url, "secret": "s3cret"})).await;
        assert_eq!(created.status(), StatusCode::CREATED);
        assert!(!created.text().await.unwrap().contains("s3cret"),
            "the secret is the endpoint's proof, and must not be readable back");

        put(&c, &n.url(), "k1", 1).await;
        put(&c, &n.url(), "k1", 2).await;
        c.delete(format!("{}/collections/c/docs/k1", n.url())).send().await.unwrap();

        let events = sink.wait_for_events(3, SETTLE).await;
        assert_eq!(ops(&events), vec!["insert", "update", "delete"], "{:?}", events);

        let delivery = &sink.received()[0];
        let timestamp: u64 = delivery.timestamp.as_ref().expect("a timestamp to sign over")
            .parse().unwrap();
        assert_eq!(delivery.signature.as_deref(),
            Some(sign("s3cret", timestamp, &delivery.raw).as_str()),
            "the endpoint has to be able to recompute the signature from what it received");
        assert!(delivery.delivery.is_some(), "a delivery id is what makes a redelivery detectable");
        assert_eq!(delivery.body["collection"].as_str(), Some("c"));

        let state = state_of(&c, &n.url(), "orders").await;
        assert!(state["delivery"]["position"].as_u64().is_some_and(|p| p > 0), "{}", state);
        assert_eq!(state["delivery"]["failures"].as_u64(), Some(0));
        assert_eq!(state["signed"].as_bool(), Some(true));
    }

    /// An endpoint that fails is retried rather than skipped, and the retry carries the same
    /// delivery id, so a consumer that did receive the first attempt can tell it from a new batch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failing_endpoint_is_retried_with_the_same_delivery() {
        let root = temp_root();
        let mut n = TestNode::new("solo", next_test_port(), &root, "primary");
        n.webhooks = serde_json::json!({"initial_backoff_ms": 50, "max_backoff_ms": 200});
        n.start();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let sink = WebhookSink::start().await;
        let c = reqwest::Client::new();
        put(&c, &n.url(), "seed", 0).await;
        assert_eq!(register(&c, &n.url(), serde_json::json!({"id": "orders", "url": sink.url}))
            .await.status(), StatusCode::CREATED);

        sink.fail_next(2);
        put(&c, &n.url(), "k1", 1).await;

        assert!(wait_for(SETTLE, || sink.received().len() >= 3).await,
            "a failed delivery has to be retried: {} attempts", sink.received().len());
        let attempts = sink.received();
        let ids: Vec<Option<String>> = attempts.iter().take(3).map(|d| d.delivery.clone()).collect();
        assert_eq!(ids[0], ids[1], "a retry is the same batch, so it carries the same id");
        assert_eq!(ids[1], ids[2]);
        assert!(attempts.iter().take(3)
            .all(|d| d.body["events"].as_array().map(|e| e.len()) == Some(1)),
            "the retry must carry the same events, not fewer");

        assert!(wait_for(SETTLE, || n.state.as_ref().and_then(|state| state.webhooks.get(
            &("c".to_string(), "orders".to_string())))
            .is_some_and(|subscription| subscription.delivery.delivered == 1
                && subscription.delivery.failures == 0)).await,
            "the successful attempt did not commit its cursor and clear the retry state");

        let state = state_of(&c, &n.url(), "orders").await;
        assert!(state["delivery"]["attempts"].as_u64().is_some_and(|a| a >= 3), "{}", state);
        assert_eq!(state["delivery"]["failures"].as_u64(), Some(0),
            "an acknowledgement clears the run of failures the backoff is computed from");
        assert_eq!(state["delivery"]["delivered"].as_u64(), Some(1));
    }

    /// The position is on disk for exactly this: a node that comes back does not replay what the
    /// endpoint already acknowledged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delivery_resumes_from_the_durable_position_after_a_restart() {
        let root = temp_root();
        let port = next_test_port();
        let mut n = TestNode::new("solo", port, &root, "primary");
        n.start();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let sink = WebhookSink::start().await;
        let c = reqwest::Client::new();
        put(&c, &n.url(), "seed", 0).await;
        assert_eq!(register(&c, &n.url(), serde_json::json!({"id": "orders", "url": sink.url}))
            .await.status(), StatusCode::CREATED);

        put(&c, &n.url(), "k1", 1).await;
        assert_eq!(sink.wait_for_events(1, SETTLE).await.len(), 1, "nothing was delivered");
        n.kill();

        let mut back = TestNode::new("solo", port, &root, "primary");
        back.start();
        assert!(wait_for(SETTLE, || back.is_leader()).await, "the node has to lead again to deliver");
        put(&c, &back.url(), "k2", 2).await;

        let after = sink.wait_for_events(2, SETTLE).await;
        assert_eq!(after.len(), 2, "a restart must not replay what was acknowledged: {:?}", after);
        assert_eq!(after[1]["key"].as_str(), Some("k2"));
        assert_eq!(state_of(&c, &back.url(), "orders").await["url"].as_str(),
            Some(sink.url.as_str()), "the registration itself has to survive the restart");
    }

    /// `410` is the endpoint saying to stop, not that it is busy. Retrying it forever would be
    /// this node arguing with the answer it asked for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_endpoint_that_is_gone_disables_the_subscription() {
        let root = temp_root();
        let n = single_node(&root).await;
        let sink = WebhookSink::start().await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "seed", 0).await;
        assert_eq!(register(&c, &n.url(), serde_json::json!({"id": "orders", "url": sink.url}))
            .await.status(), StatusCode::CREATED);

        sink.answer_with(410);
        put(&c, &n.url(), "k1", 1).await;

        let mut stopped = serde_json::Value::Null;
        let start = std::time::Instant::now();
        while start.elapsed() < SETTLE {
            stopped = state_of(&c, &n.url(), "orders").await;
            if stopped["delivery"]["disabled"].as_str().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(stopped["delivery"]["disabled"].as_str().is_some_and(|d| d.contains("410")),
            "a 410 has to stop the subscription and say why rather than be retried forever: {}",
            stopped);

        sink.answer_with(200);
        // Registering it again is how an operator resumes, and it keeps the position it stopped at.
        assert_eq!(register(&c, &n.url(), serde_json::json!({"id": "orders", "url": sink.url}))
            .await.status(), StatusCode::CREATED);
        put(&c, &n.url(), "k2", 2).await;
        let events = sink.wait_for_events(2, SETTLE).await;
        assert!(events.iter().any(|e| e["key"] == "k2"), "delivery did not resume: {:?}", events);
    }

    /// A registration names a destination this node will post documents to, so what it accepts is
    /// checked at registration rather than discovered by a subscription that never fires.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_registration_is_checked_bounded_and_refused_on_a_router() {
        let root = temp_root();
        let mut shard = TestNode::new("s1", next_test_port(), &root, "primary");
        shard.webhooks = serde_json::json!({"max_subscriptions": 2});
        shard.start();
        let router = router_for(&root, &[(shard.url(), Vec::new())]).await;
        let c = reqwest::Client::new();
        put(&c, &router.url(), "seed", 0).await;

        let base = shard.url();
        let post = |body: serde_json::Value| register(&c, &base, body);
        assert_eq!(post(serde_json::json!({"id": "has space", "url": "http://x/hook"})).await.status(),
            StatusCode::BAD_REQUEST);
        assert_eq!(post(serde_json::json!({"id": "ok", "url": "not-a-url"})).await.status(),
            StatusCode::BAD_REQUEST);
        assert_eq!(post(serde_json::json!({"id": "ok", "url": "http://x/hook", "ops": "upsert"}))
            .await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(post(serde_json::json!({
            "id": "ok", "url": "http://x/hook", "filter": r#"{"$nope":1}"#})).await.status(),
            StatusCode::BAD_REQUEST,
            "a filter the engine cannot evaluate is refused here, not at the first change");

        for id in ["a", "b"] {
            assert_eq!(post(serde_json::json!({"id": id, "url": "http://x/hook"})).await.status(),
                StatusCode::CREATED);
        }
        assert_eq!(post(serde_json::json!({"id": "c", "url": "http://x/hook"})).await.status(),
            StatusCode::CONFLICT, "the ceiling refuses rather than growing");
        assert_eq!(post(serde_json::json!({"id": "a", "url": "http://y/hook"})).await.status(),
            StatusCode::CREATED, "replacing one adds nothing to the count");

        let routed = register(&c, &router.url(),
            serde_json::json!({"id": "a", "url": "http://x/hook"})).await;
        assert_eq!(routed.status(), StatusCode::NOT_IMPLEMENTED);
        assert!(routed.json::<serde_json::Value>().await.unwrap()["shards"].as_array()
            .is_some_and(|s| !s.is_empty()), "a refusal has to name where the registration belongs");

        assert_eq!(c.delete(format!("{}/collections/c/webhooks/a", shard.url()))
            .send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.get(format!("{}/collections/c/webhooks/a", shard.url()))
            .send().await.unwrap().status(), StatusCode::NOT_FOUND);
    }

    // A reachable follower reconciles the committed catalogue before the request returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_registration_reaches_the_rest_of_the_group() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let sink = WebhookSink::start().await;
        let c = reqwest::Client::new();
        assert!(wait_for(SETTLE, || leaders(&[&n1, &n2, &n3]) == vec!["n1".to_string()]).await,
            "the test needs a settled leader to register against");

        put(&c, &n1.url(), "seed", 0).await;
        let created = register(&c, &n1.url(), serde_json::json!({
            "id": "orders", "url": sink.url, "secret": "s3cret"})).await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let body: serde_json::Value = created.json().await.unwrap();
        assert_eq!(body["replicated_to"].as_array().map(|r| r.len()), Some(2), "{}", body);
        assert!(body.get("unreachable").is_none(), "{}", body);

        for follower in [&n2, &n3] {
            let held = c.get(format!("{}/collections/c/webhooks/orders", follower.url()))
                .send().await.unwrap();
            assert_eq!(held.status(), StatusCode::OK,
                "{} did not take the registration", follower.url());
            let state: serde_json::Value = held.json().await.unwrap();
            assert_eq!(state["url"].as_str(), Some(sink.url.as_str()));
            assert_eq!(state["signed"].as_bool(), Some(true),
                "a follower that cannot sign would deliver unverifiable batches once elected");
        }

        // A replica delivers nothing while it is one, so its own view has to say so.
        let listed: serde_json::Value = c.get(format!("{}/collections/c/webhooks", n2.url()))
            .send().await.unwrap().json().await.unwrap();
        assert_eq!(listed["delivering"].as_bool(), Some(false), "{}", listed);
        assert_eq!(c.post(format!("{}/collections/c/webhooks", n2.url()))
            .json(&serde_json::json!({"id": "direct", "url": sink.url}))
            .send().await.unwrap().status(), StatusCode::FORBIDDEN,
            "the group has one node that delivers, and this is not it");

        assert_eq!(c.delete(format!("{}/collections/c/webhooks/orders", n1.url()))
            .send().await.unwrap().status(), StatusCode::OK);
        for follower in [&n2, &n3] {
            assert_eq!(c.get(format!("{}/collections/c/webhooks/orders", follower.url()))
                .send().await.unwrap().status(), StatusCode::NOT_FOUND,
                "a removal has to reach the group too, or a failover resurrects the destination");
        }
    }

    /// IB-026: a registration is durable, so a restart does not end one the way it ends a connection.
    /// Without a re-check, a key removed from the config kept posting documents across restarts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_registration_stops_delivering_once_the_key_that_created_it_is_gone() {
        use crate::auth::API_KEY_HEADER;

        let root = temp_root();
        let port = next_test_port();
        let mut n = TestNode::new("solo", port, &root, "primary");
        n.auth = serde_json::json!({"api_keys": ["old", "new"]});
        n.start();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let sink = WebhookSink::start().await;
        let c = reqwest::Client::new();
        let write = |base: String, key: &'static str, doc: &'static str| c
            .put(format!("{}/collections/c/docs/{}", base, doc)).header(API_KEY_HEADER, key)
            .json(&serde_json::json!({"value": {"v": 1}})).send();

        assert!(write(n.url(), "old", "seed").await.unwrap().status().is_success());
        assert_eq!(c.post(format!("{}/collections/c/webhooks", n.url()))
            .header(API_KEY_HEADER, "old")
            .json(&serde_json::json!({"id": "orders", "url": sink.url}))
            .send().await.unwrap().status(), StatusCode::CREATED);

        assert!(write(n.url(), "old", "before").await.unwrap().status().is_success());
        assert_eq!(sink.wait_for_events(1, SETTLE).await.len(), 1, "nothing was delivered");
        n.kill();

        // The registration comes back off disk; the key that created it does not come back at all.
        let mut back = TestNode::new("solo", port, &root, "primary");
        back.auth = serde_json::json!({"api_keys": ["new"]});
        back.start();
        assert!(wait_for(SETTLE, || back.is_leader()).await, "the node has to lead to deliver");
        assert!(write(back.url(), "new", "after").await.unwrap().status().is_success());

        let held = format!("{}/collections/c/webhooks/orders", back.url());
        let mut state = serde_json::Value::Null;
        let deadline = std::time::Instant::now() + SETTLE;
        while std::time::Instant::now() < deadline {
            state = c.get(&held).header(API_KEY_HEADER, "new")
                .send().await.unwrap().json().await.unwrap();
            if state["delivery"]["disabled"].as_str().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(state["delivery"]["disabled"].as_str()
            .is_some_and(|d| d.contains("no longer accepted")),
            "the registration has to stop and say why: {}", state);
        assert_eq!(sink.received().len(), 1,
            "a committed change reached the endpoint under a credential the node no longer holds");
    }

    /// IB-030: the acknowledged cursor belongs to the group, and every registered replica keeps the feed
    /// window it resumes inside. The boundary asserted is at-least-once, which is what failover promises.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn a_promoted_replica_resumes_webhook_delivery_after_the_last_acknowledged_event() {
        let root = temp_root();
        let (mut n1, mut n2, mut n3) = three_node_cluster_with_timeout(&root, 3).await;
        let sink = WebhookSink::start().await;
        let c = reqwest::Client::new();
        assert!(wait_for(SETTLE, || n1.is_leader()).await, "n1 did not settle as leader");

        assert!(put_value(&c, &n1.url(), "c", "seed", serde_json::json!({"v": 0}),
            "?w=majority").await.is_success());
        assert_eq!(register(&c, &n1.url(), serde_json::json!({
            "id": "orders", "url": sink.url,
        })).await.status(), StatusCode::CREATED);
        for follower in [&n2, &n3] {
            let feed_active = follower.state.as_ref().and_then(|state| state.db.as_ref())
                .and_then(|db| db.existing_collection("c"))
                .is_some_and(|col| col.changefeed.active());
            assert!(feed_active, "{} did not pin the feed when it took the registration", follower.url());
        }

        put(&c, &n1.url(), "before", 1).await;
        let first = sink.wait_for_events(1, SETTLE).await;
        assert_eq!(first.iter().filter_map(|event| event["key"].as_str()).collect::<Vec<_>>(),
            vec!["before"], "the initial event was not delivered exactly once: {:?}", first);
        // A promotion resumes from the group's cursor, so the precondition is the majority commit
        // rather than the deliverer's own copy of it (IB-041).
        let key = ("c".to_string(), "orders".to_string());
        let acknowledged = |node: &TestNode| node.state.as_ref()
            .map_or(0, |state| crate::webhook::replicated_position(state, &key));
        assert!(wait_for(SETTLE, || [&n1, &n2, &n3].into_iter()
            .filter(|node| acknowledged(node) > 0).count() >= 2).await,
            "the acknowledged position did not reach a majority");

        n1.kill();
        // Settled rather than merely counted: a node that is leading at the moment of the pick and
        // steps down before the write refuses it as a replica would.
        let holder = settle_leader(&[&n2, &n3], Duration::from_secs(30)).await
            .expect("the surviving quorum did not settle on one leader");
        let promoted = node_by_id(&[&n2, &n3], &holder);

        // The write the assertion is about reaches the feed once, so a lost response is not
        // retried into a second change that reads back as a redelivery (IB-041).
        let deadline = std::time::Instant::now() + SETTLE;
        let mut accepted = put_value(&c, &promoted.url(), "c", "after",
            serde_json::json!({"v": 2}), "").await;
        while !accepted.is_success() {
            assert!(std::time::Instant::now() < deadline,
                "the promoted leader never accepted the write the assertion is about: {}", accepted);
            tokio::time::sleep(Duration::from_millis(100)).await;
            if get_raw(&c, &promoted.url(), "c", "after").await {
                break;
            }
            accepted = put_value(&c, &promoted.url(), "c", "after",
                serde_json::json!({"v": 2}), "").await;
        }

        assert!(wait_for(SETTLE, || sink.events().iter()
            .any(|event| event["key"].as_str() == Some("after"))).await,
            "the promoted leader did not deliver the write it accepted: {:?}", sink.events());
        tokio::time::sleep(Duration::from_millis(500)).await;

        // At-least-once across a promotion: the batch straddling the cursor may be sent again, so
        // `before` is bounded rather than exact and repeats the `lsn` it is deduped on.
        let events = sink.events();
        let keys: Vec<&str> = events.iter().filter_map(|event| event["key"].as_str()).collect();
        assert_eq!(keys.iter().filter(|key| **key == "after").count(), 1,
            "an event the promoted leader acknowledged was delivered twice: {:?}", events);
        assert!(keys.split_last().is_some_and(|(last, earlier)|
                *last == "after" && earlier.iter().all(|key| *key == "before")),
            "promotion skipped or reordered around the acknowledged cursor: {:?}", events);
        let before_lsns: std::collections::HashSet<u64> = events.iter()
            .filter(|event| event["key"].as_str() == Some("before"))
            .filter_map(|event| event["lsn"].as_u64()).collect();
        assert_eq!(before_lsns.len(), 1,
            "a redelivery has to carry the lsn the endpoint dedups on: {:?}", events);
        let delivery = state_of(&c, &promoted.url(), "orders").await;
        assert_eq!(delivery["delivery"]["gaps"].as_u64(), Some(0), "{}", delivery);

        n2.kill();
        n3.kill();
    }
}
