//! Webhook subscription administration.
//!
//! A registration is node-local durable state rather than a replicated log entry, so the leader
//! that accepts one also pushes it to the rest of its group: whichever node leads next has to hold
//! the same destinations, or a failover would quietly stop delivering.

use crate::api::middleware::{client_collection, CollectionPath};
use crate::model::err_json;
use crate::state::AppState;
use crate::util::encode_path_segment;
use crate::webhook::{
    valid_webhook_id, valid_webhook_url, WebhookKey, WebhookSpec, MAX_WEBHOOK_ID_LEN,
    MAX_WEBHOOK_URL_LEN,
};
use axum::extract::{Query, State};
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

/// Best effort, and reported as such: a peer that missed the registration keeps delivering nothing
/// until it is reachable again, and an operator that is told which one can re-run the request.
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
            Ok(r) if r.status().is_success() || r.status() == StatusCode::NOT_FOUND =>
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

    let spec = WebhookSpec {
        id: payload.id.clone(),
        collection: col_name.clone(),
        url: payload.url.clone(),
        secret: payload.secret.clone(),
        filter: payload.filter.clone(),
        ops: payload.ops.clone(),
    };
    // From here rather than from the start of the log: a new subscription is "from now on", the
    // way a change stream that names no position is.
    let subscription = match state.webhooks.upsert(spec, col.applied_lsn(), state.config.webhooks.max_subscriptions) {
        Ok(subscription) => subscription,
        Err(max) => return err_json(StatusCode::CONFLICT, format!(
            "this node already holds the maximum of {} webhook subscriptions", max)),
    };

    let mut body = subscription.public();
    if !params.local {
        let forwarded = serde_json::json!({
            "id": payload.id, "url": payload.url, "secret": payload.secret,
            "filter": payload.filter, "ops": payload.ops,
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
    let removed = state.webhooks.remove(&key);
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
        leaders, next_test_port, put_value, router_for, single_node, temp_root, three_node_cluster,
        wait_for, TestNode, WebhookSink,
    };
    use crate::webhook::sign;
    use axum::http::StatusCode;
    use std::time::Duration;

    const SETTLE: Duration = Duration::from_secs(10);

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

    /// The registration is node-local, so the leader pushes it to the rest of the group: whichever
    /// node leads next has to hold the same destinations, or a failover stops delivering silently.
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
}
