//! Authentication and latency-recording middleware, and the gates on collection names.

use crate::auth::{authorize, AuthOutcome, API_KEY_HEADER, INTERNAL_SECRET_HEADER};
use crate::consensus::config::{is_system_collection, valid_collection_name, MAX_COLLECTION_NAME_LEN};
use crate::model::err_json;
use crate::state::AppState;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tracing::warn;

/// The matched-path template, which is what `metrics_middleware` compares against.
const CHANGES_ROUTE: &str = "/collections/:name/changes";

struct ActiveRequest<'a>(&'a crate::metrics::Metrics);

impl Drop for ActiveRequest<'_> {
    fn drop(&mut self) {
        self.0.end_request();
    }
}

pub async fn metrics_middleware(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = req.extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "<unmatched>".to_string());

    // A change stream lives as long as its subscriber, so timing it would fold minutes into the
    // latency EWMA that load-aware routing reads and take this node out of replica reads for good.
    if path == "/metrics" || path == CHANGES_ROUTE {
        return next.run(req).await;
    }

    let key = format!("{} {}", req.method(), path);
    state.metrics.begin_request();
    let _active = ActiveRequest(&state.metrics);
    let started = std::time::Instant::now();
    let response = next.run(req).await;
    let nanos = started.elapsed().as_nanos() as u64;

    state.metrics.observe(key, nanos, response.status().is_server_error() || response.status().is_client_error());
    response
}

/// The one gate on collection names, in place of a check in each of the eleven
/// `/collections/:name` handlers. It replaces `Path` in their signatures rather than sitting in a
/// layer, because a layer judges the still-encoded URI while the handler acts on the decoded name:
/// `%5Fconfig` passed the reserved check and reached `_config`, and `..%2F..%2Fx` reached a
/// directory outside the data root. Internal replication carries a name in a body, not on a path,
/// and is gated at `Database::collection_dir` instead.
pub struct CollectionPath<T>(pub T);

/// Which captured segment is the collection: the whole capture on `/:name` routes, the first of
/// two on `/:name/docs/:id`.
pub trait NamedCollection {
    fn collection(&self) -> &str;
}

impl NamedCollection for String {
    fn collection(&self) -> &str { self }
}

impl NamedCollection for (String, String) {
    fn collection(&self) -> &str { &self.0 }
}

#[axum::async_trait]
impl<S, T> FromRequestParts<S> for CollectionPath<T>
where
    S: Send + Sync,
    T: NamedCollection + serde::de::DeserializeOwned + Send,
{
    type Rejection = axum::response::Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(captured) = Path::<T>::from_request_parts(parts, state).await
            .map_err(IntoResponse::into_response)?;
        let name = captured.collection();

        // A system log is a consensus structure, and a client writing to one moves the quorum.
        if is_system_collection(name) {
            return Err(err_json(StatusCode::FORBIDDEN, format!(
                "'{}' is a system collection; names beginning with '_' are reserved", name)));
        }
        if !valid_collection_name(name) {
            return Err(err_json(StatusCode::BAD_REQUEST, format!(
                "invalid collection name: expected 1-{} of [A-Za-z0-9._-]", MAX_COLLECTION_NAME_LEN)));
        }
        Ok(Self(captured))
    }
}

/// The second gate on a client-supplied name, after `CollectionPath` has judged its shape.
/// `get_collection` opens on miss, so resolving with it let a typo in a read create a directory, a
/// commit task and a map entry that nothing evicts (bugs.md `H15`).
pub fn client_collection(
    state: &AppState,
    name: &str,
) -> Result<std::sync::Arc<crate::storage::Collection>, axum::response::Response> {
    let db = state.db.as_ref()
        .ok_or_else(|| err_json(StatusCode::INTERNAL_SERVER_ERROR, "No database on this node".to_string()))?;
    match db.lookup_collection(name) {
        Ok(Some(col)) => Ok(col),
        Ok(None) => Err(err_json(StatusCode::NOT_FOUND,
            format!("collection '{}' does not exist", name))),
        Err(e) => Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

pub async fn auth_middleware(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = req.uri().path().to_string();
    let method = req.method().as_str().to_string();

    let headers = req.headers();
    let secret = headers.get(INTERNAL_SECRET_HEADER).and_then(|v| v.to_str().ok()).map(str::to_string);
    let api_key = headers.get(API_KEY_HEADER).and_then(|v| v.to_str().ok()).map(str::to_string);
    let authorization = headers.get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok()).map(str::to_string);

    let outcome = authorize(
        &path,
        &method,
        &state.config.auth,
        secret.as_deref(),
        api_key.as_deref(),
        authorization.as_deref(),
    );

    match outcome {
        AuthOutcome::Allow => next.run(req).await,
        AuthOutcome::Deny(reason) => {
            warn!(target: "auth", path = %path, method = %method, reason,
                "Rejected unauthenticated request");
            err_json(StatusCode::UNAUTHORIZED, reason.to_string())
        }
    }
}

/// Applies `chaos`'s link faults at the receiving end, where the sender is known from its
/// `NODE_HEADER`. A cut hangs rather than answering, so the sender fails the way a partition makes
/// it fail -- on its own timeout -- instead of learning that the peer is up and refusing.
#[cfg(test)]
pub async fn chaos_middleware(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let from = req.headers().get(crate::auth::NODE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    if let Some(from) = from {
        match crate::chaos::lookup(&from, &state.own_url()) {
            Some(crate::chaos::Fault::Cut) => std::future::pending::<()>().await,
            Some(crate::chaos::Fault::Delay(by)) => tokio::time::sleep(by).await,
            None => {},
        }
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use crate::auth::API_KEY_HEADER;
    use crate::test_support::{next_test_port, single_node, temp_root, TestNode};
    use axum::http::StatusCode;

    /// C24 and C25: the gate used to read the still-encoded URI and the handler the decoded name,
    /// so `%5F` reached the config log and `..%2F..%2F` reached a directory outside the data root.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_encoded_name_is_judged_as_the_name_the_handler_would_use() {
        let root = temp_root();
        let n = single_node(&root).await;
        let c = reqwest::Client::new();

        for name in ["_config", "%5Fconfig", "%5f%63onfig"] {
            let r = c.put(format!("{}/collections/{}/docs/k1", n.url(), name))
                .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::FORBIDDEN, "{} reached the config log", name);
        }

        for name in ["..%2Fescaped", "..%2F..%2Fescaped", "%2Fabs", "a%00b", ".hidden", "t.tmp", "t.old", ""] {
            let r = c.put(format!("{}/collections/{}/docs/k1", n.url(), name))
                .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap();
            assert!(r.status().is_client_error(), "{} was accepted as a collection name", name);
        }

        // One level up from the node's data directory is still inside this run's own root.
        assert!(!root.join("escaped").exists(),
            "a name off the URL path became a directory outside the data root");

        let listed = c.get(format!("{}/collections", n.url())).send().await.unwrap()
            .json::<serde_json::Value>().await.unwrap();
        assert!(listed["collections"].as_array().unwrap().is_empty(), "{}", listed);

        assert_eq!(c.put(format!("{}/collections/app.events-1/docs/k1", n.url()))
            .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap().status(),
            StatusCode::CREATED, "an ordinary name is still a name");

    }

    /// L3: the client key and the operator key were the same key, so anything that could write a
    /// document could also drop the collection and repartition the ring.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_admin_key_is_required_for_topology_and_drops_but_not_for_documents() {
        let root = temp_root();
        let mut n = TestNode::new("solo", next_test_port(), &root, "primary");
        n.auth = serde_json::json!({"api_keys": ["client"], "admin_keys": ["root"]});
        n.start();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let c = reqwest::Client::new();
        let doc = format!("{}/collections/c/docs/k1", n.url());
        let collection = format!("{}/collections/c", n.url());
        let ring = format!("{}/cluster/ring", n.url());
        let body = serde_json::json!({"value": {"v": 1}});

        assert_eq!(c.put(&doc).header(API_KEY_HEADER, "client").json(&body)
            .send().await.unwrap().status(),
            StatusCode::CREATED, "the client key still writes documents");
        assert_eq!(c.delete(&doc).header(API_KEY_HEADER, "client").send().await.unwrap().status(),
            StatusCode::OK, "deleting a document is not an admin action");

        assert_eq!(c.delete(&collection).header(API_KEY_HEADER, "client").send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED, "the client key dropped a collection");
        assert_eq!(c.post(&ring).header(API_KEY_HEADER, "client")
            .json(&serde_json::json!({"shards": []})).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED, "the client key rewrote the ring");
        assert_eq!(c.delete(&collection).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED, "no key at all dropped a collection");

        // The admin key reaches both tiers, so one credential inspects and then drops.
        assert_eq!(c.put(&doc).header(API_KEY_HEADER, "root").json(&body)
            .send().await.unwrap().status(), StatusCode::CREATED);
        assert_eq!(c.delete(&collection).header(API_KEY_HEADER, "root").send().await.unwrap().status(),
            StatusCode::OK);

    }

    /// A routed drop is the one admin route a node calls on another node, so a router's
    /// `upstream_api_key` has to be an admin key wherever the shards set `admin_keys`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_router_forwards_a_drop_with_its_upstream_key() {
        let root = temp_root();
        let mut shard = TestNode::new("s1", next_test_port(), &root, "primary");
        shard.auth = serde_json::json!({"api_keys": ["client"], "admin_keys": ["root"]});
        shard.start();

        let mut router = TestNode::new("router", next_test_port(), &root, "primary");
        router.role = "router".to_string();
        router.shard_map = vec![(shard.url(), Vec::new())];
        router.auth = serde_json::json!({
            "api_keys": ["client"], "admin_keys": ["root"], "upstream_api_key": "root"});
        router.start();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let c = reqwest::Client::new();
        let collection = format!("{}/collections/c", router.url());

        assert_eq!(c.put(format!("{}/collections/c/docs/k1", router.url()))
            .header(API_KEY_HEADER, "client").json(&serde_json::json!({"value": {"v": 1}}))
            .send().await.unwrap().status(),
            StatusCode::CREATED);
        assert_eq!(c.delete(&collection).header(API_KEY_HEADER, "client").send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED, "the router must gate the drop before forwarding it");

        let r = c.delete(&collection).header(API_KEY_HEADER, "root").send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let body = r.json::<serde_json::Value>().await.unwrap();
        assert_eq!(body["shards"][0]["status"].as_u64(), Some(200),
            "the shard refused the router's forwarded credential: {}", body);

    }
}
