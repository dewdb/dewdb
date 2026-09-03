//! Authentication and latency-recording middleware, and the gate on collection names.

use crate::auth::{authorize, AuthOutcome, API_KEY_HEADER, INTERNAL_SECRET_HEADER};
use crate::consensus::config::{is_system_collection, valid_collection_name, MAX_COLLECTION_NAME_LEN};
use crate::model::err_json;
use crate::state::AppState;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tracing::warn;

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

    if path == "/metrics" {
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

pub async fn auth_middleware(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = req.uri().path().to_string();

    let headers = req.headers();
    let secret = headers.get(INTERNAL_SECRET_HEADER).and_then(|v| v.to_str().ok()).map(str::to_string);
    let api_key = headers.get(API_KEY_HEADER).and_then(|v| v.to_str().ok()).map(str::to_string);
    let authorization = headers.get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok()).map(str::to_string);

    let outcome = authorize(
        &path,
        &state.config.auth,
        secret.as_deref(),
        api_key.as_deref(),
        authorization.as_deref(),
    );

    match outcome {
        AuthOutcome::Allow => next.run(req).await,
        AuthOutcome::Deny(reason) => {
            warn!(target: "auth", path = %path, reason, "Rejected unauthenticated request");
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
    use crate::test_support::{cleanup, single_node, temp_root};
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

        drop(n);
        cleanup(&root).await;
    }
}
