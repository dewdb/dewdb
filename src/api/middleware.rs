//! Authentication and latency-recording middleware.

use crate::auth::{authorize, AuthOutcome, API_KEY_HEADER, INTERNAL_SECRET_HEADER};
use crate::consensus::config::is_system_collection;
use crate::model::err_json;
use crate::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
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

/// The one gate on reserved names, rather than a check in each of the eleven `/collections/:name`
/// handlers: a system log is a consensus structure, and a client writing to one moves the quorum.
/// Internal replication reaches it by collection name in a body, not by path, so it is unaffected.
pub async fn reserved_name_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let name = req.uri().path()
        .strip_prefix("/collections/")
        .map(|rest| rest.split('/').next().unwrap_or(rest));

    match name {
        Some(name) if is_system_collection(name) => err_json(
            StatusCode::FORBIDDEN,
            format!("'{}' is a system collection; names beginning with '_' are reserved", name)),
        _ => next.run(req).await,
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
