//! Authentication and latency-recording middleware.

use crate::auth::{authorize, AuthOutcome, API_KEY_HEADER, INTERNAL_SECRET_HEADER};
use crate::model::err_json;
use crate::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use tracing::warn;

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
    let started = std::time::Instant::now();
    let response = next.run(req).await;
    let nanos = started.elapsed().as_nanos() as u64;

    state.metrics.observe(key, nanos, response.status().is_server_error() || response.status().is_client_error());
    response
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
