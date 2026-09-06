//! Client-facing wire types: the public REST contract.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CreateDoc {
    pub value: serde_json::Value,
}

#[derive(Deserialize)]
pub struct BulkDoc {
    #[serde(default)]
    pub id: Option<String>,
    pub value: serde_json::Value,
}

/// Upper bound on `?limit`. A page is preallocated from this number, so an unbounded one is an
/// allocation the process aborts on rather than a slow query. Callers page with `cursor` instead.
pub const MAX_QUERY_LIMIT: usize = 10_000;

pub const DEFAULT_QUERY_LIMIT: usize = 100;

#[derive(Deserialize)]
pub struct QueryParams {
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
    pub filter: Option<String>,
    pub read: Option<String>,
    pub cursor: Option<String>,
    pub sort: Option<String>,
    pub fields: Option<String>,
    /// Return each row's key alongside it. The cross-shard merge needs them to order ties and to
    /// build the next cursor, so the router always asks; a client may.
    pub keys: Option<bool>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct QueryPage {
    pub items: Vec<serde_json::Value>,
    pub next_cursor: Option<String>,
    /// Parallel to `items`, and only present when the request asked for it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<String>,
}

#[derive(Deserialize)]
pub struct AggregateParams {
    pub start: Option<String>,
    pub end: Option<String>,
    pub filter: Option<String>,
    pub read: Option<String>,
    /// Dotted paths to group by. Absent is one group over everything the filter matched.
    pub group: Option<String>,
    /// `count`, `sum:field`, `avg:field`, `min:field`, `max:field`. Absent is `count`.
    pub metrics: Option<String>,
}

#[derive(Deserialize)]
pub struct ReadParams {
    pub read: Option<String>,
}

#[derive(Deserialize)]
pub struct MetricsParams {
    pub format: Option<String>,
}

pub fn err_json(status: StatusCode, msg: String) -> axum::response::Response {
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}
