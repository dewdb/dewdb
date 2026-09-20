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
    pub max_docs: Option<usize>,
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
    pub keys: Option<KeyMode>,
}

impl QueryParams {
    /// Absent is the same as `keys=false`: no key anywhere in the answer.
    pub fn key_mode(&self) -> KeyMode {
        self.keys.unwrap_or(KeyMode::None)
    }
}

/// Which of the two shapes a page reports its keys in, parsed once at the edge so the paths that
/// build a page match on a mode rather than re-reading a query string.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeyMode {
    /// `keys=false`, or omitted: the keys stay out of the response.
    #[default]
    None,
    /// `keys=true`, 1.0's shape: a `keys` array parallel to `items`.
    Parallel,
    /// `keys=embed`: each item becomes `{"id": …, "value": <stored value>}` and there is no array.
    Embedded,
}

impl<'de> Deserialize<'de> for KeyMode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // 1.0 parsed this as a `bool`, which accepted `true` and `false` and nothing else. `embed`
        // joins that set rather than widening it: every spelling that was a `400` still is.
        match String::deserialize(deserializer)?.as_str() {
            "true" => Ok(KeyMode::Parallel),
            "false" => Ok(KeyMode::None),
            "embed" => Ok(KeyMode::Embedded),
            other => Err(serde::de::Error::custom(format!(
                "keys must be `true`, `false` or `embed`, not `{}`", other))),
        }
    }
}

/// Projects a row's key into the response without touching the document it belongs to. `{id,value}`
/// rather than `{id, ...fields}` because a stored value is arbitrary JSON: it may be an array or a
/// scalar with nowhere to put an id, and an object may already have an `id` of its own.
pub fn embed_key(key: &str, value: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"id": key, "value": value})
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
    /// Documents the aggregation may read on each shard. Per shard rather than per cluster: it
    /// bounds one node's walk, which is the thing that occupies a blocking thread.
    pub max_docs: Option<usize>,
    /// Accept the totals over what the budget bought instead of a refusal when it runs out.
    pub partial: Option<bool>,
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
