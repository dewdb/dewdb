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
}

#[derive(Serialize, Deserialize)]
pub struct QueryPage {
    pub items: Vec<serde_json::Value>,
    pub next_cursor: Option<String>,
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
