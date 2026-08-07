//! Document endpoints.

use super::write::{local_patch, local_write, local_write_batch};
use crate::cluster::router::{
    parse_read_pref, router_forward_write, router_read_doc, router_query, bulk_router_forward,
    passthrough, ForwardMethod,
};
use crate::json::{parse_fields, project};
use crate::model::{err_json, BulkDoc, CreateDoc, QueryPage, QueryParams, ReadParams};
use crate::query::{compare_by_sort, matches_filter, parse_sort, Filter};
use crate::replication::{parse_write_concern, wc_query_string, WriteConcernParams, DEFAULT_WTIMEOUT_MS};
use crate::cluster::ownership::Ownership;
use crate::state::AppState;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::io;
use std::time::Duration;
use uuid::Uuid;


/// Refuses a key this node must not hold. `Elsewhere` names the owner so a router on a stale view
/// can retry rather than write a second copy; `Moving` is a wait, not an error, because the key is
/// mid-handover and will be writable at its new owner shortly.
fn wrong_owner(state: &AppState, collection: &str, key: &str) -> Option<axum::response::Response> {
    match state.ownership(collection, key)? {
        Ownership::Ours => None,
        Ownership::Elsewhere(owner) => Some((StatusCode::CONFLICT, Json(serde_json::json!({
            "error": "this shard does not own that key",
            "owner": owner,
            "key": key,
        }))).into_response()),
        Ownership::Moving { to } => Some((
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            Json(serde_json::json!({
                "error": "key is being handed over and is briefly read-only",
                "moving_to": to,
                "key": key,
            })),
        ).into_response()),
    }
}

/// A server-generated id only has to be unique, so when this shard does not own the first one it
/// draws another. Expected draws equal the shard count; the bound is there for the pathological
/// case where the ring says this node owns nothing at all.
fn own_id(state: &AppState, collection: &str, first: String) -> Option<String> {
    let mut candidate = first;
    for _ in 0..64 {
        match state.ownership(collection, &candidate) {
            None | Some(Ownership::Ours) => return Some(candidate),
            _ => candidate = Uuid::new_v4().to_string(),
        }
    }
    None
}

pub async fn create_doc(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
    Query(wcp): Query<WriteConcernParams>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    let id = Uuid::new_v4().to_string();

    if state.config.role == "router" {
        let wc_query = wc_query_string(&wcp);
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Put, Some(&payload), &wc_query).await {
            Ok(reply) => passthrough(reply),
            Err(resp) => resp,
        };
    }

    // The id is ours to choose, so choose one this shard owns rather than refuse the write.
    let id = match own_id(&state, &col_name, id) {
        Some(id) => id,
        None => return err_json(StatusCode::SERVICE_UNAVAILABLE,
            "could not generate a key this shard owns; retry".to_string()),
    };

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    match local_write(&state, &col_name, id.clone(), Some(payload.value), wc, wtimeout).await {
        Ok(o) if o.met => (StatusCode::CREATED, Json(serde_json::json!({"id": id, "status": "created"}))).into_response(),
        Ok(o) => (StatusCode::ACCEPTED, Json(serde_json::json!({
            "id": id,
            "status": "created",
            "warning": "write concern not met",
            "acks": o.acks,
            "required": o.required,
        }))).into_response(),
        Err(resp) => resp,
    }
}

pub async fn put_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Query(wcp): Query<WriteConcernParams>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if state.config.role == "router" {
        let wc_query = wc_query_string(&wcp);
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Put, Some(&payload), &wc_query).await {
            Ok(reply) => passthrough(reply),
            Err(resp) => resp,
        };
    }

    if let Some(refusal) = wrong_owner(&state, &col_name, &id) {
        return refusal;
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    match local_write(&state, &col_name, id.clone(), Some(payload.value), wc, wtimeout).await {
        Ok(o) if o.met => {
            let status = if o.existed { StatusCode::OK } else { StatusCode::CREATED };
            let label = if o.existed { "replaced" } else { "created" };
            (status, Json(serde_json::json!({"id": id, "status": label}))).into_response()
        },
        Ok(o) => (StatusCode::ACCEPTED, Json(serde_json::json!({
            "id": id,
            "status": if o.existed { "replaced" } else { "created" },
            "warning": "write concern not met",
            "acks": o.acks,
            "required": o.required,
        }))).into_response(),
        Err(resp) => resp,
    }
}

pub async fn bulk_create_docs(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
    Query(wcp): Query<WriteConcernParams>,
    Json(payload): Json<Vec<BulkDoc>>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if payload.is_empty() {
        return err_json(StatusCode::BAD_REQUEST, "bulk request must contain at least one document".to_string());
    }

    let wc_query = wc_query_string(&wcp);

    if state.config.role == "router" {
        return bulk_router_forward(&state, &col_name, payload, &wc_query).await;
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    let ids: Vec<String> = payload.iter()
        .map(|d| d.id.clone().unwrap_or_else(|| Uuid::new_v4().to_string()))
        .collect();

    // Whole batch or none. A partial bulk would be harder to reason about than a retry, and the
    // router splits by owner anyway, so a rejected batch means its ring is stale.
    for id in &ids {
        if let Some(refusal) = wrong_owner(&state, &col_name, id) {
            return refusal;
        }
    }
    let items: Vec<(String, serde_json::Value)> = ids.iter().cloned()
        .zip(payload.into_iter().map(|d| d.value))
        .collect();

    match local_write_batch(&state, &col_name, items, wc, wtimeout).await {
        Ok(outcomes) => {
            let results: Vec<serde_json::Value> = ids.into_iter().zip(outcomes.into_iter()).map(|(id, o)| {
                if o.met {
                    serde_json::json!({"id": id, "status": "created"})
                } else {
                    serde_json::json!({
                        "id": id,
                        "status": "created",
                        "warning": "write concern not met",
                        "acks": o.acks,
                        "required": o.required,
                    })
                }
            }).collect();
            (StatusCode::CREATED, Json(serde_json::json!({"results": results}))).into_response()
        }
        Err(resp) => resp,
    }
}

pub async fn get_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Query(rp): Query<ReadParams>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        let pref = parse_read_pref(rp.read.as_deref());
        return router_read_doc(&state, &col_name, &id, pref).await;
    }

    // Only a wrong owner redirects. A key mid-handover still reads correctly here: the source is
    // the owner until the flip, and refusing would take reads down for no reason.
    if let Some(Ownership::Elsewhere(owner)) = state.ownership(&col_name, &id) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({
            "error": "this shard does not own that key", "owner": owner, "key": id,
        }))).into_response();
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let key = id.clone();
    let col_clone = col.clone();

    match tokio::task::spawn_blocking(move || col_clone.get(&key)).await {
        Ok(Ok(Some(val))) => (StatusCode::OK, Json(val)).into_response(),
        Ok(Ok(None)) => err_json(StatusCode::NOT_FOUND, "not found".to_string()),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn update_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Query(wcp): Query<WriteConcernParams>,
    Json(payload): Json<CreateDoc>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if payload.value.is_null() {
        return err_json(StatusCode::BAD_REQUEST, "PATCH body must not be null; use DELETE to remove a document".to_string());
    }

    if state.config.role == "router" {
        let wc_query = wc_query_string(&wcp);
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Patch, Some(&payload), &wc_query).await {
            Ok(reply) => passthrough(reply),
            Err(resp) => resp,
        };
    }

    if let Some(refusal) = wrong_owner(&state, &col_name, &id) {
        return refusal;
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    match local_patch(&state, &col_name, id.clone(), payload.value, wc, wtimeout).await {
        Ok(None) => err_json(StatusCode::NOT_FOUND, "not found".to_string()),
        Ok(Some(o)) if o.met => (StatusCode::OK, Json(serde_json::json!({"id": id, "status": "updated"}))).into_response(),
        Ok(Some(o)) => (StatusCode::ACCEPTED, Json(serde_json::json!({
            "id": id,
            "status": "updated",
            "warning": "write concern not met",
            "acks": o.acks,
            "required": o.required,
        }))).into_response(),
        Err(resp) => resp,
    }
}

pub async fn delete_doc(
    State(state): State<AppState>,
    AxumPath((col_name, id)): AxumPath<(String, String)>,
    Query(wcp): Query<WriteConcernParams>,
) -> impl axum::response::IntoResponse {
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if state.config.role == "router" {
        let wc_query = wc_query_string(&wcp);
        return match router_forward_write(&state, &col_name, &id, ForwardMethod::Delete, None, &wc_query).await {
            Ok(reply) => passthrough(reply),
            Err(resp) => resp,
        };
    }

    if let Some(refusal) = wrong_owner(&state, &col_name, &id) {
        return refusal;
    }

    let wc = parse_write_concern(wcp.w.as_deref());
    let wtimeout = Duration::from_millis(wcp.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    match local_write(&state, &col_name, id.clone(), None, wc, wtimeout).await {
        Ok(o) if o.met => (StatusCode::OK, Json(serde_json::json!({"status": "deleted", "existed": o.existed}))).into_response(),
        Ok(o) => (StatusCode::ACCEPTED, Json(serde_json::json!({
            "status": "deleted",
            "existed": o.existed,
            "warning": "write concern not met",
            "acks": o.acks,
            "required": o.required,
        }))).into_response(),
        Err(resp) => resp,
    }
}

pub async fn list_docs(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return (StatusCode::NOT_IMPLEMENTED, "Use /query for cross-shard iteration").into_response();
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let col_clone = col.clone();
    match tokio::task::spawn_blocking(move || col_clone.list_all()).await {
        Ok(Ok(vals)) => (StatusCode::OK, Json(vals)).into_response(),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn query_docs(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
    Query(params): Query<QueryParams>,
    _req: axum::extract::Request,
) -> impl axum::response::IntoResponse {
    let limit = params.limit.unwrap_or(100).max(1);
    let sort = parse_sort(params.sort.as_deref());
    let fields = parse_fields(params.fields.as_deref());

    if state.config.role == "router" {
        return router_query(&state, &col_name, &params, limit, &sort, &fields).await;
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let col_clone = col.clone();
    let filter_obj: Option<Filter> = params.filter
        .as_ref()
        .and_then(|f| serde_json::from_str::<Filter>(f).ok());
    let after = params.cursor.clone();
    let start = params.start.clone();
    let end = params.end.clone();

    let result = tokio::task::spawn_blocking(move || -> io::Result<(Vec<serde_json::Value>, Option<String>)> {
        if let Some(sort) = &sort {
            let mut items = Vec::new();
            for key in col_clone.range_from(None, start.as_deref(), end.as_deref()).into_iter() {
                if let Some(val) = col_clone.get(&key)? {
                    let matched = filter_obj.as_ref().map_or(true, |f| matches_filter(&val, f));
                    if matched {
                        items.push(val);
                    }
                }
            }
            items.sort_by(|a, b| compare_by_sort(a, b, sort));
            items.truncate(limit);
            let projected = items.iter().map(|v| project(v, &fields)).collect();
            Ok((projected, None))
        } else {
            let (items, next_cursor) = col_clone.query_page(after.as_deref(), start.as_deref(), end.as_deref(), &filter_obj, limit)?;
            let projected = items.iter().map(|v| project(v, &fields)).collect();
            Ok((projected, next_cursor))
        }
    }).await;

    match result {
        Ok(Ok((items, next_cursor))) => (StatusCode::OK, Json(QueryPage { items, next_cursor })).into_response(),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
