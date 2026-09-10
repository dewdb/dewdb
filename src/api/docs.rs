//! Document endpoints.

use super::write::{local_patch, local_write, local_write_batch};
use crate::aggregate::{
    budget_spent, parse_group, parse_metrics, AggregateSpec, DEFAULT_AGGREGATE_SCAN,
    MAX_AGGREGATE_SCAN, SCAN_ADMISSION_WAIT_MS,
};
use crate::cluster::router::{
    parse_read_pref, router_aggregate, router_forward_write, router_read_doc, router_query,
    bulk_router_forward, passthrough, stale_ring_response, ForwardMethod, ReadPreference,
};
use crate::consensus::read_index::read_index;
use crate::json::{parse_fields, project};
use crate::model::{
    err_json, AggregateParams, BulkDoc, CreateDoc, QueryPage, QueryParams, ReadParams,
    DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT,
};
use crate::query::{
    check_key_range, decode_cursor, encode_cursor, parse_filter, parse_sort, sort_position,
    KeyCursor, SortCursor, SortedRow,
};
use crate::replication::{parse_write_concern, wc_query_string, WriteConcernParams, DEFAULT_WTIMEOUT_MS};
use crate::cluster::ownership::Ownership;
use crate::api::middleware::{client_collection, CollectionPath};
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::io;
use std::time::Duration;
use uuid::Uuid;


/// Redirects stale owners and retries writes paused by migration finalization.
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

/// A server-generated id only has to be unique, so a shard that does not own the first draws again.
/// Expected draws equal the shard count; the bound covers a ring that says this node owns nothing.
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

/// `read=primary` is a guarantee, not a hint: a node that does not lead refuses rather than
/// answering from a log it may be behind on. The router forwards the preference for this check.
pub(super) fn not_the_primary(state: &AppState, pref: &ReadPreference) -> Option<axum::response::Response> {
    if !matches!(pref, ReadPreference::Primary | ReadPreference::Quorum)
        || !state.is_shard() || state.is_leader() {
        return None;
    }
    Some((
        StatusCode::SERVICE_UNAVAILABLE,
        [(axum::http::header::RETRY_AFTER, "1")],
        Json(serde_json::json!({
            "error": "this node is not the primary; retry, or ask for read=replica",
        })),
    ).into_response())
}

pub async fn create_doc(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
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

    let _write_gate = state.write_gate.read().await;

    // The id is ours to choose, so choose one this shard owns rather than refuse the write.
    let id = match own_id(&state, &col_name, id) {
        Some(id) => id,
        None => return err_json(StatusCode::SERVICE_UNAVAILABLE,
            "could not generate a key this shard owns; retry".to_string()),
    };

    let wc = match parse_write_concern(wcp.w.as_deref()) {
        Ok(wc) => wc,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
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
    CollectionPath((col_name, id)): CollectionPath<(String, String)>,
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

    let _write_gate = state.write_gate.read().await;

    if let Some(refusal) = wrong_owner(&state, &col_name, &id) {
        return refusal;
    }

    let wc = match parse_write_concern(wcp.w.as_deref()) {
        Ok(wc) => wc,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
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
    CollectionPath(col_name): CollectionPath<String>,
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

    let _write_gate = state.write_gate.read().await;

    let wc = match parse_write_concern(wcp.w.as_deref()) {
        Ok(wc) => wc,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
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
            let met = outcomes.iter().all(|o| o.met);
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
            // `207` when the batch is not uniformly what `201` promises, the way the fan-outs answer
            // it. A per-item `warning` a client has to go looking for is not a status (M19).
            let status = if met { StatusCode::CREATED } else { StatusCode::MULTI_STATUS };
            (status, Json(serde_json::json!({"results": results}))).into_response()
        }
        Err(resp) => resp,
    }
}

/// `read=quorum` is the guarantee `read=primary` is not: a leader that has confirmed with a majority,
/// at an index it has applied. Every refusal is `503` with `Retry-After` -- all of them mean "not now".
async fn unconfirmed_leader(
    state: &AppState,
    pref: &ReadPreference,
    collection: &str,
) -> Option<axum::response::Response> {
    if !matches!(pref, ReadPreference::Quorum) {
        return None;
    }
    match read_index(state, collection).await {
        Ok(_) => None,
        Err(refusal) => Some((
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            Json(serde_json::json!({"error": refusal.message()})),
        ).into_response()),
    }
}

pub async fn get_doc(
    State(state): State<AppState>,
    CollectionPath((col_name, id)): CollectionPath<(String, String)>,
    Query(rp): Query<ReadParams>,
) -> impl axum::response::IntoResponse {
    let pref = match parse_read_pref(rp.read.as_deref()) {
        Ok(p) => p,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };

    if state.config.role == "router" {
        return router_read_doc(&state, &col_name, &id, pref).await;
    }

    if let Some(refusal) = not_the_primary(&state, &pref) {
        return refusal;
    }
    if let Some(refusal) = unconfirmed_leader(&state, &pref, &col_name).await {
        return refusal;
    }

    // Only a wrong owner redirects. A key mid-handover still reads correctly here: the source is
    // the owner until the flip, and refusing would take reads down for no reason.
    if let Some(Ownership::Elsewhere(owner)) = state.ownership(&col_name, &id) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({
            "error": "this shard does not own that key", "owner": owner, "key": id,
        }))).into_response();
    }

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
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
    CollectionPath((col_name, id)): CollectionPath<(String, String)>,
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

    let _write_gate = state.write_gate.read().await;

    if let Some(refusal) = wrong_owner(&state, &col_name, &id) {
        return refusal;
    }

    let wc = match parse_write_concern(wcp.w.as_deref()) {
        Ok(wc) => wc,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
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
    CollectionPath((col_name, id)): CollectionPath<(String, String)>,
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

    let _write_gate = state.write_gate.read().await;

    if let Some(refusal) = wrong_owner(&state, &col_name, &id) {
        return refusal;
    }

    let wc = match parse_write_concern(wcp.w.as_deref()) {
        Ok(wc) => wc,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
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
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<QueryParams>,
    req: axum::extract::Request,
) -> axum::response::Response {
    if state.config.role == "router" {
        return (StatusCode::NOT_IMPLEMENTED, "Use /query for cross-shard iteration").into_response();
    }
    query_docs(State(state), CollectionPath(col_name), Query(params), req).await.into_response()
}

pub async fn query_docs(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<QueryParams>,
    _req: axum::extract::Request,
) -> impl axum::response::IntoResponse {
    let budget = match params.max_docs {
        Some(n) if n > MAX_AGGREGATE_SCAN => return err_json(StatusCode::BAD_REQUEST,
            format!("max_docs {} exceeds the maximum of {}", n, MAX_AGGREGATE_SCAN)),
        Some(n) => n.max(1),
        None => DEFAULT_AGGREGATE_SCAN,
    };
    let limit = match params.limit {
        Some(n) if n > MAX_QUERY_LIMIT => return err_json(StatusCode::BAD_REQUEST,
            format!("limit {} exceeds the maximum of {}; page with `cursor`", n, MAX_QUERY_LIMIT)),
        Some(n) => n.max(1),
        None => DEFAULT_QUERY_LIMIT,
    };
    let sort = match parse_sort(params.sort.as_deref()) {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let fields = parse_fields(params.fields.as_deref());
    let filter_obj = match params.filter.as_deref().map(parse_filter).transpose() {
        Ok(f) => f,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let pref = match parse_read_pref(params.read.as_deref()) {
        Ok(p) => p,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    if let Err(e) = check_key_range(params.start.as_deref(), params.end.as_deref()) {
        return err_json(StatusCode::BAD_REQUEST, e);
    }
    // A sorted cursor is a position in the sort order and an unsorted one a position in a shard's
    // keyspace, so a cursor from a differently-shaped query is refused rather than ignored.
    let sort_cursor = match (&sort, params.cursor.as_deref()) {
        // The arity is part of belonging: a position taken under one set of sort keys says nothing
        // about where a different set resumes.
        (Some(order), Some(c)) => match decode_cursor::<SortCursor>(c)
            .filter(|c| c.positions().len() == order.keys.len()) {
            Some(c) => Some(c),
            None => return err_json(StatusCode::BAD_REQUEST,
                "cursor does not belong to this sorted query".to_string()),
        },
        _ => None,
    };

    if state.config.role == "router" {
        return router_query(&state, &col_name, &params, limit, &sort, &fields, pref).await;
    }

    if let Some(refusal) = not_the_primary(&state, &pref) {
        return refusal;
    }
    // Makes the page's starting point linearizable, not the scan atomic: a walk is not a snapshot
    // whatever it is asked for, and rows written between chunks may or may not be seen.
    if let Some(refusal) = unconfirmed_leader(&state, &pref, &col_name).await {
        return refusal;
    }

    // Below the router branch: a router carries its own `ShardCursor` here, and this shape is the
    // one a shard issues for itself.
    let key_cursor = match (&sort, params.cursor.as_deref()) {
        (None, Some(c)) => match decode_cursor::<KeyCursor>(c) {
            Some(c) => Some(c),
            None => return err_json(StatusCode::BAD_REQUEST,
                "cursor does not belong to this unsorted query".to_string()),
        },
        _ => None,
    };

    // Ahead of the collection handle and the scan slot: a refused page should cost neither.
    let ownership = state.scan_ownership(&col_name);
    // Pins the pagination session, where the snapshot alone pins only the page: a flip moves keys
    // across positions taken before it, so the scan restarts rather than duplicate or drop (IB-047).
    if key_cursor.as_ref().is_some_and(|c| c.ring.is_some_and(|r| r != ownership.fingerprint())) {
        return stale_ring_response();
    }

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    let slot = if sort.is_some() {
        match tokio::time::timeout(Duration::from_millis(SCAN_ADMISSION_WAIT_MS),
            state.scan_slots.clone().acquire_owned()).await {
            Ok(Ok(slot)) => Some(slot),
            _ => return err_json(StatusCode::TOO_MANY_REQUESTS,
                "too many scans running on this node; retry".to_string()),
        }
    } else { None };
    let col_clone = col.clone();
    let after = key_cursor.map(|c| c.key);
    let start = params.start.clone();
    let end = params.end.clone();
    let want_keys = params.keys.unwrap_or(false);
    let ring = ownership.fingerprint();

    let result = tokio::task::spawn_blocking(move || -> io::Result<(Vec<SortedRow>, Option<String>)> {
        let _slot = slot;
        match &sort {
            Some(sort) => {
                let (rows, more) = col_clone.sorted_page_owned(
                    start.as_deref(), end.as_deref(), &filter_obj, sort, sort_cursor.as_ref(), limit, budget,
                    &|key| ownership.includes(key))?;
                // The last row of the page is where the next one resumes, in sort order.
                let next = match (more, rows.last()) {
                    (true, Some(last)) => Some(encode_cursor(&SortCursor::at(
                        sort_position(&last.value, sort), last.key.clone()))),
                    _ => None,
                };
                Ok((rows, next))
            },
            None => {
                let (rows, next) = col_clone.query_page_owned(
                    after.as_deref(), start.as_deref(), end.as_deref(), &filter_obj, limit, budget,
                    &|key| ownership.includes(key))?;
                Ok((rows, next.map(|key| encode_cursor(&KeyCursor { key, ring: Some(ring) }))))
            },
        }
    }).await;

    match result {
        Ok(Ok((rows, next_cursor))) => {
            let keys = if want_keys { rows.iter().map(|r| r.key.clone()).collect() } else { Vec::new() };
            let items = rows.iter().map(|r| project(&r.value, &fields)).collect();
            (StatusCode::OK, Json(QueryPage { items, next_cursor, keys })).into_response()
        },
        Ok(Err(e)) if e.kind() == io::ErrorKind::InvalidInput =>
            err_json(StatusCode::BAD_REQUEST, e.to_string()),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn aggregate_docs(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<AggregateParams>,
) -> impl axum::response::IntoResponse {
    // Refused rather than clamped, the way `/query` refuses an oversized `limit`: a client that
    // asked for a walk this size is told it is bounded instead of being handed a short answer.
    let budget = match params.max_docs {
        Some(n) if n > MAX_AGGREGATE_SCAN => return err_json(StatusCode::BAD_REQUEST,
            format!("max_docs {} exceeds the maximum of {}; narrow the aggregation instead",
                n, MAX_AGGREGATE_SCAN)),
        Some(n) => n.max(1),
        None => DEFAULT_AGGREGATE_SCAN,
    };
    let allow_partial = params.partial.unwrap_or(false);
    let filter_obj = match params.filter.as_deref().map(parse_filter).transpose() {
        Ok(f) => f,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let group = match parse_group(params.group.as_deref()) {
        Ok(g) => g,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let metrics = match parse_metrics(params.metrics.as_deref()) {
        Ok(m) => m,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let pref = match parse_read_pref(params.read.as_deref()) {
        Ok(p) => p,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    if let Err(e) = check_key_range(params.start.as_deref(), params.end.as_deref()) {
        return err_json(StatusCode::BAD_REQUEST, e);
    }

    if state.config.role == "router" {
        return router_aggregate(&state, &col_name, &params, pref).await;
    }

    if let Some(refusal) = not_the_primary(&state, &pref) {
        return refusal;
    }
    // The same guarantee a `read=quorum` page gets: the starting point is linearizable, and the
    // walk that follows it is not a snapshot.
    if let Some(refusal) = unconfirmed_leader(&state, &pref, &col_name).await {
        return refusal;
    }

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    // Admission before the walk, not a queue of walks: waiting costs a task, and a scan that has
    // started holds a blocking thread for as long as its budget lasts.
    let wait = Duration::from_millis(SCAN_ADMISSION_WAIT_MS);
    let slot = match tokio::time::timeout(wait, state.scan_slots.clone().acquire_owned()).await {
        Ok(Ok(slot)) => slot,
        // Retryable and shard-local, so a router tries the next replica rather than giving up.
        _ => return err_json(StatusCode::TOO_MANY_REQUESTS,
            "too many aggregations running on this node; retry".to_string()),
    };

    let (start, end) = (params.start.clone(), params.end.clone());
    let spec = AggregateSpec { group, metrics };
    let ownership = state.scan_ownership(&col_name);
    let result = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        col.aggregate(start.as_deref(), end.as_deref(), &filter_obj, spec, budget,
            &|key| ownership.includes(key))
    }).await;

    match result {
        // A partial the client did not ask for is refused for the reason the group ceiling is: the
        // totals are short, and short is indistinguishable from complete once a router merges them.
        Ok(Ok(agg)) if agg.partial && !allow_partial =>
            err_json(StatusCode::BAD_REQUEST, budget_spent(budget)),
        Ok(Ok(agg)) => (StatusCode::OK, Json(agg)).into_response(),
        Ok(Err(e)) if e.kind() == io::ErrorKind::InvalidInput =>
            err_json(StatusCode::BAD_REQUEST, e.to_string()),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregate::{MAX_AGGREGATE_SCAN, MAX_CONCURRENT_SCANS};
    use crate::cluster::metadata::{Adoption, Migration, MigrationPhase};
    use crate::ring::{hash_key, HashRing, RingShard};
    use crate::test_support::{
        next_test_port, node_by_id, put_doc_http, put_value, router_for, single_node, temp_root,
        three_node_cluster, two_shard_cluster, wait_for, TestNode,
    };
    use axum::http::StatusCode;
    use std::time::Duration;

    /// IB-054: an unsorted filtered page had no read budget, so a selective filter read a shard's whole
    /// owned range. `max_docs` bounds it, and because the page resumes the bound ends it, not refuses it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unsorted_filtered_query_pages_within_its_read_budget_across_shards() {
        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();

        let rows = 40i64;
        for i in 0..rows {
            let value = serde_json::json!({"n": i, "hot": i % 20 == 19});
            assert_eq!(put_value(&client, &router.url(), "t", &format!("k{:02}", i), value, "").await,
                StatusCode::CREATED);
        }

        let mut seen: Vec<i64> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0;
        loop {
            let mut q = vec![
                ("filter".to_string(), r#"{"hot": true}"#.to_string()),
                ("limit".to_string(), "10".to_string()),
                // Two reads per shard per page: far short of what the filter has to walk past.
                ("max_docs".to_string(), "2".to_string()),
            ];
            if let Some(c) = &cursor { q.push(("cursor".to_string(), c.clone())); }
            let r = client.get(format!("{}/collections/t/query", router.url()))
                .query(&q).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::OK,
                "a resumable page is bounded, not refused: {}", r.text().await.unwrap());
            let body = r.json::<serde_json::Value>().await.unwrap();
            seen.extend(body["items"].as_array().unwrap().iter()
                .map(|v| v["n"].as_i64().unwrap()));
            pages += 1;
            assert!(pages <= 60, "the cursor stopped advancing after {} rows", seen.len());
            match body["next_cursor"].as_str() {
                Some(c) => cursor = Some(c.to_string()),
                None => break,
            }
        }

        seen.sort();
        assert_eq!(seen, vec![19, 39],
            "every match has to survive a budget that cannot reach it in one page");
        assert!(pages > 2, "a budget of two reads per shard cannot have covered 40 keys in {} pages",
            pages);
    }

    /// H8: with `?sort=`, `cursor` was ignored and `next_cursor` was always `None`. The cursor is now a
    /// position in the sort order, which is one position for the whole cluster.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_sorted_query_pages_across_shards_in_one_global_order() {
        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();

        // Rank descends as the key ascends, so a page that came out in key order would show it.
        let rows = 15i64;
        for i in 0..rows {
            let value = serde_json::json!({"rank": rows - i, "tag": if i % 2 == 0 { "even" } else { "odd" }});
            assert_eq!(put_value(&client, &router.url(), "t", &format!("k{:02}", i), value, "").await,
                StatusCode::CREATED);
        }

        let page = |q: Vec<(String, String)>| {
            let (c, base) = (client.clone(), router.url());
            async move {
                let r = c.get(&format!("{}/collections/t/query", base)).query(&q).send().await.unwrap();
                assert_eq!(r.status(), StatusCode::OK);
                r.json::<serde_json::Value>().await.unwrap()
            }
        };
        let sorted_page = |dir: &'static str, limit: usize, cursor: Option<String>| {
            let mut q = vec![
                ("sort".to_string(), format!("rank:{}", dir)),
                ("limit".to_string(), limit.to_string()),
            ];
            if let Some(c) = cursor { q.push(("cursor".to_string(), c)); }
            page(q)
        };

        for dir in ["asc", "desc"] {
            let mut ranks: Vec<i64> = Vec::new();
            let mut cursor: Option<String> = None;
            for _ in 0..20 {
                let body = sorted_page(dir, 4, cursor.clone()).await;
                let items = body["items"].as_array().unwrap().clone();
                assert!(items.len() <= 4, "a page of {} rows for a limit of 4", items.len());
                ranks.extend(items.iter().map(|v| v["rank"].as_i64().unwrap()));
                cursor = body["next_cursor"].as_str().map(str::to_string);
                if cursor.is_none() {
                    break;
                }
            }

            let mut expected: Vec<i64> = (1..=rows).collect();
            if dir == "desc" {
                expected.reverse();
            }
            assert_eq!(ranks, expected,
                "{}: every row once, in one global order across both shards", dir);
        }

        // The filter has to travel with the cursor, or a later page widens the result set.
        let mut tagged: Vec<i64> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            let mut q = vec![
                ("sort".to_string(), "rank:asc".to_string()),
                ("limit".to_string(), "3".to_string()),
                ("filter".to_string(), r#"{"tag": "even"}"#.to_string()),
            ];
            if let Some(c) = cursor.clone() { q.push(("cursor".to_string(), c)); }
            let body = page(q).await;
            tagged.extend(body["items"].as_array().unwrap().iter().map(|v| v["rank"].as_i64().unwrap()));
            cursor = body["next_cursor"].as_str().map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(tagged.len(), 8, "eight even keys, paged three at a time: {:?}", tagged);
        assert!(tagged.windows(2).all(|w| w[0] < w[1]), "still ordered: {:?}", tagged);

        // A projection that drops the sort field must not disturb the order it is merged on.
        let body = page(vec![
            ("sort".to_string(), "rank:asc".to_string()),
            ("fields".to_string(), "tag".to_string()),
            ("limit".to_string(), "5".to_string()),
        ]).await;
        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 5);
        assert!(items.iter().all(|v| v.get("rank").is_none() && v.get("tag").is_some()),
            "projection applies after the merge: {:?}", items);

        // Keys are opt-in and parallel to items, on both paths through the router.
        for q in [vec![("sort".to_string(), "rank:asc".to_string()), ("keys".to_string(), "true".to_string()),
                       ("limit".to_string(), "3".to_string())],
                  vec![("keys".to_string(), "true".to_string()), ("limit".to_string(), "3".to_string())]] {
            let sorted = q.iter().any(|(k, _)| k == "sort");
            let body = page(q).await;
            let items = body["items"].as_array().unwrap();
            let keys = body["keys"].as_array().expect("keys were asked for");
            assert_eq!(keys.len(), items.len(), "sorted={}", sorted);
            if sorted {
                assert_eq!(keys.iter().map(|k| k.as_str().unwrap()).collect::<Vec<_>>(),
                    vec!["k14", "k13", "k12"], "rank ascends as the key descends");
            }
        }
        assert!(page(vec![("limit".to_string(), "3".to_string())]).await.get("keys").is_none(),
            "keys stay out of the response unless asked for");

        // A cursor from a differently-shaped query is refused rather than quietly ignored.
        let unsorted = page(vec![("limit".to_string(), "2".to_string())]).await;
        let key_cursor = unsorted["next_cursor"].as_str().unwrap().to_string();
        let refused = client.get(&format!("{}/collections/t/query", router.url()))
            .query(&[("sort", "rank:asc"), ("cursor", &key_cursor)])
            .send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST, "an unsorted cursor is not a sort position");
    }

    /// M3: `ceil(limit/n)` per shard, concatenated untrimmed, returned up to `n - 1` rows too many.
    /// Trimming is not the fix -- a trimmed row sits behind a moved cursor -- so shares sum to the limit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cross_shard_page_never_exceeds_its_limit_or_loses_a_row() {
        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();

        let keys: Vec<String> = (0..12).map(|i| format!("k{:02}", i)).collect();
        for key in &keys {
            assert_eq!(put_value(&client, &router.url(), "t", key, serde_json::json!({"k": key}), "").await,
                StatusCode::CREATED, "write for {} did not land", key);
        }

        let held = |node: &crate::test_support::TestNode| {
            node.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|c| c.list_all().map(|v| v.len()).unwrap_or(0)).unwrap_or(0)
        };
        assert!(held(&s1) > 0 && held(&s2) > 0, "both shards must hold rows or the fan-out is not tested");
        assert_eq!(held(&s1) + held(&s2), keys.len(), "the router split the writes across the ring");

        let page = |limit: usize, cursor: Option<String>| {
            let (c, base) = (client.clone(), router.url());
            async move {
                let mut q = vec![("limit".to_string(), limit.to_string())];
                if let Some(c) = cursor { q.push(("cursor".to_string(), c)); }
                let r = c.get(&format!("{}/collections/t/query", base)).query(&q).send().await.unwrap();
                assert_eq!(r.status(), StatusCode::OK);
                r.json::<serde_json::Value>().await.unwrap()
            }
        };

        // Unfixed: 3 rows from each of two shards for a limit of 5, and 1 from each for a limit of 1.
        for limit in 1..=13usize {
            let body = page(limit, None).await;
            let n = body["items"].as_array().unwrap().len();
            assert!(n <= limit, "limit {} returned {} rows", limit, n);
        }

        // Paging the whole collection five at a time: every row once, none invented.
        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            let body = page(5, cursor.clone()).await;
            let items = body["items"].as_array().unwrap().clone();
            assert!(items.len() <= 5, "a page of {} rows for a limit of 5", items.len());
            seen.extend(items.iter().map(|v| v["k"].as_str().unwrap().to_string()));
            cursor = body["next_cursor"].as_str().map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        seen.sort();
        assert_eq!(seen, keys, "paging must return every row exactly once");

        // A limit smaller than the ring: the shard that got no share this page still gets its turn.
        let mut ones: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..40 {
            let body = page(1, cursor.clone()).await;
            ones.extend(body["items"].as_array().unwrap().iter().map(|v| v["k"].as_str().unwrap().to_string()));
            cursor = body["next_cursor"].as_str().map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        ones.sort();
        assert_eq!(ones, keys, "one row per page must still walk the whole ring");
    }

    /// M13: unsorted positions are per shard, so a key that changes owners mid-scan lands behind a
    /// position that never covered it. The cursor now records the layout it was taken against.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unsorted_cursor_is_refused_when_the_shard_layout_moved_under_it() {
        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();

        for i in 0..10 {
            put_value(&client, &router.url(), "t", &format!("k{:02}", i),
                serde_json::json!({"i": i}), "").await;
        }

        let page = |cursor: Option<String>| {
            let (c, base) = (client.clone(), router.url());
            async move {
                let mut q = vec![("limit".to_string(), "3".to_string())];
                if let Some(c) = cursor { q.push(("cursor".to_string(), c)); }
                let r = c.get(&format!("{}/collections/t/query", base)).query(&q).send().await.unwrap();
                let status = r.status();
                (status, r.json::<serde_json::Value>().await.unwrap())
            }
        };

        let (status, first) = page(None).await;
        assert_eq!(status, StatusCode::OK);
        let cursor = first["next_cursor"].as_str().unwrap().to_string();

        let (status, _) = page(Some(cursor.clone())).await;
        assert_eq!(status, StatusCode::OK, "an unchanged layout keeps the scan going");

        // Move the boundary between the two ranges. No data moves here, which is the point: the
        // router cannot tell whether it did, so the cursor is no longer answerable either way.
        let state = router.state.as_ref().unwrap();
        let mut moved = state.cluster.read().unwrap().clone();
        let boundary = moved.shards[0].end_hash / 2;
        moved.shards[0].end_hash = boundary;
        moved.shards[1].start_hash = boundary;
        moved.version += 1;
        moved.updated_by = "test".to_string();
        moved.seeded = false;
        assert!(matches!(state.adopt_cluster(moved), crate::cluster::metadata::Adoption::Adopted { .. }),
            "the test could not move the ring, so it proves nothing");

        let (status, body) = page(Some(cursor)).await;
        assert_eq!(status, StatusCode::CONFLICT,
            "unfixed this answered from positions the layout had invalidated: {}", body);

        let (status, restarted) = page(None).await;
        assert_eq!(status, StatusCode::OK, "restarting the scan is the documented remedy");
        assert_eq!(restarted["items"].as_array().unwrap().len(), 3);

        // A sorted scan carries one position for the cluster, so the same move does not touch it.
        let sorted = |cursor: Option<String>| {
            let (c, base) = (client.clone(), router.url());
            async move {
                let mut q = vec![("sort".to_string(), "i:asc".to_string()), ("limit".to_string(), "3".to_string())];
                if let Some(c) = cursor { q.push(("cursor".to_string(), c)); }
                let r = c.get(&format!("{}/collections/t/query", base)).query(&q).send().await.unwrap();
                (r.status(), r.json::<serde_json::Value>().await.unwrap())
            }
        };
        let (_, first_sorted) = sorted(None).await;
        let sorted_cursor = first_sorted["next_cursor"].as_str().unwrap().to_string();

        let mut back = state.cluster.read().unwrap().clone();
        back.shards[0].end_hash = boundary * 2;
        back.shards[1].start_hash = boundary * 2;
        back.version += 1;
        back.updated_by = "test".to_string();
        state.adopt_cluster(back);

        let (status, body) = sorted(Some(sorted_cursor)).await;
        assert_eq!(status, StatusCode::OK, "a sorted cursor is not tied to the layout: {}", body);
        assert_eq!(body["items"].as_array().unwrap()[0]["i"].as_i64(), Some(3));
    }

    /// M4: `read=primary` used to be satisfiable by any replica that answered first — the router
    /// listed them as fallbacks and a shard never checked the preference at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_primary_read_is_never_answered_by_a_follower() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::new();

        let doc = format!("{}/collections/t/docs/k", n1.url());
        assert!(client.put(&format!("{}?w=majority&wtimeout=4000", doc))
            .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap().status().is_success());

        let follower = node_by_id(&[&n2, &n3], if n2.is_leader() { "n3" } else { "n2" });
        assert!(!follower.is_leader());
        assert!(wait_for(Duration::from_secs(10), || {
            follower.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|c| c.exists("k")).unwrap_or(false)
        }).await, "the follower must hold the key, or this proves nothing about the refusal");

        let read = |base: String, q: &'static str| {
            let c = client.clone();
            async move { c.get(&format!("{}/collections/t/docs/k{}", base, q)).send().await.unwrap().status() }
        };

        assert_eq!(read(follower.url(), "").await, StatusCode::OK,
            "an unspecified preference still reads from wherever it was sent");
        assert_eq!(read(follower.url(), "?read=replica").await, StatusCode::OK);
        assert_eq!(read(follower.url(), "?read=primary").await, StatusCode::SERVICE_UNAVAILABLE,
            "unfixed the follower served this as if it were the primary");
        assert_eq!(read(n1.url(), "?read=primary").await, StatusCode::OK, "the leader still answers");

        // Same on /query, which reaches the check by a different path.
        let q = |base: String, q: &'static str| {
            let c = client.clone();
            async move { c.get(&format!("{}/collections/t/query{}", base, q)).send().await.unwrap().status() }
        };
        assert_eq!(q(follower.url(), "?read=primary").await, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(q(follower.url(), "").await, StatusCode::OK);

        // A preference nobody implements is refused rather than quietly downgraded to primary.
        assert_eq!(read(n1.url(), "?read=Primary").await, StatusCode::BAD_REQUEST);
        assert_eq!(q(n1.url(), "?read=nearest").await, StatusCode::BAD_REQUEST);
    }

    /// The guarantee `read=primary` cannot give: it asks the node for its own opinion of who leads, and
    /// a replaced leader still holds that opinion. `read=quorum` confirms with a majority first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_quorum_read_is_refused_by_a_leader_that_cannot_reach_its_voters() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::new();

        let doc = format!("{}/collections/t/docs/k", n1.url());
        assert!(client.put(&format!("{}?w=majority&wtimeout=4000", doc))
            .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap().status().is_success());

        let mut nodes = vec![n1, n2, n3];
        let leader = nodes.iter().position(|n| n.is_leader()).expect("the cluster settled on a leader");
        let base = nodes[leader].url();

        let read = |base: String, q: &'static str| {
            let c = client.clone();
            async move { c.get(&format!("{}/collections/t/docs/k{}", base, q)).send().await.unwrap().status() }
        };

        assert_eq!(read(base.clone(), "?read=quorum").await, StatusCode::OK,
            "a leader that can reach its voters answers, and pays one heartbeat round for it");

        for (i, node) in nodes.iter_mut().enumerate() {
            if i != leader {
                node.kill();
            }
        }

        assert_eq!(read(base.clone(), "?read=primary").await, StatusCode::OK,
            "read=primary is an opinion, and this node still holds it");
        // Killing them did not withdraw the promises they made while up, and until those lapse the
        // leader is right to answer: nodes that cannot vote cannot have elected anyone.
        let window = crate::consensus::lease::refusal_window(
            Duration::from_secs(nodes[leader].heartbeat_timeout_secs));
        tokio::time::sleep(window + Duration::from_millis(300)).await;

        assert_eq!(read(base.clone(), "?read=quorum").await, StatusCode::SERVICE_UNAVAILABLE,
            "with the lease lapsed and no majority reachable, nothing rules out a leader \
             elected on the other side of the partition, and answering here is stale");

        // /query and /aggregate reach the barrier by their own paths, so each has its own assertion.
        for path in ["query", "aggregate"] {
            let status = client.get(&format!("{}/collections/t/{}?read=quorum", base, path))
                .send().await.unwrap().status();
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "/{}", path);
        }
    }

    /// The router half: it still lists replicas so a promoted one is found, so the guarantee holds
    /// only because the preference travels with the request and the answering node enforces it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_router_reports_no_primary_rather_than_reading_a_replica() {
        let root = temp_root();
        let (mut n1, n2, n3) = three_node_cluster(&root).await;
        let router = router_for(&root, &[(n1.url(), vec![n2.url(), n3.url()])]).await;
        let client = reqwest::Client::new();

        let doc = format!("{}/collections/t/docs/k", n1.url());
        assert!(client.put(&format!("{}?w=majority&wtimeout=4000", doc))
            .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap().status().is_success());

        let via_router = |q: &'static str| {
            let (c, base) = (client.clone(), router.url());
            async move { c.get(&format!("{}/collections/t/docs/k{}", base, q)).send().await.unwrap().status() }
        };
        assert_eq!(via_router("?read=primary").await, StatusCode::OK);

        // Both followers must have applied it, not merely staged it, or the replica read below is
        // a 404 for reasons that have nothing to do with the preference.
        for follower in [&n2, &n3] {
            assert!(wait_for(Duration::from_secs(15), || {
                follower.state.as_ref().unwrap().db.as_ref().unwrap()
                    .get_collection("t").map(|c| c.exists("k")).unwrap_or(false)
            }).await, "a follower never applied the write");
        }

        // The leader is gone and the followers have not elected yet: the fallback list is exactly
        // what used to turn this into a silent stale read.
        n1.kill();
        assert_eq!(via_router("?read=primary").await, StatusCode::SERVICE_UNAVAILABLE,
            "unfixed the router served a follower's copy here");
        assert_eq!(via_router("?read=replica").await, StatusCode::OK,
            "a client that accepts a replica read still gets one");

        // Once a follower wins the election it is a primary, and the same read succeeds again.
        assert!(wait_for(Duration::from_secs(20), || n2.is_leader() || n3.is_leader()).await,
            "no election, so the recovery half is untested");

        let mut recovered = false;
        for _ in 0..40 {
            if via_router("?read=primary").await.is_success() {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(recovered, "the router never found the new primary");
    }

    /// A second sort key only matters where the first ties, and the page has to keep meaning the
    /// same thing across shards and across the cursor that resumes it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_multi_key_sort_pages_across_shards_in_one_global_order() {
        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();

        // Four bands of three, so the second key decides inside every band.
        let rows = 12i64;
        for i in 0..rows {
            let value = serde_json::json!({"band": i % 4, "score": i});
            assert_eq!(put_value(&client, &router.url(), "t", &format!("k{:02}", i), value, "").await,
                StatusCode::CREATED);
        }

        let mut seen: Vec<(i64, i64)> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            let mut q = vec![("sort".to_string(), "band:asc,score:desc".to_string()),
                             ("limit".to_string(), "5".to_string())];
            if let Some(c) = &cursor { q.push(("cursor".to_string(), c.clone())); }
            let r = client.get(&format!("{}/collections/t/query", router.url()))
                .query(&q).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            let body: serde_json::Value = r.json().await.unwrap();
            seen.extend(body["items"].as_array().unwrap().iter()
                .map(|v| (v["band"].as_i64().unwrap(), v["score"].as_i64().unwrap())));
            match body["next_cursor"].as_str() {
                Some(c) => cursor = Some(c.to_string()),
                None => break,
            }
        }

        let mut expected: Vec<(i64, i64)> = (0..rows).map(|i| (i % 4, i)).collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        assert_eq!(seen, expected, "every row once, ascending by band and descending by score");

        // A cursor is a position under one set of sort keys and says nothing under another.
        let first = client.get(&format!("{}/collections/t/query", router.url()))
            .query(&[("sort", "band:asc,score:desc"), ("limit", "2")]).send().await.unwrap()
            .json::<serde_json::Value>().await.unwrap();
        let two_key = first["next_cursor"].as_str().unwrap().to_string();
        let refused = client.get(&format!("{}/collections/t/query", router.url()))
            .query(&[("sort", "band:asc"), ("cursor", &two_key)]).send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST,
            "a two-key position is not a one-key position");
    }

    /// IB-019: `sort=v:descc` sorted ascending and `sort=:desc` dropped the sort, both answered
    /// `200`, so a client had no way to find out its query had been reinterpreted.
    #[tokio::test]
    async fn invalid_sort_syntax_is_refused_rather_than_reinterpreted() {
        let root = temp_root();
        let node = single_node(&root).await;
        let client = reqwest::Client::new();
        put_value(&client, &node.url(), "t", "k1", serde_json::json!({"v": 1}), "").await;

        for bad in ["v:descc", ":desc", "", "v:", "v,v"] {
            let r = client.get(&format!("{}/collections/t/query", node.url()))
                .query(&[("sort", bad)]).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::BAD_REQUEST, "sort `{}` must be refused", bad);
        }
        let ok = client.get(&format!("{}/collections/t/query", node.url()))
            .query(&[("sort", "v:DESC")]).send().await.unwrap();
        assert_eq!(ok.status(), StatusCode::OK, "the direction is case-insensitive, not free-form");
    }

    /// IB-018: `start=z&end=a` reached `BTreeMap::range`, which panics on a reversed pair, and the
    /// blocking-task failure surfaced as a 500.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reversed_key_range_is_refused_and_a_stale_cursor_is_not() {
        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();
        for k in ["a", "b", "c"] {
            put_value(&client, &router.url(), "t", k, serde_json::json!({"v": 1}), "").await;
        }

        for path in ["query", "aggregate"] {
            let r = client.get(&format!("{}/collections/t/{}", router.url(), path))
                .query(&[("start", "z"), ("end", "a"), ("metrics", "count")])
                .send().await.unwrap();
            assert_eq!(r.status(), StatusCode::BAD_REQUEST, "{} must refuse a reversed range", path);
        }

        let equal = client.get(&format!("{}/collections/t/query", router.url()))
            .query(&[("start", "b"), ("end", "b")]).send().await.unwrap();
        assert_eq!(equal.status(), StatusCode::OK, "an equal pair is a one-key range");
        assert_eq!(equal.json::<serde_json::Value>().await.unwrap()["items"].as_array().map(Vec::len),
            Some(1));

        // A cursor is opaque, so carrying one past a narrower `end` is an empty page and not a
        // client error the way an explicit reversed pair is.
        let first = client.get(&format!("{}/collections/t/query", router.url()))
            .query(&[("limit", "1")]).send().await.unwrap()
            .json::<serde_json::Value>().await.unwrap();
        let cursor = first["next_cursor"].as_str().unwrap().to_string();
        let narrowed = client.get(&format!("{}/collections/t/query", router.url()))
            .query(&[("cursor", cursor.as_str()), ("end", "a")]).send().await.unwrap();
        assert_eq!(narrowed.status(), StatusCode::OK);
        assert_eq!(narrowed.json::<serde_json::Value>().await.unwrap()["items"].as_array().map(Vec::len),
            Some(0));
    }

    /// Grouping splits across shards by key, so the merge has to be over groups. An average is the
    /// case that shows it: averaging the shard averages is a different number.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_aggregation_merges_across_shards_by_group() {
        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();

        let rows = 20i64;
        for i in 0..rows {
            let value = serde_json::json!({
                "tier": if i % 2 == 0 { "gold" } else { "silver" },
                "amount": i,
            });
            assert_eq!(put_value(&client, &router.url(), "t", &format!("k{:02}", i), value, "").await,
                StatusCode::CREATED);
        }

        let aggregate = |q: Vec<(&'static str, String)>| {
            let (c, base) = (client.clone(), router.url());
            async move {
                let r = c.get(&format!("{}/collections/t/aggregate", base)).query(&q).send().await.unwrap();
                assert_eq!(r.status(), StatusCode::OK);
                r.json::<serde_json::Value>().await.unwrap()
            }
        };

        let whole = aggregate(vec![("metrics", "count,sum:amount,avg:amount,min:amount,max:amount".into())]).await;
        let sorted_refusal = client.get(format!("{}/collections/t/query", router.url()))
            .query(&[("sort", "amount"), ("limit", "1"), ("max_docs", "1")])
            .send().await.unwrap();
        assert_eq!(sorted_refusal.status(), StatusCode::BAD_REQUEST);
        assert!(sorted_refusal.json::<serde_json::Value>().await.unwrap()["error"]
            .as_str().unwrap().contains("max_docs"));
        assert_eq!(whole["matched"].as_u64(), Some(20));
        assert_eq!(whole["groups"].as_array().map(Vec::len), Some(1));
        let m = &whole["groups"][0]["metrics"];
        assert_eq!(m["count"]["count"].as_u64(), Some(20));
        assert_eq!(m["sum:amount"]["sum"].as_f64(), Some(190.0));
        assert_eq!(m["avg:amount"]["avg"].as_f64(), Some(9.5));
        assert_eq!(m["min:amount"]["min"].as_i64(), Some(0));
        assert_eq!(m["max:amount"]["max"].as_i64(), Some(19));

        let grouped = aggregate(vec![("group", "tier".into()), ("metrics", "avg:amount".into())]).await;
        let groups = grouped["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 2, "two tiers, however the keys hashed across the shards");
        let by_tier = |name: &str| groups.iter()
            .find(|g| g["key"]["tier"] == serde_json::json!(name)).expect("tier present").clone();
        assert_eq!(by_tier("gold")["count"].as_u64(), Some(10));
        assert_eq!(by_tier("gold")["metrics"]["avg:amount"]["avg"].as_f64(), Some(9.0));
        assert_eq!(by_tier("silver")["metrics"]["avg:amount"]["avg"].as_f64(), Some(10.0));

        let filtered = aggregate(vec![
            ("filter", r#"{"amount": {"$gte": 10}}"#.into()),
            ("metrics", "count".into()),
        ]).await;
        assert_eq!(filtered["matched"].as_u64(), Some(10), "the filter travels to every shard");

        for bad in [vec![("metrics", "total".to_string())],
                    vec![("metrics", "sum".to_string())],
                    vec![("group", "".to_string())],
                    vec![("filter", "{".to_string())]] {
            let r = client.get(&format!("{}/collections/t/aggregate", router.url()))
                .query(&bad).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::BAD_REQUEST, "{:?} must be refused", bad);
        }

        let missing = client.get(&format!("{}/collections/nosuch/aggregate", router.url()))
            .send().await.unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND, "no shard holds it");
    }

    /// IB-025: `/query` capped `limit` and `/aggregate` capped nothing, so one request could walk a
    /// whole collection. The budget is per shard, and one shard stopping short marks the merge.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_aggregation_is_refused_when_it_would_outrun_its_read_budget() {
        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();

        let rows = 20i64;
        for i in 0..rows {
            assert_eq!(put_value(&client, &router.url(), "t", &format!("k{:02}", i),
                serde_json::json!({"amount": i}), "").await, StatusCode::CREATED);
        }

        let ask = |q: Vec<(&'static str, String)>| {
            let (c, base) = (client.clone(), router.url());
            async move {
                let r = c.get(&format!("{}/collections/t/aggregate", base)).query(&q).send().await.unwrap();
                (r.status(), r.json::<serde_json::Value>().await.unwrap())
            }
        };

        let (status, whole) = ask(vec![("metrics", "count,sum:amount".into())]).await;
        assert_eq!(status, StatusCode::OK, "the default budget covers twenty rows");
        assert_eq!(whole["matched"].as_u64(), Some(20));
        assert_eq!(whole["scanned"].as_u64(), Some(20), "the reads are published, not only the matches");
        assert_eq!(whole["partial"].as_bool(), Some(false));

        // One document per shard fits inside the budget on neither shard, and the request is
        // refused rather than answered with whatever the budget bought.
        let (status, body) = ask(vec![("metrics", "count".into()), ("max_docs", "1".into())]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "a spent budget refuses by default");
        assert!(body["error"].as_str().unwrap_or_default().contains("max_docs"),
            "and the refusal names the remedy: {}", body["error"]);

        let (status, partial) = ask(vec![
            ("metrics", "count,sum:amount".into()),
            ("max_docs", "1".into()),
            ("partial", "true".into()),
        ]).await;
        assert_eq!(status, StatusCode::OK, "`partial=true` accepts what the budget bought");
        assert_eq!(partial["partial"].as_bool(), Some(true),
            "and the answer says so, so a short total cannot read as a complete one");
        assert_eq!(partial["scanned"].as_u64(), Some(2), "one document from each of the two shards");
        assert_eq!(partial["matched"].as_u64(), Some(2));

        // Above the ceiling is refused rather than clamped: a client asking for an unbounded walk
        // is told the walk is bounded.
        let (status, _) = ask(vec![("max_docs", (MAX_AGGREGATE_SCAN + 1).to_string())]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = ask(vec![("max_docs", MAX_AGGREGATE_SCAN.to_string())]).await;
        assert_eq!(status, StatusCode::OK, "the ceiling itself is allowed");
    }

    /// The other half of IB-025: a budget bounds one walk, and concurrent walks are what occupy the
    /// blocking pool the ordinary reads and group commits share.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_node_admits_a_bounded_number_of_aggregation_scans() {
        let root = temp_root();
        let (s1, _s2, router) = two_shard_cluster(&root).await;
        let client = reqwest::Client::new();
        for i in 0..4 {
            assert_eq!(put_value(&client, &router.url(), "t", &format!("k{}", i),
                serde_json::json!({"amount": i}), "").await, StatusCode::CREATED);
        }

        let slots = s1.state.as_ref().unwrap().scan_slots.clone();
        let held = slots.clone().acquire_many_owned(MAX_CONCURRENT_SCANS as u32).await.unwrap();

        let refused = client.get(&format!("{}/collections/t/aggregate", s1.url()))
            .query(&[("metrics", "count")]).send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS,
            "every slot is taken, so the scan is refused rather than queued behind them");

        // A load refusal is not a missing primary and not a bad request, and the router says which:
        // a partial merged over the shard that could not answer would be short without saying so.
        let through_router = client.get(&format!("{}/collections/t/aggregate", router.url()))
            .query(&[("metrics", "count")]).send().await.unwrap();
        assert_eq!(through_router.status(), StatusCode::TOO_MANY_REQUESTS);

        let sorted_refusal = client.get(format!("{}/collections/t/query?sort=amount", router.url()))
            .send().await.unwrap();
        assert_eq!(sorted_refusal.status(), StatusCode::TOO_MANY_REQUESTS);

        drop(held);
        let admitted = client.get(&format!("{}/collections/t/aggregate", router.url()))
            .query(&[("metrics", "count")]).send().await.unwrap();
        assert_eq!(admitted.status(), StatusCode::OK, "and a returned slot is reusable");
        assert_eq!(admitted.json::<serde_json::Value>().await.unwrap()["matched"].as_u64(), Some(4));
        assert_eq!(slots.available_permits(), MAX_CONCURRENT_SCANS,
            "an answered aggregation releases its slot");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib016_queries_and_aggregates_ignore_non_owner_migration_copies() {
        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let current = HashRing {
            vnodes: 32,
            shards: vec![
                RingShard { node_url: s1.url(), replica_urls: Vec::new() },
                RingShard { node_url: s2.url(), replica_urls: Vec::new() },
            ],
        };
        let target = HashRing { vnodes: 64, shards: current.shards.clone() };
        let before = current.build();
        let after = target.build();
        let key = (0..100_000).map(|i| format!("copied-{}", i)).find(|key| {
            before.owner(hash_key("t", key)).is_some_and(|owner| owner.node_url == s1.url())
                && after.owner(hash_key("t", key)).is_some_and(|owner| owner.node_url == s2.url())
        }).expect("changing the vnode layout must move a key between these groups");

        let mut view = router.state.as_ref().unwrap().cluster_view();
        view.version += 1;
        view.updated_by = "ib016".to_string();
        view.seeded = false;
        view.ring = Some(current);
        view.migration = Some(Migration {
            id: "ib016-copy".to_string(),
            target,
            started_by: "ib016".to_string(),
            phase: MigrationPhase::Copy,
        });
        for state in [s1.state.as_ref().unwrap(), s2.state.as_ref().unwrap(),
            router.state.as_ref().unwrap()]
        {
            assert!(matches!(state.adopt_cluster(view.clone()), Adoption::Adopted { .. }));
        }

        for (node, value) in [
            (&s1, serde_json::json!({"amount": 1, "copy": false})),
            (&s2, serde_json::json!({"amount": 100, "copy": true})),
        ] {
            let col = node.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").unwrap();
            let (_, _, _, lsn) = col.put(key.clone(), value, 1).unwrap();
            col.apply_committed(lsn).unwrap();
        }

        let client = reqwest::Client::new();
        let query = format!("{}/collections/t/query", router.url());
        let mut cursor = None;
        let mut seen = Vec::new();
        for _ in 0..3 {
            let mut q = vec![("limit", "1"), ("keys", "true")];
            if let Some(value) = cursor.as_deref() { q.push(("cursor", value)); }
            let response = client.get(&query).query(&q).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let page = response.json::<serde_json::Value>().await.unwrap();
            seen.extend(page["items"].as_array().unwrap().iter().cloned());
            cursor = page["next_cursor"].as_str().map(str::to_string);
            if cursor.is_none() { break; }
        }
        assert_eq!(seen, vec![serde_json::json!({"amount": 1, "copy": false})]);

        let sorted = client.get(&query)
            .query(&[("sort", "amount:asc"), ("keys", "true")])
            .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
        assert_eq!(sorted["keys"], serde_json::json!([key]));
        assert_eq!(sorted["items"], serde_json::json!([{"amount": 1, "copy": false}]));

        let aggregate = client.get(format!("{}/collections/t/aggregate", router.url()))
            .query(&[("metrics", "count,sum:amount")]).send().await.unwrap();
        assert_eq!(aggregate.status(), StatusCode::OK);
        let aggregate = aggregate.json::<serde_json::Value>().await.unwrap();
        assert_eq!(aggregate["matched"].as_u64(), Some(1));
        assert_eq!(aggregate["groups"][0]["count"].as_u64(), Some(1));
        assert_eq!(aggregate["groups"][0]["metrics"]["sum:amount"]["sum"].as_f64(), Some(1.0));

        for (node, count) in [(&s1, 1), (&s2, 0)] {
            let r = client.get(format!("{}/collections/t/docs?limit=1", node.url())).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            let body = r.json::<serde_json::Value>().await.unwrap();
            assert_eq!(body["items"].as_array().unwrap().len(), count);
        }

        let flipped = view.with_ring("ib016", view.migration.as_ref().unwrap().target.clone());
        for state in [s1.state.as_ref().unwrap(), s2.state.as_ref().unwrap(),
            router.state.as_ref().unwrap()]
        {
            assert!(matches!(state.adopt_cluster(flipped.clone()), Adoption::Adopted { .. }));
        }

        let sorted = client.get(&query)
            .query(&[("sort", "amount:asc"), ("keys", "true")])
            .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
        assert_eq!(sorted["keys"], serde_json::json!([key]));
        assert_eq!(sorted["items"], serde_json::json!([{"amount": 100, "copy": true}]));

        let aggregate = client.get(format!("{}/collections/t/aggregate", router.url()))
            .query(&[("metrics", "count,sum:amount")]).send().await.unwrap()
            .json::<serde_json::Value>().await.unwrap();
        assert_eq!(aggregate["matched"].as_u64(), Some(1));
        assert_eq!(aggregate["groups"][0]["metrics"]["sum:amount"]["sum"].as_f64(), Some(100.0));
    }

    /// IB-047: the ownership snapshot is per request, so a flip between two pages of one unsorted scan
    /// leaves a moved key behind a position that never covered it. Applied to the shards only.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib047_a_ring_flip_between_two_pages_of_one_scan_is_refused() {
        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let current = HashRing {
            vnodes: 32,
            shards: vec![
                RingShard { node_url: s1.url(), replica_urls: Vec::new() },
                RingShard { node_url: s2.url(), replica_urls: Vec::new() },
            ],
        };
        let target = HashRing { vnodes: 64, shards: current.shards.clone() };
        let (before, after) = (current.build(), target.build());
        let owner_is = |ring: &crate::ring::BuiltRing, key: &str, url: &str| {
            ring.owner(hash_key("t", key)).is_some_and(|o| o.node_url == url)
        };
        let candidates = (0..200_000).map(|i| format!("k-{:06}", i));
        // Two keys on the source: one that the flip moves away, and one that keeps the source in
        // the scan so page one issues a position at all.
        let mut moved = None;
        let mut anchor = None;
        for key in candidates {
            if moved.is_none() && owner_is(&before, &key, &s1.url()) && owner_is(&after, &key, &s2.url()) {
                moved = Some(key);
            } else if anchor.is_none() && owner_is(&before, &key, &s1.url())
                && owner_is(&after, &key, &s1.url()) {
                anchor = Some(key);
            }
            if moved.is_some() && anchor.is_some() { break; }
        }
        let (moved, anchor) = (moved.expect("a key the flip moves"), anchor.expect("a key it does not"));

        let mut view = router.state.as_ref().unwrap().cluster_view();
        view.version += 1;
        view.updated_by = "ib047".to_string();
        view.seeded = false;
        view.ring = Some(current);
        view.migration = Some(Migration {
            id: "ib047-copy".to_string(),
            target,
            started_by: "ib047".to_string(),
            phase: MigrationPhase::Copy,
        });
        for state in [s1.state.as_ref().unwrap(), s2.state.as_ref().unwrap(),
            router.state.as_ref().unwrap()]
        {
            assert!(matches!(state.adopt_cluster(view.clone()), Adoption::Adopted { .. }));
        }

        // `moved` exists on both: the source owns it, and the destination holds the copy it will
        // own after the flip.
        for (node, keys) in [(&s1, vec![moved.clone(), anchor.clone()]), (&s2, vec![moved.clone()])] {
            let col = node.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").unwrap();
            for key in keys {
                let (_, _, _, lsn) = col.put(key, serde_json::json!({"v": 1}), 1).unwrap();
                col.apply_committed(lsn).unwrap();
            }
        }

        let client = reqwest::Client::new();
        let page = |base: String, limit: &'static str, cursor: Option<String>| {
            let client = client.clone();
            async move {
                let mut q = vec![("limit".to_string(), limit.to_string()),
                    ("keys".to_string(), "true".to_string())];
                if let Some(c) = cursor { q.push(("cursor".to_string(), c)); }
                client.get(format!("{}/collections/t/query", base)).query(&q).send().await.unwrap()
            }
        };

        let first = page(router.url(), "2", None).await;
        assert_eq!(first.status(), StatusCode::OK);
        let first = first.json::<serde_json::Value>().await.unwrap();
        let routed = first["next_cursor"].as_str().expect("the source has a key left to page to")
            .to_string();
        let direct = page(s1.url(), "1", None).await;
        assert_eq!(direct.status(), StatusCode::OK);
        let direct = direct.json::<serde_json::Value>().await.unwrap()["next_cursor"]
            .as_str().expect("two owned keys, one per page").to_string();

        let unchanged = page(router.url(), "2", Some(routed.clone())).await;
        assert_eq!(unchanged.status(), StatusCode::OK,
            "an unchanged layout must still resume: {}", unchanged.text().await.unwrap());

        // Only the shards, so the router's own cursor check still passes the page through.
        let flipped = view.with_ring("ib047", view.migration.as_ref().unwrap().target.clone());
        for state in [s1.state.as_ref().unwrap(), s2.state.as_ref().unwrap()] {
            assert!(matches!(state.adopt_cluster(flipped.clone()), Adoption::Adopted { .. }));
        }

        for (base, cursor, who) in [(router.url(), routed, "router"), (s1.url(), direct, "shard")] {
            let stale = page(base, "2", Some(cursor)).await;
            assert_eq!(stale.status(), StatusCode::CONFLICT,
                "{}: a position taken against the old ring cannot resume against the new one", who);
            let body = stale.json::<serde_json::Value>().await.unwrap();
            assert!(body["error"].as_str().unwrap().contains("restart the scan"),
                "{}: the client has to be told to start again, got {}", who, body);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib040_sorted_budget_and_listing_pages() {
        let root = temp_root();
        let node = single_node(&root).await;
        let client = reqwest::Client::new();
        for i in 0..3 {
            put_value(&client, &node.url(), "t", &format!("k{}", i),
                serde_json::json!({"n": 3-i}), "").await;
        }
        for route in ["query", "docs"] {
            let url = format!("{}/collections/t/{}", node.url(), route);
            for extra in ["", "&filter=%7B%22n%22%3A99%7D"] {
                let r = client.get(format!("{}?sort=n&limit=1&max_docs=2{}", url, extra))
                    .send().await.unwrap();
                assert_eq!(r.status(), StatusCode::BAD_REQUEST);
                let body = r.json::<serde_json::Value>().await.unwrap();
                assert!(body["error"].as_str().unwrap().contains("max_docs"));
                assert!(body.get("next_cursor").is_none());
            }
            let r = client.get(format!("{}?sort=n&limit=1&max_docs=3", url)).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            assert_eq!(r.json::<serde_json::Value>().await.unwrap()["items"][0]["n"], 1);
            let mut cursor = None;
            let mut seen = Vec::new();
            loop {
                let mut q = vec![("limit", "1".to_string()), ("keys", "true".to_string())];
                if let Some(c) = cursor { q.push(("cursor", c)); }
                let r = client.get(&url).query(&q).send().await.unwrap();
                assert_eq!(r.status(), StatusCode::OK);
                let body = r.json::<serde_json::Value>().await.unwrap();
                seen.extend(body["keys"].as_array().unwrap().iter().cloned());
                cursor = body["next_cursor"].as_str().map(str::to_string);
                if cursor.is_none() { break; }
                assert!(seen.len() < 4);
            }
            assert_eq!(seen, serde_json::json!(["k0", "k1", "k2"]).as_array().unwrap().clone());
        }
        let slots = node.state.as_ref().unwrap().scan_slots.clone();
        let held = slots.clone().acquire_many_owned(MAX_CONCURRENT_SCANS as u32).await.unwrap();
        let r = client.get(format!("{}/collections/t/query?sort=n", node.url())).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        drop(held);
        assert_eq!(slots.available_permits(), MAX_CONCURRENT_SCANS);
    }

    /// M1: a filter the engine cannot evaluate is a client error. Before the fix it was dropped and
    /// the query answered as if no filter had been sent.
    #[tokio::test]
    async fn a_filter_the_engine_cannot_evaluate_is_refused() {
        let root = temp_root();
        let node = single_node(&root).await;
        let client = reqwest::Client::new();
        put_value(&client, &node.url(), "t", "k1", serde_json::json!({"n": 1}), "").await;
        put_value(&client, &node.url(), "t", "k2", serde_json::json!({"meta": {"v": 1}}), "").await;
        put_value(&client, &node.url(), "t", "k3", serde_json::json!({"meta": {"v": 2}}), "").await;

        let query = |filter: &'static str| {
            let c = client.clone();
            let url = format!("{}/collections/t/query", node.url());
            async move { c.get(&url).query(&[("filter", filter)]).send().await.unwrap() }
        };

        for bad in [r#"{"n": {"$regex": "x"}}"#, r#"{"n": {"$in": 1}}"#, r#"{"$or": []}"#, "not json"] {
            assert_eq!(query(bad).await.status(), StatusCode::BAD_REQUEST, "filter {} must be refused", bad);
        }

        let res = query(r#"{"meta": {"v": 1}}"#).await;
        assert_eq!(res.status(), StatusCode::OK);
        let page: serde_json::Value = res.json().await.unwrap();
        assert_eq!(page["items"].as_array().map(|a| a.len()), Some(1),
            "a literal object matches by value, not by the field merely being present");
    }

    /// H1: `Vec::with_capacity(limit)` on a caller-supplied `limit` is an allocation the process
    /// aborts on, not a panic a handler can catch. The node must survive and answer.
    #[tokio::test]
    async fn an_oversized_limit_is_refused_instead_of_killing_the_node() {
        let root = temp_root();
        let node = single_node(&root).await;
        let client = reqwest::Client::new();
        put_value(&client, &node.url(), "t", "k1", serde_json::json!({"n": 1}), "").await;

        let query = |limit: String| {
            let c = client.clone();
            let url = format!("{}/collections/t/query", node.url());
            async move { c.get(&url).query(&[("limit", limit)]).send().await.map(|r| r.status()) }
        };

        // A multi-terabyte reservation before the fix.
        assert_eq!(query("100000000000".into()).await.ok(), Some(StatusCode::BAD_REQUEST));
        // usize::MAX takes the capacity-overflow path instead.
        assert_eq!(query(usize::MAX.to_string()).await.ok(), Some(StatusCode::BAD_REQUEST));

        assert_eq!(query("100".into()).await.ok(), Some(StatusCode::OK),
            "the node is still serving, which is the half of this that the status code cannot show");
    }

    /// M18: an unrecognised `w=` became `w=1`, so a client asking for durability got `200` and no way
    /// to tell. M19: the bulk path answered `201` regardless. L18: `/query` took any string as a cursor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_request_that_asks_for_something_unknown_is_refused_not_reinterpreted() {
        let root = temp_root();
        let mut node = TestNode::new("hyg", next_test_port(), &root, "primary");
        node.start();
        let c = reqwest::Client::new();
        let base = node.url();

        for spelling in ["quorum", "majorty", "abc", "-1"] {
            let url = format!("{}/collections/t/docs/k?w={}", base, spelling);
            let r = c.put(&url).json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::BAD_REQUEST,
                "`w={}` silently asked for no replication at all", spelling);
        }

        // A real write, so the cursor below is a real one.
        for i in 0..3 {
            assert!(put_doc_http(&c, &base, &format!("k{}", i), i as i64).await.is_success());
        }

        let page = c.get(format!("{}/collections/t/query?limit=1", base))
            .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
        let cursor = page["next_cursor"].as_str().expect("a page with more behind it").to_string();
        let resumed = c.get(format!("{}/collections/t/query?limit=10", base))
            .query(&[("cursor", &cursor)]).send().await.unwrap();
        assert_eq!(resumed.status(), StatusCode::OK, "its own cursor must still resume the scan");

        for bogus in ["k1", "not-a-cursor", "eyJyaW5nIjoxLCJwb3NpdGlvbnMiOnt9fQ=="] {
            let r = c.get(format!("{}/collections/t/query?limit=10", base))
                .query(&[("cursor", bogus)]).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::BAD_REQUEST,
                "`{}` was read as a start key and answered with rows after whatever it sorts as",
                bogus);
        }

        node.kill();
    }

    /// M19, the bulk half: `local_write_batch` at `w=majority` against replicas that are not there
    /// reports every item short, and the status is the part a client acts on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_bulk_write_that_missed_its_concern_does_not_answer_created() {
        let root = temp_root();
        let mut node = TestNode::new("blk", next_test_port(), &root, "primary");
        node.replicas = vec![
            format!("http://127.0.0.1:{}", next_test_port()),
            format!("http://127.0.0.1:{}", next_test_port()),
        ];
        node.start();
        let c = reqwest::Client::new();

        let r = c.post(format!("{}/collections/t/docs/bulk?w=majority&wtimeout=300", node.url()))
            .json(&serde_json::json!([{"value": {"v": 1}}, {"value": {"v": 2}}]))
            .send().await.unwrap();
        let status = r.status();
        let body = r.json::<serde_json::Value>().await.unwrap();
        assert_eq!(status, StatusCode::MULTI_STATUS,
            "a `201` for a batch no quorum holds is the one signal a bulk caller has: {}", body);
        assert_eq!(body["results"][0]["required"], 2, "{}", body);

        let met = c.post(format!("{}/collections/t/docs/bulk?w=1", node.url()))
            .json(&serde_json::json!([{"value": {"v": 3}}])).send().await.unwrap();
        assert_eq!(met.status(), StatusCode::CREATED, "a batch that met its concern is still 201");

        node.kill();
    }

    /// IB-014: a bulk write checked the uncommitted bound once, against the count before its own
    /// frames, so a batch of any width was admitted whole and left the buffer over the bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_bulk_write_cannot_stage_past_the_uncommitted_bound() {
        let root = temp_root();
        let mut node = TestNode::new("bnd", next_test_port(), &root, "primary");
        node.replicas = vec![
            format!("http://127.0.0.1:{}", next_test_port()),
            format!("http://127.0.0.1:{}", next_test_port()),
        ];
        node.flow_control = serde_json::json!({ "max_uncommitted_frames": 2 });
        node.start();
        let c = reqwest::Client::new();

        let ten: Vec<serde_json::Value> =
            (0..10).map(|i| serde_json::json!({"value": {"v": i}})).collect();
        let wide = c.post(format!("{}/collections/t/docs/bulk?w=majority&wtimeout=200", node.url()))
            .json(&ten).send().await.unwrap();
        assert_eq!(wide.status(), StatusCode::PAYLOAD_TOO_LARGE,
            "ten frames under a bound of two used to be staged in full");

        // Fills the bound with frames no quorum will ever commit.
        let filled = c.post(format!("{}/collections/t/docs/bulk?w=majority&wtimeout=200", node.url()))
            .json(&serde_json::json!([{"value": {"v": 1}}, {"value": {"v": 2}}]))
            .send().await.unwrap();
        assert_eq!(filled.status(), StatusCode::MULTI_STATUS);

        let refused = c.post(format!("{}/collections/t/docs?w=majority&wtimeout=200", node.url()))
            .json(&serde_json::json!({"value": {"v": 3}})).send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE,
            "a full buffer refuses the next write, and this one is retriable unlike the wide batch");

        node.kill();
    }
}
