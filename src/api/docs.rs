//! Document endpoints.

use super::write::{local_patch, local_write, local_write_batch};
use crate::cluster::router::{
    parse_read_pref, router_forward_write, router_read_doc, router_query, bulk_router_forward,
    passthrough, ForwardMethod, ReadPreference,
};
use crate::consensus::read_index::read_index;
use crate::json::{parse_fields, project};
use crate::model::{
    err_json, BulkDoc, CreateDoc, QueryPage, QueryParams, ReadParams, DEFAULT_QUERY_LIMIT,
    MAX_QUERY_LIMIT,
};
use crate::query::{decode_cursor, encode_cursor, parse_filter, parse_sort, sort_value, KeyCursor, SortCursor, SortedRow};
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

/// `read=primary` is a guarantee, not a hint: a node that does not lead refuses rather than
/// answering from a log it may be behind on. The router forwards the preference for this check.
fn not_the_primary(state: &AppState, pref: &ReadPreference) -> Option<axum::response::Response> {
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
            // `207` when the batch is not uniformly what `201` promises, the way the fan-outs
            // already answer it. The status is the part a client acts on, and a per-item `warning`
            // it has to go looking for is not one -- every single-document path answers `202` for
            // exactly this (M19).
            let status = if met { StatusCode::CREATED } else { StatusCode::MULTI_STATUS };
            (status, Json(serde_json::json!({"results": results}))).into_response()
        }
        Err(resp) => resp,
    }
}

/// `read=quorum` is the guarantee `read=primary` is not: the answer comes from a leader that has
/// confirmed with a majority that it still leads, at an index it has already applied.
///
/// Every refusal is `503` with `Retry-After`, because none of them means the read was wrong -- an
/// unconfirmed leader, a term too new to answer at, and an index not yet visible are all "not now".
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
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return (StatusCode::NOT_IMPLEMENTED, "Use /query for cross-shard iteration").into_response();
    }

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
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
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<QueryParams>,
    _req: axum::extract::Request,
) -> impl axum::response::IntoResponse {
    let limit = match params.limit {
        Some(n) if n > MAX_QUERY_LIMIT => return err_json(StatusCode::BAD_REQUEST,
            format!("limit {} exceeds the maximum of {}; page with `cursor`", n, MAX_QUERY_LIMIT)),
        Some(n) => n.max(1),
        None => DEFAULT_QUERY_LIMIT,
    };
    let sort = parse_sort(params.sort.as_deref());
    let fields = parse_fields(params.fields.as_deref());
    let filter_obj = match params.filter.as_deref().map(parse_filter).transpose() {
        Ok(f) => f,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let pref = match parse_read_pref(params.read.as_deref()) {
        Ok(p) => p,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    // A sorted cursor is a position in the sort order and an unsorted one is a position in one
    // shard's keyspace, so a cursor carried over from a differently-shaped query cannot be honoured
    // and must not be ignored. Both are checked: the unsorted one used to be a bare key, which any
    // string is a valid one of, so the mix-up in that direction was answered rather than refused.
    let sort_cursor = match (&sort, params.cursor.as_deref()) {
        (Some(_), Some(c)) => match decode_cursor::<SortCursor>(c) {
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
            Some(c) => Some(c.key),
            None => return err_json(StatusCode::BAD_REQUEST,
                "cursor does not belong to this unsorted query".to_string()),
        },
        _ => None,
    };

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    let col_clone = col.clone();
    let after = key_cursor;
    let start = params.start.clone();
    let end = params.end.clone();
    let want_keys = params.keys.unwrap_or(false);

    let result = tokio::task::spawn_blocking(move || -> io::Result<(Vec<SortedRow>, Option<String>)> {
        match &sort {
            Some(sort) => {
                let (rows, more) = col_clone.sorted_page(
                    start.as_deref(), end.as_deref(), &filter_obj, sort, sort_cursor.as_ref(), limit)?;
                // The last row of the page is where the next one resumes, in sort order.
                let next = match (more, rows.last()) {
                    (true, Some(last)) => Some(encode_cursor(&SortCursor {
                        value: sort_value(&last.value, sort).clone(),
                        key: last.key.clone(),
                    })),
                    _ => None,
                };
                Ok((rows, next))
            },
            None => {
                let (rows, next) = col_clone.query_page(
                    after.as_deref(), start.as_deref(), end.as_deref(), &filter_obj, limit)?;
                Ok((rows, next.map(|key| encode_cursor(&KeyCursor { key }))))
            },
        }
    }).await;

    match result {
        Ok(Ok((rows, next_cursor))) => {
            let keys = if want_keys { rows.iter().map(|r| r.key.clone()).collect() } else { Vec::new() };
            let items = rows.iter().map(|r| project(&r.value, &fields)).collect();
            (StatusCode::OK, Json(QueryPage { items, next_cursor, keys })).into_response()
        },
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::{
        next_test_port, node_by_id, put_doc_http, put_value, router_for, single_node, temp_root,
        three_node_cluster, two_shard_cluster, wait_for, TestNode,
    };
    use axum::http::StatusCode;
    use std::time::Duration;

    /// H8: with `?sort=`, `cursor` was ignored and `next_cursor` was always `None` — sorted
    /// pagination was silently a no-op. The cursor is now a position in the sort order, which is one
    /// position for the whole cluster rather than a per-shard map.
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

    /// M3: `ceil(limit/n)` per shard, concatenated untrimmed, returned up to `n - 1` rows more than
    /// asked for. Trimming afterwards is not the fix — a trimmed row sits behind the cursor its
    /// shard already moved past — so the shares sum to the limit instead.
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

    /// The guarantee `read=primary` cannot give. It asks the node for its own opinion of who leads,
    /// and a leader that has already been replaced still holds that opinion; `read=quorum` makes it
    /// confirm with a majority before answering.
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

        // /query reaches the barrier by its own path, so it has its own assertion.
        let status = client.get(&format!("{}/collections/t/query?read=quorum", base))
            .send().await.unwrap().status();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
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

        for bad in [r#"{"n": {"$exists": true}}"#, r#"{"n": {"$in": 1}}"#, r#"{"$or": []}"#, "not json"] {
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

    /// M18: anything that was not `1`, `majority`, `all` or a number became `w=1`, so a client that
    /// asked for durability was told `200` and had no way to find out it got none. M19: the bulk
    /// path answered `201` whatever the concern did, with the shortfall buried in each item.
    /// L18: an unsorted `/query` took any string as a start key, so a cursor from a differently
    /// shaped query was answered instead of refused.
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
}
