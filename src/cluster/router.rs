//! Request forwarding, shard failover, and cross-shard fan-out.

use crate::aggregate::{merge as merge_aggregates, AggregateResult};
use crate::model::{err_json, AggregateParams, BulkDoc, CreateDoc, QueryPage, QueryParams};
use crate::query::{
    decode_cursor, encode_cursor, kway_merge, sort_position, ShardCursor, SortCursor, SortOrder,
    SortedRow,
};
use crate::json::project;
use crate::cluster::probe::unique_shards;
use crate::metrics::NodeLoad;
use crate::replication::write_concern::{wc_query_string, WriteConcernParams};
use crate::ring::hash_key;
use crate::util::encode_path_segment;
use crate::state::AppState;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

pub enum ForwardMethod {
    Put,
    Patch,
    Delete,
}

fn build_forward(client: &reqwest::Client, method: &ForwardMethod, url: &str, body: Option<&CreateDoc>) -> reqwest::RequestBuilder {
    let rb = match method {
        ForwardMethod::Put => client.put(url),
        ForwardMethod::Patch => client.patch(url),
        ForwardMethod::Delete => client.delete(url),
    };
    match body {
        Some(b) => rb.json(b),
        None => rb,
    }
}

/// A shard's reply, buffered. The forward path has to look inside a `409` to see whether it is a
/// redirect, and a `reqwest::Response` cannot be read twice or rebuilt.
pub struct ShardReply {
    pub status: StatusCode,
    pub body: String,
}

impl ShardReply {
    pub(crate) async fn of(r: reqwest::Response) -> Self {
        Self { status: r.status(), body: r.text().await.unwrap_or_default() }
    }

    /// `Some(owner)` when the shard is telling us our view is stale rather than answering.
    fn redirect(&self) -> Option<String> {
        if self.status != StatusCode::CONFLICT {
            return None;
        }
        serde_json::from_str::<serde_json::Value>(&self.body).ok()
            .and_then(|b| b.get("owner").and_then(|o| o.as_str().map(str::to_string)))
    }
}

pub fn passthrough(reply: ShardReply) -> axum::response::Response {
    let json: serde_json::Value = serde_json::from_str(&reply.body)
        .unwrap_or(serde_json::Value::String(reply.body));
    (reply.status, Json(json)).into_response()
}

// A shard's 4xx is an answer, not a failure: a PATCH 404 means no document, not a dead node.
fn authoritative_write_status(s: StatusCode) -> bool {
    s.is_success()
        || s == StatusCode::BAD_REQUEST
        || s == StatusCode::NOT_FOUND
        || s == StatusCode::CONFLICT
        || s == StatusCode::PAYLOAD_TOO_LARGE
        || s == StatusCode::UNPROCESSABLE_ENTITY
}

pub async fn router_forward_write(
    state: &AppState,
    col_name: &str,
    key: &str,
    method: ForwardMethod,
    body: Option<&CreateDoc>,
    wc_query: &str,
) -> Result<ShardReply, axum::response::Response> {
    let hash = hash_key(col_name, key);

    let (effective_url, original_url, replica_urls) = match state.get_effective_shard_url(hash) {
        Some(t) => t,
        None => return Err((StatusCode::BAD_REQUEST, "Key not owned by any shard").into_response()),
    };

    // The ring hashed the decoded key, so the shard has to store that same key: the path it
    // arrives on is the only thing that can lose it.
    let path = format!("/collections/{}/docs/{}{}",
        encode_path_segment(col_name), encode_path_segment(key), wc_query);

    let full_url = format!("{}{}", effective_url, path);
    if let Ok(r) = build_forward(&state.client, &method, &full_url, body).send().await {
        let reply = ShardReply::of(r).await;
        // The shard says the key is not its own, which means this router's ring is behind. Its
        // answer names the owner, so one retry gets the write to the right place instead of
        // handing the client a conflict it can do nothing about.
        if let Some(owner) = reply.redirect() {
            info!(target: "router", key, %owner, "Shard redirected the write; our ring is stale");
            let retry = format!("{}{}", owner, path);
            if let Ok(r2) = build_forward(&state.client, &method, &retry, body).send().await {
                let second = ShardReply::of(r2).await;
                if authoritative_write_status(second.status) && second.redirect().is_none() {
                    return Ok(second);
                }
            }
            return Ok(reply);
        }
        if authoritative_write_status(reply.status) {
            if effective_url != original_url {
                state.set_primary_override(&original_url, &effective_url);
            }
            return Ok(reply);
        }
    }

    let failover_lock = {
        let mut locks = state.shard_failover_locks.lock().unwrap();
        locks.entry(original_url.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    let _guard = failover_lock.lock().await;

    if let Some((latest_url, _, _)) = state.get_effective_shard_url(hash) {
        if latest_url != effective_url {
            let retry_url = format!("{}{}", latest_url, path);
            if let Ok(r) = build_forward(&state.client, &method, &retry_url, body).send().await {
                let reply = ShardReply::of(r).await;
                if authoritative_write_status(reply.status) {
                    return Ok(reply);
                }
            }
        }
    }

    state.primary_overrides.lock().unwrap().remove(&original_url);
    for replica in &replica_urls {
        let fallback_url = format!("{}{}", replica, path);
        if let Ok(r) = build_forward(&state.client, &method, &fallback_url, body).send().await {
            let reply = ShardReply::of(r).await;
            if authoritative_write_status(reply.status) {
                state.set_primary_override(&original_url, replica);
                info!(target: "router", "Cached new primary: {} -> {}", original_url, replica);
                return Ok(reply);
            }
        }
    }

    Err((StatusCode::BAD_GATEWAY, "All shard nodes unreachable").into_response())
}

async fn router_forward_bulk(
    state: &AppState,
    col_name: &str,
    effective_url: &str,
    original_url: &str,
    replica_urls: &[String],
    body: &[serde_json::Value],
    wc_query: &str,
) -> Result<reqwest::Response, String> {
    let path = format!("/collections/{}/docs/bulk{}", encode_path_segment(col_name), wc_query);
    let full_url = format!("{}{}", effective_url, path);
    if let Ok(r) = state.client.post(&full_url).json(body).send().await {
        if authoritative_write_status(r.status()) {
            if effective_url != original_url {
                state.set_primary_override(original_url, effective_url);
            }
            return Ok(r);
        }
    }

    let failover_lock = {
        let mut locks = state.shard_failover_locks.lock().unwrap();
        locks.entry(original_url.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    let _guard = failover_lock.lock().await;

    state.primary_overrides.lock().unwrap().remove(original_url);
    for replica in replica_urls {
        let fallback_url = format!("{}{}", replica, path);
        if let Ok(r) = state.client.post(&fallback_url).json(body).send().await {
            if authoritative_write_status(r.status()) {
                state.set_primary_override(original_url, replica);
                info!(target: "router", "Cached new primary: {} -> {}", original_url, replica);
                return Ok(r);
            }
        }
    }

    Err("All shard nodes unreachable".to_string())
}

pub async fn bulk_router_forward(
    state: &AppState,
    col_name: &str,
    docs: Vec<BulkDoc>,
    wc_query: &str,
) -> axum::response::Response {
    let n = docs.len();
    let mut groups: HashMap<String, (String, String, Vec<String>, Vec<(usize, String, serde_json::Value)>)> = HashMap::new();

    for (idx, d) in docs.into_iter().enumerate() {
        let id = d.id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let hash = hash_key(col_name, &id);
        let (effective_url, original_url, replica_urls) = match state.get_effective_shard_url(hash) {
            Some(t) => t,
            None => return err_json(StatusCode::BAD_REQUEST, format!("Key {} not owned by any shard", id)),
        };
        groups.entry(original_url.clone())
            .or_insert_with(|| (effective_url, original_url, replica_urls, Vec::new()))
            .3.push((idx, id, d.value));
    }

    let futures = groups.into_values().map(|(effective_url, original_url, replica_urls, items)| {
        let state = state.clone();
        let col_name = col_name.to_string();
        let wc_query = wc_query.to_string();
        async move {
            let body: Vec<serde_json::Value> = items.iter()
                .map(|(_, id, value)| serde_json::json!({"id": id, "value": value}))
                .collect();
            let resp = router_forward_bulk(&state, &col_name, &effective_url, &original_url, &replica_urls, &body, &wc_query).await;
            (items, resp)
        }
    });

    let results = futures::future::join_all(futures).await;

    let mut ordered: Vec<serde_json::Value> = vec![serde_json::Value::Null; n];
    for (items, resp) in results {
        match resp {
            Ok(r) => {
                let shard_results: Vec<serde_json::Value> = r.json::<serde_json::Value>().await.ok()
                    .and_then(|b| b.get("results").and_then(|v| v.as_array().cloned()))
                    .unwrap_or_default();
                if shard_results.len() == items.len() {
                    for ((idx, _, _), res) in items.iter().zip(shard_results.into_iter()) {
                        ordered[*idx] = res;
                    }
                } else {
                    for (idx, id, _) in &items {
                        ordered[*idx] = serde_json::json!({"id": id, "status": "error", "error": "malformed shard response"});
                    }
                }
            }
            Err(e) => {
                for (idx, id, _) in &items {
                    ordered[*idx] = serde_json::json!({"id": id, "status": "error", "error": e});
                }
            }
        }
    }

    // A group that was entirely unreachable put `{"status":"error"}` in its items and the answer
    // was still `201 Created`; so was a group whose writes did not meet their concern (M19).
    let all_created = ordered.iter().all(|r| r.get("status").and_then(|s| s.as_str()) == Some("created")
        && r.get("warning").is_none());
    let status = if all_created { StatusCode::CREATED } else { StatusCode::MULTI_STATUS };
    (status, Json(serde_json::json!({"results": ordered}))).into_response()
}

pub enum ReadPreference {
    /// Explicitly asked for. A node that is not the leader refuses instead of answering.
    Primary,
    /// `Primary` plus a confirmed one: the answering node establishes a read index first, so the
    /// answer cannot come from a leader that has already been replaced. See consensus/read_index.rs.
    Quorum,
    Replica,
    /// Nothing asked for: leader first, replicas after, no guarantee either way.
    Any,
}

pub fn parse_read_pref(r: Option<&str>) -> Result<ReadPreference, String> {
    match r {
        None => Ok(ReadPreference::Any),
        Some("primary") => Ok(ReadPreference::Primary),
        Some("quorum") => Ok(ReadPreference::Quorum),
        Some("replica") => Ok(ReadPreference::Replica),
        Some(other) => Err(format!(
            "unknown read preference `{}`; use `primary`, `quorum` or `replica`", other)),
    }
}

/// Forwarded so the node that answers is the one enforcing it; the router's view of who leads can
/// be stale, and its candidate list stays as it was so a promoted replica is still found.
fn forwarded_read_pref(pref: &ReadPreference) -> Option<&'static str> {
    match pref {
        ReadPreference::Primary => Some("primary"),
        ReadPreference::Quorum => Some("quorum"),
        _ => None,
    }
}

/// 409, not 400: the cursor was well formed and correct when it was issued. A key that changed
/// owners mid-scan sits behind a position that never covered it, and no amount of adapting the
/// positions recovers it — the scan has to start again. A sorted scan is not affected: its cursor
/// is a position in the sort order, which every shard answers the same way.
fn stale_ring_response() -> axum::response::Response {
    err_json(
        StatusCode::CONFLICT,
        "cursor was issued against a different shard layout; restart the scan".to_string(),
    )
}

/// 503, not 502: the read is not wrong, it is unavailable until the shard has a leader again.
pub(crate) fn no_primary_response() -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(axum::http::header::RETRY_AFTER, "1")],
        Json(serde_json::json!({
            "error": "no reachable primary for this shard; retry, or ask for read=replica",
        })),
    ).into_response()
}

/// The primary's own refusal when it is the one that refused, rather than the router's guess at
/// what every refusal meant. `read=primary` failing really is "no reachable primary", but
/// `read=quorum` can be refused by a leader that answered and said it could not confirm leadership
/// with a majority, or that its term is too new to read at -- a partition on the *leader's* side,
/// which is the opposite diagnosis (L11). Same status class and same remedy either way, so this
/// costs diagnosability only; `Retry-After` is kept because the remedy is still to retry.
pub(crate) fn refusal_response(from_primary: Option<ShardReply>) -> axum::response::Response {
    match from_primary {
        Some(reply) => {
            let json: serde_json::Value = serde_json::from_str(&reply.body)
                .unwrap_or(serde_json::Value::String(reply.body));
            (reply.status, [(axum::http::header::RETRY_AFTER, "1")], Json(json)).into_response()
        },
        None => no_primary_response(),
    }
}

fn load_score(load: NodeLoad, unknown_latency_us: u64) -> u64 {
    let latency = if load.latency_ewma_us == 0 {
        unknown_latency_us
    } else {
        load.latency_ewma_us
    };
    load.inflight.saturating_add(1).saturating_mul(latency)
}

pub(crate) fn read_targets(
    pref: &ReadPreference,
    effective_primary: &str,
    replicas: &[String],
    rr: usize,
    loads: &HashMap<String, NodeLoad>,
) -> Vec<String> {
    let mut targets = Vec::new();
    match pref {
        // Replicas stay in the list for `Primary` and `Quorum` alike: both refuse on a follower,
        // and dropping them would stop a promoted one from ever being found.
        ReadPreference::Primary | ReadPreference::Quorum | ReadPreference::Any => {
            targets.push(effective_primary.to_string());
            for r in replicas {
                targets.push(r.clone());
            }
        },
        ReadPreference::Replica => {
            let n = replicas.len();
            if n == 0 {
                targets.push(effective_primary.to_string());
            } else {
                let measured: Vec<u64> = replicas.iter()
                    .filter_map(|url| loads.get(&crate::util::node_key(url)))
                    .map(|load| load.latency_ewma_us)
                    .filter(|latency| *latency > 0)
                    .collect();
                let unknown_latency_us = if measured.is_empty() {
                    1_000
                } else {
                    measured.iter().fold(0u64, |sum, latency| sum.saturating_add(*latency))
                        / measured.len() as u64
                };
                let mut ranked: Vec<(usize, String)> = (0..n)
                    .map(|i| (i, replicas[(rr + i) % n].clone()))
                    .collect();
                ranked.sort_by_key(|(tie, url)| {
                    match loads.get(&crate::util::node_key(url)) {
                        Some(load) => (0u8, load_score(*load, unknown_latency_us), *tie),
                        None => (1u8, 0, *tie),
                    }
                });
                targets.extend(ranked.into_iter().map(|(_, url)| url));
                targets.push(effective_primary.to_string());
            }
        }
    }
    let mut seen = HashSet::new();
    targets.retain(|t| seen.insert(t.clone()));
    targets
}

pub async fn router_read_doc(state: &AppState, col_name: &str, id: &str, pref: ReadPreference) -> axum::response::Response {
    let hash = hash_key(col_name, id);
    let (effective, _original, replicas) = match state.get_effective_shard_url(hash) {
        Some(t) => t,
        None => return (StatusCode::BAD_REQUEST, "Key not owned by any shard").into_response(),
    };

    let doc = format!("/collections/{}/docs/{}", encode_path_segment(col_name), encode_path_segment(id));
    let path = match forwarded_read_pref(&pref) {
        Some(v) => format!("{}?read={}", doc, v),
        None => doc,
    };
    let rr = state.read_rr.fetch_add(1, Ordering::Relaxed);
    let loads = state.fresh_node_loads();
    let targets = read_targets(&pref, &effective, &replicas, rr, &loads);
    let mut refused = false;
    let mut primary_refusal: Option<ShardReply> = None;

    for target in targets {
        let _routed = state.track_routed_read(&target);
        let url = format!("{}{}", target, path);
        if let Ok(r) = state.client.get(&url).send().await {
            let reply = ShardReply::of(r).await;
            // Same redirect as writes. Without it a stale router reads a moved key from its old
            // owner and gets a 404, which is a wrong answer rather than a visible failure.
            if let Some(owner) = reply.redirect() {
                let retry = format!("{}{}", owner, path);
                if let Ok(r2) = state.client.get(&retry).send().await {
                    let second = ShardReply::of(r2).await;
                    if second.status.is_success() || second.status == StatusCode::NOT_FOUND {
                        return passthrough(second);
                    }
                }
                continue;
            }
            if reply.status.is_success() || reply.status == StatusCode::NOT_FOUND {
                return passthrough(reply);
            }
            if reply.status == StatusCode::SERVICE_UNAVAILABLE && forwarded_read_pref(&pref).is_some() {
                refused = true;
                if crate::util::same_endpoint(&target, &effective) {
                    primary_refusal = Some(reply);
                }
            }
        }
        state.clear_node_load(&target);
    }

    if refused {
        return refusal_response(primary_refusal);
    }
    (StatusCode::BAD_GATEWAY, "No shard node could serve the read").into_response()
}

async fn admin_call(client: &reqwest::Client, post: bool, url: &str) -> Option<(StatusCode, serde_json::Value)> {
    admin_call_with(client, if post { AdminMethod::Post } else { AdminMethod::Delete }, url, None).await
}

#[derive(Clone, Copy, PartialEq)]
enum AdminMethod {
    Get,
    Post,
    Delete,
}

async fn admin_call_with(
    client: &reqwest::Client,
    method: AdminMethod,
    url: &str,
    body: Option<&serde_json::Value>,
) -> Option<(StatusCode, serde_json::Value)> {
    let rb = match method {
        AdminMethod::Get => client.get(url),
        AdminMethod::Post => client.post(url),
        AdminMethod::Delete => client.delete(url),
    };
    let rb = match body {
        Some(b) => rb.json(b),
        None => rb,
    };
    let r = rb.send().await.ok()?;
    let status = r.status();
    let body = r.json::<serde_json::Value>().await.unwrap_or(serde_json::Value::Null);
    Some((status, body))
}

fn node_result(node: &str, outcome: Option<(StatusCode, serde_json::Value)>) -> serde_json::Value {
    match outcome {
        Some((status, body)) => serde_json::json!({
            "node": node,
            "status": status.as_u16(),
            "response": body,
        }),
        None => serde_json::json!({
            "node": node,
            "status": serde_json::Value::Null,
            "error": "unreachable",
        }),
    }
}

pub async fn router_fanout_maintenance(state: &AppState, col_name: &str, action: &str) -> axum::response::Response {
    // Replicas refuse compaction, so sending it to them would report a 403 per replica as a
    // partial failure. Snapshots are safe everywhere and every node wants its own.
    let include_replicas = action != "compact";

    let mut targets = Vec::new();
    for (original, replicas) in unique_shards(state) {
        targets.push(state.effective_primary(&original));
        if include_replicas {
            for r in replicas {
                targets.push(r);
            }
        }
    }
    targets.sort();
    targets.dedup();

    let results = futures::future::join_all(targets.into_iter().map(|node| {
        let client = state.client.clone();
        let col_name = col_name.to_string();
        let action = action.to_string();
        async move {
            let url = format!("{}/collections/{}/{}", node, encode_path_segment(&col_name), action);
            let outcome = admin_call(&client, true, &url).await;
            node_result(&node, outcome)
        }
    })).await;

    // A node whose ring share never took a key for this collection does not hold one, and since
    // reading stopped creating it on demand that is a normal answer, not a partial failure. Every
    // node saying so is the collection being nowhere, which is the client's error.
    let node_status = |r: &serde_json::Value| r.get("status").and_then(|s| s.as_u64());
    let absent = results.iter().filter(|r| node_status(r) == Some(404)).count();
    let ok = results.iter().filter(|r| node_status(r).map_or(false, |s| s < 300)).count();
    if ok == 0 && absent > 0 {
        return collection_absent_response(col_name);
    }
    let status = if ok + absent == results.len() { StatusCode::OK } else { StatusCode::MULTI_STATUS };
    (status, Json(serde_json::json!({"nodes": results}))).into_response()
}

/// One replicated admin write per shard group, sent to whichever candidate answers
/// authoritatively. `suffix` is appended to `/collections/<name>` already encoded, so a caller that
/// splices a client-supplied segment into it has to encode that segment itself.
async fn fanout_to_owners(
    state: &AppState,
    col_name: &str,
    suffix: &str,
    query: &str,
    method: AdminMethod,
    body: Option<serde_json::Value>,
) -> Vec<serde_json::Value> {
    futures::future::join_all(unique_shards(state).into_iter().map(|(original, replicas)| {
        let state = state.clone();
        let col_name = col_name.to_string();
        let query = query.to_string();
        let suffix = suffix.to_string();
        let body = body.clone();
        async move {
            let effective = state.effective_primary(&original);
            let mut candidates = vec![effective.clone()];
            if original != effective {
                candidates.push(original.clone());
            }
            candidates.extend(replicas.into_iter().filter(|r| *r != effective));

            for node in candidates {
                let url = format!("{}/collections/{}{}{}",
                    node, encode_path_segment(&col_name), suffix, query);
                if let Some((status, reply)) = admin_call_with(&state.client, method, &url, body.as_ref()).await {
                    if authoritative_write_status(status) {
                        if node != original {
                            state.set_primary_override(&original, &node);
                        }
                        return node_result(&node, Some((status, reply)));
                    }
                }
            }
            node_result(&original, None)
        }
    })).await
}

pub async fn router_fanout_drop(
    state: &AppState,
    col_name: &str,
    params: &WriteConcernParams,
) -> axum::response::Response {
    let query = wc_query_string(params);
    let results = fanout_to_owners(
        state, col_name, "", &query, AdminMethod::Delete, None).await;

    let all_ok = results.iter().all(|r| r.get("status").and_then(|s| s.as_u64()).map_or(false, |s| s < 300));
    let status = if all_ok { StatusCode::OK } else { StatusCode::MULTI_STATUS };
    (status, Json(serde_json::json!({"shards": results}))).into_response()
}

/// An index definition, to every shard group that holds the collection. A group that holds none of
/// it answers `404`, which is not a failure here for the same reason it is not one in
/// `router_fanout_maintenance` -- and not a definition either, which is why the caller records the
/// change in the cluster index catalogue: that is what reaches a group taking its first key for
/// this collection later. See `cluster::catalog`.
pub async fn router_fanout_index(
    state: &AppState,
    col_name: &str,
    suffix: &str,
    method_is_create: bool,
    body: Option<serde_json::Value>,
    params: &WriteConcernParams,
) -> axum::response::Response {
    let query = wc_query_string(params);
    let method = if method_is_create { AdminMethod::Post } else { AdminMethod::Delete };
    let results = fanout_to_owners(state, col_name, suffix, &query, method, body).await;

    let node_status = |r: &serde_json::Value| r.get("status").and_then(|s| s.as_u64());
    let absent = results.iter().filter(|r| node_status(r) == Some(404)).count();
    let ok = results.iter().filter(|r| node_status(r).map_or(false, |s| s < 300)).count();
    if ok == 0 && absent > 0 {
        return collection_absent_response(col_name);
    }
    let status = if ok + absent == results.len() { StatusCode::OK } else { StatusCode::MULTI_STATUS };
    (status, Json(serde_json::json!({"shards": results}))).into_response()
}

/// The union of what each shard group reports, since an index is defined per group and a client
/// asked the cluster. `state` is the weakest of the groups': one still building answers rows the
/// planner is not using yet.
pub async fn router_list_indexes(state: &AppState, col_name: &str) -> axum::response::Response {
    let mut targets = Vec::new();
    for (original, _) in unique_shards(state) {
        targets.push(state.effective_primary(&original));
    }
    targets.sort();
    targets.dedup();

    let per_shard = futures::future::join_all(targets.into_iter().map(|node| {
        let client = state.client.clone();
        let col_name = col_name.to_string();
        async move {
            let url = format!("{}/collections/{}/indexes", node, encode_path_segment(&col_name));
            admin_call_with(&client, AdminMethod::Get, &url, None).await
        }
    })).await;

    match merge_index_listings(per_shard.into_iter().flatten()) {
        Some(indexes) => (StatusCode::OK,
            Json(serde_json::json!({"collection": col_name, "indexes": indexes}))).into_response(),
        None => collection_absent_response(col_name),
    }
}

/// The union across the groups that answered. `None` is every one of them saying the collection is
/// not theirs, which is the client's error rather than an empty list of indexes.
fn merge_index_listings(
    replies: impl Iterator<Item = (StatusCode, serde_json::Value)>,
) -> Option<Vec<serde_json::Value>> {
    let mut merged: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut holders: HashMap<String, usize> = HashMap::new();
    let mut answered = 0usize;
    let mut absent = 0usize;

    for (status, body) in replies {
        if status == StatusCode::NOT_FOUND {
            absent += 1;
            continue;
        }
        if !status.is_success() {
            continue;
        }
        answered += 1;
        for row in body.get("indexes").and_then(|v| v.as_array()).into_iter().flatten() {
            let Some(name) = row.get("name").and_then(|n| n.as_str()) else { continue };
            *holders.entry(name.to_string()).or_default() += 1;
            match merged.get_mut(name) {
                Some(existing) => merge_index_row(existing, row),
                None => { merged.insert(name.to_string(), row.clone()); },
            }
        }
    }

    if answered == 0 && absent > 0 {
        return None;
    }
    // A group that does not hold the definition at all is not ready either, and reads as building
    // for the same reason one still filling its postings does: half the fan-out is on a scan. This
    // is what a client sees while reconciliation catches a group up that gained the collection
    // after the index was defined.
    for (name, row) in merged.iter_mut() {
        if holders.get(name).copied().unwrap_or(0) < answered {
            row["state"] = serde_json::json!("building");
        }
    }
    Some(merged.into_values().collect())
}

/// Counts add; readiness is the weaker of the two, since a client's query only uses the index on
/// the shard that has finished building it.
fn merge_index_row(into: &mut serde_json::Value, row: &serde_json::Value) {
    for field in ["documents", "values"] {
        let sum = into.get(field).and_then(|v| v.as_u64()).unwrap_or(0)
            + row.get(field).and_then(|v| v.as_u64()).unwrap_or(0);
        into[field] = serde_json::json!(sum);
    }
    if row.get("state").and_then(|s| s.as_str()) != Some("ready") {
        into["state"] = serde_json::json!("building");
    }
}

/// Rows to ask each shard for, summing to exactly `limit`: `limit / n` each and one more to the
/// first `limit % n`. A share that rounded up instead let the page exceed `limit`, and trimming it
/// afterwards would strand the trimmed rows behind the cursor their shard already moved past.
///
/// Shares are allocated over the shards still in the scan, not every shard in the ring, or a
/// drained shard would keep its share and a `limit` smaller than the ring would never reach the
/// shards behind it.
fn shard_shares(limit: usize, shards: usize) -> Vec<usize> {
    if shards == 0 {
        return Vec::new();
    }
    let base = limit / shards;
    let extra = limit % shards;
    (0..shards).map(|i| base + usize::from(i < extra)).collect()
}

enum ShardQueryOutcome {
    Page(QueryPage),
    /// Every candidate refused a `read=primary` query. Distinct from `Failed`: the shard is up.
    /// Carries the effective primary's own refusal when that is who refused, so the router does not
    /// answer for it (L11).
    NoPrimary(Option<ShardReply>),
    /// The shard holds no such collection. Distinct from an empty page only in that it carries no
    /// position, and from `Failed` in that a collection narrower than the ring is not an error.
    Absent,
    Failed,
}

/// No shard holds the collection, so the fan-out is answering for all of them.
pub(crate) fn collection_absent_response(name: &str) -> axum::response::Response {
    err_json(StatusCode::NOT_FOUND, format!("collection '{}' does not exist", name))
}

pub async fn router_query(
    state: &AppState,
    col_name: &str,
    params: &QueryParams,
    limit: usize,
    sort: &Option<SortOrder>,
    fields: &[String],
    pref: ReadPreference,
) -> axum::response::Response {
        let primary_only = forwarded_read_pref(&pref).is_some();
        let incoming: Option<ShardCursor> = if sort.is_none() {
            match params.cursor.as_deref() {
                Some(c) => match decode_cursor(c) {
                    Some(c) => Some(c),
                    None => return err_json(StatusCode::BAD_REQUEST,
                        "cursor does not belong to this query".to_string()),
                },
                None => None,
            }
        } else {
            None
        };
        let (ring, owners) = state.partitioning();
        if let Some(c) = &incoming {
            if c.ring != ring {
                return stale_ring_response();
            }
        }

        // Drained shards drop out here rather than in the loop: they must not hold a share.
        let active: Vec<(String, Vec<String>, Option<String>)> = owners.into_iter()
            .filter_map(|(original, replicas)| {
                // `incoming` is already None for a sorted query, which does not paginate.
                let after = match &incoming {
                    Some(c) => match c.positions.get(&original) {
                        Some(pos) => pos.clone(),
                        None => return None,
                    },
                    None => None,
                };
                Some((original, replicas, after))
            })
            .collect();

        let sorted = sort.is_some();
        // The merge needs keys for a sorted page whether or not the client wanted them back.
        let want_keys = params.keys.unwrap_or(false);
        let shares = if sorted {
            // The top rows may all live on one shard, so a share of limit/n would misorder the merge.
            vec![limit; active.len()]
        } else {
            shard_shares(limit, active.len())
        };

        // Carried, not dropped: a shard this page had no rows to spend on resumes on the next one.
        let mut positions: BTreeMap<String, Option<String>> = BTreeMap::new();
        let mut futures = Vec::new();
        for ((original, replicas, after), per_shard) in active.into_iter().zip(shares) {
            if per_shard == 0 {
                positions.insert(original, after);
                continue;
            }

            let effective = state.effective_primary(&original);
            let rr = state.read_rr.fetch_add(1, Ordering::Relaxed);
            let loads = state.fresh_node_loads();
            let targets = read_targets(&pref, &effective, &replicas, rr, &loads);
            let client = state.client.clone();
            let route_state = state.clone();
            let col = col_name.to_string();

            let mut q: Vec<(String, String)> = vec![("limit".to_string(), per_shard.to_string())];
            if let Some(s) = &params.start { q.push(("start".to_string(), s.clone())); }
            if let Some(e) = &params.end { q.push(("end".to_string(), e.clone())); }
            if let Some(f) = &params.filter { q.push(("filter".to_string(), f.clone())); }
            if let Some(s) = &params.sort { q.push(("sort".to_string(), s.clone())); }
            if let Some(a) = &after { q.push(("cursor".to_string(), a.clone())); }
            if let Some(v) = forwarded_read_pref(&pref) { q.push(("read".to_string(), v.to_string())); }
            if sorted || want_keys {
                q.push(("keys".to_string(), "true".to_string()));
            }
            if sorted {
                // One position covers every shard, so it is forwarded untouched rather than split.
                if let Some(c) = &params.cursor { q.push(("cursor".to_string(), c.clone())); }
            }

            let primary = effective.clone();
            futures.push(tokio::spawn(async move {
                let mut refused = false;
                let mut primary_refusal: Option<ShardReply> = None;
                let mut absent = false;
                for target in targets {
                    let _routed = route_state.track_routed_read(&target);
                    let url = format!("{}/collections/{}/query", target, encode_path_segment(&col));
                    if let Ok(res) = client.get(&url).query(&q).send().await {
                        if res.status().is_success() {
                            if let Ok(page) = res.json::<QueryPage>().await {
                                return (original, ShardQueryOutcome::Page(page));
                            }
                        } else if res.status() == StatusCode::SERVICE_UNAVAILABLE && primary_only {
                            refused = true;
                            if crate::util::same_endpoint(&target, &primary) {
                                primary_refusal = Some(ShardReply::of(res).await);
                            }
                        } else if res.status() == StatusCode::NOT_FOUND {
                            // Not a failed read: a collection whose keys never hashed here has no
                            // directory here, and reading used to be what created one.
                            absent = true;
                            break;
                        }
                    }
                    route_state.clear_node_load(&target);
                }
                let outcome = match (refused, absent) {
                    (true, _) => ShardQueryOutcome::NoPrimary(primary_refusal),
                    (_, true) => ShardQueryOutcome::Absent,
                    _ => ShardQueryOutcome::Failed,
                };
                (original, outcome)
            }));
        }

        let joined = futures::future::join_all(futures).await;

        if let Some(sort) = &sort {
            let mut lists = Vec::new();
            let mut received = 0usize;
            let mut shard_has_more = false;
            let (mut present, mut absent) = (0usize, 0usize);
            for res in joined {
                let (_original, outcome) = match res {
                    Ok(t) => t,
                    Err(_) => return (StatusCode::BAD_GATEWAY, "Shard query task failed").into_response(),
                };
                match outcome {
                    ShardQueryOutcome::Page(p) => {
                        if p.keys.len() != p.items.len() {
                            return (StatusCode::BAD_GATEWAY, "Shard returned a sorted page without keys").into_response();
                        }
                        present += 1;
                        shard_has_more |= p.next_cursor.is_some();
                        received += p.items.len();
                        lists.push(p.keys.into_iter().zip(p.items)
                            .map(|(key, value)| SortedRow { key, value })
                            .collect::<Vec<_>>());
                    },
                    ShardQueryOutcome::NoPrimary(from_primary) => return refusal_response(from_primary),
                    ShardQueryOutcome::Absent => absent += 1,
                    ShardQueryOutcome::Failed => return (StatusCode::BAD_GATEWAY, "Shard query failed").into_response(),
                }
            }
            if present == 0 && absent > 0 {
                return collection_absent_response(col_name);
            }

            let merged = kway_merge(lists, sort, limit);
            // Rows this page did not reach are either past a shard's own page or past the merge cut.
            let next_cursor = match merged.last() {
                Some(last) if shard_has_more || received > merged.len() => Some(encode_cursor(
                    &SortCursor::at(sort_position(&last.value, sort), last.key.clone()))),
                _ => None,
            };
            let keys = if want_keys { merged.iter().map(|r| r.key.clone()).collect() } else { Vec::new() };
            let items: Vec<serde_json::Value> = merged.iter().map(|r| project(&r.value, fields)).collect();
            return (StatusCode::OK, Json(QueryPage { items, next_cursor, keys })).into_response();
        }

        let mut merged = Vec::new();
        let mut keys = Vec::new();
        let (mut present, mut absent) = (0usize, 0usize);
        for res in joined {
            let (original, outcome) = match res {
                Ok(t) => t,
                Err(_) => return (StatusCode::BAD_GATEWAY, "Shard query task failed").into_response(),
            };
            match outcome {
                ShardQueryOutcome::Page(p) => {
                    present += 1;
                    for item in p.items {
                        merged.push(project(&item, fields));
                    }
                    keys.extend(p.keys);

                    if let Some(k) = p.next_cursor {
                        positions.insert(original, Some(k));
                    }
                },
                ShardQueryOutcome::NoPrimary(from_primary) => return refusal_response(from_primary),
                // No position carried either: there is nothing here to resume from next page.
                ShardQueryOutcome::Absent => absent += 1,
                ShardQueryOutcome::Failed => return (StatusCode::BAD_GATEWAY, "Shard query failed").into_response(),
            }
        }
        if present == 0 && absent > 0 && positions.is_empty() {
            return collection_absent_response(col_name);
        }

        let next_cursor = if positions.is_empty() {
            None
        } else {
            Some(encode_cursor(&ShardCursor { ring, positions }))
        };

        return (StatusCode::OK, Json(QueryPage { items: merged, next_cursor, keys })).into_response();
}

/// What one shard answered an aggregation with. `Page` is absent here because an aggregate is not
/// paginated: a shard folds its whole range or it contributes nothing.
enum ShardAggregateOutcome {
    Result(AggregateResult),
    NoPrimary(Option<ShardReply>),
    Absent,
    Refused(ShardReply),
    Failed,
}

/// Fans the aggregation out whole and merges the partials. Every shard sees the same filter and the
/// same metrics, so the merge is over groups rather than over rows.
pub async fn router_aggregate(
    state: &AppState,
    col_name: &str,
    params: &AggregateParams,
    pref: ReadPreference,
) -> axum::response::Response {
    let primary_only = forwarded_read_pref(&pref).is_some();
    let (_ring, owners) = state.partitioning();

    let mut q: Vec<(String, String)> = Vec::new();
    if let Some(v) = &params.start { q.push(("start".to_string(), v.clone())); }
    if let Some(v) = &params.end { q.push(("end".to_string(), v.clone())); }
    if let Some(v) = &params.filter { q.push(("filter".to_string(), v.clone())); }
    if let Some(v) = &params.group { q.push(("group".to_string(), v.clone())); }
    if let Some(v) = &params.metrics { q.push(("metrics".to_string(), v.clone())); }
    if let Some(v) = forwarded_read_pref(&pref) { q.push(("read".to_string(), v.to_string())); }

    let mut futures = Vec::new();
    for (original, replicas) in owners {
        let effective = state.effective_primary(&original);
        let rr = state.read_rr.fetch_add(1, Ordering::Relaxed);
        let loads = state.fresh_node_loads();
        let targets = read_targets(&pref, &effective, &replicas, rr, &loads);
        let client = state.client.clone();
        let route_state = state.clone();
        let col = col_name.to_string();
        let q = q.clone();
        let primary = effective.clone();

        futures.push(tokio::spawn(async move {
            let mut refused = false;
            let mut primary_refusal: Option<ShardReply> = None;
            let mut absent = false;
            let mut rejected: Option<ShardReply> = None;
            for target in targets {
                let _routed = route_state.track_routed_read(&target);
                let url = format!("{}/collections/{}/aggregate", target, encode_path_segment(&col));
                if let Ok(res) = client.get(&url).query(&q).send().await {
                    if res.status().is_success() {
                        if let Ok(part) = res.json::<AggregateResult>().await {
                            return ShardAggregateOutcome::Result(part);
                        }
                    } else if res.status() == StatusCode::BAD_REQUEST {
                        // The request is wrong for every shard, so retrying the next one only
                        // spends round trips to reach the same answer.
                        rejected = Some(ShardReply::of(res).await);
                        break;
                    } else if res.status() == StatusCode::SERVICE_UNAVAILABLE && primary_only {
                        refused = true;
                        if crate::util::same_endpoint(&target, &primary) {
                            primary_refusal = Some(ShardReply::of(res).await);
                        }
                    } else if res.status() == StatusCode::NOT_FOUND {
                        absent = true;
                        break;
                    }
                }
                route_state.clear_node_load(&target);
            }
            match (rejected, refused, absent) {
                (Some(reply), _, _) => ShardAggregateOutcome::Refused(reply),
                (_, true, _) => ShardAggregateOutcome::NoPrimary(primary_refusal),
                (_, _, true) => ShardAggregateOutcome::Absent,
                _ => ShardAggregateOutcome::Failed,
            }
        }));
    }

    let mut parts = Vec::new();
    let (mut present, mut absent) = (0usize, 0usize);
    for res in futures::future::join_all(futures).await {
        match res {
            Ok(ShardAggregateOutcome::Result(part)) => {
                present += 1;
                parts.push(part);
            },
            Ok(ShardAggregateOutcome::Refused(reply)) => {
                let body: serde_json::Value = serde_json::from_str(&reply.body)
                    .unwrap_or(serde_json::Value::String(reply.body));
                return (StatusCode::BAD_REQUEST, Json(body)).into_response();
            },
            Ok(ShardAggregateOutcome::NoPrimary(from_primary)) => return refusal_response(from_primary),
            Ok(ShardAggregateOutcome::Absent) => absent += 1,
            Ok(ShardAggregateOutcome::Failed) =>
                return (StatusCode::BAD_GATEWAY, "Shard aggregation failed").into_response(),
            // Partial aggregates cannot be merged into an honest total, so a lost task is an error.
            Err(_) => return (StatusCode::BAD_GATEWAY, "Shard aggregation task failed").into_response(),
        }
    }
    if present == 0 && absent > 0 {
        return collection_absent_response(col_name);
    }

    match merge_aggregates(parts) {
        Ok(merged) => (StatusCode::OK, Json(merged)).into_response(),
        Err(e) => err_json(StatusCode::BAD_REQUEST, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_loads() -> HashMap<String, NodeLoad> {
        HashMap::new()
    }

    /// M3: `ceil(limit/n)` per shard summed to more than `limit`, and the fan-out concatenated the
    /// pages without trimming. Shares that sum to `limit` make the trim unnecessary, which is the
    /// point: a trimmed row is stranded behind the cursor its shard already returned.
    #[test]
    fn shard_shares_sum_to_the_limit() {
        assert_eq!(shard_shares(10, 3), vec![4, 3, 3], "ten over three, not four each");
        assert_eq!(shard_shares(9, 3), vec![3, 3, 3]);
        assert_eq!(shard_shares(100, 1), vec![100]);
        assert_eq!(shard_shares(5, 0), Vec::<usize>::new(), "no shards is a division by zero otherwise");

        // Fewer rows than shards: the tail gets nothing this page and is carried to the next.
        assert_eq!(shard_shares(1, 3), vec![1, 0, 0]);
        assert_eq!(shard_shares(2, 3), vec![1, 1, 0]);
        assert_eq!(shard_shares(0, 3), vec![0, 0, 0]);

        for (limit, shards) in [(10, 3), (1, 8), (7, 7), (0, 4), (usize::MAX, 4), (usize::MAX, 1)] {
            let shares = shard_shares(limit, shards);
            assert_eq!(shares.len(), shards);
            assert_eq!(shares.iter().fold(0usize, |a, s| a.saturating_add(*s)), limit,
                "{} rows over {} shards", limit, shards);
            assert!(shares.iter().max().unwrap_or(&0) - shares.iter().min().unwrap_or(&0) <= 1,
                "shares differ by at most one row");
        }
    }

    #[test]
    fn read_targets_primary_prefers_leader() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];
        let t = read_targets(&ReadPreference::Primary, "http://p", &replicas, 0, &no_loads());
        assert_eq!(t, vec!["http://p", "http://r1", "http://r2"]);
    }

    #[test]
    fn read_targets_replica_prefers_replicas_and_spreads() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];

        let t0 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 0, &no_loads());
        assert_eq!(t0, vec!["http://r1", "http://r2", "http://p"]);

        let t1 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 1, &no_loads());
        assert_eq!(t1, vec!["http://r2", "http://r1", "http://p"], "round-robin rotates the starting replica");

        let t2 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 2, &no_loads());
        assert_eq!(t2, vec!["http://r1", "http://r2", "http://p"], "rotation wraps");
    }

    #[test]
    fn read_targets_replica_falls_back_to_primary_when_no_replicas() {
        let t = read_targets(&ReadPreference::Replica, "http://p", &[], 0, &no_loads());
        assert_eq!(t, vec!["http://p"]);
    }

    #[test]
    fn read_targets_dedupes_when_override_points_at_a_replica() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];
        let t = read_targets(&ReadPreference::Primary, "http://r1", &replicas, 0, &no_loads());
        assert_eq!(t, vec!["http://r1", "http://r2"], "promoted replica isn't tried twice");
    }

    #[test]
    fn replica_reads_prefer_the_lowest_estimated_queue_time() {
        let replicas = vec!["http://busy".to_string(), "http://slow".to_string(), "http://free".to_string()];
        let loads = HashMap::from([
            ("busy".to_string(), NodeLoad { inflight: 8, latency_ewma_us: 1_000 }),
            ("slow".to_string(), NodeLoad { inflight: 0, latency_ewma_us: 20_000 }),
            ("free".to_string(), NodeLoad { inflight: 0, latency_ewma_us: 2_000 }),
        ]);

        let targets = read_targets(&ReadPreference::Replica, "http://p", &replicas, 0, &loads);
        assert_eq!(targets, vec!["http://free", "http://busy", "http://slow", "http://p"]);
    }

    #[test]
    fn telemetry_ties_still_rotate() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];
        let loads = HashMap::from([
            ("r1".to_string(), NodeLoad { inflight: 1, latency_ewma_us: 1_000 }),
            ("r2".to_string(), NodeLoad { inflight: 1, latency_ewma_us: 1_000 }),
        ]);

        assert_eq!(
            read_targets(&ReadPreference::Replica, "http://p", &replicas, 1, &loads),
            vec!["http://r2", "http://r1", "http://p"],
        );
    }

    #[test]
    fn a_cold_replica_uses_the_groups_measured_latency() {
        let replicas = vec!["http://warm".to_string(), "http://cold".to_string()];
        let loads = HashMap::from([
            ("warm".to_string(), NodeLoad { inflight: 0, latency_ewma_us: 2_000 }),
            ("cold".to_string(), NodeLoad { inflight: 0, latency_ewma_us: 0 }),
        ]);

        assert_eq!(
            read_targets(&ReadPreference::Replica, "http://p", &replicas, 1, &loads),
            vec!["http://cold", "http://warm", "http://p"],
        );
    }

    /// M4: an unasked-for preference and an explicit `primary` used to be the same value, so the
    /// guarantee could not be enforced without also refusing every read that never asked for it.
    #[test]
    fn a_read_preference_is_distinguishable_from_no_preference() {
        assert!(matches!(parse_read_pref(None), Ok(ReadPreference::Any)));
        assert!(matches!(parse_read_pref(Some("primary")), Ok(ReadPreference::Primary)));
        assert!(matches!(parse_read_pref(Some("quorum")), Ok(ReadPreference::Quorum)));
        assert!(matches!(parse_read_pref(Some("replica")), Ok(ReadPreference::Replica)));
        assert!(matches!(parse_read_pref(Some("garbage")), Err(ref e) if e.contains("quorum")),
            "the error has to name every preference, or a client cannot discover this one");

        // Unfixed, `read=Primary` and `read=preimary` both silently meant primary.
        assert!(parse_read_pref(Some("garbage")).is_err());
        assert!(parse_read_pref(Some("Primary")).is_err());
        assert!(parse_read_pref(Some("")).is_err());
    }

    #[test]
    fn only_an_explicit_primary_read_is_forwarded_for_enforcement() {
        assert_eq!(forwarded_read_pref(&ReadPreference::Primary), Some("primary"));
        assert_eq!(forwarded_read_pref(&ReadPreference::Any), None, "no preference forwards nothing");
        assert_eq!(forwarded_read_pref(&ReadPreference::Replica), None);
    }

    /// The candidate list is unchanged by M4 — a promoted replica still has to be findable. What
    /// changed is that each candidate is asked to prove it leads before its answer is used.
    #[test]
    fn read_targets_no_preference_matches_primary_preferred_order() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];
        assert_eq!(
            read_targets(&ReadPreference::Any, "http://p", &replicas, 0, &no_loads()),
            read_targets(&ReadPreference::Primary, "http://p", &replicas, 0, &no_loads()),
        );
    }

    #[test]
    fn router_treats_client_errors_as_authoritative() {
        assert!(authoritative_write_status(StatusCode::OK));
        assert!(authoritative_write_status(StatusCode::CREATED));
        assert!(authoritative_write_status(StatusCode::ACCEPTED));
        assert!(authoritative_write_status(StatusCode::NOT_FOUND), "a PATCH 404 must not trigger shard failover");
        assert!(authoritative_write_status(StatusCode::BAD_REQUEST));

        assert!(!authoritative_write_status(StatusCode::FORBIDDEN), "a replica rejecting writes must trigger failover");
        assert!(!authoritative_write_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(!authoritative_write_status(StatusCode::BAD_GATEWAY));
        assert!(!authoritative_write_status(StatusCode::SERVICE_UNAVAILABLE));
    }

    /// C28: the forward spliced the decoded key straight into a URL, so `a?x=1`, `a#frag` and
    /// `a/b` all stopped being one segment. Two of them landed as the key `a` on two shards -- the
    /// router having hashed the full key and the shard having stored the truncation -- and the
    /// third 404'd on a path that matched no route.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_key_the_client_escaped_reaches_the_shard_whole() {
        use crate::test_support::{temp_root, two_shard_cluster};

        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();
        let keys = ["a", "a?x=1", "a#frag", "a/b", "a b", "a%2Fb"];

        for (i, key) in keys.iter().enumerate() {
            let url = format!("{}/collections/t/docs/{}", router.url(), encode_path_segment(key));
            let r = c.put(&url).json(&serde_json::json!({"value": {"n": i}}))
                .send().await.unwrap();
            assert_eq!(r.status(), StatusCode::CREATED, "PUT {:?}", key);
        }

        for (i, key) in keys.iter().enumerate() {
            let url = format!("{}/collections/t/docs/{}", router.url(), encode_path_segment(key));
            let r = c.get(&url).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::OK, "GET {:?}", key);
            let body = r.json::<serde_json::Value>().await.unwrap();
            assert_eq!(body["n"], i, "{:?} came back holding another key's document", key);
        }

        let listed = c.get(format!("{}/collections/t/query?limit=100&keys=true", router.url()))
            .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
        let stored: Vec<&str> = listed["keys"].as_array().unwrap().iter()
            .map(|k| k.as_str().unwrap()).collect();
        let mut want = keys.to_vec();
        want.sort();
        assert_eq!(stored, want, "the shards hold the keys they were written with: {}", listed);

    }

    /// H15's blast radius on a router. Reads stopped creating collections on demand, so a shard
    /// whose ring share never took a key for one now answers `404` — which the fan-outs used to
    /// read as a failed shard (`502`) and as a partial maintenance failure (`207`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_collection_narrower_than_the_ring_still_reads_through_the_router() {
        use crate::test_support::{put_value, temp_root, two_shard_cluster};

        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();

        // One key, so exactly one of the two shards ends up holding the collection at all.
        assert_eq!(put_value(&c, &router.url(), "narrow", "only", serde_json::json!({"v": 1}), "").await,
            StatusCode::CREATED);

        let mut direct = Vec::new();
        for base in [s1.url(), s2.url()] {
            direct.push(c.get(format!("{}/collections/narrow/docs", base))
                .send().await.unwrap().status());
        }
        assert_eq!(direct.iter().filter(|s| **s == StatusCode::NOT_FOUND).count(), 1,
            "the premise: one shard owns the key, the other has no such collection; got {:?}", direct);

        let page = c.get(format!("{}/collections/narrow/query?limit=10", router.url()))
            .send().await.unwrap();
        assert_eq!(page.status(), StatusCode::OK,
            "a shard holding none of the collection is not a shard that failed the read");
        let body = page.json::<serde_json::Value>().await.unwrap();
        assert_eq!(body["items"].as_array().unwrap().len(), 1, "{}", body);

        for action in ["compact", "snapshot"] {
            let r = c.post(format!("{}/collections/narrow/{}", router.url(), action))
                .send().await.unwrap();
            assert_eq!(r.status(), StatusCode::OK,
                "{} reported a partial failure for a node with nothing to do", action);
        }

        // Nowhere at all is the client's error, and the fan-out answers it for every shard.
        for url in [format!("{}/collections/ghost/query", router.url()),
                    format!("{}/collections/ghost/docs/k", router.url())] {
            assert_eq!(c.get(&url).send().await.unwrap().status(), StatusCode::NOT_FOUND, "{}", url);
        }
        for action in ["compact", "snapshot"] {
            let r = c.post(format!("{}/collections/ghost/{}", router.url(), action))
                .send().await.unwrap();
            assert_eq!(r.status(), StatusCode::NOT_FOUND, "{} of a collection no shard holds", action);
        }

    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ib015_absent_shard_preserves_unqueried_positions() {
        use crate::test_support::{put_value, temp_root, two_shard_cluster};

        let root = temp_root();
        let (_s1, _s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();
        let (_, owners) = router.state.as_ref().unwrap().partitioning();
        assert_eq!(owners.len(), 2);
        assert_eq!(put_value(&c, &owners[1].0, "narrow", "only",
            serde_json::json!({"v": 1}), "").await, StatusCode::CREATED);

        for (collection, filter, expected) in [
            ("narrow", "{}", Some(serde_json::json!({"v": 1}))),
            ("narrow", r#"{"v":2}"#, None),
            ("ghost", "{}", None),
        ] {
            let url = format!("{}/collections/{}/query", router.url(), collection);
            let first = c.get(&url).query(&[("limit", "1"), ("keys", "true"),
                ("filter", filter)]).send().await.unwrap();
            assert_eq!(first.status(), StatusCode::OK, "{collection}: unqueried owner remains");
            let first = first.json::<QueryPage>().await.unwrap();
            assert!(first.items.is_empty());
            assert!(first.keys.is_empty());
            let cursor = first.next_cursor.expect("unqueried owner must remain reachable");
            let decoded: ShardCursor = decode_cursor(&cursor).unwrap();
            assert_eq!(decoded.positions, BTreeMap::from([(owners[1].0.clone(), None)]));

            let last = c.get(&url).query(&[("limit", "1"), ("keys", "true"),
                ("filter", filter), ("cursor", cursor.as_str())]).send().await.unwrap();
            if collection == "ghost" {
                assert_eq!(last.status(), StatusCode::NOT_FOUND);
                continue;
            }
            assert_eq!(last.status(), StatusCode::OK);
            let last = last.json::<QueryPage>().await.unwrap();
            assert_eq!(last.items, expected.into_iter().collect::<Vec<_>>());
            assert_eq!(last.keys, if filter == "{}" { vec!["only"] } else { vec![] });
            assert!(last.next_cursor.is_none());
        }
    }

    fn listing(rows: serde_json::Value) -> (StatusCode, serde_json::Value) {
        (StatusCode::OK, serde_json::json!({"indexes": rows}))
    }

    fn row(name: &str, state: &str, documents: u64) -> serde_json::Value {
        serde_json::json!({"name": name, "field": "age", "state": state,
            "documents": documents, "values": 1})
    }

    #[test]
    fn a_cluster_wide_listing_sums_the_groups_and_takes_the_weaker_state() {
        let merged = super::merge_index_listings([
            listing(serde_json::json!([row("i", "ready", 10)])),
            listing(serde_json::json!([row("i", "building", 4)])),
        ].into_iter()).expect("both groups answered");

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["documents"], 14);
        assert_eq!(merged[0]["state"], "building",
            "a query only uses the index on the group that finished building it");
    }

    /// The half `IB-024` showed up in: a group that gained the collection after the index was
    /// defined has no definition at all, and a listing that called that "ready" would report a
    /// cluster-wide index while half the fan-out was still scanning.
    #[test]
    fn an_index_a_group_has_not_got_yet_reads_as_building() {
        let merged = super::merge_index_listings([
            listing(serde_json::json!([row("i", "ready", 10)])),
            listing(serde_json::json!([])),
        ].into_iter()).expect("both groups answered");

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["state"], "building");
    }

    #[test]
    fn a_group_holding_none_of_the_collection_is_not_a_group_that_lacks_the_index() {
        let merged = super::merge_index_listings([
            listing(serde_json::json!([row("i", "ready", 10)])),
            (StatusCode::NOT_FOUND, serde_json::Value::Null),
        ].into_iter()).expect("one group answered");
        assert_eq!(merged[0]["state"], "ready");

        assert!(super::merge_index_listings([
            (StatusCode::NOT_FOUND, serde_json::Value::Null),
        ].into_iter()).is_none(), "nowhere at all is the client's error");
    }
}
