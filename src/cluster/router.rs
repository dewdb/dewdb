//! Request forwarding, shard failover, and cross-shard fan-out.

use crate::model::{err_json, BulkDoc, CreateDoc, QueryPage, QueryParams};
use crate::query::{decode_cursor, encode_cursor, kway_merge, ShardCursor, SortSpec};
use crate::json::project;
use crate::cluster::probe::unique_shards;
use crate::metrics::NodeLoad;
use crate::ring::hash_key;
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
    async fn of(r: reqwest::Response) -> Self {
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

    let full_url = format!("{}/collections/{}/docs/{}{}", effective_url, col_name, key, wc_query);
    if let Ok(r) = build_forward(&state.client, &method, &full_url, body).send().await {
        let reply = ShardReply::of(r).await;
        // The shard says the key is not its own, which means this router's ring is behind. Its
        // answer names the owner, so one retry gets the write to the right place instead of
        // handing the client a conflict it can do nothing about.
        if let Some(owner) = reply.redirect() {
            info!(target: "router", key, %owner, "Shard redirected the write; our ring is stale");
            let retry = format!("{}/collections/{}/docs/{}{}", owner, col_name, key, wc_query);
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
            let retry_url = format!("{}/collections/{}/docs/{}{}", latest_url, col_name, key, wc_query);
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
        let fallback_url = format!("{}/collections/{}/docs/{}{}", replica, col_name, key, wc_query);
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
    let full_url = format!("{}/collections/{}/docs/bulk{}", effective_url, col_name, wc_query);
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
        let fallback_url = format!("{}/collections/{}/docs/bulk{}", replica, col_name, wc_query);
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

    (StatusCode::CREATED, Json(serde_json::json!({"results": ordered}))).into_response()
}

pub enum ReadPreference {
    Primary,
    Replica,
}

pub fn parse_read_pref(r: Option<&str>) -> ReadPreference {
    match r {
        Some("replica") => ReadPreference::Replica,
        _ => ReadPreference::Primary,
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

fn read_targets(
    pref: &ReadPreference,
    effective_primary: &str,
    replicas: &[String],
    rr: usize,
    loads: &HashMap<String, NodeLoad>,
) -> Vec<String> {
    let mut targets = Vec::new();
    match pref {
        ReadPreference::Primary => {
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
                    .filter_map(|url| loads.get(crate::util::endpoint_of(url)))
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
                    match loads.get(crate::util::endpoint_of(url)) {
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

    let path = format!("/collections/{}/docs/{}", col_name, id);
    let rr = state.read_rr.fetch_add(1, Ordering::Relaxed);
    let loads = state.fresh_node_loads();
    let targets = read_targets(&pref, &effective, &replicas, rr, &loads);

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
        }
        state.clear_node_load(&target);
    }

    (StatusCode::BAD_GATEWAY, "No shard node could serve the read").into_response()
}

async fn admin_call(client: &reqwest::Client, post: bool, url: &str) -> Option<(StatusCode, serde_json::Value)> {
    let rb = if post { client.post(url) } else { client.delete(url) };
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
            let url = format!("{}/collections/{}/{}", node, col_name, action);
            let outcome = admin_call(&client, true, &url).await;
            node_result(&node, outcome)
        }
    })).await;

    let all_ok = results.iter().all(|r| r.get("status").and_then(|s| s.as_u64()).map_or(false, |s| s < 300));
    let status = if all_ok { StatusCode::OK } else { StatusCode::MULTI_STATUS };
    (status, Json(serde_json::json!({"nodes": results}))).into_response()
}

pub async fn router_fanout_drop(state: &AppState, col_name: &str) -> axum::response::Response {
    let results = futures::future::join_all(unique_shards(state).into_iter().map(|(original, replicas)| {
        let state = state.clone();
        let col_name = col_name.to_string();
        async move {
            let effective = state.effective_primary(&original);
            let mut candidates = vec![effective.clone()];
            if original != effective {
                candidates.push(original.clone());
            }
            candidates.extend(replicas.into_iter().filter(|r| *r != effective));

            for node in candidates {
                let url = format!("{}/collections/{}", node, col_name);
                if let Some((status, body)) = admin_call(&state.client, false, &url).await {
                    if authoritative_write_status(status) {
                        if node != original {
                            state.set_primary_override(&original, &node);
                        }
                        return node_result(&node, Some((status, body)));
                    }
                }
            }
            node_result(&original, None)
        }
    })).await;

    let all_ok = results.iter().all(|r| r.get("status").and_then(|s| s.as_u64()).map_or(false, |s| s < 300));
    let status = if all_ok { StatusCode::OK } else { StatusCode::MULTI_STATUS };
    (status, Json(serde_json::json!({"shards": results}))).into_response()
}

// div_ceil, not (limit + n - 1) / n: that form overflows and wraps to 0 on a near-usize::MAX limit.
fn per_shard_limit(limit: usize, shards: usize, sorted: bool) -> usize {
    // Full limit per shard when sorted: the top rows may all live on one, and limit/n misorders the merge.
    if sorted { limit } else { limit.div_ceil(shards.max(1)).max(1) }
}

pub async fn router_query(
    state: &AppState,
    col_name: &str,
    params: &QueryParams,
    limit: usize,
    sort: &Option<SortSpec>,
    fields: &[String],
) -> axum::response::Response {
        let pref = parse_read_pref(params.read.as_deref());
        let incoming = if sort.is_none() {
            params.cursor.as_deref().and_then(decode_cursor)
        } else {
            None
        };
        let shards = unique_shards(state);
        let n = shards.len().max(1);
        let per_shard = per_shard_limit(limit, n, sort.is_some());

        let mut futures = Vec::new();
        for (original, replicas) in shards {
            let after: Option<String> = if sort.is_some() {
                None
            } else {
                match &incoming {
                    Some(c) => match c.positions.get(&original) {
                        Some(k) => Some(k.clone()),
                        None => continue,
                    },
                    None => None,
                }
            };

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

            futures.push(tokio::spawn(async move {
                for target in targets {
                    let _routed = route_state.track_routed_read(&target);
                    let url = format!("{}/collections/{}/query", target, col);
                    if let Ok(res) = client.get(&url).query(&q).send().await {
                        if res.status().is_success() {
                            if let Ok(page) = res.json::<QueryPage>().await {
                                return (original, Some(page));
                            }
                        }
                    }
                    route_state.clear_node_load(&target);
                }
                (original, None)
            }));
        }

        let joined = futures::future::join_all(futures).await;

        if let Some(sort) = &sort {
            let mut lists = Vec::new();
            for res in joined {
                let (_original, page) = match res {
                    Ok(t) => t,
                    Err(_) => return (StatusCode::BAD_GATEWAY, "Shard query task failed").into_response(),
                };
                match page {
                    Some(p) => lists.push(p.items),
                    None => return (StatusCode::BAD_GATEWAY, "Shard query failed").into_response(),
                }
            }
            let merged = kway_merge(lists, sort, limit);
            let items: Vec<serde_json::Value> = merged.iter().map(|v| project(v, fields)).collect();
            return (StatusCode::OK, Json(QueryPage { items, next_cursor: None })).into_response();
        }

        let mut merged = Vec::new();
        let mut positions = BTreeMap::new();
        for res in joined {
            let (original, page) = match res {
                Ok(t) => t,
                Err(_) => return (StatusCode::BAD_GATEWAY, "Shard query task failed").into_response(),
            };
            match page {
                Some(p) => {
                    for item in p.items {
                        merged.push(project(&item, fields));
                    }
                    if let Some(k) = p.next_cursor {
                        positions.insert(original, k);
                    }
                },
                None => return (StatusCode::BAD_GATEWAY, "Shard query failed").into_response(),
            }
        }

        let next_cursor = if positions.is_empty() {
            None
        } else {
            Some(encode_cursor(&ShardCursor { positions }))
        };

        return (StatusCode::OK, Json(QueryPage { items: merged, next_cursor })).into_response();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_loads() -> HashMap<String, NodeLoad> {
        HashMap::new()
    }

    #[test]
    fn the_per_shard_limit_never_overflows_or_collapses_to_zero() {
        assert_eq!(per_shard_limit(10, 3, false), 4, "ceil, so three shards can cover ten rows");
        assert_eq!(per_shard_limit(10, 3, true), 10, "a sorted merge needs the full limit from each");
        assert_eq!(per_shard_limit(1, 8, false), 1, "never zero: a shard asked for 0 returns nothing");
        assert_eq!(per_shard_limit(10, 0, false), 10, "no shards is a division by zero otherwise");

        // (limit + n - 1) wraps here and the old form asked every shard for 0 rows.
        assert_eq!(per_shard_limit(usize::MAX, 4, false), usize::MAX / 4 + 1);
        assert_eq!(per_shard_limit(usize::MAX, 1, false), usize::MAX);
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

    #[test]
    fn parse_read_pref_defaults_to_primary() {
        assert!(matches!(parse_read_pref(None), ReadPreference::Primary));
        assert!(matches!(parse_read_pref(Some("primary")), ReadPreference::Primary));
        assert!(matches!(parse_read_pref(Some("garbage")), ReadPreference::Primary));
        assert!(matches!(parse_read_pref(Some("replica")), ReadPreference::Replica));
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
}
