//! Request forwarding, shard failover, and cross-shard fan-out.

use crate::model::{err_json, BulkDoc, CreateDoc, QueryPage, QueryParams};
use crate::query::{decode_cursor, encode_cursor, kway_merge, ShardCursor, SortSpec};
use crate::json::project;
use crate::cluster::probe::unique_shards;
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

// A shard's 4xx is a real answer and must not trigger failover: a PATCH 404
// means the document is missing, not that the node is down.
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
) -> Result<reqwest::Response, axum::response::Response> {
    let hash = hash_key(col_name, key);

    let (effective_url, original_url, replica_urls) = match state.get_effective_shard_url(hash) {
        Some(t) => t,
        None => return Err((StatusCode::BAD_REQUEST, "Key not owned by any shard").into_response()),
    };

    let full_url = format!("{}/collections/{}/docs/{}{}", effective_url, col_name, key, wc_query);
    if let Ok(r) = build_forward(&state.client, &method, &full_url, body).send().await {
        if authoritative_write_status(r.status()) {
            if effective_url != original_url {
                state.set_primary_override(&original_url, &effective_url);
            }
            return Ok(r);
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
                if authoritative_write_status(r.status()) {
                    return Ok(r);
                }
            }
        }
    }

    state.primary_overrides.lock().unwrap().remove(&original_url);
    for replica in &replica_urls {
        let fallback_url = format!("{}/collections/{}/docs/{}{}", replica, col_name, key, wc_query);
        if let Ok(r) = build_forward(&state.client, &method, &fallback_url, body).send().await {
            if authoritative_write_status(r.status()) {
                state.set_primary_override(&original_url, replica);
                info!(target: "router", "Cached new primary: {} -> {}", original_url, replica);
                return Ok(r);
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

pub async fn passthrough_json(r: reqwest::Response) -> axum::response::Response {
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    let json_body: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body));
    (status, Json(json_body)).into_response()
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

fn read_targets(pref: &ReadPreference, effective_primary: &str, replicas: &[String], rr: usize) -> Vec<String> {
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
                for i in 0..n {
                    targets.push(replicas[(rr + i) % n].clone());
                }
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
    let targets = read_targets(&pref, &effective, &replicas, rr);

    for target in targets {
        let url = format!("{}{}", target, path);
        if let Ok(r) = state.client.get(&url).send().await {
            let status = r.status();
            if status.is_success() || status == StatusCode::NOT_FOUND {
                return passthrough_json(r).await;
            }
        }
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
    let mut targets = Vec::new();
    for (original, replicas) in unique_shards(state) {
        targets.push(state.effective_primary(&original));
        for r in replicas {
            targets.push(r);
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
        // Sorted queries ask each shard for the full limit: the top rows may all live on
        // one shard, so limit/n per shard would return the wrong global order.
        let per_shard = if sort.is_some() { limit } else { ((limit + n - 1) / n).max(1) };

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
            let targets = read_targets(&pref, &effective, &replicas, rr);
            let client = state.client.clone();
            let col = col_name.to_string();

            let mut q: Vec<(String, String)> = vec![("limit".to_string(), per_shard.to_string())];
            if let Some(s) = &params.start { q.push(("start".to_string(), s.clone())); }
            if let Some(e) = &params.end { q.push(("end".to_string(), e.clone())); }
            if let Some(f) = &params.filter { q.push(("filter".to_string(), f.clone())); }
            if let Some(s) = &params.sort { q.push(("sort".to_string(), s.clone())); }
            if let Some(a) = &after { q.push(("cursor".to_string(), a.clone())); }

            futures.push(tokio::spawn(async move {
                for target in targets {
                    let url = format!("{}/collections/{}/query", target, col);
                    if let Ok(res) = client.get(&url).query(&q).send().await {
                        if res.status().is_success() {
                            if let Ok(page) = res.json::<QueryPage>().await {
                                return (original, Some(page));
                            }
                        }
                    }
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

    #[test]
    fn read_targets_primary_prefers_leader() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];
        let t = read_targets(&ReadPreference::Primary, "http://p", &replicas, 0);
        assert_eq!(t, vec!["http://p", "http://r1", "http://r2"]);
    }

    #[test]
    fn read_targets_replica_prefers_replicas_and_spreads() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];

        let t0 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 0);
        assert_eq!(t0, vec!["http://r1", "http://r2", "http://p"]);

        let t1 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 1);
        assert_eq!(t1, vec!["http://r2", "http://r1", "http://p"], "round-robin rotates the starting replica");

        let t2 = read_targets(&ReadPreference::Replica, "http://p", &replicas, 2);
        assert_eq!(t2, vec!["http://r1", "http://r2", "http://p"], "rotation wraps");
    }

    #[test]
    fn read_targets_replica_falls_back_to_primary_when_no_replicas() {
        let t = read_targets(&ReadPreference::Replica, "http://p", &[], 0);
        assert_eq!(t, vec!["http://p"]);
    }

    #[test]
    fn read_targets_dedupes_when_override_points_at_a_replica() {
        let replicas = vec!["http://r1".to_string(), "http://r2".to_string()];
        let t = read_targets(&ReadPreference::Primary, "http://r1", &replicas, 0);
        assert_eq!(t, vec!["http://r1", "http://r2"], "promoted replica isn't tried twice");
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
