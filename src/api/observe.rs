//! /metrics and /health exposition.

use crate::metrics::LATENCY_BUCKETS_MS;
use crate::model::MetricsParams;
use crate::state::AppState;
use crate::storage::{Collection, Database};
use crate::cluster::probe::unique_shards;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn collection_metrics(db: &Database) -> Vec<serde_json::Value> {
    let collections: Vec<(String, Arc<Collection>)> = {
        db.collections.read().unwrap().iter().map(|(n, c)| (n.clone(), c.clone())).collect()
    };

    collections.into_iter().map(|(name, col)| {
        let usage = col.space_usage().ok();
        let index = col.index.read().unwrap();
        let cached = index.values().filter(|e| e.inline.is_some()).count();
        let documents = index.len();
        drop(index);

        serde_json::json!({
            "name": name,
            "documents": documents,
            "last_lsn": col.last_appended_lsn(),
            "wal_bytes": usage.as_ref().map(|u| u.total_bytes),
            "live_bytes": usage.as_ref().map(|u| u.live_bytes),
            "dead_bytes": usage.as_ref().map(|u| u.dead_bytes()),
            "dead_ratio": usage.as_ref().map(|u| u.dead_ratio()),
            "applied_lsn": col.applied_lsn(),
            "pending_apply": col.pending_len(),
            "cached_documents": cached,
            "cache_bytes": col.inline_bytes.load(Ordering::Relaxed),
            "compacting": col.compacting.load(Ordering::Relaxed),
        })
    }).collect()
}

fn replication_metrics(state: &AppState) -> serde_json::Value {
    let db = match state.db.as_ref() {
        Some(db) => db,
        None => return serde_json::Value::Null,
    };

    let durable_lsn = db.durable_lsn.load(Ordering::SeqCst);
    let leader = state.is_leader();
    let term = state.current_term();

    let (primary_addr, primary_position, last_replication_secs) = match state.replication.as_ref() {
        Some(repl) => {
            let r = repl.read().unwrap();
            (
                r.primary_addr.clone(),
                r.last_known_primary_position,
                r.last_replication.map(|t| t.elapsed().as_secs()),
            )
        },
        None => (None, None, None),
    };

    let (gaps, divergences, resyncs) = state.metrics.repair_counts();

    if leader {
        let replicas = state.get_replicas();
        // Per-collection: a replica can be current on one collection and behind on another.
        let tails: Vec<(String, u64)> = {
            let open = db.collections.read().unwrap();
            open.iter().map(|(n, c)| (n.clone(), c.last_appended_lsn())).collect()
        };
        let mut per_replica: std::collections::BTreeMap<String, (serde_json::Map<String, serde_json::Value>, u64)> =
            replicas.iter().map(|url| (url.clone(), (serde_json::Map::new(), 0))).collect();
        let mut committed = serde_json::Map::new();

        for (name, tail) in &tails {
            committed.insert(name.clone(), serde_json::Value::from(state.committed_lsn(name)));
            for (url, (matched_map, worst_lag)) in per_replica.iter_mut() {
                let matched = state.matched_lsn(url, name);
                matched_map.insert(name.clone(), serde_json::Value::from(matched));
                *worst_lag = (*worst_lag).max(tail.saturating_sub(matched));
            }
        }

        let max_lag = per_replica.values().map(|(_, l)| *l).max().unwrap_or(0);
        serde_json::json!({
            "role": "primary",
            "term": term,
            "durable_lsn": durable_lsn,
            "commit_index": state.max_committed_lsn(),
            "committed_by_collection": committed,
            "last_log_term": db.last_log_term.load(Ordering::SeqCst),
            "replica_count": replicas.len(),
            "max_replica_lag": max_lag,
            "repairs": {
                "gaps": gaps,
                "divergences": divergences,
                "resyncs_triggered": resyncs,
            },
            "replicas": per_replica.into_iter().map(|(url, (matched, l))| serde_json::json!({
                "url": url,
                "matched": matched,
                "lag": l,
            })).collect::<Vec<_>>(),
        })
    } else {
        serde_json::json!({
            "role": "replica",
            "term": term,
            "durable_lsn": durable_lsn,
            "last_log_term": db.last_log_term.load(Ordering::SeqCst),
            "primary": primary_addr,
            "primary_commit_index": primary_position,
            "lag": primary_position.map(|p| p.saturating_sub(durable_lsn)),
            "seconds_since_replication": last_replication_secs,
        })
    }
}

fn prometheus_line(out: &mut String, name: &str, labels: &str, value: f64) {
    use std::fmt::Write as _;
    if labels.is_empty() {
        let _ = writeln!(out, "{} {}", name, value);
    } else {
        let _ = writeln!(out, "{}{{{}}} {}", name, labels, value);
    }
}

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"")
}

fn render_prometheus(state: &AppState, collections: &[serde_json::Value], replication: &serde_json::Value) -> String {
    let mut out = String::new();
    let node = escape_label(&state.config.node_id);

    out.push_str("# HELP dewdb_up Node is serving.\n# TYPE dewdb_up gauge\n");
    prometheus_line(&mut out, "dewdb_up", &format!("node_id=\"{}\"", node), 1.0);

    out.push_str("# HELP dewdb_uptime_seconds Seconds since process start.\n# TYPE dewdb_uptime_seconds counter\n");
    prometheus_line(&mut out, "dewdb_uptime_seconds", &format!("node_id=\"{}\"", node), state.metrics.uptime_secs() as f64);

    out.push_str("# HELP dewdb_leader Whether this node is the shard primary.\n# TYPE dewdb_leader gauge\n");
    prometheus_line(&mut out, "dewdb_leader", &format!("node_id=\"{}\"", node), if state.is_leader() { 1.0 } else { 0.0 });

    out.push_str("# HELP dewdb_term Current replication term.\n# TYPE dewdb_term gauge\n");
    prometheus_line(&mut out, "dewdb_term", &format!("node_id=\"{}\"", node), state.current_term() as f64);

    out.push_str("# HELP dewdb_collection_documents Live documents per collection.\n# TYPE dewdb_collection_documents gauge\n");
    for c in collections {
        let name = escape_label(c["name"].as_str().unwrap_or(""));
        let labels = format!("node_id=\"{}\",collection=\"{}\"", node, name);
        prometheus_line(&mut out, "dewdb_collection_documents", &labels, c["documents"].as_u64().unwrap_or(0) as f64);
    }

    out.push_str("# HELP dewdb_wal_bytes Total WAL bytes on disk per collection.\n# TYPE dewdb_wal_bytes gauge\n");
    for c in collections {
        let name = escape_label(c["name"].as_str().unwrap_or(""));
        let labels = format!("node_id=\"{}\",collection=\"{}\"", node, name);
        prometheus_line(&mut out, "dewdb_wal_bytes", &labels, c["wal_bytes"].as_u64().unwrap_or(0) as f64);
        prometheus_line(&mut out, "dewdb_wal_dead_bytes", &labels, c["dead_bytes"].as_u64().unwrap_or(0) as f64);
    }

    if let Some(reps) = replication.get("replicas").and_then(|r| r.as_array()) {
        out.push_str("# HELP dewdb_replication_lag_lsn Primary LSN minus replica acked LSN.\n# TYPE dewdb_replication_lag_lsn gauge\n");
        for r in reps {
            let url = escape_label(r["url"].as_str().unwrap_or(""));
            let labels = format!("node_id=\"{}\",replica=\"{}\"", node, url);
            prometheus_line(&mut out, "dewdb_replication_lag_lsn", &labels, r["lag"].as_u64().unwrap_or(0) as f64);
        }
    }
    if let Some(lag) = replication.get("lag").and_then(|l| l.as_u64()) {
        out.push_str("# HELP dewdb_replica_lag_lsn Known primary LSN minus this replica's LSN.\n# TYPE dewdb_replica_lag_lsn gauge\n");
        prometheus_line(&mut out, "dewdb_replica_lag_lsn", &format!("node_id=\"{}\"", node), lag as f64);
    }

    let (gaps, divergences, resyncs) = state.metrics.repair_counts();
    let node_label = format!("node_id=\"{}\"", node);
    out.push_str("# HELP dewdb_commit_index Highest LSN a quorum has acknowledged.
# TYPE dewdb_commit_index gauge
");
    prometheus_line(&mut out, "dewdb_commit_index", &node_label, state.max_committed_lsn() as f64);
    out.push_str("# HELP dewdb_durable_lsn Highest LSN this node has fsynced.
# TYPE dewdb_durable_lsn gauge
");
    prometheus_line(&mut out, "dewdb_durable_lsn", &node_label,
        state.db.as_ref().map_or(0.0, |db| db.durable_lsn.load(Ordering::SeqCst) as f64));
    out.push_str("# HELP dewdb_replication_gaps_total Replicas that reported missing frames.\n# TYPE dewdb_replication_gaps_total counter\n");
    prometheus_line(&mut out, "dewdb_replication_gaps_total", &node_label, gaps as f64);
    out.push_str("# HELP dewdb_replication_divergences_total Replicas whose log conflicted with ours.\n# TYPE dewdb_replication_divergences_total counter\n");
    prometheus_line(&mut out, "dewdb_replication_divergences_total", &node_label, divergences as f64);
    out.push_str("# HELP dewdb_replication_resyncs_total Snapshot resyncs this node triggered.\n# TYPE dewdb_replication_resyncs_total counter\n");
    prometheus_line(&mut out, "dewdb_replication_resyncs_total", &node_label, resyncs as f64);

    out.push_str("# HELP dewdb_request_duration_ms Request latency histogram.\n# TYPE dewdb_request_duration_ms histogram\n");
    for (key, stats) in state.metrics.routes_snapshot() {
        let (method, route) = key.split_once(' ').unwrap_or(("", key.as_str()));
        let base = format!("node_id=\"{}\",method=\"{}\",route=\"{}\"", node, escape_label(method), escape_label(route));
        let mut cumulative = 0u64;
        for (i, n) in stats.buckets.iter().enumerate() {
            cumulative += n;
            let le = LATENCY_BUCKETS_MS.get(i).map(|b| b.to_string()).unwrap_or_else(|| "+Inf".to_string());
            prometheus_line(&mut out, "dewdb_request_duration_ms_bucket", &format!("{},le=\"{}\"", base, le), cumulative as f64);
        }
        prometheus_line(&mut out, "dewdb_request_duration_ms_sum", &base, stats.nanos_total as f64 / 1_000_000.0);
        prometheus_line(&mut out, "dewdb_request_duration_ms_count", &base, stats.count as f64);
        prometheus_line(&mut out, "dewdb_requests_total", &base, stats.count as f64);
        prometheus_line(&mut out, "dewdb_request_errors_total", &base, stats.errors as f64);
    }

    out
}

pub async fn metrics_handler(
    State(state): State<AppState>,
    Query(params): Query<MetricsParams>,
) -> impl axum::response::IntoResponse {
    let collections = match state.db.clone() {
        Some(db) => tokio::task::spawn_blocking(move || collection_metrics(&db)).await.unwrap_or_default(),
        None => Vec::new(),
    };

    let replication = replication_metrics(&state);

    if params.format.as_deref() == Some("prometheus") {
        let body = render_prometheus(&state, &collections, &replication);
        return ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response();
    }

    let requests: serde_json::Map<String, serde_json::Value> = state.metrics.routes_snapshot()
        .into_iter()
        .map(|(key, s)| (key, serde_json::json!({
            "count": s.count,
            "errors": s.errors,
            "avg_ms": (s.avg_ms() * 1000.0).round() / 1000.0,
            "p50_ms": s.quantile_ms(0.50),
            "p95_ms": s.quantile_ms(0.95),
            "p99_ms": s.quantile_ms(0.99),
        })))
        .collect();

    let total_wal_bytes: u64 = collections.iter().filter_map(|c| c["wal_bytes"].as_u64()).sum();
    let total_documents: u64 = collections.iter().filter_map(|c| c["documents"].as_u64()).sum();

    let router = if state.config.role == "router" {
        let shards: Vec<serde_json::Value> = unique_shards(&state).into_iter().map(|(original, replicas)| {
            let effective = state.effective_primary(&original);
            serde_json::json!({
                "shard": original,
                "effective_primary": effective,
                "failed_over": effective != original,
                "replicas": replicas,
            })
        }).collect();
        serde_json::json!({ "shards": shards })
    } else {
        serde_json::Value::Null
    };

    (StatusCode::OK, Json(serde_json::json!({
        "node_id": state.config.node_id,
        "role": state.config.role,
        "leader": state.is_leader(),
        "term": state.current_term(),
        "uptime_secs": state.metrics.uptime_secs(),
        "storage": {
            "collections": collections,
            "total_collections": collections.len(),
            "total_documents": total_documents,
            "total_wal_bytes": total_wal_bytes,
        },
        "replication": replication,
        "router": router,
        "requests": requests,
    }))).into_response()
}

pub async fn health_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    let mut reasons: Vec<String> = Vec::new();

    if state.is_shard() {
        if state.db.is_none() {
            reasons.push("shard has no open database".to_string());
        }
        if !state.is_leader() {
            let (primary, since) = match state.replication.as_ref() {
                Some(repl) => {
                    let r = repl.read().unwrap();
                    (r.primary_addr.clone(), r.last_heartbeat.map(|t| t.elapsed().as_secs()))
                },
                None => (None, None),
            };
            match primary {
                None => reasons.push("replica has no known primary".to_string()),
                Some(_) => {
                    let stale_for = since.unwrap_or_else(|| state.metrics.uptime_secs());
                    if stale_for > state.config.heartbeat_timeout_secs {
                        let detail = if since.is_some() { "no primary heartbeat for" } else { "never reached primary in" };
                        reasons.push(format!("{} {}s", detail, stale_for));
                    }
                },
            }
        }
    }

    if state.config.role == "router" && state.config.shard_map.is_empty() {
        reasons.push("router has no shards configured".to_string());
    }

    let healthy = reasons.is_empty();
    let status = if healthy { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };

    (status, Json(serde_json::json!({
        "status": if healthy { "ok" } else { "degraded" },
        "node_id": state.config.node_id,
        "role": state.config.role,
        "leader": state.is_leader(),
        "term": state.current_term(),
        "uptime_secs": state.metrics.uptime_secs(),
        "collections": state.db.as_ref().map_or(0, |db| db.collections.read().unwrap().len()),
        "reasons": reasons,
    }))).into_response()
}
