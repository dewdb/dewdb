//! Collection administration endpoints.

use crate::cluster::metadata::MigrationPhase;
use crate::cluster::probe::unique_shards;
use crate::cluster::router::{router_fanout_drop, router_fanout_maintenance};
use crate::model::err_json;
use crate::replication::DropRequest;
use crate::state::AppState;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::collections::HashSet;
use std::io;
use tracing::info;

pub async fn list_collections(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        let mut targets = Vec::new();
        for (original, replicas) in unique_shards(&state) {
            targets.push((state.effective_primary(&original), replicas));
        }

        let per_shard = futures::future::join_all(targets.into_iter().map(|(primary, replicas)| {
            let client = state.client.clone();
            async move {
                let mut candidates = vec![primary];
                candidates.extend(replicas);
                for node in candidates {
                    let url = format!("{}/collections", node);
                    if let Ok(r) = client.get(&url).send().await {
                        if r.status().is_success() {
                            if let Ok(body) = r.json::<serde_json::Value>().await {
                                return body.get("collections")
                                    .and_then(|c| c.as_array().cloned())
                                    .unwrap_or_default();
                            }
                        }
                    }
                }
                Vec::new()
            }
        })).await;

        let mut names: HashSet<String> = HashSet::new();
        for list in per_shard {
            for v in list {
                if let Some(s) = v.as_str() {
                    names.insert(s.to_string());
                }
            }
        }
        let mut out: Vec<String> = names.into_iter().collect();
        out.sort();
        return (StatusCode::OK, Json(serde_json::json!({"collections": out}))).into_response();
    }

    let db = state.db.as_ref().unwrap().clone();
    match tokio::task::spawn_blocking(move || db.list_collections()).await {
        Ok(Ok(names)) => (StatusCode::OK, Json(serde_json::json!({"collections": names}))).into_response(),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn drop_collection(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return router_fanout_drop(&state, &col_name).await;
    }

    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }
    let _movement_guard = state.migration_write_gate.read().await;
    if state.migration().is_some_and(|migration| {
        migration.phase == MigrationPhase::Finalizing
    }) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            "collection drops pause during migration finalization",
        ).into_response();
    }

    let term = state.current_term();
    // Learners hold the collection too, so the drop has to reach them.
    let replicas = state.replication_targets();

    let db = state.db.as_ref().unwrap().clone();
    let name = col_name.clone();
    let existed = match tokio::task::spawn_blocking(move || db.drop_collection(&name)).await {
        Ok(Ok(e)) => e,
        Ok(Err(e)) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let acks = futures::future::join_all(replicas.iter().map(|replica| {
        let client = state.client.clone();
        let url = format!("{}/internal/drop", replica);
        let req = DropRequest { collection: col_name.clone(), term };
        async move {
            match client.post(&url).json(&req).send().await {
                Ok(r) if r.status().is_success() => true,
                _ => false,
            }
        }
    })).await;

    let replicated = acks.iter().filter(|ok| **ok).count();
    info!(target: "admin", collection = %col_name, acked = replicated, replicas = replicas.len(), "Collection dropped");

    (StatusCode::OK, Json(serde_json::json!({
        "collection": col_name,
        "status": "dropped",
        "existed": existed,
        "replicas_acked": replicated,
        "replicas": replicas.len(),
    }))).into_response()
}

pub async fn compact_collection(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return router_fanout_maintenance(&state, &col_name, "compact").await;
    }

    // Same rule the scheduler applies: compacting a replica breaks the chain repair streams over.
    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN,
            "Compaction runs on the leader only; a replica's log must stay streamable for repair")
            .into_response();
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let before = col.space_usage().ok();

    let col_clone = col.clone();
    match tokio::task::spawn_blocking(move || col_clone.compact()).await {
        Ok(Ok(())) => {
            let after = col.space_usage().ok();
            let wal_id = col.wal_writer.lock().unwrap().current_wal_id;
            (StatusCode::OK, Json(serde_json::json!({
                "collection": col_name,
                "status": "compacted",
                "wal_id": wal_id,
                "documents": col.index.read().unwrap().len(),
                "bytes_before": before.as_ref().map(|u| u.total_bytes),
                "bytes_after": after.as_ref().map(|u| u.total_bytes),
                "dead_ratio_before": before.as_ref().map(|u| u.dead_ratio()),
            }))).into_response()
        },
        Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => {
            err_json(StatusCode::CONFLICT, e.to_string())
        },
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn snapshot_collection(
    State(state): State<AppState>,
    AxumPath(col_name): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return router_fanout_maintenance(&state, &col_name, "snapshot").await;
    }

    let col = match state.db.as_ref().unwrap().get_collection(&col_name) {
        Ok(c) => c,
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let col_clone = col.clone();
    match tokio::task::spawn_blocking(move || col_clone.save_index()).await {
        Ok(Ok(saved_lsn)) => {
            (StatusCode::OK, Json(serde_json::json!({
                "collection": col_name,
                "status": "snapshotted",
                "last_lsn": saved_lsn,
                "documents": col.index.read().unwrap().len(),
            }))).into_response()
        },
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::storage::Database;
    use crate::test_support::{live_put, temp_root};
    use std::sync::Arc;

    async fn node(root: &std::path::Path, is_leader: bool) -> AppState {
        let db = Arc::new(Database::new(root).unwrap());
        let col = db.get_collection("c").unwrap();
        for v in 1..=5 {
            live_put(&col, "a", v);
        }
        let config: NodeConfig = serde_json::from_value(serde_json::json!({
            "node_id": "n1", "role": "shard", "shard_role": "primary",
            "listen_addr": "127.0.0.1:1", "data_dir": root.to_string_lossy(),
        })).unwrap();
        AppState::for_admission_test(config, db, is_leader)
    }

    /// H9: any caller could force compaction on a replica, which drops superseded frames and
    /// leaves the leader no chain to repair from — a full snapshot resync instead.
    #[tokio::test]
    async fn a_replica_refuses_compaction_but_still_snapshots() {
        let root = temp_root();

        let replica = node(&root, false).await;
        let refused = compact_collection(State(replica.clone()), AxumPath("c".into()))
            .await.into_response();
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        assert!(replica.db.as_ref().unwrap().get_collection("c").unwrap()
            .space_usage().unwrap().dead_bytes() > 0, "and the log is untouched, not just the answer");

        let snapshotted = snapshot_collection(State(replica), AxumPath("c".into()))
            .await.into_response();
        assert_eq!(snapshotted.status(), StatusCode::OK,
            "snapshots add a file and remove nothing, so a replica is free to take one");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_leader_still_compacts_on_request() {
        let root = temp_root();
        let leader = node(&root, true).await;

        let res = compact_collection(State(leader.clone()), AxumPath("c".into()))
            .await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(leader.db.as_ref().unwrap().get_collection("c").unwrap()
            .space_usage().unwrap().dead_bytes(), 0);

        let _ = std::fs::remove_dir_all(&root);
    }
}
