//! Collection administration endpoints.

use crate::api::middleware::{client_collection, CollectionPath};
use crate::api::write::local_drop;
use crate::cluster::metadata::MigrationPhase;
use crate::cluster::probe::unique_shards;
use crate::cluster::router::{router_fanout_drop, router_fanout_maintenance};
use crate::model::err_json;
use crate::replication::write_concern::{
    parse_write_concern, WriteConcernParams, DEFAULT_WTIMEOUT_MS,
};
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::collections::HashSet;
use std::io;
use std::time::Duration;
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
    match tokio::task::spawn_blocking(move || db.live_collections()).await {
        Ok(Ok(names)) => (StatusCode::OK, Json(serde_json::json!({"collections": names}))).into_response(),
        Ok(Err(e)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn drop_collection(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<WriteConcernParams>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        let reply = router_fanout_drop(&state, &col_name, &params).await;
        if reply.status().is_success() {
            state.forget_collection_indexes(&col_name);
        }
        return reply;
    }

    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }
    let _write_gate = state.write_gate.read().await;
    if state.migration().is_some_and(|migration| {
        migration.phase == MigrationPhase::Finalizing
    }) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            "collection drops pause during migration finalization",
        ).into_response();
    }

    let db = state.db.as_ref().unwrap().clone();
    let name = col_name.clone();
    let present = match tokio::task::spawn_blocking(move || db.live_collections()).await {
        Ok(Ok(names)) => names.iter().any(|n| *n == name),
        Ok(Err(e)) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    // The definitions go with the documents, and the entry is emptied rather than removed: a node
    // that missed the drop would otherwise win the merge and put them back on whoever recreates
    // the collection. Ahead of the append, so the log can only be behind the catalogue.
    state.forget_collection_indexes(&col_name);

    // Nothing to log: appending a drop here would create the collection in order to tombstone it.
    if !present {
        return (StatusCode::OK, Json(serde_json::json!({
            "collection": col_name,
            "status": "dropped",
            "existed": false,
        }))).into_response();
    }

    let wc = match parse_write_concern(params.w.as_deref()) {
        Ok(wc) => wc,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let wtimeout = Duration::from_millis(params.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    let outcome = match local_drop(&state, &col_name, wc, wtimeout).await {
        Ok(o) => o,
        Err(response) => return response,
    };

    info!(target: "admin", collection = %col_name, acks = outcome.acks,
        required = outcome.required, "Collection dropped");

    // 202 on a short quorum, as writes answer: the drop is durable and staged, and it applies
    // wherever it commits. Reporting 200 would promise a removal a later leader can still revoke.
    let status = if outcome.met { StatusCode::OK } else { StatusCode::ACCEPTED };
    (status, Json(serde_json::json!({
        "collection": col_name,
        "status": if outcome.met { "dropped" } else { "staged" },
        "existed": true,
        "acks": outcome.acks,
        "required": outcome.required,
    }))).into_response()
}

pub async fn compact_collection(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
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

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    let before = col.space_usage().ok();

    let col_clone = col.clone();
    let retention = crate::maintenance::retention_for(
        &state, &col_name, &state.config.maintenance, false);
    match tokio::task::spawn_blocking(move || col_clone.compact(retention)).await {
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
    CollectionPath(col_name): CollectionPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return router_fanout_maintenance(&state, &col_name, "snapshot").await;
    }

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
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
    use crate::test_support::{
        live_put, next_test_port, put_doc_http, temp_root, three_node_cluster,
        three_node_cluster_with_timeout, wait_for, wait_for_doc, TestNode,
    };
    use std::sync::Arc;
    use std::time::Duration;

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
        let refused = compact_collection(State(replica.clone()), CollectionPath("c".into()))
            .await.into_response();
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        assert!(replica.db.as_ref().unwrap().get_collection("c").unwrap()
            .space_usage().unwrap().dead_bytes() > 0, "and the log is untouched, not just the answer");

        let snapshotted = snapshot_collection(State(replica), CollectionPath("c".into()))
            .await.into_response();
        assert_eq!(snapshotted.status(), StatusCode::OK,
            "snapshots add a file and remove nothing, so a replica is free to take one");
    }

    /// H15: all five resolved the name with `get_collection`, which opens on miss, so a typo in a
    /// read left a directory, a commit task and a map entry behind for the process's life.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reading_a_collection_that_does_not_exist_does_not_create_it() {
        let root = temp_root();
        let mut node = TestNode::new("solo", next_test_port(), &root, "primary");
        node.start();
        let client = reqwest::Client::new();
        let base = node.url();

        // A real collection alongside it, so "nothing was created" is not just "nothing exists".
        assert!(put_doc_http(&client, &base, "k1", 1).await.is_success());

        for path in ["docs/k1", "docs", "query"] {
            let url = format!("{}/collections/ghost/{}", base, path);
            assert_eq!(client.get(&url).send().await.unwrap().status(), StatusCode::NOT_FOUND,
                "GET {} answered for a collection that is not there", url);
        }
        for action in ["compact", "snapshot"] {
            let url = format!("{}/collections/ghost/{}", base, action);
            assert_eq!(client.post(&url).send().await.unwrap().status(), StatusCode::NOT_FOUND,
                "POST {} answered for a collection that is not there", url);
        }

        let listed = collections_on(&client, &base).await.unwrap();
        assert_eq!(listed, vec!["t".to_string()],
            "a probe invented a collection and every client can see it now: {:?}", listed);
        assert!(!root.join("ghost").exists(),
            "and left the directory, the wal and a commit task behind with it");

        node.kill();
    }

    async fn collections_on(client: &reqwest::Client, base: &str) -> Option<Vec<String>> {
        let r = client.get(format!("{}/collections", base)).send().await.ok()?;
        let body = r.json::<serde_json::Value>().await.ok()?;
        Some(body["collections"].as_array()?.iter()
            .filter_map(|v| v.as_str().map(str::to_string)).collect())
    }

    fn collections_of(node: &TestNode) -> Option<Vec<String>> {
        node.state.as_ref()?.db.as_ref()?.live_collections().ok()
    }

    async fn drop_via(client: &reqwest::Client, base: &str, query: &str) -> (StatusCode, serde_json::Value) {
        let r = client.delete(format!("{}/collections/t{}", base, query)).send().await.unwrap();
        let status = r.status();
        (status, r.json::<serde_json::Value>().await.unwrap_or(serde_json::Value::Null))
    }

    /// M9: the drop fanned out best-effort and was in no log, so a replica that was down for it
    /// came back still holding the collection and nothing on the leader could tell it otherwise.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replica_that_was_down_for_the_drop_picks_it_up_from_the_log() {
        let root = temp_root();
        let (n1, _n2, mut n3) = three_node_cluster(&root).await;
        let client = reqwest::Client::new();

        assert_eq!(put_doc_http(&client, &n1.url(), "k", 1).await, StatusCode::CREATED);
        assert!(wait_for(Duration::from_secs(10), || {
            collections_of(&n3).map_or(false, |c| c.contains(&"t".to_string()))
        }).await, "the replica has to hold the collection before it can miss its drop");

        n3.kill();

        let (status, body) = drop_via(&client, &n1.url(), "?w=majority&wtimeout=3000").await;
        assert_eq!(status, StatusCode::OK, "n1 and n2 are a majority of three: {}", body);
        assert_eq!(body["existed"], true);
        assert_eq!(collections_on(&client, &n1.url()).await.unwrap(), Vec::<String>::new(),
            "the tombstone still holds the log, but a client must not see a dropped collection");

        n3.start();
        assert!(wait_for(Duration::from_secs(20), || {
            collections_of(&n3).map_or(false, |c| c.is_empty())
        }).await, "unfixed the drop reached n3 once, missed it, and was never retried");
    }

    /// M9: with the drop outside the commit index it applied locally whatever the quorum did.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_drop_that_misses_its_quorum_is_staged_not_applied() {
        let root = temp_root();
        let (n1, mut n2, mut n3) = three_node_cluster_with_timeout(&root, 30).await;
        let client = reqwest::Client::new();

        assert_eq!(put_doc_http(&client, &n1.url(), "k", 1).await, StatusCode::CREATED);
        assert!(wait_for_doc(&client, &n1.url(), "t", "k", 1, Duration::from_secs(10)).await,
            "the write has to commit while the quorum is up, or the drop is not what hides it");

        n2.kill();
        n3.kill();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let (status, body) = drop_via(&client, &n1.url(), "?w=majority&wtimeout=1000").await;
        assert_eq!(status, StatusCode::ACCEPTED, "no quorum, so the drop is durable but not applied: {}", body);
        assert_eq!(body["status"], "staged");

        assert_eq!(collections_of(&n1).unwrap(), vec!["t".to_string()],
            "unfixed the collection was already gone here while the quorum knew nothing about it");
        assert_eq!(
            client.get(format!("{}/collections/t/docs/k", n1.url())).send().await.unwrap().status(),
            StatusCode::OK,
            "an uncommitted drop must not hide committed data");
    }

    #[tokio::test]
    async fn dropping_a_collection_that_is_not_there_writes_nothing() {
        let root = temp_root();
        let mut node = crate::test_support::single_node(&root).await;
        let client = reqwest::Client::new();

        let (status, body) = drop_via(&client, &node.url(), "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["existed"], false);
        assert_eq!(collections_of(&node).unwrap(), Vec::<String>::new(),
            "a drop of nothing must not create the collection in order to tombstone it");

        node.kill();
    }

    #[tokio::test]
    async fn a_leader_still_compacts_on_request() {
        let root = temp_root();
        let leader = node(&root, true).await;

        let res = compact_collection(State(leader.clone()), CollectionPath("c".into()))
            .await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(leader.db.as_ref().unwrap().get_collection("c").unwrap()
            .space_usage().unwrap().dead_bytes(), 0);
    }
}
