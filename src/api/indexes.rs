//! Secondary index administration. A definition is a replicated log entry, so these take `?w=` and
//! answer as a write does: `202` is durable and staged but not agreed, and a later leader can revoke it.

use crate::api::middleware::{client_collection, CollectionPath};
use crate::api::write::local_index_change;
use crate::cluster::catalog::schema_lock;
use crate::cluster::router::{router_fanout_index, router_list_indexes};
use crate::model::err_json;
use crate::replication::write_concern::{
    parse_write_concern, WriteConcernParams, DEFAULT_WTIMEOUT_MS,
};
use crate::state::AppState;
use crate::storage::secondary::{
    valid_field_path, valid_index_name, IndexChange, IndexSpec, MAX_FIELD_PATH_LEN,
    MAX_INDEXES_PER_COLLECTION, MAX_INDEX_NAME_LEN,
};
use crate::util::encode_path_segment;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::time::Duration;
use tracing::info;

#[derive(Deserialize)]
pub struct CreateIndex {
    pub name: String,
    pub field: String,
}

pub async fn list_indexes(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        return router_list_indexes(&state, &col_name).await;
    }

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    (StatusCode::OK, Json(serde_json::json!({
        "collection": col_name,
        "indexes": col.index_status(),
    }))).into_response()
}

pub async fn create_index(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<WriteConcernParams>,
    Json(payload): Json<CreateIndex>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        let body = serde_json::json!({"name": payload.name, "field": payload.field});
        let reply = router_fanout_index(
            &state, &col_name, "/indexes", true, Some(body), &params).await;
        // Recorded for anything but a `404`: a group that failed, or only staged its definition, is what
        // the catalogue is for, and reconciliation finishes the fan-out from it.
        if reply.status() != StatusCode::NOT_FOUND {
            state.record_index_catalog(&col_name, &IndexChange::Create {
                spec: IndexSpec { name: payload.name.clone(), field: payload.field.clone() },
            });
        }
        return reply;
    }

    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    if !valid_index_name(&payload.name) {
        return err_json(StatusCode::BAD_REQUEST, format!(
            "invalid index name: expected 1-{} of [A-Za-z0-9_-]", MAX_INDEX_NAME_LEN));
    }
    if !valid_field_path(&payload.field) {
        return err_json(StatusCode::BAD_REQUEST, format!(
            "invalid field path: expected up to {} bytes of dot-separated document members",
            MAX_FIELD_PATH_LEN));
    }
    let wc = match parse_write_concern(params.w.as_deref()) {
        Ok(wc) => wc,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let wtimeout = Duration::from_millis(params.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    let _write_gate = state.write_gate.read().await;

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    let schema = schema_lock(&state, &col_name);
    let _schema = schema.lock().await;

    // Against the definitions in force rather than the committed ones: two creates in flight would
    // otherwise both pass the bound and both be appended.
    let existing = col.active_index_specs();
    let already = existing.iter().find(|s| s.name == payload.name).cloned();
    match &already {
        Some(current) if current.field != payload.field => {
            return err_json(StatusCode::CONFLICT, format!(
                "index '{}' already indexes '{}'; drop it before redefining it",
                payload.name, current.field));
        },
        None if existing.len() >= MAX_INDEXES_PER_COLLECTION => {
            return err_json(StatusCode::CONFLICT, format!(
                "collection '{}' already has the maximum of {} secondary indexes",
                col_name, MAX_INDEXES_PER_COLLECTION));
        },
        _ => {},
    }

    let change = IndexChange::Create {
        spec: IndexSpec { name: payload.name.clone(), field: payload.field.clone() },
    };
    // Recorded before it is appended, so reconciliation is only ever behind the catalogue. The other
    // order leaves this group holding a definition the catalogue does not -- a dropped index's shape.
    state.record_index_catalog(&col_name, &change);

    if already.is_some() {
        return (StatusCode::OK, Json(serde_json::json!({
            "collection": col_name,
            "index": payload.name,
            "field": payload.field,
            "status": "exists",
        }))).into_response();
    }

    let outcome = match local_index_change(&state, &col_name, change, wc, wtimeout).await {
        Ok(o) => o,
        Err(response) => return response,
    };

    info!(target: "admin", collection = %col_name, index = %payload.name, field = %payload.field,
        acks = outcome.acks, required = outcome.required, "Secondary index defined");

    // The build runs after the entry commits, so even a `201` says the index exists, not that it
    // answers queries yet. `GET /collections/:name/indexes` is where that shows.
    let status = if outcome.met { StatusCode::CREATED } else { StatusCode::ACCEPTED };
    (status, Json(serde_json::json!({
        "collection": col_name,
        "index": payload.name,
        "field": payload.field,
        "status": if outcome.met { "created" } else { "staged" },
        "acks": outcome.acks,
        "required": outcome.required,
    }))).into_response()
}

pub async fn drop_index(
    State(state): State<AppState>,
    CollectionPath((col_name, index_name)): CollectionPath<(String, String)>,
    Query(params): Query<WriteConcernParams>,
) -> impl axum::response::IntoResponse {
    if state.config.role == "router" {
        let suffix = format!("/indexes/{}", encode_path_segment(&index_name));
        let reply = router_fanout_index(&state, &col_name, &suffix, false, None, &params).await;
        if reply.status() != StatusCode::NOT_FOUND {
            state.record_index_catalog(&col_name,
                &IndexChange::Drop { name: index_name.clone() });
        }
        return reply;
    }

    if state.is_shard() && !state.is_leader() {
        return (StatusCode::FORBIDDEN, "Replica nodes reject direct writes").into_response();
    }

    let wc = match parse_write_concern(params.w.as_deref()) {
        Ok(wc) => wc,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let wtimeout = Duration::from_millis(params.wtimeout.unwrap_or(DEFAULT_WTIMEOUT_MS));

    let _write_gate = state.write_gate.read().await;

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    let schema = schema_lock(&state, &col_name);
    let _schema = schema.lock().await;

    let existed = col.active_index_specs().iter().any(|s| s.name == index_name);
    let change = IndexChange::Drop { name: index_name.clone() };
    // Before the append, for the same reason a create is: the catalogue is the intent and the log
    // is what a group has done about it, and the reconciler follows the first.
    state.record_index_catalog(&col_name, &change);

    // Nothing to log, the way dropping an absent collection writes nothing: an entry here would
    // define an index only to remove it.
    if !existed {
        return (StatusCode::OK, Json(serde_json::json!({
            "collection": col_name,
            "index": index_name,
            "status": "dropped",
            "existed": false,
        }))).into_response();
    }

    let outcome = match local_index_change(&state, &col_name, change, wc, wtimeout).await {
        Ok(o) => o,
        Err(response) => return response,
    };

    info!(target: "admin", collection = %col_name, index = %index_name,
        acks = outcome.acks, required = outcome.required, "Secondary index dropped");

    let status = if outcome.met { StatusCode::OK } else { StatusCode::ACCEPTED };
    (status, Json(serde_json::json!({
        "collection": col_name,
        "index": index_name,
        "status": if outcome.met { "dropped" } else { "staged" },
        "existed": true,
        "acks": outcome.acks,
        "required": outcome.required,
    }))).into_response()
}

#[cfg(test)]
mod tests {
    use crate::test_support::{
        cleanup, put_doc_at, put_value, router_for, sharded_cluster, single_node, temp_root,
        voter_group, wait_for, wait_for_doc, TestNode,
    };
    use axum::http::StatusCode;
    use std::time::Duration;

    async fn create(client: &reqwest::Client, base: &str, col: &str, body: serde_json::Value)
        -> (StatusCode, serde_json::Value)
    {
        let r = client.post(format!("{}/collections/{}/indexes", base, col))
            .json(&body).send().await.unwrap();
        let status = r.status();
        (status, r.json::<serde_json::Value>().await.unwrap_or(serde_json::Value::Null))
    }

    async fn list(client: &reqwest::Client, base: &str, col: &str) -> (StatusCode, serde_json::Value) {
        let r = client.get(format!("{}/collections/{}/indexes", base, col)).send().await.unwrap();
        let status = r.status();
        (status, r.json::<serde_json::Value>().await.unwrap_or(serde_json::Value::Null))
    }

    async fn drop_one(client: &reqwest::Client, base: &str, col: &str, index: &str)
        -> (StatusCode, serde_json::Value)
    {
        let r = client.delete(format!("{}/collections/{}/indexes/{}", base, col, index))
            .send().await.unwrap();
        let status = r.status();
        (status, r.json::<serde_json::Value>().await.unwrap_or(serde_json::Value::Null))
    }

    async fn query(client: &reqwest::Client, base: &str, col: &str, q: &str) -> serde_json::Value {
        let url = format!("{}/collections/{}/query?{}", base, col, q);
        client.get(&url).send().await.unwrap().json::<serde_json::Value>().await.unwrap()
    }

    /// Blocks until every index the node reports is answering queries. A building index is
    /// maintained but not selected, so a test that queried before this would pass on the scan.
    async fn await_ready(client: &reqwest::Client, base: &str, col: &str) -> bool {
        let deadline = Duration::from_secs(20);
        let started = std::time::Instant::now();
        while started.elapsed() < deadline {
            let (status, body) = list(client, base, col).await;
            if status == StatusCode::OK {
                let rows = body["indexes"].as_array().cloned().unwrap_or_default();
                if !rows.is_empty() && rows.iter().all(|r| r["state"] == "ready") {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_index_is_created_listed_used_and_dropped() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let client = reqwest::Client::new();
        let base = node.url();

        for i in 0..12 {
            assert!(put_value(&client, &base, "t", &format!("k{:02}", i),
                serde_json::json!({"age": i % 3}), "").await.is_success());
        }

        let (status, body) = create(&client, &base, "t",
            serde_json::json!({"name": "by_age", "field": "age"})).await;
        assert_eq!(status, StatusCode::CREATED, "{}", body);
        assert!(await_ready(&client, &base, "t").await, "the build did not finish");

        let (_, listed) = list(&client, &base, "t").await;
        assert_eq!(listed["indexes"][0]["name"], "by_age");
        assert_eq!(listed["indexes"][0]["field"], "age");
        assert_eq!(listed["indexes"][0]["documents"], 12);
        assert_eq!(listed["indexes"][0]["values"], 3);

        let page = query(&client, &base, "t", "filter=%7B%22age%22%3A%201%7D&keys=true").await;
        let keys: Vec<String> = page["keys"].as_array().unwrap().iter()
            .map(|k| k.as_str().unwrap().to_string()).collect();
        assert_eq!(keys, vec!["k01", "k04", "k07", "k10"],
            "the index has to answer the same rows the scan would");

        let (status, body) = drop_one(&client, &base, "t", "by_age").await;
        assert_eq!(status, StatusCode::OK, "{}", body);
        assert_eq!(body["existed"], true);
        let (_, listed) = list(&client, &base, "t").await;
        assert_eq!(listed["indexes"].as_array().unwrap().len(), 0);

        // The rows are the collection's, not the index's: dropping one changes nothing a client sees.
        let page = query(&client, &base, "t", "filter=%7B%22age%22%3A%201%7D&keys=true").await;
        assert_eq!(page["keys"].as_array().unwrap().len(), 4);

        node.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_definition_and_its_answers_survive_a_restart() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let client = reqwest::Client::new();
        let base = node.url();

        for i in 0..8 {
            assert!(put_value(&client, &base, "t", &format!("k{}", i),
                serde_json::json!({"age": i}), "").await.is_success());
        }
        assert_eq!(create(&client, &base, "t",
            serde_json::json!({"name": "by_age", "field": "age"})).await.0, StatusCode::CREATED);
        assert!(await_ready(&client, &base, "t").await);

        node.kill();
        node.start();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let base = node.url();

        assert!(await_ready(&client, &base, "t").await,
            "the definition is durable and the postings are rebuilt from it");
        let page = query(&client, &base, "t", "filter=%7B%22age%22%3A%205%7D&keys=true").await;
        assert_eq!(page["keys"], serde_json::json!(["k5"]));

        node.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn creating_the_same_index_twice_is_idempotent_and_redefining_it_conflicts() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let client = reqwest::Client::new();
        let base = node.url();

        assert!(put_value(&client, &base, "t", "k", serde_json::json!({"age": 1}), "").await.is_success());
        assert_eq!(create(&client, &base, "t",
            serde_json::json!({"name": "i", "field": "age"})).await.0, StatusCode::CREATED);

        let (status, body) = create(&client, &base, "t",
            serde_json::json!({"name": "i", "field": "age"})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "exists", "an unchanged definition is not a second log entry");

        let (status, _) = create(&client, &base, "t",
            serde_json::json!({"name": "i", "field": "score"})).await;
        assert_eq!(status, StatusCode::CONFLICT, "a silent redefinition would change every query");

        node.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn malformed_definitions_are_refused_before_they_reach_the_log() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let client = reqwest::Client::new();
        let base = node.url();

        assert!(put_value(&client, &base, "t", "k", serde_json::json!({"age": 1}), "").await.is_success());

        for body in [
            serde_json::json!({"name": "", "field": "age"}),
            serde_json::json!({"name": "a/b", "field": "age"}),
            serde_json::json!({"name": "ok", "field": ""}),
            serde_json::json!({"name": "ok", "field": "a..b"}),
            serde_json::json!({"name": "ok", "field": "$where"}),
        ] {
            let (status, _) = create(&client, &base, "t", body.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "accepted {}", body);
        }

        let (status, _) = create(&client, &base, "ghost",
            serde_json::json!({"name": "i", "field": "age"})).await;
        assert_eq!(status, StatusCode::NOT_FOUND,
            "defining an index must not create the collection it indexes");
        assert_eq!(list(&client, &base, "ghost").await.0, StatusCode::NOT_FOUND);

        node.kill();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_an_index_that_is_not_there_writes_nothing() {
        let root = temp_root();
        let mut node = single_node(&root).await;
        let client = reqwest::Client::new();
        let base = node.url();

        assert!(put_value(&client, &base, "t", "k", serde_json::json!({"age": 1}), "").await.is_success());
        let (status, body) = drop_one(&client, &base, "t", "nothing_here").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["existed"], false);

        node.kill();
    }

    /// An index is per shard group, so the router has to define it on every one of them and read
    /// the union back. `cluster::catalog` covers the groups that gain the collection afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn a_router_defines_the_index_on_every_shard_group() {
        let root = temp_root();
        let (mut shards, mut router) = sharded_cluster(&root, 2).await;
        let client = reqwest::Client::new();
        let base = router.url();

        for i in 0..20 {
            assert!(put_value(&client, &base, "t", &format!("k{:02}", i),
                serde_json::json!({"age": i % 4}), "").await.is_success());
        }

        let (status, body) = create(&client, &base, "t",
            serde_json::json!({"name": "by_age", "field": "age"})).await;
        assert_eq!(status, StatusCode::CREATED, "every group created it, as the shard route says: {}", body);
        assert_eq!(body["shards"].as_array().unwrap().len(), 2);
        assert!(await_ready(&client, &base, "t").await, "the router unions the groups' answers");

        let (_, listed) = list(&client, &base, "t").await;
        assert_eq!(listed["indexes"].as_array().unwrap().len(), 1);
        assert_eq!(listed["indexes"][0]["documents"], 20, "counts add across the groups");

        let page = query(&client, &base, "t", "filter=%7B%22age%22%3A%202%7D&keys=true&limit=100").await;
        let mut keys: Vec<String> = page["keys"].as_array().unwrap().iter()
            .map(|k| k.as_str().unwrap().to_string()).collect();
        keys.sort();
        let expected: Vec<String> = (0..20).filter(|i| i % 4 == 2).map(|i| format!("k{:02}", i)).collect();
        assert_eq!(keys, expected, "a fan-out over indexed shards must answer what the scan did");

        assert_eq!(drop_one(&client, &base, "t", "by_age").await.0, StatusCode::OK);

        router.kill();
        for shard in shards.iter_mut() {
            shard.kill();
        }
        cleanup(&root).await;
    }

    /// IB-038: `router_fanout_index` counted every status below 300 as committed, so a client asking for
    /// `majority` could not tell a staged definition from an agreed one. The `201` half is a real create.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn a_router_reports_a_staged_definition_as_accepted_not_ok() {
        let root = temp_root();
        let mut nodes = voter_group(&root, 3, 30).await;
        let mut router = router_for(
            &root, &[(nodes[0].url(), vec![nodes[1].url(), nodes[2].url()])]).await;
        let client = reqwest::Client::new();
        let base = router.url();

        let create_wc = |name: &'static str, field: &'static str, wc: &'static str| {
            let (client, base) = (client.clone(), base.clone());
            async move {
                let r = client.post(format!("{}/collections/t/indexes{}", base, wc))
                    .json(&serde_json::json!({"name": name, "field": field}))
                    .send().await.unwrap();
                let status = r.status();
                (status, r.json::<serde_json::Value>().await.unwrap())
            }
        };

        assert_eq!(put_doc_at(&client, &base, "t", "k", 1, "").await, StatusCode::CREATED);
        assert!(wait_for_doc(&client, &base, "t", "k", 1, Duration::from_secs(10)).await,
            "the collection has to exist on the owner before a definition can reach it");

        let (status, body) = create_wc("by_v", "v", "?w=majority&wtimeout=2000").await;
        assert_eq!(status, StatusCode::CREATED, "the quorum is up, so this one committed: {}", body);
        assert_eq!(body["shards"][0]["status"], 201);

        nodes[2].kill();
        nodes[1].kill();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let (status, body) = create_wc("by_w", "w", "?w=majority&wtimeout=1000").await;
        assert_eq!(status, StatusCode::ACCEPTED,
            "the owner staged its definition, so the aggregate is pending too: {}", body);
        assert_eq!(body["shards"][0]["status"], 202);
        assert_eq!(body["shards"][0]["response"]["status"], "staged");

        // The drop half of the same fan-out, against the definition that did commit.
        let dropped = client.delete(
            format!("{}/collections/t/indexes/by_v?w=majority&wtimeout=1000", base))
            .send().await.unwrap();
        let status = dropped.status();
        let body = dropped.json::<serde_json::Value>().await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED,
            "a staged removal is not a removal a client can rely on: {}", body);
        assert_eq!(body["shards"][0]["status"], 202);
        assert_eq!(body["shards"][0]["response"]["status"], "staged");

        router.kill();
        for node in nodes.iter_mut() {
            node.kill();
        }
        cleanup(&root).await;
    }

    /// The definition is a log entry, so the node that missed it picks it up from the log rather
    /// than having to be told -- and rebuilds its own postings, which never travel.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replica_that_was_down_for_the_definition_picks_it_up_from_the_log() {
        let root = temp_root();
        let (n1, _n2, mut n3) = crate::test_support::three_node_cluster(&root).await;
        let client = reqwest::Client::new();

        for i in 0..6 {
            assert!(put_value(&client, &n1.url(), "t", &format!("k{}", i),
                serde_json::json!({"age": i}), "").await.is_success());
        }
        assert!(wait_for(Duration::from_secs(10), || {
            n3.state.as_ref().and_then(|s| s.db.as_ref())
                .and_then(|db| db.existing_collection("t"))
                .is_some_and(|c| c.index.read().unwrap().len() == 6)
        }).await, "the replica has to hold the documents before it can miss their index");

        n3.kill();

        let (status, body) = create(&client, &n1.url(), "t",
            serde_json::json!({"name": "by_age", "field": "age"})).await;
        assert_eq!(status, StatusCode::CREATED, "two of three is a majority: {}", body);

        n3.start();
        assert!(wait_for(Duration::from_secs(30), || {
            n3.state.as_ref().and_then(|s| s.db.as_ref())
                .and_then(|db| db.existing_collection("t"))
                .is_some_and(|c| c.index_status().iter().any(|s| s.name == "by_age" && s.state == "ready"))
        }).await, "the definition replicates and the replica builds its own postings");

        n3.kill();
        cleanup(&root).await;
    }

    /// A schema change is admin, the way dropping a collection is. Reading the definitions is not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_admin_tier_gates_index_definitions() {
        let root = temp_root();
        let mut node = TestNode::new("solo", crate::test_support::next_test_port(), &root, "primary");
        node.auth = serde_json::json!({"api_keys": ["client"], "admin_keys": ["root"]});
        node.start();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let base = node.url();

        let client_key = reqwest::Client::builder()
            .default_headers([(reqwest::header::HeaderName::from_static("x-api-key"),
                reqwest::header::HeaderValue::from_static("client"))].into_iter().collect())
            .build().unwrap();
        let admin_key = reqwest::Client::builder()
            .default_headers([(reqwest::header::HeaderName::from_static("x-api-key"),
                reqwest::header::HeaderValue::from_static("root"))].into_iter().collect())
            .build().unwrap();

        assert!(put_value(&client_key, &base, "t", "k", serde_json::json!({"age": 1}), "").await.is_success());

        let body = serde_json::json!({"name": "i", "field": "age"});
        assert_eq!(create(&client_key, &base, "t", body.clone()).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(list(&client_key, &base, "t").await.0, StatusCode::OK,
            "reading the definitions is data-path, like listing collections");
        assert_eq!(create(&admin_key, &base, "t", body).await.0, StatusCode::CREATED);
        assert_eq!(drop_one(&client_key, &base, "t", "i").await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(drop_one(&admin_key, &base, "t", "i").await.0, StatusCode::OK);

        node.kill();
    }
}
