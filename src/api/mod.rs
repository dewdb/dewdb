//! HTTP surface: the route table.

pub mod collections;
pub mod docs;
pub mod internal;
pub mod members;
pub mod middleware;
pub mod observe;
pub mod ring;
pub mod write;

use collections::{compact_collection, drop_collection, list_collections, snapshot_collection};
use docs::{bulk_create_docs, create_doc, delete_doc, get_doc, list_docs, put_doc, query_docs, update_doc};
use internal::{
    cluster_update_handler, cluster_view_handler, data_summary_handler, heartbeat_handler,
    internal_drop_handler, replicate_handler, resync_handler, snapshot_handler, vote_handler,
};
use members::{join_handler, leave_handler};
use middleware::{auth_middleware, metrics_middleware};
use observe::{cluster_handler, health_handler, metrics_handler};
use ring::set_ring_handler;

use crate::state::AppState;
use axum::routing::{delete, get, post};
use axum::Router;

pub fn build_app(state: &AppState) -> Router {
    let mut app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/cluster", get(cluster_handler))
        .route("/cluster/members", post(join_handler).delete(leave_handler))
        .route("/cluster/ring", post(set_ring_handler))
        .route("/collections", get(list_collections))
        .route("/collections/:name", delete(drop_collection))
        .route("/collections/:name/compact", post(compact_collection))
        .route("/collections/:name/snapshot", post(snapshot_collection))
        .route("/collections/:name/docs", post(create_doc).get(list_docs))
        .route("/collections/:name/docs/bulk", post(bulk_create_docs))
        .route("/collections/:name/query", get(query_docs))
        .route("/collections/:name/docs/:id", get(get_doc).put(put_doc).patch(update_doc).delete(delete_doc));

    // Every role carries a cluster view, so these are not gated on being a shard the way the
    // consensus routes below are.
    app = app
        .route("/internal/cluster", get(cluster_view_handler).post(cluster_update_handler));

    if state.config.role == "shard" {
        app = app
            .route("/internal/replicate", post(replicate_handler))
            .route("/internal/snapshot", get(snapshot_handler))
            .route("/internal/resync", post(resync_handler))
            .route("/internal/vote", post(vote_handler))
            .route("/internal/drop", post(internal_drop_handler))
            .route("/internal/heartbeat", get(heartbeat_handler))
            .route("/internal/data-summary", get(data_summary_handler));
    }

    app.layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware))
        .layer(axum::middleware::from_fn_with_state(state.clone(), metrics_middleware))
        .with_state(state.clone())
}
