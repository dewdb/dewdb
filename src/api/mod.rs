//! HTTP surface: the route table.

pub mod changes;
pub mod collections;
pub mod docs;
pub mod indexes;
pub mod internal;
pub mod members;
pub mod migrate;
pub mod middleware;
pub mod observe;
pub mod ring;
pub mod write;

use changes::stream_changes;
use collections::{compact_collection, drop_collection, list_collections, snapshot_collection};
use crate::cluster::rebalance::rebalance_status_handler;
use docs::{aggregate_docs, bulk_create_docs, create_doc, delete_doc, get_doc, list_docs, put_doc,
    query_docs, update_doc};
use indexes::{create_index, drop_index, list_indexes};
use internal::{
    cluster_update_handler, cluster_view_handler, data_summary_handler, heartbeat_handler,
    migrate_cleanup_handler, migrate_handler, migrate_reset_handler,
    migration_status_handler,
    pre_vote_handler, replicate_handler, resync_handler, snapshot_handler,
    timeout_now_handler, vote_handler,
};
use members::{
    configuration_handler, join_handler, leave_handler, set_configuration_handler,
    transfer_leadership_handler,
};
use migrate::{abort_migration_handler, migration_status, start_migration_handler};
use middleware::{auth_middleware, metrics_middleware};
use observe::{cluster_handler, health_handler, metrics_handler};
use ring::set_ring_handler;

use crate::state::AppState;
use crate::storage::frame::{MAX_INTERNAL_BODY, MAX_PUBLIC_BODY};
use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post};
use axum::Router;

pub fn build_app(state: &AppState) -> Router {
    let mut app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/cluster", get(cluster_handler))
        .route("/cluster/members", post(join_handler).delete(leave_handler))
        .route("/cluster/configuration", get(configuration_handler).post(set_configuration_handler))
        .route("/cluster/ring", post(set_ring_handler))
        .route("/cluster/transfer-leadership", post(transfer_leadership_handler))
        .route("/cluster/migrate", post(start_migration_handler)
            .get(migration_status).delete(abort_migration_handler))
        .route("/cluster/rebalance", get(rebalance_status_handler))
        .route("/collections", get(list_collections))
        .route("/collections/:name", delete(drop_collection))
        .route("/collections/:name/compact", post(compact_collection))
        .route("/collections/:name/indexes", get(list_indexes).post(create_index))
        .route("/collections/:name/indexes/:index", delete(drop_index))
        .route("/collections/:name/snapshot", post(snapshot_collection))
        .route("/collections/:name/docs", post(create_doc).get(list_docs))
        .route("/collections/:name/docs/bulk", post(bulk_create_docs))
        .route("/collections/:name/query", get(query_docs))
        .route("/collections/:name/aggregate", get(aggregate_docs))
        .route("/collections/:name/changes", get(stream_changes))
        .route("/collections/:name/docs/:id", get(get_doc).put(put_doc).patch(update_doc).delete(delete_doc));

    // Every role carries a cluster view, so these are not gated on being a shard the way the
    // consensus routes below are.
    app = app
        .route("/internal/cluster", get(cluster_view_handler).post(cluster_update_handler));

    if state.config.role == "shard" {
        app = app
            // Frame-carrying, so bounded by `storage::frame`'s chain, not the public limit: a
            // frame the log holds has to be shippable or the collection stops replicating (H13).
            .route("/internal/replicate", post(replicate_handler)
                .layer(DefaultBodyLimit::max(MAX_INTERNAL_BODY)))
            .route("/internal/snapshot", get(snapshot_handler))
            .route("/internal/resync", post(resync_handler))
            .route("/internal/vote", post(vote_handler))
            .route("/internal/pre-vote", post(pre_vote_handler))
            .route("/internal/timeout-now", post(timeout_now_handler))
            .route("/internal/heartbeat", get(heartbeat_handler))
            .route("/internal/data-summary", get(data_summary_handler))
            .route("/internal/migrate", post(migrate_handler)
                .layer(DefaultBodyLimit::max(MAX_INTERNAL_BODY)))
            .route("/internal/migrate-reset", post(migrate_reset_handler))
            .route("/internal/migrate-cleanup", post(migrate_cleanup_handler))
            .route("/internal/migration-status", get(migration_status_handler));
    }

    #[cfg(test)]
    let app = app.layer(axum::middleware::from_fn_with_state(
        state.clone(), middleware::chaos_middleware));

    // Chosen, not inherited from axum's default, and outside the route layers above so those win.
    app.layer(DefaultBodyLimit::max(MAX_PUBLIC_BODY))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware))
        .layer(axum::middleware::from_fn_with_state(state.clone(), metrics_middleware))
        .with_state(state.clone())
}
