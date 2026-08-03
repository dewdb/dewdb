//! Entry point: load config, open storage, wire shared state, start role tasks.

mod api;
mod auth;
mod cluster;
mod config;
mod consensus;
mod json;
mod logging;
mod maintenance;
mod metrics;
mod model;
mod query;
mod replication;
mod ring;
mod state;
mod storage;
mod util;

#[cfg(test)]
mod test_support;

use crate::api::build_app;
use crate::auth::build_client;
use crate::cluster::probe::{router_probe_task, ROUTER_PROBE_INTERVAL_SECS};
use crate::config::{config_warnings, NodeConfig};
use crate::consensus::{heartbeat_poll_task, ReplicationMeta, ReplicationState};
use crate::logging::init_logging;
use crate::maintenance::maintenance_task;
use crate::metrics::Metrics;
use crate::replication::snapshot::replica_sync_from_primary;
use crate::state::AppState;
use crate::storage::Database;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path = "node.json".to_string();

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" && i + 1 < args.len() {
            config_path = args[i + 1].clone();
            i += 2;
        } else {
            i += 1;
        }
    }

    let config_content = fs::read_to_string(&config_path).expect("Failed to read config file");
    let config: NodeConfig = serde_json::from_str(&config_content).expect("Invalid config JSON format");
    config.validate().expect("Invalid config map constraints");

    init_logging(&config.logging, &config.node_id);

    for warning in config_warnings(&config) {
        warn!(target: "config", "{}", warning);
    }

    info!(target: "boot", role = %config.role, shard_role = ?config.shard_role, "Booting node");

    if config.role == "router" && Path::new(&config.data_dir).exists() {
        warn!(target: "boot", "Router node should not use local storage");
    }

    let db = if config.role == "shard" {
        Some(Arc::new(Database::with_cache(&config.data_dir, config.read_cache.clone())?))
    } else {
        None
    };

    let db_clone = db.clone();
    tokio::spawn(async move {
        // Flush pending group-commit waiters before exit so their writes are durable.
        let _ = tokio::signal::ctrl_c().await;
        info!(target: "boot", "Received Ctrl-C; shutting down and forcing WAL commits");
        if let Some(d) = db_clone {
            d.force_commit_all();
        }
        std::process::exit(0);
    });

    let replication = if config.role == "shard" {
        let meta = ReplicationMeta::load(&config.data_dir);
        // A node that led before restart rejoins as a follower unless it is a solo
        // primary: the cluster may have elected someone else while it was down.
        let solo_primary = config.peers.is_empty() && config.shard_role.as_deref() == Some("primary");

        let (term, is_leader, voted_for) = if let Some(ref m) = meta {
            info!(target: "boot", "Restored replication state: term={}, was_leader={}", m.term, m.is_leader);
            if m.is_leader && !solo_primary {
                info!(target: "boot", "Rejoining as follower at term {}; the cluster may have elected a new leader", m.term);
            }
            (m.term, solo_primary, m.voted_for.clone())
        } else {
            let is_primary = config.shard_role.as_deref() == Some("primary");
            info!(target: "boot", "No prior replication state; bootstrapping as {}",
                if is_primary { "primary" } else { "replica" });
            (0, is_primary, None)
        };

        let _ = ReplicationMeta { term, is_leader, voted_for: voted_for.clone() }.save(&config.data_dir);

        Some(Arc::new(RwLock::new(ReplicationState {
            term,
            is_leader,
            voted_for,
            last_heartbeat: None,
            last_replication: None,
            was_receiving_replication: false,
            heartbeat_running: !is_leader,
            replicas: config.replicas.clone(),
            primary_addr: config.primary_addr.clone(),
            last_known_primary_position: None,
        })))
    } else {
        None
    };

    let client = build_client(&config.auth);

    if config.auth.internal_secret.is_none() && config.role == "shard" {
        warn!(target: "boot", "auth.internal_secret is not set; /internal/* endpoints accept unauthenticated requests");
    }
    if !config.auth.public_locked() {
        warn!(target: "boot", "auth.api_keys is empty; the public API accepts unauthenticated requests");
    }

    let state = AppState {
        db: db.clone(),
        config: Arc::new(config.clone()),
        client: client.clone(),
        replication,
        primary_overrides: Arc::new(std::sync::Mutex::new(HashMap::new())),
        shard_failover_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        repair_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        resyncing: Arc::new(std::sync::Mutex::new(HashSet::new())),
        read_rr: Arc::new(AtomicUsize::new(0)),
        metrics: Arc::new(Metrics::new()),
    };

    if config.shard_role.as_deref() == Some("replica") {
        if let (Some(primary_addr), Some(db)) = (&config.primary_addr, &db) {
            info!(target: "replica", "Performing full sync from primary: {}", primary_addr);

            if let Ok(entries) = fs::read_dir(&config.data_dir) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            if let Err(e) = replica_sync_from_primary(&client, primary_addr, db, name).await {
                                warn!(target: "replica", "Sync failed for '{}': {}", name, e);
                            }
                        }
                    }
                }
            }

            if let Err(e) = db.recompute_commit_index() {
                warn!(target: "replica", "Could not recompute commit index after boot sync: {}", e);
            }
        }
    }

    let app = build_app(&state);

    if config.role == "shard" && !state.is_leader() {
        info!(target: "boot", "Starting heartbeat poll task (timeout={}s, delay={}ms)",
            config.heartbeat_timeout_secs, config.election_delay_ms);
        heartbeat_poll_task(state.clone());
    }

    if config.role == "router" {
        info!(target: "boot", "Starting router primary-probe task (interval={}s)", ROUTER_PROBE_INTERVAL_SECS);
        router_probe_task(state.clone());
    }

    if let Some(ref database) = state.db {
        if config.maintenance.enabled {
            maintenance_task(database.clone(), config.maintenance.clone());
        } else {
            info!(target: "boot", "Maintenance scheduler disabled by config");
        }
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    info!(target: "boot", addr = %config.listen_addr, "Server listening");
    axum::serve(listener, app).await?;

    Ok(())
}
