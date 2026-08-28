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
mod bench;
#[cfg(test)]
mod test_support;

use crate::api::build_app;
use crate::auth::build_client;
use crate::cluster::metadata::ClusterMetadata;
use crate::cluster::migration::MigrationRuns;
use crate::cluster::probe::{router_probe_task, ROUTER_PROBE_INTERVAL_SECS};
use crate::cluster::rebalance::rebalance_task;
use crate::config::{config_warnings, NodeConfig};
use crate::consensus::{
    boot_resync, heartbeat_poll_task, progress_flush_task, seed_leader_progress, Progress,
    ReplicationMeta, ReplicationState,
};
use crate::logging::init_logging;
use crate::maintenance::maintenance_task;
use crate::metrics::Metrics;
use crate::replication::stream::replication_drive_task;
use crate::state::AppState;
use crate::storage::Database;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

fn has_collection_dirs(data_dir: &str) -> bool {
    fs::read_dir(data_dir)
        .map(|entries| entries.flatten().any(|e| e.path().is_dir()))
        .unwrap_or(false)
}

/// The durable view wins over config. Config is a bootstrap seed, so an edit to it after the first
/// boot is silently ignored -- warn loudly rather than let an operator think their change took.
fn load_cluster_view(config: &NodeConfig) -> ClusterMetadata {
    let stored = ClusterMetadata::load(&config.data_dir)
        .expect("Cannot read cluster.meta; delete it to re-seed the topology from config");

    match stored {
        Some(view) => {
            let seeded = ClusterMetadata::seed_from_config(config);
            if view.shards != seeded.shards {
                warn!(target: "boot",
                    "shard_map in config differs from the cluster view on disk (v{}); the durable \
                     view is authoritative. Delete cluster.meta to re-seed from config", view.version);
            }
            info!(target: "boot", version = view.version, members = view.members.len(),
                shards = view.shards.len(), "Loaded cluster view");
            view
        },
        None => {
            let seeded = ClusterMetadata::seed_from_config(config);
            info!(target: "boot", members = seeded.members.len(), shards = seeded.shards.len(),
                "No cluster view on disk; seeding v1 from config");
            if let Err(e) = seeded.save(&config.data_dir) {
                warn!(target: "boot", error = %e, "Could not persist the seeded cluster view");
            }
            seeded
        },
    }
}

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

    if config.allow_unsafe_ring_changes {
        warn!(target: "boot", "allow_unsafe_ring_changes is set: a ring change may reassign keys \
            on a populated cluster, and reassigned keys read as missing until their data is moved");
    }

    for warning in config_warnings(&config) {
        warn!(target: "config", "{}", warning);
    }

    info!(target: "boot", role = %config.role, shard_role = ?config.shard_role, "Booting node");

    // Routers keep cluster.meta in data_dir, so the directory existing is expected now;
    // collection subdirectories in it are not.
    if config.role == "router" && has_collection_dirs(&config.data_dir) {
        warn!(target: "boot", "Router node should not use local storage");
    }

    let cluster = Arc::new(RwLock::new(load_cluster_view(&config)));

    let db = if config.role == "shard" {
        Some(Arc::new(Database::with_cache(&config.data_dir, config.read_cache.clone())?))
    } else {
        None
    };

    let db_clone = db.clone();
    tokio::spawn(async move {
        // Pending group-commit waiters are not yet durable.
        let _ = tokio::signal::ctrl_c().await;
        info!(target: "boot", "Received Ctrl-C; shutting down and forcing WAL commits");
        if let Some(d) = db_clone {
            d.force_commit_all();
        }
        std::process::exit(0);
    });

    let replication = if config.role == "shard" {
        let meta = ReplicationMeta::load(&config.data_dir)
            .expect("Cannot read replication.meta; starting would rewind this node's term and vote");
        // A former leader rejoins as a follower unless solo; the cluster may have moved on.
        // A learner is never either: it must not reach a majority of one before it is admitted.
        let solo_primary = !config.is_learner()
            && config.peers.is_empty()
            && config.shard_role.as_deref() == Some("primary");

        let (term, is_leader, voted_for) = if let Some(ref m) = meta {
            info!(target: "boot", "Restored replication state: term={}, was_leader={}", m.term, m.is_leader);
            if m.is_leader && !solo_primary {
                info!(target: "boot", "Rejoining as follower at term {}; the cluster may have elected a new leader", m.term);
            }
            (m.term, solo_primary, m.voted_for.clone())
        } else {
            let is_primary = !config.is_learner() && config.shard_role.as_deref() == Some("primary");
            info!(target: "boot", "No prior replication state; bootstrapping as {}",
                if is_primary { "primary" } else { "replica" });
            (0, is_primary, None)
        };

        if let Err(e) = (ReplicationMeta { term, is_leader, voted_for: voted_for.clone() }).save(&config.data_dir) {
            warn!(target: "boot", "Could not persist replication state: {}; votes cannot be durably recorded", e);
        }

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
            progress: Progress::new(),
            leader_committed: HashMap::new(),
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
        node_loads: Arc::new(std::sync::Mutex::new(HashMap::new())),
        routed_reads: Arc::new(std::sync::Mutex::new(HashMap::new())),
        metrics: Arc::new(Metrics::new()),
        replication_slots: Arc::new(tokio::sync::Semaphore::new(
            config.flow_control.max_inflight_requests.max(1))),
        cluster,
        ring_cache: Arc::new(std::sync::Mutex::new(Default::default())),
        migrations: Arc::new(std::sync::Mutex::new(MigrationRuns::restored(&config.data_dir))),
        migration_write_gate: Arc::new(tokio::sync::RwLock::new(())),
    };

    if config.shard_role.as_deref() == Some("replica") {
        boot_resync(&state).await;
    }

    let app = build_app(&state);

    if config.role == "shard" {
        // A node admitted last time comes back knowing only what the durable view says.
        state.follow_from_view();
        state.react_to_migration();
        if state.is_leader() {
            seed_leader_progress(&state);
        }
        progress_flush_task(state.clone());
        replication_drive_task(state.clone());
        if config.rebalance.enabled {
            rebalance_task(state.clone(), config.rebalance.clone());
        } else {
            info!(target: "rebalance", "Automatic rebalancing disabled by config");
        }
    }

    if config.role == "shard" && !state.is_leader() {
        info!(target: "boot", "Starting heartbeat poll task (timeout={}s, delay={}ms)",
            config.heartbeat_timeout_secs, config.election_delay_ms);
        heartbeat_poll_task(state.clone());
    }

    if config.role == "router" {
        info!(target: "boot", "Starting router primary-probe task (interval={}s)", ROUTER_PROBE_INTERVAL_SECS);
        router_probe_task(state.clone());
    }

    if state.db.is_some() {
        if config.maintenance.enabled {
            maintenance_task(state.clone(), config.maintenance.clone());
        } else {
            info!(target: "boot", "Maintenance scheduler disabled by config");
        }
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    info!(target: "boot", addr = %config.listen_addr, "Server listening");
    axum::serve(listener, app).await?;

    Ok(())
}
