//! Entry point: load config, open storage, wire shared state, start role tasks.

mod aggregate;
mod api;
mod auth;
mod cdc;
mod changefeed;
mod cli;
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
mod webhook;

#[cfg(test)]
mod bench;
#[cfg(test)]
mod chaos;
#[cfg(test)]
mod soak;
#[cfg(test)]
mod test_support;

use crate::api::build_app;
use crate::auth::build_client;
use crate::cli::Cli;
use crate::cluster::catalog::index_catalog_task;
use crate::cluster::metadata::ClusterMetadata;
use crate::cluster::migration::MigrationRuns;
use crate::cluster::probe::{router_probe_task, ROUTER_PROBE_INTERVAL_SECS};
use crate::cluster::rebalance::rebalance_task;
use crate::config::{config_warnings, NodeConfig};
use crate::consensus::{
    boot_resync, heartbeat_poll_task, leader_contact_task, progress_flush_task, publish_inherited_tails,
    seed_leader_progress, Progress, ReplicationMeta, ReplicationState,
};
use crate::logging::init_logging;
use crate::maintenance::maintenance_task;
use crate::metrics::Metrics;
use crate::replication::stream::replication_drive_task;
use crate::state::AppState;
use crate::storage::Database;
use crate::webhook::{webhook_task, WebhookStore};
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

/// The one line that says which node this process is and where its state lives, emitted once the
/// listener is bound and the node is about to serve.
///
/// Two apps pointed at the same default port is a silent mistake: the second one writes its
/// collections into the first one's database and nothing in the log says so. `/health` has carried
/// `node_id` all along, but only for someone who already suspects it. Fields rather than a
/// preformatted string, so `logging.format = "json"` keeps them separate.
///
/// Config is the source: no secret, no key, and nothing derived is in here.
fn log_listening(config: &NodeConfig) {
    // Every shard reports the role it runs as, including the one that left `shard_role` out of its
    // file -- an operator reading the log cares which node is leading, not which fields were typed.
    // The answer comes from the same accessor the boot path leads with, never restated here.
    // A router has no shard role at all, so it has none to report.
    match config.effective_shard_role() {
        Some(shard_role) => info!(target: "boot",
            node_id = %config.node_id, role = %config.role, shard_role = %shard_role,
            listen_addr = %config.listen_addr, data_dir = %config.data_dir, "listening"),
        None => info!(target: "boot",
            node_id = %config.node_id, role = %config.role,
            listen_addr = %config.listen_addr, data_dir = %config.data_dir, "listening"),
    }
}

/// Between reads of the config file for a credential change. A rotation is operator-driven, so
/// this is a bound on noticing one rather than something a request waits on.
const AUTH_RELOAD_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Re-reads `auth.api_keys` and `auth.admin_keys` when the config file changes, ending the streams and
/// subscriptions opened under a removed key (IB-026). Nothing else in the file is re-read.
fn auth_reload_task(state: AppState, path: String) {
    tokio::spawn(async move {
        let mut seen = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
        loop {
            tokio::time::sleep(AUTH_RELOAD_INTERVAL).await;
            let stamp = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
            if stamp == seen {
                continue;
            }
            seen = stamp;
            match reload_auth(&path) {
                Ok(next) => {
                    if next.internal_secret != state.config.auth.internal_secret
                        || next.upstream_api_key != state.config.auth.upstream_api_key {
                        warn!(target: "auth",
                            "auth.internal_secret and auth.upstream_api_key are wired into this \
                             node's outbound clients at boot and were not reloaded; restart the \
                             node to change either");
                    }
                    if state.rotate_client_keys(next.api_keys.clone(), next.admin_keys.clone()) {
                        info!(target: "auth", api_keys = next.api_keys.len(),
                            admin_keys = next.admin_keys.len(),
                            "Reloaded the credential set from the config file");
                    }
                },
                // The set in force is left alone: a half-written file must not unlock the API.
                Err(e) => warn!(target: "auth", path = %path, error = %e,
                    "Could not re-read the credential set; keeping the one in force"),
            }
        }
    });
}

fn reload_auth(path: &str) -> Result<crate::auth::AuthConfig, String> {
    let raw = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let file: serde_json::Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    let auth: crate::auth::AuthConfig = match file.get("auth") {
        Some(section) => serde_json::from_value(section.clone()).map_err(|e| e.to_string())?,
        None => Default::default(),
    };
    auth.validate()?;
    Ok(auth)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Answered before the config file is read: asking a binary what it is, how to run it, or to
    // write a config has to work on a host that has no dew.json yet.
    let config_path = match cli::parse(&args) {
        Cli::Version => {
            println!("dewdb {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        },
        Cli::Help => {
            print!("{}", cli::usage());
            return Ok(());
        },
        Cli::Init(config) => match cli::init(&config) {
            Ok(note) => {
                println!("{}", note);
                return Ok(());
            },
            Err(e) => {
                eprintln!("{}", e);
                std::process::exit(cli::EXIT_INIT_FAILED);
            },
        },
        Cli::Start(config) => config,
    };

    // A config that is not there is the user's typo, not a bug in the node: say which path and stop.
    let config_content = match cli::read_config(&config_path) {
        Ok(body) => body,
        Err(e) => {
            eprintln!("{}", e.message());
            std::process::exit(cli::EXIT_NO_CONFIG);
        },
    };
    let config_path = config_path.path;

    // Same for a config that is there and wrong. The reasons are the ones these two have always
    // given -- serde's line and column, and the rule `validate` refused on -- without the panic
    // frame and backtrace note wrapped around them, which never told the writer of the file anything.
    let config: NodeConfig = match serde_json::from_str(&config_content) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Invalid config JSON format: {}", e);
            std::process::exit(cli::EXIT_BAD_CONFIG);
        },
    };
    if let Err(e) = config.validate() {
        eprintln!("Invalid config map constraints: {}", e);
        std::process::exit(cli::EXIT_BAD_CONFIG);
    }

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
        Some(Arc::new(Database::with_config(
            &config.data_dir, config.read_cache.clone(), config.changefeed.clone())?))
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
        let solo_primary = config.runs_as_primary() && config.peers.is_empty();

        let (term, is_leader, voted_for) = if let Some(ref m) = meta {
            info!(target: "boot", "Restored replication state: term={}, was_leader={}", m.term, m.is_leader);
            if m.is_leader && !solo_primary {
                info!(target: "boot", "Rejoining as follower at term {}; the cluster may have elected a new leader", m.term);
            }
            (m.term, solo_primary, m.voted_for.clone())
        } else {
            let is_primary = config.runs_as_primary();
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
            leases: Default::default(),
            handing_over: false,
            novote_until: None,
            booted_at: std::time::Instant::now(),
            leader_matched: HashMap::new(),
            configuration: None,
        })))
    } else {
        None
    };

    let client = build_client(&config.auth, &config.own_url());

    if config.auth.internal_secret.is_none() && config.role == "shard" {
        warn!(target: "boot", "auth.internal_secret is not set; /internal/* endpoints accept unauthenticated requests");
    }
    if !config.auth.public_locked() {
        warn!(target: "boot", "auth.api_keys is empty; the public API accepts unauthenticated requests");
    }
    if config.auth.admin_locked() {
        if !config.auth.public_locked() {
            warn!(target: "boot",
                "auth.admin_keys is set but auth.api_keys is empty; only /cluster/* and collection drops need a key");
        }
    } else if config.auth.public_locked() {
        warn!(target: "boot",
            "auth.admin_keys is empty; any auth.api_keys entry can change cluster topology and drop collections");
    }

    let state = AppState {
        db: db.clone(),
        auth: Arc::new(RwLock::new(config.auth.clone())),
        config: Arc::new(config.clone()),
        client: client.clone(),
        stream_client: crate::auth::build_stream_client(&config.auth, &config.own_url()),
        replication,
        primary_overrides: Arc::new(std::sync::Mutex::new(HashMap::new())),
        shard_failover_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        repair_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        resyncing: Arc::new(std::sync::Mutex::new(HashSet::new())),
        read_rr: Arc::new(AtomicUsize::new(0)),
        node_loads: Arc::new(std::sync::Mutex::new(HashMap::new())),
        routed_reads: Arc::new(std::sync::Mutex::new(HashMap::new())),
        frame_reservations: Arc::new(std::sync::Mutex::new(HashMap::new())),
        metrics: Arc::new(Metrics::new()),
        replication_slots: Arc::new(tokio::sync::Semaphore::new(
            config.flow_control.max_inflight_requests.max(1))),
        scan_slots: Arc::new(tokio::sync::Semaphore::new(crate::aggregate::MAX_CONCURRENT_SCANS)),
        cluster,
        ring_cache: Arc::new(std::sync::Mutex::new(Default::default())),
        migrations: Arc::new(std::sync::Mutex::new(MigrationRuns::restored(&config.data_dir))),
        webhooks: Arc::new(WebhookStore::restored(&config.data_dir)),
        write_gate: Arc::new(tokio::sync::RwLock::new(())),
        campaign: Arc::new(tokio::sync::Mutex::new(())),
        election_history: Arc::new(tokio::sync::RwLock::new(())),
        membership_changes: Arc::new(Default::default()),
    };

    if config.shard_role.as_deref() == Some("replica") {
        boot_resync(&state).await;
    }

    let app = build_app(&state);

    if config.role == "shard" {
        // A node admitted last time comes back knowing only what the durable view says.
        state.follow_from_view();
        // Before any task can decide anything: the configuration in force is a log entry, and this
        // node's own log is the only copy of it that survived the restart.
        state.refresh_configuration();
        state.react_to_migration();
        if state.is_leader() {
            seed_leader_progress(&state);
            // A solo primary booting on an uncommitted tail is the same stranded state a promotion
            // inherits, and has the same floor to set.
            publish_inherited_tails(&state);
            // Same for a change the previous process was in the middle of.
            crate::consensus::reconfigure::resume_change(&state);
        }
        progress_flush_task(state.clone());
        leader_contact_task(state.clone());
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

    // Both roles: a shard reconciles its own definitions, and a router is the only node that can
    // name every shard group, so it is what carries a definition between them.
    index_catalog_task(state.clone());
    webhook_task(state.clone());
    auth_reload_task(state.clone(), config_path.clone());

    if state.db.is_some() {
        if config.maintenance.enabled {
            maintenance_task(state.clone(), config.maintenance.clone());
        } else {
            info!(target: "boot", "Maintenance scheduler disabled by config");
        }
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    log_listening(&config);
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{log_listening, reload_auth};
    use crate::logging::{render_events, LoggingConfig};
    use std::fs;

    fn config_from(json: &str) -> crate::config::NodeConfig {
        let config: crate::config::NodeConfig = serde_json::from_str(json).expect("a valid config");
        config.validate().expect("a config the node would accept");
        config
    }

    fn rendered(format: &str, json: &str) -> String {
        let config = config_from(json);
        let cfg = LoggingConfig { level: "info".to_string(), format: format.to_string() };
        render_events(&cfg, &config.node_id, || log_listening(&config))
    }

    /// The wrong-node mistake: two apps on one machine, one default port, and the second one seeding
    /// its collections into the first one's database. This line is what makes that visible at a
    /// glance, so it has to carry the identity and the storage location as separate greppable fields.
    #[test]
    fn the_startup_line_names_the_node_and_where_its_state_lives() {
        let line = rendered("text", r#"{"node_id":"kanban-1","role":"shard","shard_role":"primary",
            "listen_addr":"127.0.0.1:18081","data_dir":"./data"}"#);

        assert_eq!(line.lines().count(), 1, "one event, not several: {:?}", line);
        assert!(line.contains(" listening "), "the word to grep for: {}", line);
        for field in ["node_id=kanban-1", "role=shard", "shard_role=primary",
                      "listen_addr=127.0.0.1:18081", "data_dir=./data"] {
            assert!(line.contains(field), "missing {:?} in {:?}", field, line);
        }
        assert!(line.contains("[boot]"), "it belongs to the boot target: {}", line);
    }

    /// `logging.format = "json"` has to keep them as fields rather than one preformatted string,
    /// which is the whole reason they are recorded as fields and not interpolated into the message.
    #[test]
    fn the_startup_line_stays_structured_under_json_logging() {
        let line = rendered("json", r#"{"node_id":"kanban-1","role":"shard","shard_role":"primary",
            "listen_addr":"127.0.0.1:18081","data_dir":"./data"}"#);
        assert_eq!(line.lines().count(), 1, "one event, not several: {:?}", line);

        let event: serde_json::Value = serde_json::from_str(line.trim()).expect("one JSON object");
        assert_eq!(event["message"], "listening");
        assert_eq!(event["node_id"], "kanban-1");
        assert_eq!(event["role"], "shard");
        assert_eq!(event["shard_role"], "primary");
        assert_eq!(event["listen_addr"], "127.0.0.1:18081");
        assert_eq!(event["data_dir"], "./data");
        assert_eq!(event["target"], "boot");
        assert_eq!(event["level"], "INFO");
    }

    /// Every shard says which role it runs as; a router has none to say.
    ///
    /// The omitted case is the point: `shard_role` is optional, and an operator reading the log wants
    /// to know which node is leading, not which fields someone typed. The expected value is not
    /// hardcoded here -- it is read back from `runs_as_primary`, the same call the boot path leads
    /// with, so if that default ever changes this test follows it instead of pinning the old answer.
    #[test]
    fn every_shard_reports_the_role_it_runs_as_and_a_router_reports_none() {
        let shard = |json: &str| -> serde_json::Value {
            let line = rendered("json", json);
            assert_eq!(line.lines().count(), 1, "one event, not several: {:?}", line);
            serde_json::from_str(line.trim()).expect("one JSON object")
        };
        let runtime_role = |json: &str| -> &'static str {
            let config = config_from(json);
            assert_eq!(config.role, "shard");
            if config.runs_as_primary() { "primary" } else { "replica" }
        };

        const PRIMARY: &str = r#"{"node_id":"s1","role":"shard","shard_role":"primary",
            "listen_addr":"127.0.0.1:8081","data_dir":"./data"}"#;
        const REPLICA: &str = r#"{"node_id":"s2","role":"shard","shard_role":"replica",
            "listen_addr":"127.0.0.1:8082","data_dir":"./data","primary_addr":"http://127.0.0.1:8081"}"#;
        const OMITTED: &str = r#"{"node_id":"s3","role":"shard",
            "listen_addr":"127.0.0.1:8083","data_dir":"./data"}"#;

        assert_eq!(shard(PRIMARY)["shard_role"], "primary");
        assert_eq!(shard(REPLICA)["shard_role"], "replica");

        // Omitted is a replica because leadership says so, not because the logger decided it is.
        assert_eq!(runtime_role(OMITTED), "replica",
            "the runtime default for an omitted shard_role is what the line must report");
        assert_eq!(shard(OMITTED)["shard_role"], runtime_role(OMITTED));
        assert_eq!(shard(OMITTED)["shard_role"], "replica");
        assert!(!config_from(OMITTED).runs_as_primary(),
            "a shard that names no shard_role does not lead, so it is not logged as one");

        // And the logged value is the runtime's answer for each of the three, not a parallel reading.
        for json in [PRIMARY, REPLICA, OMITTED] {
            assert_eq!(shard(json)["shard_role"], runtime_role(json), "config: {}", json);
            assert_eq!(config_from(json).effective_shard_role(), Some(runtime_role(json)));
        }

        let router = rendered("json", r#"{"node_id":"r1","role":"router","listen_addr":"127.0.0.1:8080",
            "data_dir":"./data","shard_map":[{"start_hash":0,"end_hash":0,
            "node_url":"http://127.0.0.1:8081","replica_urls":[]}]}"#);
        let event: serde_json::Value = serde_json::from_str(router.trim()).unwrap();
        assert_eq!(event["role"], "router");
        assert!(event.get("shard_role").is_none(), "a router has no shard role at all: {}", router);
        assert_eq!(event["data_dir"], "./data", "a router still says where its cluster view lives");
    }

    /// The line is built from four config fields by name. If it ever grows to log the config itself,
    /// this is what fails.
    #[test]
    fn the_startup_line_carries_no_credentials() {
        let line = rendered("json", r#"{"node_id":"n1","role":"shard","shard_role":"primary",
            "listen_addr":"127.0.0.1:8081","data_dir":"./data",
            "auth":{"api_keys":["public-key-value"],"admin_keys":["admin-key-value"],
                    "internal_secret":"internal-secret-value","upstream_api_key":"upstream-key-value"}}"#);
        for secret in ["public-key-value", "admin-key-value", "internal-secret-value",
                       "upstream-key-value", "api_keys", "internal_secret"] {
            assert!(!line.contains(secret), "{:?} must never be logged: {}", secret, line);
        }
    }

    /// The reload is the only live half of `auth` and has to be exact about which fields it takes: a new
    /// `internal_secret` must not be read as one, and an unparseable file must not read as no keys.
    #[test]
    fn a_credential_reload_takes_the_client_tiers_and_refuses_a_file_it_cannot_trust() {
        let dir = std::env::temp_dir().join(format!("dew-auth-reload-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dew.json");
        let write = |body: &str| fs::write(&path, body).unwrap();

        write(r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1",
            "auth":{"api_keys":["alpha"],"admin_keys":["root"],"internal_secret":"s"}}"#);
        let loaded = reload_auth(path.to_str().unwrap()).expect("a valid file has to load");
        assert_eq!(loaded.api_keys, vec!["alpha".to_string()]);
        assert_eq!(loaded.admin_keys, vec!["root".to_string()]);
        assert_eq!(loaded.internal_secret.as_deref(), Some("s"),
            "the reader still reports it; whether it is applied is the caller's rule");

        write(r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1"}"#);
        assert!(reload_auth(path.to_str().unwrap()).unwrap().api_keys.is_empty(),
            "no auth section is the same open API it is at boot");

        write("{ not json");
        assert!(reload_auth(path.to_str().unwrap()).is_err(),
            "a half-written file must not be read as an empty key set");

        write(r#"{"auth":{"api_keys":[""]}}"#);
        assert!(reload_auth(path.to_str().unwrap()).is_err(),
            "a key that could not go in a header would silently disable the check");

        let _ = fs::remove_dir_all(&dir);
    }
}
