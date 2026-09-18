//! Node configuration, boot validation, and non-fatal misconfiguration warnings.

use crate::auth::AuthConfig;
use crate::changefeed::ChangefeedConfig;
use crate::cluster::rebalance::RebalanceConfig;
use crate::cluster::migration::DataMovementConfig;
use crate::logging::LoggingConfig;
use crate::maintenance::MaintenanceConfig;
use crate::ring::{validate_shard_ring, HashRing, ShardInfo};
use crate::storage::ReadCacheConfig;
use crate::webhook::WebhookConfig;
use crate::util::{node_key, same_endpoint};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};

/// `deny_unknown_fields` throughout the config tree: a typo taking a default silently is a node running
/// something else (L1). Not on `ShardInfo` or `HashRing`, which are wire format and face version skew.
#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub node_id: String,
    pub role: String,
    pub listen_addr: String,
    #[serde(default)]
    pub shard_map: Vec<ShardInfo>,
    /// Consistent-hash ownership, as an alternative to `shard_map`. A new deployment should use
    /// this; a running one migrates with POST /cluster/ring rather than a config edit.
    #[serde(default)]
    pub ring: Option<HashRing>,
    #[serde(default)]
    pub shard_role: Option<String>,
    /// `voter` (default) or `learner`. A learner never campaigns, even with no peers in sight. Set in
    /// config rather than learned, because the window this closes is the one before any view arrives.
    #[serde(default = "default_membership_mode")]
    pub membership_mode: String,
    #[serde(default)]
    pub primary_addr: Option<String>,
    #[serde(default)]
    pub replicas: Vec<String>,
    #[serde(default)]
    pub peers: Vec<String>,
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,
    #[serde(default = "default_election_delay")]
    pub election_delay_ms: u64,
    #[serde(default)]
    pub flow_control: FlowControlConfig,
    #[serde(default)]
    pub maintenance: MaintenanceConfig,
    #[serde(default)]
    pub rebalance: RebalanceConfig,
    #[serde(default)]
    pub data_movement: DataMovementConfig,
    #[serde(default)]
    pub read_cache: ReadCacheConfig,
    #[serde(default)]
    pub changefeed: ChangefeedConfig,
    #[serde(default)]
    pub webhooks: WebhookConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Development only: lets a ring change reassign ownership on a cluster that already holds data,
    /// leaving those keys unreadable at their new owners. Off by default, warned about loudly when on.
    #[serde(default)]
    pub allow_unsafe_ring_changes: bool,
}

/// Bounds on how far the leader lets replication fall behind before it stops accepting writes. Without
/// `max_uncommitted_frames` a leader that lost quorum stages frames forever -- the buffer drains on commit.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FlowControlConfig {
    #[serde(default = "default_max_uncommitted")]
    pub max_uncommitted_frames: usize,
    #[serde(default = "default_max_inflight")]
    pub max_inflight_requests: usize,
    #[serde(default = "default_drive_interval")]
    pub drive_interval_ms: u64,
}

// 0 disables the bound, which is the only way to get the old unbounded behaviour back.
fn default_max_uncommitted() -> usize { 4096 }
fn default_max_inflight() -> usize { 16 }
fn default_drive_interval() -> u64 { 500 }

impl Default for FlowControlConfig {
    fn default() -> Self {
        Self {
            max_uncommitted_frames: default_max_uncommitted(),
            max_inflight_requests: default_max_inflight(),
            drive_interval_ms: default_drive_interval(),
        }
    }
}

fn default_data_dir() -> String { "./data".to_string() }
fn default_membership_mode() -> String { "voter".to_string() }

pub const MEMBERSHIP_LEARNER: &str = "learner";

fn default_heartbeat_timeout() -> u64 { 6 }
fn default_election_delay() -> u64 { 2000 }

impl NodeConfig {
    pub fn own_url(&self) -> String {
        if self.listen_addr.contains("://") {
            self.listen_addr.clone()
        } else {
            format!("http://{}", self.listen_addr)
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        // Every role check downstream is `== "shard"` or `== "router"`, so a typo boots as neither:
        // no storage, the public routes registered anyway, and a panic on the first read (L2).
        if self.role != "shard" && self.role != "router" {
            return Err(format!("role must be 'shard' or 'router', got '{}'", self.role));
        }
        if self.role == "shard" {
            match self.shard_role.as_deref() {
                None | Some("primary") | Some("replica") => {},
                Some(other) => return Err(format!(
                    "shard_role must be 'primary' or 'replica', got '{}'", other)),
            }
        }
        if let Some(ring) = &self.ring {
            ring.validate()?;
            if !self.shard_map.is_empty() {
                return Err("set either shard_map or ring, not both; ring would silently win".into());
            }
        } else if self.role == "router" {
            validate_shard_ring(&self.shard_map)?;
        }
        if self.data_dir.trim().is_empty() {
            return Err("data_dir must not be empty".into());
        }
        // A replica without primary_addr used to be inert and rejected at boot; a runtime join can now
        // tell it who to follow. `config_warnings` still flags it, since an unjoined node does nothing.

        if self.membership_mode != "voter" && self.membership_mode != MEMBERSHIP_LEARNER {
            return Err(format!(
                "membership_mode must be 'voter' or 'learner', got '{}'", self.membership_mode));
        }
        if self.is_learner() && self.shard_role.as_deref() == Some("primary") {
            return Err("membership_mode 'learner' cannot be combined with shard_role 'primary'; \
                        a learner is never a leader".to_string());
        }
        // Not clamped at the point of use like the flow_control knobs: at 0 `leader_contact_task` sleeps
        // `ZERO` and pins a core, and `contact_lost` is permanently true, so every poll campaigns (L15).
        if self.heartbeat_timeout_secs == 0 {
            return Err("heartbeat_timeout_secs must be at least 1".to_string());
        }
        self.maintenance.validate()?;
        self.rebalance.validate()?;
        self.data_movement.validate()?;
        if self.rebalance.enabled && self.role != "shard" {
            return Err("automatic rebalancing may only run on shard nodes".to_string());
        }
        self.logging.validate()?;
        self.auth.validate()?;
        self.changefeed.validate()?;
        self.webhooks.validate()?;
        Ok(())
    }

    pub fn is_learner(&self) -> bool {
        self.membership_mode == MEMBERSHIP_LEARNER
    }

    /// Whether this node leads its shard at boot. This is the expression the boot path already
    /// decided leadership with, named once so that nothing has to restate it: a learner never leads,
    /// whatever the file says, and `validate` refuses that combination anyway.
    pub fn runs_as_primary(&self) -> bool {
        !self.is_learner() && self.shard_role.as_deref() == Some("primary")
    }

    /// The shard role this node runs as, as opposed to the one the file spells out. `shard_role` is
    /// optional and leadership turns on `runs_as_primary`, so an omitted field runs as a replica --
    /// which is already what the boot log calls it. `None` on a router, which has no shard role.
    pub fn effective_shard_role(&self) -> Option<&'static str> {
        if self.role != "shard" {
            return None;
        }
        Some(if self.runs_as_primary() { "primary" } else { "replica" })
    }
}

pub fn config_warnings(cfg: &NodeConfig) -> Vec<String> {
    let mut out = Vec::new();

    if cfg.role == "router" {
        let mut replica_lists: BTreeMap<&str, &Vec<String>> = BTreeMap::new();
        for shard in &cfg.shard_map {
            if let Some(previous) = replica_lists.get(shard.node_url.as_str()) {
                if **previous != shard.replica_urls {
                    out.push(format!(
                        "shard {} appears in multiple ranges with different replica_urls; \
                         router failover will use whichever range matched the key",
                        shard.node_url));
                }
            } else {
                replica_lists.insert(shard.node_url.as_str(), &shard.replica_urls);
            }

            let mut seen = HashSet::new();
            for replica in &shard.replica_urls {
                if !seen.insert(replica.as_str()) {
                    out.push(format!("shard {} lists replica {} more than once", shard.node_url, replica));
                }
                if same_endpoint(replica, &shard.node_url) {
                    out.push(format!("shard {} lists itself as its own replica", shard.node_url));
                }
            }

            if shard.replica_urls.is_empty() {
                out.push(format!("shard {} has no replica_urls; reads and writes cannot fail over", shard.node_url));
            }
        }

        for shard in &cfg.shard_map {
            for other in &cfg.shard_map {
                if other.node_url == shard.node_url {
                    continue;
                }
                if other.replica_urls.iter().any(|r| same_endpoint(r, &shard.node_url)) {
                    out.push(format!(
                        "{} is a primary for one range and a replica of {} for another; \
                         a failover there would promote a node that already serves writes",
                        shard.node_url, other.node_url));
                }
            }
        }
    }

    if cfg.role == "shard" {
        // A node with no peers reaches a majority of one. Left a voter it elects itself over an empty log
        // the moment its timeout expires, whatever the operator intended.
        if !cfg.is_learner()
            && cfg.peers.is_empty()
            && cfg.primary_addr.is_none()
            && cfg.shard_role.as_deref() == Some("replica")
        {
            out.push("replica has no primary_addr and no peers, and membership_mode is 'voter'; \
                      it will elect itself once its heartbeat timeout expires. Set membership_mode \
                      to 'learner' if this node is meant to join an existing cluster".to_string());
        }
        if cfg.is_learner() && !cfg.peers.is_empty() {
            out.push("membership_mode is 'learner' but peers is set; a learner never campaigns, \
                      so peers has no effect on it".to_string());
        }

        if cfg.peers.iter().any(|p| same_endpoint(p, &cfg.listen_addr)) {
            out.push(format!(
                "peers contains this node's own address ({}); peers must list only the other nodes, \
                 otherwise the majority threshold is computed against an inflated cluster size",
                cfg.listen_addr));
        }
        if cfg.replicas.iter().any(|r| same_endpoint(r, &cfg.listen_addr)) {
            out.push(format!("replicas contains this node's own address ({})", cfg.listen_addr));
        }

        let mut seen = HashSet::new();
        for replica in &cfg.replicas {
            if !seen.insert(node_key(replica)) {
                out.push(format!("replicas lists {} more than once", replica));
            }
        }

        if let Some(primary) = &cfg.primary_addr {
            if !cfg.peers.is_empty() && !cfg.peers.iter().any(|p| same_endpoint(p, primary)) {
                out.push(format!("primary_addr {} is not listed in peers; it cannot be voted for", primary));
            }
        }

        if !cfg.replicas.is_empty() {
            for replica in &cfg.replicas {
                if !cfg.peers.is_empty() && !cfg.peers.iter().any(|p| same_endpoint(p, replica)) {
                    out.push(format!("replica {} is not listed in peers; it cannot vote in an election", replica));
                }
            }
            if cfg.peers.is_empty() {
                out.push("replicas are configured but peers is empty; this node can never be replaced by an election".to_string());
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warn_cfg(json: &str) -> Vec<String> {
        let cfg: NodeConfig = serde_json::from_str(json).unwrap();
        config_warnings(&cfg)
    }

    fn parse(json: &str) -> Result<NodeConfig, String> {
        serde_json::from_str::<NodeConfig>(json).map_err(|e| e.to_string())
    }

    /// L1: a key nobody reads is a setting that did not take, and defaulting silently is how a
    /// node ends up running something other than what its file says.
    #[test]
    fn a_misspelled_config_key_is_refused_rather_than_defaulted() {
        let typo = parse(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9601",
            "heartbeat_timeout_sec":30}"#);
        assert!(typo.is_err(), "a typo booted on defaults");

        let nested = parse(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9601",
            "maintenance":{"enabled":true,"interval_sec":30}}"#);
        assert!(nested.is_err(), "a typo in a nested section booted on defaults");

        assert!(parse(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9601",
            "heartbeat_timeout_secs":30}"#).is_ok(), "the spelled-right key must still parse");
    }

    /// L2: nothing downstream matches a third role, so `db` stays `None`, the public routes are
    /// registered anyway, and the first read unwraps it.
    #[test]
    fn an_unknown_role_fails_at_boot_instead_of_on_the_first_read() {
        let cfg = parse(r#"{"node_id":"n1","role":"shrad","listen_addr":"127.0.0.1:9602"}"#).unwrap();
        assert!(cfg.validate().unwrap_err().contains("role"), "an unknown role validated");

        let bad_shard_role = parse(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9603",
            "shard_role":"leader"}"#).unwrap();
        assert!(bad_shard_role.validate().unwrap_err().contains("shard_role"));

        let ok = parse(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9604",
            "shard_role":"primary"}"#).unwrap();
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn warns_when_peers_includes_this_node() {
        let w = warn_cfg(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9501",
            "peers":["http://127.0.0.1:9501","http://127.0.0.1:9502"]}"#);
        assert!(w.iter().any(|m| m.contains("own address")), "got {:?}", w);

        let clean = warn_cfg(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9501",
            "peers":["http://127.0.0.1:9502"]}"#);
        assert!(!clean.iter().any(|m| m.contains("own address")), "got {:?}", clean);
    }

    #[test]
    fn warns_when_replicas_and_peers_disagree() {
        let w = warn_cfg(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9501",
            "replicas":["http://127.0.0.1:9502"],"peers":["http://127.0.0.1:9503"]}"#);
        assert!(w.iter().any(|m| m.contains("9502") && m.contains("cannot vote")), "got {:?}", w);

        let orphaned = warn_cfg(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9501",
            "replicas":["http://127.0.0.1:9502"]}"#);
        assert!(orphaned.iter().any(|m| m.contains("peers is empty")), "got {:?}", orphaned);

        let dup = warn_cfg(r#"{"node_id":"n1","role":"shard","listen_addr":"127.0.0.1:9501",
            "replicas":["http://127.0.0.1:9502","http://127.0.0.1:9502"],
            "peers":["http://127.0.0.1:9502"]}"#);
        assert!(dup.iter().any(|m| m.contains("more than once")), "got {:?}", dup);
    }

    #[test]
    fn warns_when_a_replica_never_appears_in_the_shard_map_consistently() {
        let w = warn_cfg(r#"{"node_id":"r","role":"router","listen_addr":"127.0.0.1:9500",
            "shard_map":[
              {"start_hash":0,"end_hash":9223372036854775808,"node_url":"http://a","replica_urls":["http://x"]},
              {"start_hash":9223372036854775808,"end_hash":0,"node_url":"http://a","replica_urls":["http://y"]}]}"#);
        assert!(w.iter().any(|m| m.contains("different replica_urls")), "got {:?}", w);

        let cross = warn_cfg(r#"{"node_id":"r","role":"router","listen_addr":"127.0.0.1:9500",
            "shard_map":[
              {"start_hash":0,"end_hash":9223372036854775808,"node_url":"http://a","replica_urls":["http://b"]},
              {"start_hash":9223372036854775808,"end_hash":0,"node_url":"http://b","replica_urls":["http://a"]}]}"#);
        assert!(cross.iter().any(|m| m.contains("already serves writes")), "got {:?}", cross);

        let bare = warn_cfg(r#"{"node_id":"r","role":"router","listen_addr":"127.0.0.1:9500",
            "shard_map":[{"start_hash":0,"end_hash":0,"node_url":"http://a"}]}"#);
        assert!(bare.iter().any(|m| m.contains("cannot fail over")), "got {:?}", bare);
    }

    #[test]
    fn membership_mode_defaults_to_voter_and_is_checked_at_boot() {
        let cfg = |json: &str| serde_json::from_str::<NodeConfig>(json).unwrap();

        let default = cfg(r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1"}"#);
        assert_eq!(default.membership_mode, "voter",
            "an existing config must keep behaving exactly as it did");
        assert!(!default.is_learner());
        assert!(default.validate().is_ok());

        let learner = cfg(r#"{"node_id":"n","role":"shard","shard_role":"replica",
            "listen_addr":"127.0.0.1:1","membership_mode":"learner"}"#);
        assert!(learner.is_learner());
        assert!(learner.validate().is_ok());

        let typo = cfg(r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1",
            "membership_mode":"observer"}"#);
        assert!(typo.validate().is_err(),
            "a misspelled mode must fail the boot, not silently fall back to voter");

        let contradiction = cfg(r#"{"node_id":"n","role":"shard","shard_role":"primary",
            "listen_addr":"127.0.0.1:1","membership_mode":"learner"}"#);
        assert!(contradiction.validate().is_err(), "a learner is never a primary");
    }

    #[test]
    fn a_lone_voter_that_will_elect_itself_is_called_out() {
        let w = warn_cfg(r#"{"node_id":"n","role":"shard","shard_role":"replica",
            "listen_addr":"127.0.0.1:9501"}"#);
        assert!(w.iter().any(|m| m.contains("elect itself") && m.contains("learner")),
            "the warning must name the fix, not just the symptom: {:?}", w);

        let fixed = warn_cfg(r#"{"node_id":"n","role":"shard","shard_role":"replica",
            "listen_addr":"127.0.0.1:9501","membership_mode":"learner"}"#);
        assert!(!fixed.iter().any(|m| m.contains("elect itself")), "got {:?}", fixed);

        let pointless = warn_cfg(r#"{"node_id":"n","role":"shard","shard_role":"replica",
            "listen_addr":"127.0.0.1:9501","membership_mode":"learner",
            "peers":["http://127.0.0.1:9502"]}"#);
        assert!(pointless.iter().any(|m| m.contains("peers has no effect")), "got {:?}", pointless);
    }

    #[test]
    fn data_dir_defaults_and_is_configurable() {
        let default: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1"}"#).unwrap();
        assert_eq!(default.data_dir, "./data");
        assert!(default.validate().is_ok());

        let custom: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1","data_dir":"/var/lib/dewdb"}"#).unwrap();
        assert_eq!(custom.data_dir, "/var/lib/dewdb");
        assert!(custom.validate().is_ok());

        let blank: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1","data_dir":"  "}"#).unwrap();
        assert!(blank.validate().is_err(), "a blank data_dir would write into the process cwd");
    }

    /// L15: at 0 this knob turns `leader_contact_task` into `sleep(ZERO)` on every shard node and makes
    /// `contact_lost` permanently true -- not a slow setting but a different program.
    #[test]
    fn a_zero_heartbeat_timeout_is_refused_at_boot() {
        let cfg = |secs: u64| -> Result<(), String> {
            let c: NodeConfig = serde_json::from_value(serde_json::json!({
                "node_id": "n1", "role": "shard", "shard_role": "primary",
                "listen_addr": "127.0.0.1:1", "data_dir": "/tmp/x",
                "heartbeat_timeout_secs": secs,
            })).unwrap();
            c.validate()
        };
        assert!(cfg(0).is_err(), "0 pins a core for the life of the process");
        assert!(cfg(1).is_ok());
    }
}
