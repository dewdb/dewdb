//! Versioned cluster topology: the runtime source of truth for routing and membership.

use crate::config::NodeConfig;
use crate::ring::{validate_shard_ring, HashRing, ShardInfo};
use crate::util::{endpoint_of, write_atomic};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const CLUSTER_FILE: &str = "cluster.meta";
const CLUSTER_TMP: &str = "cluster.meta.tmp";

// Same reason as replication.meta: one staging path, and the rename order must follow the
// decision order rather than whichever fsync returned first.
static SAVE_LOCK: Mutex<()> = Mutex::new(());

/// A node the cluster knows about. Identity is `url`: config only ever names peers by address and
/// every existing comparison in this codebase is endpoint-based. `node_id` is informational until
/// joins carry it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_role: Option<String>,
    /// Counted toward election and commit quorums. Runtime-added nodes are never voting: the view
    /// converges rather than being agreed, and a quorum computed from disagreeing views can be two
    /// disjoint majorities. Promoting a learner needs the joint consensus of commit 44.
    #[serde(default)]
    pub voting: bool,
    /// For a learner, the primary whose group it is catching up with. A leader ships frames only to
    /// learners naming it, so one cluster-wide list can serve several shard groups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follows: Option<String>,
}

/// The whole topology as one versioned value. Replacing it wholesale rather than patching fields
/// keeps the ring validatable as a unit: a half-applied update is a routing hole.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClusterMetadata {
    pub version: u64,
    pub updated_by: String,
    /// Derived from one node's config rather than decided by the cluster. Every node seeds at
    /// version 1 with different content -- a router seeds a ring, a shard seeds none -- so seeds
    /// are not comparable views and must never travel.
    #[serde(default)]
    pub seeded: bool,
    #[serde(default)]
    pub members: Vec<Member>,
    /// Explicit ranges. Superseded by `ring` when both are present, and kept rather than cleared so
    /// a cluster can be published back onto ranges if a ring change goes wrong.
    #[serde(default)]
    pub shards: Vec<ShardInfo>,
    /// Consistent-hash ownership. Takes precedence over `shards` wherever a key is routed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ring: Option<HashRing>,
}

impl ClusterMetadata {
    /// The bootstrap view, used only when no durable copy exists. Config is a seed, not an
    /// authority: once version 1 is on disk, later config edits no longer move the cluster.
    pub fn seed_from_config(cfg: &NodeConfig) -> Self {
        let mut members = Vec::new();
        // Everything config names is a full member: the config quorum is the pre-existing one.
        let mut push = |url: &str, role: &str, shard_role: Option<&str>, node_id: Option<&str>| {
            if url.is_empty() || members.iter().any(|m: &Member| same_url(&m.url, url)) {
                return;
            }
            members.push(Member {
                url: url.to_string(),
                node_id: node_id.map(str::to_string),
                role: role.to_string(),
                shard_role: shard_role.map(str::to_string),
                voting: role == "shard",
                follows: None,
            });
        };

        push(&normalize_self_url(&cfg.listen_addr), &cfg.role, cfg.shard_role.as_deref(), Some(&cfg.node_id));
        if let Some(primary) = &cfg.primary_addr {
            push(primary, "shard", Some("primary"), None);
        }
        for replica in &cfg.replicas {
            push(replica, "shard", Some("replica"), None);
        }
        for peer in &cfg.peers {
            push(peer, "shard", None, None);
        }
        for shard in &cfg.shard_map {
            push(&shard.node_url, "shard", Some("primary"), None);
            for replica in &shard.replica_urls {
                push(replica, "shard", Some("replica"), None);
            }
        }
        for shard in cfg.ring.iter().flat_map(|r| r.shards.iter()) {
            push(&shard.node_url, "shard", Some("primary"), None);
            for replica in &shard.replica_urls {
                push(replica, "shard", Some("replica"), None);
            }
        }

        Self {
            version: 1,
            updated_by: cfg.node_id.clone(),
            seeded: true,
            members,
            shards: cfg.shard_map.clone(),
            ring: cfg.ring.clone(),
        }
    }

    /// Total order over views, so every node converges on the same one.
    ///
    /// The `updated_by` tiebreak makes equal versions converge instead of splitting the cluster's
    /// routing view, which is the failure that actually loses writes. It does so by discarding one
    /// of two concurrent updates: nothing here makes concurrent updates safe, only deterministic.
    pub fn supersedes(&self, other: &Self) -> bool {
        // A shard seeds an empty ring. Letting that outrank a router's seeded ring on a tiebreak
        // would route every key nowhere, so a seed loses to everything and wins against nothing.
        if self.seeded {
            return false;
        }
        if other.seeded {
            return self.version >= other.version;
        }
        (self.version, self.updated_by.as_str()) > (other.version, other.updated_by.as_str())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version == 0 {
            return Err("cluster metadata version must be at least 1".to_string());
        }
        if let Some(ring) = &self.ring {
            ring.validate()?;
        }
        // Validated even when a ring supersedes it, so a rollback publish cannot restore a bad map.
        if !self.shards.is_empty() {
            validate_shard_ring(&self.shards)?;
        }

        let mut seen = HashSet::new();
        for member in &self.members {
            if member.url.trim().is_empty() {
                return Err("member url must not be empty".to_string());
            }
            if !seen.insert(endpoint_of(&member.url)) {
                return Err(format!("member {} appears more than once", member.url));
            }
            if member.role != "shard" && member.role != "router" {
                return Err(format!("member {} has unknown role '{}'", member.url, member.role));
            }
        }
        Ok(())
    }

    /// `Ok(None)` is a node that has never been seeded. Unreadable is an error: falling back to a
    /// config-derived map would route keys by a topology the cluster has already moved past.
    /// Deleting the file is the documented way to force a re-seed from config.
    pub fn load(data_dir: &str) -> io::Result<Option<Self>> {
        let dir = Path::new(data_dir);
        let mut corrupt: Option<(String, String)> = None;

        for name in [CLUSTER_FILE, CLUSTER_TMP] {
            match fs::read_to_string(dir.join(name)) {
                Ok(content) => match serde_json::from_str::<Self>(&content) {
                    Ok(meta) => match meta.validate() {
                        Ok(()) => return Ok(Some(meta)),
                        Err(why) => corrupt = Some((name.to_string(), why)),
                    },
                    Err(e) => corrupt = Some((name.to_string(), e.to_string())),
                },
                Err(e) if e.kind() == io::ErrorKind::NotFound => {},
                Err(e) => return Err(e),
            }
        }

        match corrupt {
            Some((name, why)) => Err(io::Error::new(io::ErrorKind::InvalidData, format!(
                "{} is unusable ({}); delete it to re-seed the topology from config", name, why))),
            None => Ok(None),
        }
    }

    pub fn save(&self, data_dir: &str) -> io::Result<()> {
        let dir = PathBuf::from(data_dir);
        let _guard = SAVE_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // Adoption decides under the in-memory lock and persists after releasing it, so saves can
        // arrive out of order. The durable view must move the same direction as the in-memory one.
        if let Ok(Some(existing)) = Self::load(data_dir) {
            if !self.supersedes(&existing) {
                return Ok(());
            }
        }

        fs::create_dir_all(&dir)?;
        let content = serde_json::to_vec(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        write_atomic(&dir, CLUSTER_FILE, &content)
    }

    pub fn member(&self, url: &str) -> Option<&Member> {
        self.members.iter().find(|m| same_url(&m.url, url))
    }

    /// Shard nodes catching up with `primary`, which are exactly the members that receive frames
    /// but are absent from every quorum computation.
    pub fn learners_following(&self, primary: &str) -> Vec<String> {
        self.members.iter()
            .filter(|m| !m.voting && m.role == "shard")
            .filter(|m| m.follows.as_deref().map_or(false, |f| same_url(f, primary)))
            .map(|m| m.url.clone())
            .collect()
    }

    /// True only for a node that is present and explicitly non-voting. An unknown node is not a
    /// learner: a node missing from the view must keep behaving as its config says, or a view that
    /// has not reached it yet would silently strip its vote.
    pub fn is_learner(&self, url: &str) -> bool {
        self.member(url).map_or(false, |m| !m.voting && m.role == "shard")
    }

    /// Next version of this view with `member` added or replaced. Returns a value rather than
    /// mutating, so a rejected change never half-applies.
    pub fn with_member(&self, by: &str, member: Member) -> Self {
        let mut next = self.clone();
        next.members.retain(|m| !same_url(&m.url, &member.url));
        next.members.push(member);
        next.bump(by);
        next
    }

    pub fn without_member(&self, by: &str, url: &str) -> Self {
        let mut next = self.clone();
        next.members.retain(|m| !same_url(&m.url, url));
        next.bump(by);
        next
    }

    fn bump(&mut self, by: &str) {
        self.version += 1;
        self.updated_by = by.to_string();
        // The result is a decision, not a guess, however it was derived.
        self.seeded = false;
    }

    /// Owners in the model actually in force, so fan-out and probing never disagree with routing.
    pub fn shard_owners(&self) -> Vec<(String, Vec<String>)> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        if let Some(ring) = &self.ring {
            for shard in &ring.shards {
                if seen.insert(shard.node_url.clone()) {
                    out.push((shard.node_url.clone(), shard.replica_urls.clone()));
                }
            }
            return out;
        }
        for shard in &self.shards {
            if seen.insert(shard.node_url.clone()) {
                out.push((shard.node_url.clone(), shard.replica_urls.clone()));
            }
        }
        out
    }

    /// Next version of this view with `ring` in force.
    pub fn with_ring(&self, by: &str, ring: HashRing) -> Self {
        let mut next = self.clone();
        next.ring = Some(ring);
        next.bump(by);
        next
    }
}

fn same_url(a: &str, b: &str) -> bool {
    endpoint_of(a) == endpoint_of(b)
}

// listen_addr is a bind address, not a URL; peers and shard maps always carry a scheme.
fn normalize_self_url(listen_addr: &str) -> String {
    if listen_addr.contains("://") {
        listen_addr.to_string()
    } else {
        format!("http://{}", listen_addr)
    }
}

/// Outcome of offering a view to a node, kept distinct so callers can tell "already current" from
/// "refused as invalid" -- the first is the steady state, the second needs an operator.
#[derive(Debug, PartialEq, Eq)]
pub enum Adoption {
    Adopted { from: u64, to: u64 },
    Stale { current: u64 },
    Rejected(String),
}

/// Validates before comparing: a malformed view must not win on version alone, or one bad push
/// would take routing down cluster-wide and outrank every correction that follows.
pub fn adopt(current: &mut ClusterMetadata, incoming: ClusterMetadata) -> Adoption {
    if let Err(why) = incoming.validate() {
        return Adoption::Rejected(why);
    }
    if !incoming.supersedes(current) {
        return Adoption::Stale { current: current.version };
    }
    let from = current.version;
    let to = incoming.version;
    *current = incoming;
    Adoption::Adopted { from, to }
}

/// What an operator asked for. `voting` is absent by design: it is not the operator's to set.
#[derive(Deserialize, Debug, Clone)]
pub struct JoinRequest {
    pub url: String,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub shard_role: Option<String>,
    #[serde(default)]
    pub follows: Option<String>,
    /// Present only so asking for it fails loudly instead of being quietly dropped.
    #[serde(default)]
    pub voting: Option<bool>,
}

/// Builds the next view for a join. `leader_url` is the node accepting the change, and the default
/// group for a shard that did not name one.
pub fn plan_join(current: &ClusterMetadata, by: &str, leader_url: &str, req: &JoinRequest)
    -> Result<ClusterMetadata, String>
{
    if req.url.trim().is_empty() {
        return Err("url is required".to_string());
    }
    if req.voting == Some(true) {
        return Err("a node cannot join as voting; runtime quorum changes need joint consensus \
                    (commit 44). It joins as a learner and replicates immediately".to_string());
    }
    let role = req.role.clone().unwrap_or_else(|| "shard".to_string());
    if role != "shard" && role != "router" {
        return Err(format!("unknown role '{}'", role));
    }
    if same_url(&req.url, leader_url) {
        return Err("a leader cannot add itself as a learner".to_string());
    }
    // Re-adding a config member as a learner would drop it out of the quorum it is already in.
    if let Some(existing) = current.member(&req.url) {
        if existing.voting {
            return Err(format!(
                "{} is already a voting member; demoting it would shrink the quorum", req.url));
        }
    }

    let member = Member {
        url: req.url.clone(),
        node_id: req.node_id.clone(),
        role: role.clone(),
        shard_role: req.shard_role.clone().or_else(|| (role == "shard").then(|| "replica".to_string())),
        voting: false,
        follows: match role.as_str() {
            "shard" => Some(req.follows.clone().unwrap_or_else(|| leader_url.to_string())),
            _ => None,
        },
    };

    let next = current.with_member(by, member);
    next.validate().map(|_| next).map_err(|e| e)
}

pub fn plan_leave(current: &ClusterMetadata, by: &str, url: &str) -> Result<ClusterMetadata, String> {
    match current.member(url) {
        None => Err(format!("{} is not a member", url)),
        // Losing a voter shrinks every majority it was counted in, and nothing here orders that
        // against an election already in flight.
        Some(m) if m.voting => Err(format!(
            "{} is a voting member; removing it would shrink the quorum, which needs joint \
             consensus (commit 44)", url)),
        Some(_) => {
            let next = current.without_member(by, url);
            next.validate().map(|_| next).map_err(|e| e)
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::temp_root;

    const HALF: u64 = 9223372036854775808;

    fn shard(start: u64, end: u64, url: &str, replicas: &[&str]) -> ShardInfo {
        ShardInfo {
            start_hash: start,
            end_hash: end,
            node_url: url.to_string(),
            replica_urls: replicas.iter().map(|r| r.to_string()).collect(),
        }
    }

    fn view(version: u64, by: &str, shards: Vec<ShardInfo>) -> ClusterMetadata {
        ClusterMetadata {
            version, updated_by: by.to_string(), seeded: false, members: Vec::new(), shards, ring: None,
        }
    }

    fn cfg(json: serde_json::Value) -> NodeConfig {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn a_view_is_adopted_only_when_it_is_strictly_newer() {
        let mut current = view(3, "n1", vec![shard(0, 0, "http://a", &[])]);

        assert_eq!(adopt(&mut current, view(2, "n1", vec![shard(0, 0, "http://b", &[])])),
            Adoption::Stale { current: 3 });
        assert_eq!(current.shards[0].node_url, "http://a", "an older view must not overwrite");

        assert_eq!(adopt(&mut current, view(3, "n1", vec![shard(0, 0, "http://b", &[])])),
            Adoption::Stale { current: 3 },
            "the same version from the same writer is a retransmit, not an update");

        assert_eq!(adopt(&mut current, view(4, "n1", vec![shard(0, 0, "http://b", &[])])),
            Adoption::Adopted { from: 3, to: 4 });
        assert_eq!(current.shards[0].node_url, "http://b");
    }

    #[test]
    fn equal_versions_converge_on_one_view_rather_than_splitting() {
        // Two nodes independently publish version 5. Every node must land on the same one,
        // whichever it is, or two routers send the same key to different shards.
        let a = view(5, "alpha", vec![shard(0, 0, "http://a", &[])]);
        let b = view(5, "bravo", vec![shard(0, 0, "http://b", &[])]);

        let mut saw_a_first = view(1, "seed", vec![]);
        adopt(&mut saw_a_first, a.clone());
        adopt(&mut saw_a_first, b.clone());

        let mut saw_b_first = view(1, "seed", vec![]);
        adopt(&mut saw_b_first, b);
        adopt(&mut saw_b_first, a);

        assert_eq!(saw_a_first, saw_b_first, "arrival order must not decide the surviving view");
        assert_eq!(saw_a_first.updated_by, "bravo", "the tiebreak is total and deterministic");
    }

    #[test]
    fn an_invalid_view_is_refused_however_new_it_claims_to_be() {
        let mut current = view(1, "n1", vec![shard(0, 0, "http://a", &[])]);

        let gap = view(99, "n1", vec![shard(0, HALF, "http://a", &[])]);
        match adopt(&mut current, gap) {
            Adoption::Rejected(why) => assert!(why.contains("uncovered"), "got: {}", why),
            other => panic!("a ring with a hole must not be adopted: {:?}", other),
        }
        assert_eq!(current.version, 1, "the refused view must not take effect");

        let overlap = view(99, "n1", vec![shard(0, HALF + 100, "http://a", &[]), shard(HALF, 0, "http://b", &[])]);
        assert!(matches!(adopt(&mut current, overlap), Adoption::Rejected(_)));

        let mut dupes = view(99, "n1", vec![]);
        dupes.members = vec![
            Member { url: "http://a".into(), node_id: None, role: "shard".into(), shard_role: None, voting: true, follows: None },
            Member { url: "http://a/".into(), node_id: None, role: "shard".into(), shard_role: None, voting: true, follows: None },
        ];
        match adopt(&mut current, dupes) {
            Adoption::Rejected(why) => assert!(why.contains("more than once"), "got: {}", why),
            other => panic!("the same endpoint twice is not a membership list: {:?}", other),
        }

        let mut bad_role = view(99, "n1", vec![]);
        bad_role.members = vec![
            Member { url: "http://a".into(), node_id: None, role: "coordinator".into(), shard_role: None, voting: true, follows: None },
        ];
        assert!(matches!(adopt(&mut current, bad_role), Adoption::Rejected(_)));

        assert!(matches!(adopt(&mut current, view(0, "n1", vec![])), Adoption::Rejected(_)),
            "version 0 means unseeded and must never be published");
    }

    #[test]
    fn seeding_from_config_captures_every_node_the_config_names() {
        let router = ClusterMetadata::seed_from_config(&cfg(serde_json::json!({
            "node_id": "r1", "role": "router", "listen_addr": "127.0.0.1:9500",
            "shard_map": [
                {"start_hash": 0, "end_hash": HALF, "node_url": "http://a", "replica_urls": ["http://a2"]},
                {"start_hash": HALF, "end_hash": 0, "node_url": "http://b", "replica_urls": ["http://b2"]}]
        })));

        assert_eq!(router.version, 1);
        assert_eq!(router.shards.len(), 2);
        router.validate().expect("a seed from a valid config must itself be valid");

        let urls: Vec<&str> = router.members.iter().map(|m| m.url.as_str()).collect();
        assert!(urls.contains(&"http://127.0.0.1:9500"), "the router itself is a member: {:?}", urls);
        for expected in ["http://a", "http://a2", "http://b", "http://b2"] {
            assert!(urls.contains(&expected), "{} missing from {:?}", expected, urls);
        }
        assert_eq!(router.members.iter().find(|m| m.url.contains("9500")).unwrap().role, "router");

        let shard = ClusterMetadata::seed_from_config(&cfg(serde_json::json!({
            "node_id": "n1", "role": "shard", "shard_role": "primary", "listen_addr": "127.0.0.1:9501",
            "replicas": ["http://127.0.0.1:9502"],
            "peers": ["http://127.0.0.1:9502", "http://127.0.0.1:9503"]
        })));

        assert_eq!(shard.members.len(), 3, "self, the replica, and the third peer: {:?}", shard.members);
        assert!(shard.shards.is_empty(), "a shard node carries no ring");
        shard.validate().unwrap();

        let listed_twice = shard.members.iter().filter(|m| m.url.contains("9502")).count();
        assert_eq!(listed_twice, 1, "a node in both replicas and peers is one member, not two");
    }

    #[test]
    fn metadata_survives_a_restart_and_never_rewinds_on_disk() {
        let root = temp_root();
        let dir = root.to_string_lossy().to_string();

        assert!(ClusterMetadata::load(&dir).unwrap().is_none(), "an unseeded node has no view");

        view(4, "n1", vec![shard(0, 0, "http://a", &[])]).save(&dir).unwrap();
        let back = ClusterMetadata::load(&dir).unwrap().expect("must persist");
        assert_eq!(back.version, 4);
        assert_eq!(back.shards[0].node_url, "http://a");

        view(2, "n1", vec![shard(0, 0, "http://old", &[])]).save(&dir).unwrap();
        assert_eq!(ClusterMetadata::load(&dir).unwrap().unwrap().version, 4,
            "a late save from an older view must not rewind the durable topology");

        view(5, "n1", vec![shard(0, 0, "http://b", &[])]).save(&dir).unwrap();
        assert_eq!(ClusterMetadata::load(&dir).unwrap().unwrap().shards[0].node_url, "http://b");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn an_unreadable_view_fails_the_boot_instead_of_silently_reverting_to_config() {
        let root = temp_root();
        let dir = root.to_string_lossy().to_string();

        fs::write(root.join(CLUSTER_FILE), b"{ this is not json").unwrap();
        let err = ClusterMetadata::load(&dir).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("delete it"), "the operator needs the way out: {}", err);

        // A file that parses but describes an impossible ring is just as unusable.
        let holed = serde_json::to_vec(&view(3, "n1", vec![shard(0, HALF, "http://a", &[])])).unwrap();
        fs::write(root.join(CLUSTER_FILE), holed).unwrap();
        assert!(ClusterMetadata::load(&dir).is_err(), "a validated file must be validated on the way in too");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_staging_file_left_by_a_crash_is_read_back() {
        let root = temp_root();
        let dir = root.to_string_lossy().to_string();

        let bytes = serde_json::to_vec(&view(9, "n1", vec![shard(0, 0, "http://a", &[])])).unwrap();
        fs::write(root.join(CLUSTER_TMP), bytes).unwrap();

        let back = ClusterMetadata::load(&dir).unwrap().expect("the staging file is complete once fsynced");
        assert_eq!(back.version, 9);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_config_seed_never_overrides_another_nodes_view() {
        let router_seed = ClusterMetadata::seed_from_config(&cfg(serde_json::json!({
            "node_id": "r1", "role": "router", "listen_addr": "127.0.0.1:9500",
            "shard_map": [{"start_hash": 0, "end_hash": 0, "node_url": "http://a", "replica_urls": []}]
        })));
        let shard_seed = ClusterMetadata::seed_from_config(&cfg(serde_json::json!({
            "node_id": "zz", "role": "shard", "shard_role": "primary", "listen_addr": "127.0.0.1:9501"
        })));

        assert!(router_seed.seeded && shard_seed.seeded);
        assert!(shard_seed.shards.is_empty());
        assert!(shard_seed.version == router_seed.version, "every node seeds at the same version");
        assert!("zz" > "r1", "the tiebreak alone would hand this to the shard");

        let mut router = router_seed.clone();
        assert_eq!(adopt(&mut router, shard_seed.clone()), Adoption::Stale { current: 1 });
        assert_eq!(router.shards.len(), 1,
            "a shard's empty ring must not displace a router's; every key would route nowhere");

        // A published view outranks any seed, whatever the versions say.
        let published = view(1, "aaa", vec![shard(0, 0, "http://b", &[])]);
        let mut router = router_seed.clone();
        assert_eq!(adopt(&mut router, published), Adoption::Adopted { from: 1, to: 1 });
        assert_eq!(router.shards[0].node_url, "http://b");
        assert!(!router.seeded, "once a decision arrives the node is no longer running on a guess");

        let mut decided = view(2, "n1", vec![shard(0, 0, "http://b", &[])]);
        assert_eq!(adopt(&mut decided, router_seed), Adoption::Stale { current: 2 },
            "a node restarting into its own config seed must not undo a published view");
    }

    fn join(url: &str) -> JoinRequest {
        JoinRequest {
            url: url.to_string(), node_id: None, role: None,
            shard_role: None, follows: None, voting: None,
        }
    }

    fn cluster_of(voters: &[&str]) -> ClusterMetadata {
        let mut v = view(1, "n1", vec![]);
        v.members = voters.iter().map(|u| Member {
            url: u.to_string(), node_id: None, role: "shard".into(),
            shard_role: None, voting: true, follows: None,
        }).collect();
        v
    }

    #[test]
    fn a_node_joins_as_a_learner_and_the_quorum_set_is_untouched() {
        let current = cluster_of(&["http://n1", "http://n2", "http://n3"]);
        let next = plan_join(&current, "n1", "http://n1", &join("http://n4")).unwrap();

        assert_eq!(next.version, 2, "a membership change publishes a new version");
        assert!(!next.seeded, "a decision is not a guess");

        let added = next.member("http://n4").expect("the node must be in the view");
        assert!(!added.voting, "admitting a voter at runtime can split the quorum");
        assert_eq!(added.follows.as_deref(), Some("http://n1"), "defaults to the leader that admitted it");
        assert_eq!(added.shard_role.as_deref(), Some("replica"));

        let voters = next.members.iter().filter(|m| m.voting).count();
        assert_eq!(voters, 3, "the quorum set must be the same size as before the join");
        assert_eq!(next.learners_following("http://n1"), vec!["http://n4".to_string()]);
        assert!(next.is_learner("http://n4"));
        assert!(!next.is_learner("http://n2"));
    }

    #[test]
    fn a_learner_belongs_to_the_group_it_named() {
        let current = cluster_of(&["http://a", "http://b"]);
        let mut req = join("http://new");
        req.follows = Some("http://b".into());
        let next = plan_join(&current, "a", "http://a", &req).unwrap();

        assert!(next.learners_following("http://a").is_empty(),
            "a leader must not ship frames to a learner catching up with another group");
        assert_eq!(next.learners_following("http://b"), vec!["http://new".to_string()]);
    }

    #[test]
    fn membership_changes_that_would_move_the_quorum_are_refused() {
        let current = cluster_of(&["http://n1", "http://n2", "http://n3"]);

        let mut voting = join("http://n4");
        voting.voting = Some(true);
        let err = plan_join(&current, "n1", "http://n1", &voting).unwrap_err();
        assert!(err.contains("joint consensus"), "the refusal must say what would make it safe: {}", err);

        let err = plan_join(&current, "n1", "http://n1", &join("http://n2")).unwrap_err();
        assert!(err.contains("already a voting member"),
            "re-adding a voter as a learner would quietly drop it from the quorum: {}", err);

        let err = plan_join(&current, "n1", "http://n1", &join("http://n1")).unwrap_err();
        assert!(err.contains("itself"), "got: {}", err);

        let err = plan_leave(&current, "n1", "http://n2").unwrap_err();
        assert!(err.contains("shrink the quorum"), "got: {}", err);

        let err = plan_leave(&current, "n1", "http://nobody").unwrap_err();
        assert!(err.contains("not a member"), "got: {}", err);

        let mut odd = join("http://n4");
        odd.role = Some("coordinator".into());
        assert!(plan_join(&current, "n1", "http://n1", &odd).is_err());
        assert!(plan_join(&current, "n1", "http://n1", &join("  ")).is_err());

        assert_eq!(current.version, 1, "no refusal may have moved the view");
    }

    #[test]
    fn a_learner_can_be_removed_again() {
        let current = cluster_of(&["http://n1", "http://n2"]);
        let joined = plan_join(&current, "n1", "http://n1", &join("http://n3")).unwrap();
        assert_eq!(joined.members.len(), 3);

        let left = plan_leave(&joined, "n1", "http://n3").unwrap();
        assert_eq!(left.version, 3, "each change is its own version");
        assert!(left.member("http://n3").is_none());
        assert!(left.learners_following("http://n1").is_empty());
        assert_eq!(left.members.iter().filter(|m| m.voting).count(), 2);
    }

    #[test]
    fn re_joining_a_learner_updates_it_in_place() {
        let current = cluster_of(&["http://a", "http://b"]);
        let once = plan_join(&current, "a", "http://a", &join("http://c")).unwrap();

        let mut moved = join("http://c");
        moved.follows = Some("http://b".into());
        let twice = plan_join(&once, "a", "http://a", &moved).unwrap();

        assert_eq!(twice.members.len(), 3, "the same node must not appear twice: {:?}", twice.members);
        assert_eq!(twice.member("http://c").unwrap().follows.as_deref(), Some("http://b"));
        twice.validate().expect("a duplicate would have failed validation");
    }

    #[test]
    fn shard_owners_dedupes_a_node_that_owns_several_ranges() {
        let meta = view(1, "n1", vec![
            shard(0, 1000, "http://a", &["http://a2"]),
            shard(1000, HALF, "http://a", &["http://a2"]),
            shard(HALF, 0, "http://b", &["http://b2"]),
        ]);
        let owners = meta.shard_owners();
        assert_eq!(owners.len(), 2, "two nodes own the ring between them: {:?}", owners);
        assert_eq!(owners[0].0, "http://a");
        assert_eq!(owners[1].0, "http://b");
    }
}
