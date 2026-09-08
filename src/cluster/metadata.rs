//! Versioned cluster topology: the runtime source of truth for routing and membership.

use crate::config::NodeConfig;
use crate::ring::{validate_shard_ring, HashRing, ShardInfo};
use crate::storage::secondary::{
    valid_field_path, valid_index_name, IndexChange, IndexSpec, MAX_INDEXES_PER_COLLECTION,
};
use crate::util::{node_key, write_atomic};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
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
    /// A handover in flight. `ring` stays authoritative for routing throughout; this only says
    /// where the keys are going, so every node can agree on which keys are in the middle of moving.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration: Option<Migration>,
    /// What indexes each collection has, cluster-wide. Merged rather than replaced when views
    /// meet, so it converges on its own clock -- see `IndexCatalog`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub index_catalog: IndexCatalog,
}

/// Index definitions as a fact about a *collection* rather than about the shard group that happens
/// to hold its keys today. `LogEntry::Index` stays the durable definition inside each group; this
/// is what a group consults to find out which ones it is missing.
///
/// Versioned per collection and merged rather than replaced, because it changes on a different
/// clock from the topology. A shard handed its ring by config never adopts anyone's topology --
/// `supersedes` refuses a seed -- and replacing the catalogue with the winning view's would mean
/// such a node never learns a definition either.
pub type IndexCatalog = BTreeMap<String, CollectionIndexes>;

/// One collection's definitions, and how far along they are. `updated_by` is the same tiebreak
/// `supersedes` uses, for the same reason: two nodes that changed the same collection at the same
/// version must converge on one of the two rather than alternate.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct CollectionIndexes {
    pub version: u64,
    #[serde(default)]
    pub updated_by: String,
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
}

impl CollectionIndexes {
    fn supersedes(&self, other: &Self) -> bool {
        (self.version, self.updated_by.as_str()) > (other.version, other.updated_by.as_str())
    }
}

/// Ceiling on how many collections the catalogue names. It rides every cluster view, and an entry
/// survives the collection it describes so a drop cannot be resurrected by a stale copy, so the
/// only bound on its growth is this one.
pub const MAX_CATALOG_COLLECTIONS: usize = 4096;

/// Why no node could act on this entry, or `None`. Separate from `validate` because the rules it
/// applies tighten between releases: an entry a newer rule made unaddressable must cost the entry
/// and not the whole view carrying it, or a rolling upgrade takes routing down (IB-035).
fn unaddressable_entry(collection: &str, entry: &CollectionIndexes) -> Option<String> {
    // The same gate the API puts in front of a name that becomes a directory.
    if !crate::consensus::config::valid_collection_name(collection) {
        return Some(format!("'{}' is not a usable collection name", collection));
    }
    if entry.indexes.len() > MAX_INDEXES_PER_COLLECTION {
        return Some(format!("'{}' is given more than {} indexes", collection,
            MAX_INDEXES_PER_COLLECTION));
    }
    let mut seen = HashSet::new();
    for spec in &entry.indexes {
        if !valid_index_name(&spec.name) {
            return Some(format!("'{}' has an invalid index name", collection));
        }
        if !valid_field_path(&spec.field) {
            return Some(format!("'{}.{}' has an invalid field path", collection, spec.name));
        }
        if !seen.insert(spec.name.as_str()) {
            return Some(format!("'{}' names '{}' more than once", collection, spec.name));
        }
    }
    None
}

/// Drops the entries no node could act on, returning one reason per drop. Runs on every catalogue
/// arriving from disk or off the wire before the view is compared or stored, so the fingerprint is
/// computed over what the node actually holds rather than over what it was handed.
pub fn sanitize_catalog(catalog: &mut IndexCatalog) -> Vec<String> {
    let mut dropped = Vec::new();
    catalog.retain(|collection, entry| match unaddressable_entry(collection, entry) {
        Some(why) => {
            dropped.push(why);
            false
        },
        None => true,
    });
    dropped
}

/// Takes the newer entry per collection. Returns whether `into` moved, which is what says a
/// merge is worth persisting or handing on.
pub fn merge_catalog(into: &mut IndexCatalog, from: &IndexCatalog) -> bool {
    let mut moved = false;
    for (collection, incoming) in from {
        if unaddressable_entry(collection, incoming).is_some() {
            continue;
        }
        match into.get(collection) {
            Some(current) if !incoming.supersedes(current) => {},
            _ => {
                into.insert(collection.clone(), incoming.clone());
                moved = true;
            },
        }
    }
    moved
}

/// The plan, not the progress. Progress is per node and lives in memory: a half-copied shard is
/// not a fact about the cluster, and putting it in the view would make every batch a publish.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Migration {
    pub id: String,
    pub target: HashRing,
    pub started_by: String,
    #[serde(default)]
    pub phase: MigrationPhase,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    #[default]
    Copy,
    Finalizing,
}

/// The three fields the total order over views is computed from. A heartbeat advertises this
/// rather than the version alone: a poller gated on `version >` will not fetch a peer whose view
/// wins the `updated_by` tiebreak at the same version, so two nodes that published concurrently
/// during a partition stayed split with no poll that would ever repair it (IB-023).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ViewId {
    pub version: u64,
    /// `None` from a peer that predates the advertised field. Equal versions are unorderable
    /// then, so the comparison falls back to strictly-greater rather than guessing a winner.
    pub updated_by: Option<String>,
    pub seeded: bool,
}

impl ViewId {
    /// `ClusterMetadata::supersedes` reads nothing else, so a decision made here and one made
    /// against the whole view cannot disagree.
    pub fn supersedes(&self, other: &ViewId) -> bool {
        // A shard seeds an empty ring. Letting that outrank a router's seeded ring on a tiebreak
        // would route every key nowhere, so a seed loses to everything and wins against nothing.
        if self.seeded {
            return false;
        }
        if other.seeded {
            return self.version >= other.version;
        }
        match (&self.updated_by, &other.updated_by) {
            (Some(mine), Some(theirs)) => (self.version, mine) > (other.version, theirs),
            _ => self.version > other.version,
        }
    }
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
            migration: None,
            index_catalog: IndexCatalog::new(),
        }
    }

    /// Everything the total order reads, and nothing else, so a peer can be asked whether
    /// fetching its view would gain anything without fetching it.
    pub fn view_id(&self) -> ViewId {
        ViewId {
            version: self.version,
            updated_by: Some(self.updated_by.clone()),
            seeded: self.seeded,
        }
    }

    /// Total order over views, so every node converges on the same one.
    ///
    /// The `updated_by` tiebreak makes equal versions converge instead of splitting the cluster's
    /// routing view, which is the failure that actually loses writes. It does so by discarding one
    /// of two concurrent updates: nothing here makes concurrent updates safe, only deterministic.
    pub fn supersedes(&self, other: &Self) -> bool {
        self.view_id().supersedes(&other.view_id())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version == 0 {
            return Err("cluster metadata version must be at least 1".to_string());
        }
        if let Some(ring) = &self.ring {
            ring.validate()?;
        }
        if let Some(migration) = &self.migration {
            migration.target.validate().map_err(|e| format!("migration target: {}", e))?;
            if self.ring.is_none() {
                return Err("a migration needs a ring to move away from".to_string());
            }
        }
        // Validated even when a ring supersedes it, so a rollback publish cannot restore a bad map.
        if !self.shards.is_empty() {
            validate_shard_ring(&self.shards)?;
        }
        self.validate_catalog()?;

        let mut seen = HashSet::new();
        for member in &self.members {
            if member.url.trim().is_empty() {
                return Err("member url must not be empty".to_string());
            }
            if !seen.insert(crate::util::node_key(&member.url)) {
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
                    Ok(mut meta) => {
                        for why in sanitize_catalog(&mut meta.index_catalog) {
                            tracing::warn!(target: "cluster", file = %name,
                                "Dropped an unusable index catalogue entry: {}", why);
                        }
                        match meta.validate() {
                            Ok(()) => return Ok(Some(meta)),
                            Err(why) => corrupt = Some((name.to_string(), why)),
                        }
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
        let mut merged = None;
        if let Ok(Some(mut existing)) = Self::load(data_dir) {
            if !self.supersedes(&existing) {
                // The catalogue moves on its own clock, so a view that loses on topology can still
                // carry a definition the durable copy has not got. Keep that copy's topology.
                if !merge_catalog(&mut existing.index_catalog, &self.index_catalog) {
                    return Ok(());
                }
                merged = Some(existing);
            }
        }

        fs::create_dir_all(&dir)?;
        let content = serde_json::to_vec(merged.as_ref().unwrap_or(self))
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

    /// The members a quorum is computed over. Routers never vote, and a learner is shipped frames
    /// without being counted, so neither belongs here.
    pub fn voting_shards(&self) -> Vec<String> {
        self.members.iter()
            .filter(|m| m.voting && m.role == "shard")
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

    pub(crate) fn bump(&mut self, by: &str) {
        self.version += 1;
        self.updated_by = by.to_string();
        // The result is a decision, not a guess, however it was derived.
        self.seeded = false;
    }

    /// Fingerprint of what maps a key to an owner: the ranges and the endpoints owning them, plus
    /// the target of a migration in flight, since that moves keys too. Replicas are deliberately
    /// out of it — one joining or leaving moves no key, and a scan must not be disturbed by it.
    pub fn partition_fingerprint(&self) -> u64 {
        let mut parts: Vec<String> = match &self.ring {
            // Vnode tokens come from `node_key` and index alone, so the owning set fixes the mapping.
            Some(ring) => std::iter::once(format!("vnodes={}", ring.vnodes))
                .chain(ring.shards.iter().map(|s| node_key(&s.node_url)))
                .collect(),
            None => self.shards.iter()
                .map(|s| format!("{}-{}@{}", s.start_hash, s.end_hash, node_key(&s.node_url)))
                .collect(),
        };
        if let Some(m) = &self.migration {
            parts.push(format!("->{}", m.id));
        }
        // Sorted: the same owners listed in a different order are the same partitioning.
        parts.sort();
        xxhash_rust::xxh64::xxh64(parts.join("|").as_bytes(), 0)
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

    /// Whether this view records shard groups at all. Neither model present means one group, and
    /// then every shard member is in it -- which is what a single-group deployment looks like.
    pub fn groups_known(&self) -> bool {
        self.ring.is_some() || !self.shards.is_empty()
    }

    /// The nodes sharing a shard group with `url`, `url` included. `members` is one flat list
    /// across every group and `Member` carries no shard affinity, so the ring (or the legacy shard
    /// map) is the only record of who belongs with whom.
    ///
    /// `None` when nothing in force names this node: either no groups are recorded, or one is and
    /// this node is outside all of them. The two cases are different and `groups_known` tells them
    /// apart -- callers must not treat the second as licence to fall back to the whole cluster.
    pub fn shard_group(&self, url: &str) -> Option<Vec<String>> {
        let groups = self.shard_owners();
        let names = |group: &(String, Vec<String>), who: &str| {
            same_url(&group.0, who) || group.1.iter().any(|replica| same_url(replica, who))
        };
        // A learner is not in the ring; the primary it follows is what places it in a group.
        let anchor = match self.member(url).and_then(|m| m.follows.clone()) {
            Some(follows) if !groups.iter().any(|group| names(group, url)) => follows,
            _ => url.to_string(),
        };

        groups.into_iter()
            .find(|group| names(group, &anchor))
            .map(|(node_url, replicas)| {
                let mut out = vec![node_url];
                out.extend(replicas);
                out
            })
    }

    /// Next version of this view with `ring` in force. Clears any migration: the ring landing is
    /// what completing one means, and leaving the plan behind would freeze the keys it named.
    pub fn with_ring(&self, by: &str, ring: HashRing) -> Self {
        let mut next = self.clone();
        next.ring = Some(ring);
        next.migration = None;
        next.bump(by);
        next
    }

    pub fn with_migration(&self, by: &str, migration: Option<Migration>) -> Self {
        let mut next = self.clone();
        next.migration = migration;
        next.bump(by);
        next
    }

    /// Records `change` against this collection's entry. Returns whether anything moved: a create
    /// naming a definition already there is not a version, or the fan-out that reaches every group
    /// would bump one per group for a single client request.
    ///
    /// Deliberately not a `bump`: the topology version is a decision about routing, and a schema
    /// change is not one. Bumping it would make a shard's seeded view outrank a router's seeded
    /// ring and route every key nowhere.
    pub fn record_index_change(&mut self, by: &str, collection: &str, change: &IndexChange) -> bool {
        let current = self.index_catalog.get(collection);
        let mut indexes = current.map(|e| e.indexes.clone()).unwrap_or_default();
        change.apply_to(&mut indexes);
        if current.is_some_and(|e| e.indexes == indexes) {
            return false;
        }
        // A drop naming an index of a collection the catalogue has never recorded changes nothing,
        // and an entry would say more than that: an empty entry licenses reconciliation to remove
        // whatever a group holds, and silence must not turn into that by way of a stray request.
        if current.is_none() && indexes.is_empty() {
            return false;
        }
        if current.is_none() && self.index_catalog.len() >= MAX_CATALOG_COLLECTIONS {
            return false;
        }
        let version = current.map_or(0, |e| e.version) + 1;
        self.index_catalog.insert(collection.to_string(), CollectionIndexes {
            version,
            updated_by: by.to_string(),
            indexes,
        });
        true
    }

    /// A dropped collection keeps its entry, emptied. Removing it would let a copy held by a node
    /// that missed the drop win the merge and put the definitions back on whoever recreates it.
    pub fn forget_collection_indexes(&mut self, by: &str, collection: &str) -> bool {
        match self.index_catalog.get(collection) {
            Some(entry) if entry.indexes.is_empty() => false,
            Some(entry) => {
                let version = entry.version + 1;
                self.index_catalog.insert(collection.to_string(), CollectionIndexes {
                    version, updated_by: by.to_string(), indexes: Vec::new(),
                });
                true
            },
            None => false,
        }
    }

    /// "Do we hold the same definitions?", in one number. What the gossip loop hands a peer its
    /// view back over, so two nodes already in step exchange nothing further.
    pub fn catalog_fingerprint(&self) -> u64 {
        let mut parts: Vec<String> = self.index_catalog.iter()
            .map(|(name, entry)| format!("{}@{}:{}", name, entry.version, entry.updated_by))
            .collect();
        parts.sort();
        xxhash_rust::xxh64::xxh64(parts.join("|").as_bytes(), 0)
    }

    /// The size bound only. Per-entry shape belongs to `unaddressable_entry`, which drops the entry
    /// rather than failing the document; how many entries ride every view is a fact about the
    /// document and has no per-entry answer.
    fn validate_catalog(&self) -> Result<(), String> {
        if self.index_catalog.len() > MAX_CATALOG_COLLECTIONS {
            return Err(format!("index catalogue names more than {} collections",
                MAX_CATALOG_COLLECTIONS));
        }
        Ok(())
    }
}

fn same_url(a: &str, b: &str) -> bool {
    crate::util::same_endpoint(a, b)
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
pub fn adopt(current: &mut ClusterMetadata, mut incoming: ClusterMetadata) -> Adoption {
    // Before the validate and before the version comparison the catalogue feeds: an entry no node
    // can act on is dropped, never a reason to refuse the topology it rode in on (IB-035).
    for why in sanitize_catalog(&mut incoming.index_catalog) {
        tracing::warn!(target: "cluster", version = incoming.version,
            "Dropped an index catalogue entry from an offered view: {}", why);
    }
    if let Err(why) = incoming.validate() {
        return Adoption::Rejected(why);
    }
    // Merged before the topology decision and kept across it, in both directions. The catalogue is
    // versioned per collection, so a view that loses on topology can still carry a definition this
    // node lacks -- and one that wins must not take away the definitions this node already had.
    let mut catalog = current.index_catalog.clone();
    merge_catalog(&mut catalog, &incoming.index_catalog);

    if !incoming.supersedes(current) {
        current.index_catalog = catalog;
        return Adoption::Stale { current: current.version };
    }
    let from = current.version;
    let to = incoming.version;
    *current = incoming;
    current.index_catalog = catalog;
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
        return Err("a node cannot join as voting; it joins as a learner and replicates \
                    immediately, and POST /cluster/configuration promotes it through joint \
                    consensus once it has caught up".to_string());
    }
    let role = req.role.clone().unwrap_or_else(|| "shard".to_string());
    if role != "shard" && role != "router" {
        return Err(format!("unknown role '{}'", role));
    }
    let shard_role = req
        .shard_role
        .clone()
        .or_else(|| (role == "shard").then(|| "replica".to_string()));
    if shard_role.as_deref() != Some("replica") && req.follows.is_some() {
        return Err("only a shard replica may name a primary to follow".to_string());
    }
    if same_url(&req.url, leader_url) {
        return Err("a leader cannot add itself as a learner".to_string());
    }
    if let Some(follows) = req.follows.as_deref() {
        if same_url(&req.url, follows) {
            return Err("a replica cannot follow itself".to_string());
        }
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
        shard_role: shard_role.clone(),
        voting: false,
        follows: (shard_role.as_deref() == Some("replica"))
            .then(|| req.follows.clone().unwrap_or_else(|| leader_url.to_string())),
    };

    let next = current.with_member(by, member);
    next.validate().map(|_| next).map_err(|e| e)
}

/// The view caught up with a committed configuration. Descriptive, not decisive: the log is what
/// moved the quorum, and this only records where it moved to, so a node that has not adopted the
/// view yet is behind on routing rather than voting against the change.
///
/// A demoted node stays a member as a learner rather than being dropped. It keeps receiving frames,
/// and `in_quorum` reads the view when no configuration entry has reached the node yet.
pub fn plan_configuration(
    current: &ClusterMetadata,
    by: &str,
    leader_url: &str,
    voters: &[String],
) -> Result<ClusterMetadata, String> {
    let mut next = current.clone();
    for member in next.members.iter_mut() {
        if member.role != "shard" {
            continue;
        }
        let voting = voters.iter().any(|v| same_url(v, &member.url));
        if voting == member.voting {
            continue;
        }
        member.voting = voting;
        member.follows = (!voting && !same_url(&member.url, leader_url))
            .then(|| leader_url.to_string());
    }
    // A voter admitted straight from config may never have been a member of the view.
    for voter in voters {
        if next.member(voter).is_none() {
            next.members.push(Member {
                url: voter.clone(),
                node_id: None,
                role: "shard".to_string(),
                shard_role: Some(if same_url(voter, leader_url) { "primary" } else { "replica" }.to_string()),
                voting: true,
                follows: None,
            });
        }
    }
    next.bump(by);
    next.validate().map(|_| next)
}

pub fn plan_leave(current: &ClusterMetadata, by: &str, url: &str) -> Result<ClusterMetadata, String> {
    match current.member(url) {
        None => Err(format!("{} is not a member", url)),
        // Dropping it from the view is not what shrinks the quorum -- the log is -- but a member
        // the log still counts and the view cannot name is a voter nothing can route to.
        Some(m) if m.voting => Err(format!(
            "{} is a voting member; demote it with POST /cluster/configuration first, then \
             remove it", url)),
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
            version, updated_by: by.to_string(), seeded: false, members: Vec::new(), shards,
            ring: None, migration: None, index_catalog: Default::default(),
        }
    }

    fn cfg(json: serde_json::Value) -> NodeConfig {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn a_shard_group_comes_from_the_ring_never_from_the_member_list() {
        use crate::ring::{HashRing, RingShard};

        let group = |url: &str, replicas: &[&str]| RingShard {
            node_url: url.to_string(),
            replica_urls: replicas.iter().map(|r| r.to_string()).collect(),
        };
        let member = |url: &str, follows: Option<&str>| Member {
            url: url.to_string(), node_id: None, role: "shard".to_string(),
            shard_role: None, voting: follows.is_none(),
            follows: follows.map(str::to_string),
        };

        let mut v = view(2, "op", Vec::new());
        // Every node in one flat list, which is what the member list always is.
        v.members = ["http://a", "http://b1", "http://b2", "http://b3"]
            .iter().map(|u| member(u, None)).collect();
        v.members.push(member("http://learner", Some("http://b1")));

        assert!(!v.groups_known(), "no ring and no shard map is one group, not zero");
        assert_eq!(v.shard_group("http://b2"), None);

        v.ring = Some(HashRing {
            vnodes: 128,
            shards: vec![group("http://a", &[]), group("http://b1", &["http://b2", "http://b3"])],
        });

        assert!(v.groups_known());
        assert_eq!(v.shard_group("http://a"), Some(vec!["http://a".to_string()]));
        assert_eq!(v.shard_group("http://b2"), Some(vec![
            "http://b1".to_string(), "http://b2".to_string(), "http://b3".to_string(),
        ]), "a replica's group is the shard that names it, not every shard node in the cluster");
        assert_eq!(v.shard_group("http://learner"), v.shard_group("http://b1"),
            "a learner is not in the ring; the primary it follows is what places it");
        assert_eq!(v.shard_group("http://nobody"), None,
            "a node no shard names has no group -- which is not the same as having them all");
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
    }

    #[test]
    fn a_staging_file_left_by_a_crash_is_read_back() {
        let root = temp_root();
        let dir = root.to_string_lossy().to_string();

        let bytes = serde_json::to_vec(&view(9, "n1", vec![shard(0, 0, "http://a", &[])])).unwrap();
        fs::write(root.join(CLUSTER_TMP), bytes).unwrap();

        let back = ClusterMetadata::load(&dir).unwrap().expect("the staging file is complete once fsynced");
        assert_eq!(back.version, 9);
    }

    fn spec(name: &str, field: &str) -> IndexSpec {
        IndexSpec { name: name.to_string(), field: field.to_string() }
    }

    fn created(name: &str, field: &str) -> IndexChange {
        IndexChange::Create { spec: spec(name, field) }
    }

    #[test]
    fn a_definition_is_recorded_once_however_many_groups_the_request_reached() {
        let mut v = view(4, "n1", vec![]);
        assert!(v.record_index_change("n1", "t", &created("by_age", "age")));
        assert_eq!(v.index_catalog["t"].version, 1);
        assert!(!v.record_index_change("n1", "t", &created("by_age", "age")),
            "a fan-out that reaches three groups is one client request, not three versions");
        assert_eq!(v.index_catalog["t"].version, 1);

        assert!(v.record_index_change("n1", "t", &created("by_age", "score")),
            "the same name over a different field is a different definition");
        assert_eq!(v.index_catalog["t"].version, 2);
    }

    /// The catalogue is a schema change, and routing must not move because one happened: a bumped
    /// topology version on a shard's seeded view would outrank a router's seeded ring.
    #[test]
    fn recording_a_definition_moves_no_topology_version() {
        let mut v = ClusterMetadata::seed_from_config(&cfg(serde_json::json!({
            "node_id": "n1", "role": "shard", "listen_addr": "127.0.0.1:1", "data_dir": "d",
        })));
        assert!(v.seeded && v.version == 1);
        assert!(v.record_index_change("n1", "t", &created("i", "age")));
        assert_eq!((v.version, v.seeded), (1, true));
    }

    #[test]
    fn a_drop_naming_a_collection_the_catalogue_never_recorded_records_nothing() {
        let mut v = view(4, "n1", vec![]);
        assert!(!v.record_index_change("n1", "t", &IndexChange::Drop { name: "i".into() }));
        assert!(v.index_catalog.is_empty(),
            "an empty entry says the collection has no indexes, which licenses a group to drop \
             what it holds; a stray request must not say that");
    }

    #[test]
    fn a_dropped_collection_keeps_an_emptied_entry_rather_than_losing_it() {
        let mut v = view(4, "n1", vec![]);
        v.record_index_change("n1", "t", &created("i", "age"));
        assert!(v.forget_collection_indexes("n1", "t"));
        let entry = &v.index_catalog["t"];
        assert!(entry.indexes.is_empty());
        assert_eq!(entry.version, 2, "a node that missed the drop must lose the merge");
        assert!(!v.forget_collection_indexes("n1", "t"), "and dropping it twice is not a version");
        assert!(!v.forget_collection_indexes("n1", "never_indexed"));
    }

    #[test]
    fn merging_takes_the_newer_entry_per_collection_in_both_directions() {
        let mut a = view(4, "n1", vec![]);
        a.record_index_change("n1", "orders", &created("i", "age"));
        let mut b = view(4, "n2", vec![]);
        b.record_index_change("n2", "users", &created("j", "email"));

        assert!(merge_catalog(&mut a.index_catalog, &b.index_catalog));
        assert_eq!(a.index_catalog.len(), 2, "neither node knew about the other's collection");
        assert!(!merge_catalog(&mut a.index_catalog, &b.index_catalog),
            "a second round has nothing to hand over");

        // A newer entry for the same collection replaces the older one whole.
        let mut newer = a.clone();
        newer.record_index_change("n3", "orders", &IndexChange::Drop { name: "i".into() });
        assert!(merge_catalog(&mut a.index_catalog, &newer.index_catalog));
        assert!(a.index_catalog["orders"].indexes.is_empty());
    }

    /// The half of this that a topology-only merge would miss: a shard behind a router never adopts
    /// anyone's view, so a catalogue carried only by the winner would never reach it.
    #[test]
    fn a_view_that_loses_on_topology_still_hands_over_its_definitions() {
        let mut current = view(9, "n1", vec![]);
        let mut behind = view(2, "n2", vec![]);
        behind.record_index_change("n2", "t", &created("i", "age"));

        assert_eq!(adopt(&mut current, behind), Adoption::Stale { current: 9 });
        assert_eq!(current.version, 9, "the topology it lost with must not travel");
        assert_eq!(current.index_catalog["t"].indexes, vec![spec("i", "age")]);
    }

    #[test]
    fn a_view_that_wins_on_topology_does_not_take_away_definitions_it_never_had() {
        let mut current = view(2, "n1", vec![]);
        current.record_index_change("n1", "t", &created("i", "age"));
        let ahead = view(9, "n2", vec![]);

        assert_eq!(adopt(&mut current, ahead), Adoption::Adopted { from: 2, to: 9 });
        assert_eq!(current.version, 9);
        assert_eq!(current.index_catalog["t"].indexes, vec![spec("i", "age")],
            "a ring change is not a schema change and must not read as one");
    }

    fn entry(indexes: Vec<IndexSpec>) -> CollectionIndexes {
        CollectionIndexes { version: 1, updated_by: "n1".into(), indexes }
    }

    #[test]
    fn a_malformed_catalogue_entry_is_dropped_rather_than_kept() {
        let cases = [
            ("t", entry(vec![spec("ok", "a..b")]), "a field path off the wire is stored by every node"),
            ("../escape", entry(vec![spec("i", "a")]), "and the name is what becomes a directory"),
            ("t", entry(vec![spec("i", "a"), spec("i", "b")]), "one name cannot index two fields"),
            ("t", entry((0..MAX_INDEXES_PER_COLLECTION + 1)
                .map(|i| spec(&format!("i{}", i), "a")).collect()), "an unbounded entry"),
        ];
        for (collection, entry, why) in cases {
            let mut catalog = IndexCatalog::new();
            catalog.insert(collection.into(), entry);
            assert_eq!(sanitize_catalog(&mut catalog).len(), 1, "{}", why);
            assert!(catalog.is_empty(), "{}", why);
        }
    }

    #[test]
    fn a_catalogue_naming_more_collections_than_the_bound_is_refused() {
        let mut v = view(4, "n1", vec![]);
        for i in 0..MAX_CATALOG_COLLECTIONS + 1 {
            v.index_catalog.insert(format!("c{}", i), entry(vec![spec("i", "a")]));
        }
        assert!(v.validate().is_err(), "the size bound is a fact about the document");
    }

    /// IB-035: the collection-name rule narrowed after these documents were written, and a view
    /// that fails to validate is refused whole -- routing included.
    #[test]
    fn a_name_a_newer_rule_rejects_costs_its_entry_and_not_the_view() {
        let mut incoming = view(9, "n2", vec![]);
        incoming.index_catalog.insert("Legacy.Name".into(), entry(vec![spec("i", "a")]));
        incoming.index_catalog.insert("users".into(), entry(vec![spec("j", "email")]));

        let mut current = view(4, "n1", vec![]);
        assert_eq!(adopt(&mut current, incoming), Adoption::Adopted { from: 4, to: 9 });
        assert_eq!(current.version, 9, "an old peer's push must not take routing down");
        assert!(!current.index_catalog.contains_key("Legacy.Name"));
        assert_eq!(current.index_catalog["users"].indexes, vec![spec("j", "email")],
            "the rest of the catalogue still travels");
    }

    #[test]
    fn a_dropped_entry_leaves_the_fingerprint_where_a_node_that_never_saw_it_stands() {
        let mut theirs = view(9, "n2", vec![]);
        theirs.index_catalog.insert("Legacy.Name".into(), entry(vec![spec("i", "a")]));
        theirs.record_index_change("n2", "users", &created("j", "email"));

        let mut ours = view(4, "n1", vec![]);
        adopt(&mut ours, theirs.clone());

        sanitize_catalog(&mut theirs.index_catalog);
        assert_eq!(ours.catalog_fingerprint(), theirs.catalog_fingerprint(),
            "or every gossip round reads the dropped entry as something still to exchange");
    }

    #[test]
    fn a_catalogue_that_cannot_be_addressed_is_read_back_rather_than_called_corrupt() {
        let root = temp_root();
        let dir = root.to_string_lossy().to_string();

        let mut written = view(5, "n1", vec![]);
        written.index_catalog.insert("Legacy.Name".into(), entry(vec![spec("i", "a")]));
        written.record_index_change("n1", "users", &created("j", "email"));
        fs::create_dir_all(&dir).unwrap();
        fs::write(Path::new(&dir).join(CLUSTER_FILE),
            serde_json::to_vec(&written).unwrap()).unwrap();

        let back = ClusterMetadata::load(&dir).unwrap().expect("the topology is still readable");
        assert_eq!(back.version, 5);
        assert!(!back.index_catalog.contains_key("Legacy.Name"));
        assert!(back.index_catalog.contains_key("users"));
    }

    #[test]
    fn a_merge_never_takes_an_entry_no_node_could_act_on() {
        let mut from = IndexCatalog::new();
        from.insert("Legacy.Name".into(), entry(vec![spec("i", "a")]));
        let mut into = IndexCatalog::new();
        assert!(!merge_catalog(&mut into, &from), "nothing moved, so nothing is worth persisting");
        assert!(into.is_empty());
    }

    #[test]
    fn the_fingerprint_moves_with_the_catalogue_and_with_nothing_else() {
        let mut v = view(4, "n1", vec![]);
        let empty = v.catalog_fingerprint();
        v.record_index_change("n1", "t", &created("i", "age"));
        let defined = v.catalog_fingerprint();
        assert_ne!(empty, defined);

        let mut moved = v.clone();
        moved.bump("n2");
        assert_eq!(moved.catalog_fingerprint(), defined,
            "a topology change is not something to exchange catalogues over");
    }

    #[test]
    fn the_catalogue_is_persisted_and_survives_a_view_that_does_not_supersede() {
        let root = temp_root();
        let dir = root.to_string_lossy().to_string();

        let mut current = view(5, "n1", vec![]);
        current.record_index_change("n1", "t", &created("i", "age"));
        current.save(&dir).unwrap();

        // An older view carrying a definition this one has not got: its topology is refused and
        // its catalogue is kept, which is the same rule `adopt` follows in memory.
        let mut older = view(2, "n2", vec![]);
        older.record_index_change("n2", "users", &created("j", "email"));
        older.save(&dir).unwrap();

        let back = ClusterMetadata::load(&dir).unwrap().unwrap();
        assert_eq!(back.version, 5, "the durable topology must never rewind");
        assert_eq!(back.index_catalog.len(), 2);
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
    fn only_replicas_may_name_a_primary() {
        let current = cluster_of(&["http://a", "http://b"]);
        let mut primary = join("http://new");
        primary.shard_role = Some("primary".into());
        primary.follows = Some("http://a".into());
        assert!(plan_join(&current, "a", "http://a", &primary).is_err());

        let mut router = join("http://router");
        router.role = Some("router".into());
        router.follows = Some("http://a".into());
        assert!(plan_join(&current, "a", "http://a", &router).is_err());

        let mut own_follower = join("http://new");
        own_follower.follows = Some("http://new".into());
        assert!(plan_join(&current, "a", "http://a", &own_follower).is_err());
    }

    #[test]
    fn membership_changes_that_would_move_the_quorum_are_refused() {
        let current = cluster_of(&["http://n1", "http://n2", "http://n3"]);

        let mut voting = join("http://n4");
        voting.voting = Some(true);
        let err = plan_join(&current, "n1", "http://n1", &voting).unwrap_err();
        assert!(err.contains("/cluster/configuration"),
            "the refusal must name what would make it safe: {}", err);

        let err = plan_join(&current, "n1", "http://n1", &join("http://n2")).unwrap_err();
        assert!(err.contains("already a voting member"),
            "re-adding a voter as a learner would quietly drop it from the quorum: {}", err);

        let err = plan_join(&current, "n1", "http://n1", &join("http://n1")).unwrap_err();
        assert!(err.contains("itself"), "got: {}", err);

        let err = plan_leave(&current, "n1", "http://n2").unwrap_err();
        assert!(err.contains("demote it"), "got: {}", err);

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

    /// M13: an unsorted cursor's positions only mean anything against the layout they were taken
    /// against, so the fingerprint has to move when ownership moves and stay put when it does not.
    #[test]
    fn the_partition_fingerprint_tracks_ownership_and_nothing_else() {
        let range = |start: u64, end: u64, url: &str, replicas: Vec<&str>| ShardInfo {
            start_hash: start,
            end_hash: end,
            node_url: url.to_string(),
            replica_urls: replicas.into_iter().map(str::to_string).collect(),
        };
        let split = |at: u64, replicas: Vec<&str>| view(1, "t", vec![
            range(0, at, "http://a", replicas.clone()),
            range(at, 0, "http://b", Vec::new()),
        ]);

        let base = split(100, Vec::new()).partition_fingerprint();
        assert_eq!(split(100, Vec::new()).partition_fingerprint(), base, "the same layout twice");
        assert_eq!(split(100, vec!["http://r1"]).partition_fingerprint(), base,
            "a replica joining moves no key and must not invalidate a scan");
        assert_ne!(split(200, Vec::new()).partition_fingerprint(), base, "a moved boundary moves keys");

        let mut reordered = split(100, Vec::new());
        reordered.shards.reverse();
        assert_eq!(reordered.partition_fingerprint(), base, "the same ranges listed the other way");

        let mut renamed = split(100, Vec::new());
        renamed.shards[1].node_url = "http://c".to_string();
        assert_ne!(renamed.partition_fingerprint(), base, "a different owner for the same range");

        let mut trailing_slash = split(100, Vec::new());
        trailing_slash.shards[0].node_url = "http://a/".to_string();
        assert_eq!(trailing_slash.partition_fingerprint(), base, "endpoints, not raw URLs");

        let ringed = ClusterMetadata {
            ring: Some(HashRing { vnodes: 8, shards: vec![
                crate::ring::RingShard { node_url: "http://a".to_string(), replica_urls: Vec::new() },
            ]}),
            ..view(2, "t", Vec::new())
        };
        let mut migrating = ringed.clone();
        migrating.migration = Some(Migration {
            id: "m1".to_string(),
            target: HashRing { vnodes: 8, shards: vec![
                crate::ring::RingShard { node_url: "http://a".to_string(), replica_urls: Vec::new() },
                crate::ring::RingShard { node_url: "http://b".to_string(), replica_urls: Vec::new() },
            ]},
            started_by: "t".to_string(),
            phase: MigrationPhase::default(),
        });
        assert_ne!(migrating.partition_fingerprint(), ringed.partition_fingerprint(),
            "keys are moving while a handover is in flight");
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
