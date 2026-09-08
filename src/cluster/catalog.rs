//! The cluster-wide index catalogue: how it travels, and how a shard group catches up with it.
//!
//! `LogEntry::Index` stays the authoritative durable definition inside each shard group, and the
//! postings stay derived from committed documents. What this adds is the one thing a per-group log
//! cannot hold: a record of the collection's indexes that outlives which group owns its keys. A
//! group that takes its first key for a collection after the index was defined -- a new shard, a
//! handover, a ring change -- reads the catalogue, appends the definitions it is missing through
//! the ordinary quorum path, and builds its postings before the planner may select them.
//!
//! The catalogue travels by gossip rather than by publication. It rides `ClusterMetadata`, but it
//! is merged per collection instead of being replaced with the winning view, because a schema
//! change is not a routing decision: shards handed their topology by config never adopt anyone's
//! view (`supersedes` refuses a seed), and a catalogue carried only by the winner would never
//! reach them.
//!
//! The round carries topology as well, in both directions. It is the only poll any node runs
//! against another group's leader, so two leaders left holding conflicting views converge through
//! no other path (IB-037). The two halves stay independent: `adopt_cluster` takes a view only if
//! it wins the total order, and merges the catalogue whoever won.

use crate::api::write::local_index_change;
use crate::cluster::metadata::{sanitize_catalog, Adoption, ClusterMetadata};
use crate::consensus::config::is_system_collection;
use crate::replication::write_concern::DEFAULT_WTIMEOUT_MS;
use crate::replication::WriteConcern;
use crate::state::AppState;
use crate::storage::secondary::{IndexChange, IndexSpec};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// One round of gossip and one reconciliation pass. Short because it is the convergence bound for
/// a definition reaching a group that does not hold the collection yet, and affordable because a
/// round between nodes already in step is one small `GET` per peer and no writes at all.
pub const CATALOG_SYNC_INTERVAL_SECS: u64 = 3;

/// Serializes an admin change against reconciliation for one collection, so the reconciler cannot
/// read `active_index_specs` between a catalogue entry landing and the log entry that follows it.
pub fn schema_lock(state: &AppState, collection: &str) -> Arc<tokio::sync::Mutex<()>> {
    let key = format!("index-catalog:{}", collection);
    let mut locks = state.repair_locks.lock().unwrap();
    locks.entry(key)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

pub fn index_catalog_task(state: AppState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(CATALOG_SYNC_INTERVAL_SECS)).await;
            gossip_catalog(&state).await;
            reconcile_local_indexes(&state).await;
        }
    });
}

/// Every node this one can name: its own group from `members`, and one endpoint per shard group
/// from the model in force. A router is the only node that knows every group, which is why it
/// gossips too even though it holds no collection of its own.
fn gossip_targets(state: &AppState) -> Vec<String> {
    let own = state.own_url();
    let view = state.cluster_view();
    let mut seen = HashSet::new();
    let mut targets = Vec::new();
    let mut push = |url: String, targets: &mut Vec<String>| {
        if crate::util::same_endpoint(&url, &own) {
            return;
        }
        if seen.insert(crate::util::node_key(&url)) {
            targets.push(url);
        }
    };

    for member in &view.members {
        push(member.url.clone(), &mut targets);
    }
    for (owner, replicas) in view.shard_owners() {
        push(state.effective_primary(&owner), &mut targets);
        for replica in replicas {
            push(replica, &mut targets);
        }
    }
    targets
}

/// Pulls every peer's catalogue and hands ours back to the ones that differ. Two directions
/// because a definition can originate anywhere: at a shard leader a client asked directly, or at
/// the router that fanned one out.
///
/// Fetched in parallel, the way the router probes: sequentially, one unreachable peer would cost
/// the whole round its connect timeout and push convergence out by that much per dead node.
async fn gossip_catalog(state: &AppState) {
    let fetched = futures::future::join_all(gossip_targets(state).into_iter().map(|target| {
        let client = state.client.clone();
        async move {
            let url = format!("{}/internal/cluster", target);
            let view = match client.get(&url).send().await {
                Ok(r) if r.status().is_success() => r.json::<ClusterMetadata>().await.ok(),
                _ => None,
            };
            // Validated before it is taken: this is a view off the wire, and every node that takes
            // one stores it and hands it on. Sanitized ahead of the fingerprint below, or an entry
            // this node drops and an older peer keeps reads as a difference every round (IB-035).
            let view = view.map(|mut v| {
                sanitize_catalog(&mut v.index_catalog);
                v
            });
            (target, view.filter(|v| v.validate().is_ok()))
        }
    })).await;

    // Sampled once: a catalogue merge moves nothing the total order reads.
    let ours_id = state.cluster_view_id();
    let mut behind = Vec::new();
    let mut winner: Option<(String, ClusterMetadata)> = None;
    for (target, theirs) in fetched {
        let Some(theirs) = theirs else { continue };
        let theirs_fingerprint = theirs.catalog_fingerprint();
        if state.merge_index_catalog(&theirs.index_catalog) {
            info!(target: "catalog", peer = %target, "Took index definitions from a peer");
        }
        behind.push((target.clone(), theirs_fingerprint));
        // Only the best of the round is adopted, not each in turn: an intermediate view would be
        // persisted and reacted to on its way to one that already supersedes it.
        if theirs.view_id().supersedes(&ours_id)
            && winner.as_ref().map_or(true, |(_, best)| theirs.supersedes(best))
        {
            winner = Some((target, theirs));
        }
    }

    // No node polls another group's leader for a topology view, so two leaders holding conflicting
    // ones do not converge (IB-037). The push below already offers a whole view to
    // `/internal/cluster`; taking one here is the same authority in the other direction, decided
    // by the same total order.
    if let Some((source, view)) = winner {
        match state.adopt_cluster(view) {
            Adoption::Adopted { from, to } => info!(target: "catalog", peer = %source,
                "Adopted cluster view v{} from a catalogue round (was v{})", to, from),
            Adoption::Rejected(why) => warn!(target: "catalog", peer = %source,
                "Refused a cluster view offered by a catalogue round: {}", why),
            Adoption::Stale { .. } => {},
        }
    }

    // Sampled after the merges and the adoption, so a peer is only sent a view that is already
    // the union.
    let ours = state.catalog_fingerprint();
    let view = state.cluster_view();
    futures::future::join_all(behind.into_iter()
        .filter(|(_, theirs)| *theirs != ours)
        .map(|(target, _)| {
            let client = state.client.clone();
            let view = view.clone();
            async move {
                let url = format!("{}/internal/cluster", target);
                if let Err(e) = client.post(&url).json(&view).send().await {
                    warn!(target: "catalog", peer = %target, error = %e,
                        "Could not hand the index catalogue to a peer; the next round retries");
                }
            }
        })).await;
}

/// What this group has to append or drop to match the catalogue. A definition is matched on name
/// *and* field, so a redefinition elsewhere is a create here rather than something already in hand.
fn diff(want: &[IndexSpec], have: &[IndexSpec]) -> (Vec<IndexSpec>, Vec<String>) {
    let create: Vec<IndexSpec> = want.iter()
        .filter(|spec| !have.iter().any(|h| h == *spec))
        .cloned()
        .collect();
    let drop: Vec<String> = have.iter()
        .filter(|spec| !want.iter().any(|w| w.name == spec.name))
        .map(|spec| spec.name.clone())
        .collect();
    (create, drop)
}

/// Brings this group's log into line with the catalogue. Only the leader appends: a definition is
/// an ordinary replicated write, so a follower gets it the way it gets every other one.
///
/// A collection the catalogue does not name is left alone. Silence is not an instruction to drop
/// what a group already has -- a node whose catalogue was never populated would otherwise unindex
/// the whole cluster on its first tick.
async fn reconcile_local_indexes(state: &AppState) {
    if !state.is_shard() || !state.is_leader() {
        return;
    }
    let Some(db) = state.db.as_ref() else { return };

    for (name, entry) in state.index_catalog() {
        if is_system_collection(&name) {
            continue;
        }
        let Some(col) = db.existing_collection(&name) else { continue };
        if col.is_dropped() {
            continue;
        }

        // Gate before schema lock, the order the admin handlers take them in. The gate is an
        // RwLock a data movement takes for writing, and tokio queues readers behind a waiting
        // writer: the other order deadlocks against a request holding the gate and waiting here.
        let _write_gate = state.write_gate.read().await;
        let lock = schema_lock(state, &name);
        let _held = lock.lock().await;
        // Against the definitions *in force* rather than the committed ones, so one still staged
        // is not appended a second time.
        let (create, drop) = diff(&entry.indexes, &col.active_index_specs());
        if create.is_empty() && drop.is_empty() {
            continue;
        }

        let changes = create.into_iter().map(|spec| IndexChange::Create { spec })
            .chain(drop.into_iter().map(|name| IndexChange::Drop { name }));
        for change in changes {
            let described = describe(&change);
            match local_index_change(state, &name, change, WriteConcern::Majority,
                Duration::from_millis(DEFAULT_WTIMEOUT_MS)).await
            {
                Ok(outcome) => info!(target: "catalog", collection = %name, change = %described,
                    acks = outcome.acks, required = outcome.required,
                    "Reconciled a secondary index against the cluster catalogue"),
                Err(response) => {
                    warn!(target: "catalog", collection = %name, change = %described,
                        status = %response.status(),
                        "Could not reconcile a secondary index; the next round retries");
                    break;
                },
            }
        }
    }
}

fn describe(change: &IndexChange) -> String {
    match change {
        IndexChange::Create { spec } => format!("create {} on {}", spec.name, spec.field),
        IndexChange::Drop { name } => format!("drop {}", name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        cleanup, keys_for_group, put_value, sharded_cluster, temp_root, TestNode,
    };
    use axum::http::StatusCode;

    fn spec(name: &str, field: &str) -> IndexSpec {
        IndexSpec { name: name.to_string(), field: field.to_string() }
    }

    #[test]
    fn a_group_holding_what_the_catalogue_names_has_nothing_to_do() {
        let want = vec![spec("a", "x"), spec("b", "y")];
        let (create, drop) = diff(&want, &want.iter().rev().cloned().collect::<Vec<_>>());
        assert!(create.is_empty(), "order is not a difference");
        assert!(drop.is_empty());
    }

    #[test]
    fn a_group_that_missed_a_definition_appends_it_and_one_that_missed_a_drop_removes_it() {
        let (create, drop) = diff(&[spec("a", "x")], &[]);
        assert_eq!(create, vec![spec("a", "x")]);
        assert!(drop.is_empty());

        let (create, drop) = diff(&[], &[spec("a", "x")]);
        assert!(create.is_empty());
        assert_eq!(drop, vec!["a".to_string()]);
    }

    /// A create on an existing name replaces it, so the same name over a different field is a
    /// definition this group does not have -- not one it already holds.
    #[test]
    fn a_redefinition_elsewhere_is_a_create_here() {
        let (create, drop) = diff(&[spec("a", "score")], &[spec("a", "age")]);
        assert_eq!(create, vec![spec("a", "score")]);
        assert!(drop.is_empty(), "the create replaces it; dropping it first would lose the name");
    }

    /// Keys the router sends to one half of a two-group ring. Which group a key lands on is the
    /// whole subject here, so they are picked by the hash the router routes by rather than hoped
    /// for.
    fn local_index(node: &TestNode, col: &str, index: &str) -> Option<&'static str> {
        node.state.as_ref()
            .and_then(|s| s.db.as_ref())
            .and_then(|db| db.existing_collection(col))
            .and_then(|c| c.index_status().into_iter()
                .find(|s| s.name == index)
                .map(|s| s.state))
    }

    async fn create(client: &reqwest::Client, base: &str, col: &str, name: &str, field: &str)
        -> StatusCode
    {
        client.post(format!("{}/collections/{}/indexes", base, col))
            .json(&serde_json::json!({"name": name, "field": field}))
            .send().await.unwrap().status()
    }

    async fn matching_keys(client: &reqwest::Client, base: &str, col: &str, filter: &str)
        -> Vec<String>
    {
        let url = format!("{}/collections/{}/query?filter={}&keys=true&limit=200",
            base, col, crate::util::encode_path_segment(filter));
        let body = client.get(&url).send().await.unwrap()
            .json::<serde_json::Value>().await.unwrap();
        let mut keys: Vec<String> = body["keys"].as_array().cloned().unwrap_or_default()
            .iter().filter_map(|k| k.as_str().map(str::to_string)).collect();
        keys.sort();
        keys
    }

    async fn wait_until(deadline: Duration, mut check: impl FnMut() -> bool) -> bool {
        let started = std::time::Instant::now();
        while started.elapsed() < deadline {
            if check() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    /// `IB-024`: the index is defined while one group holds none of the collection, and that group
    /// takes its first key for it afterwards. Nothing in the ring carries schema, so before the
    /// catalogue this group had no definition and no path ever created one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn a_group_that_gains_the_collection_afterwards_picks_up_its_indexes() {
        let root = temp_root();
        let (mut shards, mut router) = sharded_cluster(&root, 2).await;
        let client = reqwest::Client::new();
        let base = router.url();

        let early = keys_for_group("t", true, 8);
        let late = keys_for_group("t", false, 8);
        assert!(early.len() == 8 && late.len() == 8, "both halves need keys to be a test");

        for (i, key) in early.iter().enumerate() {
            assert!(put_value(&client, &base, "t", key,
                serde_json::json!({"age": i % 2}), "").await.is_success());
        }
        assert!(local_index(&shards[1], "t", "by_age").is_none());

        assert_eq!(create(&client, &base, "t", "by_age", "age").await, StatusCode::CREATED);
        assert!(wait_until(Duration::from_secs(20),
            || local_index(&shards[0], "t", "by_age") == Some("ready")).await,
            "the group that held the collection indexes it directly");

        // The second group's first key for this collection, long after the definition.
        for (i, key) in late.iter().enumerate() {
            assert!(put_value(&client, &base, "t", key,
                serde_json::json!({"age": i % 2}), "").await.is_success());
        }

        assert!(wait_until(Duration::from_secs(30),
            || local_index(&shards[1], "t", "by_age") == Some("ready")).await,
            "the catalogue is the only record of the definition this group missed");

        // Same rows the scan gave, on both sides of the reconciliation.
        let mut expected: Vec<String> = early.iter().enumerate().filter(|(i, _)| i % 2 == 1)
            .chain(late.iter().enumerate().filter(|(i, _)| i % 2 == 1))
            .map(|(_, k)| k.clone()).collect();
        expected.sort();
        assert_eq!(matching_keys(&client, &base, "t", r#"{"age": 1}"#).await, expected);

        let listed = client.get(format!("{}/collections/t/indexes", base))
            .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
        assert_eq!(listed["indexes"][0]["state"], "ready");
        assert_eq!(listed["indexes"][0]["documents"], 16, "counts add across the groups");

        router.kill();
        for shard in shards.iter_mut() {
            shard.kill();
        }
        cleanup(&root).await;
    }

    /// The other direction, and the one that says the catalogue is authoritative rather than
    /// additive: a group that was down for the drop must not come back still holding the index.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn a_group_that_was_down_for_a_drop_loses_the_index_when_it_returns() {
        let root = temp_root();
        let (mut shards, mut router) = sharded_cluster(&root, 2).await;
        let client = reqwest::Client::new();
        let base = router.url();

        for (i, key) in keys_for_group("t", true, 4).into_iter()
            .chain(keys_for_group("t", false, 4)).enumerate()
        {
            assert!(put_value(&client, &base, "t", &key,
                serde_json::json!({"age": i}), "").await.is_success());
        }
        assert_eq!(create(&client, &base, "t", "by_age", "age").await, StatusCode::CREATED);
        assert!(wait_until(Duration::from_secs(20),
            || local_index(&shards[1], "t", "by_age") == Some("ready")).await);

        shards[1].kill();
        let dropped = client.delete(format!("{}/collections/t/indexes/by_age", base))
            .send().await.unwrap();
        assert_eq!(dropped.status(), StatusCode::MULTI_STATUS,
            "one group is unreachable, so the fan-out is partial by construction");

        shards[1].start();
        assert!(wait_until(Duration::from_secs(30),
            || shards[1].state.is_some() && local_index(&shards[1], "t", "by_age").is_none()).await,
            "the definition is durable in that group's log; only the catalogue says it is gone");

        // The rows are the collection's, not the index's.
        assert_eq!(matching_keys(&client, &base, "t", r#"{"age": 3}"#).await.len(), 1);

        router.kill();
        for shard in shards.iter_mut() {
            shard.kill();
        }
        cleanup(&root).await;
    }

    /// `IB-037`: two shard leaders left holding conflicting equal-version views. Neither runs a
    /// poller against the other -- `heartbeat_poll_task` only starts on a node that is not leader,
    /// and `leader_contact_task` probes a leader's own replicas -- so the catalogue round is the
    /// only place either of them hears the other's view.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn two_shard_leaders_converge_on_the_winning_view_through_the_catalogue_round() {
        let root = temp_root();
        let (mut shards, mut router) = sharded_cluster(&root, 2).await;
        let client = reqwest::Client::new();

        let view_of = |node: &TestNode| node.state.as_ref().unwrap().cluster_view();
        // The rule the widened round must not break: every node here is still on its own config
        // seed, and a seed loses to everything, so several rounds move nobody.
        tokio::time::sleep(Duration::from_secs(CATALOG_SYNC_INTERVAL_SECS * 2 + 1)).await;
        for node in shards.iter() {
            let view = view_of(node);
            assert!(view.seeded && view.version == 1 && view.ring.is_none(),
                "a config seed must not travel between nodes");
        }

        // One published view per leader at the same version, differing only in the tiebreak.
        let published = |updated_by: &str| {
            let mut v = view_of(&router);
            v.version = 2;
            v.updated_by = updated_by.to_string();
            v.seeded = false;
            v
        };
        let (winner, loser) = (published("zzz-node"), published("aaa-node"));
        assert!(winner.supersedes(&loser), "the tiebreak has to be decided for this to be a test");

        for (node, view) in [(&shards[0], &winner), (&shards[1], &loser)] {
            let answer = client.post(format!("{}/internal/cluster", node.url()))
                .json(view).send().await.unwrap()
                .json::<serde_json::Value>().await.unwrap();
            assert_eq!(answer["status"], "adopted", "{:?}", answer);
        }
        assert_eq!(view_of(&shards[1]).updated_by, "aaa-node",
            "the split has to exist before convergence can be asserted");

        assert!(wait_until(Duration::from_secs(30),
            || view_of(&shards[1]).view_id() == winner.view_id()).await,
            "the losing leader had no poller that would ever reach the winner");
        assert_eq!(view_of(&shards[0]).view_id(), winner.view_id(),
            "and the winner does not take the loser back");

        router.kill();
        for shard in shards.iter_mut() {
            shard.kill();
        }
        cleanup(&root).await;
    }

    /// A restart is the case the catalogue has to be durable for: the group comes back with its
    /// own log and nothing else, and `cluster.meta` is where the cluster-wide half lives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn the_catalogue_is_durable_and_a_restarted_group_reconciles_against_it() {
        let root = temp_root();
        let (mut shards, mut router) = sharded_cluster(&root, 2).await;
        let client = reqwest::Client::new();
        let base = router.url();

        for (i, key) in keys_for_group("t", true, 4).into_iter().enumerate() {
            assert!(put_value(&client, &base, "t", &key,
                serde_json::json!({"age": i}), "").await.is_success());
        }
        assert_eq!(create(&client, &base, "t", "by_age", "age").await, StatusCode::CREATED);
        assert!(wait_until(Duration::from_secs(20),
            || local_index(&shards[0], "t", "by_age") == Some("ready")).await);
        assert!(wait_until(Duration::from_secs(20), || router.state.as_ref()
            .is_some_and(|s| !s.index_catalog().is_empty())).await);

        router.kill();
        router.start();
        assert!(wait_until(Duration::from_secs(20), || router.state.as_ref()
            .and_then(|s| s.index_catalog().get("t").map(|e| e.indexes.len()))
            == Some(1)).await, "the catalogue came back off disk, not from the shards");

        // And it reaches the group that gains the collection after the restart.
        for (i, key) in keys_for_group("t", false, 4).into_iter().enumerate() {
            assert!(put_value(&client, &router.url(), "t", &key,
                serde_json::json!({"age": 100 + i}), "").await.is_success());
        }
        assert!(wait_until(Duration::from_secs(30),
            || local_index(&shards[1], "t", "by_age") == Some("ready")).await);

        router.kill();
        for shard in shards.iter_mut() {
            shard.kill();
        }
        cleanup(&root).await;
    }
}
