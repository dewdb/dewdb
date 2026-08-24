//! Who is allowed to hold a key, decided from the node's own view of the ring.
//!
//! Routing alone cannot make a handover safe. Views converge rather than switch together, so during
//! a ring change two routers can disagree about a key's owner for as long as propagation takes. The
//! shard refusing keys it does not own is what makes that window harmless: the losing router gets
//! told where to go instead of writing a second copy.

use crate::cluster::metadata::{ClusterMetadata, MigrationPhase};
use crate::ring::{hash_key, RingShard};
use crate::util::same_endpoint;

#[derive(Debug, PartialEq, Eq)]
pub enum Ownership {
    /// This node's group owns the key, or the cluster has no ring and every node owns everything.
    Ours,
    /// Another group owns it. The URL is where the caller should have gone.
    Elsewhere(String),
    /// Ours today, but a migration in flight hands it to `to`. Writes wait rather than land on a
    /// node that is about to stop being the owner.
    Moving { to: String },
}

/// A ring entry belongs to a whole group, not one process: after a failover the node answering is
/// a replica URL, and it owns exactly what the entry it belongs to owns.
fn entry_for<'a>(shards: &'a [RingShard], node_url: &str) -> Option<&'a RingShard> {
    shards.iter().find(|s| {
        same_endpoint(&s.node_url, node_url)
            || s.replica_urls.iter().any(|r| same_endpoint(r, node_url))
    })
}

/// `None` only when there is no ring at all, which is a single shard or a range-based cluster and
/// must not be broken by a check meant for sharded ones.
///
/// A node that is *outside* an existing ring owns nothing, so every key is elsewhere. That case is
/// not theoretical: a shard removed by a handover keeps its data until cleanup, and a router that
/// has not adopted the new ring yet still sends keys to it. Returning `None` there answered those
/// requests with a 404 -- a wrong answer, where a redirect sends the caller to the real owner.
pub fn classify(view: &ClusterMetadata, own_url: &str, collection: &str, key: &str) -> Option<Ownership> {
    let ring = view.ring.as_ref()?;
    let hash = hash_key(collection, key);
    let owner = ring.build().owner(hash)?.node_url.clone();

    let mine = match entry_for(&ring.shards, own_url) {
        Some(entry) => entry,
        None => return Some(Ownership::Elsewhere(owner)),
    };

    if !same_endpoint(&owner, &mine.node_url) {
        return Some(Ownership::Elsewhere(owner));
    }

    // The final pass requires a stable source before ownership flips.
    if let Some(migration) = view.migration.as_ref()
        .filter(|m| m.phase == MigrationPhase::Finalizing)
    {
        if let Some(next) = migration.target.build().owner(hash) {
            if !same_endpoint(&next.node_url, &mine.node_url) {
                return Some(Ownership::Moving { to: next.node_url.clone() });
            }
        }
    }

    Some(Ownership::Ours)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::metadata::Migration;
    use crate::ring::HashRing;

    fn ring(entries: &[(&str, &[&str])]) -> HashRing {
        HashRing {
            vnodes: 128,
            shards: entries.iter().map(|(url, replicas)| RingShard {
                node_url: url.to_string(),
                replica_urls: replicas.iter().map(|r| r.to_string()).collect(),
            }).collect(),
        }
    }

    fn view(r: Option<HashRing>, migration: Option<HashRing>) -> ClusterMetadata {
        ClusterMetadata {
            version: 2,
            updated_by: "op".into(),
            seeded: false,
            members: Vec::new(),
            shards: Vec::new(),
            ring: r,
            migration: migration.map(|target| Migration {
                id: "m1".into(), target, started_by: "op".into(), phase: MigrationPhase::Copy,
            }),
        }
    }

    /// A key each of the two shards owns, so the cases below are about the rules and not about
    /// which side of the ring a particular string happens to land on.
    fn key_owned_by(r: &HashRing, url: &str) -> String {
        let built = r.build();
        (0..10_000)
            .map(|i| format!("k{}", i))
            .find(|k| built.owner(hash_key("t", k)).unwrap().node_url == url)
            .expect("the ring must give each shard some keys")
    }

    #[test]
    fn a_node_dropped_from_the_ring_redirects_instead_of_answering() {
        let two = ring(&[("http://a", &[]), ("http://b", &[])]);
        let v = view(Some(two.clone()), None);
        let key = key_owned_by(&two, "http://a");

        // The shape a shard has just after a handover removed it: still holding data, no longer an
        // owner. Answering from here would serve a stale value or a 404 to a router that has not
        // caught up; redirecting sends it to whoever owns the key now.
        assert_eq!(classify(&v, "http://departed", "t", &key),
            Some(Ownership::Elsewhere("http://a".into())));

        assert_eq!(classify(&view(None, None), "http://a", "t", "k1"), None,
            "a cluster with no ring has no ownership to enforce, and must not gain one");
    }

    #[test]
    fn keys_are_accepted_by_their_owner_and_refused_elsewhere() {
        let two = ring(&[("http://a", &[]), ("http://b", &[])]);
        let mine = key_owned_by(&two, "http://a");
        let theirs = key_owned_by(&two, "http://b");
        let v = view(Some(two), None);

        assert_eq!(classify(&v, "http://a", "t", &mine), Some(Ownership::Ours));
        assert_eq!(classify(&v, "http://a", "t", &theirs),
            Some(Ownership::Elsewhere("http://b".into())),
            "the refusal must name where the caller should have gone");
        assert_eq!(classify(&v, "http://b", "t", &theirs), Some(Ownership::Ours));
    }

    #[test]
    fn a_failed_over_replica_owns_what_its_group_owns() {
        let two = ring(&[("http://a", &["http://a2"]), ("http://b", &[])]);
        let mine = key_owned_by(&two, "http://a");
        let v = view(Some(two), None);

        assert_eq!(classify(&v, "http://a2", "t", &mine), Some(Ownership::Ours),
            "after a failover the node answering is a replica url; refusing there would take the \
             shard down for every key it legitimately owns");
    }

    #[test]
    fn a_key_on_its_way_out_is_neither_ours_nor_elsewhere() {
        let two = ring(&[("http://a", &[]), ("http://b", &[])]);
        let three = ring(&[("http://a", &[]), ("http://b", &[]), ("http://c", &[])]);

        // A key that a owns now and c owns after the move.
        let built_now = two.build();
        let built_next = three.build();
        let moving = (0..10_000)
            .map(|i| format!("k{}", i))
            .find(|k| {
                let h = hash_key("t", k);
                built_now.owner(h).unwrap().node_url == "http://a"
                    && built_next.owner(h).unwrap().node_url == "http://c"
            })
            .expect("adding a shard must take some of a's keys");

        let staying = (0..10_000)
            .map(|i| format!("k{}", i))
            .find(|k| {
                let h = hash_key("t", k);
                built_now.owner(h).unwrap().node_url == "http://a"
                    && built_next.owner(h).unwrap().node_url == "http://a"
            })
            .expect("a must keep most of its keys");

        let mut v = view(Some(two), Some(three));
        assert_eq!(classify(&v, "http://a", "t", &moving), Some(Ownership::Ours),
            "bulk copying must not freeze foreground writes");

        v.migration.as_mut().unwrap().phase = MigrationPhase::Finalizing;
        assert_eq!(classify(&v, "http://a", "t", &moving),
            Some(Ownership::Moving { to: "http://c".into() }),
            "finalization must freeze writes before the last copy");
        assert_eq!(classify(&v, "http://a", "t", &staying), Some(Ownership::Ours),
            "a migration must not stall writes to keys it does not touch");
    }
}
