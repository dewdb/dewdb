//! Two ownership models: explicit hash ranges, and a consistent-hash token ring.
//!
//! Ranges came first and are kept for clusters already running on them. The token ring is what
//! makes a topology change cost `1/n` of the keyspace instead of a hand-written re-partition.

use crate::util::endpoint_of;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

// Serialized as part of the cluster view, so field names here are a wire format between nodes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShardInfo {
    pub start_hash: u64,
    pub end_hash: u64,
    pub node_url: String,
    #[serde(default)]
    pub replica_urls: Vec<String>,
}

pub fn hash_key(col: &str, key: &str) -> u64 {
    xxhash_rust::xxh64::xxh64(format!("{}:{}", col, key).as_bytes(), 0)
}

pub const DEFAULT_VNODES: u32 = 128;
// Tokens cost 16 bytes each and are rebuilt on every topology change; this bounds both.
pub const MAX_VNODES: u32 = 4096;

fn default_vnodes() -> u32 { DEFAULT_VNODES }

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RingShard {
    pub node_url: String,
    #[serde(default)]
    pub replica_urls: Vec<String>,
}

/// Membership plus a vnode count. Tokens are *derived* from this, never stored: a stored token list
/// would be large, and two nodes could disagree about it while agreeing on the membership.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HashRing {
    #[serde(default = "default_vnodes")]
    pub vnodes: u32,
    pub shards: Vec<RingShard>,
}

/// Positions for one shard. Hashed from the endpoint rather than the raw URL so a trailing slash
/// or a scheme change does not silently move a node to a different part of the ring.
pub fn vnode_token(node_url: &str, index: u32) -> u64 {
    xxhash_rust::xxh64::xxh64(format!("{}#{}", endpoint_of(node_url), index).as_bytes(), 0)
}

impl HashRing {
    pub fn validate(&self) -> Result<(), String> {
        if self.shards.is_empty() {
            return Err("hash ring must contain at least one shard".to_string());
        }
        if self.vnodes == 0 {
            return Err("vnodes must be at least 1".to_string());
        }
        if self.vnodes > MAX_VNODES {
            return Err(format!("vnodes must be at most {}, got {}", MAX_VNODES, self.vnodes));
        }
        let mut seen = HashSet::new();
        for shard in &self.shards {
            if shard.node_url.trim().is_empty() {
                return Err("ring shard node_url must not be empty".to_string());
            }
            if !seen.insert(endpoint_of(&shard.node_url)) {
                return Err(format!("ring lists {} more than once", shard.node_url));
            }
        }
        let primaries = seen;
        let mut placed = HashSet::new();
        for shard in &self.shards {
            let primary = endpoint_of(&shard.node_url);
            let mut local = HashSet::new();
            for replica in &shard.replica_urls {
                if replica.trim().is_empty() {
                    return Err(format!("shard {} has an empty replica url", shard.node_url));
                }
                let endpoint = endpoint_of(replica);
                if endpoint == primary {
                    return Err(format!("shard {} lists itself as a replica", shard.node_url));
                }
                if primaries.contains(endpoint) {
                    return Err(format!("primary {} cannot also be a replica", replica));
                }
                if !local.insert(endpoint) {
                    return Err(format!(
                        "shard {} lists replica {} more than once",
                        shard.node_url, replica));
                }
                if !placed.insert(endpoint) {
                    return Err(format!("replica {} is assigned to more than one shard", replica));
                }
            }
        }
        Ok(())
    }

    pub fn build(&self) -> BuiltRing {
        let mut tokens: Vec<(u64, u32)> = Vec::with_capacity(self.shards.len() * self.vnodes as usize);
        for (index, shard) in self.shards.iter().enumerate() {
            for v in 0..self.vnodes {
                tokens.push((vnode_token(&shard.node_url, v), index as u32));
            }
        }
        // Ties broken by endpoint, not by input order: two nodes must agree on the owner even when
        // two tokens collide, and the shard list can arrive in any order.
        tokens.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| endpoint_of(&self.shards[a.1 as usize].node_url)
                    .cmp(endpoint_of(&self.shards[b.1 as usize].node_url)))
        });
        BuiltRing { tokens, shards: self.shards.clone() }
    }
}

/// The ring in lookup form. Built once per topology version, not per request.
pub struct BuiltRing {
    tokens: Vec<(u64, u32)>,
    shards: Vec<RingShard>,
}

impl BuiltRing {
    /// First token clockwise from `hash`, wrapping at the top of the ring.
    pub fn owner(&self, hash: u64) -> Option<&RingShard> {
        if self.tokens.is_empty() {
            return None;
        }
        let at = match self.tokens.binary_search_by(|(t, _)| t.cmp(&hash)) {
            Ok(i) => i,
            Err(i) if i < self.tokens.len() => i,
            Err(_) => 0,
        };
        self.shards.get(self.tokens[at].1 as usize)
    }

    pub fn shards(&self) -> &[RingShard] {
        &self.shards
    }

    fn owner_url(&self, hash: u64) -> &str {
        self.owner(hash).map_or("", |s| s.node_url.as_str())
    }
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Transfer {
    pub from: String,
    pub to: String,
    pub fraction: f64,
}

#[derive(Serialize, Debug, Clone)]
pub struct Movement {
    pub moved_fraction: f64,
    pub transfers: Vec<Transfer>,
}

/// Exactly how much of the keyspace changes hands between two rings.
///
/// Computed over arcs rather than by sampling keys: every token from either ring is a boundary, and
/// between two adjacent boundaries ownership is constant on both sides, so the arc widths sum to an
/// exact answer. Sampling would only ever approximate the number an operator is deciding on.
pub fn keyspace_movement(before: &BuiltRing, after: &BuiltRing) -> Movement {
    let mut boundaries: Vec<u64> = before.tokens.iter().map(|(t, _)| *t)
        .chain(after.tokens.iter().map(|(t, _)| *t))
        .collect();
    boundaries.sort_unstable();
    boundaries.dedup();

    if boundaries.is_empty() {
        return Movement { moved_fraction: 0.0, transfers: Vec::new() };
    }

    const RING: f64 = 18446744073709551616.0; // 2^64
    let mut moved = 0f64;
    let mut per_pair: BTreeMap<(String, String), f64> = BTreeMap::new();

    for i in 0..boundaries.len() {
        let start = boundaries[i];
        let end = boundaries[(i + 1) % boundaries.len()];
        // The final arc wraps past the top of the ring back to the first boundary.
        let width = if end > start {
            (end - start) as f64
        } else {
            (RING - start as f64) + end as f64
        };
        // Ownership is constant on (start, end], so probe just inside the arc.
        let probe = start.wrapping_add(1);
        let from = before.owner_url(probe);
        let to = after.owner_url(probe);
        if from != to {
            moved += width;
            *per_pair.entry((from.to_string(), to.to_string())).or_insert(0.0) += width;
        }
    }

    let mut transfers: Vec<Transfer> = per_pair.into_iter()
        .map(|((from, to), width)| Transfer { from, to, fraction: width / RING })
        .collect();
    transfers.sort_by(|a, b| b.fraction.partial_cmp(&a.fraction).unwrap_or(std::cmp::Ordering::Equal));

    Movement { moved_fraction: moved / RING, transfers }
}

pub fn shard_owns(shard: &ShardInfo, hash: u64) -> bool {
    if shard.start_hash == shard.end_hash {
        true
    } else if shard.start_hash < shard.end_hash {
        hash >= shard.start_hash && hash < shard.end_hash
    } else {
        hash >= shard.start_hash || hash < shard.end_hash
    }
}

const RING_SIZE: u128 = 1u128 << 64;

fn ring_segments(shard: &ShardInfo) -> Vec<(u128, u128)> {
    if shard.start_hash == shard.end_hash {
        vec![(0, RING_SIZE)]
    } else if shard.start_hash < shard.end_hash {
        vec![(shard.start_hash as u128, shard.end_hash as u128)]
    } else {
        vec![(shard.start_hash as u128, RING_SIZE), (0, shard.end_hash as u128)]
    }
}

pub fn validate_shard_ring(shards: &[ShardInfo]) -> Result<(), String> {
    if shards.is_empty() {
        return Err("Router requires at least one shard".to_string());
    }

    let full: Vec<&ShardInfo> = shards.iter().filter(|s| s.start_hash == s.end_hash).collect();
    if !full.is_empty() && shards.len() > 1 {
        return Err(format!(
            "Shard {} claims the whole ring (start_hash == end_hash) but {} other shard(s) are configured; ranges overlap",
            full[0].node_url, shards.len() - 1));
    }

    let mut segments: Vec<(u128, u128, &str)> = Vec::new();
    for shard in shards {
        for (lo, hi) in ring_segments(shard) {
            if lo < hi {
                segments.push((lo, hi, shard.node_url.as_str()));
            }
        }
    }
    segments.sort_by_key(|(lo, _, _)| *lo);

    let mut cursor: u128 = 0;
    let mut previous_owner = "";
    for (lo, hi, owner) in &segments {
        if *lo < cursor {
            return Err(format!(
                "Shard ranges overlap: {} covers [{}, {}) which re-enters territory already owned by {}",
                owner, lo, hi, previous_owner));
        }
        if *lo > cursor {
            return Err(format!(
                "Shard map has an uncovered hash range [{}, {}) before {}", cursor, lo, owner));
        }
        cursor = *hi;
        previous_owner = owner;
    }

    if cursor != RING_SIZE {
        return Err(format!("Shard map has an uncovered hash range [{}, {})", cursor, RING_SIZE));
    }

    Ok(())
}

#[cfg(test)]
mod consistent_hashing_tests {
    use super::*;

    fn ring(urls: &[&str]) -> HashRing {
        HashRing {
            vnodes: DEFAULT_VNODES,
            shards: urls.iter().map(|u| RingShard {
                node_url: u.to_string(), replica_urls: Vec::new(),
            }).collect(),
        }
    }

    fn sample_keys(n: usize) -> Vec<u64> {
        (0..n).map(|i| hash_key("docs", &format!("key-{}", i))).collect()
    }

    fn owners(r: &BuiltRing, keys: &[u64]) -> Vec<String> {
        keys.iter().map(|k| r.owner(*k).unwrap().node_url.clone()).collect()
    }

    #[test]
    fn every_key_has_exactly_one_owner_and_the_same_one_every_time() {
        let built = ring(&["http://a", "http://b", "http://c"]).build();
        for key in sample_keys(500) {
            let first = built.owner(key).unwrap().node_url.clone();
            assert_eq!(built.owner(key).unwrap().node_url, first, "lookup must be pure");
        }
        assert!(built.owner(0).is_some(), "hash 0 falls before the first token and wraps");
        assert!(built.owner(u64::MAX).is_some(), "hash at the top of the ring wraps to the start");
    }

    #[test]
    fn two_nodes_build_the_same_ring_from_the_same_membership() {
        let a = ring(&["http://a", "http://b", "http://c"]).build();
        // Same shards, different order, and one written with a trailing slash.
        let b = HashRing {
            vnodes: DEFAULT_VNODES,
            shards: vec![
                RingShard { node_url: "http://c".into(), replica_urls: vec![] },
                RingShard { node_url: "http://a/".into(), replica_urls: vec![] },
                RingShard { node_url: "http://b".into(), replica_urls: vec![] },
            ],
        }.build();

        let keys = sample_keys(2000);
        let left = owners(&a, &keys);
        let right: Vec<String> = owners(&b, &keys).into_iter()
            .map(|u| u.trim_end_matches('/').to_string())
            .collect();
        assert_eq!(left, right,
            "two nodes disagreeing about ownership would send the same key to different shards");
    }

    /// The property consistent hashing exists for. Modulo hashing over 3 -> 4 shards moves ~75% of
    /// keys and shuffles them between every pair of nodes; this must move ~1/4, all of it inbound.
    #[test]
    fn adding_a_shard_moves_only_its_own_share_and_only_to_it() {
        let before = ring(&["http://a", "http://b", "http://c"]).build();
        let after = ring(&["http://a", "http://b", "http://c", "http://d"]).build();

        let keys = sample_keys(20_000);
        let old = owners(&before, &keys);
        let new = owners(&after, &keys);

        let mut moved = 0usize;
        for (was, now) in old.iter().zip(new.iter()) {
            if was == now {
                continue;
            }
            moved += 1;
            assert_eq!(now, "http://d",
                "a key moved from {} to {}; consistent hashing must only ever move keys onto the \
                 node being added, never reshuffle between existing ones", was, now);
        }

        // A node's share has roughly 1/sqrt(vnodes) relative spread, so at 128 tokens the fourth
        // node lands anywhere near a quarter rather than on it. The bound is wide on purpose: the
        // structural assertion above is the property, this only rules out a wholesale reshuffle.
        let fraction = moved as f64 / keys.len() as f64;
        assert!((0.10..0.40).contains(&fraction),
            "expected roughly a quarter of the keyspace to move, got {:.1}%", fraction * 100.0);

        // The arc-based calculation must agree with what the keys actually did.
        let movement = keyspace_movement(&before, &after);
        assert!((movement.moved_fraction - fraction).abs() < 0.03,
            "exact movement {:.4} disagrees with sampled {:.4}", movement.moved_fraction, fraction);
        assert!(movement.transfers.iter().all(|t| t.to == "http://d"),
            "every transfer must be inbound to the new node: {:?}", movement.transfers);
    }

    /// More tokens, tighter spread. Confirms the quarter is real and that any single measurement
    /// off it is sampling noise in where one node's tokens fell, not a bias in the derivation.
    #[test]
    fn the_share_converges_on_one_over_n_as_vnodes_rise() {
        let measure = |vnodes: u32| {
            let mk = |urls: &[&str]| HashRing {
                vnodes,
                shards: urls.iter().map(|u| RingShard {
                    node_url: u.to_string(), replica_urls: vec![],
                }).collect(),
            }.build();
            let before = mk(&["http://a", "http://b", "http://c"]);
            let after = mk(&["http://a", "http://b", "http://c", "http://d"]);
            keyspace_movement(&before, &after).moved_fraction
        };

        let coarse = (measure(32) - 0.25f64).abs();
        let fine = (measure(2048) - 0.25f64).abs();
        assert!(fine < 0.02, "2048 tokens should land within 2 points of a quarter, off by {:.3}", fine);
        assert!(fine < coarse.max(0.02),
            "more tokens must not be worse: 32 was off by {:.3}, 2048 by {:.3}", coarse, fine);
    }

    #[test]
    fn removing_a_shard_moves_only_its_keys_and_spreads_them() {
        let before = ring(&["http://a", "http://b", "http://c"]).build();
        let after = ring(&["http://a", "http://b"]).build();

        let keys = sample_keys(20_000);
        let old = owners(&before, &keys);
        let new = owners(&after, &keys);

        for (was, now) in old.iter().zip(new.iter()) {
            if was != now {
                assert_eq!(was, "http://c",
                    "only the departing node's keys may move, but one moved off {}", was);
            }
        }

        let movement = keyspace_movement(&before, &after);
        assert!(movement.transfers.iter().all(|t| t.from == "http://c"));
        let destinations: std::collections::HashSet<&str> =
            movement.transfers.iter().map(|t| t.to.as_str()).collect();
        assert_eq!(destinations.len(), 2,
            "the departing node's share must spread across both survivors, not land on one");
    }

    #[test]
    fn vnodes_are_what_keeps_the_distribution_even() {
        let keys = sample_keys(30_000);

        let spread = |vnodes: u32| {
            let built = HashRing {
                vnodes,
                shards: ["http://a", "http://b", "http://c"].iter()
                    .map(|u| RingShard { node_url: u.to_string(), replica_urls: vec![] }).collect(),
            }.build();
            let mut counts: BTreeMap<String, usize> = BTreeMap::new();
            for owner in owners(&built, &keys) {
                *counts.entry(owner).or_insert(0) += 1;
            }
            let max = *counts.values().max().unwrap() as f64;
            let min = *counts.values().min().unwrap() as f64;
            (max / min, counts)
        };

        let (ratio, counts) = spread(DEFAULT_VNODES);
        assert!(ratio < 1.35,
            "128 vnodes should keep the shards within a third of each other, got {:.2}: {:?}",
            ratio, counts);
        assert_eq!(counts.len(), 3, "every shard must own something");

        // One token each is the degenerate ring: three arbitrary points on a 64-bit circle.
        let (single, _) = spread(1);
        assert!(single > ratio,
            "vnodes must actually be doing the smoothing; 1 token gave {:.2} vs {:.2}", single, ratio);
    }

    #[test]
    fn a_ring_that_cannot_route_is_refused() {
        assert!(ring(&[]).validate().is_err());
        assert!(ring(&["http://a"]).validate().is_ok(), "one shard owning everything is valid");

        let mut zero = ring(&["http://a"]);
        zero.vnodes = 0;
        assert!(zero.validate().is_err(), "no tokens means no owner for any key");

        let mut huge = ring(&["http://a"]);
        huge.vnodes = MAX_VNODES + 1;
        assert!(huge.validate().is_err());

        let dupes = ring(&["http://a", "http://a/"]);
        assert!(dupes.validate().unwrap_err().contains("more than once"),
            "the same endpoint twice would double that node's share of the ring");
    }

    #[test]
    fn replica_placement_rejects_collocation_and_reuse() {
        let placed = |a: &[&str], b: &[&str]| HashRing {
            vnodes: 16,
            shards: vec![
                RingShard {
                    node_url: "http://a".into(),
                    replica_urls: a.iter().map(|url| url.to_string()).collect(),
                },
                RingShard {
                    node_url: "http://b".into(),
                    replica_urls: b.iter().map(|url| url.to_string()).collect(),
                },
            ],
        };

        assert!(placed(&["http://a"], &[]).validate().unwrap_err().contains("itself"));
        assert!(placed(&["http://b"], &[]).validate().unwrap_err().contains("primary"));
        assert!(placed(&["http://r", "http://r/"], &[])
            .validate().unwrap_err().contains("more than once"));
        assert!(placed(&["http://r"], &["https://r/"])
            .validate().unwrap_err().contains("more than one shard"));
        assert!(placed(&["http://r1"], &["http://r2"]).validate().is_ok());
    }

    #[test]
    fn movement_against_an_unchanged_ring_is_zero() {
        let a = ring(&["http://a", "http://b"]).build();
        let b = ring(&["http://b", "http://a"]).build();
        let movement = keyspace_movement(&a, &b);
        assert_eq!(movement.moved_fraction, 0.0,
            "reordering the shard list must not be read as a topology change");
        assert!(movement.transfers.is_empty());
    }

    #[test]
    fn changing_the_vnode_count_is_reported_as_the_large_change_it_is() {
        let before = ring(&["http://a", "http://b", "http://c"]).build();
        let mut retuned = ring(&["http://a", "http://b", "http://c"]);
        retuned.vnodes = 32;
        let movement = keyspace_movement(&before, &retuned.build());

        assert!(movement.moved_fraction > 0.3,
            "re-deriving every token reshuffles far more than adding a node; operators must see \
             that in the number, got {:.1}%", movement.moved_fraction * 100.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HALF: u64 = 9223372036854775808;

    fn sh(start: u64, end: u64, url: &str, replicas: &[&str]) -> ShardInfo {
        ShardInfo {
            start_hash: start,
            end_hash: end,
            node_url: url.to_string(),
            replica_urls: replicas.iter().map(|r| r.to_string()).collect(),
        }
    }

    #[test]
    fn a_single_shard_owning_the_whole_ring_validates_and_routes() {
        let one = vec![sh(0, 0, "http://a", &[])];
        assert!(validate_shard_ring(&one).is_ok(), "one shard with start == end owns the entire ring");

        assert!(shard_owns(&one[0], 0));
        assert!(shard_owns(&one[0], u64::MAX));
        assert!(shard_owns(&one[0], 123456789));
    }

    #[test]
    fn shard_ring_rejects_overlaps() {
        let overlap = vec![
            sh(0, HALF + 100, "http://a", &[]),
            sh(HALF, 0, "http://b", &[]),
        ];
        let err = validate_shard_ring(&overlap).unwrap_err();
        assert!(err.contains("overlap"), "got: {}", err);

        let duplicated = vec![
            sh(0, HALF, "http://a", &[]),
            sh(0, HALF, "http://b", &[]),
            sh(HALF, 0, "http://c", &[]),
        ];
        assert!(validate_shard_ring(&duplicated).unwrap_err().contains("overlap"));

        let two_full = vec![sh(0, 0, "http://a", &[]), sh(0, 0, "http://b", &[])];
        assert!(validate_shard_ring(&two_full).unwrap_err().contains("whole ring"));

        let full_plus_one = vec![sh(0, 0, "http://a", &[]), sh(0, HALF, "http://b", &[])];
        assert!(validate_shard_ring(&full_plus_one).unwrap_err().contains("whole ring"));
    }

    #[test]
    fn shard_ring_rejects_gaps() {
        let gap_in_middle = vec![
            sh(0, 100, "http://a", &[]),
            sh(200, 0, "http://b", &[]),
        ];
        assert!(validate_shard_ring(&gap_in_middle).unwrap_err().contains("uncovered"));

        let gap_at_start = vec![sh(100, 0, "http://a", &[])];
        assert!(validate_shard_ring(&gap_at_start).unwrap_err().contains("uncovered"));

        let gap_at_end = vec![sh(0, HALF, "http://a", &[])];
        assert!(validate_shard_ring(&gap_at_end).unwrap_err().contains("uncovered"));

        assert!(validate_shard_ring(&[]).is_err());
    }

    #[test]
    fn shard_ring_accepts_a_correctly_covered_wrap_around_map() {
        let two = vec![
            sh(0, HALF, "http://a", &[]),
            sh(HALF, 0, "http://b", &[]),
        ];
        assert!(validate_shard_ring(&two).is_ok());

        let three = vec![
            sh(0, 1000, "http://a", &[]),
            sh(1000, HALF, "http://b", &[]),
            sh(HALF, 0, "http://c", &[]),
        ];
        assert!(validate_shard_ring(&three).is_ok(), "order in the file must not matter");

        let shuffled = vec![
            sh(HALF, 0, "http://c", &[]),
            sh(0, 1000, "http://a", &[]),
            sh(1000, HALF, "http://b", &[]),
        ];
        assert!(validate_shard_ring(&shuffled).is_ok());
    }

    #[test]
    fn every_hash_routes_to_exactly_one_shard() {
        let shards = vec![
            sh(0, 1000, "http://a", &[]),
            sh(1000, HALF, "http://b", &[]),
            sh(HALF, 0, "http://c", &[]),
        ];
        validate_shard_ring(&shards).unwrap();

        for hash in [0u64, 1, 999, 1000, 1001, HALF - 1, HALF, HALF + 1, u64::MAX] {
            let owners: Vec<&str> = shards.iter()
                .filter(|s| shard_owns(s, hash))
                .map(|s| s.node_url.as_str())
                .collect();
            assert_eq!(owners.len(), 1, "hash {} had owners {:?}", hash, owners);
        }
    }
}
