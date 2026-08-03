//! Hash-ring ownership math and shard-map validation.

use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
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
