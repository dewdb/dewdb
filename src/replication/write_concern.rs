//! How many acknowledgements a write needs before the client hears success.

use crate::consensus::election::majority;
use crate::storage::frame::Configuration;
use serde::Deserialize;

#[derive(Clone, Copy)]
pub enum WriteConcern {
    Local,
    Majority,
    All,
    N(usize),
}

pub fn parse_write_concern(w: Option<&str>) -> WriteConcern {
    match w {
        None | Some("1") => WriteConcern::Local,
        Some("majority") => WriteConcern::Majority,
        Some("all") => WriteConcern::All,
        Some(s) => s.parse::<usize>().map(WriteConcern::N).unwrap_or(WriteConcern::Local),
    }
}

// Counts the primary: a majority of three is 2 acks total.
pub fn required_acks(wc: &WriteConcern, replica_count: usize) -> usize {
    let total = 1 + replica_count;
    match wc {
        WriteConcern::Local => 1,
        WriteConcern::Majority => total / 2 + 1,
        WriteConcern::All => total,
        WriteConcern::N(n) => (*n).max(1).min(total),
    }
}

/// What a write waits for, resolved against the configuration in force. `Majority` is not a count
/// while a change is in flight: a majority of each half is needed, and any number of acks from one
/// half alone is not one.
#[derive(Clone)]
pub enum WriteQuorum {
    Count(usize),
    Majority(Configuration),
}

pub fn write_quorum(wc: &WriteConcern, config: &Configuration) -> WriteQuorum {
    match wc {
        WriteConcern::Majority => WriteQuorum::Majority(config.clone()),
        other => WriteQuorum::Count(required_acks(other, config.members().len().saturating_sub(1))),
    }
}

impl WriteQuorum {
    /// `holders` names this node plus every voter known to hold the frame.
    pub fn met(&self, holders: &[String]) -> bool {
        match self {
            Self::Count(n) => holders.len() >= *n,
            Self::Majority(config) => config.has_quorum(holders),
        }
    }

    /// For the client's benefit only. A floor while joint: a set that is a majority of both halves
    /// is at least this large, and may have to be larger.
    pub fn required(&self) -> usize {
        match self {
            Self::Count(n) => *n,
            Self::Majority(config) => {
                let new = majority(config.voters.len());
                match &config.outgoing {
                    Some(old) => new.max(majority(old.len())),
                    None => new,
                }
            },
        }
    }
}

#[derive(Deserialize)]
pub struct WriteConcernParams {
    pub w: Option<String>,
    pub wtimeout: Option<u64>,
}

pub const DEFAULT_WTIMEOUT_MS: u64 = 5000;

pub fn wc_query_string(p: &WriteConcernParams) -> String {
    let mut parts = Vec::new();
    if let Some(w) = &p.w {
        parts.push(format!("w={}", w));
    }
    if let Some(t) = p.wtimeout {
        parts.push(format!("wtimeout={}", t));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_concern_resolves_required_acks() {
        assert_eq!(required_acks(&parse_write_concern(None), 2), 1);
        assert_eq!(required_acks(&parse_write_concern(Some("1")), 2), 1);

        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 2), 2);
        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 1), 2);
        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 4), 3);

        assert_eq!(required_acks(&parse_write_concern(Some("all")), 2), 3);
        assert_eq!(required_acks(&parse_write_concern(Some("all")), 0), 1);

        assert_eq!(required_acks(&parse_write_concern(Some("3")), 2), 3);
        assert_eq!(required_acks(&parse_write_concern(Some("9")), 2), 3, "N is capped at total node count");
        assert_eq!(required_acks(&parse_write_concern(Some("0")), 2), 1, "N is floored at 1");

        assert_eq!(required_acks(&parse_write_concern(Some("garbage")), 2), 1, "unparseable w falls back to local");
    }

    #[test]
    fn a_majority_write_needs_both_halves_while_joint() {
        let urls = |n: &[&str]| n.iter().map(|s| format!("http://{}", s)).collect::<Vec<_>>();
        let joint = Configuration::joint(urls(&["a", "b", "c"]), urls(&["c", "d", "e"]));
        let q = write_quorum(&parse_write_concern(Some("majority")), &joint);

        assert!(!q.met(&urls(&["a", "b"])), "the whole outgoing majority is still not a decision");
        assert!(!q.met(&urls(&["a", "b", "d"])), "and one node of the incoming half does not add up");
        assert!(q.met(&urls(&["a", "b", "d", "e"])));
        assert!(q.met(&urls(&["a", "c", "d"])), "c is in both halves and counts in both");
        assert_eq!(q.required(), 2, "reported as the floor a joint quorum can be met at");

        let simple = Configuration::simple(urls(&["a", "b", "c"]));
        let q = write_quorum(&parse_write_concern(Some("majority")), &simple);
        assert!(q.met(&urls(&["a", "b"])));
        assert!(!q.met(&urls(&["a"])));
    }

    #[test]
    fn a_counted_concern_is_unchanged_by_the_configuration_shape() {
        let urls = |n: &[&str]| n.iter().map(|s| format!("http://{}", s)).collect::<Vec<_>>();
        let config = Configuration::simple(urls(&["a", "b", "c"]));

        assert!(write_quorum(&parse_write_concern(None), &config).met(&urls(&["a"])),
            "w=1 is the local write itself");
        assert_eq!(write_quorum(&parse_write_concern(Some("all")), &config).required(), 3);
        assert_eq!(write_quorum(&parse_write_concern(Some("2")), &config).required(), 2);
    }

    #[test]
    fn wc_query_string_roundtrips() {
        let p = WriteConcernParams { w: Some("majority".into()), wtimeout: Some(2000) };
        assert_eq!(wc_query_string(&p), "?w=majority&wtimeout=2000");

        let p2 = WriteConcernParams { w: None, wtimeout: None };
        assert_eq!(wc_query_string(&p2), "");

        let p3 = WriteConcernParams { w: Some("all".into()), wtimeout: None };
        assert_eq!(wc_query_string(&p3), "?w=all");
    }
}
