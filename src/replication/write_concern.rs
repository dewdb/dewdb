//! How many acknowledgements a write needs before the client hears success.

use crate::consensus::config::voter_count;
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

/// `Err` rather than a fallback, the way `parse_read_pref` already does it. `quorum` is the
/// spelling `?read=` uses for its strongest guarantee and so the obvious wrong guess here; silently
/// reading it as `w=1` answers `200` to a client that asked for durability and gives it no way to
/// find out it got none (M18). `w=0` stays `N(0)` and is floored at 1 by `required_acks` -- the
/// same answer, but by a route the client can reason about.
pub fn parse_write_concern(w: Option<&str>) -> Result<WriteConcern, String> {
    match w {
        None | Some("1") => Ok(WriteConcern::Local),
        Some("majority") => Ok(WriteConcern::Majority),
        Some("all") => Ok(WriteConcern::All),
        Some(s) => s.parse::<usize>().map(WriteConcern::N).map_err(|_| format!(
            "unknown write concern `{}`; use `majority`, `all` or a number", s)),
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
                let new = majority(voter_count(&config.voters));
                match &config.outgoing {
                    Some(old) => new.max(majority(voter_count(old))),
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

    fn parse_write_concern_ok(w: Option<&str>) -> WriteConcern {
        parse_write_concern(w).expect("valid write concern")
    }

    #[test]
    fn write_concern_resolves_required_acks() {
        assert_eq!(required_acks(&parse_write_concern_ok(None), 2), 1);
        assert_eq!(required_acks(&parse_write_concern_ok(Some("1")), 2), 1);

        assert_eq!(required_acks(&parse_write_concern_ok(Some("majority")), 2), 2);
        assert_eq!(required_acks(&parse_write_concern_ok(Some("majority")), 1), 2);
        assert_eq!(required_acks(&parse_write_concern_ok(Some("majority")), 4), 3);

        assert_eq!(required_acks(&parse_write_concern_ok(Some("all")), 2), 3);
        assert_eq!(required_acks(&parse_write_concern_ok(Some("all")), 0), 1);

        assert_eq!(required_acks(&parse_write_concern_ok(Some("3")), 2), 3);
        assert_eq!(required_acks(&parse_write_concern_ok(Some("9")), 2), 3, "N is capped at total node count");
        assert_eq!(required_acks(&parse_write_concern_ok(Some("0")), 2), 1, "N is floored at 1");

        assert!(parse_write_concern(Some("garbage")).is_err(),
            "an unparseable w must be refused, not read as w=1");
        for spelling in ["majorty", "quorum", "abc", "-1", ""] {
            assert!(parse_write_concern(Some(spelling)).is_err(),
                "`{}` silently asked for no replication at all", spelling);
        }
    }

    #[test]
    fn a_majority_write_needs_both_halves_while_joint() {
        let urls = |n: &[&str]| n.iter().map(|s| format!("http://{}", s)).collect::<Vec<_>>();
        let joint = Configuration::joint(urls(&["a", "b", "c"]), urls(&["c", "d", "e"]));
        let q = write_quorum(&parse_write_concern_ok(Some("majority")), &joint);

        assert!(!q.met(&urls(&["a", "b"])), "the whole outgoing majority is still not a decision");
        assert!(!q.met(&urls(&["a", "b", "d"])), "and one node of the incoming half does not add up");
        assert!(q.met(&urls(&["a", "b", "d", "e"])));
        assert!(q.met(&urls(&["a", "c", "d"])), "c is in both halves and counts in both");
        assert_eq!(q.required(), 2, "reported as the floor a joint quorum can be met at");

        let simple = Configuration::simple(urls(&["a", "b", "c"]));
        let q = write_quorum(&parse_write_concern_ok(Some("majority")), &simple);
        assert!(q.met(&urls(&["a", "b"])));
        assert!(!q.met(&urls(&["a"])));
    }

    #[test]
    fn a_counted_concern_is_unchanged_by_the_configuration_shape() {
        let urls = |n: &[&str]| n.iter().map(|s| format!("http://{}", s)).collect::<Vec<_>>();
        let config = Configuration::simple(urls(&["a", "b", "c"]));

        assert!(write_quorum(&parse_write_concern_ok(None), &config).met(&urls(&["a"])),
            "w=1 is the local write itself");
        assert_eq!(write_quorum(&parse_write_concern_ok(Some("all")), &config).required(), 3);
        assert_eq!(write_quorum(&parse_write_concern_ok(Some("2")), &config).required(), 2);
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
