//! Quorum arithmetic over a configuration, and the log it travels in. The type itself lives in
//! `storage::frame`, next to the other log payloads.

use crate::consensus::election::majority;
use crate::storage::frame::Configuration;
use crate::util::same_endpoint;

/// The collection whose log carries configuration entries: one per shard group, replicated and
/// committed on the ordinary path. Reserved, so `_`-prefixed names are refused on the public API.
pub const CONFIG_LOG: &str = "_config";

pub fn is_system_collection(name: &str) -> bool {
    name.starts_with('_')
}

pub const MAX_COLLECTION_NAME_LEN: usize = 128;

/// Win32 resolves a device name in any directory and ignores the extension, so the stem decides.
fn is_windows_reserved(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name);
    ["con", "prn", "aux", "nul",
     "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
     "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"]
        .iter().any(|device| stem.eq_ignore_ascii_case(device))
}

/// Replicated identity and a directory name: lowercase-only, so no two valid names alias on a
/// case-insensitive filesystem; `.`-prefixed and `.tmp` / `.old` read as staging, not collections.
pub fn valid_collection_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_COLLECTION_NAME_LEN
        && !name.starts_with('.')
        && !name.ends_with('.')
        && !name.ends_with(".tmp")
        && !name.ends_with(".old")
        && !is_windows_reserved(name)
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-' | b'.'))
}

/// The physical nodes a voter slice names, first spelling of each kept. Two entries naming one node
/// would both count toward a threshold the second raised, letting one node satisfy it alone.
fn distinct(voters: &[String]) -> impl Iterator<Item = &String> + '_ {
    voters.iter().enumerate()
        .filter(move |(i, v)| !voters[..*i].iter().any(|w| same_endpoint(w, v.as_str())))
        .map(|(_, v)| v)
}

/// Physical nodes in one half. What `majority` is taken of, and what a client is told a write needs.
pub fn voter_count(voters: &[String]) -> usize {
    distinct(voters).count()
}

/// Majority of one half, by endpoint. `held` answers for a member this node has evidence about.
fn half_quorum_lsn(voters: &[String], held: &impl Fn(&str) -> u64) -> u64 {
    let mut lsns: Vec<u64> = distinct(voters).map(|v| held(v)).collect();
    if lsns.is_empty() {
        return 0;
    }
    lsns.sort_unstable_by(|a, b| b.cmp(a));
    lsns[majority(lsns.len()) - 1]
}

fn half_has_quorum(voters: &[String], granted: &[String]) -> bool {
    let total = voter_count(voters);
    if total == 0 {
        return true;
    }
    let count = distinct(voters)
        .filter(|v| granted.iter().any(|g| same_endpoint(g, v)))
        .count();
    count >= majority(total)
}

/// One entry per physical node, first spelling kept.
fn canonical(voters: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(voters.len());
    for url in voters {
        if !out.iter().any(|u| same_endpoint(u, &url)) {
            out.push(url);
        }
    }
    out
}

impl Configuration {
    pub fn simple(voters: Vec<String>) -> Self {
        Self { voters: canonical(voters), outgoing: None }
    }

    /// The transitional entry. Both halves decide together until it commits and `simple` replaces it.
    pub fn joint(outgoing: Vec<String>, voters: Vec<String>) -> Self {
        Self { voters: canonical(voters), outgoing: Some(canonical(outgoing)) }
    }

    /// A configuration decoded from a log frame or a snapshot. Deduped on the way in, because an
    /// entry appended before the duplicate rule existed is a stored fact and cannot be refused.
    pub fn canonicalized(self) -> Self {
        match self.outgoing {
            Some(outgoing) => Self::joint(outgoing, self.voters),
            None => Self::simple(self.voters),
        }
    }

    pub fn is_joint(&self) -> bool {
        self.outgoing.is_some()
    }

    /// What this change is heading for: the entry a leader appends once the joint one commits.
    pub fn target(&self) -> Self {
        Self::simple(self.voters.clone())
    }

    /// Everyone in either half. Who is asked for votes and who is shipped frames — a member of the
    /// outgoing half still decides, so it must still be reachable and still be sent entries.
    pub fn members(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(self.voters.len());
        for url in self.voters.iter().chain(self.outgoing.iter().flatten()) {
            if !out.iter().any(|u| same_endpoint(u, url)) {
                out.push(url.clone());
            }
        }
        out
    }

    pub fn contains(&self, url: &str) -> bool {
        self.members().iter().any(|m| same_endpoint(m, url))
    }

    /// A majority of *each* half. Counting the union instead is the split-brain this whole entry
    /// exists to close: five of the seven in the union can be four old plus one new.
    pub fn has_quorum(&self, granted: &[String]) -> bool {
        half_has_quorum(&self.voters, granted)
            && self.outgoing.as_ref().map_or(true, |old| half_has_quorum(old, granted))
    }

    /// Highest LSN a quorum holds. The lower of the two halves' medians while joint, since an
    /// entry only one half holds is one the other half's next leader can still overwrite.
    pub fn quorum_lsn(&self, held: impl Fn(&str) -> u64) -> u64 {
        let new = half_quorum_lsn(&self.voters, &held);
        match &self.outgoing {
            Some(old) => new.min(half_quorum_lsn(old, &held)),
            None => new,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| format!("http://{}", n)).collect()
    }

    #[test]
    fn a_simple_configuration_is_a_plain_majority() {
        let c = Configuration::simple(urls(&["a", "b", "c"]));

        assert!(!c.is_joint());
        assert!(!c.has_quorum(&urls(&["a"])), "one of three is not a majority");
        assert!(c.has_quorum(&urls(&["a", "b"])));
        assert!(c.has_quorum(&urls(&["a", "b", "c"])));
    }

    #[test]
    fn a_joint_configuration_needs_a_majority_of_each_half() {
        let c = Configuration::joint(urls(&["a", "b", "c"]), urls(&["c", "d", "e"]));

        assert!(c.is_joint());
        assert!(!c.has_quorum(&urls(&["a", "b"])), "a majority of the old half alone decides nothing");
        assert!(!c.has_quorum(&urls(&["d", "e"])), "nor a majority of the new half alone");
        assert!(c.has_quorum(&urls(&["a", "b", "d", "e"])));
        assert!(c.has_quorum(&urls(&["a", "c", "d"])), "c counts in both halves at once");
    }

    #[test]
    fn the_union_is_not_the_quorum() {
        let c = Configuration::joint(urls(&["a", "b", "c"]), urls(&["d", "e", "f"]));

        // Four of the six members, and still not a decision: it is three old plus one new.
        assert_eq!(c.members().len(), 6);
        assert!(!c.has_quorum(&urls(&["a", "b", "c", "d"])),
            "counting the union is exactly the split brain the joint entry exists to close");
    }

    #[test]
    fn members_spans_both_halves_without_duplicating_the_overlap() {
        let c = Configuration::joint(urls(&["a", "b"]), urls(&["b", "c"]));

        assert_eq!(c.members(), urls(&["b", "c", "a"]));
        assert!(c.contains("b"), "identity is the endpoint, so an unschemed url still resolves");
        assert!(!c.contains("http://d"));
    }

    #[test]
    fn the_quorum_lsn_is_the_lower_of_the_two_medians() {
        let c = Configuration::joint(urls(&["a", "b", "c"]), urls(&["c", "d", "e"]));

        // Old half holds 9 at a majority; the new half has only c there.
        let held = |url: &str| match crate::util::endpoint_of(url) {
            "a" => 9, "b" => 9, "c" => 9, "d" => 0, "e" => 0,
            _ => 0,
        };
        assert_eq!(c.quorum_lsn(held), 0,
            "an entry the incoming half does not hold is one its next leader can overwrite");

        let caught_up = |url: &str| match crate::util::endpoint_of(url) {
            "a" => 9, "b" => 9, "c" => 9, "d" => 9, "e" => 0,
            _ => 0,
        };
        assert_eq!(c.quorum_lsn(caught_up), 9);
    }

    #[test]
    fn a_lone_voter_commits_on_its_own() {
        let c = Configuration::simple(urls(&["a"]));
        assert_eq!(c.quorum_lsn(|_| 7), 7);
        assert!(c.has_quorum(&urls(&["a"])));
    }

    #[test]
    fn an_empty_half_decides_nothing_and_blocks_nothing() {
        // Reachable only from a hand-written view; the arithmetic must not read as "always short".
        let c = Configuration::simple(Vec::new());
        assert!(c.has_quorum(&[]));
        assert_eq!(c.quorum_lsn(|_| 5), 0);
    }

    #[test]
    fn the_target_of_a_joint_entry_is_its_incoming_half() {
        let c = Configuration::joint(urls(&["a", "b", "c"]), urls(&["b", "c", "d"]));
        assert_eq!(c.target(), Configuration::simple(urls(&["b", "c", "d"])));
        assert!(!c.target().is_joint());
    }

    /// A stored entry, so the constructors are bypassed the way a decoded frame bypasses them.
    fn stored(outgoing: Option<&[&str]>, voters: &[&str]) -> Configuration {
        Configuration { voters: urls(voters), outgoing: outgoing.map(urls) }
    }

    #[test]
    fn ib043_a_duplicated_voter_does_not_inflate_a_majority() {
        let c = stored(None, &["a", "a", "d"]);

        assert!(!c.has_quorum(&urls(&["a"])),
            "two entries naming one node let it reach the threshold its own duplicate raised");
        assert!(c.has_quorum(&urls(&["a", "d"])), "both physical nodes is the majority of two");
        assert_eq!(c.quorum_lsn(|url| if crate::util::endpoint_of(url) == "a" { 9 } else { 0 }), 0,
            "a duplicated voter's lsn entered the median twice");

        let aliased = Configuration {
            voters: vec!["http://A:1".into(), "http://a:1/".into(), "http://d:1".into()],
            outgoing: None,
        };
        assert!(!aliased.has_quorum(&["http://a:1".to_string()]),
            "identity is the endpoint, so two spellings are one voter");
    }

    #[test]
    fn ib043_a_duplicate_in_either_half_of_a_stored_joint_entry_is_one_voter() {
        let c = stored(Some(&["a", "a", "b"]), &["c", "c", "d"]);

        assert!(!c.has_quorum(&urls(&["a", "c"])), "neither half is decided by one node of two");
        assert!(c.has_quorum(&urls(&["a", "b", "c", "d"])));
    }

    #[test]
    fn ib043_constructed_and_decoded_configurations_name_each_node_once() {
        assert_eq!(Configuration::simple(urls(&["a", "a", "d"])).voters, urls(&["a", "d"]));

        let joint = Configuration::joint(urls(&["a", "a", "b"]), urls(&["b", "b"]));
        assert_eq!(joint.voters, urls(&["b"]));
        assert_eq!(joint.outgoing, Some(urls(&["a", "b"])));
        assert_eq!(joint.target(), Configuration::simple(urls(&["b"])));
        assert_eq!(voter_count(&urls(&["a", "a", "d"])), 2);

        let decoded = stored(Some(&["a", "a", "b"]), &["c", "c"]).canonicalized();
        assert_eq!(decoded, Configuration::joint(urls(&["a", "b"]), urls(&["c"])));
        assert_eq!(decoded.clone().canonicalized(), decoded, "canonicalising is idempotent");
    }

    #[test]
    fn system_collections_are_the_underscore_prefix() {
        assert!(is_system_collection(CONFIG_LOG));
        assert!(!is_system_collection("users"));
        assert!(!is_system_collection("my_collection"));
    }

    #[test]
    fn a_valid_name_is_one_ordinary_path_component() {
        for name in ["users", "my_collection", "app.events", "a-b", CONFIG_LOG, "x"] {
            assert!(valid_collection_name(name), "{}", name);
        }
        // The reserved rule is separate: `_config` is a well-formed name that the API refuses.
        for name in ["", "..", "../x", "a/b", r"a\b", "a b", "a\0b", "caf\u{e9}",
                     ".hidden", "t.tmp", "t.old", "a:b"] {
            assert!(!valid_collection_name(name), "{:?}", name);
        }
        assert!(valid_collection_name(&"a".repeat(MAX_COLLECTION_NAME_LEN)));
        assert!(!valid_collection_name(&"a".repeat(MAX_COLLECTION_NAME_LEN + 1)));
    }

    #[test]
    fn ib009_canonical_names_reject_case_and_filesystem_aliases() {
        for name in [
            "Orders", "orders.", "orders..", "users_A", "CON", "con", "nul", "aux", "prn",
            "com1", "lpt1", "nul.events", "con.log",
        ] {
            assert!(!valid_collection_name(name), "alias {:?} must be rejected", name);
        }
        for name in ["orders", "users_a", "app.events-1", "a-b", CONFIG_LOG] {
            assert!(valid_collection_name(name), "canonical {:?} should be accepted", name);
        }
    }
}
