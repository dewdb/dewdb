//! Secondary indexes: definitions carried by the collection's own log, postings derived from the
//! committed index.
//!
//! Two halves with different durability. A definition is a `LogEntry::Index`, so it replicates,
//! survives a leader change and rides `applied.meta` past the compaction that retires its frame --
//! the same treatment `Config` and `Handover` get, for the same reason. The postings are derived
//! state, rebuilt by a background walk whenever a definition appears or a collection opens, so
//! nothing on disk can disagree with the documents it indexes.
//!
//! Maintenance is exact rather than best-effort: a query uses an index to *narrow* the keys it
//! reads and then re-applies the filter, so a false positive costs a read and a false negative is
//! a missing row. Every path that changes a key changes the postings with it.

use crate::json::{get_path_value, json_cmp};
use crate::query::Filter;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;

pub const MAX_INDEXES_PER_COLLECTION: usize = 8;
pub const MAX_INDEX_NAME_LEN: usize = 64;
pub const MAX_FIELD_PATH_LEN: usize = 256;
pub const MAX_FIELD_PATH_SEGMENTS: usize = 16;

/// A candidate set this small is used whatever the collection's size. Without it a filter matching
/// two of five documents would scan, which is correct but makes the planner's behaviour depend on
/// how much unrelated data happens to sit beside them.
const ALWAYS_WORTH_IT: usize = 64;

/// Keys read per lock acquisition while building. Same trade as `SCAN_CHUNK`: the documents are
/// read without the index lock and absorbed under it.
pub const BUILD_CHUNK: usize = 256;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub name: String,
    /// Dotted path into the document. A document without it is not indexed, which is sound
    /// because `matches_filter` cannot match a condition on a field a document does not have.
    pub field: String,
}

/// What a `LogEntry::Index` carries. A create naming an existing index replaces it, so replaying
/// the same log twice lands on the same definitions whatever order the API refused things in.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "index_op", rename_all = "lowercase")]
pub enum IndexChange {
    Create { spec: IndexSpec },
    Drop { name: String },
}

impl IndexChange {
    pub fn apply_to(&self, specs: &mut Vec<IndexSpec>) {
        match self {
            Self::Create { spec } => match specs.iter_mut().find(|s| s.name == spec.name) {
                Some(existing) => *existing = spec.clone(),
                None => specs.push(spec.clone()),
            },
            Self::Drop { name } => specs.retain(|s| s.name != *name),
        }
    }
}

pub fn valid_index_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_INDEX_NAME_LEN
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// One or more dotted segments of the same shape a document member has. Bounded here rather than
/// where it is split, because the path arrives from a client and is stored in every node's log.
pub fn valid_field_path(field: &str) -> bool {
    !field.is_empty()
        && field.len() <= MAX_FIELD_PATH_LEN
        && field.split('.').count() <= MAX_FIELD_PATH_SEGMENTS
        && field.split('.').all(|part| !part.is_empty() && !part.starts_with('$'))
}

/// A JSON value ordered by `json_cmp`, which is the order the query layer already compares in.
/// Equality is defined *as* that comparison so `Ord` and `Eq` cannot disagree, which a `BTreeMap`
/// key may not do.
#[derive(Clone, Debug)]
pub struct IndexKey(pub serde_json::Value);

impl PartialEq for IndexKey {
    fn eq(&self, other: &Self) -> bool {
        json_cmp(&self.0, &other.0) == std::cmp::Ordering::Equal
    }
}

impl Eq for IndexKey {}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        json_cmp(&self.0, &other.0)
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// The indexed values of `doc` under `specs`, computed where the document is still in hand. The
/// write path stages these with the frame so committing one costs no read.
pub fn index_values(specs: &[IndexSpec], doc: &serde_json::Value) -> Vec<(String, IndexKey)> {
    specs.iter()
        .filter_map(|s| get_path_value(doc, &s.field)
            .map(|v| (s.name.clone(), IndexKey(v.clone()))))
        .collect()
}

/// Whether this index answers queries yet. A build walks the committed index, so until it finishes
/// the postings are a subset and the planner must not select them.
enum BuildState {
    /// Keys a live write changed while the build was running. The build skips them: the write
    /// already filed the current value, and re-filing what the build read would resurrect the old.
    Building { touched: HashSet<String> },
    Ready,
}

struct SecondaryIndex {
    field: String,
    postings: BTreeMap<IndexKey, BTreeSet<String>>,
    /// The value each key is filed under, so removing its posting costs no document read.
    keys: HashMap<String, IndexKey>,
    state: BuildState,
}

impl SecondaryIndex {
    fn building(field: String) -> Self {
        Self {
            field,
            postings: BTreeMap::new(),
            keys: HashMap::new(),
            state: BuildState::Building { touched: HashSet::new() },
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self.state, BuildState::Ready)
    }

    fn note_touched(&mut self, key: &str) {
        if let BuildState::Building { touched } = &mut self.state {
            touched.insert(key.to_string());
        }
    }

    fn unfile(&mut self, key: &str) {
        if let Some(old) = self.keys.remove(key) {
            if let Some(holders) = self.postings.get_mut(&old) {
                holders.remove(key);
                if holders.is_empty() {
                    self.postings.remove(&old);
                }
            }
        }
    }

    fn file(&mut self, key: &str, value: IndexKey) {
        self.unfile(key);
        self.postings.entry(value.clone()).or_default().insert(key.to_string());
        self.keys.insert(key.to_string(), value);
    }
}

/// One index's answer to the list endpoint.
#[derive(Serialize, Debug, Clone)]
pub struct IndexStatus {
    pub name: String,
    pub field: String,
    /// `"ready"` or `"building"`. A building index is maintained but not selected by the planner.
    pub state: &'static str,
    pub documents: usize,
    pub values: usize,
}

/// The keys an index offers for a filter, and which index offered them.
pub struct Selection {
    pub index: String,
    pub field: String,
    pub keys: Vec<String>,
}

/// What one filter condition can be answered with. `$ne` produces neither: its complement is the
/// whole index, so the scan it would replace is the scan it would become.
enum Probe {
    Exact(Vec<IndexKey>),
    Range { lower: Bound<IndexKey>, upper: Bound<IndexKey> },
}

/// All bools sort below all numbers, and all strings above them, so a numeric comparison's
/// candidates are exactly the band between these two. Without it a `$gt` scans every string,
/// array and object in the index and discards them one document read at a time.
fn numeric_band() -> (Bound<IndexKey>, Bound<IndexKey>) {
    (
        Bound::Excluded(IndexKey(serde_json::Value::Bool(true))),
        Bound::Excluded(IndexKey(serde_json::Value::String(String::new()))),
    )
}

fn probe_for(cond: &serde_json::Value) -> Option<Probe> {
    let map = match cond.as_object() {
        Some(m) if m.keys().any(|k| k.starts_with('$')) => m,
        // A literal condition, object or scalar, is an equality test.
        _ => return Some(Probe::Exact(vec![IndexKey(cond.clone())])),
    };

    // Membership before range: a validated `$in` is a short list of exact lookups, which is at
    // least as selective as any band the same condition also names.
    if let Some(values) = map.get("$in").and_then(|v| v.as_array()) {
        return Some(Probe::Exact(values.iter().map(|v| IndexKey(v.clone())).collect()));
    }

    let (band_low, band_high) = numeric_band();
    let lower = match (map.get("$gt"), map.get("$gte")) {
        (Some(v), _) => Bound::Excluded(IndexKey(v.clone())),
        (None, Some(v)) => Bound::Included(IndexKey(v.clone())),
        (None, None) => band_low,
    };
    let upper = match (map.get("$lt"), map.get("$lte")) {
        (Some(v), _) => Bound::Excluded(IndexKey(v.clone())),
        (None, Some(v)) => Bound::Included(IndexKey(v.clone())),
        (None, None) => band_high,
    };
    // Only `$ne`, or an operator set that names no band: nothing here narrows anything.
    if !map.keys().any(|k| matches!(k.as_str(), "$gt" | "$gte" | "$lt" | "$lte")) {
        return None;
    }
    Some(Probe::Range { lower, upper })
}

/// `BTreeMap::range` panics on a reversed pair, and a filter is free to name one.
fn range_is_empty(lower: &Bound<IndexKey>, upper: &Bound<IndexKey>) -> bool {
    let (low, low_open) = match lower {
        Bound::Included(k) => (Some(k), false),
        Bound::Excluded(k) => (Some(k), true),
        Bound::Unbounded => (None, false),
    };
    let (high, high_open) = match upper {
        Bound::Included(k) => (Some(k), false),
        Bound::Excluded(k) => (Some(k), true),
        Bound::Unbounded => (None, false),
    };
    match (low, high) {
        (Some(a), Some(b)) => match a.cmp(b) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => low_open || high_open,
            std::cmp::Ordering::Less => false,
        },
        _ => false,
    }
}

#[derive(Default)]
pub struct Indexes {
    map: BTreeMap<String, SecondaryIndex>,
}

impl Indexes {
    /// Registers the committed definitions this collection opened with. Everything starts
    /// `Building`: the postings are derived, so a restart rebuilds them rather than trusting a
    /// file that no fsync ordering ties to the documents.
    pub fn seed(specs: &[IndexSpec]) -> Self {
        let mut map = BTreeMap::new();
        for spec in specs {
            map.insert(spec.name.clone(), SecondaryIndex::building(spec.field.clone()));
        }
        Self { map }
    }

    pub fn apply_change(&mut self, change: &IndexChange) {
        match change {
            // Replaces rather than keeps: a create on an existing name is a redefinition, and the
            // old postings answer for the old field.
            IndexChange::Create { spec } => {
                self.map.insert(spec.name.clone(), SecondaryIndex::building(spec.field.clone()));
            },
            IndexChange::Drop { name } => {
                self.map.remove(name);
            },
        }
    }

    /// A committed collection drop takes the definitions with the documents: the collection is
    /// gone as far as a client is concerned, and a schema outliving it would be invisible state.
    pub fn clear_all(&mut self) {
        self.map.clear();
    }

    /// `values` is what the write staged, so an index whose field the document lacks is absent
    /// from it -- and its previous posting for this key still has to go.
    pub fn put(&mut self, key: &str, values: &[(String, IndexKey)]) {
        for (name, index) in self.map.iter_mut() {
            index.note_touched(key);
            match values.iter().find(|(n, _)| n == name) {
                Some((_, value)) => index.file(key, value.clone()),
                None => index.unfile(key),
            }
        }
    }

    pub fn remove(&mut self, key: &str) {
        for index in self.map.values_mut() {
            index.note_touched(key);
            index.unfile(key);
        }
    }

    /// Empties the postings without forgetting the definitions: a released handle and a truncation
    /// both invalidate what was filed, not what the log says exists.
    pub fn clear_entries(&mut self) {
        let specs: Vec<(String, String)> = self.map.iter()
            .map(|(name, index)| (name.clone(), index.field.clone()))
            .collect();
        self.map = specs.into_iter()
            .map(|(name, field)| (name, SecondaryIndex::building(field)))
            .collect();
    }

    pub fn has_building(&self) -> bool {
        self.map.values().any(|i| !i.is_ready())
    }

    pub fn next_building(&self) -> Option<(String, String)> {
        self.map.iter()
            .find(|(_, i)| !i.is_ready())
            .map(|(name, i)| (name.clone(), i.field.clone()))
    }

    /// Files a chunk of the build's reads. Returns whether this index is still the one being
    /// built: a drop or a redefinition mid-walk abandons the run rather than filing into it.
    pub fn absorb_build(&mut self, name: &str, field: &str, rows: &[(String, serde_json::Value)]) -> bool {
        let index = match self.map.get_mut(name) {
            Some(i) if i.field == field => i,
            _ => return false,
        };
        let touched = match &index.state {
            BuildState::Building { touched } => touched.clone(),
            BuildState::Ready => return false,
        };
        for (key, doc) in rows {
            // A live write filed the current value while this chunk was being read; what the build
            // holds for this key is the value that write replaced.
            if touched.contains(key) {
                continue;
            }
            if let Some(value) = get_path_value(doc, field) {
                index.file(key, IndexKey(value.clone()));
            }
        }
        true
    }

    pub fn finish_build(&mut self, name: &str, field: &str) {
        if let Some(index) = self.map.get_mut(name) {
            if index.field == field && !index.is_ready() {
                index.state = BuildState::Ready;
            }
        }
    }

    pub fn status(&self) -> Vec<IndexStatus> {
        self.map.iter().map(|(name, index)| IndexStatus {
            name: name.clone(),
            field: index.field.clone(),
            state: if index.is_ready() { "ready" } else { "building" },
            documents: index.keys.len(),
            values: index.postings.len(),
        }).collect()
    }

    /// The narrowest single-index answer to `filter`, or `None` to scan. `total_docs` is what the
    /// choice is measured against: a candidate set that is most of the collection costs a sorted
    /// materialisation to save nothing.
    ///
    /// One index, never an intersection of several: the caller re-applies the whole filter to
    /// every candidate, so a second index would remove reads a first one already narrowed to a
    /// page's worth.
    pub fn select(&self, filter: &Filter, total_docs: usize) -> Option<Selection> {
        // `Filter::fields` is a `HashMap`, so the order it yields is not the same twice. Sorted,
        // because two plans of equal size must not be picked differently on two shards.
        let mut fields: Vec<(&String, &serde_json::Value)> = filter.fields.iter().collect();
        fields.sort_by(|a, b| a.0.cmp(b.0));

        let mut best: Option<(usize, &String, &SecondaryIndex, Probe)> = None;
        for (field, cond) in fields {
            for (name, index) in self.map.iter() {
                if !index.is_ready() || index.field != *field {
                    continue;
                }
                let probe = match probe_for(cond) {
                    Some(p) => p,
                    None => continue,
                };
                let count = self.count(index, &probe);
                if best.as_ref().map_or(true, |(seen, _, _, _)| count < *seen) {
                    best = Some((count, name, index, probe));
                }
            }
        }

        let (count, name, index, probe) = best?;
        if count > ALWAYS_WORTH_IT && count.saturating_mul(2) > total_docs {
            return None;
        }

        // Deduplicated and key-ordered: an unsorted page resumes by key, so the candidates have to
        // arrive in the order the scan they replace would have produced them in.
        let mut keys: BTreeSet<String> = BTreeSet::new();
        self.walk(index, &probe, |holders| keys.extend(holders.iter().cloned()));
        Some(Selection {
            index: name.clone(),
            field: index.field.clone(),
            keys: keys.into_iter().collect(),
        })
    }

    fn count(&self, index: &SecondaryIndex, probe: &Probe) -> usize {
        let mut total = 0usize;
        self.walk(index, probe, |holders| total += holders.len());
        total
    }

    fn walk<F: FnMut(&BTreeSet<String>)>(&self, index: &SecondaryIndex, probe: &Probe, mut visit: F) {
        match probe {
            Probe::Exact(values) => {
                let distinct: BTreeSet<&IndexKey> = values.iter().collect();
                for value in distinct {
                    if let Some(holders) = index.postings.get(value) {
                        visit(holders);
                    }
                }
            },
            Probe::Range { lower, upper } => {
                if range_is_empty(lower, upper) {
                    return;
                }
                let bounds = (as_ref_bound(lower), as_ref_bound(upper));
                for (_, holders) in index.postings.range(bounds) {
                    visit(holders);
                }
            },
        }
    }
}

fn as_ref_bound(bound: &Bound<IndexKey>) -> Bound<&IndexKey> {
    match bound {
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse_filter;
    use serde_json::json;

    fn spec(name: &str, field: &str) -> IndexSpec {
        IndexSpec { name: name.to_string(), field: field.to_string() }
    }

    /// Everything the planner does rests on the build having filed every document, so the tests
    /// below build the same way the collection does: seed, absorb, finish.
    fn built(field: &str, docs: &[(&str, serde_json::Value)]) -> Indexes {
        let mut idx = Indexes::seed(&[spec("i", field)]);
        let rows: Vec<(String, serde_json::Value)> = docs.iter()
            .map(|(k, v)| (k.to_string(), v.clone())).collect();
        assert!(idx.absorb_build("i", field, &rows));
        idx.finish_build("i", field);
        idx
    }

    fn selected(idx: &Indexes, filter: &str, total: usize) -> Option<Vec<String>> {
        idx.select(&parse_filter(filter).unwrap(), total).map(|s| s.keys)
    }

    #[test]
    fn an_equality_filter_selects_only_the_keys_holding_that_value() {
        let idx = built("age", &[
            ("a", json!({"age": 30})),
            ("b", json!({"age": 40})),
            ("c", json!({"age": 30})),
        ]);
        assert_eq!(selected(&idx, r#"{"age": 30}"#, 3), Some(vec!["a".to_string(), "c".to_string()]));
        assert_eq!(selected(&idx, r#"{"age": 99}"#, 3), Some(Vec::new()));
    }

    #[test]
    fn candidates_come_out_in_key_order_whatever_order_they_were_filed_in() {
        let idx = built("t", &[
            ("z", json!({"t": 1})), ("a", json!({"t": 1})), ("m", json!({"t": 1})),
        ]);
        assert_eq!(selected(&idx, r#"{"t": 1}"#, 3),
            Some(vec!["a".to_string(), "m".to_string(), "z".to_string()]),
            "an unsorted page resumes by key, so the candidates must arrive in that order");
    }

    #[test]
    fn a_range_filter_stays_inside_the_numeric_band() {
        let idx = built("n", &[
            ("a", json!({"n": 1})),
            ("b", json!({"n": 5})),
            ("c", json!({"n": 9})),
            ("s", json!({"n": "text"})),
            ("t", json!({"n": true})),
        ]);
        assert_eq!(selected(&idx, r#"{"n": {"$gt": 1}}"#, 5),
            Some(vec!["b".to_string(), "c".to_string()]),
            "strings sort above every number and can never satisfy a numeric comparison");
        assert_eq!(selected(&idx, r#"{"n": {"$gte": 5, "$lte": 9}}"#, 5),
            Some(vec!["b".to_string(), "c".to_string()]));
        assert_eq!(selected(&idx, r#"{"n": {"$lt": 5}}"#, 5), Some(vec!["a".to_string()]),
            "and booleans sort below every number");
    }

    /// IB-018 in shape: `BTreeMap::range` panics on a reversed pair, and a filter may name one.
    #[test]
    fn a_reversed_range_answers_nothing_instead_of_panicking() {
        let idx = built("n", &[("a", json!({"n": 1})), ("b", json!({"n": 9}))]);
        assert_eq!(selected(&idx, r#"{"n": {"$gt": 9, "$lt": 1}}"#, 2), Some(Vec::new()));
        assert_eq!(selected(&idx, r#"{"n": {"$gt": 5, "$lt": 5}}"#, 2), Some(Vec::new()),
            "an open pair at the same value is empty too");
    }

    #[test]
    fn in_unions_its_members_and_ne_selects_nothing() {
        let idx = built("tag", &[
            ("a", json!({"tag": "x"})), ("b", json!({"tag": "y"})), ("c", json!({"tag": "z"})),
        ]);
        assert_eq!(selected(&idx, r#"{"tag": {"$in": ["x", "z"]}}"#, 3),
            Some(vec!["a".to_string(), "c".to_string()]));
        assert!(selected(&idx, r#"{"tag": {"$ne": "x"}}"#, 3).is_none(),
            "the complement of an index is the scan it would replace");
    }

    #[test]
    fn a_document_without_the_field_is_not_indexed_and_cannot_match() {
        let idx = built("age", &[("a", json!({"age": 1})), ("b", json!({"other": 1}))]);
        assert_eq!(selected(&idx, r#"{"age": 1}"#, 2), Some(vec!["a".to_string()]));
        // Explicit null is a value, not an absence: `matches_filter` matches it, so it is indexed.
        let nulls = built("age", &[("a", json!({"age": null})), ("b", json!({"other": 1}))]);
        assert_eq!(selected(&nulls, r#"{"age": null}"#, 2), Some(vec!["a".to_string()]));
    }

    #[test]
    fn a_dotted_path_indexes_the_nested_member() {
        let idx = built("meta.tier", &[
            ("a", json!({"meta": {"tier": "gold"}})),
            ("b", json!({"meta": {"tier": "silver"}})),
            ("c", json!({"meta": 7})),
        ]);
        assert_eq!(selected(&idx, r#"{"meta.tier": "gold"}"#, 3), Some(vec!["a".to_string()]));
    }

    #[test]
    fn an_index_that_is_still_building_is_never_selected() {
        let mut idx = Indexes::seed(&[spec("i", "age")]);
        idx.put("a", &[("i".to_string(), IndexKey(json!(30)))]);
        assert!(selected(&idx, r#"{"age": 30}"#, 1).is_none(), "a partial index answers wrong");
        idx.finish_build("i", "age");
        assert_eq!(selected(&idx, r#"{"age": 30}"#, 1), Some(vec!["a".to_string()]),
            "and is maintained while it builds, so finishing needs no second pass");
    }

    /// The race the build's `touched` set exists for: a write lands between the build reading a
    /// document and filing it, and the value the build holds is the one the write replaced.
    #[test]
    fn a_write_during_a_build_is_not_overwritten_by_what_the_build_read() {
        let mut idx = Indexes::seed(&[spec("i", "age")]);
        // The build has read `a` as 30 but not yet absorbed it.
        let stale = vec![("a".to_string(), json!({"age": 30}))];
        idx.put("a", &[("i".to_string(), IndexKey(json!(40)))]);
        assert!(idx.absorb_build("i", "age", &stale));
        idx.finish_build("i", "age");

        assert_eq!(selected(&idx, r#"{"age": 40}"#, 1), Some(vec!["a".to_string()]));
        assert_eq!(selected(&idx, r#"{"age": 30}"#, 1), Some(Vec::new()),
            "unfixed the build resurrects the value the write replaced");
    }

    #[test]
    fn a_delete_during_a_build_stays_deleted() {
        let mut idx = Indexes::seed(&[spec("i", "age")]);
        let stale = vec![("a".to_string(), json!({"age": 30}))];
        idx.remove("a");
        assert!(idx.absorb_build("i", "age", &stale));
        idx.finish_build("i", "age");
        assert_eq!(selected(&idx, r#"{"age": 30}"#, 0), Some(Vec::new()));
    }

    #[test]
    fn a_build_abandoned_by_a_drop_files_nothing() {
        let mut idx = Indexes::seed(&[spec("i", "age")]);
        idx.apply_change(&IndexChange::Drop { name: "i".to_string() });
        assert!(!idx.absorb_build("i", "age", &[("a".to_string(), json!({"age": 1}))]));
        assert!(idx.status().is_empty());
    }

    #[test]
    fn a_redefinition_abandons_the_run_for_the_field_it_replaced() {
        let mut idx = Indexes::seed(&[spec("i", "age")]);
        idx.apply_change(&IndexChange::Create { spec: spec("i", "score") });
        assert!(!idx.absorb_build("i", "age", &[("a".to_string(), json!({"age": 1}))]),
            "the walk in flight was reading the old field");
        assert_eq!(idx.next_building(), Some(("i".to_string(), "score".to_string())));
    }

    #[test]
    fn replacing_a_value_leaves_no_posting_behind() {
        let mut idx = Indexes::seed(&[spec("i", "age")]);
        idx.finish_build("i", "age");
        idx.put("a", &[("i".to_string(), IndexKey(json!(30)))]);
        idx.put("a", &[("i".to_string(), IndexKey(json!(40)))]);
        assert_eq!(selected(&idx, r#"{"age": 30}"#, 1), Some(Vec::new()));
        assert_eq!(selected(&idx, r#"{"age": 40}"#, 1), Some(vec!["a".to_string()]));

        // The field going away is a write that files nothing, not a write that changes nothing.
        idx.put("a", &[]);
        assert_eq!(selected(&idx, r#"{"age": 40}"#, 1), Some(Vec::new()));
    }

    #[test]
    fn the_planner_prefers_the_narrower_of_two_eligible_indexes() {
        let mut idx = Indexes::seed(&[spec("wide", "w"), spec("narrow", "n")]);
        let rows: Vec<(String, serde_json::Value)> = (0..100)
            .map(|i| (format!("k{:03}", i), json!({"w": i % 2, "n": i})))
            .collect();
        assert!(idx.absorb_build("wide", "w", &rows));
        idx.finish_build("wide", "w");
        assert!(idx.absorb_build("narrow", "n", &rows));
        idx.finish_build("narrow", "n");

        let filter = parse_filter(r#"{"w": 0, "n": 42}"#).unwrap();
        let chosen = idx.select(&filter, 100).expect("both are eligible");
        assert_eq!(chosen.index, "narrow");
        assert_eq!(chosen.keys, vec!["k042".to_string()]);
    }

    #[test]
    fn a_candidate_set_that_is_most_of_the_collection_falls_back_to_a_scan() {
        let filled: Vec<(String, serde_json::Value)> = (0..200)
            .map(|i| (format!("k{:03}", i), json!({"w": 1})))
            .collect();
        let mut idx = Indexes::seed(&[spec("i", "w")]);
        assert!(idx.absorb_build("i", "w", &filled));
        idx.finish_build("i", "w");

        assert!(selected(&idx, r#"{"w": 1}"#, 200).is_none(),
            "materialising every key to save no reads is worse than the walk it replaces");
    }

    #[test]
    fn names_and_field_paths_are_bounded_where_they_enter_the_log() {
        assert!(valid_index_name("by_age"));
        assert!(valid_index_name("a-b1"));
        for bad in ["", "a.b", "a/b", "a b", "a\u{e9}"] {
            assert!(!valid_index_name(bad), "{:?}", bad);
        }
        assert!(!valid_index_name(&"a".repeat(MAX_INDEX_NAME_LEN + 1)));

        assert!(valid_field_path("age"));
        assert!(valid_field_path("meta.tier.rank"));
        for bad in ["", ".", "a.", ".a", "a..b", "$where"] {
            assert!(!valid_field_path(bad), "{:?}", bad);
        }
        assert!(!valid_field_path(&"a".repeat(MAX_FIELD_PATH_LEN + 1)));
        assert!(!valid_field_path(&vec!["a"; MAX_FIELD_PATH_SEGMENTS + 1].join(".")));
    }

    #[test]
    fn a_create_on_an_existing_name_replaces_it_so_a_replay_is_deterministic() {
        let mut specs = vec![spec("i", "age")];
        IndexChange::Create { spec: spec("i", "score") }.apply_to(&mut specs);
        assert_eq!(specs, vec![spec("i", "score")]);
        IndexChange::Drop { name: "i".to_string() }.apply_to(&mut specs);
        assert!(specs.is_empty());
        IndexChange::Drop { name: "gone".to_string() }.apply_to(&mut specs);
        assert!(specs.is_empty(), "dropping what is not there is not an error in a log");
    }

    /// `IndexKey` is a `BTreeMap` key, which may not have `Ord` and `Eq` disagreeing.
    #[test]
    fn index_key_equality_is_its_ordering() {
        let a = IndexKey(json!({"x": 1, "y": 2}));
        let b: IndexKey = IndexKey(serde_json::from_str(r#"{"y": 2, "x": 1}"#).unwrap());
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), std::cmp::Ordering::Equal);
        assert!(IndexKey(json!(1)) < IndexKey(json!("1")));
        assert!(IndexKey(json!(null)) < IndexKey(json!(false)));
    }
}
