//! Query semantics: filters, sort keys, cross-shard merge, pagination cursors.

use crate::json::{get_path_value, json_cmp};
use crate::model::MAX_QUERY_LIMIT;
use crate::util::base64_bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

/// Per-shard scan positions for an unsorted page. `Some(key)` resumes after that key, `None` is a
/// shard the page ahead of this one had no rows to spend on, and an absent shard has been drained.
///
/// `ring` is the partitioning these positions were taken against. An unsorted scan walks keyspaces
/// per shard, so a key moving between shards mid-scan puts it behind a position it was never
/// covered by; the fingerprint is what makes that visible instead of a silently short answer.
#[derive(Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ShardCursor {
    pub ring: u64,
    pub positions: BTreeMap<String, Option<String>>,
}

/// Where an unsorted scan stopped on one shard: a position in that shard's keyspace. It was a bare
/// key on the wire, which any string is a valid one of, so a sorted or router-issued cursor handed
/// to an unsorted query was read as a start key and answered `200` with rows after whatever that
/// string sorts as (L18). Encoded like the other two so the three are told apart rather than
/// guessed at; `deny_unknown_fields` on all of them is what makes that work.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyCursor {
    pub key: String,
}

/// Where a sorted scan stopped, as a position in the sort order rather than in the keyspace. One
/// of these covers the whole cluster: "after this row" is the same question on every shard, so a
/// sorted page needs no per-shard state and survives a shard joining mid-scan.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortCursor {
    pub value: serde_json::Value,
    pub key: String,
}

/// A document with the key it is stored under. Sorting and merging need the key: it is the
/// tiebreaker that makes the order total, and a cursor cannot resume against anything less.
#[derive(Clone)]
pub struct SortedRow {
    pub key: String,
    pub value: serde_json::Value,
}

pub fn encode_cursor<T: Serialize>(c: &T) -> String {
    let json = serde_json::to_vec(c).unwrap_or_default();
    base64_bytes::base64_encode_url(&json)
}

pub fn decode_cursor<T: DeserializeOwned>(s: &str) -> Option<T> {
    let bytes = base64_bytes::base64_decode(s).ok()?;
    serde_json::from_slice(&bytes).ok()
}

#[derive(Deserialize)]
pub struct Filter {
    #[serde(flatten)]
    pub fields: HashMap<String, serde_json::Value>,
}

pub struct SortSpec {
    pub field: String,
    pub desc: bool,
}

pub fn parse_sort(s: Option<&str>) -> Option<SortSpec> {
    let s = s?;
    let (field, dir) = match s.split_once(':') {
        Some((f, d)) => (f, d),
        None => (s, "asc"),
    };
    if field.is_empty() {
        return None;
    }
    Some(SortSpec { field: field.to_string(), desc: dir.eq_ignore_ascii_case("desc") })
}

/// The sort field, or `Null` where the document does not have it.
pub fn sort_value<'a>(doc: &'a serde_json::Value, sort: &SortSpec) -> &'a serde_json::Value {
    get_path_value(doc, &sort.field).unwrap_or(&serde_json::Value::Null)
}

/// Direction applies to the field only; the key tiebreaker always ascends. Both halves matter:
/// without the key two rows with the same field value tie, and a cursor cannot resume past a tie.
pub fn compare_positions(
    a: (&serde_json::Value, &str),
    b: (&serde_json::Value, &str),
    sort: &SortSpec,
) -> Ordering {
    let ord = json_cmp(a.0, b.0);
    let ord = if sort.desc { ord.reverse() } else { ord };
    ord.then_with(|| a.1.cmp(b.1))
}

pub fn compare_rows(a: &SortedRow, b: &SortedRow, sort: &SortSpec) -> Ordering {
    compare_positions((sort_value(&a.value, sort), &a.key), (sort_value(&b.value, sort), &b.key), sort)
}

/// Rows strictly after the cursor, in the same order the pages are emitted in.
pub fn is_after(row: &SortedRow, cursor: &SortCursor, sort: &SortSpec) -> bool {
    compare_positions((sort_value(&row.value, sort), &row.key), (&cursor.value, &cursor.key), sort)
        == Ordering::Greater
}

pub fn kway_merge(lists: Vec<Vec<SortedRow>>, sort: &SortSpec, limit: usize) -> Vec<SortedRow> {
    let mut heads = vec![0usize; lists.len()];
    let mut out = Vec::with_capacity(limit.min(MAX_QUERY_LIMIT));

    while out.len() < limit {
        let mut best: Option<usize> = None;
        for i in 0..lists.len() {
            if heads[i] < lists[i].len() {
                match best {
                    None => best = Some(i),
                    Some(b) => {
                        if compare_rows(&lists[i][heads[i]], &lists[b][heads[b]], sort) == Ordering::Less {
                            best = Some(i);
                        }
                    }
                }
            }
        }
        match best {
            Some(i) => {
                out.push(lists[i][heads[i]].clone());
                heads[i] += 1;
            },
            None => break,
        }
    }
    out
}

/// Operators `matches_filter` implements. Anything else is a client error, not a silent no-op.
const SUPPORTED_OPS: [&str; 6] = ["$gt", "$gte", "$lt", "$lte", "$ne", "$in"];

fn is_operator_object(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.keys().any(|k| k.starts_with('$'))
}

pub fn parse_filter(s: &str) -> Result<Filter, String> {
    let filter: Filter = serde_json::from_str(s)
        .map_err(|e| format!("filter must be a JSON object: {}", e))?;
    validate_filter(&filter)?;
    Ok(filter)
}

/// Every condition `matches_filter` sees has been through here, so it never meets an operator it
/// cannot evaluate and never has to guess whether an object is a comparison or a literal.
fn validate_filter(filter: &Filter) -> Result<(), String> {
    for (field, cond) in &filter.fields {
        if field.starts_with('$') {
            return Err(format!("unsupported top-level operator `{}`", field));
        }

        let map = match cond.as_object() {
            Some(m) => m,
            None => continue,
        };
        let ops = map.keys().filter(|k| k.starts_with('$')).count();
        if ops == 0 {
            continue;
        }
        if ops != map.len() {
            return Err(format!("condition on `{}` mixes operators with literal keys", field));
        }

        for (op, operand) in map {
            if !SUPPORTED_OPS.contains(&op.as_str()) {
                return Err(format!(
                    "unsupported operator `{}` on `{}`; supported: {}",
                    op, field, SUPPORTED_OPS.join(", ")
                ));
            }
            match op.as_str() {
                "$in" if !operand.is_array() => {
                    return Err(format!("`$in` on `{}` requires an array", field));
                },
                "$gt" | "$gte" | "$lt" | "$lte" if !operand.is_number() => {
                    return Err(format!("`{}` on `{}` requires a number", op, field));
                },
                _ => {},
            }
        }
    }
    Ok(())
}

pub fn matches_filter(doc: &serde_json::Value, filter: &Filter) -> bool {
    for (k, cond) in &filter.fields {
        let val = match get_path_value(doc, k) {
            Some(v) => v,
            None => return false,
        };

        if let Some(cmp) = cond.as_object().filter(|m| is_operator_object(m)) {
            if let Some(gt) = cmp.get("$gt") {
                if !val.as_f64().zip(gt.as_f64()).map_or(false, |(a,b)| a > b) {
                    return false;
                }
            }

            if let Some(gte) = cmp.get("$gte") {
                if !val.as_f64().zip(gte.as_f64()).map_or(false, |(a,b)| a >= b) {
                    return false;
                }
            }

            if let Some(lt) = cmp.get("$lt") {
                if !val.as_f64().zip(lt.as_f64()).map_or(false, |(a,b)| a < b) {
                    return false;
                }
            }

            if let Some(lte) = cmp.get("$lte") {
                if !val.as_f64().zip(lte.as_f64()).map_or(false, |(a,b)| a <= b) {
                    return false;
                }
            }

            if let Some(ne) = cmp.get("$ne") {
                if val == ne {
                    return false;
                }
            }

            if let Some(in_arr) = cmp.get("$in") {
                match in_arr.as_array() {
                    Some(arr) if arr.contains(val) => {},
                    _ => return false,
                }
            }
        } else if val != cond {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(sort_field: &str, ns: &[i64]) -> Vec<SortedRow> {
        ns.iter().map(|n| SortedRow {
            key: format!("k{}", n),
            value: serde_json::json!({ sort_field: n }),
        }).collect()
    }

    fn ns_of(rows: &[SortedRow]) -> Vec<i64> {
        rows.iter().map(|r| r.value["n"].as_i64().unwrap()).collect()
    }

    #[test]
    fn a_merge_bounded_by_a_huge_limit_allocates_for_what_it_holds() {
        let lists = vec![rows("n", &[1]), rows("n", &[2])];
        let sort = parse_sort(Some("n:asc")).unwrap();

        // Unfixed this reserves usize::MAX values before reading the first row.
        let out = kway_merge(lists, &sort, usize::MAX);
        assert_eq!(out.len(), 2, "the limit still bounds the result, it just cannot bound the capacity");
    }

    #[test]
    fn shard_cursor_round_trips_through_base64() {
        let mut positions = BTreeMap::new();
        positions.insert("http://s1".to_string(), Some("k42".to_string()));
        positions.insert("http://s2".to_string(), Some("k17".to_string()));
        positions.insert("http://s3".to_string(), None);
        let c = ShardCursor { ring: 99, positions };

        let encoded = encode_cursor(&c);
        assert!(!encoded.contains('{'), "encoded cursor should be opaque, not raw JSON");

        let decoded: ShardCursor = decode_cursor(&encoded).expect("must decode");
        assert_eq!(decoded.positions.get("http://s1"), Some(&Some("k42".to_string())));
        assert_eq!(decoded.positions.get("http://s2"), Some(&Some("k17".to_string())));
        assert_eq!(decoded.positions.get("http://s3"), Some(&None), "unstarted is not the same as drained");
        assert_eq!(decoded.positions.get("http://s4"), None, "drained shards are absent");
        assert_eq!(decoded.ring, 99, "the positions are only meaningful against their partitioning");

        assert!(decode_cursor::<ShardCursor>("!!!not base64 json!!!").is_none());
    }

    #[test]
    fn parse_sort_directions() {
        let a = parse_sort(Some("age")).unwrap();
        assert_eq!(a.field, "age");
        assert!(!a.desc);

        let d = parse_sort(Some("age:desc")).unwrap();
        assert!(d.desc);

        let asc = parse_sort(Some("score:asc")).unwrap();
        assert!(!asc.desc);

        assert!(parse_sort(None).is_none());
        assert!(parse_sort(Some("")).is_none());
    }

    #[test]
    fn kway_merge_produces_global_order_bounded_by_limit() {
        let sort = SortSpec { field: "n".to_string(), desc: false };
        let lists = vec![rows("n", &[1, 4, 7]), rows("n", &[2, 3, 8]), rows("n", &[5, 6])];

        let merged = kway_merge(lists, &sort, 5);
        assert_eq!(ns_of(&merged), vec![1, 2, 3, 4, 5], "globally sorted, bounded to limit");
    }

    /// M1: an object condition with no operators is an equality test, not "has this field".
    #[test]
    fn a_literal_object_condition_compares_by_value() {
        use serde_json::json;
        let f = parse_filter(r#"{"meta": {"v": 1}}"#).expect("a literal object is a valid condition");

        assert!(matches_filter(&json!({"meta": {"v": 1}}), &f));
        assert!(!matches_filter(&json!({"meta": {"v": 2}}), &f), "unfixed this matched");
        assert!(!matches_filter(&json!({"meta": {}}), &f), "unfixed this matched");
    }

    #[test]
    fn unusable_conditions_are_rejected_not_skipped() {
        assert!(parse_filter(r#"{"a": {"$exists": true}}"#).is_err(), "unknown operator");
        assert!(parse_filter(r#"{"a": {"$regex": "x"}}"#).is_err(), "unknown operator");
        assert!(parse_filter(r#"{"a": {"$in": 3}}"#).is_err(), "$in wants an array");
        assert!(parse_filter(r#"{"a": {"$gt": "b"}}"#).is_err(), "range operators are numeric");
        assert!(parse_filter(r#"{"a": {"$gt": 1, "unit": "kg"}}"#).is_err(), "operators mixed with literals");
        assert!(parse_filter(r#"{"$or": [{"a": 1}]}"#).is_err(), "no top-level operators");
        assert!(parse_filter("[1,2]").is_err(), "not an object");
        assert!(parse_filter("{").is_err(), "not JSON");
    }

    #[test]
    fn supported_conditions_still_parse_and_match() {
        use serde_json::json;
        let f = parse_filter(r#"{"n": {"$gte": 2, "$lt": 5}, "tag": {"$in": ["a", "b"]}, "s": {"$ne": "x"}}"#)
            .expect("all supported");

        assert!(matches_filter(&json!({"n": 3, "tag": "b", "s": "y"}), &f));
        assert!(!matches_filter(&json!({"n": 5, "tag": "b", "s": "y"}), &f));
        assert!(!matches_filter(&json!({"n": 3, "tag": "c", "s": "y"}), &f));
        assert!(!matches_filter(&json!({"n": 3, "tag": "b", "s": "x"}), &f));
        assert!(!matches_filter(&json!({"n": 3, "tag": "b"}), &f), "a missing field cannot match");
    }

    /// The operand is an array, so a non-array document value can only match element-wise.
    #[test]
    fn in_rejects_a_value_that_is_not_a_member() {
        use serde_json::json;
        let f = parse_filter(r#"{"tag": {"$in": []}}"#).unwrap();
        assert!(!matches_filter(&json!({"tag": "a"}), &f));
    }

    #[test]
    fn kway_merge_desc() {
        let sort = SortSpec { field: "n".to_string(), desc: true };
        let merged = kway_merge(vec![rows("n", &[9, 3]), rows("n", &[7, 1])], &sort, 3);
        assert_eq!(ns_of(&merged), vec![9, 7, 3]);
    }

    /// H8: a sorted page resumes from a position in the sort order, so rows that tie on the sort
    /// field have to be separated by something. Without the key the merge picks a tied row
    /// arbitrarily and the cursor built from it either repeats its twin or skips it.
    #[test]
    fn ties_on_the_sort_field_are_broken_by_key() {
        use serde_json::json;
        let sort = SortSpec { field: "n".to_string(), desc: false };
        let tied = |key: &str| SortedRow { key: key.to_string(), value: json!({"n": 1}) };

        assert_eq!(compare_rows(&tied("a"), &tied("b"), &sort), Ordering::Less);
        assert_eq!(compare_rows(&tied("b"), &tied("a"), &sort), Ordering::Greater);
        assert_eq!(compare_rows(&tied("a"), &tied("a"), &sort), Ordering::Equal);

        // Descending applies to the field, never to the tiebreaker.
        let desc = SortSpec { field: "n".to_string(), desc: true };
        assert_eq!(compare_rows(&tied("a"), &tied("b"), &desc), Ordering::Less);

        let merged = kway_merge(vec![vec![tied("c")], vec![tied("a")], vec![tied("b")]], &sort, 3);
        assert_eq!(merged.iter().map(|r| r.key.clone()).collect::<Vec<_>>(), vec!["a", "b", "c"],
            "a merge of tied rows must still have one answer");
    }

    #[test]
    fn a_sort_cursor_admits_exactly_the_rows_after_it() {
        use serde_json::json;
        let sort = SortSpec { field: "n".to_string(), desc: false };
        let at = SortCursor { value: json!(2), key: "k2".to_string() };
        let row = |key: &str, n: i64| SortedRow { key: key.to_string(), value: json!({"n": n}) };

        assert!(!is_after(&row("k1", 1), &at, &sort));
        assert!(!is_after(&row("k2", 2), &at, &sort), "the cursor row itself is behind it");
        assert!(is_after(&row("k3", 2), &at, &sort), "a tie is separated by the key");
        assert!(!is_after(&row("k0", 2), &at, &sort), "and separated in both directions");
        assert!(is_after(&row("k0", 3), &at, &sort));

        let desc = SortSpec { field: "n".to_string(), desc: true };
        assert!(is_after(&row("k1", 1), &at, &desc), "descending walks the other way");
        assert!(!is_after(&row("k9", 3), &at, &desc));

        // A document without the field sorts as null, which is before every number ascending.
        let missing = SortedRow { key: "k5".to_string(), value: json!({"other": 1}) };
        assert!(!is_after(&missing, &at, &sort));
        assert!(is_after(&missing, &at, &desc));
    }
}
