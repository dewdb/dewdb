//! Query semantics: filters, sort keys, cross-shard merge, pagination cursors.

use crate::json::{get_path_value, json_cmp};
use crate::model::MAX_QUERY_LIMIT;
use crate::util::base64_bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Serialize, Deserialize, Default)]
pub struct ShardCursor {
    pub positions: BTreeMap<String, String>,
}

pub fn encode_cursor(c: &ShardCursor) -> String {
    let json = serde_json::to_vec(c).unwrap_or_default();
    base64_bytes::base64_encode(&json)
}

pub fn decode_cursor(s: &str) -> Option<ShardCursor> {
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

pub fn compare_by_sort(a: &serde_json::Value, b: &serde_json::Value, sort: &SortSpec) -> std::cmp::Ordering {
    let va = get_path_value(a, &sort.field).cloned().unwrap_or(serde_json::Value::Null);
    let vb = get_path_value(b, &sort.field).cloned().unwrap_or(serde_json::Value::Null);
    let ord = json_cmp(&va, &vb);
    if sort.desc {
        ord.reverse()
    } else {
        ord
    }
}

pub fn kway_merge(lists: Vec<Vec<serde_json::Value>>, sort: &SortSpec, limit: usize) -> Vec<serde_json::Value> {
    let mut heads = vec![0usize; lists.len()];
    let mut out = Vec::with_capacity(limit.min(MAX_QUERY_LIMIT));

    while out.len() < limit {
        let mut best: Option<usize> = None;
        for i in 0..lists.len() {
            if heads[i] < lists[i].len() {
                match best {
                    None => best = Some(i),
                    Some(b) => {
                        if compare_by_sort(&lists[i][heads[i]], &lists[b][heads[b]], sort) == std::cmp::Ordering::Less {
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

    #[test]
    fn a_merge_bounded_by_a_huge_limit_allocates_for_what_it_holds() {
        let lists = vec![vec![serde_json::json!({"n": 1})], vec![serde_json::json!({"n": 2})]];
        let sort = parse_sort(Some("n:asc")).unwrap();

        // Unfixed this reserves usize::MAX values before reading the first row.
        let out = kway_merge(lists, &sort, usize::MAX);
        assert_eq!(out.len(), 2, "the limit still bounds the result, it just cannot bound the capacity");
    }

    #[test]
    fn shard_cursor_round_trips_through_base64() {
        let mut positions = BTreeMap::new();
        positions.insert("http://s1".to_string(), "k42".to_string());
        positions.insert("http://s2".to_string(), "k17".to_string());
        let c = ShardCursor { positions };

        let encoded = encode_cursor(&c);
        assert!(!encoded.contains('{'), "encoded cursor should be opaque, not raw JSON");

        let decoded = decode_cursor(&encoded).expect("must decode");
        assert_eq!(decoded.positions.get("http://s1").map(|s| s.as_str()), Some("k42"));
        assert_eq!(decoded.positions.get("http://s2").map(|s| s.as_str()), Some("k17"));

        assert!(decode_cursor("!!!not base64 json!!!").is_none());
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
        use serde_json::json;
        let sort = SortSpec { field: "n".to_string(), desc: false };
        let l1 = vec![json!({"n": 1}), json!({"n": 4}), json!({"n": 7})];
        let l2 = vec![json!({"n": 2}), json!({"n": 3}), json!({"n": 8})];
        let l3 = vec![json!({"n": 5}), json!({"n": 6})];

        let merged = kway_merge(vec![l1, l2, l3], &sort, 5);
        let ns: Vec<i64> = merged.iter().map(|v| v["n"].as_i64().unwrap()).collect();
        assert_eq!(ns, vec![1, 2, 3, 4, 5], "globally sorted, bounded to limit");
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
        use serde_json::json;
        let sort = SortSpec { field: "n".to_string(), desc: true };
        let l1 = vec![json!({"n": 9}), json!({"n": 3})];
        let l2 = vec![json!({"n": 7}), json!({"n": 1})];
        let merged = kway_merge(vec![l1, l2], &sort, 3);
        let ns: Vec<i64> = merged.iter().map(|v| v["n"].as_i64().unwrap()).collect();
        assert_eq!(ns, vec![9, 7, 3]);
    }
}
