//! Query semantics: filter expressions, sort keys, cross-shard merge, pagination cursors.

use crate::json::{get_path_value, json_cmp};
use crate::model::MAX_QUERY_LIMIT;
use crate::util::base64_bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// Nesting allowed under `$and`/`$or`/`$nor`/`$not`/`$elemMatch`. Evaluation and parsing both
/// recurse, so an unbounded filter is a stack overflow rather than a slow query.
pub const MAX_FILTER_DEPTH: usize = 16;

/// Sort keys allowed in one `?sort=`. Each one costs a path lookup per comparison.
pub const MAX_SORT_KEYS: usize = 8;

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
/// of these covers the whole cluster: "after this row" is the same question on every shard.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortCursor {
    /// One value per sort key, in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<serde_json::Value>,
    /// The single-key spelling a cursor issued before multi-field sort carries, read as a
    /// one-element position so a page in flight across an upgrade resumes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    pub key: String,
}

impl SortCursor {
    pub fn at(values: Vec<serde_json::Value>, key: String) -> Self {
        Self { values, value: None, key }
    }

    pub fn positions(&self) -> &[serde_json::Value] {
        match &self.value {
            Some(v) if self.values.is_empty() => std::slice::from_ref(v),
            _ => &self.values,
        }
    }
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

pub struct SortKey {
    pub field: String,
    pub desc: bool,
}

/// One or more sort keys applied in order, with the document key as the final tiebreaker.
pub struct SortOrder {
    pub keys: Vec<SortKey>,
}

/// `field[:asc|desc]`, comma-separated. Every rejection here used to be an accepted query meaning
/// something else: `:descc` sorted ascending, `:desc` dropped the sort, neither reported (IB-019).
pub fn parse_sort(s: Option<&str>) -> Result<Option<SortOrder>, String> {
    let Some(s) = s else { return Ok(None) };
    let mut keys: Vec<SortKey> = Vec::new();

    for part in s.split(',') {
        let part = part.trim();
        let (field, dir) = match part.split_once(':') {
            Some((f, d)) => (f, d),
            None => (part, "asc"),
        };
        if field.is_empty() {
            return Err(format!("sort key `{}` names no field", part));
        }
        if dir.contains(':') {
            return Err(format!("sort key `{}` has more than one direction", part));
        }
        let desc = match dir {
            d if d.eq_ignore_ascii_case("asc") => false,
            d if d.eq_ignore_ascii_case("desc") => true,
            other => return Err(format!(
                "unknown sort direction `{}` on `{}`; use `asc` or `desc`", other, field)),
        };
        if keys.iter().any(|k| k.field == field) {
            return Err(format!("sort names `{}` twice", field));
        }
        keys.push(SortKey { field: field.to_string(), desc });
    }

    if keys.len() > MAX_SORT_KEYS {
        return Err(format!("sort has {} keys, more than the maximum of {}", keys.len(), MAX_SORT_KEYS));
    }
    Ok(Some(SortOrder { keys }))
}

/// Inclusive bounds on the key order. A reversed pair reached `BTreeMap::range` and panicked
/// there (IB-018); it is a malformed request, not an empty page, so the client is told.
pub fn check_key_range(start: Option<&str>, end: Option<&str>) -> Result<(), String> {
    match start.zip(end) {
        Some((s, e)) if s > e => Err(format!(
            "start `{}` is above end `{}`; both bounds are inclusive and ascending", s, e)),
        _ => Ok(()),
    }
}

/// The sort field, or `Null` where the document does not have it.
fn key_value<'a>(doc: &'a serde_json::Value, key: &SortKey) -> &'a serde_json::Value {
    get_path_value(doc, &key.field).unwrap_or(&serde_json::Value::Null)
}

/// The position a cursor built from this row records: one value per sort key.
pub fn sort_position(doc: &serde_json::Value, order: &SortOrder) -> Vec<serde_json::Value> {
    order.keys.iter().map(|k| key_value(doc, k).clone()).collect()
}

/// Direction applies per key; the document key always ascends. Both halves matter: without the key
/// two rows equal on every sort field tie, and a cursor cannot resume past a tie.
fn compare_positions(
    a: (&[&serde_json::Value], &str),
    b: (&[&serde_json::Value], &str),
    order: &SortOrder,
) -> Ordering {
    for (i, key) in order.keys.iter().enumerate() {
        let (Some(x), Some(y)) = (a.0.get(i), b.0.get(i)) else { break };
        let ord = json_cmp(x, y);
        let ord = if key.desc { ord.reverse() } else { ord };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.1.cmp(b.1)
}

pub fn compare_rows(a: &SortedRow, b: &SortedRow, order: &SortOrder) -> Ordering {
    for key in &order.keys {
        let ord = json_cmp(key_value(&a.value, key), key_value(&b.value, key));
        let ord = if key.desc { ord.reverse() } else { ord };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.key.cmp(&b.key)
}

/// Rows strictly after the cursor, in the same order the pages are emitted in. The cursor's arity
/// is checked where it is decoded, so a position shorter than the order stops comparing early.
pub fn is_after(row: &SortedRow, cursor: &SortCursor, order: &SortOrder) -> bool {
    let row_values: Vec<&serde_json::Value> =
        order.keys.iter().map(|k| key_value(&row.value, k)).collect();
    let at: Vec<&serde_json::Value> = cursor.positions().iter().collect();
    compare_positions((&row_values, &row.key), (&at, &cursor.key), order) == Ordering::Greater
}

pub fn kway_merge(lists: Vec<Vec<SortedRow>>, order: &SortOrder, limit: usize) -> Vec<SortedRow> {
    let mut heads = vec![0usize; lists.len()];
    let mut out = Vec::with_capacity(limit.min(MAX_QUERY_LIMIT));

    while out.len() < limit {
        let mut best: Option<usize> = None;
        for i in 0..lists.len() {
            if heads[i] < lists[i].len() {
                match best {
                    None => best = Some(i),
                    Some(b) => {
                        if compare_rows(&lists[i][heads[i]], &lists[b][heads[b]], order) == Ordering::Less {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl JsonKind {
    pub fn of(v: &serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(_) => Self::Bool,
            serde_json::Value::Number(_) => Self::Number,
            serde_json::Value::String(_) => Self::String,
            serde_json::Value::Array(_) => Self::Array,
            serde_json::Value::Object(_) => Self::Object,
        }
    }

    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "null" => Self::Null,
            "bool" => Self::Bool,
            "number" => Self::Number,
            "string" => Self::String,
            "array" => Self::Array,
            "object" => Self::Object,
            _ => return None,
        })
    }
}

const TYPE_NAMES: [&str; 6] = ["null", "bool", "number", "string", "array", "object"];

/// One test against the value at a field path. A condition is a conjunction of these.
pub enum Op {
    Eq(serde_json::Value),
    Ne(serde_json::Value),
    In(Vec<serde_json::Value>),
    Nin(Vec<serde_json::Value>),
    Gt(serde_json::Value),
    Gte(serde_json::Value),
    Lt(serde_json::Value),
    Lte(serde_json::Value),
    Exists(bool),
    Type(Vec<JsonKind>),
    Prefix(String),
    Suffix(String),
    Contains(String),
    All(Vec<serde_json::Value>),
    Size(usize),
    ElemMatch(Box<ElemPredicate>),
    Not(Box<Condition>),
}

/// `$elemMatch` against scalar elements (`{"$gt": 5}`) or against object elements (`{"a": 5}`).
pub enum ElemPredicate {
    Ops(Condition),
    Doc(Filter),
}

/// Every test on one field path, ANDed.
pub struct Condition {
    ops: Vec<Op>,
}

impl Condition {
    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    /// `None` is the document not having the path. Only `$exists` and `$not` can match that: every
    /// other operator needs a value, which is also what makes a single-field index sound to plan on.
    pub fn matches(&self, val: Option<&serde_json::Value>) -> bool {
        self.ops.iter().all(|op| op_matches(op, val))
    }
}

fn op_matches(op: &Op, val: Option<&serde_json::Value>) -> bool {
    match op {
        Op::Exists(want) => val.is_some() == *want,
        Op::Not(inner) => !inner.matches(val),
        _ => match val {
            None => false,
            Some(v) => present_op_matches(op, v),
        },
    }
}

/// `json_cmp` within one type only: a number is never above a string, it is a different kind of
/// thing, and admitting the cross-type order here would make `$gt: 1` match `"zebra"`.
fn same_type_cmp(v: &serde_json::Value, operand: &serde_json::Value) -> Option<Ordering> {
    (JsonKind::of(v) == JsonKind::of(operand)).then(|| json_cmp(v, operand))
}

fn present_op_matches(op: &Op, v: &serde_json::Value) -> bool {
    match op {
        Op::Eq(want) => v == want,
        Op::Ne(want) => v != want,
        Op::In(list) => list.contains(v),
        Op::Nin(list) => !list.contains(v),
        Op::Gt(b) => same_type_cmp(v, b) == Some(Ordering::Greater),
        Op::Gte(b) => matches!(same_type_cmp(v, b), Some(Ordering::Greater | Ordering::Equal)),
        Op::Lt(b) => same_type_cmp(v, b) == Some(Ordering::Less),
        Op::Lte(b) => matches!(same_type_cmp(v, b), Some(Ordering::Less | Ordering::Equal)),
        Op::Type(kinds) => kinds.contains(&JsonKind::of(v)),
        Op::Prefix(p) => v.as_str().is_some_and(|s| s.starts_with(p.as_str())),
        Op::Suffix(p) => v.as_str().is_some_and(|s| s.ends_with(p.as_str())),
        Op::Contains(p) => v.as_str().is_some_and(|s| s.contains(p.as_str())),
        Op::All(want) => v.as_array().is_some_and(|a| want.iter().all(|w| a.contains(w))),
        Op::Size(n) => v.as_array().is_some_and(|a| a.len() == *n),
        Op::ElemMatch(pred) => v.as_array().is_some_and(|a| a.iter().any(|e| match pred.as_ref() {
            ElemPredicate::Ops(cond) => cond.matches(Some(e)),
            ElemPredicate::Doc(filter) => filter.matches(e),
        })),
        Op::Exists(_) | Op::Not(_) => unreachable!("handled before the value is unwrapped"),
    }
}

enum Expr {
    All(Vec<Expr>),
    Any(Vec<Expr>),
    Nor(Vec<Expr>),
    Field { path: String, cond: Condition },
}

/// A parsed filter document.
pub struct Filter {
    root: Expr,
}

impl Filter {
    pub fn matches(&self, doc: &serde_json::Value) -> bool {
        eval(&self.root, doc)
    }

    /// The field conditions that hold for every matching document: those reachable through `$and`
    /// alone. An index chosen inside an `$or` would drop the rows the other branches match.
    pub fn conjuncts(&self) -> Vec<(&str, &Condition)> {
        let mut out = Vec::new();
        collect_conjuncts(&self.root, &mut out);
        out
    }
}

fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<(&'a str, &'a Condition)>) {
    match expr {
        Expr::Field { path, cond } => out.push((path.as_str(), cond)),
        Expr::All(parts) => parts.iter().for_each(|p| collect_conjuncts(p, out)),
        Expr::Any(_) | Expr::Nor(_) => {},
    }
}

fn eval(expr: &Expr, doc: &serde_json::Value) -> bool {
    match expr {
        Expr::Field { path, cond } => cond.matches(get_path_value(doc, path)),
        Expr::All(parts) => parts.iter().all(|p| eval(p, doc)),
        Expr::Any(parts) => parts.iter().any(|p| eval(p, doc)),
        Expr::Nor(parts) => !parts.iter().any(|p| eval(p, doc)),
    }
}

pub fn matches_filter(doc: &serde_json::Value, filter: &Filter) -> bool {
    filter.matches(doc)
}

pub fn parse_filter(s: &str) -> Result<Filter, String> {
    let value: serde_json::Value = serde_json::from_str(s)
        .map_err(|e| format!("filter must be a JSON object: {}", e))?;
    Ok(Filter { root: parse_expr(&value, 0)? })
}

fn parse_expr(value: &serde_json::Value, depth: usize) -> Result<Expr, String> {
    if depth > MAX_FILTER_DEPTH {
        return Err(format!("filter nests deeper than the maximum of {}", MAX_FILTER_DEPTH));
    }
    let map = value.as_object().ok_or("filter must be a JSON object")?;

    let mut parts = Vec::new();
    for (key, operand) in map {
        let part = match key.as_str() {
            "$and" => Expr::All(parse_branches(key, operand, depth)?),
            "$or" => Expr::Any(parse_branches(key, operand, depth)?),
            "$nor" => Expr::Nor(parse_branches(key, operand, depth)?),
            k if k.starts_with('$') => return Err(format!(
                "unsupported document operator `{}`; supported: $and, $or, $nor", k)),
            path => Expr::Field {
                path: path.to_string(),
                cond: parse_condition(path, operand, depth)?,
            },
        };
        parts.push(part);
    }
    Ok(Expr::All(parts))
}

fn parse_branches(op: &str, operand: &serde_json::Value, depth: usize) -> Result<Vec<Expr>, String> {
    let arr = operand.as_array()
        .filter(|a| !a.is_empty())
        .ok_or_else(|| format!("`{}` requires a non-empty array of filters", op))?;
    arr.iter().map(|b| parse_expr(b, depth + 1)).collect()
}

fn parse_condition(field: &str, cond: &serde_json::Value, depth: usize) -> Result<Condition, String> {
    let Some(map) = cond.as_object() else {
        return Ok(Condition { ops: vec![Op::Eq(cond.clone())] });
    };
    let ops = map.keys().filter(|k| k.starts_with('$')).count();
    // An object with no operators is a literal, which is how a document member is matched whole.
    if ops == 0 {
        return Ok(Condition { ops: vec![Op::Eq(cond.clone())] });
    }
    if ops != map.len() {
        return Err(format!("condition on `{}` mixes operators with literal keys", field));
    }

    let mut parsed = Vec::with_capacity(map.len());
    for (op, operand) in map {
        parsed.push(parse_op(field, op, operand, depth)?);
    }
    check_range_agreement(field, &parsed)?;
    Ok(Condition { ops: parsed })
}

fn parse_op(field: &str, op: &str, operand: &serde_json::Value, depth: usize) -> Result<Op, String> {
    let list = |name: &str| -> Result<Vec<serde_json::Value>, String> {
        operand.as_array().cloned()
            .ok_or_else(|| format!("`{}` on `{}` requires an array", name, field))
    };
    let text = |name: &str| -> Result<String, String> {
        operand.as_str().map(str::to_string)
            .ok_or_else(|| format!("`{}` on `{}` requires a string", name, field))
    };
    let bound = |name: &str| -> Result<serde_json::Value, String> {
        match operand {
            serde_json::Value::Number(_) | serde_json::Value::String(_) => Ok(operand.clone()),
            _ => Err(format!("`{}` on `{}` requires a number or a string", name, field)),
        }
    };

    Ok(match op {
        "$eq" => Op::Eq(operand.clone()),
        "$ne" => Op::Ne(operand.clone()),
        "$in" => Op::In(list("$in")?),
        "$nin" => Op::Nin(list("$nin")?),
        "$gt" => Op::Gt(bound("$gt")?),
        "$gte" => Op::Gte(bound("$gte")?),
        "$lt" => Op::Lt(bound("$lt")?),
        "$lte" => Op::Lte(bound("$lte")?),
        "$exists" => Op::Exists(operand.as_bool()
            .ok_or_else(|| format!("`$exists` on `{}` requires a boolean", field))?),
        "$type" => Op::Type(parse_types(field, operand)?),
        "$prefix" => Op::Prefix(text("$prefix")?),
        "$suffix" => Op::Suffix(text("$suffix")?),
        "$contains" => Op::Contains(text("$contains")?),
        "$all" => Op::All(list("$all")?),
        "$size" => Op::Size(operand.as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| format!("`$size` on `{}` requires a non-negative integer", field))?),
        "$elemMatch" => Op::ElemMatch(Box::new(parse_elem_match(field, operand, depth)?)),
        "$not" => {
            let inner = operand.as_object()
                .filter(|m| m.keys().all(|k| k.starts_with('$')) && !m.is_empty())
                .ok_or_else(|| format!("`$not` on `{}` requires an operator object", field))?;
            Op::Not(Box::new(parse_condition(
                field, &serde_json::Value::Object(inner.clone()), depth + 1)?))
        },
        other => return Err(format!(
            "unsupported operator `{}` on `{}`; supported: {}", other, field, FIELD_OPS.join(", "))),
    })
}

const FIELD_OPS: [&str; 17] = [
    "$eq", "$ne", "$in", "$nin", "$gt", "$gte", "$lt", "$lte", "$exists", "$type",
    "$prefix", "$suffix", "$contains", "$all", "$size", "$elemMatch", "$not",
];

fn parse_types(field: &str, operand: &serde_json::Value) -> Result<Vec<JsonKind>, String> {
    let names: Vec<&str> = match operand {
        serde_json::Value::String(s) => vec![s.as_str()],
        serde_json::Value::Array(a) => a.iter()
            .map(|v| v.as_str().ok_or_else(|| format!(
                "`$type` on `{}` requires type names; got {}", field, v)))
            .collect::<Result<_, _>>()?,
        _ => return Err(format!("`$type` on `{}` requires a type name or an array of them", field)),
    };
    if names.is_empty() {
        return Err(format!("`$type` on `{}` names no type", field));
    }
    names.into_iter()
        .map(|n| JsonKind::parse(n).ok_or_else(|| format!(
            "unknown type `{}` on `{}`; supported: {}", n, field, TYPE_NAMES.join(", "))))
        .collect()
}

fn parse_elem_match(field: &str, operand: &serde_json::Value, depth: usize) -> Result<ElemPredicate, String> {
    let map = operand.as_object()
        .ok_or_else(|| format!("`$elemMatch` on `{}` requires an object", field))?;
    // Operators test the element itself; anything else is a filter over an object element.
    if map.keys().any(|k| k.starts_with('$')) {
        return Ok(ElemPredicate::Ops(parse_condition(field, operand, depth + 1)?));
    }
    Ok(ElemPredicate::Doc(Filter { root: parse_expr(operand, depth + 1)? }))
}

/// Range bounds are compared within one JSON type, so a condition naming two types names an empty
/// set. Refused rather than answered empty, since it can only be a mistake.
fn check_range_agreement(field: &str, ops: &[Op]) -> Result<(), String> {
    let mut kind: Option<JsonKind> = None;
    for op in ops {
        let operand = match op {
            Op::Gt(v) | Op::Gte(v) | Op::Lt(v) | Op::Lte(v) => v,
            _ => continue,
        };
        let this = JsonKind::of(operand);
        match kind {
            Some(seen) if seen != this => return Err(format!(
                "range bounds on `{}` mix types; all of $gt/$gte/$lt/$lte must compare the same kind", field)),
            _ => kind = Some(this),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn order(s: &str) -> SortOrder {
        parse_sort(Some(s)).unwrap().unwrap()
    }

    fn rows(sort_field: &str, ns: &[i64]) -> Vec<SortedRow> {
        ns.iter().map(|n| SortedRow {
            key: format!("k{}", n),
            value: json!({ sort_field: n }),
        }).collect()
    }

    fn ns_of(rows: &[SortedRow]) -> Vec<i64> {
        rows.iter().map(|r| r.value["n"].as_i64().unwrap()).collect()
    }

    fn hits(filter: &str, doc: serde_json::Value) -> bool {
        matches_filter(&doc, &parse_filter(filter).expect("valid filter"))
    }

    #[test]
    fn a_merge_bounded_by_a_huge_limit_allocates_for_what_it_holds() {
        let lists = vec![rows("n", &[1]), rows("n", &[2])];

        // Unfixed this reserves usize::MAX values before reading the first row.
        let out = kway_merge(lists, &order("n:asc"), usize::MAX);
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
        let a = order("age");
        assert_eq!(a.keys[0].field, "age");
        assert!(!a.keys[0].desc);

        assert!(order("age:desc").keys[0].desc);
        assert!(!order("score:ASC").keys[0].desc);
        assert!(parse_sort(None).unwrap().is_none());
    }

    /// IB-019: every one of these was accepted and quietly meant something else — an unknown
    /// direction sorted ascending, and an empty field turned a sorted query into an unsorted one.
    #[test]
    fn invalid_sort_syntax_is_refused_rather_than_reinterpreted() {
        for bad in ["", ":desc", "v:descc", "v:", "v:asc:desc", "a,,b", "a:asc,a:desc"] {
            assert!(parse_sort(Some(bad)).is_err(), "`{}` must be refused", bad);
        }
        let many: Vec<String> = (0..=MAX_SORT_KEYS).map(|i| format!("f{}", i)).collect();
        assert!(parse_sort(Some(&many.join(","))).is_err(), "the key count is bounded");
    }

    #[test]
    fn a_multi_key_sort_falls_through_to_the_later_keys() {
        let o = order("a:asc,b:desc");
        let row = |key: &str, a: i64, b: i64| SortedRow { key: key.to_string(), value: json!({"a": a, "b": b}) };

        assert_eq!(compare_rows(&row("k1", 1, 9), &row("k2", 2, 0), &o), Ordering::Less, "first key decides");
        assert_eq!(compare_rows(&row("k1", 1, 1), &row("k2", 1, 9), &o), Ordering::Greater,
            "equal on the first, and the second descends");
        assert_eq!(compare_rows(&row("a", 1, 1), &row("b", 1, 1), &o), Ordering::Less,
            "equal on every key, and the document key ascends");

        let merged = kway_merge(vec![vec![row("x", 1, 1), row("y", 2, 5)], vec![row("z", 1, 7)]], &o, 3);
        assert_eq!(merged.iter().map(|r| r.key.clone()).collect::<Vec<_>>(), vec!["z", "x", "y"]);
    }

    #[test]
    fn kway_merge_produces_global_order_bounded_by_limit() {
        let lists = vec![rows("n", &[1, 4, 7]), rows("n", &[2, 3, 8]), rows("n", &[5, 6])];

        let merged = kway_merge(lists, &order("n"), 5);
        assert_eq!(ns_of(&merged), vec![1, 2, 3, 4, 5], "globally sorted, bounded to limit");
    }

    /// M1: an object condition with no operators is an equality test, not "has this field".
    #[test]
    fn a_literal_object_condition_compares_by_value() {
        assert!(hits(r#"{"meta": {"v": 1}}"#, json!({"meta": {"v": 1}})));
        assert!(!hits(r#"{"meta": {"v": 1}}"#, json!({"meta": {"v": 2}})), "unfixed this matched");
        assert!(!hits(r#"{"meta": {"v": 1}}"#, json!({"meta": {}})), "unfixed this matched");
    }

    #[test]
    fn unusable_conditions_are_rejected_not_skipped() {
        for bad in [
            r#"{"a": {"$regex": "x"}}"#,
            r#"{"a": {"$in": 3}}"#,
            r#"{"a": {"$gt": true}}"#,
            r#"{"a": {"$gt": 1, "$lt": "z"}}"#,
            r#"{"a": {"$gt": 1, "unit": "kg"}}"#,
            r#"{"a": {"$exists": "yes"}}"#,
            r#"{"a": {"$type": "int"}}"#,
            r#"{"a": {"$size": -1}}"#,
            r#"{"a": {"$prefix": 3}}"#,
            r#"{"a": {"$elemMatch": 3}}"#,
            r#"{"a": {"$not": 3}}"#,
            r#"{"$where": "x"}"#,
            r#"{"$or": []}"#,
            r#"{"$and": {"a": 1}}"#,
            "[1,2]",
            "{",
        ] {
            assert!(parse_filter(bad).is_err(), "`{}` must be refused", bad);
        }
    }

    #[test]
    fn supported_conditions_still_parse_and_match() {
        let f = r#"{"n": {"$gte": 2, "$lt": 5}, "tag": {"$in": ["a", "b"]}, "s": {"$ne": "x"}}"#;
        assert!(hits(f, json!({"n": 3, "tag": "b", "s": "y"})));
        assert!(!hits(f, json!({"n": 5, "tag": "b", "s": "y"})));
        assert!(!hits(f, json!({"n": 3, "tag": "c", "s": "y"})));
        assert!(!hits(f, json!({"n": 3, "tag": "b", "s": "x"})));
        assert!(!hits(f, json!({"n": 3, "tag": "b"})), "a missing field cannot match");
    }

    /// The operand is an array, so a non-array document value can only match element-wise.
    #[test]
    fn in_rejects_a_value_that_is_not_a_member() {
        assert!(!hits(r#"{"tag": {"$in": []}}"#, json!({"tag": "a"})));
        assert!(hits(r#"{"tag": {"$nin": ["b"]}}"#, json!({"tag": "a"})));
        assert!(!hits(r#"{"tag": {"$nin": ["a"]}}"#, json!({"tag": "a"})));
        assert!(!hits(r#"{"tag": {"$nin": ["b"]}}"#, json!({"other": 1})), "$nin still needs the field");
    }

    #[test]
    fn range_operators_compare_strings_within_the_string_type() {
        assert!(hits(r#"{"s": {"$gte": "b", "$lt": "d"}}"#, json!({"s": "c"})));
        assert!(!hits(r#"{"s": {"$gte": "b"}}"#, json!({"s": "a"})));
        assert!(!hits(r#"{"s": {"$gt": "b"}}"#, json!({"s": 99})),
            "a number is not above a string, it is a different kind of thing");
        assert!(!hits(r#"{"s": {"$gt": 1}}"#, json!({"s": "zebra"})));
    }

    #[test]
    fn existence_and_type_tests() {
        assert!(hits(r#"{"a": {"$exists": true}}"#, json!({"a": null})), "an explicit null is present");
        assert!(!hits(r#"{"a": {"$exists": true}}"#, json!({"b": 1})));
        assert!(hits(r#"{"a": {"$exists": false}}"#, json!({"b": 1})));
        assert!(!hits(r#"{"a": {"$exists": false}}"#, json!({"a": null})));

        assert!(hits(r#"{"a": {"$type": "number"}}"#, json!({"a": 1})));
        assert!(!hits(r#"{"a": {"$type": "number"}}"#, json!({"a": "1"})));
        assert!(hits(r#"{"a": {"$type": ["number", "string"]}}"#, json!({"a": "1"})));
        assert!(hits(r#"{"a": {"$type": "null"}}"#, json!({"a": null})));
    }

    #[test]
    fn string_predicates_apply_only_to_strings() {
        assert!(hits(r#"{"s": {"$prefix": "ab"}}"#, json!({"s": "abc"})));
        assert!(!hits(r#"{"s": {"$prefix": "ab"}}"#, json!({"s": "xbc"})));
        assert!(hits(r#"{"s": {"$suffix": "bc"}}"#, json!({"s": "abc"})));
        assert!(hits(r#"{"s": {"$contains": "b"}}"#, json!({"s": "abc"})));
        assert!(!hits(r#"{"s": {"$contains": "b"}}"#, json!({"s": 12})),
            "a number holding those digits is not a string that contains them");
    }

    #[test]
    fn array_containment_and_matching() {
        let doc = json!({"tags": ["a", "b", "c"]});
        assert!(hits(r#"{"tags": {"$all": ["a", "c"]}}"#, doc.clone()));
        assert!(!hits(r#"{"tags": {"$all": ["a", "z"]}}"#, doc.clone()));
        assert!(hits(r#"{"tags": {"$size": 3}}"#, doc.clone()));
        assert!(!hits(r#"{"tags": {"$size": 2}}"#, doc.clone()));
        assert!(!hits(r#"{"tags": {"$all": ["a"]}}"#, json!({"tags": "a"})), "$all needs an array");

        assert!(hits(r#"{"ns": {"$elemMatch": {"$gt": 5}}}"#, json!({"ns": [1, 9]})));
        assert!(!hits(r#"{"ns": {"$elemMatch": {"$gt": 5}}}"#, json!({"ns": [1, 2]})));

        let items = json!({"items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 9}]});
        assert!(hits(r#"{"items": {"$elemMatch": {"sku": "y", "qty": {"$gte": 5}}}}"#, items.clone()));
        assert!(!hits(r#"{"items": {"$elemMatch": {"sku": "x", "qty": {"$gte": 5}}}}"#, items),
            "both members have to hold on the same element");
    }

    #[test]
    fn logical_expressions_compose() {
        let f = r#"{"$or": [{"a": 1}, {"b": {"$gt": 5}}]}"#;
        assert!(hits(f, json!({"a": 1})));
        assert!(hits(f, json!({"b": 9})));
        assert!(!hits(f, json!({"a": 2, "b": 1})));

        assert!(hits(r#"{"$nor": [{"a": 1}]}"#, json!({"a": 2})));
        assert!(!hits(r#"{"$nor": [{"a": 1}]}"#, json!({"a": 1})));

        // An implicit AND beside a nested one, so the two have to hold together.
        let mixed = r#"{"t": "x", "$or": [{"a": 1}, {"b": 2}]}"#;
        assert!(hits(mixed, json!({"t": "x", "b": 2})));
        assert!(!hits(mixed, json!({"t": "y", "b": 2})));

        assert!(hits(r#"{"a": {"$not": {"$gt": 5}}}"#, json!({"a": 1})));
        assert!(!hits(r#"{"a": {"$not": {"$gt": 5}}}"#, json!({"a": 9})));
        assert!(hits(r#"{"a": {"$not": {"$gt": 5}}}"#, json!({"b": 1})),
            "$not is the one operator that can hold for a document without the field");
    }

    /// The planner may only use a condition that constrains every matching document, or the rows
    /// the other branches of an `$or` match are dropped rather than merely re-read.
    #[test]
    fn only_conditions_reachable_through_and_are_conjuncts() {
        let f = parse_filter(r#"{"a": 1, "$and": [{"b": 2}], "$or": [{"c": 3}, {"d": 4}]}"#).unwrap();
        let mut paths: Vec<&str> = f.conjuncts().into_iter().map(|(p, _)| p).collect();
        paths.sort();
        assert_eq!(paths, vec!["a", "b"], "nothing under the $or is a conjunct");

        let nor = parse_filter(r#"{"$nor": [{"a": 1}]}"#).unwrap();
        assert!(nor.conjuncts().is_empty());
    }

    #[test]
    fn filter_nesting_is_bounded() {
        let mut deep = r#"{"a": 1}"#.to_string();
        for _ in 0..MAX_FILTER_DEPTH + 2 {
            deep = format!(r#"{{"$and": [{}]}}"#, deep);
        }
        assert!(parse_filter(&deep).is_err(), "recursion has to be bounded where it is parsed");
    }

    #[test]
    fn kway_merge_desc() {
        let merged = kway_merge(vec![rows("n", &[9, 3]), rows("n", &[7, 1])], &order("n:desc"), 3);
        assert_eq!(ns_of(&merged), vec![9, 7, 3]);
    }

    /// H8: a sorted page resumes from a position in the sort order, so rows that tie on the sort
    /// field have to be separated by something. Without the key the merge picks a tied row
    /// arbitrarily and the cursor built from it either repeats its twin or skips it.
    #[test]
    fn ties_on_the_sort_field_are_broken_by_key() {
        let asc = order("n");
        let tied = |key: &str| SortedRow { key: key.to_string(), value: json!({"n": 1}) };

        assert_eq!(compare_rows(&tied("a"), &tied("b"), &asc), Ordering::Less);
        assert_eq!(compare_rows(&tied("b"), &tied("a"), &asc), Ordering::Greater);
        assert_eq!(compare_rows(&tied("a"), &tied("a"), &asc), Ordering::Equal);

        // Descending applies to the field, never to the tiebreaker.
        assert_eq!(compare_rows(&tied("a"), &tied("b"), &order("n:desc")), Ordering::Less);

        let merged = kway_merge(vec![vec![tied("c")], vec![tied("a")], vec![tied("b")]], &asc, 3);
        assert_eq!(merged.iter().map(|r| r.key.clone()).collect::<Vec<_>>(), vec!["a", "b", "c"],
            "a merge of tied rows must still have one answer");
    }

    #[test]
    fn a_sort_cursor_admits_exactly_the_rows_after_it() {
        let asc = order("n");
        let at = SortCursor::at(vec![json!(2)], "k2".to_string());
        let row = |key: &str, n: i64| SortedRow { key: key.to_string(), value: json!({"n": n}) };

        assert!(!is_after(&row("k1", 1), &at, &asc));
        assert!(!is_after(&row("k2", 2), &at, &asc), "the cursor row itself is behind it");
        assert!(is_after(&row("k3", 2), &at, &asc), "a tie is separated by the key");
        assert!(!is_after(&row("k0", 2), &at, &asc), "and separated in both directions");
        assert!(is_after(&row("k0", 3), &at, &asc));

        let desc = order("n:desc");
        assert!(is_after(&row("k1", 1), &at, &desc), "descending walks the other way");
        assert!(!is_after(&row("k9", 3), &at, &desc));

        // A document without the field sorts as null, which is before every number ascending.
        let missing = SortedRow { key: "k5".to_string(), value: json!({"other": 1}) };
        assert!(!is_after(&missing, &at, &asc));
        assert!(is_after(&missing, &at, &desc));
    }

    #[test]
    fn a_sort_cursor_carries_one_position_per_key_and_reads_the_older_spelling() {
        let o = order("a:asc,b:desc");
        let row = |key: &str, a: i64, b: i64| SortedRow { key: key.to_string(), value: json!({"a": a, "b": b}) };
        let at = SortCursor::at(sort_position(&row("k", 1, 5).value, &o), "k".to_string());
        assert_eq!(at.positions(), &[json!(1), json!(5)]);

        assert!(!is_after(&row("k", 1, 5), &at, &o));
        assert!(is_after(&row("k", 1, 4), &at, &o), "the second key descends");
        assert!(is_after(&row("k", 2, 9), &at, &o));

        let decoded: SortCursor = decode_cursor(&encode_cursor(&at)).expect("round trips");
        assert_eq!(decoded.positions(), &[json!(1), json!(5)]);

        let legacy: SortCursor = decode_cursor(&encode_cursor(&serde_json::json!({"value": 7, "key": "k"})))
            .expect("a cursor issued before multi-field sort still decodes");
        assert_eq!(legacy.positions(), &[json!(7)], "read as a one-key position");
    }
}
