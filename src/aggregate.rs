//! Aggregation: what a shard accumulates over its own documents, and how a router merges the
//! partials into one answer.
//!
//! Every metric travels with the counts its merge needs rather than as a finished number, because
//! an average of averages is not an average. A router sums `sum` and `count` and divides once.

use crate::json::get_path_value;
use crate::json::json_cmp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Distinct groups one aggregation may produce. The result is not paginated -- a partial grouping
/// merged across shards would be wrong, not merely short -- so the bound is a refusal.
pub const MAX_AGGREGATE_GROUPS: usize = 10_000;

/// Documents one aggregation may read on one shard before it stops, absent `max_docs`. It bounds
/// the reads rather than the groups, which are the same number only when every document is its own
/// group (IB-025).
pub const DEFAULT_AGGREGATE_SCAN: usize = 100_000;

/// Ceiling on `max_docs`. Above it the request is refused rather than clamped, so a client that
/// asked for an unbounded walk is told the walk is bounded instead of quietly getting a short one.
pub const MAX_AGGREGATE_SCAN: usize = 1_000_000;

/// Aggregation and sorted-query scans one node runs at once. A budget bounds one walk; this bounds how many walks
/// share the blocking pool with the reads and group commits that also live there.
pub const MAX_CONCURRENT_SCANS: usize = 4;

/// How long an aggregation waits for a scan slot before it is refused `429`. A waiting request
/// holds no blocking thread, so the wait costs a task; refusing is for the queue that is not moving.
pub const SCAN_ADMISSION_WAIT_MS: u64 = 2_000;

pub const MAX_AGGREGATE_METRICS: usize = 16;

pub const MAX_GROUP_FIELDS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// One requested metric. `label` is the text the client wrote, so the response is keyed by exactly
/// what was asked for.
pub struct MetricSpec {
    pub label: String,
    pub kind: MetricKind,
    pub field: Option<String>,
}

pub struct AggregateSpec {
    /// Dotted paths the rows are grouped by. Empty is one group over everything.
    pub group: Vec<String>,
    pub metrics: Vec<MetricSpec>,
}

/// One metric's partial state, which is also its published shape: `avg` is derivable from `sum`
/// and `count`, and publishing all three is what lets a client read it and a router re-merge it.
#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq)]
pub struct MetricValue {
    /// Documents this metric saw: every one in the group for `count`, and for the others only
    /// those holding a value it can use.
    pub count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avg: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<serde_json::Value>,
}

impl MetricValue {
    fn absorb(&mut self, other: &MetricValue) {
        self.count += other.count;
        if let Some(s) = other.sum {
            self.sum = Some(self.sum.unwrap_or(0.0) + s);
        }
        if let Some(v) = &other.min {
            if self.min.as_ref().is_none_or(|held| json_cmp(v, held) == std::cmp::Ordering::Less) {
                self.min = Some(v.clone());
            }
        }
        if let Some(v) = &other.max {
            if self.max.as_ref().is_none_or(|held| json_cmp(v, held) == std::cmp::Ordering::Greater) {
                self.max = Some(v.clone());
            }
        }
        // Recomputed from the merged totals, never averaged with the other side's average.
        if self.avg.is_some() || other.avg.is_some() {
            self.avg = (self.count > 0).then(|| self.sum.unwrap_or(0.0) / self.count as f64);
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AggregateGroup {
    /// One member per `group` field, omitting the ones the document does not have. Absent when the
    /// aggregation is ungrouped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<serde_json::Value>,
    /// Documents in this group, whatever any individual metric could use.
    pub count: u64,
    pub metrics: BTreeMap<String, MetricValue>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct AggregateResult {
    pub groups: Vec<AggregateGroup>,
    /// Documents the filter matched, across every group.
    pub matched: u64,
    /// Documents read to produce this, matched or not. The request's cost, which `matched` is not:
    /// a filter that selects nothing still reads everything the plan offered.
    #[serde(default)]
    pub scanned: u64,
    /// The read budget stopped the walk with keys left, so the totals cover `scanned` documents
    /// rather than the range. Set on the shard that stopped and carried by the merged answer.
    #[serde(default)]
    pub partial: bool,
}

/// `group=a,b.c`, dotted paths.
pub fn parse_group(s: Option<&str>) -> Result<Vec<String>, String> {
    let Some(s) = s else { return Ok(Vec::new()) };
    let mut fields = Vec::new();
    for part in s.split(',') {
        let field = part.trim();
        if field.is_empty() {
            return Err("group names an empty field".to_string());
        }
        if fields.iter().any(|f| f == field) {
            return Err(format!("group names `{}` twice", field));
        }
        fields.push(field.to_string());
    }
    if fields.len() > MAX_GROUP_FIELDS {
        return Err(format!("group has {} fields, more than the maximum of {}",
            fields.len(), MAX_GROUP_FIELDS));
    }
    Ok(fields)
}

/// `count`, `sum:field`, `avg:field`, `min:field`, `max:field`, comma-separated. An absent list is
/// `count`, which is the aggregate every group already carries.
pub fn parse_metrics(s: Option<&str>) -> Result<Vec<MetricSpec>, String> {
    let Some(s) = s.filter(|s| !s.trim().is_empty()) else {
        return Ok(vec![MetricSpec { label: "count".to_string(), kind: MetricKind::Count, field: None }]);
    };

    let mut metrics: Vec<MetricSpec> = Vec::new();
    for part in s.split(',') {
        let label = part.trim().to_string();
        let (name, field) = match label.split_once(':') {
            Some((n, f)) => (n, Some(f.trim().to_string())),
            None => (label.as_str(), None),
        };
        let kind = match name.trim() {
            "count" => MetricKind::Count,
            "sum" => MetricKind::Sum,
            "avg" => MetricKind::Avg,
            "min" => MetricKind::Min,
            "max" => MetricKind::Max,
            other => return Err(format!(
                "unknown metric `{}`; use count, sum:field, avg:field, min:field or max:field", other)),
        };
        match kind {
            MetricKind::Count if field.is_some() => return Err("`count` takes no field".to_string()),
            MetricKind::Count => {},
            _ if field.as_deref().is_none_or(str::is_empty) => {
                return Err(format!("`{}` requires a field", name.trim()));
            },
            _ => {},
        }
        if metrics.iter().any(|m| m.label == label) {
            return Err(format!("metric `{}` is asked for twice", label));
        }
        metrics.push(MetricSpec { label, kind, field });
    }

    if metrics.len() > MAX_AGGREGATE_METRICS {
        return Err(format!("aggregation asks for {} metrics, more than the maximum of {}",
            metrics.len(), MAX_AGGREGATE_METRICS));
    }
    Ok(metrics)
}

/// The message a group ceiling produces, on the shard and at the router alike.
pub fn too_many_groups() -> String {
    format!("aggregation produced more than {} groups; narrow it with `filter` or fewer `group` fields",
        MAX_AGGREGATE_GROUPS)
}

/// The message a spent read budget produces. Both remedies are named because they are different
/// requests: a narrower one reads less, and `partial=true` accepts what this budget bought.
pub fn budget_spent(budget: usize) -> String {
    format!("aggregation read its budget of {} documents on one shard without finishing; \
narrow it with `filter`, `start`/`end` or an indexed field, raise `max_docs` up to {}, or ask \
for `partial=true` to accept the totals over what was read",
        budget, MAX_AGGREGATE_SCAN)
}

struct Bucket {
    key: Option<serde_json::Value>,
    count: u64,
    metrics: Vec<MetricValue>,
}

/// Accumulates one shard's documents. Buckets are held under the canonical JSON text of their key,
/// which orders the output the same way on every shard because `serde_json::Map` is sorted.
pub struct Aggregator {
    spec: AggregateSpec,
    buckets: BTreeMap<String, Bucket>,
    matched: u64,
}

impl Aggregator {
    pub fn new(spec: AggregateSpec) -> Self {
        Self { spec, buckets: BTreeMap::new(), matched: 0 }
    }

    pub fn add(&mut self, doc: &serde_json::Value) -> Result<(), String> {
        self.matched += 1;
        let key = group_key(doc, &self.spec.group);
        let slot = serde_json::to_string(&key).unwrap_or_default();

        if !self.buckets.contains_key(&slot) {
            if self.buckets.len() >= MAX_AGGREGATE_GROUPS {
                return Err(too_many_groups());
            }
            self.buckets.insert(slot.clone(), Bucket {
                key,
                count: 0,
                metrics: vec![MetricValue::default(); self.spec.metrics.len()],
            });
        }

        let bucket = self.buckets.get_mut(&slot).expect("just inserted");
        bucket.count += 1;
        for (spec, held) in self.spec.metrics.iter().zip(bucket.metrics.iter_mut()) {
            accumulate(spec, held, doc);
        }
        Ok(())
    }

    /// `scanned` and `partial` come from the walk rather than from the fold: an aggregator is told
    /// about the documents that matched, and the reads behind them are the caller's count.
    pub fn finish(self, scanned: u64, partial: bool) -> AggregateResult {
        let labels: Vec<&str> = self.spec.metrics.iter().map(|m| m.label.as_str()).collect();
        let groups = self.buckets.into_values().map(|b| AggregateGroup {
            key: b.key,
            count: b.count,
            metrics: labels.iter().zip(b.metrics)
                .map(|(label, mut v)| {
                    if v.avg.is_some() {
                        v.avg = (v.count > 0).then(|| v.sum.unwrap_or(0.0) / v.count as f64);
                    }
                    (label.to_string(), v)
                })
                .collect(),
        }).collect();
        AggregateResult { groups, matched: self.matched, scanned, partial }
    }
}

fn group_key(doc: &serde_json::Value, fields: &[String]) -> Option<serde_json::Value> {
    if fields.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for field in fields {
        if let Some(v) = get_path_value(doc, field) {
            map.insert(field.clone(), v.clone());
        }
    }
    Some(serde_json::Value::Object(map))
}

fn accumulate(spec: &MetricSpec, held: &mut MetricValue, doc: &serde_json::Value) {
    let value = match &spec.field {
        None => {
            held.count += 1;
            return;
        },
        Some(field) => match get_path_value(doc, field) {
            Some(v) => v,
            None => return,
        },
    };

    match spec.kind {
        // Numeric, so a document holding a string there is not counted rather than counted as zero.
        MetricKind::Sum | MetricKind::Avg => {
            let Some(n) = value.as_f64() else { return };
            held.count += 1;
            held.sum = Some(held.sum.unwrap_or(0.0) + n);
            if spec.kind == MetricKind::Avg {
                held.avg = Some(0.0);
            }
        },
        // Any JSON value, ordered the way sorting orders one.
        MetricKind::Min => {
            held.count += 1;
            if held.min.as_ref().is_none_or(|m| json_cmp(value, m) == std::cmp::Ordering::Less) {
                held.min = Some(value.clone());
            }
        },
        MetricKind::Max => {
            held.count += 1;
            if held.max.as_ref().is_none_or(|m| json_cmp(value, m) == std::cmp::Ordering::Greater) {
                held.max = Some(value.clone());
            }
        },
        MetricKind::Count => held.count += 1,
    }
}

/// Merges shard partials into one result. Groups are matched by their key, so a group split across
/// shards is one group here and the counts behind an average are summed before it is taken.
pub fn merge(parts: Vec<AggregateResult>) -> Result<AggregateResult, String> {
    let mut buckets: BTreeMap<String, AggregateGroup> = BTreeMap::new();
    let mut matched = 0u64;
    let mut scanned = 0u64;
    // One shard stopping short makes the whole answer partial: the merge cannot tell which groups
    // the unread keys belonged to, so no group is known complete.
    let mut partial = false;

    for part in parts {
        matched += part.matched;
        scanned += part.scanned;
        partial |= part.partial;
        for group in part.groups {
            let slot = serde_json::to_string(&group.key).unwrap_or_default();
            match buckets.get_mut(&slot) {
                Some(held) => {
                    held.count += group.count;
                    for (label, value) in &group.metrics {
                        held.metrics.entry(label.clone()).or_default().absorb(value);
                    }
                },
                None => {
                    if buckets.len() >= MAX_AGGREGATE_GROUPS {
                        return Err(too_many_groups());
                    }
                    buckets.insert(slot, group);
                },
            }
        }
    }

    Ok(AggregateResult { groups: buckets.into_values().collect(), matched, scanned, partial })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(group: &str, metrics: &str) -> AggregateSpec {
        AggregateSpec {
            group: parse_group(Some(group).filter(|g| !g.is_empty())).unwrap(),
            metrics: parse_metrics(Some(metrics)).unwrap(),
        }
    }

    fn run(group: &str, metrics: &str, docs: &[serde_json::Value]) -> AggregateResult {
        let mut agg = Aggregator::new(spec(group, metrics));
        for doc in docs {
            agg.add(doc).expect("within the group ceiling");
        }
        agg.finish(docs.len() as u64, false)
    }

    fn only(result: &AggregateResult, label: &str) -> MetricValue {
        result.groups[0].metrics.get(label).cloned().expect("metric present")
    }

    #[test]
    fn an_ungrouped_aggregation_is_one_group_with_no_key() {
        let r = run("", "count,sum:n,avg:n,min:n,max:n",
            &[json!({"n": 1}), json!({"n": 5}), json!({"n": 3})]);

        assert_eq!(r.groups.len(), 1);
        assert!(r.groups[0].key.is_none());
        assert_eq!(r.groups[0].count, 3);
        assert_eq!(r.matched, 3);
        assert_eq!(only(&r, "count").count, 3);
        assert_eq!(only(&r, "sum:n").sum, Some(9.0));
        assert_eq!(only(&r, "avg:n").avg, Some(3.0));
        assert_eq!(only(&r, "min:n").min, Some(json!(1)));
        assert_eq!(only(&r, "max:n").max, Some(json!(5)));
    }

    #[test]
    fn a_metric_counts_only_the_documents_it_could_use() {
        let r = run("", "count,sum:n", &[json!({"n": 2}), json!({"n": "x"}), json!({"other": 1})]);

        assert_eq!(r.groups[0].count, 3, "the group holds every matching document");
        assert_eq!(only(&r, "sum:n").sum, Some(2.0));
        assert_eq!(only(&r, "sum:n").count, 1, "a string and a missing field are not zero");
    }

    #[test]
    fn grouping_keys_on_the_named_paths_and_omits_the_ones_a_document_lacks() {
        let r = run("tier", "count", &[
            json!({"tier": "gold"}), json!({"tier": "silver"}), json!({"tier": "gold"}),
            json!({"other": 1}),
        ]);

        let by_key: Vec<(String, u64)> = r.groups.iter()
            .map(|g| (serde_json::to_string(&g.key).unwrap(), g.count)).collect();
        assert_eq!(by_key, vec![
            (r#"{"tier":"gold"}"#.to_string(), 2),
            (r#"{"tier":"silver"}"#.to_string(), 1),
            (r#"{}"#.to_string(), 1),
        ], "a document without the path groups with the others that lack it");
    }

    /// The whole reason a metric travels with its count: averaging the shard averages gives 3.5
    /// here, and the answer is 3.
    #[test]
    fn averages_merge_through_their_totals_not_through_each_other() {
        let left = run("", "avg:n", &[json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]);
        let right = run("", "avg:n", &[json!({"n": 6})]);

        let merged = merge(vec![left, right]).expect("within the ceiling");
        assert_eq!(merged.groups.len(), 1);
        assert_eq!(merged.matched, 4);
        assert_eq!(only(&merged, "avg:n").avg, Some(3.0));
        assert_eq!(only(&merged, "avg:n").count, 4);
    }

    #[test]
    fn a_group_split_across_shards_merges_into_one() {
        let left = run("tier", "count,sum:n,min:n,max:n", &[json!({"tier": "gold", "n": 4})]);
        let right = run("tier", "count,sum:n,min:n,max:n",
            &[json!({"tier": "gold", "n": 1}), json!({"tier": "silver", "n": 9})]);

        let merged = merge(vec![left, right]).unwrap();
        assert_eq!(merged.groups.len(), 2);

        let gold = merged.groups.iter().find(|g| g.key == Some(json!({"tier": "gold"}))).unwrap();
        assert_eq!(gold.count, 2);
        assert_eq!(gold.metrics["sum:n"].sum, Some(5.0));
        assert_eq!(gold.metrics["min:n"].min, Some(json!(1)));
        assert_eq!(gold.metrics["max:n"].max, Some(json!(4)));
    }

    #[test]
    fn merging_is_order_independent() {
        let build = || (
            run("t", "avg:n,min:n", &[json!({"t": "a", "n": 1}), json!({"t": "b", "n": 8})]),
            run("t", "avg:n,min:n", &[json!({"t": "a", "n": 3})]),
            run("t", "avg:n,min:n", &[json!({"t": "b", "n": 2})]),
        );
        let (a, b, c) = build();
        let forward = serde_json::to_value(merge(vec![a, b, c]).unwrap()).unwrap();
        let (a, b, c) = build();
        let backward = serde_json::to_value(merge(vec![c, b, a]).unwrap()).unwrap();
        assert_eq!(forward, backward, "shards answer in whatever order they finish in");
    }

    #[test]
    fn the_group_ceiling_refuses_rather_than_truncating() {
        let mut agg = Aggregator::new(spec("k", "count"));
        for i in 0..MAX_AGGREGATE_GROUPS {
            agg.add(&json!({"k": i})).expect("under the ceiling");
        }
        assert!(agg.add(&json!({"k": "one too many"})).is_err(),
            "a truncated grouping merged across shards is wrong, not short");
        assert!(agg.add(&json!({"k": 0})).is_ok(), "an existing group still absorbs its rows");
    }

    #[test]
    fn metric_and_group_syntax_is_checked() {
        assert!(parse_metrics(Some("total")).is_err(), "unknown metric");
        assert!(parse_metrics(Some("count:n")).is_err(), "count takes no field");
        assert!(parse_metrics(Some("sum")).is_err(), "sum needs one");
        assert!(parse_metrics(Some("sum:")).is_err());
        assert!(parse_metrics(Some("sum:n,sum:n")).is_err(), "a duplicate label would collide");
        assert!(parse_metrics(Some("sum:n,avg:n")).is_ok(), "different labels on one field are fine");
        assert_eq!(parse_metrics(None).unwrap().len(), 1, "count is the default");

        assert!(parse_group(Some("a,,b")).is_err());
        assert!(parse_group(Some("a,a")).is_err());
        assert_eq!(parse_group(None).unwrap().len(), 0);
        let many: Vec<String> = (0..=MAX_GROUP_FIELDS).map(|i| format!("f{}", i)).collect();
        assert!(parse_group(Some(&many.join(","))).is_err(), "the field count is bounded");
    }

    #[test]
    fn a_partial_round_trips_through_json() {
        let r = run("t", "avg:n", &[json!({"t": "a", "n": 2})]);
        let wire = serde_json::to_string(&r).unwrap();
        let back: AggregateResult = serde_json::from_str(&wire).unwrap();
        assert_eq!(back.groups[0].metrics["avg:n"], only(&r, "avg:n"));
        assert!(wire.contains("\"sum\""), "the merge needs the total, so it is on the wire");
    }
}
