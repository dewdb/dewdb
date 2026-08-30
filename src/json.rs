//! Pure serde_json value operations used by the query and write paths.

pub fn get_path_value<'a>(doc: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut cur = doc;

    for part in path.split('.') {
        cur = cur.get(part)?;
    }

    Some(cur)
}

pub fn parse_fields(s: Option<&str>) -> Vec<String> {
    match s {
        Some(s) => s.split(',').map(|f| f.trim().to_string()).filter(|f| !f.is_empty()).collect(),
        None => Vec::new(),
    }
}

fn type_rank(v: &serde_json::Value) -> u8 {
    match v {
        serde_json::Value::Null => 0,
        serde_json::Value::Bool(_) => 1,
        serde_json::Value::Number(_) => 2,
        serde_json::Value::String(_) => 3,
        serde_json::Value::Array(_) => 4,
        serde_json::Value::Object(_) => 5,
    }
}

// Total order across types is required: shards sort locally and the router merges their pages.
pub fn json_cmp(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value;
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(_), Value::Number(_)) => {
            let x = a.as_f64().unwrap_or(f64::NAN);
            let y = b.as_f64().unwrap_or(f64::NAN);
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        },
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => {
            for (ex, ey) in x.iter().zip(y.iter()) {
                let o = json_cmp(ex, ey);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        },
        // Entry-wise over `serde_json::Map`, which is a BTreeMap: iteration is key-sorted, so two
        // objects with the same members compare the same however their JSON text was ordered.
        (Value::Object(x), Value::Object(y)) => {
            for ((kx, vx), (ky, vy)) in x.iter().zip(y.iter()) {
                let o = kx.cmp(ky).then_with(|| json_cmp(vx, vy));
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        },
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

fn set_path(map: &mut serde_json::Map<String, serde_json::Value>, parts: &[&str], val: serde_json::Value) {
    if parts.is_empty() {
        return;
    }
    if parts.len() == 1 {
        map.insert(parts[0].to_string(), val);
        return;
    }
    let entry = map.entry(parts[0].to_string()).or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    if !entry.is_object() {
        *entry = serde_json::Value::Object(serde_json::Map::new());
    }
    if let Some(m) = entry.as_object_mut() {
        set_path(m, &parts[1..], val);
    }
}

pub fn project(doc: &serde_json::Value, fields: &[String]) -> serde_json::Value {
    if fields.is_empty() {
        return doc.clone();
    }
    let mut out = serde_json::Map::new();
    for path in fields {
        if let Some(v) = get_path_value(doc, path) {
            let parts: Vec<&str> = path.split('.').collect();
            set_path(&mut out, &parts, v.clone());
        }
    }
    serde_json::Value::Object(out)
}

pub fn merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    match patch {
        serde_json::Value::Object(pmap) => {
            if !target.is_object() {
                *target = serde_json::Value::Object(serde_json::Map::new());
            }
            let tmap = target.as_object_mut().unwrap();
            for (k, v) in pmap {
                if v.is_null() {
                    tmap.remove(k);
                } else if let Some(existing) = tmap.get_mut(k) {
                    merge_patch(existing, v);
                } else {
                    let mut fresh = serde_json::Value::Null;
                    merge_patch(&mut fresh, v);
                    tmap.insert(k.clone(), fresh);
                }
            }
        },
        other => *target = other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_cmp_orders_across_and_within_types() {
        use std::cmp::Ordering;
        use serde_json::json;
        assert_eq!(json_cmp(&json!(1), &json!(2)), Ordering::Less);
        assert_eq!(json_cmp(&json!("b"), &json!("a")), Ordering::Greater);
        assert_eq!(json_cmp(&json!(false), &json!(true)), Ordering::Less);
        assert_eq!(json_cmp(&json!(null), &json!(0)), Ordering::Less, "null sorts before numbers");
        assert_eq!(json_cmp(&json!(5), &json!("5")), Ordering::Less, "numbers sort before strings");
    }

    /// M2: `x.len().cmp(&y.len())` called every same-sized object equal, so a sort on an
    /// object-valued field ordered each shard's page differently and the router merged them wrong.
    #[test]
    fn objects_have_a_total_order_not_a_size_comparison() {
        use std::cmp::Ordering;
        use serde_json::json;

        assert_eq!(json_cmp(&json!({"a": 1}), &json!({"a": 2})), Ordering::Less, "same key, ordered value");
        assert_eq!(json_cmp(&json!({"a": 1}), &json!({"b": 1})), Ordering::Less, "keys decide before values");
        assert_eq!(json_cmp(&json!({"a": 1}), &json!({"a": 1})), Ordering::Equal);
        assert_eq!(json_cmp(&json!({"a": 1}), &json!({"a": 1, "b": 2})), Ordering::Less, "a prefix sorts first");
        assert_eq!(json_cmp(&json!({"a": {"n": 2}}), &json!({"a": {"n": 1}})), Ordering::Greater, "recurses");

        // Written in opposite key orders: the map is sorted, so the comparison cannot see that.
        let a: serde_json::Value = serde_json::from_str(r#"{"x": 1, "y": 2}"#).unwrap();
        let b: serde_json::Value = serde_json::from_str(r#"{"y": 2, "x": 1}"#).unwrap();
        assert_eq!(json_cmp(&a, &b), Ordering::Equal, "shards must agree whatever the client sent");
    }

    /// A merge across shards is only defined if the order is one: no ties between unequal values,
    /// and antisymmetric and transitive across every pair.
    #[test]
    fn json_cmp_is_a_total_order_over_a_mixed_set() {
        use std::cmp::Ordering;
        use serde_json::json;

        let values = vec![
            json!(null), json!(false), json!(true), json!(-1), json!(0), json!(1),
            json!(""), json!("a"), json!("b"),
            json!([]), json!([1]), json!([1, 2]), json!([2]),
            json!({}), json!({"a": 1}), json!({"a": 2}), json!({"a": 1, "b": 1}), json!({"b": 1}),
        ];

        for a in &values {
            for b in &values {
                assert_eq!(json_cmp(a, b).reverse(), json_cmp(b, a), "antisymmetry: {} vs {}", a, b);
                if a != b {
                    assert_ne!(json_cmp(a, b), Ordering::Equal, "distinct values tied: {} vs {}", a, b);
                }
                for c in &values {
                    if json_cmp(a, b) != Ordering::Greater && json_cmp(b, c) != Ordering::Greater {
                        assert_ne!(json_cmp(a, c), Ordering::Greater,
                            "transitivity: {} <= {} <= {} but not {} <= {}", a, b, c, a, c);
                    }
                }
            }
        }

        let mut sorted = values.clone();
        sorted.sort_by(json_cmp);
        let mut reversed = values;
        reversed.reverse();
        reversed.sort_by(json_cmp);
        assert_eq!(sorted, reversed, "the sort must not depend on the input order");
    }

    #[test]
    fn projection_keeps_only_requested_paths() {
        use serde_json::json;
        let doc = json!({"a": 1, "b": {"c": 2, "d": 3}, "e": 4});

        let flat = project(&doc, &["a".to_string(), "e".to_string()]);
        assert_eq!(flat, json!({"a": 1, "e": 4}));

        let nested = project(&doc, &["b.c".to_string()]);
        assert_eq!(nested, json!({"b": {"c": 2}}), "nested path projects into nested object");

        let missing = project(&doc, &["a".to_string(), "zzz".to_string()]);
        assert_eq!(missing, json!({"a": 1}), "missing fields are omitted");

        let empty = project(&doc, &[]);
        assert_eq!(empty, doc, "empty projection returns the full doc");
    }

    #[test]
    fn merge_patch_follows_rfc7386() {
        use serde_json::json;

        let mut basic = json!({"a": "b", "c": {"d": "e", "f": "g"}});
        merge_patch(&mut basic, &json!({"a": "z", "c": {"f": null}}));
        assert_eq!(basic, json!({"a": "z", "c": {"d": "e"}}), "null removes a member, siblings survive");

        let mut arrays = json!({"a": [1, 2, 3]});
        merge_patch(&mut arrays, &json!({"a": [4]}));
        assert_eq!(arrays, json!({"a": [4]}), "arrays are replaced wholesale, never element-merged");

        let mut scalar_to_object = json!({"a": "flat"});
        merge_patch(&mut scalar_to_object, &json!({"a": {"b": 1}}));
        assert_eq!(scalar_to_object, json!({"a": {"b": 1}}), "object patch overwrites a scalar");

        let mut created = json!({});
        merge_patch(&mut created, &json!({"a": {"b": 1, "c": null}}));
        assert_eq!(created, json!({"a": {"b": 1}}), "nulls are dropped while creating a new member");

        let mut whole = json!({"a": 1});
        merge_patch(&mut whole, &json!("replaced"));
        assert_eq!(whole, json!("replaced"), "a non-object patch replaces the whole target");

        let mut deep = json!({"x": {"y": {"z": 1, "keep": true}}});
        merge_patch(&mut deep, &json!({"x": {"y": {"z": 2}}}));
        assert_eq!(deep, json!({"x": {"y": {"z": 2, "keep": true}}}), "deep merge preserves untouched siblings");

        let mut absent_delete = json!({"a": 1});
        merge_patch(&mut absent_delete, &json!({"missing": null}));
        assert_eq!(absent_delete, json!({"a": 1}), "deleting an absent member is a no-op");

        let mut noop = json!({"a": 1});
        merge_patch(&mut noop, &json!({}));
        assert_eq!(noop, json!({"a": 1}), "an empty patch changes nothing");
    }

    #[test]
    fn merge_patch_never_leaves_null_placeholders_in_new_subtrees() {
        use serde_json::json;
        let mut doc = json!({"keep": 1});
        merge_patch(&mut doc, &json!({"new": {"deep": {"k": "v", "gone": null}}}));
        assert_eq!(doc, json!({"keep": 1, "new": {"deep": {"k": "v"}}}));
    }
}
