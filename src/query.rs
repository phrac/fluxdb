use serde_json::Value;

use crate::document::Document;
use crate::error::{FluxError, Result};

/// Evaluate whether a document matches a query filter.
///
/// Query format (MongoDB-like):
/// ```json
/// {
///   "field": "value",                    // equality
///   "field": { "$gt": 10 },             // comparison
///   "field": { "$in": [1, 2, 3] },      // membership
///   "$and": [ ... ],                     // logical AND
///   "$or": [ ... ],                      // logical OR
///   "$not": { ... }                      // logical NOT
/// }
/// ```
///
/// Supported operators:
/// - `$eq`, `$ne`: equality / inequality
/// - `$gt`, `$gte`, `$lt`, `$lte`: numeric/string comparison
/// - `$in`, `$nin`: membership in array
/// - `$exists`: field existence
/// - `$and`, `$or`, `$not`: logical operators
pub fn matches_filter(doc: &Document, filter: &Value) -> Result<bool> {
    let filter_obj = filter
        .as_object()
        .ok_or_else(|| FluxError::InvalidQuery("filter must be a JSON object".into()))?;

    if filter_obj.is_empty() {
        return Ok(true);
    }

    for (key, condition) in filter_obj {
        match key.as_str() {
            "$and" => {
                let conditions = condition.as_array().ok_or_else(|| {
                    FluxError::InvalidQuery("$and must be an array".into())
                })?;
                for cond in conditions {
                    if !matches_filter(doc, cond)? {
                        return Ok(false);
                    }
                }
            }
            "$or" => {
                let conditions = condition.as_array().ok_or_else(|| {
                    FluxError::InvalidQuery("$or must be an array".into())
                })?;
                let mut any_match = false;
                for cond in conditions {
                    if matches_filter(doc, cond)? {
                        any_match = true;
                        break;
                    }
                }
                if !any_match {
                    return Ok(false);
                }
            }
            "$not" => {
                if matches_filter(doc, condition)? {
                    return Ok(false);
                }
            }
            field => {
                let doc_value = doc.get_field(field);
                if !match_field_condition(&doc_value, condition)? {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

fn match_field_condition(doc_value: &Option<&Value>, condition: &Value) -> Result<bool> {
    // If condition is an object with operator keys, evaluate operators
    if let Some(cond_obj) = condition.as_object() {
        if cond_obj.keys().any(|k| k.starts_with('$')) {
            for (op, op_value) in cond_obj {
                if !evaluate_operator(doc_value, op, op_value)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
    }

    // Otherwise, it's an implicit $eq
    match doc_value {
        Some(v) => Ok(values_equal(v, condition)),
        None => Ok(condition.is_null()),
    }
}

fn evaluate_operator(doc_value: &Option<&Value>, op: &str, op_value: &Value) -> Result<bool> {
    match op {
        "$eq" => match doc_value {
            Some(v) => Ok(values_equal(v, op_value)),
            None => Ok(op_value.is_null()),
        },
        "$ne" => match doc_value {
            Some(v) => Ok(!values_equal(v, op_value)),
            None => Ok(!op_value.is_null()),
        },
        "$gt" => Ok(compare_values(doc_value, op_value)
            .map(|ord| ord == std::cmp::Ordering::Greater)
            .unwrap_or(false)),
        "$gte" => Ok(compare_values(doc_value, op_value)
            .map(|ord| ord != std::cmp::Ordering::Less)
            .unwrap_or(false)),
        "$lt" => Ok(compare_values(doc_value, op_value)
            .map(|ord| ord == std::cmp::Ordering::Less)
            .unwrap_or(false)),
        "$lte" => Ok(compare_values(doc_value, op_value)
            .map(|ord| ord != std::cmp::Ordering::Greater)
            .unwrap_or(false)),
        "$in" => {
            let arr = op_value
                .as_array()
                .ok_or_else(|| FluxError::InvalidQuery("$in requires an array".into()))?;
            match doc_value {
                Some(v) => Ok(arr.iter().any(|item| values_equal(v, item))),
                None => Ok(arr.iter().any(|item| item.is_null())),
            }
        }
        "$nin" => {
            let arr = op_value
                .as_array()
                .ok_or_else(|| FluxError::InvalidQuery("$nin requires an array".into()))?;
            match doc_value {
                Some(v) => Ok(!arr.iter().any(|item| values_equal(v, item))),
                None => Ok(!arr.iter().any(|item| item.is_null())),
            }
        }
        "$exists" => {
            let should_exist = op_value.as_bool().ok_or_else(|| {
                FluxError::InvalidQuery("$exists requires a boolean".into())
            })?;
            Ok(doc_value.is_some() == should_exist)
        }
        other => Err(FluxError::InvalidQuery(format!(
            "unknown operator: {other}"
        ))),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    // Handle numeric comparison: treat integers and floats as comparable
    match (a, b) {
        (Value::Number(na), Value::Number(nb)) => {
            let fa = na.as_f64();
            let fb = nb.as_f64();
            match (fa, fb) {
                (Some(a), Some(b)) => a == b,
                _ => a == b,
            }
        }
        _ => a == b,
    }
}

fn compare_values(doc_value: &Option<&Value>, target: &Value) -> Option<std::cmp::Ordering> {
    let dv = (*doc_value)?;
    match (dv, target) {
        (Value::Number(a), Value::Number(b)) => {
            let fa = a.as_f64()?;
            let fb = b.as_f64()?;
            fa.partial_cmp(&fb)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Apply projection to a document's output value.
/// Projection is an object where keys map to 1 (include) or 0 (exclude).
/// You cannot mix include and exclude (except _id can always be excluded).
pub fn apply_projection(doc_value: &mut Value, projection: &Value) -> Result<()> {
    let proj_obj = match projection.as_object() {
        Some(obj) if !obj.is_empty() => obj,
        _ => return Ok(()), // no projection = return everything
    };

    let doc_obj = match doc_value.as_object_mut() {
        Some(obj) => obj,
        None => return Ok(()),
    };

    // Determine if this is an inclusion or exclusion projection
    let mut has_include = false;
    let mut has_exclude = false;
    for (key, val) in proj_obj {
        if key == "_id" {
            continue; // _id exclusion is always allowed
        }
        match val.as_i64() {
            Some(1) => has_include = true,
            Some(0) => has_exclude = true,
            _ => {
                return Err(FluxError::InvalidQuery(
                    "projection values must be 0 or 1".into(),
                ))
            }
        }
    }

    if has_include && has_exclude {
        return Err(FluxError::InvalidQuery(
            "cannot mix inclusion and exclusion in projection".into(),
        ));
    }

    if has_include {
        // Include mode: only keep specified fields (+ _id unless excluded)
        let include_id = proj_obj.get("_id").and_then(|v| v.as_i64()) != Some(0);
        let id_val = doc_obj.get("_id").cloned();

        let keys_to_keep: Vec<String> = proj_obj
            .iter()
            .filter(|(_, v)| v.as_i64() == Some(1))
            .map(|(k, _)| k.clone())
            .collect();

        doc_obj.retain(|k, _| keys_to_keep.contains(k));

        if include_id {
            if let Some(id) = id_val {
                doc_obj.insert("_id".to_string(), id);
            }
        }
    } else {
        // Exclude mode: remove specified fields
        for (key, val) in proj_obj {
            if val.as_i64() == Some(0) {
                doc_obj.remove(key);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::Document;
    use serde_json::json;

    fn make_doc(val: Value) -> Document {
        Document::new(val)
    }

    #[test]
    fn test_equality_match() {
        let doc = make_doc(json!({"name": "Alice", "age": 30}));
        assert!(matches_filter(&doc, &json!({"name": "Alice"})).unwrap());
        assert!(!matches_filter(&doc, &json!({"name": "Bob"})).unwrap());
    }

    #[test]
    fn test_comparison_operators() {
        let doc = make_doc(json!({"age": 30}));
        assert!(matches_filter(&doc, &json!({"age": {"$gt": 25}})).unwrap());
        assert!(!matches_filter(&doc, &json!({"age": {"$gt": 35}})).unwrap());
        assert!(matches_filter(&doc, &json!({"age": {"$gte": 30}})).unwrap());
        assert!(matches_filter(&doc, &json!({"age": {"$lt": 35}})).unwrap());
        assert!(matches_filter(&doc, &json!({"age": {"$lte": 30}})).unwrap());
    }

    #[test]
    fn test_in_operator() {
        let doc = make_doc(json!({"status": "active"}));
        assert!(matches_filter(&doc, &json!({"status": {"$in": ["active", "pending"]}})).unwrap());
        assert!(
            !matches_filter(&doc, &json!({"status": {"$in": ["inactive", "pending"]}})).unwrap()
        );
    }

    #[test]
    fn test_exists_operator() {
        let doc = make_doc(json!({"name": "Alice"}));
        assert!(matches_filter(&doc, &json!({"name": {"$exists": true}})).unwrap());
        assert!(!matches_filter(&doc, &json!({"email": {"$exists": true}})).unwrap());
        assert!(matches_filter(&doc, &json!({"email": {"$exists": false}})).unwrap());
    }

    #[test]
    fn test_logical_and() {
        let doc = make_doc(json!({"name": "Alice", "age": 30}));
        assert!(matches_filter(
            &doc,
            &json!({"$and": [{"name": "Alice"}, {"age": {"$gte": 25}}]})
        )
        .unwrap());
        assert!(!matches_filter(
            &doc,
            &json!({"$and": [{"name": "Alice"}, {"age": {"$gte": 35}}]})
        )
        .unwrap());
    }

    #[test]
    fn test_logical_or() {
        let doc = make_doc(json!({"age": 30}));
        assert!(
            matches_filter(&doc, &json!({"$or": [{"age": 30}, {"age": 40}]})).unwrap()
        );
        assert!(
            !matches_filter(&doc, &json!({"$or": [{"age": 20}, {"age": 40}]})).unwrap()
        );
    }

    #[test]
    fn test_logical_not() {
        let doc = make_doc(json!({"age": 30}));
        assert!(matches_filter(&doc, &json!({"$not": {"age": 40}})).unwrap());
        assert!(!matches_filter(&doc, &json!({"$not": {"age": 30}})).unwrap());
    }

    #[test]
    fn test_nested_field_query() {
        let doc = make_doc(json!({"address": {"city": "Portland"}}));
        assert!(matches_filter(&doc, &json!({"address.city": "Portland"})).unwrap());
        assert!(!matches_filter(&doc, &json!({"address.city": "Seattle"})).unwrap());
    }

    #[test]
    fn test_projection_include() {
        let doc = make_doc(json!({"_id": "1", "name": "Alice", "age": 30, "email": "a@b.com"}));
        let mut val = doc.to_value();
        apply_projection(&mut val, &json!({"name": 1, "age": 1})).unwrap();
        assert!(val.get("_id").is_some());
        assert!(val.get("name").is_some());
        assert!(val.get("age").is_some());
        assert!(val.get("email").is_none());
    }

    #[test]
    fn test_projection_exclude() {
        let doc = make_doc(json!({"_id": "1", "name": "Alice", "age": 30, "email": "a@b.com"}));
        let mut val = doc.to_value();
        apply_projection(&mut val, &json!({"email": 0})).unwrap();
        assert!(val.get("name").is_some());
        assert!(val.get("email").is_none());
    }

    #[test]
    fn test_id_query() {
        let doc = Document::new(json!({"_id": "abc123", "name": "Alice"}));
        assert!(matches_filter(&doc, &json!({"_id": "abc123"})).unwrap());
        assert!(!matches_filter(&doc, &json!({"_id": "wrong"})).unwrap());
    }
}
