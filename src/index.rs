use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;

use serde_json::Value;

use crate::document::Document;

/// Total-ordering wrapper for f64 values, using `f64::total_cmp`.
#[derive(Debug, Clone)]
pub struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == std::cmp::Ordering::Equal
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Comparable key for secondary index entries.
/// Only scalar JSON values are indexable.
///
/// All numeric values (integer and float) are stored as `Number(OrderedFloat)`
/// so that `50` and `50.0` hash to the same bucket and range queries work
/// correctly across integer/float boundaries.
#[derive(Debug, Clone)]
pub enum IndexKey {
    Null,
    Bool(bool),
    Number(OrderedFloat),
    Str(String),
}

impl PartialEq for IndexKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for IndexKey {}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (IndexKey::Null, IndexKey::Null) => Ordering::Equal,
            (IndexKey::Null, _) => Ordering::Less,
            (_, IndexKey::Null) => Ordering::Greater,

            (IndexKey::Bool(a), IndexKey::Bool(b)) => a.cmp(b),
            (IndexKey::Bool(_), _) => Ordering::Less,
            (_, IndexKey::Bool(_)) => Ordering::Greater,

            (IndexKey::Number(a), IndexKey::Number(b)) => a.cmp(b),
            (IndexKey::Number(_), IndexKey::Str(_)) => Ordering::Less,
            (IndexKey::Str(_), IndexKey::Number(_)) => Ordering::Greater,

            (IndexKey::Str(a), IndexKey::Str(b)) => a.cmp(b),
        }
    }
}

impl IndexKey {
    /// Convert a serde_json::Value to an IndexKey.
    /// Returns None for objects, arrays, or other non-indexable types.
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => Some(IndexKey::Null),
            Value::Bool(b) => Some(IndexKey::Bool(*b)),
            Value::Number(n) => n.as_f64().map(|f| IndexKey::Number(OrderedFloat(f))),
            Value::String(s) => Some(IndexKey::Str(s.clone())),
            _ => None,
        }
    }
}

/// A B-tree secondary index on a single document field.
#[derive(Debug)]
pub struct SecondaryIndex {
    pub field: String,
    tree: BTreeMap<IndexKey, HashSet<String>>,
}

impl SecondaryIndex {
    pub fn new(field: String) -> Self {
        SecondaryIndex {
            field,
            tree: BTreeMap::new(),
        }
    }

    /// Build the index from an iterator of (doc_id, document) pairs.
    pub fn build_from<'a>(&mut self, docs: impl Iterator<Item = (&'a str, &'a Document)>) {
        for (id, doc) in docs {
            self.add_doc(id, doc);
        }
    }

    /// Add a document to the index.
    pub fn add_doc(&mut self, doc_id: &str, doc: &Document) {
        if let Some(key) = doc.get_field(&self.field).and_then(IndexKey::from_value) {
            self.tree
                .entry(key)
                .or_default()
                .insert(doc_id.to_string());
        }
    }

    /// Remove a document from the index.
    pub fn remove_doc(&mut self, doc_id: &str, doc: &Document) {
        if let Some(key) = doc.get_field(&self.field).and_then(IndexKey::from_value) {
            if let Some(ids) = self.tree.get_mut(&key) {
                ids.remove(doc_id);
                if ids.is_empty() {
                    self.tree.remove(&key);
                }
            }
        }
    }

    /// Look up documents with an exact key match.
    /// Returns a reference to avoid cloning the entire HashSet.
    pub fn lookup_eq(&self, key: &IndexKey) -> Option<&HashSet<String>> {
        self.tree.get(key)
    }

    /// Look up documents within a key range.
    pub fn lookup_range(
        &self,
        lower: Bound<&IndexKey>,
        upper: Bound<&IndexKey>,
    ) -> HashSet<String> {
        let mut result = HashSet::new();
        for (_, ids) in self.tree.range((lower, upper)) {
            result.extend(ids.iter().cloned());
        }
        result
    }

    /// Estimate the number of matching documents for a key (for selectivity).
    pub fn estimate_eq(&self, key: &IndexKey) -> usize {
        self.tree.get(key).map_or(0, |s| s.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_index_key_ordering() {
        assert!(IndexKey::Null < IndexKey::Bool(false));
        assert!(IndexKey::Bool(false) < IndexKey::Bool(true));
        assert!(IndexKey::Bool(true) < IndexKey::Number(OrderedFloat(0.0)));
        assert!(IndexKey::Number(OrderedFloat(0.0)) < IndexKey::Number(OrderedFloat(1.0)));
        assert!(IndexKey::Number(OrderedFloat(1.5)) < IndexKey::Str("a".into()));
    }

    #[test]
    fn test_integer_float_same_bucket() {
        // 50 (parsed as i64) and 50.0 (parsed as f64) must land in the same index entry
        let key_int = IndexKey::from_value(&json!(50)).unwrap();
        let key_float = IndexKey::from_value(&json!(50.0)).unwrap();
        assert_eq!(key_int, key_float);

        let mut idx = SecondaryIndex::new("val".into());
        let doc1 = Document::new(json!({"_id": "1", "val": 50}));
        let doc2 = Document::new(json!({"_id": "2", "val": 50.0}));
        idx.add_doc("1", &doc1);
        idx.add_doc("2", &doc2);

        // Both should be found with either key
        let result = idx.lookup_eq(&key_int).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains("1"));
        assert!(result.contains("2"));
    }

    #[test]
    fn test_cross_type_range_query() {
        let mut idx = SecondaryIndex::new("val".into());
        // Mix integers and floats
        let doc1 = Document::new(json!({"_id": "1", "val": 10}));    // integer
        let doc2 = Document::new(json!({"_id": "2", "val": 20.5}));  // float
        let doc3 = Document::new(json!({"_id": "3", "val": 30}));    // integer
        idx.add_doc("1", &doc1);
        idx.add_doc("2", &doc2);
        idx.add_doc("3", &doc3);

        // Range query with float bounds should find integer values too
        let result = idx.lookup_range(
            Bound::Included(&IndexKey::Number(OrderedFloat(15.0))),
            Bound::Included(&IndexKey::Number(OrderedFloat(30.0))),
        );
        assert_eq!(result.len(), 2);
        assert!(result.contains("2"));
        assert!(result.contains("3"));
    }

    #[test]
    fn test_index_insert_and_lookup() {
        let mut idx = SecondaryIndex::new("age".into());
        let doc1 = Document::new(json!({"_id": "1", "age": 30}));
        let doc2 = Document::new(json!({"_id": "2", "age": 25}));
        let doc3 = Document::new(json!({"_id": "3", "age": 30}));

        idx.add_doc("1", &doc1);
        idx.add_doc("2", &doc2);
        idx.add_doc("3", &doc3);

        let result = idx.lookup_eq(&IndexKey::Number(OrderedFloat(30.0))).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains("1"));
        assert!(result.contains("3"));

        let result = idx.lookup_eq(&IndexKey::Number(OrderedFloat(25.0))).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("2"));
    }

    #[test]
    fn test_index_remove() {
        let mut idx = SecondaryIndex::new("age".into());
        let doc = Document::new(json!({"_id": "1", "age": 30}));
        idx.add_doc("1", &doc);

        assert_eq!(idx.lookup_eq(&IndexKey::Number(OrderedFloat(30.0))).unwrap().len(), 1);

        idx.remove_doc("1", &doc);
        assert!(idx.lookup_eq(&IndexKey::Number(OrderedFloat(30.0))).is_none());
    }

    #[test]
    fn test_index_range_lookup() {
        let mut idx = SecondaryIndex::new("age".into());
        for i in 0..10 {
            let doc = Document::new(json!({"_id": format!("{i}"), "age": i * 10}));
            idx.add_doc(&format!("{i}"), &doc);
        }

        // age >= 50 AND age < 80
        let result = idx.lookup_range(
            Bound::Included(&IndexKey::Number(OrderedFloat(50.0))),
            Bound::Excluded(&IndexKey::Number(OrderedFloat(80.0))),
        );
        assert_eq!(result.len(), 3); // 50, 60, 70
        assert!(result.contains("5"));
        assert!(result.contains("6"));
        assert!(result.contains("7"));
    }

    #[test]
    fn test_index_build_from() {
        let docs: Vec<Document> = (0..5)
            .map(|i| Document::new(json!({"_id": format!("{i}"), "x": i})))
            .collect();

        let mut idx = SecondaryIndex::new("x".into());
        idx.build_from(docs.iter().map(|d| (d.id.as_str(), d)));

        assert_eq!(idx.lookup_eq(&IndexKey::Number(OrderedFloat(3.0))).unwrap().len(), 1);
    }

    #[test]
    fn test_non_indexable_values_skipped() {
        let mut idx = SecondaryIndex::new("data".into());
        let doc = Document::new(json!({"_id": "1", "data": {"nested": true}}));
        idx.add_doc("1", &doc);

        // Object values are not indexable, so nothing added
        assert!(idx.tree.is_empty());
    }
}
