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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Null,
    Bool(bool),
    Integer(i64),
    Float(OrderedFloat),
    Str(String),
}

impl IndexKey {
    /// Convert a serde_json::Value to an IndexKey.
    /// Returns None for objects, arrays, or other non-indexable types.
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => Some(IndexKey::Null),
            Value::Bool(b) => Some(IndexKey::Bool(*b)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(IndexKey::Integer(i))
                } else if let Some(f) = n.as_f64() {
                    Some(IndexKey::Float(OrderedFloat(f)))
                } else {
                    None
                }
            }
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
    pub fn lookup_eq(&self, key: &IndexKey) -> HashSet<String> {
        self.tree.get(key).cloned().unwrap_or_default()
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_index_key_ordering() {
        assert!(IndexKey::Null < IndexKey::Bool(false));
        assert!(IndexKey::Bool(false) < IndexKey::Bool(true));
        assert!(IndexKey::Bool(true) < IndexKey::Integer(0));
        assert!(IndexKey::Integer(0) < IndexKey::Integer(1));
        assert!(IndexKey::Integer(1) < IndexKey::Float(OrderedFloat(1.5)));
        assert!(IndexKey::Float(OrderedFloat(1.5)) < IndexKey::Str("a".into()));
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

        let result = idx.lookup_eq(&IndexKey::Integer(30));
        assert_eq!(result.len(), 2);
        assert!(result.contains("1"));
        assert!(result.contains("3"));

        let result = idx.lookup_eq(&IndexKey::Integer(25));
        assert_eq!(result.len(), 1);
        assert!(result.contains("2"));
    }

    #[test]
    fn test_index_remove() {
        let mut idx = SecondaryIndex::new("age".into());
        let doc = Document::new(json!({"_id": "1", "age": 30}));
        idx.add_doc("1", &doc);

        assert_eq!(idx.lookup_eq(&IndexKey::Integer(30)).len(), 1);

        idx.remove_doc("1", &doc);
        assert_eq!(idx.lookup_eq(&IndexKey::Integer(30)).len(), 0);
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
            Bound::Included(&IndexKey::Integer(50)),
            Bound::Excluded(&IndexKey::Integer(80)),
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

        assert_eq!(idx.lookup_eq(&IndexKey::Integer(3)).len(), 1);
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
