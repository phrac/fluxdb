use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use serde_json::Value;

use crate::document::Document;
use crate::error::{FluxError, Result};
use crate::index::{IndexKey, SecondaryIndex};
use crate::query::{apply_projection, matches_filter};

/// A collection is a named group of documents, analogous to a table in SQL.
#[derive(Debug)]
pub struct Collection {
    pub name: String,
    documents: HashMap<String, Document>,
    indexes: HashMap<String, SecondaryIndex>,
}

impl Collection {
    pub fn new(name: String) -> Self {
        Collection {
            name,
            documents: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    /// Insert a document. Returns the document's ID.
    pub fn insert(&mut self, doc: Document) -> String {
        let id = doc.id.clone();
        for index in self.indexes.values_mut() {
            index.add_doc(&id, &doc);
        }
        self.documents.insert(id.clone(), doc);
        id
    }

    /// Get a document by ID.
    pub fn get(&self, id: &str) -> Result<&Document> {
        self.documents
            .get(id)
            .ok_or_else(|| FluxError::DocumentNotFound(id.to_string()))
    }

    /// Update a document by replacing its data entirely.
    pub fn update(&mut self, id: &str, data: Value) -> Result<()> {
        let documents = &mut self.documents;
        let indexes = &mut self.indexes;

        if !documents.contains_key(id) {
            return Err(FluxError::DocumentNotFound(id.to_string()));
        }

        // Remove old index entries
        {
            let doc = &documents[id];
            for index in indexes.values_mut() {
                index.remove_doc(id, doc);
            }
        }

        // Update data (rebuilds raw byte cache, preserves _id)
        documents.get_mut(id).unwrap().set_data(data);

        // Add new index entries
        {
            let doc = &documents[id];
            for index in indexes.values_mut() {
                index.add_doc(id, doc);
            }
        }

        Ok(())
    }

    /// Delete a document by ID. Returns true if it existed.
    pub fn delete(&mut self, id: &str) -> bool {
        let documents = &mut self.documents;
        let indexes = &mut self.indexes;

        if !documents.contains_key(id) {
            return false;
        }

        {
            let doc = &documents[id];
            for index in indexes.values_mut() {
                index.remove_doc(id, doc);
            }
        }

        documents.remove(id);
        true
    }

    /// Find all documents matching a filter, with optional projection.
    /// Uses secondary indexes when possible to narrow the scan.
    pub fn find(
        &self,
        filter: &Value,
        projection: Option<&Value>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<Vec<Value>> {
        let is_match_all = filter.as_object().map_or(false, |m| m.is_empty());
        let has_projection = projection
            .and_then(|p| p.as_object())
            .map_or(false, |o| !o.is_empty());

        // Ultra-fast path: full scan, no filter, no projection, no skip/limit
        if is_match_all && !has_projection && skip.is_none() && limit.is_none() {
            let mut results = Vec::with_capacity(self.documents.len());
            for doc in self.documents.values() {
                results.push(doc.to_value());
            }
            return Ok(results);
        }

        let capacity = if is_match_all { self.documents.len() } else { 16 };
        let mut results = Vec::with_capacity(capacity);
        let skip_count = skip.unwrap_or(0);
        let mut skipped = 0;

        let candidate_ids = if is_match_all {
            None
        } else {
            self.try_index_scan(filter)
        };

        let iter: Box<dyn Iterator<Item = &Document>> = match &candidate_ids {
            Some(ids) => Box::new(ids.iter().filter_map(|id| self.documents.get(id))),
            None => Box::new(self.documents.values()),
        };

        for doc in iter {
            if is_match_all || matches_filter(doc, filter)? {
                if skipped < skip_count {
                    skipped += 1;
                    continue;
                }

                let mut val = doc.to_value();
                if has_projection {
                    apply_projection(&mut val, projection.unwrap())?;
                }
                results.push(val);

                if let Some(lim) = limit {
                    if results.len() >= lim {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Find documents and return pre-serialized JSON bytes for each match.
    /// Much faster than `find()` for bulk reads since it avoids cloning Value trees.
    pub fn find_raw(
        &self,
        filter: &Value,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<Vec<&[u8]>> {
        let is_match_all = filter.as_object().map_or(false, |m| m.is_empty());

        // Ultra-fast path: return all raw byte slices
        if is_match_all && skip.is_none() && limit.is_none() {
            let mut results = Vec::with_capacity(self.documents.len());
            for doc in self.documents.values() {
                results.push(doc.raw_bytes());
            }
            return Ok(results);
        }

        let capacity = if is_match_all { self.documents.len() } else { 16 };
        let mut results = Vec::with_capacity(capacity);
        let skip_count = skip.unwrap_or(0);
        let mut skipped = 0;

        let candidate_ids = if is_match_all {
            None
        } else {
            self.try_index_scan(filter)
        };

        let iter: Box<dyn Iterator<Item = &Document>> = match &candidate_ids {
            Some(ids) => Box::new(ids.iter().filter_map(|id| self.documents.get(id))),
            None => Box::new(self.documents.values()),
        };

        for doc in iter {
            if is_match_all || matches_filter(doc, filter)? {
                if skipped < skip_count {
                    skipped += 1;
                    continue;
                }

                results.push(doc.raw_bytes());

                if let Some(lim) = limit {
                    if results.len() >= lim {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Count documents matching a filter.
    pub fn count(&self, filter: &Value) -> Result<usize> {
        let is_match_all = filter.as_object().map_or(false, |m| m.is_empty());
        if is_match_all {
            return Ok(self.documents.len());
        }

        let candidate_ids = self.try_index_scan(filter);

        let iter: Box<dyn Iterator<Item = &Document>> = match &candidate_ids {
            Some(ids) => Box::new(ids.iter().filter_map(|id| self.documents.get(id))),
            None => Box::new(self.documents.values()),
        };

        let mut count = 0;
        for doc in iter {
            if matches_filter(doc, filter)? {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Zero-copy iterate over all documents, calling the callback with each
    /// document's ID and pre-serialized JSON bytes. No allocations.
    pub fn scan<F>(&self, mut f: F)
    where
        F: FnMut(&str, &[u8]),
    {
        for doc in self.documents.values() {
            f(&doc.id, doc.raw_bytes());
        }
    }

    /// Return total number of documents.
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    /// Get all document IDs.
    pub fn doc_ids(&self) -> Vec<String> {
        self.documents.keys().cloned().collect()
    }

    /// Create a secondary index on a field. Scans existing documents to populate it.
    pub fn create_index(&mut self, field: &str) -> Result<()> {
        if self.indexes.contains_key(field) {
            return Err(FluxError::IndexAlreadyExists(field.to_string()));
        }

        let mut index = SecondaryIndex::new(field.to_string());
        index.build_from(
            self.documents
                .iter()
                .map(|(id, doc)| (id.as_str(), doc)),
        );
        self.indexes.insert(field.to_string(), index);
        Ok(())
    }

    /// Drop a secondary index.
    pub fn drop_index(&mut self, field: &str) -> Result<()> {
        if self.indexes.remove(field).is_none() {
            return Err(FluxError::IndexNotFound(field.to_string()));
        }
        Ok(())
    }

    /// List all indexed fields.
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    /// Try to use secondary indexes to narrow the document scan.
    /// Returns candidate doc IDs if an index applies, or None for a full scan.
    fn try_index_scan(&self, filter: &Value) -> Option<Vec<String>> {
        let filter_obj = filter.as_object()?;
        let mut best_candidates: Option<HashSet<String>> = None;

        for (field, condition) in filter_obj {
            if field.starts_with('$') {
                continue;
            }

            let index = match self.indexes.get(field.as_str()) {
                Some(idx) => idx,
                None => continue,
            };

            let candidates = if let Some(cond_obj) = condition.as_object() {
                if cond_obj.keys().any(|k| k.starts_with('$')) {
                    match Self::eval_index_operators(index, cond_obj) {
                        Some(c) => c,
                        None => continue,
                    }
                } else {
                    // Non-operator object value — not indexable
                    continue;
                }
            } else {
                // Implicit $eq
                let key = IndexKey::from_value(condition)?;
                index.lookup_eq(&key)
            };

            best_candidates = Some(match best_candidates {
                Some(existing) => existing.intersection(&candidates).cloned().collect(),
                None => candidates,
            });
        }

        best_candidates.map(|set| set.into_iter().collect())
    }

    /// Evaluate operator conditions against an index, returning candidate doc IDs.
    fn eval_index_operators(
        index: &SecondaryIndex,
        cond_obj: &serde_json::Map<String, Value>,
    ) -> Option<HashSet<String>> {
        let mut lower: Bound<IndexKey> = Bound::Unbounded;
        let mut upper: Bound<IndexKey> = Bound::Unbounded;
        let mut eq_value: Option<IndexKey> = None;
        let mut in_values: Option<&Vec<Value>> = None;
        let mut has_indexable = false;

        for (op, val) in cond_obj {
            match op.as_str() {
                "$eq" => {
                    eq_value = IndexKey::from_value(val);
                    has_indexable = true;
                }
                "$gt" => {
                    if let Some(key) = IndexKey::from_value(val) {
                        lower = Bound::Excluded(key);
                        has_indexable = true;
                    }
                }
                "$gte" => {
                    if let Some(key) = IndexKey::from_value(val) {
                        lower = Bound::Included(key);
                        has_indexable = true;
                    }
                }
                "$lt" => {
                    if let Some(key) = IndexKey::from_value(val) {
                        upper = Bound::Excluded(key);
                        has_indexable = true;
                    }
                }
                "$lte" => {
                    if let Some(key) = IndexKey::from_value(val) {
                        upper = Bound::Included(key);
                        has_indexable = true;
                    }
                }
                "$in" => {
                    in_values = val.as_array();
                    has_indexable = true;
                }
                _ => continue, // non-indexable operators handled by full filter
            }
        }

        if !has_indexable {
            return None;
        }

        if let Some(key) = eq_value {
            return Some(index.lookup_eq(&key));
        }

        if let Some(values) = in_values {
            let mut result = HashSet::new();
            for val in values {
                if let Some(key) = IndexKey::from_value(val) {
                    result.extend(index.lookup_eq(&key));
                }
            }
            return Some(result);
        }

        // Range query
        let lower_ref = match &lower {
            Bound::Included(k) => Bound::Included(k),
            Bound::Excluded(k) => Bound::Excluded(k),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper_ref = match &upper {
            Bound::Included(k) => Bound::Included(k),
            Bound::Excluded(k) => Bound::Excluded(k),
            Bound::Unbounded => Bound::Unbounded,
        };

        Some(index.lookup_range(lower_ref, upper_ref))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_insert_and_get() {
        let mut col = Collection::new("test".into());
        let doc = Document::new(json!({"_id": "1", "name": "Alice"}));
        col.insert(doc);

        let retrieved = col.get("1").unwrap();
        assert_eq!(retrieved.id, "1");
        assert_eq!(retrieved.get_field("name").unwrap(), "Alice");
    }

    #[test]
    fn test_update() {
        let mut col = Collection::new("test".into());
        col.insert(Document::new(json!({"_id": "1", "name": "Alice"})));
        col.update("1", json!({"name": "Bob"})).unwrap();

        let doc = col.get("1").unwrap();
        assert_eq!(doc.get_field("name").unwrap(), "Bob");
    }

    #[test]
    fn test_delete() {
        let mut col = Collection::new("test".into());
        col.insert(Document::new(json!({"_id": "1", "name": "Alice"})));
        assert!(col.delete("1"));
        assert!(!col.delete("1"));
        assert!(col.get("1").is_err());
    }

    #[test]
    fn test_find_with_filter() {
        let mut col = Collection::new("test".into());
        col.insert(Document::new(
            json!({"_id": "1", "name": "Alice", "age": 30}),
        ));
        col.insert(Document::new(
            json!({"_id": "2", "name": "Bob", "age": 25}),
        ));
        col.insert(Document::new(
            json!({"_id": "3", "name": "Charlie", "age": 35}),
        ));

        let results = col
            .find(&json!({"age": {"$gte": 30}}), None, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_find_with_limit_and_skip() {
        let mut col = Collection::new("test".into());
        for i in 0..10 {
            col.insert(Document::new(json!({"_id": format!("{i}"), "n": i})));
        }

        let results = col.find(&json!({}), None, Some(3), Some(2)).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_count() {
        let mut col = Collection::new("test".into());
        col.insert(Document::new(
            json!({"_id": "1", "status": "active"}),
        ));
        col.insert(Document::new(
            json!({"_id": "2", "status": "inactive"}),
        ));
        col.insert(Document::new(
            json!({"_id": "3", "status": "active"}),
        ));

        assert_eq!(col.count(&json!({"status": "active"})).unwrap(), 2);
        assert_eq!(col.count(&json!({})).unwrap(), 3);
    }

    #[test]
    fn test_find_with_projection() {
        let mut col = Collection::new("test".into());
        col.insert(Document::new(
            json!({"_id": "1", "name": "Alice", "age": 30, "email": "a@b.com"}),
        ));

        let results = col
            .find(&json!({}), Some(&json!({"name": 1})), None, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].get("name").is_some());
        assert!(results[0].get("email").is_none());
        assert!(results[0].get("_id").is_some());
    }

    #[test]
    fn test_create_index_and_query() {
        let mut col = Collection::new("test".into());
        col.insert(Document::new(
            json!({"_id": "1", "name": "Alice", "age": 30}),
        ));
        col.insert(Document::new(
            json!({"_id": "2", "name": "Bob", "age": 25}),
        ));
        col.insert(Document::new(
            json!({"_id": "3", "name": "Charlie", "age": 35}),
        ));

        col.create_index("age").unwrap();

        // Exact match via index
        let results = col.find(&json!({"age": 30}), None, None, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["name"], "Alice");

        // Range via index
        let results = col
            .find(&json!({"age": {"$gte": 30}}), None, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_index_maintained_on_insert() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();

        col.insert(Document::new(json!({"_id": "1", "age": 30})));
        let results = col.find(&json!({"age": 30}), None, None, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_index_maintained_on_update() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();

        col.insert(Document::new(json!({"_id": "1", "age": 30})));
        col.update("1", json!({"age": 40})).unwrap();

        // Old value no longer indexed
        let results = col.find(&json!({"age": 30}), None, None, None).unwrap();
        assert_eq!(results.len(), 0);

        // New value is indexed
        let results = col.find(&json!({"age": 40}), None, None, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_index_maintained_on_delete() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();

        col.insert(Document::new(json!({"_id": "1", "age": 30})));
        col.delete("1");

        let results = col.find(&json!({"age": 30}), None, None, None).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_index_range_query() {
        let mut col = Collection::new("test".into());
        col.create_index("score").unwrap();

        for i in 0..20 {
            col.insert(Document::new(
                json!({"_id": format!("{i}"), "score": i * 5}),
            ));
        }

        // score >= 50 AND score < 75
        let results = col
            .find(
                &json!({"score": {"$gte": 50, "$lt": 75}}),
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 5); // 50, 55, 60, 65, 70
    }

    #[test]
    fn test_index_in_query() {
        let mut col = Collection::new("test".into());
        col.create_index("status").unwrap();

        col.insert(Document::new(
            json!({"_id": "1", "status": "active"}),
        ));
        col.insert(Document::new(
            json!({"_id": "2", "status": "pending"}),
        ));
        col.insert(Document::new(
            json!({"_id": "3", "status": "inactive"}),
        ));

        let results = col
            .find(
                &json!({"status": {"$in": ["active", "pending"]}}),
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_duplicate_index_error() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();
        assert!(col.create_index("age").is_err());
    }

    #[test]
    fn test_drop_index() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();
        col.drop_index("age").unwrap();
        assert!(col.drop_index("age").is_err());
        assert!(col.list_indexes().is_empty());
    }

    #[test]
    fn test_list_indexes() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();
        col.create_index("name").unwrap();

        let mut indexes = col.list_indexes();
        indexes.sort();
        assert_eq!(indexes, vec!["age", "name"]);
    }
}
