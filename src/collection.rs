use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;

use serde_json::Value;

use crate::document::Document;
use crate::error::{FluxError, Result};
use crate::index::{IndexKey, SecondaryIndex};
use crate::query::{apply_projection, compare_sort_values, matches_filter};

/// A collection is a named group of documents, analogous to a table in SQL.
///
/// Documents are stored in a `BTreeMap` keyed by ID for deterministic
/// iteration order, which makes `skip`/`limit` pagination stable.
#[derive(Debug)]
pub struct Collection {
    pub name: String,
    documents: BTreeMap<String, Document>,
    indexes: BTreeMap<String, SecondaryIndex>,
}

impl Collection {
    pub fn new(name: String) -> Self {
        Collection {
            name,
            documents: BTreeMap::new(),
            indexes: BTreeMap::new(),
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

    /// Find all documents matching a filter, with optional projection and sort.
    /// Uses secondary indexes when possible to narrow the scan.
    pub fn find(
        &self,
        filter: &Value,
        projection: Option<&Value>,
        limit: Option<usize>,
        skip: Option<usize>,
        sort: Option<&Value>,
    ) -> Result<Vec<Value>> {
        let is_match_all = filter.as_object().map_or(false, |m| m.is_empty());
        let has_projection = projection
            .and_then(|p| p.as_object())
            .map_or(false, |o| !o.is_empty());
        let has_sort = sort
            .and_then(|s| s.as_object())
            .map_or(false, |o| !o.is_empty());

        // Ultra-fast path: full scan, no filter, no projection, no skip/limit, no sort
        if is_match_all && !has_projection && !has_sort && skip.is_none() && limit.is_none() {
            let mut results = Vec::with_capacity(self.documents.len());
            for doc in self.documents.values() {
                results.push(doc.to_value());
            }
            return Ok(results);
        }

        // When sort is specified, we must collect all matches before applying skip/limit
        if has_sort {
            return self.find_sorted(filter, projection, limit, skip, sort.unwrap());
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

    /// Find with sort: collect all matches, sort, then apply skip/limit/projection.
    fn find_sorted(
        &self,
        filter: &Value,
        projection: Option<&Value>,
        limit: Option<usize>,
        skip: Option<usize>,
        sort_spec: &Value,
    ) -> Result<Vec<Value>> {
        let is_match_all = filter.as_object().map_or(false, |m| m.is_empty());
        let has_projection = projection
            .and_then(|p| p.as_object())
            .map_or(false, |o| !o.is_empty());

        let candidate_ids = if is_match_all {
            None
        } else {
            self.try_index_scan(filter)
        };

        let iter: Box<dyn Iterator<Item = &Document>> = match &candidate_ids {
            Some(ids) => Box::new(ids.iter().filter_map(|id| self.documents.get(id))),
            None => Box::new(self.documents.values()),
        };

        // Collect all matching documents
        let mut matched: Vec<&Document> = Vec::new();
        for doc in iter {
            if is_match_all || matches_filter(doc, filter)? {
                matched.push(doc);
            }
        }

        // Parse sort spec: Vec<(field, ascending)>
        let sort_fields = Self::parse_sort_spec(sort_spec)?;

        // Sort
        matched.sort_by(|a, b| {
            for (field, ascending) in &sort_fields {
                let va = a.get_field(field);
                let vb = b.get_field(field);
                let ord = match (va, vb) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (Some(a), Some(b)) => {
                        compare_sort_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
                    }
                };
                let ord = if *ascending { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Apply skip/limit and projection
        let skip_count = skip.unwrap_or(0);
        let iter = matched.into_iter().skip(skip_count);
        let iter: Box<dyn Iterator<Item = &Document>> = match limit {
            Some(lim) => Box::new(iter.take(lim)),
            None => Box::new(iter),
        };

        let mut results = Vec::new();
        for doc in iter {
            let mut val = doc.to_value();
            if has_projection {
                apply_projection(&mut val, projection.unwrap())?;
            }
            results.push(val);
        }

        Ok(results)
    }

    /// Parse a sort spec like `{"age": 1, "name": -1}` into field/direction pairs.
    fn parse_sort_spec(sort_spec: &Value) -> Result<Vec<(String, bool)>> {
        let obj = sort_spec.as_object().ok_or_else(|| {
            FluxError::InvalidQuery("sort must be a JSON object".into())
        })?;
        let mut fields = Vec::with_capacity(obj.len());
        for (field, dir) in obj {
            let ascending = match dir.as_i64() {
                Some(1) => true,
                Some(-1) => false,
                _ => {
                    return Err(FluxError::InvalidQuery(
                        "sort values must be 1 (ascending) or -1 (descending)".into(),
                    ))
                }
            };
            fields.push((field.clone(), ascending));
        }
        Ok(fields)
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

        // Collect all candidate sets from indexable conditions
        let mut candidate_sets: Vec<HashSet<String>> = Vec::new();

        for (field, condition) in filter_obj {
            match field.as_str() {
                "$and" => {
                    // Each $and branch can narrow candidates via intersection
                    if let Some(branches) = condition.as_array() {
                        for branch in branches {
                            if let Some(ids) = self.try_index_scan(branch) {
                                candidate_sets.push(ids.into_iter().collect());
                            }
                        }
                    }
                }
                "$or" => {
                    // All branches must be indexable; union the results
                    if let Some(branches) = condition.as_array() {
                        let mut union = HashSet::new();
                        let mut all_indexed = true;
                        for branch in branches {
                            if let Some(ids) = self.try_index_scan(branch) {
                                union.extend(ids);
                            } else {
                                all_indexed = false;
                                break;
                            }
                        }
                        if all_indexed && !union.is_empty() {
                            candidate_sets.push(union);
                        }
                    }
                }
                _ if field.starts_with('$') => {
                    // Other operators like $not — skip
                    continue;
                }
                _ => {
                    // Regular field condition
                    let index = match self.indexes.get(field.as_str()) {
                        Some(idx) => idx,
                        None => continue,
                    };

                    if let Some(candidates) = self.eval_field_index(index, condition) {
                        candidate_sets.push(candidates);
                    }
                }
            }
        }

        if candidate_sets.is_empty() {
            return None;
        }

        // Sort by size (smallest first) for efficient intersection
        candidate_sets.sort_by_key(|s| s.len());

        // Intersect all sets, starting with the smallest
        let mut result = candidate_sets.swap_remove(0);
        for other in &candidate_sets {
            result.retain(|id| other.contains(id));
            if result.is_empty() {
                break;
            }
        }

        Some(result.into_iter().collect())
    }

    /// Evaluate a single field condition against an index, returning candidate doc IDs.
    fn eval_field_index(
        &self,
        index: &SecondaryIndex,
        condition: &Value,
    ) -> Option<HashSet<String>> {
        if let Some(cond_obj) = condition.as_object() {
            if cond_obj.keys().any(|k| k.starts_with('$')) {
                return Self::eval_index_operators(index, cond_obj);
            }
        }

        // Implicit $eq
        let key = IndexKey::from_value(condition)?;
        Some(index.lookup_eq(&key).cloned().unwrap_or_default())
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
            return Some(index.lookup_eq(&key).cloned().unwrap_or_default());
        }

        if let Some(values) = in_values {
            let mut result = HashSet::new();
            for val in values {
                if let Some(key) = IndexKey::from_value(val) {
                    if let Some(ids) = index.lookup_eq(&key) {
                        result.extend(ids.iter().cloned());
                    }
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
            .find(&json!({"age": {"$gte": 30}}), None, None, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_find_with_limit_and_skip() {
        let mut col = Collection::new("test".into());
        for i in 0..10 {
            col.insert(Document::new(json!({"_id": format!("{i}"), "n": i})));
        }

        let results = col.find(&json!({}), None, Some(3), Some(2), None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_deterministic_skip_limit() {
        let mut col = Collection::new("test".into());
        for i in 0..20 {
            col.insert(Document::new(
                json!({"_id": format!("doc-{i:03}"), "val": i}),
            ));
        }

        // Same skip/limit should always return the same results
        let r1 = col.find(&json!({}), None, Some(5), Some(3), None).unwrap();
        let r2 = col.find(&json!({}), None, Some(5), Some(3), None).unwrap();
        assert_eq!(r1, r2);

        // Verify they're sorted by _id (BTreeMap order)
        let ids: Vec<&str> = r1.iter().map(|v| v["_id"].as_str().unwrap()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn test_find_with_sort() {
        let mut col = Collection::new("test".into());
        col.insert(Document::new(json!({"_id": "1", "name": "Charlie", "age": 35})));
        col.insert(Document::new(json!({"_id": "2", "name": "Alice", "age": 30})));
        col.insert(Document::new(json!({"_id": "3", "name": "Bob", "age": 25})));

        // Sort by age ascending
        let results = col
            .find(&json!({}), None, None, None, Some(&json!({"age": 1})))
            .unwrap();
        assert_eq!(results[0]["name"], "Bob");
        assert_eq!(results[1]["name"], "Alice");
        assert_eq!(results[2]["name"], "Charlie");

        // Sort by age descending
        let results = col
            .find(&json!({}), None, None, None, Some(&json!({"age": -1})))
            .unwrap();
        assert_eq!(results[0]["name"], "Charlie");
        assert_eq!(results[1]["name"], "Alice");
        assert_eq!(results[2]["name"], "Bob");

        // Sort by name ascending
        let results = col
            .find(&json!({}), None, None, None, Some(&json!({"name": 1})))
            .unwrap();
        assert_eq!(results[0]["name"], "Alice");
        assert_eq!(results[1]["name"], "Bob");
        assert_eq!(results[2]["name"], "Charlie");
    }

    #[test]
    fn test_sort_with_skip_limit() {
        let mut col = Collection::new("test".into());
        for i in 0..10 {
            col.insert(Document::new(
                json!({"_id": format!("{i}"), "score": (9 - i) * 10}),
            ));
        }

        // Sort by score ascending, skip 2, limit 3 → should get scores 20, 30, 40
        let results = col
            .find(
                &json!({}),
                None,
                Some(3),
                Some(2),
                Some(&json!({"score": 1})),
            )
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0]["score"], 20);
        assert_eq!(results[1]["score"], 30);
        assert_eq!(results[2]["score"], 40);
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
            .find(&json!({}), Some(&json!({"name": 1})), None, None, None)
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
        let results = col.find(&json!({"age": 30}), None, None, None, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["name"], "Alice");

        // Range via index
        let results = col
            .find(&json!({"age": {"$gte": 30}}), None, None, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_index_cross_type_numeric() {
        // Integer and float queries should find each other's documents
        let mut col = Collection::new("test".into());
        col.create_index("val").unwrap();

        col.insert(Document::new(json!({"_id": "1", "val": 50})));    // i64
        col.insert(Document::new(json!({"_id": "2", "val": 50.0})));  // f64

        // Query with float should find integer doc
        let results = col
            .find(&json!({"val": 50.0}), None, None, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);

        // Query with integer should find float doc
        let results = col
            .find(&json!({"val": 50}), None, None, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);

        // Range query across types
        let results = col
            .find(&json!({"val": {"$gte": 49.5, "$lte": 50.5}}), None, None, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_index_maintained_on_insert() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();

        col.insert(Document::new(json!({"_id": "1", "age": 30})));
        let results = col.find(&json!({"age": 30}), None, None, None, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_index_maintained_on_update() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();

        col.insert(Document::new(json!({"_id": "1", "age": 30})));
        col.update("1", json!({"age": 40})).unwrap();

        // Old value no longer indexed
        let results = col.find(&json!({"age": 30}), None, None, None, None).unwrap();
        assert_eq!(results.len(), 0);

        // New value is indexed
        let results = col.find(&json!({"age": 40}), None, None, None, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_index_maintained_on_delete() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();

        col.insert(Document::new(json!({"_id": "1", "age": 30})));
        col.delete("1");

        let results = col.find(&json!({"age": 30}), None, None, None, None).unwrap();
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
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_or_index_usage() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();

        for i in 0..100 {
            col.insert(Document::new(
                json!({"_id": format!("{i}"), "age": i}),
            ));
        }

        // $or with all branches indexable
        let results = col
            .find(
                &json!({"$or": [{"age": 10}, {"age": 20}, {"age": 30}]}),
                None,
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_and_index_usage() {
        let mut col = Collection::new("test".into());
        col.create_index("age").unwrap();
        col.create_index("status").unwrap();

        col.insert(Document::new(json!({"_id": "1", "age": 30, "status": "active"})));
        col.insert(Document::new(json!({"_id": "2", "age": 30, "status": "inactive"})));
        col.insert(Document::new(json!({"_id": "3", "age": 25, "status": "active"})));

        // $and with both branches indexable
        let results = col
            .find(
                &json!({"$and": [{"age": 30}, {"status": "active"}]}),
                None,
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "1");
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
