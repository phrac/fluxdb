use std::collections::HashMap;

use serde_json::Value;

use crate::document::Document;
use crate::error::{FluxError, Result};
use crate::query::{apply_projection, matches_filter};

/// A collection is a named group of documents, analogous to a table in SQL.
#[derive(Debug, Clone)]
pub struct Collection {
    pub name: String,
    documents: HashMap<String, Document>,
}

impl Collection {
    pub fn new(name: String) -> Self {
        Collection {
            name,
            documents: HashMap::new(),
        }
    }

    /// Insert a document. Returns the document's ID.
    pub fn insert(&mut self, doc: Document) -> String {
        let id = doc.id.clone();
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
        let doc = self
            .documents
            .get_mut(id)
            .ok_or_else(|| FluxError::DocumentNotFound(id.to_string()))?;
        doc.data = data;
        Ok(())
    }

    /// Delete a document by ID. Returns true if it existed.
    pub fn delete(&mut self, id: &str) -> bool {
        self.documents.remove(id).is_some()
    }

    /// Find all documents matching a filter, with optional projection.
    pub fn find(
        &self,
        filter: &Value,
        projection: Option<&Value>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        let skip_count = skip.unwrap_or(0);
        let mut skipped = 0;

        for doc in self.documents.values() {
            if matches_filter(doc, filter)? {
                if skipped < skip_count {
                    skipped += 1;
                    continue;
                }

                let mut val = doc.to_value();
                if let Some(proj) = projection {
                    apply_projection(&mut val, proj)?;
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

    /// Count documents matching a filter.
    pub fn count(&self, filter: &Value) -> Result<usize> {
        let mut count = 0;
        for doc in self.documents.values() {
            if matches_filter(doc, filter)? {
                count += 1;
            }
        }
        Ok(count)
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
        col.insert(Document::new(json!({"_id": "1", "name": "Alice", "age": 30})));
        col.insert(Document::new(json!({"_id": "2", "name": "Bob", "age": 25})));
        col.insert(Document::new(json!({"_id": "3", "name": "Charlie", "age": 35})));

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

        let results = col
            .find(&json!({}), None, Some(3), Some(2))
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_count() {
        let mut col = Collection::new("test".into());
        col.insert(Document::new(json!({"_id": "1", "status": "active"})));
        col.insert(Document::new(json!({"_id": "2", "status": "inactive"})));
        col.insert(Document::new(json!({"_id": "3", "status": "active"})));

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
        assert!(results[0].get("_id").is_some()); // _id included by default
    }
}
