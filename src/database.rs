use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::collection::Collection;
use crate::document::Document;
use crate::error::{FluxError, Result};
use crate::wal::{Wal, WalOperation};

/// The main database engine. Manages collections and persistence.
pub struct Database {
    pub name: String,
    collections: HashMap<String, Collection>,
    wal: Wal,
    data_dir: PathBuf,
}

impl Database {
    /// Open or create a database at the given directory path.
    pub fn open(data_dir: &Path) -> Result<Self> {
        fs::create_dir_all(data_dir)?;

        let name = data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("fluxdb")
            .to_string();

        let wal_path = data_dir.join("wal.log");
        let mut db = Database {
            name,
            collections: HashMap::new(),
            wal: Wal::open(&wal_path)?,
            data_dir: data_dir.to_path_buf(),
        };

        // Replay WAL to restore state
        db.replay_wal()?;

        Ok(db)
    }

    fn replay_wal(&mut self) -> Result<()> {
        let wal_path = self.data_dir.join("wal.log");
        let entries = Wal::read_all(&wal_path)?;

        for entry in entries {
            match entry.operation {
                WalOperation::CreateCollection { name } => {
                    self.collections
                        .insert(name.clone(), Collection::new(name));
                }
                WalOperation::DropCollection { name } => {
                    self.collections.remove(&name);
                }
                WalOperation::Insert {
                    collection,
                    doc_id,
                    data,
                } => {
                    if let Some(col) = self.collections.get_mut(&collection) {
                        let mut doc = Document::new(data);
                        doc.id = doc_id;
                        col.insert(doc);
                    }
                }
                WalOperation::Update {
                    collection,
                    doc_id,
                    data,
                } => {
                    if let Some(col) = self.collections.get_mut(&collection) {
                        let _ = col.update(&doc_id, data);
                    }
                }
                WalOperation::Delete {
                    collection,
                    doc_id,
                } => {
                    if let Some(col) = self.collections.get_mut(&collection) {
                        col.delete(&doc_id);
                    }
                }
            }
        }

        Ok(())
    }

    /// Create a new collection.
    pub fn create_collection(&mut self, name: &str) -> Result<()> {
        if self.collections.contains_key(name) {
            return Err(FluxError::CollectionAlreadyExists(name.to_string()));
        }

        self.wal.append(WalOperation::CreateCollection {
            name: name.to_string(),
        })?;

        self.collections
            .insert(name.to_string(), Collection::new(name.to_string()));
        Ok(())
    }

    /// Drop a collection and all its documents.
    pub fn drop_collection(&mut self, name: &str) -> Result<()> {
        if !self.collections.contains_key(name) {
            return Err(FluxError::CollectionNotFound(name.to_string()));
        }

        self.wal.append(WalOperation::DropCollection {
            name: name.to_string(),
        })?;

        self.collections.remove(name);
        Ok(())
    }

    /// List all collection names.
    pub fn list_collections(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    /// Insert a document into a collection.
    pub fn insert(&mut self, collection: &str, value: Value) -> Result<String> {
        if !self.collections.contains_key(collection) {
            return Err(FluxError::CollectionNotFound(collection.to_string()));
        }

        let doc = Document::new(value);
        let id = doc.id.clone();
        let data = doc.data.clone();

        self.wal.append(WalOperation::Insert {
            collection: collection.to_string(),
            doc_id: id.clone(),
            data,
        })?;

        let col = self.collections.get_mut(collection).unwrap();
        col.insert(doc);

        Ok(id)
    }

    /// Get a document by ID from a collection.
    pub fn get(&self, collection: &str, id: &str) -> Result<Value> {
        let col = self
            .collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let doc = col.get(id)?;
        Ok(doc.to_value())
    }

    /// Update a document by ID with new data.
    pub fn update(&mut self, collection: &str, id: &str, data: Value) -> Result<()> {
        // Verify collection and document exist
        {
            let col = self
                .collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;
            col.get(id)?; // verify exists
        }

        let update_data = match &data {
            Value::Object(obj) => {
                let mut clean = obj.clone();
                clean.remove("_id"); // don't allow changing _id
                Value::Object(clean)
            }
            other => other.clone(),
        };

        self.wal.append(WalOperation::Update {
            collection: collection.to_string(),
            doc_id: id.to_string(),
            data: update_data.clone(),
        })?;

        let col = self.collections.get_mut(collection).unwrap();
        col.update(id, update_data)?;

        Ok(())
    }

    /// Delete a document by ID.
    pub fn delete(&mut self, collection: &str, id: &str) -> Result<bool> {
        let col = self
            .collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        // Check if document exists
        if col.get(id).is_err() {
            return Ok(false);
        }

        self.wal.append(WalOperation::Delete {
            collection: collection.to_string(),
            doc_id: id.to_string(),
        })?;

        let col = self.collections.get_mut(collection).unwrap();
        Ok(col.delete(id))
    }

    /// Find documents matching a filter.
    pub fn find(
        &self,
        collection: &str,
        filter: Value,
        projection: Option<Value>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<Vec<Value>> {
        let col = self
            .collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        col.find(&filter, projection.as_ref(), limit, skip)
    }

    /// Count documents matching a filter.
    pub fn count(&self, collection: &str, filter: Value) -> Result<usize> {
        let col = self
            .collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        col.count(&filter)
    }

    /// Compact the WAL by writing a fresh snapshot.
    pub fn compact(&mut self) -> Result<()> {
        self.wal.truncate()?;

        // Re-write current state to WAL
        for (name, col) in &self.collections {
            self.wal.append(WalOperation::CreateCollection {
                name: name.clone(),
            })?;

            for doc_id in col.doc_ids() {
                if let Ok(doc) = col.get(&doc_id) {
                    self.wal.append(WalOperation::Insert {
                        collection: name.clone(),
                        doc_id: doc.id.clone(),
                        data: doc.data.clone(),
                    })?;
                }
            }
        }

        Ok(())
    }

    /// Get database statistics.
    pub fn stats(&self) -> Value {
        let mut collections_info = serde_json::Map::new();
        let mut total_docs = 0usize;

        for (name, col) in &self.collections {
            let count = col.len();
            total_docs += count;
            collections_info.insert(
                name.clone(),
                serde_json::json!({ "documents": count }),
            );
        }

        serde_json::json!({
            "database": self.name,
            "collections": self.collections.len(),
            "total_documents": total_docs,
            "collection_details": collections_info,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        (db, dir)
    }

    #[test]
    fn test_create_and_list_collections() {
        let (mut db, _dir) = test_db();
        db.create_collection("users").unwrap();
        db.create_collection("orders").unwrap();

        let mut collections = db.list_collections();
        collections.sort();
        assert_eq!(collections, vec!["orders", "users"]);
    }

    #[test]
    fn test_duplicate_collection_error() {
        let (mut db, _dir) = test_db();
        db.create_collection("users").unwrap();
        assert!(db.create_collection("users").is_err());
    }

    #[test]
    fn test_insert_and_get() {
        let (mut db, _dir) = test_db();
        db.create_collection("users").unwrap();

        let id = db
            .insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();

        let doc = db.get("users", &id).unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 30);
        assert_eq!(doc["_id"], id);
    }

    #[test]
    fn test_update() {
        let (mut db, _dir) = test_db();
        db.create_collection("users").unwrap();
        let id = db.insert("users", json!({"name": "Alice"})).unwrap();

        db.update("users", &id, json!({"name": "Bob", "age": 25}))
            .unwrap();

        let doc = db.get("users", &id).unwrap();
        assert_eq!(doc["name"], "Bob");
        assert_eq!(doc["age"], 25);
    }

    #[test]
    fn test_delete() {
        let (mut db, _dir) = test_db();
        db.create_collection("users").unwrap();
        let id = db.insert("users", json!({"name": "Alice"})).unwrap();

        assert!(db.delete("users", &id).unwrap());
        assert!(!db.delete("users", &id).unwrap());
        assert!(db.get("users", &id).is_err());
    }

    #[test]
    fn test_find() {
        let (mut db, _dir) = test_db();
        db.create_collection("users").unwrap();
        db.insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();
        db.insert("users", json!({"name": "Bob", "age": 25}))
            .unwrap();
        db.insert("users", json!({"name": "Charlie", "age": 35}))
            .unwrap();

        let results = db
            .find("users", json!({"age": {"$gte": 30}}), None, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_persistence_via_wal() {
        let dir = tempdir().unwrap();

        // Write data
        {
            let mut db = Database::open(dir.path()).unwrap();
            db.create_collection("users").unwrap();
            db.insert("users", json!({"_id": "1", "name": "Alice"}))
                .unwrap();
            db.insert("users", json!({"_id": "2", "name": "Bob"}))
                .unwrap();
        }

        // Reopen and verify data persisted
        {
            let db = Database::open(dir.path()).unwrap();
            let doc = db.get("users", "1").unwrap();
            assert_eq!(doc["name"], "Alice");

            let doc = db.get("users", "2").unwrap();
            assert_eq!(doc["name"], "Bob");
        }
    }

    #[test]
    fn test_persistence_after_update_and_delete() {
        let dir = tempdir().unwrap();

        {
            let mut db = Database::open(dir.path()).unwrap();
            db.create_collection("items").unwrap();
            db.insert("items", json!({"_id": "a", "val": 1})).unwrap();
            db.insert("items", json!({"_id": "b", "val": 2})).unwrap();
            db.update("items", "a", json!({"val": 10})).unwrap();
            db.delete("items", "b").unwrap();
        }

        {
            let db = Database::open(dir.path()).unwrap();
            let doc = db.get("items", "a").unwrap();
            assert_eq!(doc["val"], 10);
            assert!(db.get("items", "b").is_err());
        }
    }

    #[test]
    fn test_compact() {
        let dir = tempdir().unwrap();

        {
            let mut db = Database::open(dir.path()).unwrap();
            db.create_collection("users").unwrap();
            db.insert("users", json!({"_id": "1", "name": "Alice"}))
                .unwrap();
            db.insert("users", json!({"_id": "2", "name": "Bob"}))
                .unwrap();
            db.delete("users", "2").unwrap();
            db.compact().unwrap();
        }

        // Verify state is preserved after compaction
        {
            let db = Database::open(dir.path()).unwrap();
            let doc = db.get("users", "1").unwrap();
            assert_eq!(doc["name"], "Alice");
            assert!(db.get("users", "2").is_err());
            assert_eq!(db.count("users", json!({})).unwrap(), 1);
        }
    }

    #[test]
    fn test_drop_collection() {
        let (mut db, _dir) = test_db();
        db.create_collection("users").unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();

        db.drop_collection("users").unwrap();
        assert!(db.list_collections().is_empty());
        assert!(db.insert("users", json!({"name": "Bob"})).is_err());
    }

    #[test]
    fn test_stats() {
        let (mut db, _dir) = test_db();
        db.create_collection("users").unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();
        db.insert("users", json!({"name": "Bob"})).unwrap();

        let stats = db.stats();
        assert_eq!(stats["collections"], 1);
        assert_eq!(stats["total_documents"], 2);
    }
}
