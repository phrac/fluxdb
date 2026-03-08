use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;

use crate::collection::Collection;
use crate::document::Document;
use crate::error::{FluxError, Result};
use crate::config::Config;
use crate::wal::{Wal, WalEntry, WalOperation};

/// The main database engine.
///
/// Internally synchronized with per-collection RwLocks for maximum read concurrency.
/// All public methods take `&self` — no external locking required.
pub struct Database {
    pub name: String,
    collections: RwLock<HashMap<String, Arc<RwLock<Collection>>>>,
    wal: Mutex<Wal>,
    #[allow(dead_code)]
    data_dir: PathBuf,
}

impl Database {
    /// Open or create a database with default settings.
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_with_config(&Config::default(), Some(data_dir))
    }

    /// Open or create a database using the provided config.
    /// If `data_dir_override` is Some, it takes precedence over config.data_dir.
    pub fn open_with_config(config: &Config, data_dir_override: Option<&Path>) -> Result<Self> {
        let data_dir = data_dir_override.unwrap_or(&config.data_dir);
        fs::create_dir_all(data_dir)?;

        let name = data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("fluxdb")
            .to_string();

        let wal_path = data_dir.join("wal.log");

        // Read WAL once for both replay and sequence tracking
        let entries = Wal::read_all(&wal_path)?;
        let next_seq = entries.last().map(|e| e.sequence + 1).unwrap_or(0);
        let collections = Self::replay_entries(&entries)?;
        let wal = Wal::open_at_sequence(
            &wal_path,
            next_seq,
            config.wal.batch_size,
            config.wal.batch_bytes,
        )?;

        let wrapped: HashMap<String, Arc<RwLock<Collection>>> = collections
            .into_iter()
            .map(|(name, col)| (name, Arc::new(RwLock::new(col))))
            .collect();

        Ok(Database {
            name,
            collections: RwLock::new(wrapped),
            wal: Mutex::new(wal),
            data_dir: data_dir.to_path_buf(),
        })
    }

    fn replay_entries(entries: &[WalEntry]) -> Result<HashMap<String, Collection>> {
        let mut collections: HashMap<String, Collection> = HashMap::new();

        for entry in entries {
            match &entry.operation {
                WalOperation::CreateCollection { name } => {
                    collections.insert(name.clone(), Collection::new(name.clone()));
                }
                WalOperation::DropCollection { name } => {
                    collections.remove(name);
                }
                WalOperation::Insert {
                    collection,
                    doc_id,
                    data,
                } => {
                    if let Some(col) = collections.get_mut(collection) {
                        let mut doc = Document::new(data.clone());
                        doc.id = doc_id.clone();
                        col.insert(doc);
                    }
                }
                WalOperation::Update {
                    collection,
                    doc_id,
                    data,
                } => {
                    if let Some(col) = collections.get_mut(collection) {
                        let _ = col.update(doc_id, data.clone());
                    }
                }
                WalOperation::Delete {
                    collection,
                    doc_id,
                } => {
                    if let Some(col) = collections.get_mut(collection) {
                        col.delete(doc_id);
                    }
                }
                WalOperation::CreateIndex { collection, field } => {
                    if let Some(col) = collections.get_mut(collection) {
                        let _ = col.create_index(field);
                    }
                }
                WalOperation::DropIndex { collection, field } => {
                    if let Some(col) = collections.get_mut(collection) {
                        let _ = col.drop_index(field);
                    }
                }
            }
        }

        Ok(collections)
    }

    /// Create a new collection.
    pub fn create_collection(&self, name: &str) -> Result<()> {
        let mut wal = self.wal.lock().unwrap();
        let mut collections = self.collections.write().unwrap();

        if collections.contains_key(name) {
            return Err(FluxError::CollectionAlreadyExists(name.to_string()));
        }

        wal.append(WalOperation::CreateCollection {
            name: name.to_string(),
        })?;

        collections.insert(
            name.to_string(),
            Arc::new(RwLock::new(Collection::new(name.to_string()))),
        );
        Ok(())
    }

    /// Drop a collection and all its documents.
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let mut wal = self.wal.lock().unwrap();
        let mut collections = self.collections.write().unwrap();

        if !collections.contains_key(name) {
            return Err(FluxError::CollectionNotFound(name.to_string()));
        }

        wal.append(WalOperation::DropCollection {
            name: name.to_string(),
        })?;

        collections.remove(name);
        Ok(())
    }

    /// List all collection names.
    pub fn list_collections(&self) -> Vec<String> {
        let collections = self.collections.read().unwrap();
        collections.keys().cloned().collect()
    }

    /// Insert a document into a collection.
    pub fn insert(&self, collection: &str, value: Value) -> Result<String> {
        // Get Arc to collection (read-lock the map briefly)
        let col_arc = {
            let collections = self.collections.read().unwrap();
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        let doc = Document::new(value);
        let id = doc.id.clone();
        let data = doc.data.clone();

        // WAL first
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(WalOperation::Insert {
                collection: collection.to_string(),
                doc_id: id.clone(),
                data,
            })?;
        }

        // Then mutate
        let mut col = col_arc.write().unwrap();
        col.insert(doc);

        Ok(id)
    }

    /// Get a document by ID from a collection.
    pub fn get(&self, collection: &str, id: &str) -> Result<Value> {
        let collections = self.collections.read().unwrap();
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = col_arc.read().unwrap();
        let doc = col.get(id)?;
        Ok(doc.to_value())
    }

    /// Update a document by ID with new data.
    pub fn update(&self, collection: &str, id: &str, data: Value) -> Result<()> {
        let col_arc = {
            let collections = self.collections.read().unwrap();
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        // Verify document exists
        {
            let col = col_arc.read().unwrap();
            col.get(id)?;
        }

        let update_data = match &data {
            Value::Object(obj) => {
                let mut clean = obj.clone();
                clean.remove("_id");
                Value::Object(clean)
            }
            other => other.clone(),
        };

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(WalOperation::Update {
                collection: collection.to_string(),
                doc_id: id.to_string(),
                data: update_data.clone(),
            })?;
        }

        let mut col = col_arc.write().unwrap();
        col.update(id, update_data)?;

        Ok(())
    }

    /// Delete a document by ID.
    pub fn delete(&self, collection: &str, id: &str) -> Result<bool> {
        let col_arc = {
            let collections = self.collections.read().unwrap();
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        // Check existence
        {
            let col = col_arc.read().unwrap();
            if col.get(id).is_err() {
                return Ok(false);
            }
        }

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(WalOperation::Delete {
                collection: collection.to_string(),
                doc_id: id.to_string(),
            })?;
        }

        let mut col = col_arc.write().unwrap();
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
        let collections = self.collections.read().unwrap();
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = col_arc.read().unwrap();
        col.find(&filter, projection.as_ref(), limit, skip)
    }

    /// Count documents matching a filter.
    pub fn count(&self, collection: &str, filter: Value) -> Result<usize> {
        let collections = self.collections.read().unwrap();
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = col_arc.read().unwrap();
        col.count(&filter)
    }

    /// Create a secondary index on a collection field.
    pub fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        let col_arc = {
            let collections = self.collections.read().unwrap();
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(WalOperation::CreateIndex {
                collection: collection.to_string(),
                field: field.to_string(),
            })?;
        }

        let mut col = col_arc.write().unwrap();
        col.create_index(field)
    }

    /// Drop a secondary index.
    pub fn drop_index(&self, collection: &str, field: &str) -> Result<()> {
        let col_arc = {
            let collections = self.collections.read().unwrap();
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(WalOperation::DropIndex {
                collection: collection.to_string(),
                field: field.to_string(),
            })?;
        }

        let mut col = col_arc.write().unwrap();
        col.drop_index(field)
    }

    /// List indexes on a collection.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<String>> {
        let collections = self.collections.read().unwrap();
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = col_arc.read().unwrap();
        Ok(col.list_indexes())
    }

    /// Compact the WAL by writing a fresh snapshot of current state.
    pub fn compact(&self) -> Result<()> {
        let mut wal = self.wal.lock().unwrap();
        wal.flush()?;
        wal.truncate()?;

        let collections = self.collections.read().unwrap();
        for (name, col_arc) in collections.iter() {
            wal.append(WalOperation::CreateCollection {
                name: name.clone(),
            })?;

            let col = col_arc.read().unwrap();
            for doc_id in col.doc_ids() {
                if let Ok(doc) = col.get(&doc_id) {
                    wal.append(WalOperation::Insert {
                        collection: name.clone(),
                        doc_id: doc.id.clone(),
                        data: doc.data.clone(),
                    })?;
                }
            }

            for field in col.list_indexes() {
                wal.append(WalOperation::CreateIndex {
                    collection: name.clone(),
                    field,
                })?;
            }
        }

        wal.flush()?;
        Ok(())
    }

    /// Get database statistics.
    pub fn stats(&self) -> Value {
        let collections = self.collections.read().unwrap();
        let mut collections_info = serde_json::Map::new();
        let mut total_docs = 0usize;

        for (name, col_arc) in collections.iter() {
            let col = col_arc.read().unwrap();
            let count = col.len();
            let indexes = col.list_indexes();
            total_docs += count;
            collections_info.insert(
                name.clone(),
                serde_json::json!({
                    "documents": count,
                    "indexes": indexes,
                }),
            );
        }

        serde_json::json!({
            "database": self.name,
            "collections": collections.len(),
            "total_documents": total_docs,
            "collection_details": collections_info,
        })
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if let Ok(mut wal) = self.wal.lock() {
            let _ = wal.flush();
        }
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
        let (db, _dir) = test_db();
        db.create_collection("users").unwrap();
        db.create_collection("orders").unwrap();

        let mut collections = db.list_collections();
        collections.sort();
        assert_eq!(collections, vec!["orders", "users"]);
    }

    #[test]
    fn test_duplicate_collection_error() {
        let (db, _dir) = test_db();
        db.create_collection("users").unwrap();
        assert!(db.create_collection("users").is_err());
    }

    #[test]
    fn test_insert_and_get() {
        let (db, _dir) = test_db();
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
        let (db, _dir) = test_db();
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
        let (db, _dir) = test_db();
        db.create_collection("users").unwrap();
        let id = db.insert("users", json!({"name": "Alice"})).unwrap();

        assert!(db.delete("users", &id).unwrap());
        assert!(!db.delete("users", &id).unwrap());
        assert!(db.get("users", &id).is_err());
    }

    #[test]
    fn test_find() {
        let (db, _dir) = test_db();
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

        {
            let db = Database::open(dir.path()).unwrap();
            db.create_collection("users").unwrap();
            db.insert("users", json!({"_id": "1", "name": "Alice"}))
                .unwrap();
            db.insert("users", json!({"_id": "2", "name": "Bob"}))
                .unwrap();
            // Drop flushes the WAL
        }

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
            let db = Database::open(dir.path()).unwrap();
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
            let db = Database::open(dir.path()).unwrap();
            db.create_collection("users").unwrap();
            db.insert("users", json!({"_id": "1", "name": "Alice"}))
                .unwrap();
            db.insert("users", json!({"_id": "2", "name": "Bob"}))
                .unwrap();
            db.delete("users", "2").unwrap();
            db.compact().unwrap();
        }

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
        let (db, _dir) = test_db();
        db.create_collection("users").unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();

        db.drop_collection("users").unwrap();
        assert!(db.list_collections().is_empty());
        assert!(db.insert("users", json!({"name": "Bob"})).is_err());
    }

    #[test]
    fn test_stats() {
        let (db, _dir) = test_db();
        db.create_collection("users").unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();
        db.insert("users", json!({"name": "Bob"})).unwrap();

        let stats = db.stats();
        assert_eq!(stats["collections"], 1);
        assert_eq!(stats["total_documents"], 2);
    }

    #[test]
    fn test_index_persistence() {
        let dir = tempdir().unwrap();

        {
            let db = Database::open(dir.path()).unwrap();
            db.create_collection("users").unwrap();
            db.insert("users", json!({"_id": "1", "age": 30}))
                .unwrap();
            db.create_index("users", "age").unwrap();
        }

        {
            let db = Database::open(dir.path()).unwrap();
            let indexes = db.list_indexes("users").unwrap();
            assert_eq!(indexes, vec!["age"]);

            // Index should be functional after replay
            let results = db
                .find("users", json!({"age": 30}), None, None, None)
                .unwrap();
            assert_eq!(results.len(), 1);
        }
    }

    #[test]
    fn test_concurrent_reads() {
        let (db, _dir) = test_db();
        let db = Arc::new(db);
        db.create_collection("users").unwrap();

        for i in 0..100 {
            db.insert("users", json!({"_id": format!("{i}"), "n": i}))
                .unwrap();
        }

        // Spawn multiple reader threads
        let mut handles = vec![];
        for _ in 0..8 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let doc = db.get("users", &format!("{i}")).unwrap();
                    assert_eq!(doc["n"], i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
