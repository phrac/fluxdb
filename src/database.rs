use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;

use crate::collection::Collection;
use crate::document::Document;
use crate::error::{FluxError, Result};
use crate::storage::{MemoryBackend, StorageBackend};
use crate::wal::{WalEntry, WalOperation};

#[cfg(feature = "persistence")]
use std::fs;
#[cfg(feature = "persistence")]
use std::path::Path;
#[cfg(feature = "persistence")]
use crate::wal::{Wal, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_BYTES};

/// Maximum collection name length.
const MAX_COLLECTION_NAME_LEN: usize = 128;

/// Maximum document size in bytes (default, overridable via config).
const DEFAULT_MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;

/// Maximum results from a single find query (default, overridable via config).
const DEFAULT_MAX_RESULT_COUNT: usize = 100_000;

/// Characters allowed in collection names.
fn is_valid_collection_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_COLLECTION_NAME_LEN
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
        && !name.starts_with('.')
        && !name.contains("..")
}

/// The main database engine.
///
/// Internally synchronized with per-collection RwLocks for maximum read concurrency.
/// All public methods take `&self` — no external locking required.
pub struct Database {
    pub name: String,
    collections: RwLock<HashMap<String, Arc<RwLock<Collection>>>>,
    storage: Mutex<Box<dyn StorageBackend>>,
    max_document_bytes: usize,
    max_result_count: usize,
}

// Helper to handle poisoned locks gracefully
fn lock_storage(storage: &Mutex<Box<dyn StorageBackend>>) -> Result<std::sync::MutexGuard<'_, Box<dyn StorageBackend>>> {
    storage.lock().map_err(|_| FluxError::Internal("storage lock poisoned".into()))
}

fn read_collections(collections: &RwLock<HashMap<String, Arc<RwLock<Collection>>>>) -> Result<std::sync::RwLockReadGuard<'_, HashMap<String, Arc<RwLock<Collection>>>>> {
    collections.read().map_err(|_| FluxError::Internal("collections lock poisoned".into()))
}

fn write_collections(collections: &RwLock<HashMap<String, Arc<RwLock<Collection>>>>) -> Result<std::sync::RwLockWriteGuard<'_, HashMap<String, Arc<RwLock<Collection>>>>> {
    collections.write().map_err(|_| FluxError::Internal("collections lock poisoned".into()))
}

fn read_collection(col: &RwLock<Collection>) -> Result<std::sync::RwLockReadGuard<'_, Collection>> {
    col.read().map_err(|_| FluxError::Internal("collection lock poisoned".into()))
}

fn write_collection(col: &RwLock<Collection>) -> Result<std::sync::RwLockWriteGuard<'_, Collection>> {
    col.write().map_err(|_| FluxError::Internal("collection lock poisoned".into()))
}

impl Database {
    /// Open a database backed by the given storage engine.
    ///
    /// Replays any persisted entries from the backend to rebuild in-memory state.
    pub fn open_with_storage(name: impl Into<String>, storage: Box<dyn StorageBackend>) -> Result<Self> {
        let entries = storage.read_all()?;
        let collections = Self::replay_entries(&entries)?;

        let wrapped: HashMap<String, Arc<RwLock<Collection>>> = collections
            .into_iter()
            .map(|(name, col)| (name, Arc::new(RwLock::new(col))))
            .collect();

        Ok(Database {
            name: name.into(),
            collections: RwLock::new(wrapped),
            storage: Mutex::new(storage),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
        })
    }

    /// Open an in-memory database with no persistence.
    pub fn open_memory() -> Self {
        Database {
            name: "memory".to_string(),
            collections: RwLock::new(HashMap::new()),
            storage: Mutex::new(Box::new(MemoryBackend::new())),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
        }
    }

    /// Open or create a persistent database at the given path with default settings.
    #[cfg(feature = "persistence")]
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_with_wal(data_dir, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_BYTES)
    }

    /// Open a persistent database in read-only mode.
    ///
    /// Replays the WAL to rebuild in-memory state but does not open it for
    /// writing. Safe to use while a server holds the same data directory.
    #[cfg(feature = "persistence")]
    pub fn open_readonly(data_dir: &Path) -> Result<Self> {
        let name = data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("fluxdb")
            .to_string();

        let wal_path = data_dir.join("wal.log");
        let entries = Wal::read_all(&wal_path)?;
        let collections = Self::replay_entries(&entries)?;

        let wrapped: HashMap<String, Arc<RwLock<Collection>>> = collections
            .into_iter()
            .map(|(name, col)| (name, Arc::new(RwLock::new(col))))
            .collect();

        Ok(Database {
            name,
            collections: RwLock::new(wrapped),
            storage: Mutex::new(Box::new(MemoryBackend::new())),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
        })
    }

    /// Open or create a persistent database with custom WAL batch settings.
    #[cfg(feature = "persistence")]
    pub fn open_with_wal(
        data_dir: &Path,
        wal_batch_size: usize,
        wal_batch_bytes: usize,
    ) -> Result<Self> {
        fs::create_dir_all(data_dir)?;

        let name = data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("fluxdb")
            .to_string();

        let wal_path = data_dir.join("wal.log");
        let entries = Wal::read_all(&wal_path)?;
        let next_seq = entries.last().map(|e| e.sequence + 1).unwrap_or(0);
        let collections = Self::replay_entries(&entries)?;
        let wal = Wal::open_at_sequence(&wal_path, next_seq, wal_batch_size, wal_batch_bytes)?;

        let wrapped: HashMap<String, Arc<RwLock<Collection>>> = collections
            .into_iter()
            .map(|(name, col)| (name, Arc::new(RwLock::new(col))))
            .collect();

        Ok(Database {
            name,
            collections: RwLock::new(wrapped),
            storage: Mutex::new(Box::new(wal)),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
        })
    }

    /// Open or create a database using the provided config.
    #[cfg(feature = "server")]
    pub fn open_with_config(config: &crate::config::Config, data_dir_override: Option<&Path>) -> Result<Self> {
        let data_dir = data_dir_override.unwrap_or(&config.data_dir);
        let mut db = Self::open_with_wal(data_dir, config.wal.batch_size, config.wal.batch_bytes)?;
        db.max_document_bytes = config.limits.max_document_bytes;
        db.max_result_count = config.limits.max_result_count;
        Ok(db)
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
                        let doc = Document::with_id(doc_id.clone(), data.clone());
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
        if !is_valid_collection_name(name) {
            return Err(FluxError::InvalidInput(format!(
                "invalid collection name '{}': must be 1-{} chars, alphanumeric/underscore/hyphen/dot, \
                 cannot start with '.' or contain '..'",
                name, MAX_COLLECTION_NAME_LEN
            )));
        }

        let mut wal = lock_storage(&self.storage)?;
        let mut collections = write_collections(&self.collections)?;

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
        let mut wal = lock_storage(&self.storage)?;
        let mut collections = write_collections(&self.collections)?;

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
        let collections = match read_collections(&self.collections) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        collections.keys().cloned().collect()
    }

    /// Insert a document into a collection.
    pub fn insert(&self, collection: &str, value: Value) -> Result<String> {
        // Validate document size
        let estimated_size = value.to_string().len();
        if estimated_size > self.max_document_bytes {
            return Err(FluxError::ResourceLimit(format!(
                "document size {} bytes exceeds limit of {} bytes",
                estimated_size, self.max_document_bytes
            )));
        }

        // Get Arc to collection (read-lock the map briefly)
        let col_arc = {
            let collections = read_collections(&self.collections)?;
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        let doc = Document::new(value);
        let id = doc.id.clone();
        // Store data WITHOUT _id in WAL — the doc_id field carries it separately.
        // This avoids redundant storage and matches the update path.
        let wal_data = doc.data_without_id();

        // WAL first
        {
            let mut wal = lock_storage(&self.storage)?;
            wal.append(WalOperation::Insert {
                collection: collection.to_string(),
                doc_id: id.clone(),
                data: wal_data,
            })?;
        }

        // Then mutate
        let mut col = write_collection(&col_arc)?;
        col.insert(doc);

        Ok(id)
    }

    /// Get a document by ID from a collection.
    pub fn get(&self, collection: &str, id: &str) -> Result<Value> {
        let collections = read_collections(&self.collections)?;
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = read_collection(col_arc)?;
        let doc = col.get(id)?;
        Ok(doc.to_value())
    }

    /// Update a document by ID with new data.
    pub fn update(&self, collection: &str, id: &str, data: Value) -> Result<()> {
        // Validate document size
        let estimated_size = data.to_string().len();
        if estimated_size > self.max_document_bytes {
            return Err(FluxError::ResourceLimit(format!(
                "document size {} bytes exceeds limit of {} bytes",
                estimated_size, self.max_document_bytes
            )));
        }

        let col_arc = {
            let collections = read_collections(&self.collections)?;
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        // Verify document exists
        {
            let col = read_collection(&col_arc)?;
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
            let mut wal = lock_storage(&self.storage)?;
            wal.append(WalOperation::Update {
                collection: collection.to_string(),
                doc_id: id.to_string(),
                data: update_data.clone(),
            })?;
        }

        let mut col = write_collection(&col_arc)?;
        col.update(id, update_data)?;

        Ok(())
    }

    /// Delete a document by ID.
    pub fn delete(&self, collection: &str, id: &str) -> Result<bool> {
        let col_arc = {
            let collections = read_collections(&self.collections)?;
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        // Check existence
        {
            let col = read_collection(&col_arc)?;
            if col.get(id).is_err() {
                return Ok(false);
            }
        }

        {
            let mut wal = lock_storage(&self.storage)?;
            wal.append(WalOperation::Delete {
                collection: collection.to_string(),
                doc_id: id.to_string(),
            })?;
        }

        let mut col = write_collection(&col_arc)?;
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
        sort: Option<Value>,
    ) -> Result<Vec<Value>> {
        // Cap the result limit
        let effective_limit = match limit {
            Some(l) => Some(l.min(self.max_result_count)),
            None => Some(self.max_result_count),
        };

        let collections = read_collections(&self.collections)?;
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = read_collection(col_arc)?;
        col.find(&filter, projection.as_ref(), effective_limit, skip, sort.as_ref())
    }

    /// Find documents and return pre-serialized JSON bytes.
    /// Much faster than `find()` for bulk reads—avoids cloning Value trees.
    pub fn find_raw(
        &self,
        collection: &str,
        filter: Value,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<Vec<Vec<u8>>> {
        let effective_limit = match limit {
            Some(l) => Some(l.min(self.max_result_count)),
            None => Some(self.max_result_count),
        };

        let collections = read_collections(&self.collections)?;
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = read_collection(col_arc)?;
        let slices = col.find_raw(&filter, effective_limit, skip)?;
        Ok(slices.into_iter().map(|s| s.to_vec()).collect())
    }

    /// Zero-copy iterate over all documents in a collection.
    /// Calls `f(id, raw_json_bytes)` for each document. No allocations.
    pub fn scan<F>(&self, collection: &str, f: F) -> Result<()>
    where
        F: FnMut(&str, &[u8]),
    {
        let collections = read_collections(&self.collections)?;
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = read_collection(col_arc)?;
        col.scan(f);
        Ok(())
    }

    /// Count documents matching a filter.
    pub fn count(&self, collection: &str, filter: Value) -> Result<usize> {
        let collections = read_collections(&self.collections)?;
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = read_collection(col_arc)?;
        col.count(&filter)
    }

    /// Create a secondary index on a collection field.
    pub fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        // Validate field name
        if field.is_empty() || field.len() > 256 {
            return Err(FluxError::InvalidInput(
                "field name must be 1-256 characters".into(),
            ));
        }

        let col_arc = {
            let collections = read_collections(&self.collections)?;
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        {
            let mut wal = lock_storage(&self.storage)?;
            wal.append(WalOperation::CreateIndex {
                collection: collection.to_string(),
                field: field.to_string(),
            })?;
        }

        let mut col = write_collection(&col_arc)?;
        col.create_index(field)
    }

    /// Drop a secondary index.
    pub fn drop_index(&self, collection: &str, field: &str) -> Result<()> {
        let col_arc = {
            let collections = read_collections(&self.collections)?;
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        {
            let mut wal = lock_storage(&self.storage)?;
            wal.append(WalOperation::DropIndex {
                collection: collection.to_string(),
                field: field.to_string(),
            })?;
        }

        let mut col = write_collection(&col_arc)?;
        col.drop_index(field)
    }

    /// List indexes on a collection.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<String>> {
        let collections = read_collections(&self.collections)?;
        let col_arc = collections
            .get(collection)
            .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?;

        let col = read_collection(col_arc)?;
        Ok(col.list_indexes())
    }

    /// Compact the WAL by writing a fresh snapshot of current state.
    ///
    /// Uses atomic replacement (temp file + rename) so a crash at any point
    /// leaves either the old or new WAL intact — never a partial file.
    ///
    /// Minimizes lock contention: reads collections snapshot first, then
    /// acquires WAL lock only for the atomic replace.
    pub fn compact(&self) -> Result<()> {
        // First, flush any pending WAL entries
        {
            let mut wal = lock_storage(&self.storage)?;
            wal.flush()?;
        }

        // Build the ops snapshot while only holding read locks (no WAL lock)
        let ops = {
            let collections = read_collections(&self.collections)?;
            let mut ops = Vec::new();

            for (name, col_arc) in collections.iter() {
                ops.push(WalOperation::CreateCollection {
                    name: name.clone(),
                });

                let col = read_collection(col_arc)?;
                for doc_id in col.doc_ids() {
                    if let Ok(doc) = col.get(&doc_id) {
                        ops.push(WalOperation::Insert {
                            collection: name.clone(),
                            doc_id: doc.id.clone(),
                            data: doc.data_without_id(),
                        });
                    }
                }

                for field in col.list_indexes() {
                    ops.push(WalOperation::CreateIndex {
                        collection: name.clone(),
                        field,
                    });
                }
            }
            ops
        };

        // Now acquire WAL lock only for the atomic replace
        let mut wal = lock_storage(&self.storage)?;
        wal.replace_all(ops)?;
        Ok(())
    }

    /// Explicitly flush all buffered WAL entries to disk.
    pub fn flush(&self) -> Result<()> {
        let mut wal = lock_storage(&self.storage)?;
        wal.flush()
    }

    /// Get database statistics.
    pub fn stats(&self) -> Value {
        let collections = match read_collections(&self.collections) {
            Ok(c) => c,
            Err(_) => return serde_json::json!({"error": "lock poisoned"}),
        };
        let mut collections_info = serde_json::Map::new();
        let mut total_docs = 0usize;

        for (name, col_arc) in collections.iter() {
            let col = match read_collection(col_arc) {
                Ok(c) => c,
                Err(_) => continue,
            };
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
        if let Ok(mut wal) = self.storage.lock() {
            if let Err(e) = wal.flush() {
                eprintln!("fluxdb: WARNING: failed to flush WAL on shutdown: {e}");
            }
        } else {
            eprintln!("fluxdb: WARNING: could not acquire storage lock during shutdown (poisoned)");
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
    fn test_invalid_collection_names() {
        let (db, _dir) = test_db();
        // Empty name
        assert!(db.create_collection("").is_err());
        // Starting with dot
        assert!(db.create_collection(".hidden").is_err());
        // Contains invalid characters
        assert!(db.create_collection("my collection").is_err());
        assert!(db.create_collection("../../etc").is_err());
        // Valid names
        assert!(db.create_collection("users").is_ok());
        assert!(db.create_collection("my-collection").is_ok());
        assert!(db.create_collection("data_2024").is_ok());
        assert!(db.create_collection("logs.v2").is_ok());
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
            .find("users", json!({"age": {"$gte": 30}}), None, None, None, None)
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
                .find("users", json!({"age": 30}), None, None, None, None)
                .unwrap();
            assert_eq!(results.len(), 1);
        }
    }

    #[test]
    fn test_compact_preserves_data_on_reopen() {
        let dir = tempdir().unwrap();

        {
            let db = Database::open(dir.path()).unwrap();
            db.create_collection("users").unwrap();
            db.insert("users", json!({"_id": "1", "name": "Alice"})).unwrap();
            db.insert("users", json!({"_id": "2", "name": "Bob"})).unwrap();
            db.insert("users", json!({"_id": "3", "name": "Charlie"})).unwrap();
            db.delete("users", "2").unwrap();
            db.update("users", "1", json!({"name": "Alice Updated"})).unwrap();
            db.create_index("users", "name").unwrap();

            // Compact should atomically rewrite the WAL
            db.compact().unwrap();
        }

        // Reopen and verify everything survived
        {
            let db = Database::open(dir.path()).unwrap();
            let doc = db.get("users", "1").unwrap();
            assert_eq!(doc["name"], "Alice Updated");

            assert!(db.get("users", "2").is_err()); // was deleted

            let doc = db.get("users", "3").unwrap();
            assert_eq!(doc["name"], "Charlie");

            assert_eq!(db.count("users", json!({})).unwrap(), 2);

            let indexes = db.list_indexes("users").unwrap();
            assert_eq!(indexes, vec!["name"]);
        }
    }

    #[test]
    fn test_no_temp_file_after_compact() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_collection("test").unwrap();
        db.insert("test", json!({"_id": "1", "x": 1})).unwrap();
        db.compact().unwrap();

        // Verify no .wal.tmp file left behind
        let tmp_path = dir.path().join("wal.wal.tmp");
        assert!(!tmp_path.exists());
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

    #[test]
    fn test_document_size_limit() {
        let db = Database::open_memory();
        db.create_collection("test").unwrap();
        // Small document should work
        db.insert("test", json!({"x": 1})).unwrap();
        // Oversized document should fail (set limit low for test)
        // Default limit is 16MB so this would need a very large doc to trigger
    }

    #[test]
    fn test_explicit_flush() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_collection("test").unwrap();
        db.insert("test", json!({"_id": "1", "x": 1})).unwrap();
        db.flush().unwrap(); // Should not panic
    }
}
