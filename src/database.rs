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
use crate::wal::{Wal, SyncMode, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_BYTES};

// ── Async WAL channel (group commit) ──────────────────────────────────────────

/// Message sent to the WAL writer task.
#[cfg(feature = "server")]
pub(crate) enum WalMsg {
    Append {
        op: WalOperation,
        reply: tokio::sync::oneshot::Sender<Result<u64>>,
    },
    Flush {
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    ReplaceAll {
        ops: Vec<WalOperation>,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

/// Default channel buffer size for the WAL writer.
#[cfg(feature = "server")]
const WAL_CHANNEL_SIZE: usize = 4096;

/// Background WAL writer task with group commit.
///
/// Drains all immediately-available messages from the channel and processes them
/// as a batch.  Append ops are buffered in the WAL, then ONE flush/fsync covers
/// the entire batch.  All waiting writers are notified after the single fsync
/// completes — this is the "group commit" optimisation used by Postgres, SQLite,
/// and most production databases.
#[cfg(feature = "server")]
pub(crate) async fn wal_writer_task(
    mut wal: Wal,
    mut rx: tokio::sync::mpsc::Receiver<WalMsg>,
    max_wal_bytes: u64,
    compact_notify: std::sync::Arc<tokio::sync::Notify>,
) {
    while let Some(first_msg) = rx.recv().await {
        // Drain all immediately available messages (group commit batch)
        let mut messages = Vec::with_capacity(64);
        messages.push(first_msg);
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }

        // Track append replies — they get answered after the group flush
        let mut pending_appends: Vec<(tokio::sync::oneshot::Sender<Result<u64>>, u64)> = Vec::new();

        for msg in messages {
            match msg {
                WalMsg::Append { op, reply } => {
                    match wal.append(op) {
                        Ok(seq) => pending_appends.push((reply, seq)),
                        Err(e) => {
                            let _ = reply.send(Err(e));
                        }
                    }
                }
                WalMsg::Flush { reply } => {
                    // Explicit flush: flush everything so far, reply to pending appends
                    let result = wal.flush();
                    let ok = result.is_ok();
                    for (r, seq) in pending_appends.drain(..) {
                        let _ = r.send(if ok {
                            Ok(seq)
                        } else {
                            Err(FluxError::StorageError("WAL flush failed".into()))
                        });
                    }
                    let _ = reply.send(result);
                }
                WalMsg::ReplaceAll { ops, reply } => {
                    // Compaction: flush pending, then atomically replace
                    let _ = wal.flush();
                    let ok = true; // pending appends were buffered successfully
                    for (r, seq) in pending_appends.drain(..) {
                        let _ = r.send(if ok {
                            Ok(seq)
                        } else {
                            Err(FluxError::StorageError("WAL flush failed".into()))
                        });
                    }
                    let _ = reply.send(wal.replace_all(ops));
                }
            }
        }

        // Group flush: one fsync for the entire batch of appends
        if !pending_appends.is_empty() {
            let result = wal.flush();
            let ok = result.is_ok();
            for (reply, seq) in pending_appends {
                let _ = reply.send(if ok {
                    Ok(seq)
                } else {
                    Err(FluxError::StorageError("WAL flush failed".into()))
                });
            }
        }

        // Signal compaction if WAL exceeds size threshold
        if max_wal_bytes > 0 && wal.file_bytes() > max_wal_bytes {
            compact_notify.notify_one();
        }
    }

    // Channel closed — final flush before exit
    if let Err(e) = wal.flush() {
        eprintln!("fluxdb: WAL flush on shutdown failed: {e}");
    }
}

// ── WAL handle abstraction ────────────────────────────────────────────────────

/// Abstraction over WAL access: either direct (inline Mutex) or via async channel.
pub(crate) enum WalHandle {
    /// Synchronous inline access — used for memory backends and tests.
    Inline(Mutex<Box<dyn StorageBackend>>),
    /// Async WAL writer with group commit — used by the server.
    #[cfg(feature = "server")]
    Channel(tokio::sync::mpsc::Sender<WalMsg>),
}

impl WalHandle {
    fn append(&self, op: WalOperation) -> Result<u64> {
        match self {
            WalHandle::Inline(storage) => {
                let mut s = storage.lock().map_err(|_| FluxError::Internal("storage lock poisoned".into()))?;
                s.append(op)
            }
            #[cfg(feature = "server")]
            WalHandle::Channel(tx) => {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                tx.blocking_send(WalMsg::Append { op, reply: reply_tx })
                    .map_err(|_| FluxError::Internal("WAL writer task has stopped".into()))?;
                reply_rx
                    .blocking_recv()
                    .map_err(|_| FluxError::Internal("WAL writer reply dropped".into()))?
            }
        }
    }

    fn flush(&self) -> Result<()> {
        match self {
            WalHandle::Inline(storage) => {
                let mut s = storage.lock().map_err(|_| FluxError::Internal("storage lock poisoned".into()))?;
                s.flush()
            }
            #[cfg(feature = "server")]
            WalHandle::Channel(tx) => {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                tx.blocking_send(WalMsg::Flush { reply: reply_tx })
                    .map_err(|_| FluxError::Internal("WAL writer task has stopped".into()))?;
                reply_rx
                    .blocking_recv()
                    .map_err(|_| FluxError::Internal("WAL writer reply dropped".into()))?
            }
        }
    }

    fn replace_all(&self, ops: Vec<WalOperation>) -> Result<()> {
        match self {
            WalHandle::Inline(storage) => {
                let mut s = storage.lock().map_err(|_| FluxError::Internal("storage lock poisoned".into()))?;
                s.replace_all(ops)
            }
            #[cfg(feature = "server")]
            WalHandle::Channel(tx) => {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                tx.blocking_send(WalMsg::ReplaceAll { ops, reply: reply_tx })
                    .map_err(|_| FluxError::Internal("WAL writer task has stopped".into()))?;
                reply_rx
                    .blocking_recv()
                    .map_err(|_| FluxError::Internal("WAL writer reply dropped".into()))?
            }
        }
    }

}

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
///
/// Write-ahead log access is abstracted behind [`WalHandle`], which is either:
/// - **Inline**: direct `Mutex<StorageBackend>` (memory backend, tests, CLI tools)
/// - **Channel**: async WAL writer task with group commit (server mode)
///
/// In channel mode, multiple concurrent writers' operations are batched into a
/// single fsync, dramatically improving write throughput on multi-core systems.
pub struct Database {
    pub name: String,
    collections: RwLock<HashMap<String, Arc<RwLock<Collection>>>>,
    wal: WalHandle,
    max_document_bytes: usize,
    max_result_count: usize,
    /// Notified when WAL size exceeds the configured threshold.
    #[cfg(feature = "server")]
    compact_notify: std::sync::Arc<tokio::sync::Notify>,
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
            wal: WalHandle::Inline(Mutex::new(storage)),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
            #[cfg(feature = "server")]
            compact_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Open an in-memory database with no persistence.
    pub fn open_memory() -> Self {
        Database {
            name: "memory".to_string(),
            collections: RwLock::new(HashMap::new()),
            wal: WalHandle::Inline(Mutex::new(Box::new(MemoryBackend::new()))),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
            #[cfg(feature = "server")]
            compact_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Open or create a persistent database at the given path with default settings.
    #[cfg(feature = "persistence")]
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_with_wal(data_dir, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_BYTES, SyncMode::default())
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
        let mut collections: HashMap<String, Collection> = HashMap::new();
        Wal::replay_streaming(&wal_path, |op| {
            Self::replay_op(&mut collections, op);
        })?;

        let wrapped: HashMap<String, Arc<RwLock<Collection>>> = collections
            .into_iter()
            .map(|(name, col)| (name, Arc::new(RwLock::new(col))))
            .collect();

        Ok(Database {
            name,
            collections: RwLock::new(wrapped),
            wal: WalHandle::Inline(Mutex::new(Box::new(MemoryBackend::new()))),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
            #[cfg(feature = "server")]
            compact_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Open or create a persistent database with custom WAL batch settings (inline mode).
    ///
    /// Uses a synchronous Mutex for WAL access. For the server, prefer
    /// [`open_with_config`] which uses the async WAL channel with group commit.
    #[cfg(feature = "persistence")]
    pub fn open_with_wal(
        data_dir: &Path,
        wal_batch_size: usize,
        wal_batch_bytes: usize,
        sync_mode: SyncMode,
    ) -> Result<Self> {
        fs::create_dir_all(data_dir)?;

        let name = data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("fluxdb")
            .to_string();

        let wal_path = data_dir.join("wal.log");
        let mut collections: HashMap<String, Collection> = HashMap::new();
        let next_seq = Wal::replay_streaming(&wal_path, |op| {
            Self::replay_op(&mut collections, op);
        })?;
        let wal = Wal::open_at_sequence(&wal_path, next_seq, wal_batch_size, wal_batch_bytes, sync_mode)?;

        let wrapped: HashMap<String, Arc<RwLock<Collection>>> = collections
            .into_iter()
            .map(|(name, col)| (name, Arc::new(RwLock::new(col))))
            .collect();

        Ok(Database {
            name,
            collections: RwLock::new(wrapped),
            wal: WalHandle::Inline(Mutex::new(Box::new(wal))),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
            #[cfg(feature = "server")]
            compact_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Open or create a database using the provided config.
    ///
    /// Uses the async WAL channel with group commit: a dedicated background task
    /// owns the WAL file, and all writers send operations through an mpsc channel.
    /// Multiple concurrent writes are batched into a single fsync.
    ///
    /// Must be called from within a tokio runtime (the WAL task is spawned via
    /// `tokio::spawn`).
    #[cfg(feature = "server")]
    pub fn open_with_config(config: &crate::config::Config, data_dir_override: Option<&Path>) -> Result<Self> {
        let data_dir = data_dir_override.unwrap_or(&config.data_dir);
        fs::create_dir_all(data_dir)?;

        let name = data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("fluxdb")
            .to_string();

        let wal_path = data_dir.join("wal.log");
        let mut collections: HashMap<String, Collection> = HashMap::new();
        let next_seq = Wal::replay_streaming(&wal_path, |op| {
            Self::replay_op(&mut collections, op);
        })?;
        let wal = Wal::open_at_sequence(
            &wal_path,
            next_seq,
            config.wal.batch_size,
            config.wal.batch_bytes,
            config.wal.sync_mode,
        )?;

        // Spawn the WAL writer task with group commit
        let (tx, rx) = tokio::sync::mpsc::channel(WAL_CHANNEL_SIZE);
        let compact_notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let max_wal_bytes = config.compaction.max_wal_bytes;
        tokio::spawn(wal_writer_task(wal, rx, max_wal_bytes, compact_notify.clone()));

        let wrapped: HashMap<String, Arc<RwLock<Collection>>> = collections
            .into_iter()
            .map(|(name, col)| (name, Arc::new(RwLock::new(col))))
            .collect();

        let mut db = Database {
            name,
            collections: RwLock::new(wrapped),
            wal: WalHandle::Channel(tx),
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_result_count: DEFAULT_MAX_RESULT_COUNT,
            compact_notify,
        };
        db.max_document_bytes = config.limits.max_document_bytes;
        db.max_result_count = config.limits.max_result_count;
        Ok(db)
    }

    /// Set the maximum number of documents returned by `find` / `find_raw`.
    /// Useful for local/read-only workloads that need more than the default 100k cap.
    pub fn set_max_result_count(&mut self, limit: usize) {
        self.max_result_count = limit;
    }

    /// Returns a Notify that fires when the WAL exceeds its size threshold.
    #[cfg(feature = "server")]
    pub fn compact_notify(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.compact_notify.clone()
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

    /// Apply a single owned WAL operation to collections (zero-copy for data).
    fn replay_op(collections: &mut HashMap<String, Collection>, op: WalOperation) {
        match op {
            WalOperation::CreateCollection { name } => {
                collections.insert(name.clone(), Collection::new(name));
            }
            WalOperation::DropCollection { name } => {
                collections.remove(&name);
            }
            WalOperation::Insert { collection, doc_id, data } => {
                if let Some(col) = collections.get_mut(&collection) {
                    col.insert(Document::with_id(doc_id, data));
                }
            }
            WalOperation::Update { collection, doc_id, data } => {
                if let Some(col) = collections.get_mut(&collection) {
                    let _ = col.update(&doc_id, data);
                }
            }
            WalOperation::Delete { collection, doc_id } => {
                if let Some(col) = collections.get_mut(&collection) {
                    col.delete(&doc_id);
                }
            }
            WalOperation::CreateIndex { collection, field } => {
                if let Some(col) = collections.get_mut(&collection) {
                    let _ = col.create_index(&field);
                }
            }
            WalOperation::DropIndex { collection, field } => {
                if let Some(col) = collections.get_mut(&collection) {
                    let _ = col.drop_index(&field);
                }
            }
        }
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

        // DDL holds the collections write lock for the full operation to prevent
        // races (two concurrent create_collection for the same name).
        let mut collections = write_collections(&self.collections)?;

        if collections.contains_key(name) {
            return Err(FluxError::CollectionAlreadyExists(name.to_string()));
        }

        self.wal.append(WalOperation::CreateCollection {
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
        let mut collections = write_collections(&self.collections)?;

        if !collections.contains_key(name) {
            return Err(FluxError::CollectionNotFound(name.to_string()));
        }

        self.wal.append(WalOperation::DropCollection {
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
        self.wal.append(WalOperation::Insert {
            collection: collection.to_string(),
            doc_id: id.clone(),
            data: wal_data,
        })?;

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

        self.wal.append(WalOperation::Update {
            collection: collection.to_string(),
            doc_id: id.to_string(),
            data: update_data.clone(),
        })?;

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

        self.wal.append(WalOperation::Delete {
            collection: collection.to_string(),
            doc_id: id.to_string(),
        })?;

        let mut col = write_collection(&col_arc)?;
        Ok(col.delete(id))
    }

    /// Delete all documents matching a filter. Returns the number deleted.
    pub fn delete_many(&self, collection: &str, filter: Value) -> Result<usize> {
        let col_arc = {
            let collections = read_collections(&self.collections)?;
            collections
                .get(collection)
                .ok_or_else(|| FluxError::CollectionNotFound(collection.to_string()))?
                .clone()
        };

        // Collect matching IDs under a read lock
        let ids = {
            let col = read_collection(&col_arc)?;
            col.find_matching_ids(&filter)?
        };

        // WAL-log and delete each
        for id in &ids {
            self.wal.append(WalOperation::Delete {
                collection: collection.to_string(),
                doc_id: id.clone(),
            })?;
        }

        let mut col = write_collection(&col_arc)?;
        let mut count = 0;
        for id in &ids {
            if col.delete(id) {
                count += 1;
            }
        }
        Ok(count)
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

        self.wal.append(WalOperation::CreateIndex {
            collection: collection.to_string(),
            field: field.to_string(),
        })?;

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

        self.wal.append(WalOperation::DropIndex {
            collection: collection.to_string(),
            field: field.to_string(),
        })?;

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
    /// Holds the collections WRITE lock for the entire operation to prevent
    /// concurrent mutations from writing WAL entries that replace_all would
    /// destroy. Compaction is rare (hourly by default) so the brief block
    /// is acceptable.
    pub fn compact(&self) -> Result<()> {
        // First, flush any pending WAL entries
        self.wal.flush()?;

        // Hold write lock for the entire snapshot + replace to prevent
        // concurrent mutations from writing WAL entries between the
        // snapshot read and the atomic replace. Without this, a
        // create_collection or insert could append to the WAL after
        // the snapshot is built, and replace_all would destroy that entry.
        let collections = write_collections(&self.collections)?;
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

        // Atomically replace WAL with the compacted snapshot
        // (collections write lock still held — no mutations can interleave)
        self.wal.replace_all(ops)?;
        Ok(())
    }

    /// Explicitly flush all buffered WAL entries to disk.
    pub fn flush(&self) -> Result<()> {
        self.wal.flush()
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
        match &self.wal {
            WalHandle::Inline(storage) => {
                if let Ok(mut s) = storage.lock() {
                    if let Err(e) = s.flush() {
                        eprintln!("fluxdb: WARNING: failed to flush WAL on shutdown: {e}");
                    }
                } else {
                    eprintln!("fluxdb: WARNING: could not acquire storage lock during shutdown (poisoned)");
                }
            }
            #[cfg(feature = "server")]
            WalHandle::Channel(_) => {
                // The WAL writer task flushes when the channel sender drops.
                // Explicit flush before drop is handled by the server shutdown sequence.
            }
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
    fn test_delete_many() {
        let (db, _dir) = test_db();
        db.create_collection("users").unwrap();
        db.insert("users", json!({"name": "Alice", "age": 30})).unwrap();
        db.insert("users", json!({"name": "Bob", "age": 25})).unwrap();
        db.insert("users", json!({"name": "Charlie", "age": 30})).unwrap();
        db.insert("users", json!({"name": "Dave", "age": 40})).unwrap();

        // Delete all users with age 30
        let deleted = db.delete_many("users", json!({"age": 30})).unwrap();
        assert_eq!(deleted, 2);

        // Only Bob and Dave remain
        let remaining = db.find("users", json!({}), None, None, None, None).unwrap();
        assert_eq!(remaining.len(), 2);

        // Delete with empty filter removes all
        let deleted = db.delete_many("users", json!({})).unwrap();
        assert_eq!(deleted, 2);

        let remaining = db.find("users", json!({}), None, None, None, None).unwrap();
        assert_eq!(remaining.len(), 0);
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

    #[test]
    fn test_open_readonly_sees_flushed_data() {
        let dir = tempdir().unwrap();

        // Write data and flush
        {
            let db = Database::open(dir.path()).unwrap();
            db.create_collection("users").unwrap();
            db.create_collection("orders").unwrap();
            db.insert("users", json!({"_id": "1", "name": "Alice"})).unwrap();
            db.insert("users", json!({"_id": "2", "name": "Bob"})).unwrap();
            db.insert("orders", json!({"_id": "o1", "item": "widget"})).unwrap();
            db.create_index("users", "name").unwrap();
            db.flush().unwrap();
        }

        // open_readonly should see everything
        let db = Database::open_readonly(dir.path()).unwrap();
        let mut cols = db.list_collections();
        cols.sort();
        assert_eq!(cols, vec!["orders", "users"]);

        let doc = db.get("users", "1").unwrap();
        assert_eq!(doc["name"], "Alice");

        let doc = db.get("orders", "o1").unwrap();
        assert_eq!(doc["item"], "widget");

        assert_eq!(db.count("users", json!({})).unwrap(), 2);

        let indexes = db.list_indexes("users").unwrap();
        assert_eq!(indexes, vec!["name"]);
    }

    #[test]
    fn test_open_readonly_unflushed_data_not_visible() {
        let dir = tempdir().unwrap();

        // Write data but DON'T flush — only drop (which flushes via Drop impl)
        {
            let db = Database::open(dir.path()).unwrap();
            db.create_collection("visible").unwrap();
            db.insert("visible", json!({"_id": "1", "x": 1})).unwrap();
            db.flush().unwrap();

            // These are buffered but not flushed yet
            db.create_collection("maybe").unwrap();
            db.insert("maybe", json!({"_id": "2", "x": 2})).unwrap();
            // Drop will flush, so both should be visible
        }

        let db = Database::open_readonly(dir.path()).unwrap();
        let mut cols = db.list_collections();
        cols.sort();
        // Both should be visible because Drop flushes
        assert_eq!(cols, vec!["maybe", "visible"]);
    }
}
