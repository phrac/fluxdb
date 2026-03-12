use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(feature = "persistence")]
use crate::error::{FluxError, Result};

#[cfg(feature = "persistence")]
use std::fs::{self, File, OpenOptions};
#[cfg(feature = "persistence")]
use std::io::Write;
#[cfg(feature = "persistence")]
use std::path::{Path, PathBuf};

#[cfg(feature = "persistence")]
use crate::storage::StorageBackend;

pub const DEFAULT_BATCH_SIZE: usize = 64;
pub const DEFAULT_BATCH_BYTES: usize = 64 * 1024;

/// Controls when the WAL calls fsync after flushing buffered writes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    /// fsync after every batch flush (safest, default).
    EveryFlush,
    /// No fsync — rely on OS page cache (fastest, risk of data loss on crash).
    None,
}

impl Default for SyncMode {
    fn default() -> Self {
        SyncMode::EveryFlush
    }
}

/// A WAL entry representing a single mutation.
#[derive(Clone)]
pub struct WalEntry {
    pub sequence: u64,
    pub operation: WalOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    CreateCollection {
        name: String,
    },
    DropCollection {
        name: String,
    },
    Insert {
        collection: String,
        doc_id: String,
        #[serde(skip)]
        data: Value,
    },
    Update {
        collection: String,
        doc_id: String,
        #[serde(skip)]
        data: Value,
    },
    Delete {
        collection: String,
        doc_id: String,
    },
    CreateIndex {
        collection: String,
        field: String,
    },
    DropIndex {
        collection: String,
        field: String,
    },
}

/// A bincode-friendly representation of a JSON value.
///
/// `serde_json::Value` uses `deserialize_any`, which bincode doesn't support
/// (bincode is not self-describing). This type mirrors `Value` but with
/// explicit variants that bincode can serialize/deserialize deterministically.
#[cfg(feature = "persistence")]
#[derive(Serialize, Deserialize)]
enum BinValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
    Array(Vec<BinValue>),
    Object(Vec<(String, BinValue)>),
}

#[cfg(feature = "persistence")]
impl BinValue {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => BinValue::Null,
            Value::Bool(b) => BinValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    BinValue::Int(i)
                } else if let Some(u) = n.as_u64() {
                    BinValue::UInt(u)
                } else {
                    BinValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            Value::String(s) => BinValue::Str(s.clone()),
            Value::Array(arr) => BinValue::Array(arr.iter().map(BinValue::from_value).collect()),
            Value::Object(map) => BinValue::Object(
                map.iter()
                    .map(|(k, v)| (k.clone(), BinValue::from_value(v)))
                    .collect(),
            ),
        }
    }

    fn into_value(self) -> Value {
        match self {
            BinValue::Null => Value::Null,
            BinValue::Bool(b) => Value::Bool(b),
            BinValue::Int(i) => Value::Number(i.into()),
            BinValue::UInt(u) => Value::Number(u.into()),
            BinValue::Float(f) => Value::Number(
                serde_json::Number::from_f64(f).unwrap_or_else(|| 0i64.into()),
            ),
            BinValue::Str(s) => Value::String(s),
            BinValue::Array(arr) => {
                Value::Array(arr.into_iter().map(BinValue::into_value).collect())
            }
            BinValue::Object(pairs) => {
                let map: serde_json::Map<String, Value> = pairs
                    .into_iter()
                    .map(|(k, v)| (k, v.into_value()))
                    .collect();
                Value::Object(map)
            }
        }
    }
}

/// Bincode-native representation — document data is stored as `BinValue`
/// (a bincode-friendly mirror of `serde_json::Value`) instead of pre-serialized
/// JSON bytes.
///
/// WAL format v2: eliminates the JSON-inside-bincode double-serialization.
/// Legacy v1 WAL files are detected by attempting v2 deserialization first,
/// then falling back to v1 on failure.
#[cfg(feature = "persistence")]
#[derive(Serialize, Deserialize)]
enum BinWalOp {
    CreateCollection { name: String },
    DropCollection { name: String },
    Insert { collection: String, doc_id: String, data: BinValue },
    Update { collection: String, doc_id: String, data: BinValue },
    Delete { collection: String, doc_id: String },
    CreateIndex { collection: String, field: String },
    DropIndex { collection: String, field: String },
}

/// Legacy v1 format where document data was pre-serialized as JSON bytes.
#[cfg(feature = "persistence")]
#[derive(Serialize, Deserialize)]
enum BinWalOpV1 {
    CreateCollection { name: String },
    DropCollection { name: String },
    Insert { collection: String, doc_id: String, data_json: Vec<u8> },
    Update { collection: String, doc_id: String, data_json: Vec<u8> },
    Delete { collection: String, doc_id: String },
    CreateIndex { collection: String, field: String },
    DropIndex { collection: String, field: String },
}

#[cfg(feature = "persistence")]
impl BinWalOp {
    fn from_op(op: &WalOperation) -> Self {
        match op {
            WalOperation::CreateCollection { name } => {
                BinWalOp::CreateCollection { name: name.clone() }
            }
            WalOperation::DropCollection { name } => {
                BinWalOp::DropCollection { name: name.clone() }
            }
            WalOperation::Insert { collection, doc_id, data } => {
                BinWalOp::Insert {
                    collection: collection.clone(),
                    doc_id: doc_id.clone(),
                    data: BinValue::from_value(data),
                }
            }
            WalOperation::Update { collection, doc_id, data } => {
                BinWalOp::Update {
                    collection: collection.clone(),
                    doc_id: doc_id.clone(),
                    data: BinValue::from_value(data),
                }
            }
            WalOperation::Delete { collection, doc_id } => {
                BinWalOp::Delete { collection: collection.clone(), doc_id: doc_id.clone() }
            }
            WalOperation::CreateIndex { collection, field } => {
                BinWalOp::CreateIndex { collection: collection.clone(), field: field.clone() }
            }
            WalOperation::DropIndex { collection, field } => {
                BinWalOp::DropIndex { collection: collection.clone(), field: field.clone() }
            }
        }
    }

    fn into_op(self) -> WalOperation {
        match self {
            BinWalOp::CreateCollection { name } => WalOperation::CreateCollection { name },
            BinWalOp::DropCollection { name } => WalOperation::DropCollection { name },
            BinWalOp::Insert { collection, doc_id, data } => {
                WalOperation::Insert { collection, doc_id, data: data.into_value() }
            }
            BinWalOp::Update { collection, doc_id, data } => {
                WalOperation::Update { collection, doc_id, data: data.into_value() }
            }
            BinWalOp::Delete { collection, doc_id } => {
                WalOperation::Delete { collection, doc_id }
            }
            BinWalOp::CreateIndex { collection, field } => {
                WalOperation::CreateIndex { collection, field }
            }
            BinWalOp::DropIndex { collection, field } => {
                WalOperation::DropIndex { collection, field }
            }
        }
    }
}

#[cfg(feature = "persistence")]
impl BinWalOpV1 {
    fn into_op(self) -> std::result::Result<WalOperation, FluxError> {
        Ok(match self {
            BinWalOpV1::CreateCollection { name } => WalOperation::CreateCollection { name },
            BinWalOpV1::DropCollection { name } => WalOperation::DropCollection { name },
            BinWalOpV1::Insert { collection, doc_id, data_json } => {
                WalOperation::Insert {
                    collection,
                    doc_id,
                    data: serde_json::from_slice(&data_json)?,
                }
            }
            BinWalOpV1::Update { collection, doc_id, data_json } => {
                WalOperation::Update {
                    collection,
                    doc_id,
                    data: serde_json::from_slice(&data_json)?,
                }
            }
            BinWalOpV1::Delete { collection, doc_id } => {
                WalOperation::Delete { collection, doc_id }
            }
            BinWalOpV1::CreateIndex { collection, field } => {
                WalOperation::CreateIndex { collection, field }
            }
            BinWalOpV1::DropIndex { collection, field } => {
                WalOperation::DropIndex { collection, field }
            }
        })
    }
}

/// Append-only write-ahead log with binary format and batched writes.
///
/// Binary entry format:
///   [4 bytes: payload_len (u32 LE)] — length of seq + op_data + crc
///   [8 bytes: sequence (u64 LE)]
///   [N bytes: bincode-serialized BinWalOp]
///   [4 bytes: CRC32 (u32 LE)] — over seq + op_data
#[cfg(feature = "persistence")]
pub struct Wal {
    path: PathBuf,
    file: File,
    sequence: u64,
    buffer: Vec<u8>,
    pending_count: usize,
    batch_size: usize,
    batch_bytes: usize,
    sync_mode: SyncMode,
    file_bytes: u64,
}

#[cfg(feature = "persistence")]
impl Wal {
    /// Open or create a WAL file with default batch settings.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_params(path, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_BYTES, SyncMode::default())
    }

    /// Open or create a WAL file with custom batch and sync settings.
    pub fn open_with_params(
        path: &Path,
        batch_size: usize,
        batch_bytes: usize,
        sync_mode: SyncMode,
    ) -> Result<Self> {
        let entries = if path.exists() {
            Self::read_all(path)?
        } else {
            Vec::new()
        };
        let sequence = entries.last().map(|e| e.sequence + 1).unwrap_or(0);
        Self::open_at_sequence(path, sequence, batch_size, batch_bytes, sync_mode)
    }

    /// Open a WAL file starting at a known sequence number (avoids re-reading entries).
    pub fn open_at_sequence(
        path: &Path,
        sequence: u64,
        batch_size: usize,
        batch_bytes: usize,
        sync_mode: SyncMode,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        let file_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(Wal {
            path: path.to_path_buf(),
            file,
            sequence,
            buffer: Vec::new(),
            pending_count: 0,
            batch_size,
            batch_bytes,
            sync_mode,
            file_bytes,
        })
    }

    /// Buffer an operation. Automatically flushes when the batch threshold is exceeded.
    pub fn append(&mut self, operation: WalOperation) -> Result<u64> {
        let seq = self.sequence;

        let bin_op = BinWalOp::from_op(&operation);
        let op_bytes = bincode::serialize(&bin_op)
            .map_err(|e| FluxError::StorageError(format!("bincode serialize: {e}")))?;

        // Build entry: [4: payload_len][8: seq][N: op_bytes][4: crc]
        let payload_len = (8 + op_bytes.len() + 4) as u32;
        let entry_size = 4 + payload_len as usize;

        self.buffer.reserve(entry_size);
        let start = self.buffer.len();

        self.buffer.extend_from_slice(&payload_len.to_le_bytes());
        self.buffer.extend_from_slice(&seq.to_le_bytes());
        self.buffer.extend_from_slice(&op_bytes);

        // CRC over seq + op_bytes
        let crc = crc32fast::hash(&self.buffer[start + 4..]);
        self.buffer.extend_from_slice(&crc.to_le_bytes());

        self.pending_count += 1;
        self.sequence += 1;

        if self.pending_count >= self.batch_size
            || self.buffer.len() >= self.batch_bytes
        {
            self.flush()?;
        }

        Ok(seq)
    }

    /// Flush all buffered entries to disk.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.buffer)?;
        if self.sync_mode == SyncMode::EveryFlush {
            self.file.sync_data()?;
        }
        self.file_bytes += self.buffer.len() as u64;
        self.buffer.clear();
        self.pending_count = 0;
        Ok(())
    }

    /// Current WAL file size in bytes (tracked, not stat'd).
    pub fn file_bytes(&self) -> u64 {
        self.file_bytes
    }

    /// Read all valid entries from a WAL file. Detects binary vs legacy JSON format.
    pub fn read_all(path: &Path) -> Result<Vec<WalEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let data = fs::read(path)?;
        if data.is_empty() {
            return Ok(Vec::new());
        }

        if data[0] == b'{' {
            Self::read_all_json(&data)
        } else {
            Self::read_all_binary(&data)
        }
    }

    fn read_all_binary(data: &[u8]) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut pos = 0;

        while pos + 4 <= data.len() {
            let payload_len =
                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;

            if payload_len < 12 || pos + 4 + payload_len > data.len() {
                // Partial write or too small — stop here (truncated tail)
                break;
            }

            let entry_data = &data[pos + 4..pos + 4 + payload_len];
            let seq = u64::from_le_bytes(entry_data[0..8].try_into().unwrap());
            let op_bytes = &entry_data[8..payload_len - 4];
            let stored_crc = u32::from_le_bytes(
                entry_data[payload_len - 4..payload_len]
                    .try_into()
                    .unwrap(),
            );

            let computed_crc = crc32fast::hash(&entry_data[0..payload_len - 4]);
            if computed_crc != stored_crc {
                // Corruption detected — stop here, recover everything before this point.
                // This handles torn writes / partial flushes gracefully.
                eprintln!(
                    "fluxdb: WAL checksum mismatch at sequence {seq}, \
                     recovering {} valid entries before corruption",
                    entries.len()
                );
                break;
            }

            // Try v2 (direct bincode Value) first, fall back to v1 (JSON-inside-bincode)
            let operation = if let Ok(bin_op) = bincode::deserialize::<BinWalOp>(op_bytes) {
                bin_op.into_op()
            } else if let Ok(bin_op_v1) = bincode::deserialize::<BinWalOpV1>(op_bytes) {
                match bin_op_v1.into_op() {
                    Ok(op) => op,
                    Err(_) => break, // corrupted JSON data — stop here
                }
            } else {
                break; // corrupted entry payload — stop here
            };

            entries.push(WalEntry {
                sequence: seq,
                operation,
            });
            pos += 4 + payload_len;
        }

        Ok(entries)
    }

    /// Read legacy JSON-per-line WAL format (backward compatibility).
    ///
    /// Uses a dedicated `LegacyWalOp` enum because the main `WalOperation` has
    /// `#[serde(skip)]` on data fields (needed for the binary path), which would
    /// silently drop document data during JSON deserialization.
    fn read_all_json(data: &[u8]) -> Result<Vec<WalEntry>> {
        let text = std::str::from_utf8(data)
            .map_err(|e| FluxError::StorageError(format!("invalid UTF-8 in WAL: {e}")))?;

        /// Enum without `#[serde(skip)]` so JSON data fields are deserialized correctly.
        #[derive(Serialize, Deserialize)]
        enum LegacyWalOp {
            CreateCollection { name: String },
            DropCollection { name: String },
            Insert { collection: String, doc_id: String, data: Value },
            Update { collection: String, doc_id: String, data: Value },
            Delete { collection: String, doc_id: String },
            CreateIndex { collection: String, field: String },
            DropIndex { collection: String, field: String },
        }

        impl LegacyWalOp {
            fn into_wal_op(self) -> WalOperation {
                match self {
                    LegacyWalOp::CreateCollection { name } => WalOperation::CreateCollection { name },
                    LegacyWalOp::DropCollection { name } => WalOperation::DropCollection { name },
                    LegacyWalOp::Insert { collection, doc_id, data } => WalOperation::Insert { collection, doc_id, data },
                    LegacyWalOp::Update { collection, doc_id, data } => WalOperation::Update { collection, doc_id, data },
                    LegacyWalOp::Delete { collection, doc_id } => WalOperation::Delete { collection, doc_id },
                    LegacyWalOp::CreateIndex { collection, field } => WalOperation::CreateIndex { collection, field },
                    LegacyWalOp::DropIndex { collection, field } => WalOperation::DropIndex { collection, field },
                }
            }
        }

        #[derive(Deserialize)]
        struct LegacyEntry {
            sequence: u64,
            operation: LegacyWalOp,
            checksum: u32,
        }

        let mut entries = Vec::new();
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<LegacyEntry>(line) {
                Ok(entry) => {
                    let op_bytes = serde_json::to_vec(&entry.operation)
                        .map_err(|e| FluxError::StorageError(e.to_string()))?;
                    let expected = crc32fast::hash(&op_bytes);
                    if expected != entry.checksum {
                        // Corruption — stop here, recover what we have
                        eprintln!(
                            "fluxdb: legacy WAL checksum mismatch at sequence {}, \
                             recovering {} valid entries",
                            entry.sequence,
                            entries.len()
                        );
                        break;
                    }
                    entries.push(WalEntry {
                        sequence: entry.sequence,
                        operation: entry.operation.into_wal_op(),
                    });
                }
                Err(_) => break,
            }
        }

        Ok(entries)
    }

    /// Truncate the WAL (used during compaction).
    pub fn truncate(&mut self) -> Result<()> {
        self.buffer.clear();
        self.pending_count = 0;
        self.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.sequence = 0;
        Ok(())
    }

    /// Atomically replace the WAL with a new set of operations.
    ///
    /// Writes to a temporary file, fsyncs it, then renames over the original.
    /// `rename()` is atomic on POSIX filesystems, so a crash at any point
    /// leaves either the old or new WAL intact — never a half-written file.
    fn atomic_replace(&mut self, ops: Vec<WalOperation>) -> Result<()> {
        let tmp_path = self.path.with_extension("wal.tmp");

        // Build the new WAL contents in memory, then write in one shot
        let mut buf = Vec::new();
        let mut seq = 0u64;
        for op in &ops {
            let bin_op = BinWalOp::from_op(op);
            let op_bytes = bincode::serialize(&bin_op)
                .map_err(|e| FluxError::StorageError(format!("bincode serialize: {e}")))?;

            let payload_len = (8 + op_bytes.len() + 4) as u32;
            buf.reserve(4 + payload_len as usize);

            let crc_start = buf.len() + 4; // position of seq (start of CRC'd region)
            buf.extend_from_slice(&payload_len.to_le_bytes());
            buf.extend_from_slice(&seq.to_le_bytes());
            buf.extend_from_slice(&op_bytes);

            let crc = crc32fast::hash(&buf[crc_start..]);
            buf.extend_from_slice(&crc.to_le_bytes());
            seq += 1;
        }

        // Write temp file and fsync
        {
            let mut tmp_file = File::create(&tmp_path)?;
            tmp_file.write_all(&buf)?;
            tmp_file.sync_all()?;
        }

        // Atomic rename: old WAL → new WAL
        fs::rename(&tmp_path, &self.path)?;

        // fsync the directory so the rename is durable
        Self::fsync_dir(&self.path)?;

        // Reopen the file handle for future appends
        self.file = OpenOptions::new()
            .append(true)
            .open(&self.path)?;
        self.sequence = seq;
        self.file_bytes = buf.len() as u64;
        self.buffer.clear();
        self.pending_count = 0;

        Ok(())
    }

    /// fsync the parent directory to ensure file metadata (create/rename) is durable.
    fn fsync_dir(file_path: &Path) -> Result<()> {
        if let Some(parent) = file_path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(feature = "persistence")]
impl StorageBackend for Wal {
    fn append(&mut self, op: WalOperation) -> Result<u64> {
        Wal::append(self, op)
    }

    fn flush(&mut self) -> Result<()> {
        Wal::flush(self)
    }

    fn read_all(&self) -> Result<Vec<WalEntry>> {
        Wal::read_all(&self.path)
    }

    fn truncate(&mut self) -> Result<()> {
        Wal::truncate(self)
    }

    /// Crash-safe compaction: writes to temp file, fsyncs, renames atomically.
    fn replace_all(&mut self, ops: Vec<WalOperation>) -> Result<()> {
        self.atomic_replace(ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageBackend;
    use tempfile::tempdir;

    #[test]
    fn test_wal_append_and_read() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(WalOperation::CreateCollection {
                name: "users".into(),
            })
            .unwrap();
            wal.append(WalOperation::Insert {
                collection: "users".into(),
                doc_id: "1".into(),
                data: serde_json::json!({"name": "Alice"}),
            })
            .unwrap();
            wal.flush().unwrap();
        }

        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[test]
    fn test_wal_truncate() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let mut wal = Wal::open(&wal_path).unwrap();
        wal.append(WalOperation::CreateCollection {
            name: "users".into(),
        })
        .unwrap();
        wal.flush().unwrap();

        wal.truncate().unwrap();

        let entries = Wal::read_all(&wal_path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_reopen_continues_sequence() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(WalOperation::CreateCollection {
                name: "users".into(),
            })
            .unwrap();
            wal.flush().unwrap();
        }

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            let seq = wal
                .append(WalOperation::CreateCollection {
                    name: "orders".into(),
                })
                .unwrap();
            assert_eq!(seq, 1);
            wal.flush().unwrap();
        }

        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_wal_batch_auto_flush() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            // Write more than DEFAULT_BATCH_SIZE entries
            for i in 0..DEFAULT_BATCH_SIZE + 10 {
                wal.append(WalOperation::Insert {
                    collection: "test".into(),
                    doc_id: format!("{i}"),
                    data: serde_json::json!({"i": i}),
                })
                .unwrap();
            }
            wal.flush().unwrap(); // flush remaining
        }

        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), DEFAULT_BATCH_SIZE + 10);
    }

    #[test]
    fn test_wal_index_operations() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(WalOperation::CreateIndex {
                collection: "users".into(),
                field: "age".into(),
            })
            .unwrap();
            wal.append(WalOperation::DropIndex {
                collection: "users".into(),
                field: "age".into(),
            })
            .unwrap();
            wal.flush().unwrap();
        }

        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_wal_recovers_before_corruption() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write 3 valid entries
        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(WalOperation::CreateCollection { name: "users".into() }).unwrap();
            wal.append(WalOperation::Insert {
                collection: "users".into(),
                doc_id: "1".into(),
                data: serde_json::json!({"name": "Alice"}),
            }).unwrap();
            wal.append(WalOperation::Insert {
                collection: "users".into(),
                doc_id: "2".into(),
                data: serde_json::json!({"name": "Bob"}),
            }).unwrap();
            wal.flush().unwrap();
        }

        // Append garbage bytes to simulate a torn write / corruption
        {
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            // Write a valid-looking length header but corrupt payload
            file.write_all(&100u32.to_le_bytes()).unwrap();
            file.write_all(b"CORRUPT_GARBAGE_DATA_HERE!!").unwrap();
            file.sync_all().unwrap();
        }

        // Should recover the 3 valid entries, not error
        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[2].sequence, 2);
    }

    #[test]
    fn test_wal_recovers_from_crc_mismatch() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write 2 valid entries
        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(WalOperation::CreateCollection { name: "a".into() }).unwrap();
            wal.append(WalOperation::CreateCollection { name: "b".into() }).unwrap();
            wal.flush().unwrap();
        }

        // Read the file and corrupt the CRC of the second entry
        let mut data = fs::read(&wal_path).unwrap();
        // Find the second entry — skip past first entry
        let first_payload_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let second_entry_start = 4 + first_payload_len;
        let second_payload_len = u32::from_le_bytes(
            data[second_entry_start..second_entry_start + 4].try_into().unwrap()
        ) as usize;
        // Corrupt the CRC (last 4 bytes of second entry's payload)
        let crc_pos = second_entry_start + 4 + second_payload_len - 4;
        data[crc_pos] ^= 0xFF;
        fs::write(&wal_path, &data).unwrap();

        // Should recover only the first entry
        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 0);
    }

    #[test]
    fn test_wal_truncated_tail_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write entries
        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(WalOperation::CreateCollection { name: "users".into() }).unwrap();
            wal.append(WalOperation::Insert {
                collection: "users".into(),
                doc_id: "1".into(),
                data: serde_json::json!({"x": 1}),
            }).unwrap();
            wal.flush().unwrap();
        }

        // Truncate the file mid-entry (simulate crash during write)
        let data = fs::read(&wal_path).unwrap();
        let truncated = &data[..data.len() - 5]; // chop off last 5 bytes
        fs::write(&wal_path, truncated).unwrap();

        // Should recover the first entry only
        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 0);
    }

    #[test]
    fn test_wal_atomic_replace() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let mut wal = Wal::open(&wal_path).unwrap();

        // Write initial entries
        wal.append(WalOperation::CreateCollection { name: "a".into() }).unwrap();
        wal.append(WalOperation::CreateCollection { name: "b".into() }).unwrap();
        wal.append(WalOperation::Insert {
            collection: "a".into(),
            doc_id: "1".into(),
            data: serde_json::json!({"old": true}),
        }).unwrap();
        wal.flush().unwrap();

        // Atomic replace with compacted state
        let new_ops = vec![
            WalOperation::CreateCollection { name: "a".into() },
            WalOperation::Insert {
                collection: "a".into(),
                doc_id: "1".into(),
                data: serde_json::json!({"compacted": true}),
            },
        ];
        wal.replace_all(new_ops).unwrap();

        // Verify the WAL now has only the compacted entries
        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);

        // Verify no temp file left behind
        assert!(!wal_path.with_extension("wal.tmp").exists());

        // Verify we can still append after replace
        wal.append(WalOperation::CreateCollection { name: "c".into() }).unwrap();
        wal.flush().unwrap();

        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_wal_legacy_json_format() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // The legacy format: each line is {"sequence":N,"operation":{...},"checksum":CRC}
        // Checksum was CRC32 of serde_json::to_vec(&WalOperation), which serializes
        // struct variant fields in *declaration order* (not alphabetical).
        // We replicate that by using a matching struct for serialization.

        #[derive(serde::Serialize)]
        enum TestOp {
            CreateCollection { name: String },
            Insert { collection: String, doc_id: String, data: serde_json::Value },
        }

        let op1 = TestOp::CreateCollection { name: "users".into() };
        let op1_bytes = serde_json::to_vec(&op1).unwrap();
        let crc1 = crc32fast::hash(&op1_bytes);
        let op1_val: serde_json::Value = serde_json::from_slice(&op1_bytes).unwrap();

        let op2 = TestOp::Insert {
            collection: "users".into(),
            doc_id: "1".into(),
            data: serde_json::json!({"name": "Alice"}),
        };
        let op2_bytes = serde_json::to_vec(&op2).unwrap();
        let crc2 = crc32fast::hash(&op2_bytes);
        let op2_val: serde_json::Value = serde_json::from_slice(&op2_bytes).unwrap();

        let line1 = serde_json::json!({"sequence": 0, "operation": op1_val, "checksum": crc1});
        let line2 = serde_json::json!({"sequence": 1, "operation": op2_val, "checksum": crc2});

        let content = format!("{}\n{}\n", line1, line2);
        fs::write(&wal_path, content).unwrap();

        // Should read both entries with data intact
        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 2);

        // Verify the Insert operation has its data
        match &entries[1].operation {
            WalOperation::Insert { collection, doc_id, data } => {
                assert_eq!(collection, "users");
                assert_eq!(doc_id, "1");
                assert_eq!(data["name"], "Alice");
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }
}
