use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{FluxError, Result};

const BATCH_SIZE_THRESHOLD: usize = 64;
const BATCH_BYTES_THRESHOLD: usize = 64 * 1024;

/// A WAL entry representing a single mutation.
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

/// Bincode-friendly representation where Value fields are pre-serialized as JSON bytes.
#[derive(Serialize, Deserialize)]
enum BinWalOp {
    CreateCollection { name: String },
    DropCollection { name: String },
    Insert { collection: String, doc_id: String, data_json: Vec<u8> },
    Update { collection: String, doc_id: String, data_json: Vec<u8> },
    Delete { collection: String, doc_id: String },
    CreateIndex { collection: String, field: String },
    DropIndex { collection: String, field: String },
}

impl BinWalOp {
    fn from_op(op: &WalOperation) -> std::result::Result<Self, FluxError> {
        Ok(match op {
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
                    data_json: serde_json::to_vec(data)?,
                }
            }
            WalOperation::Update { collection, doc_id, data } => {
                BinWalOp::Update {
                    collection: collection.clone(),
                    doc_id: doc_id.clone(),
                    data_json: serde_json::to_vec(data)?,
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
        })
    }

    fn into_op(self) -> std::result::Result<WalOperation, FluxError> {
        Ok(match self {
            BinWalOp::CreateCollection { name } => WalOperation::CreateCollection { name },
            BinWalOp::DropCollection { name } => WalOperation::DropCollection { name },
            BinWalOp::Insert { collection, doc_id, data_json } => {
                WalOperation::Insert {
                    collection,
                    doc_id,
                    data: serde_json::from_slice(&data_json)?,
                }
            }
            BinWalOp::Update { collection, doc_id, data_json } => {
                WalOperation::Update {
                    collection,
                    doc_id,
                    data: serde_json::from_slice(&data_json)?,
                }
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
pub struct Wal {
    path: PathBuf,
    file: File,
    sequence: u64,
    buffer: Vec<u8>,
    pending_count: usize,
}

impl Wal {
    /// Open or create a WAL file, reading existing entries to determine the next sequence number.
    pub fn open(path: &Path) -> Result<Self> {
        let entries = if path.exists() {
            Self::read_all(path)?
        } else {
            Vec::new()
        };
        let sequence = entries.last().map(|e| e.sequence + 1).unwrap_or(0);
        Self::open_at_sequence(path, sequence)
    }

    /// Open a WAL file starting at a known sequence number (avoids re-reading entries).
    pub fn open_at_sequence(path: &Path, sequence: u64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Wal {
            path: path.to_path_buf(),
            file,
            sequence,
            buffer: Vec::new(),
            pending_count: 0,
        })
    }

    /// Buffer an operation. Automatically flushes when the batch threshold is exceeded.
    pub fn append(&mut self, operation: WalOperation) -> Result<u64> {
        let seq = self.sequence;

        let bin_op = BinWalOp::from_op(&operation)?;
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

        if self.pending_count >= BATCH_SIZE_THRESHOLD
            || self.buffer.len() >= BATCH_BYTES_THRESHOLD
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
        self.file.sync_data()?;
        self.buffer.clear();
        self.pending_count = 0;
        Ok(())
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
                // Partial write or too small — stop here
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
                return Err(FluxError::CorruptionError(format!(
                    "checksum mismatch at sequence {seq}: expected {computed_crc}, got {stored_crc}"
                )));
            }

            let bin_op: BinWalOp = bincode::deserialize(op_bytes)
                .map_err(|e| FluxError::StorageError(format!("bincode decode: {e}")))?;
            let operation = bin_op.into_op()?;

            entries.push(WalEntry {
                sequence: seq,
                operation,
            });
            pos += 4 + payload_len;
        }

        Ok(entries)
    }

    /// Read legacy JSON-per-line WAL format (backward compatibility).
    fn read_all_json(data: &[u8]) -> Result<Vec<WalEntry>> {
        let text = std::str::from_utf8(data)
            .map_err(|e| FluxError::StorageError(format!("invalid UTF-8 in WAL: {e}")))?;

        #[derive(Deserialize)]
        struct LegacyEntry {
            sequence: u64,
            operation: WalOperation,
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
                        return Err(FluxError::CorruptionError(format!(
                            "checksum mismatch at sequence {}",
                            entry.sequence
                        )));
                    }
                    entries.push(WalEntry {
                        sequence: entry.sequence,
                        operation: entry.operation,
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
}

#[cfg(test)]
mod tests {
    use super::*;
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
            // Write more than BATCH_SIZE_THRESHOLD entries
            for i in 0..BATCH_SIZE_THRESHOLD + 10 {
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
        assert_eq!(entries.len(), BATCH_SIZE_THRESHOLD + 10);
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
}
