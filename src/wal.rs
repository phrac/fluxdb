use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{FluxError, Result};

/// A WAL (Write-Ahead Log) entry representing a single mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub operation: WalOperation,
    pub checksum: u32,
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
        data: Value,
    },
    Update {
        collection: String,
        doc_id: String,
        data: Value,
    },
    Delete {
        collection: String,
        doc_id: String,
    },
}

/// Append-only write-ahead log for crash recovery.
pub struct Wal {
    path: PathBuf,
    file: File,
    sequence: u64,
}

impl Wal {
    /// Open or create a WAL file at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Count existing entries to determine next sequence number
        let sequence = if path.exists() {
            Self::count_entries(path)?
        } else {
            0
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Wal {
            path: path.to_path_buf(),
            file,
            sequence,
        })
    }

    fn count_entries(path: &Path) -> Result<u64> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut max_seq = 0u64;

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => {
                    if entry.sequence >= max_seq {
                        max_seq = entry.sequence + 1;
                    }
                }
                Err(_) => {
                    // Corrupted entry at end of log — stop here
                    break;
                }
            }
        }

        Ok(max_seq)
    }

    /// Append an operation to the WAL and flush to disk.
    pub fn append(&mut self, operation: WalOperation) -> Result<u64> {
        let seq = self.sequence;

        // Compute checksum over the operation data
        let op_bytes = serde_json::to_vec(&operation)?;
        let checksum = crc32fast::hash(&op_bytes);

        let entry = WalEntry {
            sequence: seq,
            operation,
            checksum,
        };

        let mut line = serde_json::to_string(&entry)?;
        line.push('\n');

        self.file.write_all(line.as_bytes())?;
        self.file.flush()?;

        self.sequence += 1;
        Ok(seq)
    }

    /// Read all valid entries from the WAL for replay.
    pub fn read_all(path: &Path) -> Result<Vec<WalEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => {
                    // Verify checksum
                    let op_bytes = serde_json::to_vec(&entry.operation)
                        .map_err(|e| FluxError::StorageError(e.to_string()))?;
                    let expected = crc32fast::hash(&op_bytes);
                    if expected != entry.checksum {
                        return Err(FluxError::CorruptionError(format!(
                            "checksum mismatch at sequence {}: expected {expected}, got {}",
                            entry.sequence, entry.checksum
                        )));
                    }
                    entries.push(entry);
                }
                Err(_) => {
                    // Partial write at end of log — ignore
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// Truncate the WAL (after a successful snapshot/compaction).
    pub fn truncate(&mut self) -> Result<()> {
        // Close and recreate the file
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
        }

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            let seq = wal
                .append(WalOperation::CreateCollection {
                    name: "orders".into(),
                })
                .unwrap();
            assert_eq!(seq, 1);
        }

        let entries = Wal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 2);
    }
}
