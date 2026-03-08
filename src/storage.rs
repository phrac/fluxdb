use crate::error::Result;
use crate::wal::{WalEntry, WalOperation};

/// Trait for pluggable storage backends.
///
/// Implementations handle persistence of WAL operations. The database engine
/// replays entries on startup and appends new operations as mutations occur.
pub trait StorageBackend: Send {
    /// Append an operation to the log. Returns the sequence number.
    fn append(&mut self, op: WalOperation) -> Result<u64>;

    /// Flush any buffered writes to durable storage.
    fn flush(&mut self) -> Result<()>;

    /// Read all persisted entries (used for replay on startup).
    fn read_all(&self) -> Result<Vec<WalEntry>>;

    /// Truncate the log and reset sequence numbering (used during compaction).
    fn truncate(&mut self) -> Result<()>;

    /// Atomically replace all persisted operations with a new set (compaction).
    ///
    /// The default implementation truncates and re-writes, which is NOT crash-safe.
    /// Backends with durable storage should override with an atomic implementation
    /// (e.g. write to temp file, fsync, rename).
    fn replace_all(&mut self, ops: Vec<WalOperation>) -> Result<()> {
        self.truncate()?;
        for op in ops {
            self.append(op)?;
        }
        self.flush()?;
        Ok(())
    }
}

/// In-memory storage backend with no persistence.
///
/// Data lives only as long as the process. Useful for testing, WASM targets,
/// and ephemeral databases where durability is not required.
pub struct MemoryBackend {
    sequence: u64,
}

impl MemoryBackend {
    pub fn new() -> Self {
        MemoryBackend { sequence: 0 }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MemoryBackend {
    fn append(&mut self, _op: WalOperation) -> Result<u64> {
        let seq = self.sequence;
        self.sequence += 1;
        Ok(seq)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_all(&self) -> Result<Vec<WalEntry>> {
        Ok(Vec::new())
    }

    fn truncate(&mut self) -> Result<()> {
        self.sequence = 0;
        Ok(())
    }
}
