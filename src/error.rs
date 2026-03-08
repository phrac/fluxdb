use thiserror::Error;

#[derive(Error, Debug)]
pub enum FluxError {
    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("document not found: {0}")]
    DocumentNotFound(String),

    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("data corruption detected: {0}")]
    CorruptionError(String),
}

pub type Result<T> = std::result::Result<T, FluxError>;
