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

    #[error("node unreachable: {0}")]
    NodeUnreachable(String),

    #[error("cluster error: {0}")]
    ClusterError(String),

    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("authentication required")]
    Unauthorized,

    #[error("operation timed out")]
    Timeout,

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, FluxError>;
