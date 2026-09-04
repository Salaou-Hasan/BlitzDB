use thiserror::Error;

/// Errors that can occur in snapshot operations.
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for snapshot operations.
pub type SnapshotResult<T> = Result<T, SnapshotError>;
