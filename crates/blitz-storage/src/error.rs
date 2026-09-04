use thiserror::Error;

/// Errors that can occur in storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("page not found: {0}")]
    PageNotFound(u64),

    #[error("recovery error: {0}")]
    RecoveryError(#[from] crate::recovery::RecoveryError),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;
