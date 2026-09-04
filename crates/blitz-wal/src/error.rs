use thiserror::Error;

/// Errors that can occur in WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for WAL operations.
pub type WalResult<T> = Result<T, WalError>;
