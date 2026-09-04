use thiserror::Error;
use blitz_core::CoreError;

/// Errors that can occur in table operations.
#[derive(Debug, Error)]
pub enum TableError {
    #[error("table not found: {0}")]
    NotFound(String),

    #[error("table already exists: {0}")]
    AlreadyExists(String),

    #[error("row not found: {0}")]
    RowNotFound(u64),

    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("type error: {0}")]
    TypeError(#[from] blitz_types::TypeError),

    #[error("core error: {0}")]
    CoreError(#[from] CoreError),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for table operations.
pub type TableResult<T> = Result<T, TableError>;
