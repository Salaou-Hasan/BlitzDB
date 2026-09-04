use thiserror::Error;

/// Errors that can occur in the core engine.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("row not found: {0}")]
    RowNotFound(u64),

    #[error("duplicate key: {0}")]
    DuplicateKey(String),

    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("type error: {0}")]
    TypeError(#[from] blitz_types::TypeError),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for core operations.
pub type CoreResult<T> = Result<T, CoreError>;
