use thiserror::Error;
use blitz_core::CoreError;

/// Errors that can occur in transaction operations.
#[derive(Debug, Error)]
pub enum TxError {
    #[error("transaction not found: {0}")]
    NotFound(String),

    #[error("transaction already committed: {0}")]
    AlreadyCommitted(String),

    #[error("transaction already rolled back: {0}")]
    AlreadyRolledBack(String),

    #[error("conflict detected: {0}")]
    Conflict(String),

    #[error("serialization failure: {0}")]
    SerializationFailure(String),

    #[error("core error: {0}")]
    CoreError(#[from] CoreError),

    #[error("table error: {0}")]
    TableError(#[from] blitz_table::TableError),

    #[error("type error: {0}")]
    TypeError(#[from] blitz_types::TypeError),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for transaction operations.
pub type TxResult<T> = Result<T, TxError>;
