use thiserror::Error;

/// Errors that can occur during type operations.
#[derive(Debug, Error)]
pub enum TypeError {
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("null value encountered where non-null required")]
    NullValue,

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("invalid id: {0}")]
    InvalidId(String),

    #[error("schema error: {0}")]
    SchemaError(String),

    #[error("overflow: {0}")]
    Overflow(String),

    #[error("out of range: {0}")]
    OutOfRange(String),
}

/// Result type for type operations.
pub type TypeResult<T> = Result<T, TypeError>;
