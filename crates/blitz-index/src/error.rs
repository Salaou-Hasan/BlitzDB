use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index not found: {0}")]
    NotFound(String),
    #[error("duplicate key: {0}")]
    DuplicateKey(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type IndexResult<T> = Result<T, IndexError>;
