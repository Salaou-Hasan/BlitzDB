use thiserror::Error;

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("policy not found: {0}")]
    PolicyNotFound(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type PolicyResult<T> = Result<T, PolicyError>;
