use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("function not found: {0}")]
    FunctionNotFound(String),
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("argument error: {0}")]
    ArgumentError(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;
