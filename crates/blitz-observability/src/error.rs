use thiserror::Error;

#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error("metrics error: {0}")]
    MetricsError(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type ObservabilityResult<T> = Result<T, ObservabilityError>;
