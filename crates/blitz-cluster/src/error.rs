use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("cluster error: {0}")]
    ClusterError(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type ClusterResult<T> = Result<T, ClusterError>;
