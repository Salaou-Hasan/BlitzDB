use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("replication error: {0}")]
    ReplicationError(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type ReplicationResult<T> = Result<T, ReplicationError>;
