use thiserror::Error;

#[derive(Debug, Error)]
pub enum JobError {
    #[error("job error: {0}")]
    JobError(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type JobResult<T> = Result<T, JobError>;
