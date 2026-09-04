use thiserror::Error;

#[derive(Debug, Error)]
pub enum RealtimeError {
    #[error("subscription error: {0}")]
    SubscriptionError(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type RealtimeResult<T> = Result<T, RealtimeError>;
