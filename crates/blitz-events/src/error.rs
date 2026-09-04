use thiserror::Error;

#[derive(Debug, Error)]
pub enum EventError {
    #[error("event error: {0}")]
    EventError(String),
    #[error("subscriber not found: {0}")]
    SubscriberNotFound(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type EventResult<T> = Result<T, EventError>;
