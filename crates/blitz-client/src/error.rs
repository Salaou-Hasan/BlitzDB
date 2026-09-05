//! Typed SDK errors mapped from server error strings (see PROTOCOL.md
//! "Errors"). Mapping is by documented prefix; unknown strings surface as
//! `Server` verbatim — never silently reclassified.

use thiserror::Error;

/// Typed client error.
#[derive(Debug, Error)]
pub enum SdkError {
    /// TCP/TLS/frame failure (connection lost, refused, undecodable).
    #[error("transport: {0}")]
    Transport(String),
    /// Call exceeded the per-call timeout (default 5s).
    #[error("timeout after {0}ms")]
    Timeout(u64),
    /// Server rejected for auth reasons. Do not retry without fixing creds.
    #[error("auth: {0}")]
    Auth(String),
    /// Server asked for a jittered retry (WAL backpressure / rotation).
    /// Safe to retry: the SDK already replays the same `_idem` once.
    #[error("retryable: {0}")]
    Retryable(String),
    /// Row/table/procedure not found. Do not retry blindly.
    #[error("not found: {0}")]
    NotFound(String),
    /// Caller bug (validation, unique violation, bad op shape). Don't retry.
    #[error("invalid: {0}")]
    Invalid(String),
    /// Server-side error payload that matched no known prefix.
    #[error("server: {0}")]
    Server(String),
    /// The client was shut down (worker gone).
    #[error("client closed")]
    Closed,
}

/// Map a server error payload to a typed error.
pub fn map_server_error(msg: &str) -> SdkError {
    if msg.starts_with("unauthorized") || msg.starts_with("forbidden") {
        SdkError::Auth(msg.to_string())
    } else if msg.starts_with("WAL backpressure")
        || msg.starts_with("group full")
        || msg.starts_with("rotation in progress")
    {
        SdkError::Retryable(msg.to_string())
    } else if msg.contains("not found") || msg == "not found" {
        SdkError::NotFound(msg.to_string())
    } else if msg.contains("requires values")
        || msg.contains("requires row_id")
        || msg.contains("too large")
        || msg.contains("DuplicateKey")
        || msg.contains("duplicate value")
        || msg.contains("not supported")
        || msg.contains("not unique")
        || msg.contains("validation")
        || msg.contains("schema error")
        || msg.contains("type error")
    {
        SdkError::Invalid(msg.to_string())
    } else {
        // Procedure/batch abort reasons carry user semantics (insufficient
        // balance, ...): surface verbatim so callers can match on content.
        SdkError::Server(msg.to_string())
    }
}

pub type SdkResult<T> = Result<T, SdkError>;
