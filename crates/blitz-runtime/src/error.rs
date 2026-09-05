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
    /// Procedure aborted itself via `Fail`: message surfaces as the abort
    /// reason; the server rolls back the whole call (all-or-nothing).
    #[error("aborted: {0}")]
    Abort(String),
    /// Step budget exhausted (default 10K): runaway procedure killed to
    /// protect tail latency. No loops exist in v1; deep If-nesting can trip
    /// this — flatten instead.
    #[error("fuel exhausted after {0} steps")]
    FuelExhausted(u64),
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;
