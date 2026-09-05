use thiserror::Error;

#[derive(Debug, Error)]
pub enum JobError {
    #[error("job error: {0}")]
    JobError(String),
    #[error("internal error: {0}")]
    Internal(String),
    /// Module failed to compile/validate (untrusted bytes stay out).
    #[error("bad module: {0}")]
    BadModule(String),
    /// Module lacks the `run(i32,i32)->i64` + `memory` contract.
    #[error("bad signature: {0}")]
    BadSignature(String),
    /// Guest trapped (unreachable, OOB, bad UTF-8 result, ...).
    #[error("guest trap: {0}")]
    GuestTrap(String),
    /// Fuel exhausted (runaway killed; default budget in `WasmExecutor`).
    #[error("fuel exhausted after {0} units")]
    FuelExhausted(u64),
}

pub type JobResult<T> = Result<T, JobError>;
