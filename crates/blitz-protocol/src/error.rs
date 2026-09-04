use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("decode error: {0}")]
    DecodeError(String),
    #[error("encode error: {0}")]
    EncodeError(String),
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u32),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;
