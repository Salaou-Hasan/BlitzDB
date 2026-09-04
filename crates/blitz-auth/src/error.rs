use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),
    #[error("token expired")]
    TokenExpired,
    #[error("invalid token: {0}")]
    InvalidToken(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type AuthResult<T> = Result<T, AuthError>;
