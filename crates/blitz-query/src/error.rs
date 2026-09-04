use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type QueryResult<T> = Result<T, QueryError>;
