use thiserror::Error;

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("search error: {0}")]
    SearchError(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type SearchResult<T> = Result<T, SearchError>;
