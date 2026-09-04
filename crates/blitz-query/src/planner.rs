use crate::error::QueryResult;
use blitz_types::row::Row;
use blitz_types::value::Value;

pub enum Filter {
    Eq(String, Value),
    Ne(String, Value),
    Gt(String, Value),
    Lt(String, Value),
    Gte(String, Value),
    Lte(String, Value),
    And(Vec<Filter>),
    Or(Vec<Filter>),
}

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn new() -> Self {
        Self
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}
