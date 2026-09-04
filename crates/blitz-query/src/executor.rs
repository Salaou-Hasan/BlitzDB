use crate::error::QueryResult;
use crate::planner::Filter;
use blitz_types::row::Row;

pub struct QueryExecutor;

impl QueryExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn filter_rows<'a>(&self, rows: &'a [Row], filter: &Filter) -> Vec<&'a Row> {
        rows.iter()
            .filter(|row| self.matches_filter(row, filter))
            .collect()
    }

    fn matches_filter(&self, row: &Row, filter: &Filter) -> bool {
        match filter {
            Filter::Eq(col, val) => {
                row.get(col).map_or(false, |v| v == val)
            }
            Filter::Ne(col, val) => {
                row.get(col).map_or(true, |v| v != val)
            }
            Filter::Gt(col, val) => {
                row.get(col).map_or(false, |v| v > val)
            }
            Filter::Lt(col, val) => {
                row.get(col).map_or(false, |v| v < val)
            }
            Filter::Gte(col, val) => {
                row.get(col).map_or(false, |v| v >= val)
            }
            Filter::Lte(col, val) => {
                row.get(col).map_or(false, |v| v <= val)
            }
            Filter::And(filters) => {
                filters.iter().all(|f| self.matches_filter(row, f))
            }
            Filter::Or(filters) => {
                filters.iter().any(|f| self.matches_filter(row, f))
            }
        }
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}
