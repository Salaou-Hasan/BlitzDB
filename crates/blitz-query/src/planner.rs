use blitz_types::value::Value;

/// A condition for filtering rows.
#[derive(Debug, Clone)]
pub enum Filter {
    Eq(String, Value),
    Ne(String, Value),
    Gt(String, Value),
    Lt(String, Value),
    Gte(String, Value),
    Lte(String, Value),
    In(String, Vec<Value>),
    NotIn(String, Vec<Value>),
    Between(String, Value, Value),
    Like(String, String),
    IsNull(String),
    IsNotNull(String),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// A sort specification: column name + direction.
#[derive(Debug, Clone)]
pub struct Sort {
    pub column: String,
    pub direction: SortDirection,
}

impl Sort {
    pub fn asc(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: SortDirection::Asc,
        }
    }

    pub fn desc(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: SortDirection::Desc,
        }
    }
}

/// An aggregation function applied to a column.
#[derive(Debug, Clone)]
pub enum AggFunction {
    Count,
    CountColumn(String),
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

/// Result of an aggregation: a map of output column names to values.
pub type AggResult = std::collections::HashMap<String, Value>;

/// A complete query specification.
#[derive(Debug, Clone)]
pub struct Query {
    pub table: String,
    pub filter: Option<Filter>,
    pub projections: Option<Vec<String>>,
    pub sorts: Vec<Sort>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub aggregations: Vec<(String, AggFunction)>,
    pub group_by: Vec<String>,
}

impl Query {
    /// Create a new query for the given table.
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            filter: None,
            projections: None,
            sorts: Vec::new(),
            limit: None,
            offset: None,
            aggregations: Vec::new(),
            group_by: Vec::new(),
        }
    }

    /// Set a WHERE filter.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set columns to project (SELECT).
    pub fn select(mut self, columns: impl Into<Vec<String>>) -> Self {
        self.projections = Some(columns.into());
        self
    }

    /// Add an ORDER BY clause.
    pub fn order_by(mut self, sort: Sort) -> Self {
        self.sorts.push(sort);
        self
    }

    /// Set the LIMIT.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the OFFSET.
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Add a COUNT aggregation.
    pub fn count(mut self, alias: impl Into<String>) -> Self {
        self.aggregations.push((alias.into(), AggFunction::Count));
        self
    }

    /// Add a SUM aggregation.
    pub fn sum(mut self, column: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregations
            .push((alias.into(), AggFunction::Sum(column.into())));
        self
    }

    /// Add an AVG aggregation.
    pub fn avg(mut self, column: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregations
            .push((alias.into(), AggFunction::Avg(column.into())));
        self
    }

    /// Add a MIN aggregation.
    pub fn min(mut self, column: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregations
            .push((alias.into(), AggFunction::Min(column.into())));
        self
    }

    /// Add a MAX aggregation.
    pub fn max(mut self, column: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregations
            .push((alias.into(), AggFunction::Max(column.into())));
        self
    }

    /// Set GROUP BY columns.
    pub fn group_by(mut self, columns: impl Into<Vec<String>>) -> Self {
        self.group_by = columns.into();
        self
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::id::RowId;
    use blitz_types::row::Row;

    #[test]
    fn test_filter_eq() {
        let f = Filter::Eq("name".into(), Value::String("Alice".into()));
        let mut row = Row::new(RowId::new(1));
        row.set("name", Value::String("Alice".into()));
        assert!(match_value(&row, &f));
    }

    #[test]
    fn test_filter_ne() {
        let f = Filter::Ne("name".into(), Value::String("Alice".into()));
        let mut row = Row::new(RowId::new(1));
        row.set("name", Value::String("Bob".into()));
        assert!(match_value(&row, &f));
    }

    #[test]
    fn test_filter_gt() {
        let f = Filter::Gt("age".into(), Value::Int64(18));
        let mut row = Row::new(RowId::new(1));
        row.set("age", Value::Int64(25));
        assert!(match_value(&row, &f));

        let mut row2 = Row::new(RowId::new(2));
        row2.set("age", Value::Int64(15));
        assert!(!match_value(&row2, &f));
    }

    #[test]
    fn test_filter_between() {
        let f = Filter::Between("score".into(), Value::Int64(10), Value::Int64(50));
        let mut row = Row::new(RowId::new(1));
        row.set("score", Value::Int64(30));
        assert!(match_value(&row, &f));

        let mut row2 = Row::new(RowId::new(2));
        row2.set("score", Value::Int64(60));
        assert!(!match_value(&row2, &f));
    }

    #[test]
    fn test_filter_in() {
        let f = Filter::In(
            "status".into(),
            vec![
                Value::String("active".into()),
                Value::String("pending".into()),
            ],
        );
        let mut row = Row::new(RowId::new(1));
        row.set("status", Value::String("active".into()));
        assert!(match_value(&row, &f));

        let mut row2 = Row::new(RowId::new(2));
        row2.set("status", Value::String("banned".into()));
        assert!(!match_value(&row2, &f));
    }

    #[test]
    fn test_filter_like() {
        let f = Filter::Like("email".into(), "%@example.com".to_string());
        let mut row = Row::new(RowId::new(1));
        row.set("email", Value::String("alice@example.com".into()));
        assert!(match_value(&row, &f));

        let mut row2 = Row::new(RowId::new(2));
        row2.set("email", Value::String("bob@test.com".into()));
        assert!(!match_value(&row2, &f));
    }

    #[test]
    fn test_filter_is_null() {
        let f = Filter::IsNull("email".into());
        let mut row = Row::new(RowId::new(1));
        assert!(match_value(&row, &f));

        row.set("email", Value::String("a@b.com".into()));
        assert!(!match_value(&row, &f));
    }

    #[test]
    fn test_filter_and_or() {
        let f = Filter::And(vec![
            Filter::Gt("age".into(), Value::Int64(18)),
            Filter::Eq("active".into(), Value::Boolean(true)),
        ]);
        let mut row = Row::new(RowId::new(1));
        row.set("age", Value::Int64(25));
        row.set("active", Value::Boolean(true));
        assert!(match_value(&row, &f));

        let f2 = Filter::Or(vec![
            Filter::Eq("role".into(), Value::String("admin".into())),
            Filter::Eq("role".into(), Value::String("superadmin".into())),
        ]);
        let mut row2 = Row::new(RowId::new(2));
        row2.set("role", Value::String("superadmin".into()));
        assert!(match_value(&row2, &f2));
    }

    #[test]
    fn test_filter_not() {
        let f = Filter::Not(Box::new(Filter::Eq("active".into(), Value::Boolean(true))));
        let mut row = Row::new(RowId::new(1));
        row.set("active", Value::Boolean(false));
        assert!(match_value(&row, &f));

        row.set("active", Value::Boolean(true));
        assert!(!match_value(&row, &f));
    }

    #[test]
    fn test_filter_not_in() {
        let f = Filter::NotIn(
            "status".into(),
            vec![
                Value::String("banned".into()),
                Value::String("deleted".into()),
            ],
        );
        let mut row = Row::new(RowId::new(1));
        row.set("status", Value::String("active".into()));
        assert!(match_value(&row, &f));

        let mut row2 = Row::new(RowId::new(2));
        row2.set("status", Value::String("banned".into()));
        assert!(!match_value(&row2, &f));
    }

    #[test]
    fn test_filter_is_not_null() {
        let f = Filter::IsNotNull("email".into());
        let mut row = Row::new(RowId::new(1));
        assert!(!match_value(&row, &f));

        row.set("email", Value::String("a@b.com".into()));
        assert!(match_value(&row, &f));
    }

    #[test]
    fn test_query_builder() {
        let q = Query::new("users")
            .filter(Filter::Gt("age".into(), Value::Int64(18)))
            .select(vec!["name".into(), "email".into()])
            .order_by(Sort::asc("name"))
            .limit(10)
            .offset(5);
        assert_eq!(q.table, "users");
        assert!(q.filter.is_some());
        assert_eq!(q.projections.as_ref().unwrap().len(), 2);
        assert_eq!(q.sorts.len(), 1);
        assert_eq!(q.limit, Some(10));
        assert_eq!(q.offset, Some(5));
    }

    fn match_value(row: &Row, filter: &Filter) -> bool {
        match filter {
            Filter::Eq(col, val) => row.get(col).map_or(false, |v| v == val),
            Filter::Ne(col, val) => row.get(col).map_or(true, |v| v != val),
            Filter::Gt(col, val) => row.get(col).map_or(false, |v| v > val),
            Filter::Lt(col, val) => row.get(col).map_or(false, |v| v < val),
            Filter::Gte(col, val) => row.get(col).map_or(false, |v| v >= val),
            Filter::Lte(col, val) => row.get(col).map_or(false, |v| v <= val),
            Filter::In(col, vals) => row.get(col).map_or(false, |v| vals.contains(v)),
            Filter::NotIn(col, vals) => row.get(col).map_or(true, |v| !vals.contains(v)),
            Filter::Between(col, low, high) => {
                row.get(col).map_or(false, |v| v >= low && v <= high)
            }
            Filter::Like(col, pattern) => {
                row.get(col).map_or(false, |v| match v {
                    Value::String(s) => like_match(s, pattern),
                    _ => false,
                })
            }
            Filter::IsNull(col) => row.get(col).map_or(true, |v| v.is_null()),
            Filter::IsNotNull(col) => row.get(col).map_or(false, |v| !v.is_null()),
            Filter::And(filters) => filters.iter().all(|f| match_value(row, f)),
            Filter::Or(filters) => filters.iter().any(|f| match_value(row, f)),
            Filter::Not(f) => !match_value(row, f),
        }
    }

    fn like_match(s: &str, pattern: &str) -> bool {
        let pattern_lower = pattern.to_lowercase();
        let s_lower = s.to_lowercase();
        let parts: Vec<&str> = pattern_lower.split('%').collect();
        if parts.len() == 1 {
            return s_lower == pattern_lower;
        }
        let mut pos = 0;
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }
            match s_lower[pos..].find(part) {
                Some(idx) => {
                    if i == 0 && idx != 0 {
                        return false;
                    }
                    pos += idx + part.len();
                }
                None => return false,
            }
        }
        true
    }
}
