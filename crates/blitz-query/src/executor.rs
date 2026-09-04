use crate::error::QueryResult;
use crate::planner::{AggFunction, AggResult, Filter, Query, Sort, SortDirection};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::value::Value;
use std::collections::HashMap;

pub struct QueryExecutor;

impl QueryExecutor {
    pub fn new() -> Self {
        Self
    }

    /// Execute a query against a slice of rows.
    pub fn execute(&self, rows: &[Row], query: &Query) -> QueryResult<Vec<Row>> {
        let mut result: Vec<&Row> = if let Some(ref filter) = query.filter {
            rows.iter()
                .filter(|row| self.matches_filter(row, filter))
                .collect()
        } else {
            rows.iter().collect()
        };

        if !query.group_by.is_empty() || !query.aggregations.is_empty() {
            return self.execute_aggregation(result, query);
        }

        self.sort_rows(&mut result, &query.sorts);

        if let Some(offset) = query.offset {
            if offset >= result.len() {
                return Ok(Vec::new());
            }
            result = result.drain(offset..).collect();
        }

        if let Some(limit) = query.limit {
            result.truncate(limit);
        }

        let mut output: Vec<Row> = result.into_iter().cloned().collect();

        if let Some(ref projections) = query.projections {
            let cols: Vec<&str> = projections.iter().map(|s| s.as_str()).collect();
            output = output.iter().map(|r| r.project(&cols)).collect();
        }

        Ok(output)
    }

    /// Filter rows in-place (standalone utility).
    pub fn filter_rows<'a>(&self, rows: &'a [Row], filter: &Filter) -> Vec<&'a Row> {
        rows.iter()
            .filter(|row| self.matches_filter(row, filter))
            .collect()
    }

    fn matches_filter(&self, row: &Row, filter: &Filter) -> bool {
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
            Filter::Like(col, pattern) => row.get(col).map_or(false, |v| match v {
                Value::String(s) => {
                    let s_lower = s.to_lowercase();
                    let p_lower = pattern.to_lowercase();
                    let parts: Vec<&str> = p_lower.split('%').collect();
                    if parts.len() == 1 {
                        return s_lower == p_lower;
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
                _ => false,
            }),
            Filter::IsNull(col) => row.get(col).map_or(true, |v| v.is_null()),
            Filter::IsNotNull(col) => row.get(col).map_or(false, |v| !v.is_null()),
            Filter::And(filters) => filters.iter().all(|f| self.matches_filter(row, f)),
            Filter::Or(filters) => filters.iter().any(|f| self.matches_filter(row, f)),
            Filter::Not(f) => !self.matches_filter(row, f),
        }
    }

    fn sort_rows(&self, rows: &mut Vec<&Row>, sorts: &[Sort]) {
        if sorts.is_empty() {
            return;
        }
        rows.sort_by(|a, b| {
            for sort in sorts {
                let a_val = a.get(&sort.column);
                let b_val = b.get(&sort.column);
                let ord = match (a_val, b_val) {
                    (Some(av), Some(bv)) => av.cmp(bv),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                let ord = match sort.direction {
                    SortDirection::Asc => ord,
                    SortDirection::Desc => ord.reverse(),
                };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn execute_aggregation<'a>(
        &self,
        rows: Vec<&'a Row>,
        query: &Query,
    ) -> QueryResult<Vec<Row>> {
        if query.group_by.is_empty() {
            let result = self.compute_aggregations(&rows, &query.aggregations)?;
            let mut row = Row::new(RowId::new(0));
            for (alias, val) in result {
                row.set(alias, val);
            }
            return Ok(vec![row]);
        }

        let mut groups: HashMap<Vec<Value>, Vec<&Row>> = HashMap::new();
        for row in &rows {
            let key: Vec<Value> = query
                .group_by
                .iter()
                .map(|col| row.get(col).cloned().unwrap_or(Value::Null))
                .collect();
            groups.entry(key).or_default().push(row);
        }

        let mut results = Vec::new();
        for (group_key, group_rows) in groups {
            let agg_result = self.compute_aggregations(&group_rows, &query.aggregations)?;
            let mut row = Row::new(RowId::new(0));
            for (i, col) in query.group_by.iter().enumerate() {
                if let Some(val) = group_key.get(i) {
                    row.set(col, val.clone());
                }
            }
            for (alias, val) in agg_result {
                row.set(alias, val);
            }
            results.push(row);
        }

        results.sort_by(|a, b| {
            for col in &query.group_by {
                let a_val = a.get(col);
                let b_val = b.get(col);
                let ord = match (a_val, b_val) {
                    (Some(av), Some(bv)) => av.cmp(bv),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    fn compute_aggregations(
        &self,
        rows: &[&Row],
        aggregations: &[(String, AggFunction)],
    ) -> QueryResult<AggResult> {
        let mut result = AggResult::new();

        for (alias, func) in aggregations {
            let val = match func {
                AggFunction::Count => Value::Int64(rows.len() as i64),
                AggFunction::CountColumn(col) => {
                    let count = rows
                        .iter()
                        .filter(|r| r.get(col).map_or(false, |v| !v.is_null()))
                        .count();
                    Value::Int64(count as i64)
                }
                AggFunction::Sum(col) => {
                    let mut total = 0.0f64;
                    for row in rows {
                        if let Some(val) = row.get(col) {
                            if let Some(n) = val.as_f64() {
                                total += n;
                            }
                        }
                    }
                    Value::Float64(total)
                }
                AggFunction::Avg(col) => {
                    let mut total = 0.0f64;
                    let mut count = 0u64;
                    for row in rows {
                        if let Some(val) = row.get(col) {
                            if let Some(n) = val.as_f64() {
                                total += n;
                                count += 1;
                            }
                        }
                    }
                    if count == 0 {
                        Value::Null
                    } else {
                        Value::Float64(total / count as f64)
                    }
                }
                AggFunction::Min(col) => {
                    let mut min: Option<&Value> = None;
                    for row in rows {
                        if let Some(val) = row.get(col) {
                            if !val.is_null() {
                                match min {
                                    None => min = Some(val),
                                    Some(current) => {
                                        if val < current {
                                            min = Some(val);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    min.cloned().unwrap_or(Value::Null)
                }
                AggFunction::Max(col) => {
                    let mut max: Option<&Value> = None;
                    for row in rows {
                        if let Some(val) = row.get(col) {
                            if !val.is_null() {
                                match max {
                                    None => max = Some(val),
                                    Some(current) => {
                                        if val > current {
                                            max = Some(val);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    max.cloned().unwrap_or(Value::Null)
                }
            };
            result.insert(alias.clone(), val);
        }

        Ok(result)
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::id::RowId;

    fn test_rows() -> Vec<Row> {
        let mut rows = Vec::new();

        let mut r1 = Row::new(RowId::new(1));
        r1.set("name", Value::String("Alice".into()));
        r1.set("age", Value::Int64(25));
        r1.set("score", Value::Float64(85.5));
        r1.set("dept", Value::String("eng".into()));
        rows.push(r1);

        let mut r2 = Row::new(RowId::new(2));
        r2.set("name", Value::String("Bob".into()));
        r2.set("age", Value::Int64(30));
        r2.set("score", Value::Float64(92.0));
        r2.set("dept", Value::String("sales".into()));
        rows.push(r2);

        let mut r3 = Row::new(RowId::new(3));
        r3.set("name", Value::String("Charlie".into()));
        r3.set("age", Value::Int64(22));
        r3.set("score", Value::Float64(78.0));
        r3.set("dept", Value::String("eng".into()));
        rows.push(r3);

        let mut r4 = Row::new(RowId::new(4));
        r4.set("name", Value::String("Diana".into()));
        r4.set("age", Value::Int64(28));
        r4.set("score", Value::Float64(95.0));
        r4.set("dept", Value::String("sales".into()));
        rows.push(r4);

        rows
    }

    #[test]
    fn test_execute_filter() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users").filter(Filter::Gt("age".into(), Value::Int64(24)));
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_execute_select() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users").select(vec!["name".into(), "age".into()]);
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 4);
        for row in &result {
            assert!(row.has_column("name"));
            assert!(row.has_column("age"));
            assert!(!row.has_column("score"));
        }
    }

    #[test]
    fn test_execute_order_by() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users").order_by(Sort::desc("age"));
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(
            result[0].get("name"),
            Some(&Value::String("Bob".into()))
        );
        assert_eq!(
            result[3].get("name"),
            Some(&Value::String("Charlie".into()))
        );
    }

    #[test]
    fn test_execute_limit_offset() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users")
            .order_by(Sort::asc("name"))
            .limit(2)
            .offset(1);
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].get("name"),
            Some(&Value::String("Bob".into()))
        );
        assert_eq!(
            result[1].get("name"),
            Some(&Value::String("Charlie".into()))
        );
    }

    #[test]
    fn test_execute_count() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users").count("total");
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].get("total"),
            Some(&Value::Int64(4))
        );
    }

    #[test]
    fn test_execute_sum() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users").sum("age", "total_age");
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].get("total_age"),
            Some(&Value::Float64(105.0))
        );
    }

    #[test]
    fn test_execute_avg() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users").avg("score", "avg_score");
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 1);
        let avg = result[0].get("avg_score").unwrap().as_f64().unwrap();
        assert!((avg - 87.625).abs() < 0.001);
    }

    #[test]
    fn test_execute_min_max() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users")
            .min("score", "min_score")
            .max("score", "max_score");
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].get("min_score"),
            Some(&Value::Float64(78.0))
        );
        assert_eq!(
            result[0].get("max_score"),
            Some(&Value::Float64(95.0))
        );
    }

    #[test]
    fn test_execute_group_by() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users")
            .group_by(vec!["dept".into()])
            .count("cnt")
            .order_by(Sort::asc("dept"));
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].get("dept"),
            Some(&Value::String("eng".into()))
        );
        assert_eq!(result[0].get("cnt"), Some(&Value::Int64(2)));
        assert_eq!(
            result[1].get("dept"),
            Some(&Value::String("sales".into()))
        );
        assert_eq!(result[1].get("cnt"), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_execute_group_by_with_sum() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users")
            .group_by(vec!["dept".into()])
            .sum("age", "total_age")
            .order_by(Sort::asc("dept"));
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].get("total_age"),
            Some(&Value::Float64(47.0))
        );
        assert_eq!(
            result[1].get("total_age"),
            Some(&Value::Float64(58.0))
        );
    }

    #[test]
    fn test_execute_combined() {
        let executor = QueryExecutor::new();
        let rows = test_rows();
        let q = Query::new("users")
            .filter(Filter::Gte("age".into(), Value::Int64(25)))
            .select(vec!["name".into(), "score".into()])
            .order_by(Sort::desc("score"))
            .limit(2);
        let result = executor.execute(&rows, &q).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].get("name"),
            Some(&Value::String("Diana".into()))
        );
        assert_eq!(
            result[1].get("name"),
            Some(&Value::String("Bob".into()))
        );
    }
}
