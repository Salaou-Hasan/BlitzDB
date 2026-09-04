use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::TypeResult;
use crate::id::RowId;
use crate::schema::TableSchema;
use crate::value::Value;

/// A row of data in a table.
///
/// Rows are stored as maps from column name to value for flexibility.
/// The primary key is stored separately for efficient access.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub id: RowId,
    pub values: HashMap<String, Value>,
}

impl Row {
    /// Create a new row with the given ID.
    pub fn new(id: RowId) -> Self {
        Self {
            id,
            values: HashMap::new(),
        }
    }

    /// Create a new row with an auto-generated ID.
    pub fn auto_id() -> Self {
        Self::new(RowId::new(0))
    }

    /// Set a column value on this row.
    pub fn set(&mut self, name: impl Into<String>, value: Value) {
        self.values.insert(name.into(), value);
    }

    /// Get a column value from this row.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    /// Get a mutable reference to a column value.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.values.get_mut(name)
    }

    /// Check if a column exists.
    pub fn has_column(&self, name: &str) -> bool {
        self.values.contains_key(name)
    }

    /// Remove a column value.
    pub fn remove(&mut self, name: &str) -> Option<Value> {
        self.values.remove(name)
    }

    /// Get the number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterate over all column values.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.values.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Create a row from a map of values, validating against a schema.
    pub fn from_values(id: RowId, values: HashMap<String, Value>, schema: &TableSchema) -> TypeResult<Self> {
        schema.validate_row_values(&values)?;
        Ok(Self { id, values })
    }

    /// Extract the primary key values from this row.
    pub fn primary_key_values(&self, schema: &TableSchema) -> TypeResult<Vec<Value>> {
        let mut pk_values = Vec::new();
        for col in schema.primary_key_columns() {
            let val = self
                .values
                .get(&col.name)
                .cloned()
                .unwrap_or(Value::Null);
            pk_values.push(val);
        }
        Ok(pk_values)
    }

    /// Project this row to only include the specified columns.
    pub fn project(&self, columns: &[&str]) -> Self {
        let mut projected = Row::new(self.id);
        for col in columns {
            if let Some(val) = self.values.get(*col) {
                projected.values.insert(col.to_string(), val.clone());
            }
        }
        projected
    }
}

impl From<HashMap<String, Value>> for Row {
    fn from(values: HashMap<String, Value>) -> Self {
        Self {
            id: RowId::new(0),
            values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::{ColumnDef, ColumnType};

    fn test_schema() -> TableSchema {
        TableSchema::new("users")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("name", ColumnType::String))
            .with_column(ColumnDef::new("email", ColumnType::String).unique())
    }

    #[test]
    fn test_row_creation() {
        let mut row = Row::new(RowId::new(1));
        row.set("name", Value::String("Alice".into()));
        row.set("email", Value::String("alice@example.com".into()));

        assert_eq!(row.id, RowId::new(1));
        assert_eq!(row.len(), 2);
        assert_eq!(row.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_row_validation() {
        let schema = test_schema();
        let mut values = HashMap::new();
        values.insert("id".into(), Value::Int64(1));
        values.insert("name".into(), Value::String("Alice".into()));
        values.insert("email".into(), Value::String("alice@example.com".into()));

        let row = Row::from_values(RowId::new(1), values, &schema);
        assert!(row.is_ok());
    }

    #[test]
    fn test_row_validation_missing_required() {
        let schema = test_schema();
        let mut values = HashMap::new();
        values.insert("id".into(), Value::Int64(1));

        let row = Row::from_values(RowId::new(1), values, &schema);
        assert!(row.is_err());
    }

    #[test]
    fn test_row_projection() {
        let mut row = Row::new(RowId::new(1));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));
        row.set("email", Value::String("alice@example.com".into()));

        let projected = row.project(&["id", "name"]);
        assert_eq!(projected.len(), 2);
        assert!(projected.has_column("id"));
        assert!(projected.has_column("name"));
        assert!(!projected.has_column("email"));
    }
}
