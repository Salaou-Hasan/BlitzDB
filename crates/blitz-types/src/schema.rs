use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::column::ColumnDef;
use crate::error::{TypeError, TypeResult};
use crate::id::TableId;
use crate::value::Value;

/// Schema for a table in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    #[serde(skip)]
    column_index: HashMap<String, usize>,
}

impl TableSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: TableId::new(),
            name: name.into(),
            columns: Vec::new(),
            column_index: HashMap::new(),
        }
    }

    pub fn with_column(mut self, column: ColumnDef) -> Self {
        let idx = self.columns.len();
        self.column_index.insert(column.name.clone(), idx);
        self.columns.push(column);
        self
    }

    pub fn column(&self, name: &str) -> TypeResult<&ColumnDef> {
        self.column_index
            .get(name)
            .and_then(|&idx| self.columns.get(idx))
            .ok_or_else(|| TypeError::SchemaError(format!("column '{}' not found", name)))
    }

    pub fn column_index(&self, name: &str) -> TypeResult<usize> {
        self.column_index
            .get(name)
            .copied()
            .ok_or_else(|| TypeError::SchemaError(format!("column '{}' not found", name)))
    }

    pub fn primary_key_columns(&self) -> Vec<&ColumnDef> {
        self.columns.iter().filter(|c| c.primary_key).collect()
    }

    pub fn has_primary_key(&self) -> bool {
        self.columns.iter().any(|c| c.primary_key)
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn validate_row_values(
        &self,
        values: &HashMap<String, Value>,
    ) -> TypeResult<()> {
        for col in &self.columns {
            match values.get(&col.name) {
                Some(val) if val.is_null() && !col.nullable => {
                    return Err(TypeError::NullValue);
                }
                Some(val) if !val.is_null() && !value_matches_type(val, &col.column_type) => {
                    return Err(TypeError::SchemaError(format!(
                        "column '{}' expects {}, got {}",
                        col.name,
                        col.column_type,
                        val.type_name()
                    )));
                }
                None if col.default.is_some() => {}
                None if col.primary_key => {
                    return Err(TypeError::SchemaError(format!(
                        "primary key column '{}' is required",
                        col.name
                    )));
                }
                None if !col.nullable => {
                    return Err(TypeError::SchemaError(format!(
                        "column '{}' is required",
                        col.name
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Strict column-type check for trusted validation.
/// Ints accept only their exact width (no silent coercion — coercion bugs
/// caused silent data corruption in early prototypes). Null always passes
/// here (nullability handled above). Json/Array accept their exact shapes;
///
/// NOTE: `Decimal` accepts `Decimal` + numeric strings? No — strict:
/// only `Value::Decimal`. Numbers in JSON payloads map via WAL/snapshot
/// `json_to_value` to Int64/UInt64/Float64, never Decimal.
fn value_matches_type(val: &Value, ct: &crate::column::ColumnType) -> bool {
    use crate::column::ColumnType as T;
    use crate::value::Value as V;
    match (val, ct) {
        (V::Boolean(_), T::Boolean) => true,
        (V::Int8(_), T::Int8) => true,
        (V::Int16(_), T::Int16) => true,
        (V::Int32(_), T::Int32) => true,
        (V::Int64(_), T::Int64) => true,
        (V::UInt8(_), T::UInt8) => true,
        (V::UInt16(_), T::UInt16) => true,
        (V::UInt32(_), T::UInt32) => true,
        (V::UInt64(_), T::UInt64) => true,
        (V::Float32(_), T::Float32) => true,
        (V::Float64(_), T::Float64) => true,
        (V::Decimal(_), T::Decimal) => true,
        (V::String(_), T::String) => true,
        (V::Bytes(_), T::Bytes) => true,
        (V::Uuid(_), T::Uuid) => true,
        (V::Timestamp(_), T::Timestamp) => true,
        (V::Date(_), T::Date) => true,
        (V::Json(_), T::Json) => true,
        (V::Array(_), T::Array) => true,
        _ => false,
    }
}

/// Schema for the entire database.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Schema {
    pub tables: Vec<TableSchema>,
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, table: TableSchema) -> TypeResult<()> {
        if self.tables.iter().any(|t| t.name == table.name) {
            return Err(TypeError::SchemaError(format!(
                "table '{}' already exists",
                table.name
            )));
        }
        self.tables.push(table);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> TypeResult<&TableSchema> {
        self.tables
            .iter()
            .find(|t| t.name == name)
            .ok_or_else(|| TypeError::SchemaError(format!("table '{}' not found", name)))
    }

    pub fn get_table_mut(&mut self, name: &str) -> TypeResult<&mut TableSchema> {
        self.tables
            .iter_mut()
            .find(|t| t.name == name)
            .ok_or_else(|| TypeError::SchemaError(format!("table '{}' not found", name)))
    }
}
