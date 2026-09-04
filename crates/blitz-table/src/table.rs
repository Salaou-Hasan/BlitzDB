use std::collections::HashMap;

use crate::error::{TableError, TableResult};
use blitz_core::InMemoryTableEngine;
use blitz_core::TableEngine;
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;

/// High-level table abstraction wrapping the core engine.
pub struct Table {
    engine: InMemoryTableEngine,
    name: String,
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let mut engine = InMemoryTableEngine::new();
        let schema = TableSchema::new(name.clone());
        engine.create_table(schema).unwrap();
        Self { engine, name }
    }

    pub fn with_schema(schema: TableSchema) -> Self {
        let name = schema.name.clone();
        let mut engine = InMemoryTableEngine::new();
        engine.create_table(schema).unwrap();
        Self { engine, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn insert(&mut self, row: Row) -> TableResult<RowId> {
        self.engine.insert(&self.name, row).map_err(TableError::from)
    }

    pub fn get(&self, id: RowId) -> TableResult<Option<Row>> {
        self.engine.get(&self.name, id).map_err(TableError::from)
    }

    pub fn update(&mut self, id: RowId, values: HashMap<String, Value>) -> TableResult<Row> {
        self.engine
            .update(&self.name, id, values)
            .map_err(TableError::from)
    }

    pub fn delete(&mut self, id: RowId) -> TableResult<bool> {
        self.engine.delete(&self.name, id).map_err(TableError::from)
    }

    pub fn scan(&self) -> TableResult<Vec<Row>> {
        self.engine.scan(&self.name).map_err(TableError::from)
    }

    pub fn count(&self) -> TableResult<usize> {
        self.engine.count(&self.name).map_err(TableError::from)
    }

    pub fn schema(&self) -> TableResult<&TableSchema> {
        self.engine.schema(&self.name).map_err(TableError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::column::{ColumnDef, ColumnType};

    fn users_table() -> Table {
        let schema = TableSchema::new("users")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("name", ColumnType::String))
            .with_column(ColumnDef::new("email", ColumnType::String));
        Table::with_schema(schema)
    }

    #[test]
    fn test_table_insert_and_get() {
        let mut table = users_table();
        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));
        row.set("email", Value::String("alice@example.com".into()));

        let id = table.insert(row).unwrap();
        let fetched = table.get(id).unwrap().unwrap();
        assert_eq!(fetched.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_table_scan() {
        let mut table = users_table();
        for i in 0..3 {
            let mut row = Row::new(RowId::new(0));
            row.set("id", Value::Int64(i));
            row.set("name", Value::String(format!("User {}", i)));
            row.set("email", Value::String(format!("user{}@example.com", i)));
            table.insert(row).unwrap();
        }

        let rows = table.scan().unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_table_delete() {
        let mut table = users_table();
        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));
        row.set("email", Value::String("alice@example.com".into()));

        let id = table.insert(row).unwrap();
        assert!(table.delete(id).unwrap());
        assert!(table.get(id).unwrap().is_none());
    }
}
