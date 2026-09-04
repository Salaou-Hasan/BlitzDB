use std::collections::HashMap;

use crate::error::{CoreError, CoreResult};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;

/// The core table engine trait.
///
/// This defines the interface for all table operations.
/// Implementations provide the actual storage backend.
pub trait TableEngine: Send + Sync {
    /// Insert a new row into a table.
    fn insert(&mut self, table_name: &str, row: Row) -> CoreResult<RowId>;

    /// Get a row by its primary key.
    fn get(&self, table_name: &str, id: RowId) -> CoreResult<Option<Row>>;

    /// Update an existing row.
    fn update(&mut self, table_name: &str, id: RowId, values: HashMap<String, Value>) -> CoreResult<Row>;

    /// Delete a row by its primary key.
    fn delete(&mut self, table_name: &str, id: RowId) -> CoreResult<bool>;

    /// Scan all rows in a table.
    fn scan(&self, table_name: &str) -> CoreResult<Vec<Row>>;

    /// Get the schema for a table.
    fn schema(&self, table_name: &str) -> CoreResult<&TableSchema>;

    /// Get the number of rows in a table.
    fn count(&self, table_name: &str) -> CoreResult<usize>;

    /// Create a new table with the given schema.
    fn create_table(&mut self, schema: TableSchema) -> CoreResult<()>;

    /// Drop a table.
    fn drop_table(&mut self, table_name: &str) -> CoreResult<()>;

    /// Check if a table exists.
    fn table_exists(&self, table_name: &str) -> bool;
}

/// An in-memory table engine implementation.
pub struct InMemoryTableEngine {
    tables: HashMap<String, InMemoryTable>,
}

struct InMemoryTable {
    schema: TableSchema,
    rows: HashMap<RowId, Row>,
    next_id: u64,
}

impl InMemoryTable {
    fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            rows: HashMap::new(),
            next_id: 1,
        }
    }

    fn next_row_id(&mut self) -> RowId {
        let id = RowId::new(self.next_id);
        self.next_id += 1;
        id
    }
}

impl Default for InMemoryTableEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTableEngine {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}

impl TableEngine for InMemoryTableEngine {
    fn insert(&mut self, table_name: &str, mut row: Row) -> CoreResult<RowId> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;

        // Validate against schema
        table.schema.validate_row_values(&row.values)?;

        // Assign row ID
        let row_id = table.next_row_id();
        row.id = row_id;

        table.rows.insert(row_id, row);
        Ok(row_id)
    }

    fn get(&self, table_name: &str, id: RowId) -> CoreResult<Option<Row>> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;

        Ok(table.rows.get(&id).cloned())
    }

    fn update(
        &mut self,
        table_name: &str,
        id: RowId,
        values: HashMap<String, Value>,
    ) -> CoreResult<Row> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;

        let row = table
            .rows
            .get_mut(&id)
            .ok_or_else(|| CoreError::RowNotFound(id.as_u64()))?;

        for (key, value) in values {
            row.values.insert(key, value);
        }

        Ok(row.clone())
    }

    fn delete(&mut self, table_name: &str, id: RowId) -> CoreResult<bool> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;

        Ok(table.rows.remove(&id).is_some())
    }

    fn scan(&self, table_name: &str) -> CoreResult<Vec<Row>> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;

        Ok(table.rows.values().cloned().collect())
    }

    fn schema(&self, table_name: &str) -> CoreResult<&TableSchema> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;

        Ok(&table.schema)
    }

    fn count(&self, table_name: &str) -> CoreResult<usize> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;

        Ok(table.rows.len())
    }

    fn create_table(&mut self, schema: TableSchema) -> CoreResult<()> {
        let name = schema.name.clone();
        if self.tables.contains_key(&name) {
            return Err(CoreError::TableAlreadyExists(name));
        }
        self.tables.insert(name, InMemoryTable::new(schema));
        Ok(())
    }

    fn drop_table(&mut self, table_name: &str) -> CoreResult<()> {
        self.tables
            .remove(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;
        Ok(())
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::column::{ColumnDef, ColumnType};
    use blitz_types::schema::TableSchema;

    fn test_schema() -> TableSchema {
        TableSchema::new("users")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("name", ColumnType::String))
            .with_column(ColumnDef::new("email", ColumnType::String).unique())
    }

    #[test]
    fn test_create_table() {
        let mut engine = InMemoryTableEngine::new();
        let schema = test_schema();
        assert!(engine.create_table(schema).is_ok());
        assert!(engine.table_exists("users"));
    }

    #[test]
    fn test_insert_and_get() {
        let mut engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));
        row.set("email", Value::String("alice@example.com".into()));

        let id = engine.insert("users", row).unwrap();
        let fetched = engine.get("users", id).unwrap();
        assert!(fetched.is_some());
        assert_eq!(
            fetched.unwrap().get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_update() {
        let mut engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));
        row.set("email", Value::String("alice@example.com".into()));

        let id = engine.insert("users", row).unwrap();

        let mut updates = HashMap::new();
        updates.insert("name".into(), Value::String("Bob".into()));

        let updated = engine.update("users", id, updates).unwrap();
        assert_eq!(updated.get("name"), Some(&Value::String("Bob".into())));
    }

    #[test]
    fn test_delete() {
        let mut engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));
        row.set("email", Value::String("alice@example.com".into()));

        let id = engine.insert("users", row).unwrap();
        assert!(engine.delete("users", id).unwrap());
        assert!(engine.get("users", id).unwrap().is_none());
    }

    #[test]
    fn test_scan() {
        let mut engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        for i in 0..5 {
            let mut row = Row::new(RowId::new(0));
            row.set("id", Value::Int64(i));
            row.set("name", Value::String(format!("User {}", i)));
            row.set(
                "email",
                Value::String(format!("user{}@example.com", i)),
            );
            engine.insert("users", row).unwrap();
        }

        let rows = engine.scan("users").unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_count() {
        let mut engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();
        assert_eq!(engine.count("users").unwrap(), 0);

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));
        row.set("email", Value::String("alice@example.com".into()));
        engine.insert("users", row).unwrap();

        assert_eq!(engine.count("users").unwrap(), 1);
    }
}
