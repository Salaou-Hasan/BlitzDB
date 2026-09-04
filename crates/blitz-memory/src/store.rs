use std::collections::HashMap;

use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;

/// An in-memory store for table data.
pub struct MemoryStore {
    tables: HashMap<String, TableData>,
}

struct TableData {
    schema: TableSchema,
    rows: HashMap<RowId, Row>,
    next_id: u64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), String> {
        let name = schema.name.clone();
        if self.tables.contains_key(&name) {
            return Err(format!("table '{}' already exists", name));
        }
        self.tables.insert(
            name,
            TableData {
                schema,
                rows: HashMap::new(),
                next_id: 1,
            },
        );
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), String> {
        self.tables
            .remove(name)
            .ok_or_else(|| format!("table '{}' not found", name))?;
        Ok(())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn schema(&self, name: &str) -> Result<&TableSchema, String> {
        self.tables
            .get(name)
            .map(|t| &t.schema)
            .ok_or_else(|| format!("table '{}' not found", name))
    }

    pub fn insert(&mut self, table_name: &str, mut row: Row) -> Result<RowId, String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;

        table.schema.validate_row_values(&row.values)
            .map_err(|e| e.to_string())?;

        let id = RowId::new(table.next_id);
        table.next_id += 1;
        row.id = id;
        table.rows.insert(id, row);
        Ok(id)
    }

    pub fn get(&self, table_name: &str, id: RowId) -> Result<Option<Row>, String> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;
        Ok(table.rows.get(&id).cloned())
    }

    pub fn update(
        &mut self,
        table_name: &str,
        id: RowId,
        values: HashMap<String, blitz_types::value::Value>,
    ) -> Result<Row, String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;

        let row = table
            .rows
            .get_mut(&id)
            .ok_or_else(|| format!("row {} not found", id.as_u64()))?;

        for (key, value) in values {
            row.values.insert(key, value);
        }

        Ok(row.clone())
    }

    pub fn delete(&mut self, table_name: &str, id: RowId) -> Result<bool, String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;
        Ok(table.rows.remove(&id).is_some())
    }

    pub fn scan(&self, table_name: &str) -> Result<Vec<Row>, String> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;
        Ok(table.rows.values().cloned().collect())
    }

    pub fn count(&self, table_name: &str) -> Result<usize, String> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;
        Ok(table.rows.len())
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::column::{ColumnDef, ColumnType};
    use blitz_types::value::Value;

    fn test_schema() -> TableSchema {
        TableSchema::new("users")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("name", ColumnType::String))
    }

    #[test]
    fn test_create_and_insert() {
        let mut store = MemoryStore::new();
        store.create_table(test_schema()).unwrap();

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));

        let id = store.insert("users", row).unwrap();
        assert_eq!(id.as_u64(), 1);
    }

    #[test]
    fn test_get() {
        let mut store = MemoryStore::new();
        store.create_table(test_schema()).unwrap();

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));

        let id = store.insert("users", row).unwrap();
        let fetched = store.get("users", id).unwrap().unwrap();
        assert_eq!(fetched.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_count() {
        let mut store = MemoryStore::new();
        store.create_table(test_schema()).unwrap();
        assert_eq!(store.count("users").unwrap(), 0);

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));
        store.insert("users", row).unwrap();
        assert_eq!(store.count("users").unwrap(), 1);
    }
}
