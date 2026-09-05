use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::{CoreError, CoreResult};
use crate::pool::RowPool;
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;

fn lock_err() -> CoreError {
    CoreError::Internal("lock poisoned".into())
}

/// The core table engine trait.
///
/// All methods take `&self`: the engine is internally synchronized with
/// table-level locks, so concurrent operations on different tables proceed
/// in parallel and the engine can be shared across threads/tasks.
pub trait TableEngine: Send + Sync {
    /// Insert a new row into a table.
    fn insert(&self, table_name: &str, row: Row) -> CoreResult<RowId>;

    /// Get a row by its primary key (cloning read).
    fn get(&self, table_name: &str, id: RowId) -> CoreResult<Option<Row>>;

    /// Get a row by its primary key without cloning (shared handle).
    fn get_arc(&self, table_name: &str, id: RowId) -> CoreResult<Option<Arc<Row>>>;

    /// Update an existing row.
    fn update(
        &self,
        table_name: &str,
        id: RowId,
        values: HashMap<String, Value>,
    ) -> CoreResult<Row>;

    /// Delete a row by its primary key.
    fn delete(&self, table_name: &str, id: RowId) -> CoreResult<bool>;

    /// Delete a row and return it without cloning (shared handle).
    fn take(&self, table_name: &str, id: RowId) -> CoreResult<Option<Arc<Row>>>;

    /// Scan all rows in a table (cloning read).
    fn scan(&self, table_name: &str) -> CoreResult<Vec<Row>>;

    /// Scan all rows in a table without cloning (shared handles).
    fn scan_arcs(&self, table_name: &str) -> CoreResult<Vec<Arc<Row>>>;

    /// Get the schema for a table (cloned; schemas are small metadata).
    fn schema(&self, table_name: &str) -> CoreResult<TableSchema>;

    /// Get the number of rows in a table.
    fn count(&self, table_name: &str) -> CoreResult<usize>;

    /// Create a new table with the given schema.
    fn create_table(&self, schema: TableSchema) -> CoreResult<()>;

    /// Drop a table.
    fn drop_table(&self, table_name: &str) -> CoreResult<()>;

    /// Check if a table exists.
    fn table_exists(&self, table_name: &str) -> bool;
}

/// An in-memory table engine implementation with table-level locking.
///
/// The table map is behind one `RwLock`, but each table has its own
/// `RwLock`: single-table operations clone the table's `Arc` under a
/// short map read-lock, then lock only that table. Operations on
/// different tables never block each other.
pub struct InMemoryTableEngine {
    tables: RwLock<HashMap<String, Arc<RwLock<InMemoryTable>>>>,
}

struct InMemoryTable {
    schema: TableSchema,
    rows: HashMap<RowId, Arc<Row>>,
    next_id: u64,
    row_pool: RowPool,
}

impl InMemoryTable {
    fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            rows: HashMap::new(),
            next_id: 1,
            row_pool: RowPool::new(),
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
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Resolve a table handle. Holds the map lock only long enough to
    /// clone the `Arc`; the caller then locks just that table.
    fn table(&self, table_name: &str) -> CoreResult<Arc<RwLock<InMemoryTable>>> {
        self.tables
            .read()
            .map_err(|_| lock_err())?
            .get(table_name)
            .cloned()
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))
    }
}

impl TableEngine for InMemoryTableEngine {
    fn insert(&self, table_name: &str, mut row: Row) -> CoreResult<RowId> {
        let table = self.table(table_name)?;
        let mut table = table.write().map_err(|_| lock_err())?;

        // Validate against schema
        table.schema.validate_row_values(&row.values)?;

        // Assign row ID
        let row_id = table.next_row_id();
        row.id = row_id;

        table.rows.insert(row_id, Arc::new(row));
        Ok(row_id)
    }

    fn get(&self, table_name: &str, id: RowId) -> CoreResult<Option<Row>> {
        Ok(self.get_arc(table_name, id)?.map(|arc| arc.as_ref().clone()))
    }

    fn get_arc(&self, table_name: &str, id: RowId) -> CoreResult<Option<Arc<Row>>> {
        let table = self.table(table_name)?;
        let table = table.read().map_err(|_| lock_err())?;
        Ok(table.rows.get(&id).cloned())
    }

    fn update(
        &self,
        table_name: &str,
        id: RowId,
        values: HashMap<String, Value>,
    ) -> CoreResult<Row> {
        let table = self.table(table_name)?;
        let mut table = table.write().map_err(|_| lock_err())?;

        // Split borrows through the guard: rows for lookup, pool for scratch.
        let InMemoryTable { rows, row_pool: pool, .. } = &mut *table;
        let slot = rows
            .get_mut(&id)
            .ok_or_else(|| CoreError::RowNotFound(id.as_u64()))?;

        // Copy-on-write in place: clones the row only if other
        // `Arc` handles (e.g. outstanding `get_arc` results) exist.
        let row = Arc::make_mut(slot);
        for (key, value) in values {
            row.values.insert(key, value);
        }

        // Build the return value from a pooled scratch row so the
        // returned HashMap reuses a previous allocation.
        let mut pooled_row = pool.checkout();
        pooled_row.id = row.id;
        for (key, value) in row.values.iter() {
            pooled_row.values.insert(key.clone(), value.clone());
        }

        Ok(pooled_row)
    }

    fn delete(&self, table_name: &str, id: RowId) -> CoreResult<bool> {
        Ok(self.take(table_name, id)?.is_some())
    }

    fn take(&self, table_name: &str, id: RowId) -> CoreResult<Option<Arc<Row>>> {
        let table = self.table(table_name)?;
        let mut table = table.write().map_err(|_| lock_err())?;
        Ok(table.rows.remove(&id))
    }

    fn scan(&self, table_name: &str) -> CoreResult<Vec<Row>> {
        Ok(self
            .scan_arcs(table_name)?
            .iter()
            .map(|arc| arc.as_ref().clone())
            .collect())
    }

    fn scan_arcs(&self, table_name: &str) -> CoreResult<Vec<Arc<Row>>> {
        let table = self.table(table_name)?;
        let table = table.read().map_err(|_| lock_err())?;
        Ok(table.rows.values().cloned().collect())
    }

    fn schema(&self, table_name: &str) -> CoreResult<TableSchema> {
        let table = self.table(table_name)?;
        let table = table.read().map_err(|_| lock_err())?;
        Ok(table.schema.clone())
    }

    fn count(&self, table_name: &str) -> CoreResult<usize> {
        let table = self.table(table_name)?;
        let table = table.read().map_err(|_| lock_err())?;
        Ok(table.rows.len())
    }

    fn create_table(&self, schema: TableSchema) -> CoreResult<()> {
        let name = schema.name.clone();
        let mut tables = self.tables.write().map_err(|_| lock_err())?;
        if tables.contains_key(&name) {
            return Err(CoreError::TableAlreadyExists(name));
        }
        tables.insert(name, Arc::new(RwLock::new(InMemoryTable::new(schema))));
        Ok(())
    }

    fn drop_table(&self, table_name: &str) -> CoreResult<()> {
        self.tables
            .write()
            .map_err(|_| lock_err())?
            .remove(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;
        Ok(())
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.tables
            .read()
            .map(|tables| tables.contains_key(table_name))
            .unwrap_or(false)
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

    fn make_row(id: i64, name: &str) -> Row {
        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(id));
        row.set("name", Value::String(name.into()));
        row.set("email", Value::String(format!("{}@example.com", name)));
        row
    }

    #[test]
    fn test_create_table() {
        let engine = InMemoryTableEngine::new();
        let schema = test_schema();
        assert!(engine.create_table(schema).is_ok());
        assert!(engine.table_exists("users"));
    }

    #[test]
    fn test_insert_and_get() {
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let id = engine.insert("users", make_row(1, "Alice")).unwrap();
        let fetched = engine.get("users", id).unwrap();
        assert!(fetched.is_some());
        assert_eq!(
            fetched.unwrap().get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_update() {
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let id = engine.insert("users", make_row(1, "Alice")).unwrap();

        let mut updates = HashMap::new();
        updates.insert("name".into(), Value::String("Bob".into()));

        let updated = engine.update("users", id, updates).unwrap();
        assert_eq!(updated.get("name"), Some(&Value::String("Bob".into())));
    }

    #[test]
    fn test_delete() {
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let id = engine.insert("users", make_row(1, "Alice")).unwrap();
        assert!(engine.delete("users", id).unwrap());
        assert!(engine.get("users", id).unwrap().is_none());
    }

    #[test]
    fn test_scan() {
        let engine = InMemoryTableEngine::new();
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
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();
        assert_eq!(engine.count("users").unwrap(), 0);

        engine.insert("users", make_row(1, "Alice")).unwrap();

        assert_eq!(engine.count("users").unwrap(), 1);
    }

    #[test]
    fn test_get_arc_returns_shared_handle() {
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let id = engine.insert("users", make_row(1, "Alice")).unwrap();
        let a = engine.get_arc("users", id).unwrap().unwrap();
        let b = engine.get_arc("users", id).unwrap().unwrap();
        // Same allocation, no clone: pointer equality.
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_take_removes_and_returns_row() {
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let id = engine.insert("users", make_row(1, "Alice")).unwrap();
        let taken = engine.take("users", id).unwrap().unwrap();
        assert_eq!(taken.get("name"), Some(&Value::String("Alice".into())));
        assert!(engine.get("users", id).unwrap().is_none());
    }

    #[test]
    fn test_scan_arcs_matches_scan() {
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();
        engine.insert("users", make_row(1, "Alice")).unwrap();
        engine.insert("users", make_row(2, "Bob")).unwrap();

        let arcs = engine.scan_arcs("users").unwrap();
        let rows = engine.scan("users").unwrap();
        assert_eq!(arcs.len(), rows.len());
        assert_eq!(arcs.len(), 2);
    }

    #[test]
    fn test_concurrent_tables_do_not_block() {
        use std::thread;

        let engine = Arc::new(InMemoryTableEngine::new());
        engine.create_table(test_schema()).unwrap();
        engine
            .create_table(
                TableSchema::new("orders")
                    .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key()),
            )
            .unwrap();

        let mut handles = Vec::new();
        for t in 0..8 {
            let engine = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                let table = if t % 2 == 0 { "users" } else { "orders" };
                for i in 0..250 {
                    let mut row = Row::new(RowId::new(0));
                    row.set("id", Value::Int64(t * 1000 + i));
                    if table == "users" {
                        row.set("name", Value::String("n".into()));
                        row.set("email", Value::String(format!("t{t}i{i}@x.com")));
                    }
                    engine.insert(table, row).unwrap();
                }
                // Concurrent reads while others write.
                for _ in 0..10 {
                    let _ = engine.count(table).unwrap();
                    let _ = engine.scan_arcs(table).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(engine.count("users").unwrap(), 1000);
        assert_eq!(engine.count("orders").unwrap(), 1000);
    }

    #[test]
    fn test_throughput_insert_and_scan() {
        // Perf smoke test: asserts correctness, reports throughput.
        // Run with --nocapture to see ops/sec. No timing assertions.
        const N: i64 = 10_000;
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();

        let start = std::time::Instant::now();
        for i in 0..N {
            let mut row = Row::new(RowId::new(0));
            row.set("id", Value::Int64(i));
            row.set("name", Value::String(format!("User {}", i)));
            row.set(
                "email",
                Value::String(format!("user{}@example.com", i)),
            );
            engine.insert("users", row).unwrap();
        }
        let insert_elapsed = start.elapsed();

        let start = std::time::Instant::now();
        let rows = engine.scan("users").unwrap();
        let scan_elapsed = start.elapsed();
        assert_eq!(rows.len(), N as usize);

        println!("insert: {} rows in {:?} ({:.0} rows/sec)", N, insert_elapsed, N as f64 / insert_elapsed.as_secs_f64());
        println!("scan: {} rows in {:?} ({:.0} rows/sec)", N, scan_elapsed, N as f64 / scan_elapsed.as_secs_f64());
    }
}
