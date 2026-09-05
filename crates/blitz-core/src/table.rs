use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{CoreError, CoreResult};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;

/// The core table engine trait.
///
/// All methods take `&self`: the engine is internally synchronized with
/// table-level locks, so concurrent operations on different tables proceed
/// in parallel and the engine can be shared across threads/tasks.
pub trait TableEngine: Send + Sync {
    /// Insert a new row into a table.
    fn insert(&self, table_name: &str, row: Row) -> CoreResult<RowId>;

    /// Insert without schema validation. Shard/cache fast path for trusted
    /// app layers that already guarantee shape (saves 2-3 hash lookups per
    /// insert, ~10% of insert service time). Default impl validates.
    /// Untrusted clients must use `insert`.
    fn insert_unchecked(&self, table_name: &str, row: Row) -> CoreResult<RowId> {
        self.insert(table_name, row)
    }

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

    /// List all table names (for snapshots / observability).
    fn table_names(&self) -> Vec<String>;

    /// O(1) point lookup via a unique/PK column index. Errs when the column
    /// is not unique-indexed (full scans stay out of the hot path by design).
    fn lookup_by_unique(
        &self,
        table_name: &str,
        column: &str,
        value: &Value,
    ) -> CoreResult<Option<Arc<Row>>>;

    /// Restore a row with its original ID (snapshot/WAL replay).
    /// Bumps `next_id` past the restored ID so later inserts don't collide.
    /// Fails if the ID already exists (replay must be idempotent-ordered).
    fn insert_preserving_id(&self, table_name: &str, row: Row) -> CoreResult<RowId>;

    /// Check if a table exists.
    fn table_exists(&self, table_name: &str) -> bool;
}

/// An in-memory table engine implementation with table-level locking.
///
/// The table map is a sharded `DashMap` (lock-free reads, no global map
/// lock); each table has its own `RwLock`. Operations on different tables
/// never block each other, and map lookups scale across cores — the old
/// single-`RwLock` map collapsed past ~16 threads on the A-maplock bench.
pub struct InMemoryTableEngine {
    tables: dashmap::DashMap<String, Arc<RwLock<InMemoryTable>>>,
}

struct InMemoryTable {
    schema: TableSchema,
    rows: HashMap<RowId, Arc<Row>>,
    next_id: u64,
    /// Unique secondary indexes: column → (value → RowId) for PK + unique
    /// columns. Maintained under the table write lock (no extra locking);
    /// converts duplicate-key checks from O(N) scans to O(1) lookups.
    uniq: HashMap<String, HashMap<Value, RowId>>,
}

impl InMemoryTable {
    fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            rows: HashMap::new(),
            next_id: 1,
            uniq: HashMap::new(),
        }
    }

    fn next_row_id(&mut self) -> RowId {
        let id = RowId::new(self.next_id);
        self.next_id += 1;
        id
    }

    /// Column names requiring uniqueness (PK + unique, non-null values only).
    fn uniq_cols(&self) -> Vec<String> {
        self.schema
            .columns
            .iter()
            .filter(|c| c.primary_key || c.unique)
            .map(|c| c.name.clone())
            .collect()
    }

    fn check_uniq(&self, cols: &[String], values: &HashMap<String, Value>, self_id: Option<RowId>) -> CoreResult<()> {
        for col in cols {
            if let Some(v) = values.get(col) {
                if v.is_null() {
                    continue;
                }
                if let Some(idx) = self.uniq.get(col) {
                    if let Some(owner) = idx.get(v) {
                        if Some(*owner) != self_id {
                            return Err(CoreError::DuplicateKey(format!("{}={:?}", col, v)));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn index_row(&mut self, cols: &[String], id: RowId, values: &HashMap<String, Value>) {
        for col in cols {
            if let Some(v) = values.get(col) {
                if v.is_null() {
                    continue;
                }
                self.uniq.entry(col.clone()).or_default().insert(v.clone(), id);
            }
        }
    }

    fn unindex_row(&mut self, cols: &[String], id: RowId, values: &HashMap<String, Value>) {
        for col in cols {
            if let Some(v) = values.get(col) {
                if let Some(idx) = self.uniq.get_mut(col) {
                    if idx.get(v) == Some(&id) {
                        idx.remove(v);
                    }
                }
            }
        }
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
            tables: dashmap::DashMap::new(),
        }
    }

    /// Resolve a table handle. DashMap read is shard-locked, not globally
    /// locked; the caller then locks just that table.
    fn table(&self, table_name: &str) -> CoreResult<Arc<RwLock<InMemoryTable>>> {
        self.tables
            .get(table_name)
            .map(|r| r.clone())
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))
    }
}

impl TableEngine for InMemoryTableEngine {
    fn insert(&self, table_name: &str, mut row: Row) -> CoreResult<RowId> {
        let table = self.table(table_name)?;
        let mut table = table.write();

        // Validate against schema
        table.schema.validate_row_values(&row.values)?;

        // PK/unique enforcement via maintained O(1) indexes.
        let cols = table.uniq_cols();
        table.check_uniq(&cols, &row.values, None)?;

        // Assign row ID
        let row_id = table.next_row_id();
        row.id = row_id;

        table.index_row(&cols, row_id, &row.values);
        table.rows.insert(row_id, Arc::new(row));
        Ok(row_id)
    }

    fn insert_unchecked(&self, table_name: &str, mut row: Row) -> CoreResult<RowId> {
        let table = self.table(table_name)?;
        let mut table = table.write();
        // Unchecked skips schema validation but NOT uniqueness: indexes must
        // stay consistent or later validated inserts see phantom state.
        let cols = table.uniq_cols();
        table.check_uniq(&cols, &row.values, None)?;
        let row_id = table.next_row_id();
        row.id = row_id;
        table.index_row(&cols, row_id, &row.values);
        table.rows.insert(row_id, Arc::new(row));
        Ok(row_id)
    }

    fn get(&self, table_name: &str, id: RowId) -> CoreResult<Option<Row>> {
        Ok(self.get_arc(table_name, id)?.map(|arc| arc.as_ref().clone()))
    }

    fn get_arc(&self, table_name: &str, id: RowId) -> CoreResult<Option<Arc<Row>>> {
        let table = self.table(table_name)?;
        let table = table.read();
        Ok(table.rows.get(&id).cloned())
    }

    fn update(
        &self,
        table_name: &str,
        id: RowId,
        values: HashMap<String, Value>,
    ) -> CoreResult<Row> {
        let table = self.table(table_name)?;
        let mut table = table.write();

        // Snapshot pre-mutation unique values first (no slot borrow held).
        let old_uniq: Vec<(String, Value)> = {
            let cur = table.rows.get(&id).ok_or_else(|| CoreError::RowNotFound(id.as_u64()))?;
            let cols = table.uniq_cols();
            cols.iter()
                .filter_map(|c| cur.values.get(c).map(|v| (c.clone(), v.clone())))
                .collect()
        };
        // Uniqueness first (before mutating): a conflicting update must not
        // partially apply.
        let cols = table.uniq_cols();
        table.check_uniq(&cols, &values, Some(id))?;

        // Mutate in a tight scope so the slot borrow ends before index work.
        // Copy-on-write: clones the row only if other `Arc` handles exist.
        let new_row: Row = {
            let slot = table.rows.get_mut(&id).expect("checked above");
            let row = Arc::make_mut(slot);
            for (key, value) in values {
                row.values.insert(key, value);
            }
            // Single owned clone for the return value (previously double).
            row.clone()
        };
        // Maintain indexes: drop stale keys, add current.
        for (c, v) in old_uniq {
            if new_row.values.get(&c) != Some(&v) {
                if let Some(idx) = table.uniq.get_mut(&c) {
                    if idx.get(&v) == Some(&id) {
                        idx.remove(&v);
                    }
                }
            }
        }
        let cur: Vec<(String, Value)> = cols
            .iter()
            .filter_map(|c| new_row.values.get(c).map(|v| (c.clone(), v.clone())))
            .collect();
        for (c, v) in cur {
            if !v.is_null() {
                table.uniq.entry(c).or_default().insert(v, id);
            }
        }
        Ok(new_row)
    }

    fn delete(&self, table_name: &str, id: RowId) -> CoreResult<bool> {
        Ok(self.take(table_name, id)?.is_some())
    }

    fn take(&self, table_name: &str, id: RowId) -> CoreResult<Option<Arc<Row>>> {
        let table = self.table(table_name)?;
        let mut table = table.write();
        let removed = table.rows.remove(&id);
        if let Some(ref row) = removed {
            let cols = table.uniq_cols();
            table.unindex_row(&cols, id, &row.values);
        }
        Ok(removed)
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
        let table = table.read();
        Ok(table.rows.values().cloned().collect())
    }

    fn schema(&self, table_name: &str) -> CoreResult<TableSchema> {
        let table = self.table(table_name)?;
        let table = table.read();
        Ok(table.schema.clone())
    }

    fn count(&self, table_name: &str) -> CoreResult<usize> {
        let table = self.table(table_name)?;
        let table = table.read();
        Ok(table.rows.len())
    }

    fn create_table(&self, schema: TableSchema) -> CoreResult<()> {
        let name = schema.name.clone();
        if self.tables.contains_key(&name) {
            return Err(CoreError::TableAlreadyExists(name));
        }
        self.tables
            .insert(name, Arc::new(RwLock::new(InMemoryTable::new(schema))));
        Ok(())
    }

    fn drop_table(&self, table_name: &str) -> CoreResult<()> {
        self.tables
            .remove(table_name)
            .ok_or_else(|| CoreError::TableNotFound(table_name.to_string()))?;
        Ok(())
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|r| r.key().clone()).collect()
    }

    fn lookup_by_unique(
        &self,
        table_name: &str,
        column: &str,
        value: &Value,
    ) -> CoreResult<Option<Arc<Row>>> {
        let table = self.table(table_name)?;
        let table = table.read();
        let is_uniq = table
            .schema
            .columns
            .iter()
            .any(|c| c.name == column && (c.primary_key || c.unique));
        if !is_uniq {
            return Err(CoreError::ConstraintViolation(format!(
                "lookup_by_unique: '{}' is not unique-indexed (no full scans on hot path)",
                column
            )));
        }
        if value.is_null() {
            return Ok(None);
        }
        let id = table.uniq.get(column).and_then(|idx| idx.get(value).copied());
        match id {
            Some(rid) => Ok(table.rows.get(&rid).cloned()),
            None => Ok(None),
        }
    }

    fn insert_preserving_id(&self, table_name: &str, row: Row) -> CoreResult<RowId> {
        let table = self.table(table_name)?;
        let mut table = table.write();
        let want = row.id;
        if table.rows.contains_key(&want) {
            return Err(CoreError::DuplicateKey(format!("row {}", want.as_u64())));
        }
        let cols = table.uniq_cols();
        table.check_uniq(&cols, &row.values, Some(want))?;
        // Bump allocator past restored ID (replay/snapshot must not collide
        // with future auto-ids — the old recovery bug reassigned all IDs).
        if want.as_u64() >= table.next_id {
            table.next_id = want.as_u64() + 1;
        }
        table.index_row(&cols, want, &row.values);
        table.rows.insert(want, Arc::new(row));
        Ok(want)
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
    fn test_unique_enforced_and_freed_on_delete() {
        let engine = InMemoryTableEngine::new();
        engine.create_table(test_schema()).unwrap();
        engine.insert("users", make_row(1, "Alice")).unwrap();
        // Same email (make_row derives email from name) must fail.
        let dup = engine.insert("users", make_row(2, "Alice"));
        assert!(matches!(dup, Err(CoreError::DuplicateKey(_))), "got {:?}", dup);
        // Different email ok.
        engine.insert("users", make_row(2, "Bob")).unwrap();
        // Update Bob onto Alice's email must fail without partial apply.
        let bob = engine.scan("users").unwrap().into_iter().find(|r| r.get("name") == Some(&Value::String("Bob".into()))).unwrap();
        let mut bad = HashMap::new();
        bad.insert("email".into(), Value::String("Alice@example.com".into()));
        assert!(engine.update("users", bob.id, bad).is_err());
        // Delete Alice frees the address.
        let alice = engine.scan("users").unwrap().into_iter().find(|r| r.get("name") == Some(&Value::String("Alice".into()))).unwrap();
        engine.delete("users", alice.id).unwrap();
        let mut reuse = HashMap::new();
        reuse.insert("email".into(), Value::String("Alice@example.com".into()));
        let bob2 = engine.scan("users").unwrap().into_iter().find(|r| r.get("name") == Some(&Value::String("Bob".into()))).unwrap();
        assert!(engine.update("users", bob2.id, reuse).is_ok());
    }

    #[test]
    fn test_concurrent_distinct_rows_and_unique_race() {
        use std::sync::Arc as StdArc;
        let engine = StdArc::new(InMemoryTableEngine::new());
        engine.create_table(test_schema()).unwrap();
        for i in 0..200 {
            engine.insert("users", make_row(i, &format!("U{}", i))).unwrap();
        }
        // 16 threads × updates (overlapping rows are fine: same-row
        // concurrent updates are all valid when no unique columns change).
        std::thread::scope(|s| {
            for t in 0..16 {
                let e = StdArc::clone(&engine);
                s.spawn(move || {
                    for k in 0..50 {
                        let rid = RowId::new(1 + ((t * 50 + k) % 200) as u64);
                        let mut u = HashMap::new();
                        u.insert("name".into(), Value::String(format!("T{}K{}", t, k)));
                        let _ = e.update("users", rid, u);
                    }
                });
            }
        });
        assert_eq!(engine.count("users").unwrap(), 200);
        // Concurrent duplicate-email inserts: exactly one wins.
        let results: Vec<_> = std::thread::scope(|s| {
            let mut hs = Vec::new();
            for t in 0..16 {
                let e = StdArc::clone(&engine);
                hs.push(s.spawn(move || {
                    let mut row = Row::new(RowId::new(0));
                    row.set("id", Value::Int64(1000 + t));
                    row.set("name", Value::String(format!("R{}", t)));
                    row.set("email", Value::String("race@example.com".into()));
                    e.insert("users", row).is_ok()
                }));
            }
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(results.iter().filter(|&&x| x).count(), 1, "exactly one duplicate claimant wins");
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
