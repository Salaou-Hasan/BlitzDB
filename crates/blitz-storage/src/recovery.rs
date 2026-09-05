use std::collections::HashMap;
use std::path::PathBuf;

use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_types::column::{ColumnDef, ColumnType};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;
use blitz_wal::log::{EntryType, WalEntry, WriteAheadLog};
use blitz_snapshot::snapshot::{Snapshot, SnapshotManager};

/// Errors that can occur during recovery.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("wal error: {0}")]
    WalError(String),

    #[error("snapshot error: {0}")]
    SnapshotError(String),

    #[error("deserialization error: {0}")]
    DeserializationError(String),

    #[error("engine error: {0}")]
    EngineError(String),

    #[error("corruption: {0}")]
    Corruption(String),
}

/// Result type for recovery operations.
pub type RecoveryResult<T> = Result<T, RecoveryError>;

/// Manages crash recovery by combining snapshots and WAL replay.
pub struct RecoveryManager {
    data_dir: PathBuf,
    snapshot_manager: SnapshotManager,
}

impl RecoveryManager {
    /// Create a new recovery manager.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let snapshot_dir = data_dir.join("snapshots");
        let snapshot_manager = SnapshotManager::new(snapshot_dir);

        Self {
            data_dir,
            snapshot_manager,
        }
    }

    /// Recover the database from snapshot + WAL.
    pub fn recover(&self) -> RecoveryResult<InMemoryTableEngine> {
        tracing::info!("Starting crash recovery");

        // Step 1: Load latest snapshot if available
        let (mut engine, wal_sequence) = match self.snapshot_manager.load_latest() {
            Ok(Some((snapshot, wal_seq))) => {
                tracing::info!("Loading snapshot with WAL sequence {}", wal_seq);
                let engine = self.restore_snapshot(snapshot)?;
                (engine, wal_seq)
            }
            Ok(None) => {
                tracing::info!("No snapshot found, starting fresh");
                (InMemoryTableEngine::new(), 0)
            }
            Err(e) => {
                tracing::warn!("Failed to load snapshot: {}, starting fresh", e);
                (InMemoryTableEngine::new(), 0)
            }
        };

        // Step 2: Replay WAL entries after the snapshot
        let wal_path = self.wal_path();
        if wal_path.exists() {
            tracing::info!("Replaying WAL from sequence {}", wal_sequence);
            self.replay_wal(&mut engine, wal_sequence)?;
        }

        tracing::info!("Crash recovery complete");
        Ok(engine)
    }

    /// Restore engine state from a snapshot.
    fn restore_snapshot(&self, snapshot: Snapshot) -> RecoveryResult<InMemoryTableEngine> {
        let engine = InMemoryTableEngine::new();

        for table_data in &snapshot.tables {
            // Create table
            let schema = self.infer_schema(&table_data.name, &table_data.rows);
            engine.create_table(schema)
                .map_err(|e| RecoveryError::EngineError(e.to_string()))?;

            // Insert rows
            for row_data in &table_data.rows {
                let mut row = Row::new(RowId::new(row_data.id));
                // Add the id column explicitly
                row.set("id", Value::Int64(row_data.id as i64));
                for (col_name, json_val) in &row_data.values {
                    if col_name == "id" {
                        continue; // Already set
                    }
                    let value = self.json_to_value(json_val);
                    row.set(col_name.clone(), value);
                }
                engine.insert(&table_data.name, row)
                    .map_err(|e| RecoveryError::EngineError(e.to_string()))?;
            }
        }

        Ok(engine)
    }

    /// Replay WAL entries after the given sequence number.
    fn replay_wal(
        &self,
        engine: &mut InMemoryTableEngine,
        after_sequence: u64,
    ) -> RecoveryResult<()> {
        let wal_path = self.wal_path();
        let mut wal = WriteAheadLog::open(&wal_path)
            .map_err(|e| RecoveryError::WalError(e.to_string()))?;

        let entries = wal.read_all()
            .map_err(|e| RecoveryError::WalError(e.to_string()))?;

        // Track committed transactions
        let mut committed_txs: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut pending_ops: Vec<(u64, WalEntry)> = Vec::new();

        for entry in &entries {
            if entry.sequence <= after_sequence {
                continue;
            }

            match entry.entry_type {
                EntryType::Commit => {
                    committed_txs.insert(entry.row_id);
                    // Apply all pending ops for this transaction
                    let ops: Vec<_> = pending_ops.drain(..).collect();
                    for (_, op) in ops {
                        self.apply_entry(engine, &op)?;
                    }
                }
                EntryType::Rollback => {
                    // Discard pending ops
                    pending_ops.clear();
                }
                _ => {
                    pending_ops.push((entry.row_id, entry.clone()));
                }
            }
        }

        // Apply any remaining committed operations (non-transactional)
        for (_, op) in pending_ops {
            self.apply_entry(engine, &op)?;
        }

        Ok(())
    }

    /// Apply a single WAL entry to the engine.
    fn apply_entry(
        &self,
        engine: &mut InMemoryTableEngine,
        entry: &WalEntry,
    ) -> RecoveryResult<()> {
        match entry.entry_type {
            EntryType::Insert => {
                let row: Row = bincode::deserialize(&entry.data)
                    .map_err(|e| RecoveryError::DeserializationError(e.to_string()))?;
                engine.insert(&entry.table, row)
                    .map_err(|e| RecoveryError::EngineError(e.to_string()))?;
            }
            EntryType::Update => {
                let updates: HashMap<String, Value> = bincode::deserialize(&entry.data)
                    .map_err(|e| RecoveryError::DeserializationError(e.to_string()))?;
                engine.update(&entry.table, RowId::new(entry.row_id), updates)
                    .map_err(|e| RecoveryError::EngineError(e.to_string()))?;
            }
            EntryType::Delete => {
                engine.delete(&entry.table, RowId::new(entry.row_id))
                    .map_err(|e| RecoveryError::EngineError(e.to_string()))?;
            }
            EntryType::Commit | EntryType::Rollback => {}
        }
        Ok(())
    }

    /// Infer a schema from snapshot data.
    fn infer_schema(&self, name: &str, rows: &[blitz_snapshot::snapshot::SnapshotRow]) -> TableSchema {
        let mut schema = TableSchema::new(name);

        // Add a default primary key column
        schema = schema.with_column(ColumnDef::new("id", ColumnType::Int64).primary_key());

        // Infer columns from first row
        if let Some(first_row) = rows.first() {
            for (col_name, json_val) in &first_row.values {
                if col_name == "id" {
                    continue; // Already added
                }
                let col_type = self.json_to_column_type(json_val);
                schema = schema.with_column(ColumnDef::new(col_name.clone(), col_type));
            }
        }

        schema
    }

    /// Convert a JSON value to a BlitzDB Value.
    fn json_to_value(&self, json: &serde_json::Value) -> Value {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Boolean(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int64(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float64(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s.clone()),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.iter().map(|v| self.json_to_value(v)).collect())
            }
            serde_json::Value::Object(obj) => {
                Value::Json(serde_json::Value::Object(obj.clone()))
            }
        }
    }

    /// Infer column type from JSON value.
    fn json_to_column_type(&self, json: &serde_json::Value) -> ColumnType {
        match json {
            serde_json::Value::Null => ColumnType::String,
            serde_json::Value::Bool(_) => ColumnType::Boolean,
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    ColumnType::Int64
                } else {
                    ColumnType::Float64
                }
            }
            serde_json::Value::String(_) => ColumnType::String,
            serde_json::Value::Array(_) => ColumnType::Array,
            serde_json::Value::Object(_) => ColumnType::Json,
        }
    }

    /// Get the path to the WAL file.
    fn wal_path(&self) -> PathBuf {
        self.data_dir.join("blitz.wal")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_recovery_empty() {
        let dir = tempdir().unwrap();
        let manager = RecoveryManager::new(dir.path());
        let engine = manager.recover().unwrap();
        assert!(!engine.table_exists("users"));
    }

    #[test]
    fn test_recovery_with_snapshot() {
        let dir = tempdir().unwrap();
        let manager = RecoveryManager::new(dir.path());

        // Create a snapshot
        let snapshot_dir = dir.path().join("snapshots");
        let snap_manager = SnapshotManager::new(&snapshot_dir);

        let tables = vec![
            (
                "users".to_string(),
                vec![
                    (
                        1u64,
                        vec![
                            ("name".to_string(), serde_json::json!("Alice")),
                            ("email".to_string(), serde_json::json!("alice@example.com")),
                        ],
                    ),
                ],
            ),
        ];

        snap_manager.create_snapshot(&tables, &[("users".to_string(), 2)], 0).unwrap();

        // Recover
        let engine = manager.recover().unwrap();
        assert!(engine.table_exists("users"));
        assert_eq!(engine.count("users").unwrap(), 1);
    }
}
