use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use crate::error::{TxError, TxResult};
use crate::transaction::{IsolationLevel, Transaction, WriteOp};
use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_types::id::TransactionId;
use blitz_types::row::Row;

/// Manages transactions and applies committed writes to the engine.
pub struct TransactionManager {
    engine: RwLock<InMemoryTableEngine>,
    next_tx_id: AtomicU64,
}

impl TransactionManager {
    pub fn new(engine: InMemoryTableEngine) -> Self {
        Self {
            engine: RwLock::new(engine),
            next_tx_id: AtomicU64::new(1),
        }
    }

    /// Begin a new transaction.
    pub fn begin(&self, isolation: IsolationLevel) -> Transaction {
        let id = TransactionId::new(self.next_tx_id.fetch_add(1, Ordering::SeqCst));
        Transaction::new(id, isolation)
    }

    /// Commit a transaction, applying all buffered writes.
    pub fn commit(&self, tx: &mut Transaction) -> TxResult<()> {
        if !tx.is_active() {
            return Err(TxError::Internal("transaction is not active".into()));
        }

        let mut engine = self.engine.write().map_err(|e| {
            TxError::Internal(format!("failed to acquire write lock: {}", e))
        })?;

        // Apply all buffered writes
        for write in tx.writes.drain(..) {
            match write {
                WriteOp::Insert { table, row } => {
                    engine.insert(&table, row)?;
                }
                WriteOp::Update { table, id, values } => {
                    engine.update(&table, id, values)?;
                }
                WriteOp::Delete { table, id } => {
                    engine.delete(&table, id)?;
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// Rollback a transaction, discarding all buffered writes.
    pub fn rollback(&self, tx: &mut Transaction) -> TxResult<()> {
        tx.rollback()?;
        Ok(())
    }

    /// Get a reference to the underlying engine (read-only).
    pub fn engine(&self) -> &RwLock<InMemoryTableEngine> {
        &self.engine
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new(InMemoryTableEngine::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::column::{ColumnDef, ColumnType};
    use blitz_types::schema::TableSchema;
    use blitz_types::id::RowId;
    use blitz_types::value::Value;

    fn setup_manager() -> TransactionManager {
        let mut engine = InMemoryTableEngine::new();
        let schema = TableSchema::new("users")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("name", ColumnType::String));
        engine.create_table(schema).unwrap();
        TransactionManager::new(engine)
    }

    #[test]
    fn test_begin_and_commit() {
        let manager = setup_manager();
        let mut tx = manager.begin(IsolationLevel::Serializable);

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));

        tx.insert("users", row).unwrap();
        manager.commit(&mut tx).unwrap();

        let engine = manager.engine().read().unwrap();
        let row = engine.get("users", RowId::new(1)).unwrap();
        assert!(row.is_some());
    }

    #[test]
    fn test_begin_and_rollback() {
        let manager = setup_manager();
        let mut tx = manager.begin(IsolationLevel::Serializable);

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));

        tx.insert("users", row).unwrap();
        manager.rollback(&mut tx).unwrap();

        let engine = manager.engine().read().unwrap();
        assert_eq!(engine.count("users").unwrap(), 0);
    }

    #[test]
    fn test_multiple_operations_in_transaction() {
        let manager = setup_manager();
        let mut tx = manager.begin(IsolationLevel::Serializable);

        for i in 1..=5 {
            let mut row = Row::new(RowId::new(0));
            row.set("id", Value::Int64(i));
            row.set("name", Value::String(format!("User {}", i)));
            tx.insert("users", row).unwrap();
        }

        assert_eq!(tx.write_count(), 5);
        manager.commit(&mut tx).unwrap();

        let engine = manager.engine().read().unwrap();
        assert_eq!(engine.count("users").unwrap(), 5);
    }
}
