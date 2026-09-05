use std::collections::HashMap;

use crate::error::{TxError, TxResult};
use blitz_types::id::{RowId, TransactionId};
use blitz_types::row::Row;
use blitz_types::value::Value;

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read uncommitted.
    ReadUncommitted,
    /// Read committed.
    ReadCommitted,
    /// Repeatable read.
    RepeatableRead,
    /// Serializable.
    Serializable,
}

/// The state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    Active,
    Committed,
    RolledBack,
}

/// A write operation buffered within a transaction.
#[derive(Debug, Clone)]
pub enum WriteOp {
    Insert { table: String, row: Row },
    Update { table: String, id: RowId, values: HashMap<String, Value> },
    Delete { table: String, id: RowId },
}

/// A database transaction.
#[derive(Debug)]
pub struct Transaction {
    pub id: TransactionId,
    pub state: TxState,
    pub isolation: IsolationLevel,
    /// Buffered write operations.
    pub writes: Vec<WriteOp>,
    /// Read set for conflict detection: (table, row, version observed).
    pub read_set: Vec<(String, RowId, u64)>,
    /// Commit sequence observed when the transaction began.
    /// A buffered Update/Delete conflicts if the row was written by a newer commit.
    pub start_seq: u64,
}
impl Transaction {
    pub fn new(id: TransactionId, isolation: IsolationLevel) -> Self {
        Self {
            id,
            state: TxState::Active,
            isolation,
            writes: Vec::new(),
            read_set: Vec::new(),
            start_seq: 0,
        }
    }

    pub fn is_active(&self) -> bool {
        self.state == TxState::Active
    }

    pub fn is_committed(&self) -> bool {
        self.state == TxState::Committed
    }

    pub fn is_rolled_back(&self) -> bool {
        self.state == TxState::RolledBack
    }

    /// Buffer an insert operation.
    pub fn insert(&mut self, table: impl Into<String>, row: Row) -> TxResult<()> {
        if !self.is_active() {
            return Err(TxError::Internal("transaction is not active".into()));
        }
        self.writes.push(WriteOp::Insert {
            table: table.into(),
            row,
        });
        Ok(())
    }

    /// Buffer an update operation.
    pub fn update(
        &mut self,
        table: impl Into<String>,
        id: RowId,
        values: HashMap<String, Value>,
    ) -> TxResult<()> {
        if !self.is_active() {
            return Err(TxError::Internal("transaction is not active".into()));
        }
        self.writes.push(WriteOp::Update {
            table: table.into(),
            id,
            values,
        });
        Ok(())
    }

    /// Buffer a delete operation.
    pub fn delete(&mut self, table: impl Into<String>, id: RowId) -> TxResult<()> {
        if !self.is_active() {
            return Err(TxError::Internal("transaction is not active".into()));
        }
        self.writes.push(WriteOp::Delete {
            table: table.into(),
            id,
        });
        Ok(())
    }

    /// Record a read for conflict detection, with the version observed.
    pub fn record_read(&mut self, table: impl Into<String>, id: RowId, version: u64) {
        self.read_set.push((table.into(), id, version));
    }

    /// Mark the transaction as committed.
    pub fn commit(&mut self) -> TxResult<()> {
        if !self.is_active() {
            return Err(TxError::Internal("transaction is not active".into()));
        }
        self.state = TxState::Committed;
        Ok(())
    }

    /// Mark the transaction as rolled back.
    pub fn rollback(&mut self) -> TxResult<()> {
        if !self.is_active() {
            return Err(TxError::Internal("transaction is not active".into()));
        }
        self.state = TxState::RolledBack;
        self.writes.clear();
        Ok(())
    }

    /// Get the number of buffered write operations.
    pub fn write_count(&self) -> usize {
        self.writes.len()
    }

    /// Check if the transaction has any writes.
    pub fn has_writes(&self) -> bool {
        !self.writes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::column::{ColumnDef, ColumnType};

    #[test]
    fn test_transaction_lifecycle() {
        let mut tx = Transaction::new(TransactionId::new(1), IsolationLevel::Serializable);
        assert!(tx.is_active());

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));

        tx.insert("users", row).unwrap();
        assert!(tx.has_writes());
        assert_eq!(tx.write_count(), 1);

        tx.commit().unwrap();
        assert!(tx.is_committed());
        assert!(!tx.is_active());
    }

    #[test]
    fn test_transaction_rollback() {
        let mut tx = Transaction::new(TransactionId::new(1), IsolationLevel::Serializable);

        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("name", Value::String("Alice".into()));

        tx.insert("users", row).unwrap();
        tx.rollback().unwrap();
        assert!(tx.is_rolled_back());
        assert!(!tx.has_writes());
    }

    #[test]
    fn test_cannot_write_after_commit() {
        let mut tx = Transaction::new(TransactionId::new(1), IsolationLevel::Serializable);
        tx.commit().unwrap();

        let row = Row::new(RowId::new(0));
        assert!(tx.insert("users", row).is_err());
    }
}
