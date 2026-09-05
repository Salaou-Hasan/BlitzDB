use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use crate::error::{TxError, TxResult};
use crate::transaction::{IsolationLevel, Transaction, WriteOp};
use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_types::id::{RowId, TransactionId};
use blitz_types::row::Row;

/// Number of commit-striping locks. Must be a power of two (used as a mask).
const STRIPE_COUNT: usize = 1024;

/// Lock stripe for a row key. Commits touching the same row share a
/// stripe and serialize; commits on disjoint rows proceed in parallel.
fn stripe_index(table: &str, id: RowId) -> usize {
    use std::collections::hash_map::DefaultHasher;
    let mut h = DefaultHasher::new();
    table.hash(&mut h);
    id.hash(&mut h);
    (h.finish() as usize) & (STRIPE_COUNT - 1)
}

/// Manages transactions and applies committed writes to the engine.
///
/// Concurrency design:
/// * The engine uses table-level locks, so single-table operations on
///   different tables proceed in parallel without manager involvement.
/// * Commits do NOT take a global lock. Each commit locks only the
///   stripes covering its Update/Delete rows, in sorted index order
///   (deadlock-free). Validation + apply + version stamping for those
///   rows is therefore atomic with respect to any overlapping commit,
///   while commits on disjoint rows run fully concurrently — even on
///   the same table.
/// * OCC validation: `begin` snapshots the global commit sequence;
///   `commit` rejects buffered Update/Delete ops targeting rows written
///   by a newer commit. Blind inserts never conflict, and read-only
///   transactions skip validation and take no locks at all.
pub struct TransactionManager {
    engine: InMemoryTableEngine,
    next_tx_id: AtomicU64,
    /// Striped commit locks, one per row-key hash bucket.
    commit_stripes: Box<[Mutex<()>]>,
    /// Last committed sequence number, bumped once per write commit.
    commit_seq: AtomicU64,
    /// Last-writer version per row: (table, row id) -> commit seq.
    versions: RwLock<HashMap<(String, RowId), u64>>,
}

impl TransactionManager {
    pub fn new(engine: InMemoryTableEngine) -> Self {
        Self {
            engine,
            next_tx_id: AtomicU64::new(1),
            commit_stripes: (0..STRIPE_COUNT)
                .map(|_| Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            commit_seq: AtomicU64::new(0),
            versions: RwLock::new(HashMap::new()),
        }
    }

    /// Begin a new transaction, snapshotting the current commit sequence.
    pub fn begin(&self, isolation: IsolationLevel) -> Transaction {
        let id = TransactionId::new(self.next_tx_id.fetch_add(1, Ordering::SeqCst));
        let mut tx = Transaction::new(id, isolation);
        tx.start_seq = self.commit_seq.load(Ordering::Acquire);
        tx
    }

    /// Transactional read: returns the row and records its observed
    /// version in the transaction's read set for commit-time validation.
    ///
    /// The version is read BEFORE the row, so a commit landing in
    /// between can only cause a (safe) spurious conflict, never a
    /// lost update.
    pub fn get(
        &self,
        tx: &mut Transaction,
        table: &str,
        id: RowId,
    ) -> TxResult<Option<Row>> {
        if !tx.is_active() {
            return Err(TxError::Internal("transaction is not active".into()));
        }
        let version = self
            .versions
            .read()
            .map_err(|e| TxError::Internal(format!("failed to acquire version lock: {}", e)))?
            .get(&(table.to_string(), id))
            .copied()
            .unwrap_or(0);
        let row = self.engine.get(table, id)?;
        tx.record_read(table, id, version);
        Ok(row)
    }

    /// Commit a transaction, applying all buffered writes.
    ///
    /// Returns [`TxError::Conflict`] when a buffered Update/Delete targets
    /// a row written by a commit newer than this transaction's snapshot.
    /// The transaction is rolled back before the error is returned.
    pub fn commit(&self, tx: &mut Transaction) -> TxResult<()> {
        if !tx.is_active() {
            return Err(TxError::Internal("transaction is not active".into()));
        }

        // Fast path: read-only transactions can never conflict and
        // need no locks at all.
        if !tx.has_writes() {
            tx.commit()?;
            return Ok(());
        }

        // Collect the stripes covering this commit's Update/Delete rows,
        // in sorted order for deadlock-free acquisition. Inserts need no
        // stripes: a row id cannot be known (hence touched) by another
        // transaction before the insert that creates it commits.
        let mut stripes: Vec<usize> = tx
            .writes
            .iter()
            .filter_map(|write| match write {
                WriteOp::Update { table, id, .. } | WriteOp::Delete { table, id } => {
                    Some(stripe_index(table, *id))
                }
                WriteOp::Insert { .. } => None,
            })
            .collect();
        // Serializable (and repeatable-read) transactions also lock the
        // stripes of rows they read, so a concurrent writer to a read row
        // serializes against this commit instead of slipping between
        // validation and apply.
        if matches!(
            tx.isolation,
            IsolationLevel::Serializable | IsolationLevel::RepeatableRead
        ) {
            for (table, id, _) in tx.read_set.iter() {
                stripes.push(stripe_index(table, *id));
            }
        }
        stripes.sort_unstable();
        stripes.dedup();

        // Hold every stripe for the whole validate + apply + stamp
        // critical section. Overlapping commits serialize here;
        // disjoint ones never meet.
        let _guards: Vec<_> = stripes
            .iter()
            .map(|&s| {
                self.commit_stripes[s].lock().map_err(|e| {
                    TxError::Internal(format!("failed to acquire commit stripe: {}", e))
                })
            })
            .collect::<TxResult<_>>()?;

        // Validate write-write conflicts before applying anything, so a
        // conflicting transaction leaves the engine untouched.
        {
            let versions = self.versions.read().map_err(|e| {
                TxError::Internal(format!("failed to acquire version lock: {}", e))
            })?;
            // Validate the read set for repeatable-read isolation and
            // above: any row that changed since it was read aborts the
            // commit. Lower isolation levels skip this (fewer retries).
            if matches!(
                tx.isolation,
                IsolationLevel::Serializable | IsolationLevel::RepeatableRead
            ) {
                let stale = tx
                    .read_set
                    .iter()
                    .find(|(table, id, read_version)| {
                        versions.get(&(table.clone(), *id)).copied().unwrap_or(0)
                            != *read_version
                    })
                    .map(|(table, id, _)| (table.clone(), *id));
                if let Some((table, id)) = stale {
                    let _ = tx.rollback();
                    return Err(TxError::Conflict(format!(
                        "read-write conflict on {}:{} in transaction {}",
                        table, id, tx.id
                    )));
                }
            }
            for write in tx.writes.iter() {
                let conflict = match write {
                    // Blind inserts never conflict.
                    WriteOp::Insert { .. } => false,
                    WriteOp::Update { table, id, .. }
                    | WriteOp::Delete { table, id } => versions
                        .get(&(table.clone(), *id))
                        .is_some_and(|v| *v > tx.start_seq),
                };
                if conflict {
                    let _ = tx.rollback();
                    return Err(TxError::Conflict(format!(
                        "write-write conflict in transaction {}",
                        tx.id
                    )));
                }
            }
        }

        // Claim one sequence number for the whole commit so all of its
        // writes share a single version stamp.
        let seq = self.commit_seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Apply all buffered writes, collecting touched rows.
        let mut touched = Vec::with_capacity(tx.writes.len());
        for write in tx.writes.drain(..) {
            match write {
                WriteOp::Insert { table, row } => {
                    let id = self.engine.insert(&table, row)?;
                    touched.push((table, id));
                }
                WriteOp::Update { table, id, values } => {
                    self.engine.update(&table, id, values)?;
                    touched.push((table, id));
                }
                WriteOp::Delete { table, id } => {
                    self.engine.delete(&table, id)?;
                    touched.push((table, id));
                }
            }
        }

        {
            let mut versions = self.versions.write().map_err(|e| {
                TxError::Internal(format!("failed to acquire version lock: {}", e))
            })?;
            for key in touched {
                versions.insert(key, seq);
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

    /// Get a reference to the underlying engine.
    ///
    /// The engine is internally synchronized; no outer lock is needed.
    pub fn engine(&self) -> &InMemoryTableEngine {
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
    use blitz_types::row::Row;
    use blitz_types::schema::TableSchema;
    use blitz_types::value::Value;
    use std::collections::HashMap;

    fn setup_manager() -> TransactionManager {
        let engine = InMemoryTableEngine::new();
        let schema = TableSchema::new("users")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("name", ColumnType::String));
        engine.create_table(schema).unwrap();
        TransactionManager::new(engine)
    }

    fn setup_counter() -> TransactionManager {
        let engine = InMemoryTableEngine::new();
        let schema = TableSchema::new("counters")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("count", ColumnType::Int64));
        engine.create_table(schema).unwrap();
        TransactionManager::new(engine)
    }

    fn insert_user(tx: &mut Transaction, id: i64, name: &str) {
        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(id));
        row.set("name", Value::String(name.into()));
        tx.insert("users", row).unwrap();
    }

    fn rename(tx: &mut Transaction, id: RowId, name: &str) {
        let mut values = HashMap::new();
        values.insert("name".into(), Value::String(name.into()));
        tx.update("users", id, values).unwrap();
    }

    /// Read-modify-write the counter with retry. Returns the number of
    /// conflicts hit. A conflicted attempt is fully retried (fresh read),
    /// so no increment is ever lost.
    fn increment_with_retry(manager: &TransactionManager, id: RowId) -> u64 {
        let mut conflicts = 0;
        loop {
            let mut tx = manager.begin(IsolationLevel::Serializable);
            let cur = manager
                .get(&mut tx, "counters", id)
                .unwrap()
                .unwrap();
            let n = cur.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            let mut values = HashMap::new();
            values.insert("count".into(), Value::Int64(n + 1));
            tx.update("counters", id, values).unwrap();
            match manager.commit(&mut tx) {
                Ok(()) => return conflicts,
                Err(TxError::Conflict(_)) => conflicts += 1,
                Err(e) => panic!("unexpected commit error: {}", e),
            }
        }
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

        let row = manager.engine().get("users", RowId::new(1)).unwrap();
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

        assert_eq!(manager.engine().count("users").unwrap(), 0);
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

        assert_eq!(manager.engine().count("users").unwrap(), 5);
    }

    #[test]
    fn test_read_only_commit_needs_no_writes() {
        let manager = setup_manager();
        let mut tx = manager.begin(IsolationLevel::Serializable);
        assert!(!tx.has_writes());
        manager.commit(&mut tx).unwrap();
        assert!(tx.is_committed());
    }

    #[test]
    fn test_write_write_conflict_on_same_row() {
        let manager = setup_manager();

        let mut seed = manager.begin(IsolationLevel::Serializable);
        insert_user(&mut seed, 1, "Alice");
        manager.commit(&mut seed).unwrap();

        let mut tx_a = manager.begin(IsolationLevel::Serializable);
        let mut tx_b = manager.begin(IsolationLevel::Serializable);
        rename(&mut tx_a, RowId::new(1), "A");
        rename(&mut tx_b, RowId::new(1), "B");

        manager.commit(&mut tx_b).unwrap();

        let err = manager.commit(&mut tx_a).unwrap_err();
        assert!(matches!(err, TxError::Conflict(_)));
        assert!(tx_a.is_rolled_back());

        // Winner's write survived.
        let row = manager
            .engine()
            .get("users", RowId::new(1))
            .unwrap()
            .unwrap();
        assert_eq!(row.get("name"), Some(&Value::String("B".into())));
    }

    #[test]
    fn test_disjoint_writes_do_not_conflict() {
        let manager = setup_manager();

        let mut seed = manager.begin(IsolationLevel::Serializable);
        insert_user(&mut seed, 1, "Alice");
        insert_user(&mut seed, 2, "Bob");
        manager.commit(&mut seed).unwrap();

        let mut tx_a = manager.begin(IsolationLevel::Serializable);
        let mut tx_b = manager.begin(IsolationLevel::Serializable);
        rename(&mut tx_a, RowId::new(1), "A");
        rename(&mut tx_b, RowId::new(2), "B");

        manager.commit(&mut tx_a).unwrap();
        manager.commit(&mut tx_b).unwrap();

        let a = manager
            .engine()
            .get("users", RowId::new(1))
            .unwrap()
            .unwrap();
        let b = manager
            .engine()
            .get("users", RowId::new(2))
            .unwrap()
            .unwrap();
        assert_eq!(a.get("name"), Some(&Value::String("A".into())));
        assert_eq!(b.get("name"), Some(&Value::String("B".into())));
    }

    #[test]
    fn test_concurrent_inserts_do_not_conflict() {
        let manager = setup_manager();

        let mut tx_a = manager.begin(IsolationLevel::Serializable);
        let mut tx_b = manager.begin(IsolationLevel::Serializable);
        insert_user(&mut tx_a, 1, "Alice");
        insert_user(&mut tx_b, 2, "Bob");

        manager.commit(&mut tx_a).unwrap();
        manager.commit(&mut tx_b).unwrap();

        assert_eq!(manager.engine().count("users").unwrap(), 2);
    }

    #[test]
    fn test_delete_after_concurrent_update_conflicts() {
        let manager = setup_manager();

        let mut seed = manager.begin(IsolationLevel::Serializable);
        insert_user(&mut seed, 1, "Alice");
        manager.commit(&mut seed).unwrap();

        let mut tx_del = manager.begin(IsolationLevel::Serializable);
        let mut tx_upd = manager.begin(IsolationLevel::Serializable);
        tx_del.delete("users", RowId::new(1)).unwrap();
        rename(&mut tx_upd, RowId::new(1), "B");

        manager.commit(&mut tx_upd).unwrap();
        let err = manager.commit(&mut tx_del).unwrap_err();
        assert!(matches!(err, TxError::Conflict(_)));
    }

    #[test]
    fn test_concurrent_commits_on_many_threads() {
        use std::sync::Arc;

        let manager = Arc::new(setup_manager());

        let mut handles = Vec::new();
        for t in 0..8 {
            let manager = Arc::clone(&manager);
            handles.push(std::thread::spawn(move || {
                for i in 0..50 {
                    let mut tx = manager.begin(IsolationLevel::Serializable);
                    insert_user(&mut tx, t * 1000 + i, "x");
                    // Blind inserts never conflict, so no retry needed.
                    manager.commit(&mut tx).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(manager.engine().count("users").unwrap(), 400);
    }

    #[test]
    fn test_stale_read_then_write_conflicts() {
        let manager = setup_counter();

        // Seed counter row id 1 at 0.
        let mut seed = manager.begin(IsolationLevel::Serializable);
        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("count", Value::Int64(0));
        seed.insert("counters", row).unwrap();
        manager.commit(&mut seed).unwrap();

        // Transactional read observes version 1 and value 0.
        let mut tx = manager.begin(IsolationLevel::Serializable);
        let seen = manager.get(&mut tx, "counters", RowId::new(1)).unwrap().unwrap();
        assert_eq!(seen.get("count"), Some(&Value::Int64(0)));

        // A concurrent commit bumps the row to 5 (version 2).
        let mut other = manager.begin(IsolationLevel::Serializable);
        let mut values = HashMap::new();
        values.insert("count".into(), Value::Int64(5));
        other.update("counters", RowId::new(1), values).unwrap();
        manager.commit(&mut other).unwrap();

        // Our buffered write derives from the stale read: must conflict.
        let n = seen.get("count").and_then(|v| v.as_i64()).unwrap();
        let mut values = HashMap::new();
        values.insert("count".into(), Value::Int64(n + 1));
        tx.update("counters", RowId::new(1), values).unwrap();
        let err = manager.commit(&mut tx).unwrap_err();
        assert!(matches!(err, TxError::Conflict(_)));
    }

    #[test]
    fn test_parallel_same_row_increments_lose_nothing() {
        use std::sync::Arc;

        let manager = Arc::new(setup_counter());

        // Seed counter row id 1 at 0.
        let mut seed = manager.begin(IsolationLevel::Serializable);
        let mut row = Row::new(RowId::new(0));
        row.set("id", Value::Int64(1));
        row.set("count", Value::Int64(0));
        seed.insert("counters", row).unwrap();
        manager.commit(&mut seed).unwrap();

        // 8 threads hammer the SAME row. Overlapping attempts conflict
        // and retry; every increment must land exactly once.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let manager = Arc::clone(&manager);
            handles.push(std::thread::spawn(move || {
                let mut conflicts = 0u64;
                for _ in 0..20 {
                    conflicts += increment_with_retry(&manager, RowId::new(1));
                }
                conflicts
            }));
        }
        let mut total_conflicts = 0u64;
        for h in handles {
            total_conflicts += h.join().unwrap();
        }

        let row = manager
            .engine()
            .get("counters", RowId::new(1))
            .unwrap()
            .unwrap();
        // No lost updates despite full parallelism: 8 * 20 increments.
        assert_eq!(row.get("count"), Some(&Value::Int64(160)));
        println!("same-row contention: {} conflicts over 160 commits", total_conflicts);
    }

    #[test]
    fn test_parallel_disjoint_rows_never_conflict() {
        use std::sync::Arc;

        let manager = Arc::new(setup_counter());

        // Seed 8 independent counter rows.
        let mut seed = manager.begin(IsolationLevel::Serializable);
        for i in 1..=8i64 {
            let mut row = Row::new(RowId::new(0));
            row.set("id", Value::Int64(i));
            row.set("count", Value::Int64(0));
            seed.insert("counters", row).unwrap();
        }
        manager.commit(&mut seed).unwrap();

        // Each thread owns one row: row-precise validation means zero
        // conflicts even though all commits run concurrently.
        let mut handles = Vec::new();
        for t in 1..=8u64 {
            let manager = Arc::clone(&manager);
            handles.push(std::thread::spawn(move || {
                let mut conflicts = 0u64;
                for _ in 0..20 {
                    conflicts += increment_with_retry(&manager, RowId::new(t));
                }
                conflicts
            }));
        }
        let mut total_conflicts = 0u64;
        for h in handles {
            total_conflicts += h.join().unwrap();
        }

        assert_eq!(total_conflicts, 0);
        for t in 1..=8u64 {
            let row = manager
                .engine()
                .get("counters", RowId::new(t))
                .unwrap()
                .unwrap();
            assert_eq!(row.get("count"), Some(&Value::Int64(20)));
        }
    }
}
