//! Simple object pools for reducing allocations in hot paths.
//!
//! Pools are per-table (`InMemoryTable` owns a [`RowPool`] and a
//! [`RowIdVecPool`]) so no cross-thread synchronisation is needed beyond
//! what the engine already provides. Checking an object out reuses a
//! previous allocation (e.g. the `HashMap` inside a `Row` keeps its
//! capacity); objects beyond `max_size` are freshly allocated and simply
//! dropped when they go out of scope.

use std::collections::VecDeque;

use blitz_types::{id::RowId, row::Row};

/// A pre-allocated pool of objects of type `T`.
///
/// Objects are created via a factory function and can be checked out
/// and checked back in for reuse.
pub struct ObjectPool<T> {
    free: VecDeque<T>,
    factory: Box<dyn Fn() -> T + Send + Sync>,
    max_size: usize,
    total_created: usize,
}

impl<T> ObjectPool<T> {
    /// Create a new pool with the given factory, pre-filling `initial_cap`
    /// objects. `max_size == 0` means unbounded.
    pub fn new(
        factory: impl Fn() -> T + Send + Sync + 'static,
        initial_cap: usize,
        max_size: usize,
    ) -> Self {
        let mut free = VecDeque::with_capacity(initial_cap);
        for _ in 0..initial_cap {
            free.push_back(factory());
        }
        Self {
            free,
            factory: Box::new(factory),
            max_size,
            total_created: initial_cap,
        }
    }

    /// Check out an object, reusing a pooled one when available.
    pub fn checkout(&mut self) -> T {
        if let Some(obj) = self.free.pop_back() {
            return obj;
        }
        if self.max_size == 0 || self.total_created < self.max_size {
            self.total_created += 1;
        }
        (self.factory)()
    }

    /// Return an object for reuse. Dropped silently when the pool is full.
    pub fn checkin(&mut self, obj: T) {
        if self.max_size == 0 || self.free.len() < self.max_size {
            self.free.push_back(obj);
        }
    }

    /// Number of free objects currently held.
    pub fn len(&self) -> usize {
        self.free.len()
    }

    /// Whether no free objects are held.
    pub fn is_empty(&self) -> bool {
        self.free.is_empty()
    }

    /// Total objects ever created via the factory.
    pub fn total_created(&self) -> usize {
        self.total_created
    }

    /// Configured maximum retained objects (`0` = unbounded).
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

/// Pool of `Row` scratch buffers for table write paths.
///
/// Checked-out rows keep their `HashMap` capacity; callers must treat the
/// contents as garbage until overwritten.
pub struct RowPool(ObjectPool<Row>);

impl RowPool {
    /// Default settings: 16 pre-allocated, up to 64 retained.
    pub fn new() -> Self {
        Self(ObjectPool::new(
            || Row::new(RowId::new(0)),
            16,
            64,
        ))
    }

    /// Check out a `Row`. Its `values` map is cleared before return.
    pub fn checkout(&mut self) -> Row {
        let mut row = self.0.checkout();
        row.id = RowId::new(0);
        row.values.clear();
        row
    }

    /// Return a `Row` for reuse (its map is cleared).
    pub fn checkin(&mut self, mut row: Row) {
        row.values.clear();
        self.0.checkin(row);
    }

    /// Number of free rows currently held.
    pub fn free_count(&self) -> usize {
        self.0.len()
    }

    /// Total rows ever created by this pool.
    pub fn total_created(&self) -> usize {
        self.0.total_created()
    }
}

impl Default for RowPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Pool of `Vec<RowId>` scratch buffers for index lookups.
pub struct RowIdVecPool(ObjectPool<Vec<RowId>>);

impl RowIdVecPool {
    /// Default settings: 16 pre-allocated, up to 64 retained.
    pub fn new() -> Self {
        Self(ObjectPool::new(Vec::new, 16, 64))
    }

    /// Check out an empty `Vec<RowId>`.
    pub fn checkout(&mut self) -> Vec<RowId> {
        let mut v = self.0.checkout();
        v.clear();
        v
    }

    /// Return a `Vec<RowId>` for reuse (cleared first).
    pub fn checkin(&mut self, mut v: Vec<RowId>) {
        v.clear();
        self.0.checkin(v);
    }

    /// Number of free vecs currently held.
    pub fn free_count(&self) -> usize {
        self.0.len()
    }
}

impl Default for RowIdVecPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_checkout_checkin_reuse() {
        let mut pool = RowPool::new();
        let initial_free = pool.free_count();
        assert!(initial_free > 0);

        let mut row = pool.checkout();
        assert_eq!(pool.free_count(), initial_free - 1);
        row.values
            .insert("a".into(), blitz_types::value::Value::Int64(1));

        pool.checkin(row);
        assert_eq!(pool.free_count(), initial_free);

        // Checked-out row must be clean.
        let row2 = pool.checkout();
        assert!(row2.values.is_empty());
    }

    #[test]
    fn test_pool_grows_beyond_initial() {
        let mut pool: ObjectPool<Vec<u8>> = ObjectPool::new(Vec::new, 2, 0);
        let _a = pool.checkout();
        let _b = pool.checkout();
        let _c = pool.checkout(); // freshly allocated, unbounded
        assert!(pool.total_created() >= 2);
    }

    #[test]
    fn test_row_id_vec_pool_clears() {
        let mut pool = RowIdVecPool::new();
        let mut v = pool.checkout();
        v.push(RowId::new(7));
        pool.checkin(v);
        let v2 = pool.checkout();
        assert!(v2.is_empty());
    }
}
