use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct MetricsCollector {
    pub inserts: AtomicU64,
    pub reads: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_rolled_back: AtomicU64,
}

impl MetricsCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_read(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_update(&self) {
        self.updates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }
}
