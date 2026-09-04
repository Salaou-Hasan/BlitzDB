use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

#[derive(Debug, Default)]
pub struct MetricsCollector {
    pub inserts: AtomicU64,
    pub reads: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_rolled_back: AtomicU64,
    pub queries_executed: AtomicU64,
    pub index_lookups: AtomicU64,
    custom_counters: RwLock<HashMap<String, u64>>,
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

    pub fn record_transaction_commit(&self) {
        self.transactions_committed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_transaction_rollback(&self) {
        self.transactions_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query(&self) {
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_index_lookup(&self) {
        self.index_lookups.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_counter(&self, name: &str) {
        let mut counters = self.custom_counters.write().unwrap();
        *counters.entry(name.to_string()).or_insert(0) += 1;
    }

    pub fn get_counter(&self, name: &str) -> u64 {
        self.custom_counters.read().unwrap().get(name).copied().unwrap_or(0)
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            inserts: self.inserts.load(Ordering::Relaxed),
            reads: self.reads.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            transactions_committed: self.transactions_committed.load(Ordering::Relaxed),
            transactions_rolled_back: self.transactions_rolled_back.load(Ordering::Relaxed),
            queries_executed: self.queries_executed.load(Ordering::Relaxed),
            index_lookups: self.index_lookups.load(Ordering::Relaxed),
            custom_counters: self.custom_counters.read().unwrap().clone(),
        }
    }

    pub fn reset(&self) {
        self.inserts.store(0, Ordering::Relaxed);
        self.reads.store(0, Ordering::Relaxed);
        self.updates.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.transactions_committed.store(0, Ordering::Relaxed);
        self.transactions_rolled_back.store(0, Ordering::Relaxed);
        self.queries_executed.store(0, Ordering::Relaxed);
        self.index_lookups.store(0, Ordering::Relaxed);
        self.custom_counters.write().unwrap().clear();
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub inserts: u64,
    pub reads: u64,
    pub updates: u64,
    pub deletes: u64,
    pub transactions_committed: u64,
    pub transactions_rolled_back: u64,
    pub queries_executed: u64,
    pub index_lookups: u64,
    pub custom_counters: HashMap<String, u64>,
}

pub struct Timer {
    start: Instant,
    label: String,
}

impl Timer {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            start: Instant::now(),
            label: label.into(),
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    pub fn stop(self) -> (String, f64) {
        let elapsed = self.elapsed_ms();
        (self.label, elapsed)
    }
}

pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector() {
        let m = MetricsCollector::new();
        m.record_insert();
        m.record_insert();
        m.record_read();
        m.record_update();
        m.record_delete();
        m.record_transaction_commit();
        m.record_query();

        let snap = m.snapshot();
        assert_eq!(snap.inserts, 2);
        assert_eq!(snap.reads, 1);
        assert_eq!(snap.updates, 1);
        assert_eq!(snap.deletes, 1);
        assert_eq!(snap.transactions_committed, 1);
        assert_eq!(snap.queries_executed, 1);
    }

    #[test]
    fn test_custom_counter() {
        let m = MetricsCollector::new();
        m.increment_counter("cache_hits");
        m.increment_counter("cache_hits");
        m.increment_counter("cache_misses");
        assert_eq!(m.get_counter("cache_hits"), 2);
        assert_eq!(m.get_counter("cache_misses"), 1);
        assert_eq!(m.get_counter("missing"), 0);
    }

    #[test]
    fn test_metrics_reset() {
        let m = MetricsCollector::new();
        m.record_insert();
        m.record_read();
        m.increment_counter("test");
        m.reset();
        let snap = m.snapshot();
        assert_eq!(snap.inserts, 0);
        assert_eq!(snap.reads, 0);
        assert!(snap.custom_counters.is_empty());
    }

    #[test]
    fn test_timer() {
        let timer = Timer::new("test_op");
        let (_, ms) = timer.stop();
        assert!(ms >= 0.0);
    }

    #[test]
    fn test_metrics_snapshot() {
        let m = MetricsCollector::new();
        m.record_insert();
        m.record_insert();
        m.record_insert();
        let snap1 = m.snapshot();
        m.record_insert();
        let snap2 = m.snapshot();
        assert_eq!(snap1.inserts, 3);
        assert_eq!(snap2.inserts, 4);
    }
}
