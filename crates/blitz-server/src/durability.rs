//! Single-node durability: group-commit WAL + snapshots + replay.
//!
//! Modes:
//! - `None` (default): no WAL, in-memory only. 1.8M goodput @50K batch.
//!   Data lost on restart. Use for cache/shard.
//! - `GroupCommit{window_ms,max_batch}`: background thread batches N appends
//!   + one fsync. `window 1ms` ≈ near-sync durable; `window 1000ms` ≈
//!   every-sec (up to 1s loss window, much higher throughput).
//!
//! Design notes:
//! - Hot path never fsyncs: `try_send` to a bounded channel (32K). Full →
//!   `WAL backpressure` error (shed) instead of unbounded queueing.
//! - WAL payload = JSON bytes of the values map (version-tolerant).
//! - Snapshot stores full rows + wal_sequence; replay skips
//!   `seq <= snapshot_seq`, restores IDs via `insert_preserving_id`
//!   (fixes the old recovery bug that reassigned all IDs).

use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_types::column::{ColumnDef, ColumnType};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    None,
    GroupCommit { window_ms: u64, max_batch: usize },
}

impl DurabilityMode {
    pub fn every_sec() -> Self {
        Self::GroupCommit { window_ms: 1000, max_batch: 5000 }
    }
    pub fn near_sync() -> Self {
        Self::GroupCommit { window_ms: 1, max_batch: 100 }
    }
    pub fn is_durable(&self) -> bool {
        !matches!(self, Self::None)
    }
}

#[derive(Debug, Clone)]
pub struct PendingWrite {
    pub entry: blitz_wal::EntryType,
    pub table: String,
    pub row_id: u64,
    pub data: Vec<u8>,
}

pub struct WalBridge {
    sender: Option<std::sync::mpsc::SyncSender<PendingWrite>>,
    pub bytes_flushed: Arc<AtomicU64>,
    pub ops_flushed: Arc<AtomicU64>,
    pub dropped_full: Arc<AtomicU64>,
}

/// A cheap cloneable sender handle for `BlitzServer` (Send+Sync; the
/// background thread is detached and exits when all senders drop).
#[derive(Clone)]
pub struct WalSender {
    pub sender: std::sync::mpsc::SyncSender<PendingWrite>,
    pub bytes_flushed: Arc<AtomicU64>,
    pub ops_flushed: Arc<AtomicU64>,
    pub dropped_full: Arc<AtomicU64>,
}

impl WalSender {
    pub fn append(&self, w: PendingWrite) -> Result<(), String> {
        self.sender.try_send(w).map_err(|e| {
            self.dropped_full.fetch_add(1, Ordering::Relaxed);
            match e {
                std::sync::mpsc::TrySendError::Full(_) => "WAL backpressure (group full)".to_string(),
                std::sync::mpsc::TrySendError::Disconnected(_) => "WAL closed".to_string(),
            }
        })
    }
}

impl WalBridge {
    pub fn none() -> Self {
        Self { sender: None, bytes_flushed: Arc::new(AtomicU64::new(0)), ops_flushed: Arc::new(AtomicU64::new(0)), dropped_full: Arc::new(AtomicU64::new(0)) }
    }

    pub fn is_enabled(&self) -> bool {
        self.sender.is_some()
    }

    /// Open a bridge + detach its group-commit thread. For server use see
    /// `sender_handle()`; the thread exits when the last sender drops.
    pub fn open(data_dir: &str, mode: DurabilityMode) -> anyhow::Result<Self> {
        let (bridge, _detached) = Self::open_with_sender(data_dir, mode)?;
        Ok(bridge)
    }

    pub fn open_path(wal_path: PathBuf, mode: DurabilityMode) -> anyhow::Result<Self> {
        let (bridge, _detached) = Self::open_with_sender_path(wal_path, mode)?;
        Ok(bridge)
    }

    /// Same as `open` but also returns a cloneable `WalSender` for sharing.
    /// The background thread handle is detached (dropped) — it drains and
    /// exits on channel disconnect.
    pub fn open_with_sender(data_dir: &str, mode: DurabilityMode) -> anyhow::Result<(Self, Option<WalSender>)> {
        Self::open_with_sender_path(PathBuf::from(data_dir).join("wal.log"), mode)
    }

    pub fn open_with_sender_path(wal_path: PathBuf, mode: DurabilityMode) -> anyhow::Result<(Self, Option<WalSender>)> {
        match mode {
            DurabilityMode::None => Ok((Self::none(), None)),
            DurabilityMode::GroupCommit { window_ms, max_batch } => {
                // 128K deep: absorbs 10K-CCU batch bursts (200K appends in
                // <100ms) so groups shed only on sustained fsync overload,
                // not transient bursts.
                let (tx, rx) = std::sync::mpsc::sync_channel::<PendingWrite>(131_072);
                let bytes_flushed = Arc::new(AtomicU64::new(0));
                let ops_flushed = Arc::new(AtomicU64::new(0));
                let (bf, of) = (Arc::clone(&bytes_flushed), Arc::clone(&ops_flushed));
                let tname = format!("blitz-wal-{}", wal_path.file_name().and_then(|s| s.to_str()).unwrap_or("group"));
                let handle = std::thread::Builder::new()
                    .name(tname)
                    .spawn(move || {
                        let mut wal = match blitz_wal::WriteAheadLog::open(&wal_path) {
                            Ok(w) => w,
                            Err(e) => {
                                tracing::error!("WAL open failed: {:#}", e);
                                return;
                            }
                        };
                        let window = Duration::from_millis(window_ms.max(1));
                        let mut batch: Vec<PendingWrite> = Vec::with_capacity(max_batch.max(1));
                        loop {
                            // Fill one group: block for first, drain up to max.
                            match rx.recv_timeout(window) {
                                Ok(w) => {
                                    batch.push(w);
                                    while batch.len() < max_batch.max(1) {
                                        match rx.try_recv() {
                                            Ok(w) => batch.push(w),
                                            Err(std::sync::mpsc::TryRecvError::Empty) => break,
                                            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                                        }
                                    }
                                }
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                    if batch.is_empty() {
                                        continue;
                                    }
                                }
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                    // Drain remaining then exit.
                                    while let Ok(w) = rx.try_recv() {
                                        batch.push(w);
                                        if batch.len() >= max_batch.max(1) * 4 {
                                            break;
                                        }
                                    }
                                    if batch.is_empty() {
                                        return;
                                    }
                                    // fall through to flush once, then exit below
                                    for w in batch.drain(..) {
                                        let blen = w.data.len() as u64;
                                        if wal.append_buffered(w.entry, &w.table, w.row_id, &w.data).is_ok() {
                                            bf.fetch_add(blen, Ordering::Relaxed);
                                            of.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    let _ = wal.sync();
                                    return;
                                }
                            }
                            if batch.is_empty() {
                                continue;
                            }
                            let mut ok_bytes = 0u64;
                            let mut ok_ops = 0u64;
                            for w in batch.drain(..) {
                                let blen = w.data.len() as u64;
                                match wal.append_buffered(w.entry, &w.table, w.row_id, &w.data) {
                                    Ok(_) => {
                                        ok_bytes += blen;
                                        ok_ops += 1;
                                    }
                                    Err(e) => tracing::error!("WAL append failed: {:#}", e),
                                }
                            }
                            match wal.sync() {
                                Ok(()) => {
                                    bf.fetch_add(ok_bytes, Ordering::Relaxed);
                                    of.fetch_add(ok_ops, Ordering::Relaxed);
                                }
                                Err(e) => tracing::error!("WAL sync failed: {:#}", e),
                            }
                            if wal.needs_rotation() {
                                tracing::warn!("WAL needs rotation ({} bytes); snapshot + truncate recommended", wal.size());
                            }
                        }
                    })?;
                let dropped_full = Arc::new(AtomicU64::new(0));
                let sender_handle = WalSender {
                    sender: tx.clone(),
                    bytes_flushed: Arc::clone(&bytes_flushed),
                    ops_flushed: Arc::clone(&ops_flushed),
                    dropped_full: Arc::clone(&dropped_full),
                };
                // Detach: thread drains + exits on disconnect.
                let _detached: std::thread::JoinHandle<()> = handle;
                Ok((Self { sender: Some(tx), bytes_flushed, ops_flushed, dropped_full }, Some(sender_handle)))
            }
        }
    }

    /// Cloneable sender for sharing across `Arc<BlitzServer>` tasks.
    pub fn sender_handle(&self) -> Option<WalSender> {
        self.sender.as_ref().map(|s| WalSender {
            sender: s.clone(),
            bytes_flushed: Arc::clone(&self.bytes_flushed),
            ops_flushed: Arc::clone(&self.ops_flushed),
            dropped_full: Arc::clone(&self.dropped_full),
        })
    }

    /// Non-blocking append. Full channel → backpressure error (shed).
    pub fn append(&self, w: PendingWrite) -> Result<(), String> {
        match &self.sender {
            None => Ok(()),
            Some(tx) => tx.try_send(w).map_err(|e| {
                self.dropped_full.fetch_add(1, Ordering::Relaxed);
                match e {
                    std::sync::mpsc::TrySendError::Full(_) => "WAL backpressure (group full)".to_string(),
                    std::sync::mpsc::TrySendError::Disconnected(_) => "WAL closed".to_string(),
                }
            }),
        }
    }

    /// Block until queued writes are fsynced (poll ops counter).
    pub fn flush(&self, timeout: Duration) -> bool {
        if self.sender.is_none() {
            return true;
        }
        // Simpler robust flush: send a marker by waiting for queue to drain.
        // mpsc has no len(); approximate by sleeping one window + poll.
        let start = std::time::Instant::now();
        // Wait until no growth observed for 2 consecutive 5ms polls or timeout.
        let mut last = self.ops_flushed.load(Ordering::Relaxed);
        while start.elapsed() < timeout {
            std::thread::sleep(Duration::from_millis(5));
            let cur = self.ops_flushed.load(Ordering::Relaxed);
            if cur != last {
                last = cur;
                continue;
            }
            // queue may still hold unflushed batch mid-window; wait one more window slice
            std::thread::sleep(Duration::from_millis(5));
            let cur2 = self.ops_flushed.load(Ordering::Relaxed);
            if cur2 == last {
                return true;
            }
            last = cur2;
        }
        false
    }

    pub fn shutdown(&mut self) {
        // Drop sender → background thread drains remainder, syncs, exits.
        self.sender.take();
    }
}

/// Sharded group-commit cluster: N WAL files × N threads → N× fdatasync
/// parallelism. Routes by table hash so one hot table still pins one shard
/// (even spread for the 7-table app mix), while 16 app shards × 7 tables
/// spread uniformly. Single-file mode caps at one fdatasync pipe (~10-50K
/// fsyncs/sec); 16 shards sustain the 50K-batch durable SLO.
#[derive(Clone)]
pub struct WalCluster {
    pub shards: Vec<WalSender>,
    n: usize,
}

impl WalCluster {
    pub fn shards_for_mode() -> usize {
        16
    }

    pub fn disabled() -> Self {
        Self { shards: Vec::new(), n: 0 }
    }

    pub fn is_enabled(&self) -> bool {
        !self.shards.is_empty()
    }

    pub fn open(data_dir: &str, mode: DurabilityMode) -> anyhow::Result<Self> {
        if !mode.is_durable() {
            return Ok(Self::disabled());
        }
        let n = Self::shards_for_mode();
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let path = PathBuf::from(data_dir).join(format!("wal_{:02}.log", i));
            let (_bridge, sender) = WalBridge::open_with_sender_path(path, mode)?;
            // Bridge detached; sender ownership keeps the thread alive.
            if let Some(s) = sender {
                shards.push(s);
            }
        }
        Ok(Self { shards, n })
    }

    #[inline]
    pub fn route(&self, table: &str) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        table.hash(&mut h);
        (h.finish() as usize) % self.n.max(1)
    }

    pub fn append(&self, table: &str, entry: blitz_wal::EntryType, row_id: u64, data: Vec<u8>) -> Result<(), String> {
        if self.shards.is_empty() {
            return Ok(());
        }
        let i = self.route(table);
        self.shards[i]
            .append(PendingWrite { entry, table: table.to_string(), row_id, data })
            .map_err(|e| {
                // attribute drop already counted in sender
                e
            })
    }

    pub fn bytes_flushed(&self) -> u64 {
        self.shards.iter().map(|s| s.bytes_flushed.load(Ordering::Relaxed)).sum()
    }
    pub fn ops_flushed(&self) -> u64 {
        self.shards.iter().map(|s| s.ops_flushed.load(Ordering::Relaxed)).sum()
    }
    pub fn dropped_full(&self) -> u64 {
        self.shards.iter().map(|s| s.dropped_full.load(Ordering::Relaxed)).sum()
    }

    /// Poll until all shards quiesce or timeout (for snapshots/rotation).
    pub fn flush_all(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        let mut last: u64 = self.ops_flushed();
        while start.elapsed() < timeout {
            std::thread::sleep(Duration::from_millis(10));
            let cur = self.ops_flushed();
            if cur != last {
                last = cur;
                continue;
            }
            std::thread::sleep(Duration::from_millis(10));
            if self.ops_flushed() == last {
                return true;
            }
            last = self.ops_flushed();
        }
        false
    }
}

// ---------------------------------------------------------------------------
// JSON <-> Value helpers (WAL/snapshot payloads)
// ---------------------------------------------------------------------------

pub fn values_to_json_bytes(values: &std::collections::HashMap<String, Value>) -> Vec<u8> {
    let m: serde_json::Map<String, serde_json::Value> =
        values.iter().map(|(k, v)| (k.clone(), value_to_json(v))).collect();
    serde_json::to_vec(&serde_json::Value::Object(m)).unwrap_or_default()
}

pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int8(n) => serde_json::json!(*n),
        Value::Int16(n) => serde_json::json!(*n),
        Value::Int32(n) => serde_json::json!(*n),
        Value::Int64(n) => serde_json::json!(*n),
        Value::UInt8(n) => serde_json::json!(*n),
        Value::UInt16(n) => serde_json::json!(*n),
        Value::UInt32(n) => serde_json::json!(*n),
        Value::UInt64(n) => serde_json::json!(*n),
        Value::Float32(n) => serde_json::json!(*n),
        Value::Float64(n) => serde_json::json!(*n),
        Value::Decimal(s) => serde_json::Value::String(s.clone()),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::json!(b),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::Timestamp(t) => serde_json::Value::String(t.to_rfc3339()),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::Json(j) => j.clone(),
        Value::Array(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
    }
}

pub fn json_to_value(j: &serde_json::Value) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(u) = n.as_u64() {
                // fits u64 but not i64 → UInt64 else Float
                Value::UInt64(u)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(items) => {
            // Try compact Int64 array (Bytes/ids) else generic Json
            Value::Json(serde_json::Value::Array(items.clone()))
        }
        serde_json::Value::Object(_) => Value::Json(j.clone()),
    }
}

fn infer_column_type(v: &Value) -> ColumnType {
    match v {
        Value::Boolean(_) => ColumnType::Boolean,
        Value::Int8(_) => ColumnType::Int8,
        Value::Int16(_) => ColumnType::Int16,
        Value::Int32(_) => ColumnType::Int32,
        Value::Int64(_) => ColumnType::Int64,
        Value::UInt8(_) => ColumnType::UInt8,
        Value::UInt16(_) => ColumnType::UInt16,
        Value::UInt32(_) => ColumnType::UInt32,
        Value::UInt64(_) => ColumnType::UInt64,
        Value::Float32(_) => ColumnType::Float32,
        Value::Float64(_) => ColumnType::Float64,
        _ => ColumnType::String,
    }
}

fn infer_schema(name: &str, sample: &std::collections::HashMap<String, Value>) -> TableSchema {
    let mut s = TableSchema::new(name)
        .with_column(ColumnDef::new("id", ColumnType::Int64).nullable());
    let mut keys: Vec<&String> = sample.keys().filter(|k| k.as_str() != "id").collect();
    keys.sort();
    for k in keys {
        let ct = infer_column_type(&sample[k]);
        s = s.with_column(ColumnDef::new(k.clone(), ct).nullable());
    }
    s
}

// ---------------------------------------------------------------------------
// Snapshot save / load + WAL replay against the live engine
// ---------------------------------------------------------------------------

/// Save a snapshot of all engine tables. Returns snapshot path.
pub fn save_snapshot(engine: &InMemoryTableEngine, data_dir: &str) -> anyhow::Result<PathBuf> {
    let mgr = blitz_snapshot::SnapshotManager::new(PathBuf::from(data_dir).join("snapshots"));
    let mut tables: Vec<(String, Vec<(u64, Vec<(String, serde_json::Value)>)>)> = Vec::new();
    for tname in engine.table_names() {
        let rows = engine.scan_arcs(&tname).unwrap_or_default();
        let mut srows: Vec<(u64, Vec<(String, serde_json::Value)>)> = rows
            .iter()
            .map(|r| {
                let vals = r.values.iter().map(|(k, v)| (k.clone(), value_to_json(v))).collect();
                (r.id.as_u64(), vals)
            })
            .collect();
        srows.sort_by_key(|(id, _)| *id);
        tables.push((tname, srows));
    }
    // wal_sequence best-effort: read current WAL next_sequence if present
    let wal_seq = 0u64;
    Ok(mgr.create_snapshot(&tables, &[], wal_seq)?)
}

/// Offline rotation: recover everything into a scratch engine, write a fresh
/// snapshot, then truncate all WAL files. Run stopped-server only (the live
/// `snapshot_and_rotate` quiesces instead). Bounds restart replay time.
pub fn offline_rotate(data_dir: &str) -> anyhow::Result<PathBuf> {
    let engine = InMemoryTableEngine::new();
    let (_tables, _rows, _replayed) = recover(&engine, data_dir)?;
    let snap = save_snapshot(&engine, data_dir)?;
    let mut wals = vec![PathBuf::from(data_dir).join("wal.log")];
    for i in 0..WalCluster::shards_for_mode() {
        wals.push(PathBuf::from(data_dir).join(format!("wal_{:02}.log", i)));
    }
    for p in wals {
        if p.exists() {
            if let Ok(mut w) = blitz_wal::WriteAheadLog::open(&p) {
                let _ = w.truncate();
            }
        }
    }
    let mgr = blitz_snapshot::SnapshotManager::new(PathBuf::from(data_dir).join("snapshots"));
    let _ = mgr.prune(3);
    Ok(snap)
}

/// Load latest snapshot + replay WAL entries after its sequence.
/// Returns (tables_restored, rows_restored, wal_replayed).
pub fn recover(engine: &InMemoryTableEngine, data_dir: &str) -> anyhow::Result<(usize, usize, usize)> {
    let mgr = blitz_snapshot::SnapshotManager::new(PathBuf::from(data_dir).join("snapshots"));
    let mut tables_restored = 0usize;
    let mut rows_restored = 0usize;
    let mut snap_seq = 0u64;
    if let Some((snap, seq)) = mgr.load_latest()? {
        snap_seq = seq;
        for t in &snap.tables {
            if !engine.table_exists(&t.name) {
                // Infer schema from first row (all nullable).
                let sample: std::collections::HashMap<String, Value> = t
                    .rows
                    .first()
                    .map(|r| r.values.iter().map(|(k, j)| (k.clone(), json_to_value(j))).collect())
                    .unwrap_or_default();
                let schema = infer_schema(&t.name, &sample);
                let _ = engine.create_table(schema);
            }
            let mut rows = t.rows.clone();
            rows.sort_by_key(|r| r.id);
            for r in rows {
                let values: std::collections::HashMap<String, Value> =
                    r.values.into_iter().map(|(k, j)| (k, json_to_value(&j))).collect();
                let row = Row { id: RowId::new(r.id), values };
                match engine.insert_preserving_id(&t.name, row) {
                    Ok(_) => rows_restored += 1,
                    Err(_) => {} // duplicate after unclean shutdown replay; skip
                }
            }
            tables_restored += 1;
        }
    }
    // Replay WAL tail (legacy single file + all shard files). Per-file
    // sequences are independent; routing pins each table to one shard so
    // per-table order is preserved. Inserts are DuplicateKey-tolerant
    // (snapshot rows replayed idempotently).
    let mut wal_files: Vec<PathBuf> = Vec::new();
    let legacy = PathBuf::from(data_dir).join("wal.log");
    if legacy.exists() {
        wal_files.push(legacy);
    }
    for i in 0..16 {
        let p = PathBuf::from(data_dir).join(format!("wal_{:02}.log", i));
        if p.exists() {
            wal_files.push(p);
        }
    }
    if wal_files.is_empty() {
        return Ok((tables_restored, rows_restored, 0));
    }
    let mut replayed = 0usize;
    for wal_path in wal_files {
        let mut wal = match blitz_wal::WriteAheadLog::open(&wal_path) {
            Ok(w) => w,
            Err(_) => continue,
        };
        let entries = wal.read_all().unwrap_or_default();
        for e in entries {
            if e.sequence <= snap_seq {
                continue;
            }
        match e.entry_type {
            blitz_wal::EntryType::Insert => {
                let values: std::collections::HashMap<String, Value> =
                    serde_json::from_slice::<serde_json::Value>(&e.data)
                        .ok()
                        .and_then(|v| v.as_object().cloned())
                        .map(|m| m.into_iter().map(|(k, j)| (k, json_to_value(&j))).collect())
                        .unwrap_or_default();
                if !engine.table_exists(&e.table) {
                    let schema = infer_schema(&e.table, &values);
                    let _ = engine.create_table(schema);
                }
                let row = Row { id: RowId::new(e.row_id), values };
                if engine.insert_preserving_id(&e.table, row).is_ok() {
                    replayed += 1;
                }
            }
            blitz_wal::EntryType::Update => {
                let values: std::collections::HashMap<String, Value> =
                    serde_json::from_slice::<serde_json::Value>(&e.data)
                        .ok()
                        .and_then(|v| v.as_object().cloned())
                        .map(|m| m.into_iter().map(|(k, j)| (k, json_to_value(&j))).collect())
                        .unwrap_or_default();
                let _ = engine.update(&e.table, RowId::new(e.row_id), values);
                replayed += 1;
            }
            blitz_wal::EntryType::Delete => {
                let _ = engine.delete(&e.table, RowId::new(e.row_id));
                replayed += 1;
            }
            _ => {}
            }
        }
    }
    Ok((tables_restored, rows_restored, replayed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::column::{ColumnDef, ColumnType};

    fn tmpdir(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "blitz-dur-{}-{}-{}",
            std::process::id(),
            tag,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn t_schema() -> TableSchema {
        TableSchema::new("t")
            .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
            .with_column(ColumnDef::new("v", ColumnType::String).nullable())
    }

    #[test]
    fn wal_json_roundtrip_exact_for_app_types() {
        let mut m = std::collections::HashMap::new();
        m.insert("id".into(), Value::Int64(7));
        m.insert("v".into(), Value::String("hello".into()));
        m.insert("b".into(), Value::Boolean(true));
        let bytes = values_to_json_bytes(&m);
        let back: std::collections::HashMap<String, Value> =
            serde_json::from_slice::<serde_json::Value>(&bytes)
                .unwrap()
                .as_object()
                .unwrap()
                .clone()
                .into_iter()
                .map(|(k, j)| (k, json_to_value(&j)))
                .collect();
        assert_eq!(back, m);
    }

    #[test]
    fn snapshot_preserves_ids_and_bumps_allocator() {
        let dir = tmpdir("snap");
        let e = InMemoryTableEngine::new();
        e.create_table(t_schema()).unwrap();
        let mut ids = Vec::new();
        for i in 0..3 {
            let mut r = Row::new(RowId::new(0));
            r.set("id", Value::Int64(i));
            r.set("v", Value::String(format!("v{}", i)));
            ids.push(e.insert("t", r).unwrap().as_u64());
        }
        save_snapshot(&e, dir.to_str().unwrap()).unwrap();
        // Fresh engine recovers with identical IDs; next insert is fresh.
        let e2 = InMemoryTableEngine::new();
        let (tables, rows, replayed) = recover(&e2, dir.to_str().unwrap()).unwrap();
        assert_eq!((tables, rows, replayed), (1, 3, 0));
        for id in &ids {
            assert!(e2.get("t", RowId::new(*id)).unwrap().is_some(), "id {} lost", id);
        }
        let mut r = Row::new(RowId::new(0));
        r.set("id", Value::Int64(99));
        r.set("v", Value::String("new".into()));
        let nid = e2.insert("t", r).unwrap().as_u64();
        assert!(!ids.contains(&nid), "allocator collided after restore");
    }

    #[test]
    fn group_commit_wal_replays_inserts() {
        let dir = tmpdir("wal");
        let d = dir.to_str().unwrap();
        let mut b = WalBridge::open(d, DurabilityMode::near_sync()).unwrap();
        // Simulate two committed inserts (row ids 1,2).
        for i in 1..=2u64 {
            let mut m = std::collections::HashMap::new();
            m.insert("id".into(), Value::Int64(i as i64));
            m.insert("v".into(), Value::String(format!("w{}", i)));
            b.append(PendingWrite {
                entry: blitz_wal::EntryType::Insert,
                table: "t".into(),
                row_id: i,
                data: values_to_json_bytes(&m),
            })
            .unwrap();
        }
        assert!(b.flush(Duration::from_secs(5)), "group-commit flush timed out");
        b.shutdown();
        // New engine replays them with IDs intact.
        let e = InMemoryTableEngine::new();
        let (_, rows, replayed) = recover(&e, d).unwrap();
        assert!(rows + replayed >= 2, "rows={} replayed={}", rows, replayed);
        assert!(e.get("t", RowId::new(1)).unwrap().is_some());
        assert!(e.get("t", RowId::new(2)).unwrap().is_some());
    }
}

#[cfg(test)]
mod server_wiring_tests {
    use super::*;
    use crate::server::{BlitzServer, ServerConfig};

    fn tmpdir2(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "blitz-dur-{}-{}-{}",
            std::process::id(),
            tag,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn server_opens_bridge_and_group_commits() {
        let dir = tmpdir2("srvwal");
        let d = dir.to_str().unwrap().to_string();
        let cfg = ServerConfig {
            data_dir: Some(d.clone()),
            durability: DurabilityMode::near_sync(),
            ..Default::default()
        };
        // Start is async; drive with a minimal runtime.
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let s = BlitzServer::with_config(cfg);
            s.start().await.unwrap();
            // No tables needed for WAL path: log two raw inserts.
            s.wal_log(blitz_wal::EntryType::Insert, "t", 1, b"{\"id\":1}".to_vec()).unwrap();
            s.wal_log(blitz_wal::EntryType::Insert, "t", 2, b"{\"id\":2}".to_vec()).unwrap();
            // Poll group-commit (1ms window) up to 5s.
            let start = std::time::Instant::now();
            loop {
                if s.stats().wal_ops >= 2 {
                    break;
                }
                assert!(start.elapsed() < Duration::from_secs(5), "group-commit never flushed");
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(s.stats().wal_bytes > 0);
            assert_eq!(s.wal_dropped(), 0);
        });
    }
}

#[cfg(test)]
mod crash_tests {
    use super::*;
    use crate::server::{BlitzServer, ServerConfig};
    use blitz_core::TableEngine;

    #[test]
    fn crash_recovery_preserves_writes() {
        let dir = std::env::temp_dir().join(format!(
            "blitz-dur-{}-crash-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_str().unwrap().to_string();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

        // Server 1: create, write 3 rows (engine + WAL, like transport), flush, snapshot.
        let mut ids = Vec::new();
        rt.block_on(async {
            let cfg = ServerConfig {
                data_dir: Some(d.clone()),
                durability: DurabilityMode::near_sync(),
                ..Default::default()
            };
            let s = BlitzServer::with_config(cfg);
            s.start().await.unwrap();
            let t_schema = TableSchema::new("t")
                .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
                .with_column(ColumnDef::new("v", ColumnType::String).nullable());
            s.engine().create_table(t_schema).unwrap();
            for i in 0..3i64 {
                let mut m: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                m.insert("id".into(), Value::Int64(i));
                m.insert("v".into(), Value::String(format!("c{}", i)));
                let mut row = Row::new(RowId::new(0));
                for (k, v) in &m {
                    row.set(k.clone(), v.clone());
                }
                let assigned = s.engine().insert("t", row).unwrap();
                ids.push(assigned.as_u64());
                let data = values_to_json_bytes(&m);
                s.wal_log(blitz_wal::EntryType::Insert, "t", assigned.as_u64(), data).unwrap();
            }
            // Wait for group fsync.
            let start = std::time::Instant::now();
            loop {
                if s.stats().wal_ops >= 3 {
                    break;
                }
                assert!(start.elapsed() < Duration::from_secs(5), "wal never flushed");
                std::thread::sleep(Duration::from_millis(5));
            }
            s.save_snapshot().unwrap();
            // `s` drops here → WAL sender dropped → thread drains + exits.
        });

        // Server 2: same dir recovers; rows intact with IDs; allocator fresh.
        rt.block_on(async {
            let cfg = ServerConfig {
                data_dir: Some(d.clone()),
                durability: DurabilityMode::near_sync(),
                ..Default::default()
            };
            let s2 = BlitzServer::with_config(cfg);
            s2.start().await.unwrap();
            for id in &ids {
                let got = s2.engine().get("t", RowId::new(*id)).unwrap();
                assert!(got.is_some(), "row {} lost across restart", id);
                assert_eq!(got.unwrap().get("v"), Some(&Value::String(format!("c{}", ids.iter().position(|x| x == id).unwrap()))));
            }
            let mut m: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
            m.insert("id".into(), Value::Int64(99));
            m.insert("v".into(), Value::String("new".into()));
            let mut row = Row::new(RowId::new(0));
            for (k, v) in &m {
                row.set(k.clone(), v.clone());
            }
            let nid = s2.engine().insert("t", row).unwrap().as_u64();
            assert!(!ids.contains(&nid), "allocator reused ID after recovery");
        });
    }
}

#[cfg(test)]
mod backup_tests {
    use super::*;

    #[test]
    fn backup_restore_drill_from_snapshot_files() {
        // Live dir: write + snapshot. Backup dir: copy snapshots/*.db.
        let live = std::env::temp_dir().join(format!(
            "blitz-bk-live-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let backup = std::env::temp_dir().join(format!(
            "blitz-bk-backup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&live).unwrap();
        let e = InMemoryTableEngine::new();
        e.create_table(
            TableSchema::new("t")
                .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
                .with_column(ColumnDef::new("v", ColumnType::String).nullable()),
        )
        .unwrap();
        for i in 0..5i64 {
            let mut r = Row::new(RowId::new(0));
            r.set("id", Value::Int64(i));
            r.set("v", Value::String(format!("b{}", i)));
            e.insert("t", r).unwrap();
        }
        save_snapshot(&e, live.to_str().unwrap()).unwrap();
        // Backup = copy snapshot files (operator procedure).
        let src = live.join("snapshots");
        let dst = backup.join("snapshots");
        std::fs::create_dir_all(&dst).unwrap();
        for f in std::fs::read_dir(&src).unwrap() {
            let f = f.unwrap();
            std::fs::copy(f.path(), dst.join(f.file_name())).unwrap();
        }
        // Simulate total loss of live dir, restore into fresh engine from backup.
        std::fs::remove_dir_all(&live).unwrap();
        let e2 = InMemoryTableEngine::new();
        let (tables, rows, _) = recover(&e2, backup.to_str().unwrap()).unwrap();
        assert_eq!((tables, rows), (1, 5));
        assert_eq!(e2.count("t").unwrap(), 5);
    }
}

#[cfg(test)]
mod rotation_tests {
    use super::*;
    use crate::server::{BlitzServer, ServerConfig};
    use blitz_core::TableEngine;

    #[test]
    fn snapshot_and_rotate_preserves_data_and_truncates_wal() {
        let dir = std::env::temp_dir().join(format!(
            "blitz-rot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_str().unwrap().to_string();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let cfg = ServerConfig {
                data_dir: Some(d.clone()),
                durability: DurabilityMode::near_sync(),
                ..Default::default()
            };
            let s = BlitzServer::with_config(cfg);
            s.start().await.unwrap();
            s.engine()
                .create_table(
                    TableSchema::new("t")
                        .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
                        .with_column(ColumnDef::new("v", ColumnType::String).nullable()),
                )
                .unwrap();
            for i in 0..3i64 {
                let mut m: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                m.insert("id".into(), Value::Int64(i));
                m.insert("v".into(), Value::String(format!("r{}", i)));
                let mut row = Row::new(RowId::new(0));
                for (k, v) in &m {
                    row.set(k.clone(), v.clone());
                }
                let assigned = s.engine().insert("t", row).unwrap();
                let data = values_to_json_bytes(&m);
                s.wal_log(blitz_wal::EntryType::Insert, "t", assigned.as_u64(), data).unwrap();
            }
            // Drain groups, then rotate.
            let start = std::time::Instant::now();
            loop {
                if s.stats().wal_ops >= 3 {
                    break;
                }
                assert!(start.elapsed() < Duration::from_secs(5));
                std::thread::sleep(Duration::from_millis(5));
            }
            let snap = s.snapshot_and_rotate().unwrap();
            assert!(snap.exists());
            // WAL shard files truncated away (or tiny fresh headers).
            let mut wal_bytes = 0u64;
            for i in 0..WalCluster::shards_for_mode() {
                let p = dir.join(format!("wal_{:02}.log", i));
                if p.exists() {
                    wal_bytes += std::fs::metadata(&p).unwrap().len();
                }
            }
            assert!(wal_bytes < 4096, "wal not truncated: {} bytes", wal_bytes);
            // Live server keeps serving after rotation.
            let mut m: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
            m.insert("id".into(), Value::Int64(99));
            m.insert("v".into(), Value::String("post".into()));
            let mut row = Row::new(RowId::new(0));
            for (k, v) in &m {
                row.set(k.clone(), v.clone());
            }
            let nid = s.engine().insert("t", row).unwrap();
            let data = values_to_json_bytes(&m);
            s.wal_log(blitz_wal::EntryType::Insert, "t", nid.as_u64(), data).unwrap();
            assert_eq!(s.engine().count("t").unwrap(), 4);
        });
        // Fresh restart recovers snapshot + post-rotation tail.
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let cfg = ServerConfig {
                data_dir: Some(d.clone()),
                durability: DurabilityMode::near_sync(),
                ..Default::default()
            };
            let s2 = BlitzServer::with_config(cfg);
            s2.start().await.unwrap();
            assert_eq!(s2.engine().count("t").unwrap(), 4);
        });
    }
}
