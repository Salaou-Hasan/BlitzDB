use anyhow::Result;
use blitz_auth::{Identity, Permission, Session};
use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_events::{Event, EventEmitter, EventKind};
use blitz_policy::PolicyEngine;
use crate::social::{FanoutJob, FanoutSender};
use blitz_protocol::Op;
use blitz_realtime::{Delta, SubscriptionManager};
use blitz_tx::TransactionManager;
use blitz_types::column::{ColumnDef, ColumnType};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    RwLock,
};

/// One committed write, for bounded `Subscribe` long-poll reads.
/// Pushed only for acked writes (never for denied/shed ops).
#[derive(Debug, Clone)]
pub struct ChangeRecord {
    pub seq: u64,
    pub table: String,
    pub op: &'static str,
    pub row_id: u64,
    pub ts_micros: u64,
}

/// Max retained change records per table (long-poll window).
pub const MAX_CHANGES_PER_TABLE: usize = 128;

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: Option<String>,
    pub max_connections: usize,
    pub enable_auth: bool,
    pub max_message_size: usize,
    /// Shed load when `connection_count >= shed_at_connections`.
    /// `None` (default) disables shedding; admission still caps at
    /// `max_connections`. Set to ~90% of max in prod so p99 degrades
    /// via fast failures instead of unbounded queueing.
    pub shed_at_connections: Option<usize>,
    /// Count a response as "slow" when end-to-end handling exceeds this.
    /// Used for p95/p99.9 alerting via `slow_responses()`.
    pub slow_threshold_ms: u64,
    /// Skip per-insert schema validation (shard/cache fast path).
    /// Only for trusted app layers with fixed shape; untrusted clients must
    /// leave this `false`. Saves 2-3 hash lookups per insert.
    pub skip_validation: bool,
    /// Durability mode. `None` (default) = in-memory only, current speed.
    /// Set with `data_dir` for crash safety (see `durability`).
    pub durability: crate::durability::DurabilityMode,
    /// Snapshot every N seconds when durable (0 = disabled).
    pub snapshot_secs: u64,
    /// Close idle keep-alive connections after N seconds (0 = disabled).
    /// Default 300s reaps slow-loris/FD leaks; benches hold <60s so unaffected.
    pub idle_timeout_secs: u64,
    /// Max connections per source IP (0 = unlimited). Default 20000: the
    /// 8-IP bench harness stays under it at 50K (6250/IP); single-IP prod
    /// clients should raise it, public endpoints lower it.
    pub max_connections_per_ip: usize,
    /// Reject unauthenticated ops when true (default false = bypass, benches).
    /// With true: clients handshake via any op carrying
    /// `values {"_auth": token}` (usually `Ping`); the identity binds to the
    /// connection. Direct permissions or policy allow-rules then apply.
    pub require_auth: bool,
    /// Pre-shared bearer tokens → identity (seeded into the runtime
    /// identities map at `start`). Prefer short-lived tokens via
    /// `register_identity` for rotation without restart.
    pub auth_tokens: HashMap<String, Identity>,
    /// Server-side sharding: `base -> (shard count, shard-key column)`.
    /// Empty (default) = every table unsharded, today's behavior. App code
    /// always uses BASE names; the server hashes `values[column] % N` on
    /// insert and routes point ops by RowId shard bits. RowIds become
    /// `(shard<<56)|local`: globally unique, locally engine-native.
    /// Configure before data lands (no online resharding in v1); unique
    /// indexes are per-shard (global uniqueness needs the shard key to be
    /// the unique column, or unsharded tables).
    pub table_shards: HashMap<String, ShardSpec>,
    /// Row ownership: `table -> owner column`. When set (and auth on),
    /// point ops are owner-checked; collection reads filter by owner
    /// (Scan pre-window, Find/Subscribe/Search per-row; push streams stay
    /// rejected — poll instead). Empty (default) = table-level auth only.
    pub row_owner: HashMap<String, String>,
    /// HTTP bridge CORS: allowed `Origin` values (`["*"]` = any). Empty
    /// (default) = no CORS headers (same-origin / curl only). Browsers
    /// block cross-origin fetch without these — set for SPA templates.
    pub http_cors_origins: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 7420,
            data_dir: None,
            max_connections: 200_000,
            enable_auth: true,
            max_message_size: 1_048_576,
            shed_at_connections: None,
            slow_threshold_ms: 50,
            skip_validation: false,
            durability: crate::durability::DurabilityMode::None,
            snapshot_secs: 0,
            idle_timeout_secs: 300,
            max_connections_per_ip: 20_000,
            require_auth: false,
            auth_tokens: HashMap::new(),
            table_shards: HashMap::new(),
            row_owner: HashMap::new(),
            http_cors_origins: Vec::new(),
        }
    }
}

/// Server-side sharding spec for one base table.
#[derive(Debug, Clone)]
pub struct ShardSpec {
    /// Number of physical shards (`{base}_{shard:02}`).
    pub shards: usize,
    /// Row values column hashed for insert routing (`% shards`).
    pub column: String,
}

impl ShardSpec {
    pub fn new(shards: usize, column: impl Into<String>) -> Self {
        Self { shards: shards.max(1), column: column.into() }
    }
}

/// High 8 RowId bits carry the shard; low 56 are engine-local.
pub const SHARD_SHIFT: u32 = 56;
pub const LOCAL_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

/// Idempotency map shards (see `BlitzServer::idem`).
pub const IDEM_SHARDS: usize = 16;
const IDEM_SHARD_MASK: usize = IDEM_SHARDS - 1;

/// FNV-1a shard for an idempotency key (fast, deterministic).
fn idem_shard(key: &str) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in key.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) & IDEM_SHARD_MASK
}

/// Deterministic FNV-1a hash over a value's canonical bytes (insert routing
/// must be stable across processes/restarts — `RandomState` is not).
fn shard_hash(v: &Value) -> u64 {
    const FNV_OFF: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut h = FNV_OFF;
    let mut mix = |b: &[u8]| {
        for byte in b {
            h ^= *byte as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
    };
    match v {
        Value::Boolean(b) => mix(&[*b as u8]),
        Value::Int8(n) => mix(&n.to_le_bytes()),
        Value::Int16(n) => mix(&n.to_le_bytes()),
        Value::Int32(n) => mix(&n.to_le_bytes()),
        Value::Int64(n) => mix(&n.to_le_bytes()),
        Value::UInt8(n) => mix(&n.to_le_bytes()),
        Value::UInt16(n) => mix(&n.to_le_bytes()),
        Value::UInt32(n) => mix(&n.to_le_bytes()),
        Value::UInt64(n) => mix(&n.to_le_bytes()),
        Value::Float32(n) => mix(&n.to_le_bytes()),
        Value::Float64(n) => mix(&n.to_le_bytes()),
        Value::Decimal(s) | Value::String(s) => mix(s.as_bytes()),
        Value::Bytes(b) => mix(b),
        Value::Uuid(u) => mix(u.as_bytes()),
        Value::Timestamp(t) => mix(&t.timestamp_micros().to_le_bytes()),
        Value::Date(d) => mix(&d.to_string().as_bytes()),
        Value::Json(j) => mix(j.to_string().as_bytes()),
        Value::Array(a) => {
            for item in a {
                mix(&shard_hash(item).to_le_bytes());
            }
        }
        Value::Null => mix(b"null"),
    }
    h
}

/// Point-in-time server stats for alerting / shedding.
#[derive(Debug, Clone, Copy, Default)]
pub struct ServerStats {
    pub connections: usize,
    pub total_requests: u64,
    pub slow_responses: u64,
    pub shed_drops: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    /// Active transactions / conflicts / subscription fanout.
    /// `active_tx/tx_conflicts` are 0: TCP is auto-commit (see dispatch docs).
    /// `subscription_fanout` = push messages delivered; `push_dropped` =
    /// slow consumers evicted.
    pub active_tx: u64,
    pub tx_conflicts: u64,
    pub subscription_fanout: u64,
    pub push_dropped: u64,
    /// Async timeline fanout (best-effort worker): materialized / dropped.
    pub fanout_done: u64,
    pub fanout_dropped: u64,
    /// WAL bytes/ops. 0 = durability `None` (in-memory). Group-commit mode
    /// will fill these; NVMe IOPS then comes from `/proc/diskstats`.
    pub wal_bytes: u64,
    pub wal_ops: u64,
}

/// Main BlitzDB server instance.
///
/// Fully shareable via `Arc`: the engine uses table-level locks and all
/// interior state is synchronized, so every method takes `&self` and
/// connection tasks can run concurrently.
pub struct BlitzServer {
    config: ServerConfig,
    /// Shared with `tx_manager` (same `Arc`): committed tx writes are
    /// immediately visible to TCP reads and vice versa.
    engine: std::sync::Arc<InMemoryTableEngine>,
    tx_manager: TransactionManager,
    event_emitter: EventEmitter,
    subscription_manager: RwLock<SubscriptionManager>,
    policy_engine: PolicyEngine,
    identities: RwLock<HashMap<String, Identity>>,
    /// Short-lived sessions: token → Session (expiry enforced on resolve;
    /// expired entries are evicted opportunistically). Pre-shared
    /// `auth_tokens`/`identities` stay long-lived by design; sessions are
    /// for login flows (`register_session` with TTL).
    sessions: RwLock<HashMap<String, Session>>,
    connections: AtomicUsize,
    started_at: RwLock<Option<chrono::DateTime<chrono::Utc>>>,
    total_requests: std::sync::atomic::AtomicU64,
    slow_responses: std::sync::atomic::AtomicU64,
    shed_drops: std::sync::atomic::AtomicU64,
    bytes_read: std::sync::atomic::AtomicU64,
    bytes_written: std::sync::atomic::AtomicU64,
    wal_dropped_full: std::sync::atomic::AtomicU64,
    wal: RwLock<Option<crate::durability::WalCluster>>,
    wal_rotating: AtomicBool,
    /// Set when a WAL cluster is installed. Lets `wal_log` skip the map
    /// lock + 16-sender clone entirely in `None` mode (that clone cost
    /// ~30% throughput at 50K batch once shards landed).
    wal_enabled: AtomicBool,
    /// Idempotency dedup: client `_idem` key → assigned RowId.
    /// 16 shards by key hash (one global write lock collapsed past ~1K
    /// concurrent inserters; sharded maps divide it by 16). Bounded 256K
    /// total (each shard clears half past 16K); safe-retry for shed/timeout.
    idem: [RwLock<HashMap<String, u64>>; IDEM_SHARDS],
    /// Live connections per source IP for per-IP caps.
    ips: std::sync::Mutex<HashMap<std::net::IpAddr, usize>>,
    /// Bounded recent-write log per table for `Subscribe` polls.
    changes: RwLock<HashMap<String, std::sync::Arc<std::sync::Mutex<VecDeque<ChangeRecord>>>>>,
    change_seq: std::sync::atomic::AtomicU64,
    /// Sticky: set on first `Subscribe`. `record_change` early-returns while
    /// false, so write-only workloads (benches, cache shards) pay zero
    /// change-log cost (no clock read, no map lock).
    changes_used: AtomicBool,
    /// Push subscribers per table: (sub_id, sender). Bounded 64-deep
    /// channels; slow consumers are dropped + counted, never blocking writers.
    push_hub: RwLock<HashMap<String, Vec<(u64, tokio::sync::mpsc::Sender<ChangeRecord>)>>>,
    push_used: AtomicBool,
    push_dropped: std::sync::atomic::AtomicU64,
    push_delivered: std::sync::atomic::AtomicU64,
    /// Exact-term search postings: term → [(table, row_id)] (cap 128/term).
    search_index: RwLock<HashMap<String, std::sync::Arc<std::sync::Mutex<VecDeque<(String, u64)>>>>>,
    /// Async fanout ingress (set by `install_fanout_channel`; None = off).
    fanout_tx: RwLock<Option<FanoutSender>>,
    fanout_done: std::sync::atomic::AtomicU64,
    fanout_dropped: std::sync::atomic::AtomicU64,
    /// Named server-side procedures (`Op::Call` executes these
    /// transactionally). Versioned per name (monotonic u64, server-assigned
    /// on deploy); `register_procedure` deploys at version 1/first-write.
    procedures: RwLock<HashMap<String, RegisteredProcedure>>,
    /// Pure-compute functions callable from procedure steps (builtins
    /// registered at startup; embedders can add more).
    functions: RwLock<blitz_runtime::FunctionRegistry>,
    /// Submitted WASM jobs by id string. Bounded (oldest terminal evicted
    /// past the cap; submit rejects when only live jobs remain).
    wasm_jobs: RwLock<HashMap<String, StoredWasmJob>>,
    /// Shared Wasmtime engine, built once on first submit (slow ~ms).
    wasm_engine: std::sync::OnceLock<Result<blitz_jobs::WasmExecutor, String>>,
}

/// One background WASM job: the record polled over TCP plus its inputs.
#[derive(Clone)]
pub struct StoredWasmJob {
    pub job: blitz_jobs::Job,
    pub wasm: Vec<u8>,
    pub input: String,
}

/// A deployed procedure with its server-assigned monotonic version.
/// Redeploys bump the version; calls always run the latest.
#[derive(Clone, Debug)]
pub struct RegisteredProcedure {
    pub proc: blitz_runtime::Procedure,
    pub version: u64,
}

/// Max retained jobs (terminal-first eviction past this).
pub const MAX_WASM_JOBS: usize = 4096;
/// Max submitted module bytes (group framing already caps frames at 1MiB).
pub const MAX_WASM_BYTES: usize = 1_048_576;

impl BlitzServer {
    pub fn new() -> Self {
        let engine = std::sync::Arc::new(InMemoryTableEngine::new());
        let tx_manager = TransactionManager::new(std::sync::Arc::clone(&engine));
        let mut functions = blitz_runtime::FunctionRegistry::new();
        blitz_runtime::function::register_builtins(&mut functions);
        Self {
            config: ServerConfig::default(),
            engine,
            tx_manager,
            event_emitter: EventEmitter::new(),
            subscription_manager: RwLock::new(SubscriptionManager::new()),
            policy_engine: PolicyEngine::new(),
            identities: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            connections: AtomicUsize::new(0),
            started_at: RwLock::new(None),
            total_requests: std::sync::atomic::AtomicU64::new(0),
            slow_responses: std::sync::atomic::AtomicU64::new(0),
            shed_drops: std::sync::atomic::AtomicU64::new(0),
            bytes_read: std::sync::atomic::AtomicU64::new(0),
            bytes_written: std::sync::atomic::AtomicU64::new(0),
            wal_dropped_full: std::sync::atomic::AtomicU64::new(0),
            wal: RwLock::new(None),
            wal_rotating: AtomicBool::new(false),
            wal_enabled: AtomicBool::new(false),
            idem: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            ips: std::sync::Mutex::new(HashMap::new()),
            changes: RwLock::new(HashMap::new()),
            change_seq: std::sync::atomic::AtomicU64::new(1),
            changes_used: AtomicBool::new(false),
            push_hub: RwLock::new(HashMap::new()),
            push_used: AtomicBool::new(false),
            push_dropped: std::sync::atomic::AtomicU64::new(0),
            push_delivered: std::sync::atomic::AtomicU64::new(0),
            search_index: RwLock::new(HashMap::new()),
            fanout_tx: RwLock::new(None),
            fanout_done: std::sync::atomic::AtomicU64::new(0),
            fanout_dropped: std::sync::atomic::AtomicU64::new(0),
            procedures: RwLock::new(HashMap::new()),
            functions: RwLock::new(functions),
            wasm_jobs: RwLock::new(HashMap::new()),
            wasm_engine: std::sync::OnceLock::new(),
        }
    }

    pub fn with_config(config: ServerConfig) -> Self {
        Self {
            config,
            ..Self::new()
        }
    }

    pub fn engine(&self) -> &InMemoryTableEngine {
        &*self.engine
    }

    pub fn tx_manager(&self) -> &TransactionManager {
        &self.tx_manager
    }

    // -- Server-side routing (stable base names, sharded storage) --------

    /// Shard count for a base table (1 = unsharded).
    pub fn shard_count(&self, base: &str) -> usize {
        self.config.table_shards.get(base).map(|s| s.shards).unwrap_or(1).max(1)
    }

    /// Physical table for (base, shard). Unsharded bases pass through;
    /// sharded ones use the bench-compatible `{base}_{shard:02}` layout.
    pub fn physical_table(&self, base: &str, shard: usize) -> String {
        if self.shard_count(base) <= 1 {
            base.to_string()
        } else {
            format!("{}_{:02}", base, shard)
        }
    }

    /// All (shard, physical) pairs for fan-out reads, in shard order.
    pub fn shard_tables(&self, base: &str) -> Vec<(usize, String)> {
        let n = self.shard_count(base);
        if n <= 1 {
            vec![(0, base.to_string())]
        } else {
            (0..n).map(|s| (s, self.physical_table(base, s))).collect()
        }
    }

    /// Base table owning a physical name: reverses `{base}_{NN}` for
    /// configured sharded bases, else identity. Used where only the
    /// physical name survives (search postings).
    pub fn base_of_physical(&self, physical: &str) -> String {
        if let Some((base, _)) = physical.rsplit_once('_') {
            if let Some(spec) = self.config.table_shards.get(base) {
                if spec.shards > 1 {
                    if physical.len() > base.len() + 1
                        && physical[base.len() + 1..].chars().all(|c| c.is_ascii_digit())
                    {
                        return base.to_string();
                    }
                }
            }
        }
        physical.to_string()
    }

    /// Shard owning a physical table name (parses the `_{NN}` suffix when
    /// the base is sharded; 0 otherwise).
    pub fn shard_of_physical(&self, base: &str, physical: &str) -> usize {        if self.shard_count(base) <= 1 || physical == base {
            return 0;
        }
        physical
            .rsplit_once('_')
            .and_then(|(_, s)| s.parse::<usize>().ok())
            .unwrap_or(0)
    }

    /// Compose a global RowId from shard + engine-local id.
    pub fn compose_id(shard: usize, local: u64) -> u64 {
        (((shard as u64) & 0xFF) << SHARD_SHIFT) | (local & LOCAL_MASK)
    }

    /// Split a global RowId into (shard, local).
    pub fn split_id(id: u64) -> (usize, u64) {
        ((id >> SHARD_SHIFT) as usize, id & LOCAL_MASK)
    }

    /// Route an insert: hash `values[shard_column] % N`. Missing key hashes
    /// as Null (deterministic single shard, documented — rows without a
    /// shard key don't scatter).
    pub fn route_insert(&self, base: &str, values: &HashMap<String, Value>) -> (String, usize) {
        let n = self.shard_count(base);
        if n <= 1 {
            return (base.to_string(), 0);
        }
        let shard = match self.config.table_shards.get(base) {
            Some(spec) => (shard_hash(values.get(&spec.column).unwrap_or(&Value::Null)) % n as u64) as usize,
            None => 0,
        };
        (self.physical_table(base, shard), shard)
    }

    /// Route a point op by global RowId high bits. Legacy high-bits-0 ids
    /// land on shard 0 (configure sharding before data lands).
    pub fn route_id(&self, base: &str, id: u64) -> (String, u64) {
        let n = self.shard_count(base);
        if n <= 1 {
            return (base.to_string(), id);
        }
        let (shard, local) = Self::split_id(id);
        (self.physical_table(base, shard % n), local)
    }

    /// Register a server-side procedure for `Op::Call` (version 1 on
    /// first deploy, monotonic bump on redeploy). In-process path used by
    /// embedders/tests; TCP deploy validates identically.
    pub fn register_procedure(&self, proc: blitz_runtime::Procedure) -> u64 {
        self.deploy_procedure(proc).unwrap_or(0)
    }

    /// Validate + store a procedure, returning its assigned version.
    /// Same-name redeploys bump (calls always run the latest).
    pub fn deploy_procedure(&self, proc: blitz_runtime::Procedure) -> Result<u64, String> {
        let functions = self.functions.read().map_err(|e| format!("function registry locked: {}", e))?;
        blitz_runtime::validate_procedure(&proc, &functions)?;
        drop(functions);
        let mut procs = self.procedures.write().map_err(|e| format!("procedure registry locked: {}", e))?;
        let version = procs.get(&proc.name).map(|r| r.version + 1).unwrap_or(1);
        let name = proc.name.clone();
        procs.insert(name, RegisteredProcedure { proc, version });
        Ok(version)
    }

    /// Drop a deployed procedure. False when absent.
    pub fn drop_procedure(&self, name: &str) -> bool {
        self.procedures.write().ok().map(|mut g| g.remove(name).is_some()).unwrap_or(false)
    }

    /// Fetch a registered procedure by name (latest version).
    pub fn get_procedure(&self, name: &str) -> Option<blitz_runtime::Procedure> {
        self.procedures.read().ok()?.get(name).map(|r| r.proc.clone())
    }

    /// List (name, description, version, step count), sorted by name.
    pub fn list_procedures(&self) -> Vec<(String, String, u64, usize)> {
        let mut out: Vec<(String, String, u64, usize)> = self
            .procedures
            .read()
            .map(|g| {
                g.iter()
                    .map(|(n, r)| (n.clone(), r.proc.description.clone(), r.version, r.proc.steps.len()))
                    .collect()
            })
            .unwrap_or_default();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Borrow the function registry (deploy validation reads it).
    pub fn functions(&self) -> &std::sync::RwLock<blitz_runtime::FunctionRegistry> {
        &self.functions
    }

    /// Call a pure-compute function (procedure steps delegate here).
    pub fn call_function(
        &self,
        name: &str,
        args: HashMap<String, Value>,
    ) -> Result<Value, String> {
        let g = self.functions.read().map_err(|e| format!("function registry locked: {}", e))?;
        g.execute(name, args).map_err(|e| e.to_string())
    }

    // -- Background WASM jobs (submit/poll over TCP) -------------------

    /// Shared executor, built once (slow) on first submit.
    pub fn wasm_executor(&self) -> Result<blitz_jobs::WasmExecutor, String> {
        self.wasm_engine
            .get_or_init(|| {
                blitz_jobs::WasmExecutor::new().map_err(|e| format!("wasm engine: {}", e))
            })
            .clone()
    }

    /// Store a job as Pending and hand back its id. The caller (which holds
    /// `Arc<Self>`) spawns `run_wasm_job` on the blocking pool. Bounded:
    /// evicts the oldest terminal job past the cap; rejects when only live
    /// jobs remain (honest backpressure, retryable).
    pub fn store_wasm_job(&self, mut job: blitz_jobs::Job, wasm: Vec<u8>, input: String) -> Result<String, String> {
        if wasm.len() > MAX_WASM_BYTES {
            return Err(format!("wasm too large (max {} bytes)", MAX_WASM_BYTES));
        }
        let id = job.id.to_string();
        let mut jobs = self.wasm_jobs.write().map_err(|e| format!("job store locked: {}", e))?;
        if jobs.len() >= MAX_WASM_JOBS {
            // Oldest terminal first.
            let mut terminal: Vec<(chrono::DateTime<chrono::Utc>, String)> = jobs
                .iter()
                .filter(|(_, s)| matches!(s.job.status, blitz_jobs::JobStatus::Completed | blitz_jobs::JobStatus::Failed | blitz_jobs::JobStatus::Cancelled))
                .filter_map(|(k, s)| s.job.completed_at.map(|t| (t, k.clone())))
                .collect();
            terminal.sort();
            if let Some((_, oldest)) = terminal.into_iter().next() {
                jobs.remove(&oldest);
            } else {
                return Err("job queue full (all live; poll and retry)".to_string());
            }
        }
        job.payload.insert("input_len".into(), input.len().to_string());
        jobs.insert(id.clone(), StoredWasmJob { job, wasm, input });
        Ok(id)
    }

    /// Execute a stored job to terminal state (blocking-pool body): loads,
    /// marks running, runs with retries, stores back. Returns the final job.
    pub fn run_wasm_job(&self, id: &str) -> Option<blitz_jobs::Job> {
        let (mut job, wasm, input) = {
            let mut jobs = self.wasm_jobs.write().ok()?;
            let stored = jobs.get_mut(id)?;
            stored.job.mark_running();
            (stored.job.clone(), stored.wasm.clone(), stored.input.clone())
        };
        let executor = match self.wasm_executor() {
            Ok(ex) => ex,
            Err(e) => {
                job.mark_failed(format!("engine: {}", e));
                if let Ok(mut jobs) = self.wasm_jobs.write() {
                    if let Some(s) = jobs.get_mut(id) {
                        s.job = job.clone();
                    }
                }
                return Some(job);
            }
        };
        // Guest failures retry per the job model (transient-friendly);
        // deterministic traps burn retries fast (documented).
        loop {
            match blitz_jobs::run_job(&executor, &mut job, &wasm, &input) {
                Ok(()) => break,
                Err(_) if job.can_retry() => continue,
                Err(_) => break,
            }
        }
        if let Ok(mut jobs) = self.wasm_jobs.write() {
            if let Some(s) = jobs.get_mut(id) {
                s.job = job.clone();
            }
        }
        Some(job)
    }

    /// Fetch a job record for polling (clone under one read lock).
    pub fn get_wasm_job(&self, id: &str) -> Option<blitz_jobs::Job> {
        self.wasm_jobs.read().ok()?.get(id).map(|s| s.job.clone())
    }

    pub fn event_emitter(&self) -> &EventEmitter {
        &self.event_emitter
    }

    pub fn subscription_manager(&self) -> &RwLock<SubscriptionManager> {
        &self.subscription_manager
    }

    pub fn policy_engine(&self) -> &PolicyEngine {
        &self.policy_engine
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Current number of live connections.
    pub fn connection_count(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }

    /// Get maximum connection count from config.
    pub fn max_connections(&self) -> usize {
        self.config.max_connections
    }

    /// Try to admit one connection. Returns `false` when at capacity.
    pub fn try_acquire_connection(&self) -> bool {
        let mut current = self.connections.load(Ordering::Relaxed);
        loop {
            if current >= self.config.max_connections {
                return false;
            }
            match self.connections.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Release one previously acquired connection slot.
    /// Saturating: a double-release (bug) can never underflow to
    /// `usize::MAX` and wedge admission forever.
    pub fn release_connection(&self) {
        // CAS loop with saturation: fetch_sub would wrap on double-release.
        let mut current = self.connections.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return;
            }
            match self.connections.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record one dispatched request. Called by the transport hot path;
    /// `slow` marks end-to-end handling past `slow_threshold_ms`.
    pub fn record_request(&self, slow: bool) {
        use std::sync::atomic::Ordering as O;
        self.total_requests.fetch_add(1, O::Relaxed);
        if slow {
            self.slow_responses.fetch_add(1, O::Relaxed);
        }
    }

    /// Whether to shed *new* connections fast instead of queueing.
    /// Disabled (`None`) by default; set `shed_at_connections` in prod.
    pub fn should_shed(&self) -> bool {
        match self.config.shed_at_connections {
            Some(t) => self.connection_count() >= t,
            None => false,
        }
    }

    pub fn record_shed_drop(&self) {
        use std::sync::atomic::Ordering as O;
        self.shed_drops.fetch_add(1, O::Relaxed);
    }

    pub fn record_io(&self, read_bytes: u64, written_bytes: u64) {
        use std::sync::atomic::Ordering as O;
        if read_bytes > 0 {
            self.bytes_read.fetch_add(read_bytes, O::Relaxed);
        }
        if written_bytes > 0 {
            self.bytes_written.fetch_add(written_bytes, O::Relaxed);
        }
    }

    pub fn stats(&self) -> ServerStats {
        use std::sync::atomic::Ordering as O;
        let (wal_bytes, wal_ops) = match self.wal.read().ok().and_then(|g| (*g).clone()) {
            Some(w) => (w.bytes_flushed(), w.ops_flushed()),
            None => (0, 0),
        };
        ServerStats {
            connections: self.connection_count(),
            total_requests: self.total_requests.load(O::Relaxed),
            slow_responses: self.slow_responses.load(O::Relaxed),
            shed_drops: self.shed_drops.load(O::Relaxed),
            bytes_read: self.bytes_read.load(O::Relaxed),
            bytes_written: self.bytes_written.load(O::Relaxed),
            active_tx: 0,
            tx_conflicts: 0,
            subscription_fanout: self.push_delivered.load(O::Relaxed),
            push_dropped: self.push_dropped.load(O::Relaxed),
            fanout_done: self.fanout_done.load(O::Relaxed),
            fanout_dropped: self.fanout_dropped.load(O::Relaxed),
            wal_bytes,
            wal_ops,
        }
    }

    /// Slow-response rate in [0,1] for alerting. `None` when no traffic.
    pub fn slow_rate(&self) -> Option<f64> {
        use std::sync::atomic::Ordering as O;
        let total = self.total_requests.load(O::Relaxed);
        if total == 0 {
            return None;
        }
        Some(self.slow_responses.load(O::Relaxed) as f64 / total as f64)
    }

    /// Append a WAL record (non-blocking sharded group-commit). Ok(()) in
    /// `None` mode or when queued; Err backpressure message when the target
    /// shard group is full or a rotation is in progress (caller must fail
    /// the request, not ack unwritten data).
    pub fn wal_log(
        &self,
        entry: blitz_wal::EntryType,
        table: &str,
        row_id: u64,
        data: Vec<u8>,
    ) -> Result<(), String> {
        use std::sync::atomic::Ordering as O;
        // Fast path: single atomic load, no lock, no clone. All benches and
        // cache shards run here.
        if !self.wal_enabled.load(O::Relaxed) {
            return Ok(());
        }
        if self.wal_rotating.load(O::Relaxed) {
            self.wal_dropped_full.fetch_add(1, O::Relaxed);
            return Err("WAL rotation in progress".to_string());
        }
        let cluster = match self.wal.read().ok().and_then(|g| (*g).clone()) {
            Some(c) => c,
            None => return Ok(()),
        };
        if !cluster.is_enabled() {
            return Ok(());
        }
        cluster
            .append(table, entry, row_id, data)
            .map_err(|e| {
                self.wal_dropped_full.fetch_add(1, O::Relaxed);
                e
            })
    }

    pub fn wal_dropped(&self) -> u64 {
        use std::sync::atomic::Ordering as O;
        self.wal_dropped_full.load(O::Relaxed)
    }

    /// Save a snapshot of all tables (durable modes; no-op without data_dir).
    pub fn save_snapshot(&self) -> Result<Option<std::path::PathBuf>> {
        match &self.config.data_dir {
            Some(dir) => Ok(Some(crate::durability::save_snapshot(&self.engine, dir)?)),
            None => Ok(None),
        }
    }

    /// Snapshot + WAL rotation (bounds replay time). Quiesces appends
    /// briefly: concurrent writes shed with backpressure during the window
    /// (fail-fast, retryable via idempotency keys) instead of risking
    /// truncate races. Returns snapshot path.
    pub fn snapshot_and_rotate(&self) -> Result<std::path::PathBuf> {
        use std::sync::atomic::Ordering as O;
        let dir = self.config.data_dir.clone().ok_or_else(|| anyhow::anyhow!("no data_dir"))?;
        // 1. Quiesce.
        self.wal_rotating.store(true, O::Relaxed);
        // 2. Drain groups.
        if let Some(c) = self.wal.read().ok().and_then(|g| (*g).clone()) {
            c.flush_all(std::time::Duration::from_secs(30));
        }
        // 3. Snapshot (engine is consistent; WAL drained).
        let snap = crate::durability::save_snapshot(&self.engine, &dir)?;
        // 4. Swap cluster: drop old senders (threads exit), delete shard
        // files, open fresh. New appends after this get post-snapshot seqs.
        {
            let old = self.wal.write().unwrap().take();
            drop(old);
        }
        for i in 0..crate::durability::WalCluster::shards_for_mode() {
            let p = std::path::PathBuf::from(&dir).join(format!("wal_{:02}.log", i));
            let _ = std::fs::remove_file(&p);
        }
        let _ = std::fs::remove_file(std::path::PathBuf::from(&dir).join("wal.log"));
        if self.config.durability.is_durable() {
            let cluster = crate::durability::WalCluster::open(&dir, self.config.durability)?;
            if cluster.is_enabled() {
                *self.wal.write().unwrap() = Some(cluster);
                self.wal_enabled.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
        self.wal_rotating.store(false, O::Relaxed);
        // Prune old snapshots, keep 3.
        let mgr = blitz_snapshot::SnapshotManager::new(
            std::path::PathBuf::from(&dir).join("snapshots"),
        );
        let _ = mgr.prune(3);
        Ok(snap)
    }

    /// Idempotency: lookup cached RowId for a client `_idem` key.
    pub fn idem_lookup(&self, key: &str) -> Option<u64> {
        self.idem[idem_shard(key)].read().ok().and_then(|g| g.get(key).copied())
    }

    /// Record `_idem` key → RowId. Bounded at 256K total (owning shard
    /// clears half past 16K).
    pub fn idem_record(&self, key: String, row_id: u64) {
        let shard = idem_shard(&key);
        if let Ok(mut g) = self.idem[shard].write() {
            if g.len() >= 16_000 {
                let drop_n = g.len() / 2;
                let keys: Vec<String> = g.keys().take(drop_n).cloned().collect();
                for k in keys {
                    g.remove(&k);
                }
            }
            g.insert(key, row_id);
        }
    }

    /// Admit one IP slot. False when per-IP cap hit (shed).
    pub fn ip_acquire(&self, ip: std::net::IpAddr) -> bool {
        let cap = self.config.max_connections_per_ip;
        if cap == 0 {
            return true;
        }
        match self.ips.lock() {
            Ok(mut m) => {
                let n = m.get(&ip).copied().unwrap_or(0);
                if n >= cap {
                    return false;
                }
                m.insert(ip, n + 1);
                true
            }
            Err(_) => true,
        }
    }

    pub fn ip_release(&self, ip: std::net::IpAddr) {
        if let Ok(mut m) = self.ips.lock() {
            if let Some(n) = m.get(&ip).copied() {
                if n <= 1 {
                    m.remove(&ip);
                } else {
                    m.insert(ip, n - 1);
                }
            }
        }
    }

    /// Resolve a bearer token to an identity. Short-lived sessions first
    /// (expiry enforced; expired entries evicted on encounter), then the
    /// long-lived pre-shared / registered identities. One map lookup per
    /// connection handshake — far under the 1ms auth/policy budget.
    pub fn resolve_token(&self, token: &str) -> Option<Identity> {
        if let Ok(mut s) = self.sessions.write() {
            if let Some(sess) = s.get(token) {
                if sess.is_expired() {
                    s.remove(token);
                } else {
                    return Some(sess.identity.clone());
                }
            }
        }
        self.identities.read().ok().and_then(|g| g.get(token).cloned())
    }

    /// Create a short-lived session token (`ttl_secs` from now). Returns the
    /// token to hand to the client (bearer for `_auth` handshakes).
    pub fn register_session(&self, token: String, identity: Identity, ttl_secs: u64) {
        let sess = Session {
            id: uuid::Uuid::new_v4(),
            identity,
            created_at: chrono::Utc::now(),
            expires_at: chrono::Utc::now() + chrono::Duration::seconds(ttl_secs as i64),
        };
        if let Ok(mut s) = self.sessions.write() {
            s.insert(token, sess);
        }
    }

    /// Authorize one op for an (optionally authenticated) connection.
    /// `Ping` always passes (handshake + probes carry no data).
    /// Bypass when `!require_auth` (bench default). Otherwise: direct
    /// identity permission (or admin) wins; then policy allow-rules; deny
    /// by default with zero rules (secure closed default).
    pub fn authorize(
        &self,
        ident: &Option<Identity>,
        op: Op,
        table: &str,
    ) -> Result<(), &'static str> {
        if matches!(op, Op::Ping) {
            return Ok(());
        }
        if !self.config.require_auth {
            return Ok(());
        }
        let perm: Permission = match op {
            Op::Insert | Op::Update => Permission::Write,
            Op::Get | Op::Scan | Op::Find | Op::Subscribe | Op::Search => Permission::Read,
            Op::Delete => Permission::Delete,
            Op::Call => Permission::Custom("call".to_string()),
            Op::JobSubmit => Permission::Custom("job.submit".to_string()),
            Op::JobPoll => Permission::Custom("job.poll".to_string()),
            Op::ProcDeploy => Permission::Custom("proc.deploy".to_string()),
            Op::ProcList => Permission::Custom("proc.list".to_string()),
            Op::ProcDrop => Permission::Custom("proc.drop".to_string()),
            Op::Ping => return Ok(()),
        };
        let id = ident.as_ref().ok_or("unauthorized: authentication required")?;
        if id.has_permission(&perm) {
            return Ok(());
        }
        match self.policy_engine.check(id, table, &perm) {
            Ok(true) => Ok(()),
            _ => Err("forbidden: policy denies"),
        }
    }

    /// Owner column for a table, if row-level ownership is configured.
    pub fn owner_column(&self, table: &str) -> Option<String> {
        self.config.row_owner.get(table).cloned()
    }

    /// Row-level check after table-level [`authorize`] passes. No-op when
    /// the table has no owner column, when auth is bypassed, or for admin
    /// role holders. Otherwise the row's owner value must be the string
    /// subject of the caller. `Ping`/`Call(fn:*)` never reach here with a
    /// row context (Call steps check per touched table instead).
    pub fn authorize_row(
        &self,
        ident: &Option<Identity>,
        op: Op,
        table: &str,
        owner: Option<&Value>,
    ) -> Result<(), &'static str> {
        self.authorize(ident, op, table)?;
        if !self.config.require_auth {
            return Ok(());
        }
        if !self.config.row_owner.contains_key(table) {
            return Ok(());
        }
        let id = ident.as_ref().ok_or("unauthorized: authentication required")?;
        if id.has_role("admin") {
            return Ok(());
        }
        match owner {
            Some(Value::String(s)) if s == &id.subject => Ok(()),
            _ => Err("forbidden: row owner mismatch"),
        }
    }

    /// Record a committed write for `Subscribe` polls. Bounded 128/table;
    /// per-table deque locks (tables don't contend). Called only for acked
    /// writes — denied/shed ops never appear. Zero-cost until the first
    /// `Subscribe` arrives (sticky flag).
    pub fn record_change(&self, table: &str, op: &'static str, row_id: u64) {
        use std::sync::atomic::Ordering as O;
        if !self.changes_used.load(O::Relaxed) {
            return;
        }
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let seq = self.change_seq.fetch_add(1, O::Relaxed);
        let rec = ChangeRecord { seq, table: table.to_string(), op, row_id, ts_micros: ts };
        let deque = {
            match self.changes.read().ok().and_then(|m| m.get(table).cloned()) {
                Some(d) => d,
                None => {
                    let d = std::sync::Arc::new(std::sync::Mutex::new(VecDeque::new()));
                    if let Ok(mut m) = self.changes.write() {
                        m.entry(table.to_string()).or_insert_with(|| std::sync::Arc::clone(&d));
                    }
                    d
                }
            }
        };
        if let Ok(mut q) = deque.lock() {
            q.push_back(rec.clone());
            while q.len() > MAX_CHANGES_PER_TABLE {
                q.pop_front();
            }
        };
        // Push fanout to stream subscribers (bounded; evict slow readers).
        if self.push_used.load(O::Relaxed) {
            let targets: Vec<(u64, tokio::sync::mpsc::Sender<ChangeRecord>)> = self
                .push_hub
                .read()
                .ok()
                .and_then(|h| h.get(table).cloned())
                .unwrap_or_default();
            if !targets.is_empty() {
                let mut dead = Vec::new();
                let mut delivered = 0u64;
                for (id, tx) in &targets {
                    if tx.try_send(rec.clone()).is_err() {
                        dead.push(*id);
                    } else {
                        delivered += 1;
                    }
                }
                if delivered > 0 {
                    self.push_delivered.fetch_add(delivered, O::Relaxed);
                }
                if !dead.is_empty() {
                    self.push_dropped.fetch_add(dead.len() as u64, O::Relaxed);
                    if let Ok(mut hub) = self.push_hub.write() {
                        if let Some(v) = hub.get_mut(table) {
                            v.retain(|(id, _)| !dead.contains(id));
                        }
                    }
                }
            }
        }
    }

    /// Read changes for `table` with `ts_micros > since`, oldest first,
    /// capped at `limit` (clamped to 1000). Arms the change log.
    pub fn read_changes(&self, table: &str, since: u64, limit: usize) -> Vec<ChangeRecord> {
        use std::sync::atomic::Ordering as O;
        self.changes_used.store(true, O::Relaxed);
        let limit = limit.clamp(1, 1000);
        match self.changes.read().ok().and_then(|m| m.get(table).cloned()) {
            Some(d) => match d.lock() {
                Ok(q) => q.iter().filter(|r| r.ts_micros > since).take(limit).cloned().collect(),
                Err(_) => Vec::new(),
            },
            None => Vec::new(),
        }
    }

    /// Index a post body for exact-term `Search` (call after acked insert
    /// into a `posts*` table). Tokenizes ≤8 terms; postings capped 128/term.
    pub fn index_post(&self, table: &str, row_id: u64, body: &str) {
        for term in crate::social::tokenize(body).into_iter().take(8) {
            let deque = {
                match self.search_index.read().ok().and_then(|m| m.get(&term).cloned()) {
                    Some(d) => d,
                    None => {
                        let d = std::sync::Arc::new(std::sync::Mutex::new(VecDeque::new()));
                        if let Ok(mut m) = self.search_index.write() {
                            m.entry(term.clone()).or_insert_with(|| std::sync::Arc::clone(&d));
                        }
                        d
                    }
                }
            };
            if let Ok(mut q) = deque.lock() {
                // Refresh recency: drop existing same posting first.
                q.retain(|(t, r)| !(t == table && *r == row_id));
                q.push_back((table.to_string(), row_id));
                while q.len() > 128 {
                    q.pop_front();
                }
            };
        }
    }

    /// Exact-term search: postings of the first query term, ANDed with the
    /// second when present. Returns up to `limit` (table, row_id) newest-last.
    pub fn search_posts(&self, query: &str, limit: usize) -> Vec<(String, u64)> {
        let limit = limit.clamp(1, 100).max(1);
        let terms = crate::social::tokenize(query);
        if terms.is_empty() {
            return Vec::new();
        }
        let postings = |t: &str| -> Vec<(String, u64)> {
            self.search_index
                .read()
                .ok()
                .and_then(|m| m.get(t).cloned())
                .and_then(|d| d.lock().ok().map(|q| q.iter().cloned().collect()))
                .unwrap_or_default()
        };
        let mut hits = postings(&terms[0]);
        if terms.len() > 1 {
            use std::collections::HashSet;
            let second: HashSet<(String, u64)> = postings(&terms[1]).into_iter().collect();
            hits.retain(|h| second.contains(h));
        }
        // Newest-last insertion order; take the tail.
        if hits.len() > limit {
            hits[hits.len() - limit..].to_vec()
        } else {
            hits
        }
    }

    /// Install the fanout ingress channel (called once by whoever spawns
    /// `social::run_fanout_loop`). Dispatch enqueues post jobs best-effort.
    pub fn install_fanout_channel(&self, sender: FanoutSender) {
        if let Ok(mut g) = self.fanout_tx.write() {
            *g = Some(sender);
        }
    }

    /// Enqueue a post for async fanout. Fire-and-forget: full queue drops +
    /// counts (reader falls back to pull). ~50ns when worker installed, one
    /// lock read + try_send when not (None → immediate Ok).
    pub fn fanout_enqueue(&self, table: String, row_id: u64, author: String) {
        use std::sync::atomic::Ordering as O;
        let sender = match self.fanout_tx.read().ok().and_then(|g| (*g).clone()) {
            Some(s) => s,
            None => return,
        };
        if sender
            .tx
            .try_send(FanoutJob { table, row_id, author })
            .is_err()
        {
            self.fanout_dropped.fetch_add(1, O::Relaxed);
        }
    }

    pub fn fanout_record_done(&self, n: u64) {
        use std::sync::atomic::Ordering as O;
        if n > 0 {
            self.fanout_done.fetch_add(n, O::Relaxed);
        }
    }

    pub fn fanout_pending_approx(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering as O;
        (self.fanout_done.load(O::Relaxed), self.fanout_dropped.load(O::Relaxed))
    }

    /// Register a push subscriber for `table`. Returns sub id + receiver.
    /// Arms realtime (change-log + hub). Bounded 64-deep; slow consumers are
    /// evicted on next publish, never blocking writers.
    pub fn push_subscribe(&self, table: &str) -> (u64, tokio::sync::mpsc::Receiver<ChangeRecord>) {
        use std::sync::atomic::Ordering as O;
        self.changes_used.store(true, O::Relaxed);
        self.push_used.store(true, O::Relaxed);
        let sub = self.change_seq.fetch_add(1, O::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        if let Ok(mut hub) = self.push_hub.write() {
            hub.entry(table.to_string()).or_default().push((sub, tx));
        }
        (sub, rx)
    }

    pub fn push_unsubscribe(&self, table: &str, sub: u64) {
        if let Ok(mut hub) = self.push_hub.write() {
            if let Some(v) = hub.get_mut(table) {
                v.retain(|(id, _)| *id != sub);
            }
        }
    }

    /// Prometheus exposition for the SLO contract fields.
    pub fn metrics_text(&self) -> String {
        let s = self.stats();
        let slow = self.slow_rate().unwrap_or(0.0);
        let uptime = self.uptime_secs().unwrap_or(0.0);
        format!(
            "# HELP blitz_connections live connections\n# TYPE blitz_connections gauge\nblitz_connections {}\n\
             # HELP blitz_requests_total dispatched frames\n# TYPE blitz_requests_total counter\nblitz_requests_total {}\n\
             # HELP blitz_slow_responses_total frames past slow_threshold\n# TYPE blitz_slow_responses_total counter\nblitz_slow_responses_total {}\n\
             # HELP blitz_slow_rate slow/total\n# TYPE blitz_slow_rate gauge\nblitz_slow_rate {:.4}\n\
             # HELP blitz_shed_drops_total shed at admission\n# TYPE blitz_shed_drops_total counter\nblitz_shed_drops_total {}\n\
             # HELP blitz_wal_dropped_total WAL backpressure drops\n# TYPE blitz_wal_dropped_total counter\nblitz_wal_dropped_total {}\n\
             # HELP blitz_bytes_read_total TCP bytes read\n# TYPE blitz_bytes_read_total counter\nblitz_bytes_read_total {}\n\
             # HELP blitz_bytes_written_total TCP bytes written\n# TYPE blitz_bytes_written_total counter\nblitz_bytes_written_total {}\n\
             # HELP blitz_wal_bytes_total WAL bytes fsynced\n# TYPE blitz_wal_bytes_total counter\nblitz_wal_bytes_total {}\n\
             # HELP blitz_wal_ops_total WAL ops fsynced\n# TYPE blitz_wal_ops_total counter\nblitz_wal_ops_total {}\n\
             # HELP blitz_active_tx active transactions (0: auto-commit TCP)\n# TYPE blitz_active_tx gauge\nblitz_active_tx {}\n\
             # HELP blitz_push_delivered_total stream pushes delivered\n# TYPE blitz_push_delivered_total counter\nblitz_push_delivered_total {}\n\
             # HELP blitz_push_dropped_total slow push consumers evicted\n# TYPE blitz_push_dropped_total counter\nblitz_push_dropped_total {}\n\
             # HELP blitz_fanout_done_total timeline rows materialized\n# TYPE blitz_fanout_done_total counter\nblitz_fanout_done_total {}\n\
             # HELP blitz_fanout_dropped_total fanout jobs shed under pressure\n# TYPE blitz_fanout_dropped_total counter\nblitz_fanout_dropped_total {}\n\
             # HELP blitz_uptime_seconds server uptime\n# TYPE blitz_uptime_seconds gauge\nblitz_uptime_seconds {:.1}\n",
            s.connections, s.total_requests, s.slow_responses, slow,
            s.shed_drops, self.wal_dropped(),
            s.bytes_read, s.bytes_written, s.wal_bytes, s.wal_ops,
            s.active_tx, s.subscription_fanout, s.push_dropped,
            s.fanout_done, s.fanout_dropped, uptime,
        )
    }

    /// Uptime in seconds.
    pub fn uptime_secs(&self) -> Option<f64> {
        self.started_at.read().ok().and_then(|guard| {
            guard.map(|t| (chrono::Utc::now() - t).num_milliseconds() as f64 / 1000.0)
        })
    }

    /// Start the server with default schemas.
    /// When `data_dir` + durable mode are configured, replays snapshot + WAL
    /// before creating default schemas (existing tables win), then opens the
    /// group-commit bridge for new writes.
    pub async fn start(&self) -> Result<()> {
        tracing::info!(
            "BlitzDB server starting on {}:{}",
            self.config.host,
            self.config.port
        );

        // Crash recovery first so default schemas don't shadow restored ones
        // with incompatible (strict vs inferred) definitions.
        if let Some(dir) = self.config.data_dir.clone() {
            if self.config.durability.is_durable() {
                match crate::durability::recover(&self.engine, &dir) {
                    Ok((tables, rows, replayed)) => tracing::info!(
                        "recovery: tables={} rows={} wal_replayed={} dir={}",
                        tables, rows, replayed, dir
                    ),
                    Err(e) => tracing::warn!("recovery failed (starting empty): {:#}", e),
                }
                let cluster = crate::durability::WalCluster::open(
                    &dir,
                    self.config.durability,
                )?;
                if cluster.is_enabled() {
                    *self.wal.write().unwrap() = Some(cluster);
                    self.wal_enabled.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        // Seed pre-shared bearer tokens (rotation without restart via
        // `register_identity`).
        if !self.config.auth_tokens.is_empty() {
            if let Ok(mut m) = self.identities.write() {
                for (tok, ident) in &self.config.auth_tokens {
                    m.insert(tok.clone(), ident.clone());
                }
            }
        }

        let users_schema = TableSchema::new("users")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("name", ColumnType::String))
            .with_column(ColumnDef::new("email", ColumnType::String).unique());

        let sessions_schema = TableSchema::new("sessions")
            .with_column(ColumnDef::new("id", ColumnType::String).primary_key())
            .with_column(ColumnDef::new("user_id", ColumnType::Int64))
            .with_column(ColumnDef::new("token", ColumnType::String));

        // Tolerate restarts within one process (e.g. tests): creating an
        // existing table is a no-op success path here.
        for schema in [users_schema, sessions_schema] {
            match self.engine.create_table(schema) {
                Ok(()) => {}
                Err(blitz_core::CoreError::TableAlreadyExists(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }

        self.event_emitter.emit(
            Event::new(EventKind::Custom("server.started".into())).with_table("system"),
        );

        // Derived-state repair: the search index is memory-only, so a restart
        // (or crash recovery) would silently un-index every old post. Rebuild
        // it from `posts*` tables before serving. Offline at boot (not on the
        // request path); caps re-apply naturally (recency approximate).
        let indexed = self.rebuild_search_index();
        tracing::info!("search index rebuilt: {} post bodies", indexed);

        *self.started_at.write().unwrap() = Some(chrono::Utc::now());
        tracing::info!("BlitzDB server started successfully");
        Ok(())
    }

    /// Re-tokenize every `posts*` row body into the search index. Returns the
    /// number of bodies indexed. Idempotent (re-indexing refreshes postings).
    pub fn rebuild_search_index(&self) -> usize {        let mut tables: Vec<String> = self
            .engine
            .table_names()
            .into_iter()
            .filter(|t| t == "posts" || t.starts_with("posts_"))
            .collect();
        tables.sort();
        let mut n = 0usize;
        for table in tables {
            let rows = self.engine.scan_arcs(&table).unwrap_or_default();
            for row in rows {
                if let Some(Value::String(body)) = row.values.get("body") {
                    self.index_post(&table, row.id.as_u64(), body);
                    n += 1;
                }
            }
        }
        n
    }

    /// Drop all postings (test-only): simulates the memory-only index loss
    /// of a restart so tests can prove `rebuild_search_index` repairs it.
    #[cfg(test)]
    pub fn clear_search_index(&self) {
        if let Ok(mut m) = self.search_index.write() {
            m.clear();
        }
    }

    /// Insert a row and emit events + notify subscribers.
    ///
    /// Uses a zero-copy [`get_arc`](TableEngine::get_arc) read for the
    /// subscriber delta; only the delta itself is cloned.
    pub fn insert_row(
        &self,
        table: &str,
        row_id: RowId,
        data: HashMap<String, Value>,
    ) -> Result<()> {
        let mut row = Row::new(row_id);
        for (k, v) in &data {
            row.set(k.clone(), v.clone());
        }
        let assigned = self.engine.insert(table, row)?;

        // Emit event
        self.event_emitter
            .emit(Event::new(EventKind::RowInserted).with_table(table));

        // Notify subscribers (zero-copy read, clone only the delta).
        if let Ok(mut sub) = self.subscription_manager.write() {
            if let Ok(Some(new_row)) = self.engine.get_arc(table, assigned) {
                let delta = Delta::insert(assigned, new_row.as_ref().clone());
                sub.notify(Some(table), delta);
            }
        }

        Ok(())
    }

    /// Delete a row and emit events + notify subscribers.
    ///
    /// Uses [`take`](TableEngine::take): a single lookup that removes and
    /// returns the row, instead of the previous get-then-delete roundtrip.
    /// Deleting a missing row is a graceful no-op.
    pub fn delete_row(&self, table: &str, row_id: RowId) -> Result<()> {
        let old_row = match self.engine.take(table, row_id)? {
            Some(row) => row,
            None => return Ok(()),
        };

        // Emit event
        self.event_emitter
            .emit(Event::new(EventKind::RowDeleted).with_table(table));

        // Notify subscribers.
        if let Ok(mut sub) = self.subscription_manager.write() {
            let delta = Delta::delete(row_id, old_row.as_ref().clone());
            sub.notify(Some(table), delta);
        }

        Ok(())
    }

    /// Register an identity.
    pub fn register_identity(&self, id: String, identity: Identity) {
        self.identities.write().unwrap().insert(id, identity);
    }

    /// Get an identity by ID.
    pub fn get_identity(&self, id: &str) -> Option<Identity> {
        self.identities.read().unwrap().get(id).cloned()
    }
}

impl Default for BlitzServer {
    fn default() -> Self {
        Self::new()
    }
}
