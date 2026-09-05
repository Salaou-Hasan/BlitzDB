use anyhow::Result;
use blitz_auth::Identity;
use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_events::{Event, EventEmitter, EventKind};
use blitz_policy::PolicyEngine;
use blitz_realtime::{Delta, SubscriptionManager};
use blitz_tx::TransactionManager;
use blitz_types::column::{ColumnDef, ColumnType};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    RwLock,
};

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: Option<String>,
    pub max_connections: usize,
    pub enable_auth: bool,
    pub max_message_size: usize,
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
        }
    }
}

/// Main BlitzDB server instance.
///
/// Fully shareable via `Arc`: the engine uses table-level locks and all
/// interior state is synchronized, so every method takes `&self` and
/// connection tasks can run concurrently.
pub struct BlitzServer {
    config: ServerConfig,
    engine: InMemoryTableEngine,
    tx_manager: TransactionManager,
    event_emitter: EventEmitter,
    subscription_manager: RwLock<SubscriptionManager>,
    policy_engine: PolicyEngine,
    identities: RwLock<HashMap<String, Identity>>,
    connections: AtomicUsize,
    started_at: RwLock<Option<chrono::DateTime<chrono::Utc>>>,
}

impl BlitzServer {
    pub fn new() -> Self {
        let engine = InMemoryTableEngine::new();
        let tx_manager = TransactionManager::new(InMemoryTableEngine::new());
        Self {
            config: ServerConfig::default(),
            engine,
            tx_manager,
            event_emitter: EventEmitter::new(),
            subscription_manager: RwLock::new(SubscriptionManager::new()),
            policy_engine: PolicyEngine::new(),
            identities: RwLock::new(HashMap::new()),
            connections: AtomicUsize::new(0),
            started_at: RwLock::new(None),
        }
    }

    pub fn with_config(config: ServerConfig) -> Self {
        Self {
            config,
            ..Self::new()
        }
    }

    pub fn engine(&self) -> &InMemoryTableEngine {
        &self.engine
    }

    pub fn engine_mut(&mut self) -> &mut InMemoryTableEngine {
        &mut self.engine
    }

    pub fn tx_manager(&self) -> &TransactionManager {
        &self.tx_manager
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
    pub fn release_connection(&self) {
        self.connections.fetch_sub(1, Ordering::AcqRel);
    }

    /// Uptime in seconds.
    pub fn uptime_secs(&self) -> Option<f64> {
        self.started_at.read().ok().and_then(|guard| {
            guard.map(|t| (chrono::Utc::now() - t).num_milliseconds() as f64 / 1000.0)
        })
    }

    /// Start the server with default schemas.
    pub async fn start(&self) -> Result<()> {
        tracing::info!(
            "BlitzDB server starting on {}:{}",
            self.config.host,
            self.config.port
        );

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

        *self.started_at.write().unwrap() = Some(chrono::Utc::now());
        tracing::info!("BlitzDB server started successfully");
        Ok(())
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
