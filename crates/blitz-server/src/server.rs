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
use std::sync::RwLock;

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: Option<String>,
    pub max_connections: usize,
    pub enable_auth: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 7420,
            data_dir: None,
            max_connections: 100,
            enable_auth: true,
        }
    }
}

/// The main BlitzDB server instance wiring all subsystems together.
pub struct BlitzServer {
    config: ServerConfig,
    engine: InMemoryTableEngine,
    tx_manager: TransactionManager,
    event_emitter: EventEmitter,
    subscription_manager: RwLock<SubscriptionManager>,
    policy_engine: PolicyEngine,
    identities: RwLock<HashMap<String, Identity>>,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
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
            started_at: None,
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

    pub fn started_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.started_at
    }

    /// Start the server with default schemas.
    pub fn start(&mut self) -> Result<()> {
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

        self.engine.create_table(users_schema)?;
        self.engine.create_table(sessions_schema)?;

        self.event_emitter.emit(
            Event::new(EventKind::Custom("server.started".into()))
                .with_table("system"),
        );

        self.started_at = Some(chrono::Utc::now());
        tracing::info!("BlitzDB server started successfully");
        Ok(())
    }

    /// Insert a row and emit events + notify subscribers.
    pub fn insert_row(
        &mut self,
        table: &str,
        row_id: RowId,
        data: HashMap<String, Value>,
    ) -> Result<()> {
        let mut row = Row::new(row_id);
        for (k, v) in &data {
            row.set(k.clone(), v.clone());
        }
        self.engine.insert(table, row)?;

        self.event_emitter.emit(
            Event::new(EventKind::RowInserted).with_table(table),
        );

        let new_row = self.engine.get(table, row_id)?.unwrap();
        let delta = Delta::insert(row_id, new_row);
        self.subscription_manager
            .write()
            .unwrap()
            .notify(Some(table), delta);

        Ok(())
    }

    /// Delete a row and emit events + notify subscribers.
    pub fn delete_row(&mut self, table: &str, row_id: RowId) -> Result<()> {
        let old_row = self.engine.get(table, row_id)?.unwrap();
        self.engine.delete(table, row_id)?;

        self.event_emitter.emit(
            Event::new(EventKind::RowDeleted).with_table(table),
        );

        let delta = Delta::delete(row_id, old_row);
        self.subscription_manager
            .write()
            .unwrap()
            .notify(Some(table), delta);

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

    /// Get uptime in seconds.
    pub fn uptime_secs(&self) -> Option<f64> {
        self.started_at
            .map(|t| (chrono::Utc::now() - t).num_milliseconds() as f64 / 1000.0)
    }

    /// Shutdown the server.
    pub fn shutdown(&mut self) {
        tracing::info!("BlitzDB server shutting down");
        self.event_emitter.emit(
            Event::new(EventKind::Custom("server.shutdown".into()))
                .with_table("system"),
        );
    }
}

impl Default for BlitzServer {
    fn default() -> Self {
        Self::new()
    }
}
