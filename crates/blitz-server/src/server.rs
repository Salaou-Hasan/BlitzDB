use anyhow::Result;
use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_types::column::{ColumnDef, ColumnType};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;

pub struct BlitzServer {
    engine: InMemoryTableEngine,
}

impl BlitzServer {
    pub fn new() -> Self {
        Self {
            engine: InMemoryTableEngine::new(),
        }
    }

    pub fn engine(&self) -> &InMemoryTableEngine {
        &self.engine
    }

    pub fn engine_mut(&mut self) -> &mut InMemoryTableEngine {
        &mut self.engine
    }

    pub fn start(&mut self) -> Result<()> {
        tracing::info!("BlitzDB server starting");

        // Create example schema
        let schema = TableSchema::new("users")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("name", ColumnType::String))
            .with_column(ColumnDef::new("email", ColumnType::String).unique());

        self.engine.create_table(schema)?;
        tracing::info!("BlitzDB server started successfully");
        Ok(())
    }
}

impl Default for BlitzServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_startup() {
        let mut server = BlitzServer::new();
        assert!(server.start().is_ok());
        assert!(server.engine().table_exists("users"));
    }
}
