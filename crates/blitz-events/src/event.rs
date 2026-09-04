use chrono::{DateTime, Utc};
use blitz_types::value::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// Typed event kinds for database operations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// A new table was created.
    TableCreated,
    /// A table was dropped.
    TableDropped,
    /// A row was inserted.
    RowInserted,
    /// A row was updated.
    RowUpdated,
    /// A row was deleted.
    RowDeleted,
    /// A transaction was committed.
    TransactionCommitted,
    /// A transaction was rolled back.
    TransactionRolledBack,
    /// A custom event.
    Custom(String),
}

impl EventKind {
    pub fn as_str(&self) -> &str {
        match self {
            EventKind::TableCreated => "table.created",
            EventKind::TableDropped => "table.dropped",
            EventKind::RowInserted => "row.inserted",
            EventKind::RowUpdated => "row.updated",
            EventKind::RowDeleted => "row.deleted",
            EventKind::TransactionCommitted => "tx.committed",
            EventKind::TransactionRolledBack => "tx.rolled_back",
            EventKind::Custom(s) => s,
        }
    }
}

/// An event emitted by the database.
#[derive(Debug, Clone)]
pub struct Event {
    pub id: Uuid,
    pub kind: EventKind,
    pub timestamp: DateTime<Utc>,
    pub table: Option<String>,
    pub data: HashMap<String, Value>,
}

impl Event {
    /// Create a new event with the given kind.
    pub fn new(kind: EventKind) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind,
            timestamp: Utc::now(),
            table: None,
            data: HashMap::new(),
        }
    }

    /// Set the table this event relates to.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Add a data field.
    pub fn with_data(mut self, key: impl Into<String>, value: Value) -> Self {
        self.data.insert(key.into(), value);
        self
    }

    /// Get a data field.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let event = Event::new(EventKind::RowInserted)
            .with_table("users")
            .with_data("row_id", Value::Int64(1));
        assert_eq!(event.kind, EventKind::RowInserted);
        assert_eq!(event.table.as_deref(), Some("users"));
        assert_eq!(event.get("row_id"), Some(&Value::Int64(1)));
    }

    #[test]
    fn test_event_kind_as_str() {
        assert_eq!(EventKind::TableCreated.as_str(), "table.created");
        assert_eq!(EventKind::RowInserted.as_str(), "row.inserted");
        assert_eq!(EventKind::RowUpdated.as_str(), "row.updated");
        assert_eq!(EventKind::RowDeleted.as_str(), "row.deleted");
        assert_eq!(
            EventKind::Custom("my.event".into()).as_str(),
            "my.event"
        );
    }
}
