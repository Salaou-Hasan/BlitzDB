use chrono::{DateTime, Utc};
use blitz_types::value::Value;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Event {
    pub id: Uuid,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub data: HashMap<String, Value>,
}

impl Event {
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            event_type: event_type.into(),
            timestamp: Utc::now(),
            data: HashMap::new(),
        }
    }

    pub fn with_data(mut self, key: impl Into<String>, value: Value) -> Self {
        self.data.insert(key.into(), value);
        self
    }
}
