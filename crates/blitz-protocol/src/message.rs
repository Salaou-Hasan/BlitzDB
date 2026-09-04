use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use blitz_types::value::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Request(Request),
    Response(Response),
    Notification(Notification),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub method: String,
    pub params: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub result: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub event: String,
    pub data: HashMap<String, Value>,
}

impl Request {
    pub fn new(id: u64, method: impl Into<String>) -> Self {
        Self {
            id,
            method: method.into(),
            params: HashMap::new(),
        }
    }

    pub fn with_param(mut self, key: impl Into<String>, value: Value) -> Self {
        self.params.insert(key.into(), value);
        self
    }
}

impl Response {
    pub fn ok(id: u64, result: Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: u64, error: impl Into<String>) -> Self {
        Self {
            id,
            result: None,
            error: Some(error.into()),
        }
    }
}
