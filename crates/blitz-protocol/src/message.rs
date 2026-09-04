use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use blitz_types::value::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Request(Request),
    Response(Response),
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
