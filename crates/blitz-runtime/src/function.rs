use async_trait::async_trait;
use blitz_types::value::Value;
use std::collections::HashMap;

#[async_trait]
pub trait AppFunction: Send + Sync {
    fn name(&self) -> &str;
    async fn execute(&self, args: HashMap<String, Value>) -> Result<Value, String>;
}
