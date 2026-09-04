use crate::error::IndexResult;
use blitz_types::id::RowId;
use blitz_types::value::Value;
use std::collections::HashMap;

pub struct HashIndex {
    column: String,
    unique: bool,
    entries: HashMap<Value, Vec<RowId>>,
}

impl HashIndex {
    pub fn new(column: impl Into<String>, unique: bool) -> Self {
        Self {
            column: column.into(),
            unique,
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, value: Value, row_id: RowId) -> IndexResult<()> {
        if self.unique {
            if self.entries.contains_key(&value) {
                return Err(crate::error::IndexError::DuplicateKey(format!("{}", value)));
            }
            self.entries.insert(value, vec![row_id]);
        } else {
            self.entries.entry(value).or_default().push(row_id);
        }
        Ok(())
    }

    pub fn lookup(&self, value: &Value) -> Vec<RowId> {
        self.entries.get(value).cloned().unwrap_or_default()
    }

    pub fn remove(&mut self, value: &Value, row_id: RowId) {
        if let Some(ids) = self.entries.get_mut(value) {
            ids.retain(|id| *id != row_id);
            if ids.is_empty() {
                self.entries.remove(value);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
