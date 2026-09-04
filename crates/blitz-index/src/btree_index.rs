use std::collections::BTreeMap;

use crate::error::IndexResult;
use blitz_types::id::RowId;
use blitz_types::value::Value;

pub struct BTreeIndex {
    column: String,
    entries: BTreeMap<Value, Vec<RowId>>,
}

impl BTreeIndex {
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            entries: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, value: Value, row_id: RowId) -> IndexResult<()> {
        self.entries.entry(value).or_default().push(row_id);
        Ok(())
    }

    pub fn lookup(&self, value: &Value) -> Vec<RowId> {
        self.entries.get(value).cloned().unwrap_or_default()
    }

    pub fn range(
        &self,
        start: &Value,
        end: &Value,
    ) -> impl Iterator<Item = (&Value, &Vec<RowId>)> {
        self.entries.range(start.clone()..=end.clone())
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
