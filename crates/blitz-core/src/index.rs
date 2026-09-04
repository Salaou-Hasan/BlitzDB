use std::collections::HashMap;

use crate::error::{CoreError, CoreResult};
use blitz_types::id::RowId;
use blitz_types::value::Value;

/// An index that maps values to row IDs.
pub struct HashIndex {
    /// Column name this index is on.
    column: String,
    /// Whether this index enforces uniqueness.
    unique: bool,
    /// The index data: value -> set of row IDs.
    entries: HashMap<Value, Vec<RowId>>,
    /// For unique indexes: value -> single row ID.
    unique_entries: HashMap<Value, RowId>,
}

impl HashIndex {
    pub fn new(column: impl Into<String>, unique: bool) -> Self {
        Self {
            column: column.into(),
            unique,
            entries: HashMap::new(),
            unique_entries: HashMap::new(),
        }
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// Insert a value -> row_id mapping.
    pub fn insert(&mut self, value: Value, row_id: RowId) -> CoreResult<()> {
        if self.unique {
            if self.unique_entries.contains_key(&value) {
                return Err(CoreError::DuplicateKey(format!(
                    "duplicate value '{}' in unique index on column '{}'",
                    value, self.column
                )));
            }
            self.unique_entries.insert(value, row_id);
        } else {
            self.entries.entry(value).or_default().push(row_id);
        }
        Ok(())
    }

    /// Remove a value -> row_id mapping.
    pub fn remove(&mut self, value: &Value, row_id: RowId) {
        if self.unique {
            self.unique_entries.remove(value);
        } else {
            if let Some(ids) = self.entries.get_mut(value) {
                ids.retain(|id| *id != row_id);
                if ids.is_empty() {
                    self.entries.remove(value);
                }
            }
        }
    }

    /// Look up row IDs by value.
    pub fn lookup(&self, value: &Value) -> Vec<RowId> {
        if self.unique {
            self.unique_entries
                .get(value)
                .map(|id| vec![*id])
                .unwrap_or_default()
        } else {
            self.entries.get(value).cloned().unwrap_or_default()
        }
    }

    /// Check if a value exists in the index.
    pub fn contains(&self, value: &Value) -> bool {
        if self.unique {
            self.unique_entries.contains_key(value)
        } else {
            self.entries.contains_key(value)
        }
    }

    /// Get the number of entries in the index.
    pub fn len(&self) -> usize {
        if self.unique {
            self.unique_entries.len()
        } else {
            self.entries.len()
        }
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_index_insert_and_lookup() {
        let mut index = HashIndex::new("email", false);

        index.insert(Value::String("alice@example.com".into()), RowId::new(1)).unwrap();
        index.insert(Value::String("bob@example.com".into()), RowId::new(2)).unwrap();

        let results = index.lookup(&Value::String("alice@example.com".into()));
        assert_eq!(results, vec![RowId::new(1)]);
    }

    #[test]
    fn test_hash_index_unique() {
        let mut index = HashIndex::new("email", true);

        index.insert(Value::String("alice@example.com".into()), RowId::new(1)).unwrap();

        let result = index.insert(
            Value::String("alice@example.com".into()),
            RowId::new(2),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_index_remove() {
        let mut index = HashIndex::new("email", false);

        index.insert(Value::String("alice@example.com".into()), RowId::new(1)).unwrap();
        index.remove(&Value::String("alice@example.com".into()), RowId::new(1));

        let results = index.lookup(&Value::String("alice@example.com".into()));
        assert!(results.is_empty());
    }
}
