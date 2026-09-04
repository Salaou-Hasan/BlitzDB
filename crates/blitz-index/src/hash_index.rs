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

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn is_unique(&self) -> bool {
        self.unique
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

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.iter().map(|(_, ids)| ids.len()).sum()
    }

    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn keys(&self) -> Vec<&Value> {
        self.entries.keys().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_index_insert_and_lookup() {
        let mut idx = HashIndex::new("name", false);
        idx.insert(Value::String("Alice".into()), RowId::new(1)).unwrap();
        idx.insert(Value::String("Bob".into()), RowId::new(2)).unwrap();
        idx.insert(Value::String("Alice".into()), RowId::new(3)).unwrap();

        let ids = idx.lookup(&Value::String("Alice".into()));
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&RowId::new(1)));
        assert!(ids.contains(&RowId::new(3)));
    }

    #[test]
    fn test_hash_index_unique() {
        let mut idx = HashIndex::new("email", true);
        idx.insert(Value::String("a@b.com".into()), RowId::new(1)).unwrap();
        let result = idx.insert(Value::String("a@b.com".into()), RowId::new(2));
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_index_remove() {
        let mut idx = HashIndex::new("name", false);
        idx.insert(Value::String("Alice".into()), RowId::new(1)).unwrap();
        idx.insert(Value::String("Alice".into()), RowId::new(2)).unwrap();
        idx.remove(&Value::String("Alice".into()), RowId::new(1));
        let ids = idx.lookup(&Value::String("Alice".into()));
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&RowId::new(2)));
    }

    #[test]
    fn test_hash_index_remove_all() {
        let mut idx = HashIndex::new("name", false);
        idx.insert(Value::String("Alice".into()), RowId::new(1)).unwrap();
        idx.remove(&Value::String("Alice".into()), RowId::new(1));
        assert!(idx.is_empty());
    }

    #[test]
    fn test_hash_index_len() {
        let mut idx = HashIndex::new("name", false);
        idx.insert(Value::String("Alice".into()), RowId::new(1)).unwrap();
        idx.insert(Value::String("Alice".into()), RowId::new(2)).unwrap();
        idx.insert(Value::String("Bob".into()), RowId::new(3)).unwrap();
        assert_eq!(idx.len(), 3);
        assert_eq!(idx.key_count(), 2);
    }

    #[test]
    fn test_hash_index_clear() {
        let mut idx = HashIndex::new("name", false);
        idx.insert(Value::String("Alice".into()), RowId::new(1)).unwrap();
        idx.clear();
        assert!(idx.is_empty());
    }

    #[test]
    fn test_hash_index_lookup_missing() {
        let idx = HashIndex::new("name", false);
        let ids = idx.lookup(&Value::String("missing".into()));
        assert!(ids.is_empty());
    }
}
