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

    pub fn column(&self) -> &str {
        &self.column
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

    pub fn range_exclusive(
        &self,
        start: &Value,
        end: &Value,
    ) -> impl Iterator<Item = (&Value, &Vec<RowId>)> {
        use std::ops::Bound;
        self.entries
            .range((Bound::Excluded(start.clone()), Bound::Excluded(end.clone())))
    }

    pub fn min(&self) -> Option<(&Value, &Vec<RowId>)> {
        self.entries.iter().next()
    }

    pub fn max(&self) -> Option<(&Value, &Vec<RowId>)> {
        self.entries.iter().next_back()
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_insert_and_lookup() {
        let mut idx = BTreeIndex::new("age");
        idx.insert(Value::Int64(25), RowId::new(1)).unwrap();
        idx.insert(Value::Int64(30), RowId::new(2)).unwrap();
        idx.insert(Value::Int64(25), RowId::new(3)).unwrap();

        let ids = idx.lookup(&Value::Int64(25));
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn test_btree_range() {
        let mut idx = BTreeIndex::new("score");
        idx.insert(Value::Int64(10), RowId::new(1)).unwrap();
        idx.insert(Value::Int64(20), RowId::new(2)).unwrap();
        idx.insert(Value::Int64(30), RowId::new(3)).unwrap();
        idx.insert(Value::Int64(40), RowId::new(4)).unwrap();
        idx.insert(Value::Int64(50), RowId::new(5)).unwrap();

        let results: Vec<(&Value, &Vec<RowId>)> = idx
            .range(&Value::Int64(20), &Value::Int64(40))
            .collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, &Value::Int64(20));
        assert_eq!(results[2].0, &Value::Int64(40));
    }

    #[test]
    fn test_btree_min_max() {
        let mut idx = BTreeIndex::new("score");
        idx.insert(Value::Int64(10), RowId::new(1)).unwrap();
        idx.insert(Value::Int64(50), RowId::new(2)).unwrap();
        idx.insert(Value::Int64(30), RowId::new(3)).unwrap();

        assert_eq!(idx.min().unwrap().0, &Value::Int64(10));
        assert_eq!(idx.max().unwrap().0, &Value::Int64(50));
    }

    #[test]
    fn test_btree_remove() {
        let mut idx = BTreeIndex::new("age");
        idx.insert(Value::Int64(25), RowId::new(1)).unwrap();
        idx.insert(Value::Int64(25), RowId::new(2)).unwrap();
        idx.remove(&Value::Int64(25), RowId::new(1));
        let ids = idx.lookup(&Value::Int64(25));
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&RowId::new(2)));
    }

    #[test]
    fn test_btree_range_exclusive() {
        let mut idx = BTreeIndex::new("score");
        idx.insert(Value::Int64(10), RowId::new(1)).unwrap();
        idx.insert(Value::Int64(20), RowId::new(2)).unwrap();
        idx.insert(Value::Int64(30), RowId::new(3)).unwrap();
        idx.insert(Value::Int64(40), RowId::new(4)).unwrap();

        let results: Vec<(&Value, &Vec<RowId>)> = idx
            .range_exclusive(&Value::Int64(10), &Value::Int64(40))
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, &Value::Int64(20));
        assert_eq!(results[1].0, &Value::Int64(30));
    }

    #[test]
    fn test_btree_string_range() {
        let mut idx = BTreeIndex::new("name");
        idx.insert(Value::String("alice".into()), RowId::new(1)).unwrap();
        idx.insert(Value::String("bob".into()), RowId::new(2)).unwrap();
        idx.insert(Value::String("charlie".into()), RowId::new(3)).unwrap();
        idx.insert(Value::String("diana".into()), RowId::new(4)).unwrap();

        let results: Vec<_> = idx
            .range(&Value::String("bob".into()), &Value::String("diana".into()))
            .collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_btree_len_and_empty() {
        let mut idx = BTreeIndex::new("age");
        assert!(idx.is_empty());
        idx.insert(Value::Int64(25), RowId::new(1)).unwrap();
        idx.insert(Value::Int64(30), RowId::new(2)).unwrap();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.key_count(), 2);
    }
}
