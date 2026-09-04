use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::value::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use uuid::Uuid;

/// Type of change that occurred.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

/// A delta representing the difference between old and new row state.
#[derive(Debug, Clone)]
pub struct Delta {
    pub row_id: RowId,
    pub change: ChangeKind,
    pub old_row: Option<Row>,
    pub new_row: Option<Row>,
    /// Columns that changed: column_name -> (old_value, new_value).
    pub changed_columns: HashMap<String, (Option<Value>, Option<Value>)>,
}

impl Delta {
    /// Create an insert delta.
    pub fn insert(row_id: RowId, new_row: Row) -> Self {
        let changed_columns = new_row
            .iter()
            .map(|(k, v)| (k.to_string(), (None, Some(v.clone()))))
            .collect();
        Self {
            row_id,
            change: ChangeKind::Insert,
            old_row: None,
            new_row: Some(new_row),
            changed_columns,
        }
    }

    /// Create a delete delta.
    pub fn delete(row_id: RowId, old_row: Row) -> Self {
        let changed_columns = old_row
            .iter()
            .map(|(k, v)| (k.to_string(), (Some(v.clone()), None)))
            .collect();
        Self {
            row_id,
            change: ChangeKind::Delete,
            old_row: Some(old_row),
            new_row: None,
            changed_columns,
        }
    }

    /// Create an update delta by comparing old and new rows.
    pub fn update(row_id: RowId, old_row: Row, new_row: Row) -> Self {
        let mut changed_columns = HashMap::new();
        let all_cols: HashSet<&str> = old_row
            .iter()
            .map(|(k, _)| k)
            .chain(new_row.iter().map(|(k, _)| k))
            .collect();

        for col in all_cols {
            let old_val = old_row.get(col).cloned();
            let new_val = new_row.get(col).cloned();
            if old_val != new_val {
                changed_columns.insert(col.to_string(), (old_val, new_val));
            }
        }

        Self {
            row_id,
            change: ChangeKind::Update,
            old_row: Some(old_row),
            new_row: Some(new_row),
            changed_columns,
        }
    }

    /// Check if a specific column changed.
    pub fn column_changed(&self, col: &str) -> bool {
        self.changed_columns.contains_key(col)
    }

    /// Get the old value of a column.
    pub fn old_value(&self, col: &str) -> Option<&Value> {
        self.changed_columns
            .get(col)
            .and_then(|(old, _)| old.as_ref())
    }

    /// Get the new value of a column.
    pub fn new_value(&self, col: &str) -> Option<&Value> {
        self.changed_columns
            .get(col)
            .and_then(|(_, new)| new.as_ref())
    }
}

/// Filter for what changes a subscription cares about.
#[derive(Debug, Clone)]
pub struct SubscriptionFilter {
    pub tables: Option<HashSet<String>>,
    pub changes: Option<HashSet<ChangeKind>>,
    /// Only notify if specific columns changed.
    pub columns: Option<HashSet<String>>,
}

impl SubscriptionFilter {
    pub fn new() -> Self {
        Self {
            tables: None,
            changes: None,
            columns: None,
        }
    }

    pub fn tables(mut self, tables: impl Into<Vec<String>>) -> Self {
        self.tables = Some(tables.into().into_iter().collect());
        self
    }

    pub fn changes(mut self, changes: impl Into<Vec<ChangeKind>>) -> Self {
        self.changes = Some(changes.into().into_iter().collect());
        self
    }

    pub fn columns(mut self, columns: impl Into<Vec<String>>) -> Self {
        self.columns = Some(columns.into().into_iter().collect());
        self
    }

    fn matches(&self, table: Option<&str>, delta: &Delta) -> bool {
        if let Some(ref tables) = self.tables {
            match table {
                Some(t) if tables.contains(t) => {}
                _ => return false,
            }
        }
        if let Some(ref changes) = self.changes {
            if !changes.contains(&delta.change) {
                return false;
            }
        }
        if let Some(ref columns) = self.columns {
            if !delta.changed_columns.keys().any(|c| columns.contains(c)) {
                return false;
            }
        }
        true
    }
}

impl Default for SubscriptionFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// A subscription that receives deltas.
pub struct Subscription {
    pub id: Uuid,
    pub filter: SubscriptionFilter,
    pub callback: Box<dyn Fn(&Delta) + Send + Sync>,
}

/// Manages subscriptions and dispatches change deltas.
pub struct SubscriptionManager {
    subscriptions: HashMap<Uuid, Subscription>,
    change_log: VecDeque<Delta>,
    max_log_size: usize,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            change_log: VecDeque::new(),
            max_log_size: 1000,
        }
    }

    /// Set the maximum number of changes to keep in the log.
    pub fn with_max_log_size(mut self, max: usize) -> Self {
        self.max_log_size = max;
        self
    }

    /// Create a new subscription with a filter and callback.
    pub fn subscribe(
        &mut self,
        filter: SubscriptionFilter,
        callback: Box<dyn Fn(&Delta) + Send + Sync>,
    ) -> Uuid {
        let id = Uuid::new_v4();
        self.subscriptions.insert(
            id,
            Subscription {
                id,
                filter,
                callback,
            },
        );
        id
    }

    /// Remove a subscription.
    pub fn unsubscribe(&mut self, id: Uuid) -> bool {
        self.subscriptions.remove(&id).is_some()
    }

    /// Notify all matching subscribers about a change.
    pub fn notify(&mut self, table: Option<&str>, delta: Delta) {
        for sub in self.subscriptions.values() {
            if sub.filter.matches(table, &delta) {
                (sub.callback)(&delta);
            }
        }

        self.change_log.push_back(delta);
        while self.change_log.len() > self.max_log_size {
            self.change_log.pop_front();
        }
    }

    /// Get the change log.
    pub fn change_log(&self) -> Vec<&Delta> {
        self.change_log.iter().collect()
    }

    /// Get the number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Clear all subscriptions.
    pub fn clear(&mut self) {
        self.subscriptions.clear();
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn make_row(id: u64, name: &str, age: i64) -> Row {
        let mut row = Row::new(RowId::new(id));
        row.set("name", Value::String(name.into()));
        row.set("age", Value::Int64(age));
        row
    }

    #[test]
    fn test_delta_insert() {
        let row = make_row(1, "Alice", 25);
        let delta = Delta::insert(RowId::new(1), row.clone());
        assert_eq!(delta.change, ChangeKind::Insert);
        assert!(delta.old_row.is_none());
        assert_eq!(delta.new_row.as_ref().unwrap().get("name"), Some(&Value::String("Alice".into())));
        assert!(delta.column_changed("name"));
    }

    #[test]
    fn test_delta_delete() {
        let row = make_row(1, "Alice", 25);
        let delta = Delta::delete(RowId::new(1), row.clone());
        assert_eq!(delta.change, ChangeKind::Delete);
        assert!(delta.new_row.is_none());
        assert_eq!(delta.old_row.as_ref().unwrap().get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_delta_update() {
        let old = make_row(1, "Alice", 25);
        let new = make_row(1, "Alice", 26);
        let delta = Delta::update(RowId::new(1), old, new);
        assert_eq!(delta.change, ChangeKind::Update);
        assert!(delta.column_changed("age"));
        assert!(!delta.column_changed("name"));
        assert_eq!(delta.old_value("age"), Some(&Value::Int64(25)));
        assert_eq!(delta.new_value("age"), Some(&Value::Int64(26)));
    }

    #[test]
    fn test_subscription_receives_deltas() {
        let mut manager = SubscriptionManager::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        manager.subscribe(
            SubscriptionFilter::new(),
            Box::new(move |_delta| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        let row = make_row(1, "Alice", 25);
        manager.notify(
            Some("users"),
            Delta::insert(RowId::new(1), row),
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_subscription_table_filter() {
        let mut manager = SubscriptionManager::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        manager.subscribe(
            SubscriptionFilter::new().tables(vec!["users".into()]),
            Box::new(move |_delta| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        let row = make_row(1, "Alice", 25);
        manager.notify(Some("users"), Delta::insert(RowId::new(1), row.clone()));
        manager.notify(Some("posts"), Delta::insert(RowId::new(2), row));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_subscription_change_filter() {
        let mut manager = SubscriptionManager::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        manager.subscribe(
            SubscriptionFilter::new().changes(vec![ChangeKind::Insert]),
            Box::new(move |_delta| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        let row = make_row(1, "Alice", 25);
        manager.notify(Some("users"), Delta::insert(RowId::new(1), row.clone()));
        manager.notify(Some("users"), Delta::delete(RowId::new(1), row.clone()));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_subscription_column_filter() {
        let mut manager = SubscriptionManager::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        manager.subscribe(
            SubscriptionFilter::new()
                .changes(vec![ChangeKind::Update])
                .columns(vec!["email".into()]),
            Box::new(move |_delta| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        let old = make_row(1, "Alice", 25);
        let mut new = old.clone();
        new.set("age", Value::Int64(26));
        manager.notify(Some("users"), Delta::update(RowId::new(1), old.clone(), new));

        let mut new2 = old.clone();
        new2.set("email", Value::String("new@example.com".into()));
        manager.notify(Some("users"), Delta::update(RowId::new(1), old, new2));

        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_unsubscribe() {
        let mut manager = SubscriptionManager::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        let id = manager.subscribe(
            SubscriptionFilter::new(),
            Box::new(move |_delta| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        let row = make_row(1, "Alice", 25);
        manager.notify(Some("users"), Delta::insert(RowId::new(1), row.clone()));
        assert_eq!(count.load(Ordering::SeqCst), 1);

        assert!(manager.unsubscribe(id));
        manager.notify(Some("users"), Delta::insert(RowId::new(2), row));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_change_log() {
        let mut manager = SubscriptionManager::new();
        let row = make_row(1, "Alice", 25);
        manager.notify(Some("users"), Delta::insert(RowId::new(1), row.clone()));
        manager.notify(Some("users"), Delta::delete(RowId::new(1), row));
        assert_eq!(manager.change_log().len(), 2);
    }

    #[test]
    fn test_multiple_subscribers() {
        let mut manager = SubscriptionManager::new();
        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));
        let c1 = count1.clone();
        let c2 = count2.clone();

        manager.subscribe(
            SubscriptionFilter::new(),
            Box::new(move |_| { c1.fetch_add(1, Ordering::SeqCst); }),
        );
        manager.subscribe(
            SubscriptionFilter::new(),
            Box::new(move |_| { c2.fetch_add(1, Ordering::SeqCst); }),
        );

        let row = make_row(1, "Alice", 25);
        manager.notify(Some("users"), Delta::insert(RowId::new(1), row));
        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
    }
}
