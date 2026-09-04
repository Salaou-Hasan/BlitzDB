use crate::event::{Event, EventKind};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// A subscriber that receives events matching its filters.
pub struct Subscriber {
    pub id: String,
    pub event_kinds: Option<HashSet<EventKind>>,
    pub tables: Option<HashSet<String>>,
    pub callback: Box<dyn Fn(&Event) + Send + Sync>,
}

/// Event emitter that dispatches events to subscribers.
pub struct EventEmitter {
    subscribers: Arc<Mutex<HashMap<String, Subscriber>>>,
    buffer: Arc<Mutex<Vec<Event>>>,
    history: Arc<Mutex<Vec<Event>>>,
    max_history: usize,
}

impl EventEmitter {
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            buffer: Arc::new(Mutex::new(Vec::new())),
            history: Arc::new(Mutex::new(Vec::new())),
            max_history: 1000,
        }
    }

    /// Set the maximum number of events to keep in history.
    pub fn with_max_history(mut self, max: usize) -> Self {
        self.max_history = max;
        self
    }

    /// Register a subscriber with no filters (receives all events).
    pub fn subscribe(
        &self,
        id: impl Into<String>,
        callback: Box<dyn Fn(&Event) + Send + Sync>,
    ) {
        let sub = Subscriber {
            id: id.into(),
            event_kinds: None,
            tables: None,
            callback,
        };
        self.subscribers.lock().unwrap().insert(sub.id.clone(), sub);
    }

    /// Register a subscriber filtered by event kind.
    pub fn subscribe_kind(
        &self,
        id: impl Into<String>,
        kinds: Vec<EventKind>,
        callback: Box<dyn Fn(&Event) + Send + Sync>,
    ) {
        let sub = Subscriber {
            id: id.into(),
            event_kinds: Some(kinds.into_iter().collect()),
            tables: None,
            callback,
        };
        self.subscribers.lock().unwrap().insert(sub.id.clone(), sub);
    }

    /// Register a subscriber filtered by table.
    pub fn subscribe_table(
        &self,
        id: impl Into<String>,
        tables: Vec<String>,
        callback: Box<dyn Fn(&Event) + Send + Sync>,
    ) {
        let sub = Subscriber {
            id: id.into(),
            event_kinds: None,
            tables: Some(tables.into_iter().collect()),
            callback,
        };
        self.subscribers.lock().unwrap().insert(sub.id.clone(), sub);
    }

    /// Remove a subscriber.
    pub fn unsubscribe(&self, id: &str) -> bool {
        self.subscribers.lock().unwrap().remove(id).is_some()
    }

    /// Emit an event to all matching subscribers.
    pub fn emit(&self, event: Event) {
        let subscribers = self.subscribers.lock().unwrap();
        for sub in subscribers.values() {
            if self.matches(sub, &event) {
                (sub.callback)(&event);
            }
        }

        self.buffer.lock().unwrap().push(event.clone());
        let mut history = self.history.lock().unwrap();
        history.push(event);
        let excess = history.len().saturating_sub(self.max_history);
        if excess > 0 {
            history.drain(..excess);
        }
    }

    /// Drain buffered events (non-dispatched).
    pub fn drain(&self) -> Vec<Event> {
        self.buffer.lock().unwrap().drain(..).collect()
    }

    /// Get event history.
    pub fn history(&self) -> Vec<Event> {
        self.history.lock().unwrap().clone()
    }

    /// Get the number of registered subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.lock().unwrap().len()
    }

    /// Clear all subscribers.
    pub fn clear_subscribers(&self) {
        self.subscribers.lock().unwrap().clear();
    }

    fn matches(&self, sub: &Subscriber, event: &Event) -> bool {
        if let Some(ref kinds) = sub.event_kinds {
            if !kinds.contains(&event.kind) {
                return false;
            }
        }
        if let Some(ref tables) = sub.tables {
            match &event.table {
                Some(t) if tables.contains(t) => {}
                _ => return false,
            }
        }
        true
    }
}

impl Default for EventEmitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_emit_and_receive() {
        let emitter = EventEmitter::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        emitter.subscribe(
            "test",
            Box::new(move |_event| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        emitter.emit(Event::new(EventKind::RowInserted));
        emitter.emit(Event::new(EventKind::RowUpdated));
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_subscribe_filter_by_kind() {
        let emitter = EventEmitter::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        emitter.subscribe_kind(
            "inserts",
            vec![EventKind::RowInserted],
            Box::new(move |_event| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        emitter.emit(Event::new(EventKind::RowInserted));
        emitter.emit(Event::new(EventKind::RowUpdated));
        emitter.emit(Event::new(EventKind::RowInserted));
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_subscribe_filter_by_table() {
        let emitter = EventEmitter::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        emitter.subscribe_table(
            "users-only",
            vec!["users".into()],
            Box::new(move |_event| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        emitter.emit(Event::new(EventKind::RowInserted).with_table("users"));
        emitter.emit(Event::new(EventKind::RowInserted).with_table("posts"));
        emitter.emit(Event::new(EventKind::RowInserted).with_table("users"));
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_unsubscribe() {
        let emitter = EventEmitter::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        emitter.subscribe(
            "test",
            Box::new(move |_event| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        emitter.emit(Event::new(EventKind::RowInserted));
        assert_eq!(count.load(Ordering::SeqCst), 1);

        assert!(emitter.unsubscribe("test"));
        emitter.emit(Event::new(EventKind::RowInserted));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_history() {
        let emitter = EventEmitter::new();
        emitter.emit(Event::new(EventKind::RowInserted));
        emitter.emit(Event::new(EventKind::RowUpdated));
        let history = emitter.history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].kind, EventKind::RowInserted);
        assert_eq!(history[1].kind, EventKind::RowUpdated);
    }

    #[test]
    fn test_subscriber_count() {
        let emitter = EventEmitter::new();
        assert_eq!(emitter.subscriber_count(), 0);

        emitter.subscribe("a", Box::new(|_| {}));
        emitter.subscribe("b", Box::new(|_| {}));
        assert_eq!(emitter.subscriber_count(), 2);

        emitter.clear_subscribers();
        assert_eq!(emitter.subscriber_count(), 0);
    }

    #[test]
    fn test_combined_kind_and_table_filter() {
        let emitter = EventEmitter::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        emitter.subscribe_kind(
            "filtered",
            vec![EventKind::RowInserted],
            Box::new(move |_event| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        emitter.subscribe_table(
            "table-filtered",
            vec!["users".into()],
            Box::new(|_| {}),
        );

        // Only the kind-filtered subscriber should fire
        emitter.emit(Event::new(EventKind::RowInserted).with_table("users"));
        // Both should fire (kind matches first, table matches second)
        emitter.emit(Event::new(EventKind::RowInserted).with_table("posts"));
        // Only table-filtered should fire
        emitter.emit(Event::new(EventKind::RowUpdated).with_table("users"));

        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
