use crate::event::Event;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

pub struct EventEmitter {
    buffer: Arc<Mutex<VecDeque<Event>>>,
}

impl EventEmitter {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn emit(&self, event: Event) {
        self.buffer.lock().unwrap().push_back(event);
    }

    pub fn drain(&self) -> Vec<Event> {
        self.buffer.lock().unwrap().drain(..).collect()
    }
}

impl Default for EventEmitter {
    fn default() -> Self {
        Self::new()
    }
}
