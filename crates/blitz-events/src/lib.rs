pub mod event;
pub mod emitter;
pub mod error;

pub use event::Event;
pub use emitter::EventEmitter;
pub use error::{EventError, EventResult};
