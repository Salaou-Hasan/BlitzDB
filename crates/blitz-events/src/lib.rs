pub mod emitter;
pub mod error;
pub mod event;

pub use emitter::EventEmitter;
pub use error::{EventError, EventResult};
pub use event::{Event, EventKind};
