pub mod table;
pub mod index;
pub mod error;

pub use table::{InMemoryTableEngine, TableEngine};
pub use error::{CoreError, CoreResult};
