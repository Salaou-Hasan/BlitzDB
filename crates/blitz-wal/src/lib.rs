pub mod log;
pub mod error;

pub use log::{EntryType, WalEntry, WriteAheadLog};
pub use error::{WalError, WalResult};
