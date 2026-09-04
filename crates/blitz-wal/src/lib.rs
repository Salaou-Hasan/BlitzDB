pub mod log;
pub mod error;

pub use log::WriteAheadLog;
pub use error::{WalError, WalResult};
