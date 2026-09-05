//! Reference Rust SDK for BlitzDB.
//!
//! ```ignore
//! let client = blitz_client::Client::connect("127.0.0.1:7420".parse()? ).await?;
//! let row = client.insert("users", values).await?;
//! ```
//!
//! Every method looks like a single op; the worker autobatches (25 ops or
//! 2ms) into the SLO-proven wire shape. See `client` module docs for the
//! exact retry contract.

pub mod client;
pub mod error;

pub use client::{CallResult, Client, Row, BATCH_MAX_WAIT, BATCH_N, DEFAULT_TIMEOUT};
pub use error::{map_server_error, SdkError, SdkResult};
pub use blitz_protocol::{Op, Response, RowView};
pub use blitz_types::value::Value;
