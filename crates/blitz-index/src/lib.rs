pub mod hash_index;
pub mod btree_index;
pub mod error;

pub use hash_index::HashIndex;
pub use btree_index::BTreeIndex;
pub use error::{IndexError, IndexResult};
