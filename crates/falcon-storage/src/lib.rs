#![forbid(unsafe_code)]

mod cache;
mod engine;

pub use cache::{CacheEngine, CacheStats, EvictionListener, RemovalCause};
pub use engine::StorageError;
