#![forbid(unsafe_code)]

mod cache;

pub use cache::{
    now_millis_u64, CacheEngine, CacheOptions, CacheStats, EvictionListener, RemovalCause,
};
