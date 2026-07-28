use thiserror::Error;

/// The only failure mode a pure-RAM cache has left.
///
/// The cache holds nothing durable, so there is no I/O to fail, no on-disk
/// format to be incompatible with, and no backend to be unreachable. What
/// remains is the one limit the engine actively enforces: a single value too
/// large to ever fit its shard's budget, which would otherwise be inserted and
/// immediately evicted.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("value of {size} bytes exceeds the per-shard cache budget of {budget} bytes")]
    ValueTooLarge { size: usize, budget: usize },
}
