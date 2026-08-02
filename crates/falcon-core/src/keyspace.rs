use falcon_storage::{now_millis_u64, CacheEngine, CacheStats};
use std::sync::Arc;

/// One named cache and the policy that surrounds it.
///
/// The engine owns the data; this type owns the decisions the engine
/// deliberately does not make — chiefly what TTL a write gets when the caller
/// does not name one.
///
/// TTL lives *inside* the engine's entries rather than in a map beside them.
/// A parallel key→expiry map would mean a second copy of every key, two
/// structures to keep in step, and a reaper walking keys the engine has
/// already dropped. Passing an absolute expiry down to the entry avoids all
/// three: the engine drops what has expired, on read and on its own sweep.
/// The keyspace does not store its own name: [`Node`](crate::Node) already
/// keys its map by that name, so a copy here would be a second source of truth
/// for the same string.
pub struct Keyspace {
    engine: Arc<CacheEngine>,
    /// Applied to writes that don't carry their own TTL. Milliseconds;
    /// `0` = entries never expire.
    default_ttl_ms: u128,
}

impl Keyspace {
    pub fn new(engine: Arc<CacheEngine>, default_ttl_secs: u64) -> Self {
        Self {
            engine,
            default_ttl_ms: (default_ttl_secs as u128) * 1000,
        }
    }

    /// Read a key. `None` means miss — either never written, or expired or
    /// evicted since. The engine resolves expiry itself, so a stale value is
    /// never returned.
    pub fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.engine.get_shared(key)
    }

    /// Write a key with the keyspace's default TTL.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.put_with_ttl(key, value, None);
    }

    /// Write a key with an optional per-write TTL in seconds. `None` falls back
    /// to the keyspace default; `Some(0)` pins the entry with no expiry,
    /// overriding that default.
    pub fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) {
        let ttl_ms = match ttl_secs {
            Some(s) => (s as u128) * 1000,
            None => self.default_ttl_ms,
        };
        let expires_at = if ttl_ms > 0 {
            // Saturating: a TTL far enough in the future to overflow `u64`
            // millis is indistinguishable from "never", which is what the
            // caller asked for.
            (now_millis_u64() as u128)
                .saturating_add(ttl_ms)
                .try_into()
                .unwrap_or(u64::MAX)
        } else {
            0
        };
        self.engine.put_with_expiry(key, value, expires_at);
    }

    /// Remove a key. Returns whether it was present.
    pub fn delete(&self, key: &[u8]) -> bool {
        self.engine.delete(key)
    }

    pub fn stats(&self) -> CacheStats {
        self.engine.stats()
    }

    /// Number of keys currently carrying an expiry, for `/health`.
    pub fn tracked_ttl_keys(&self) -> usize {
        self.engine.tracked_ttl_keys()
    }
}
