//! The Falcon Cache engine — pure RAM, hard-bounded, no disk at any point.
//!
//! A cache exists to make data *available fast*, and its contents are
//! regenerable by definition. Every durability mechanism is therefore pure cost
//! here, and this engine has none of them: no write-ahead log, no replication
//! log, no fsync, no disk spill, and no second copy of a value anywhere.
//!
//! This is the only engine Falcon ships, and it is the whole product: there is
//! no durable tier behind it and no object store beside it. A value lives in
//! RAM until it expires or is evicted, and then it is gone — the caller
//! regenerates it from its own system of record, which is the one place the
//! data is authoritative anyway.
//!
//! ```text
//!   put(k,v)  ─▶ hash to a shard ─▶ insert ─▶ evict if over budget
//!   get(k)    ─▶ hash to a shard ─▶ lookup ─▶ return an Arc (no copy)
//!   evict     ─▶ sample N at random ─▶ drop the least recently used
//! ```
//!
//! A write is a hash-table insert. A read is a hash-table lookup plus one bool
//! store and an `Arc` clone. Neither ever touches a disk, a lock file, or a
//! serializer.
//!
//! ## Structure
//!
//! [`CacheEngine`] is `shards: Vec<Shard>`, key routed by hash. Each shard owns
//! one `RwLock<ShardInner>` holding:
//!
//! - `map: HashMap<Arc<[u8]>, Entry>` — one entry per live key, holding the
//!   value, a coarse last-access stamp, and the expiry.
//! - `bytes` — the shard's resident memory, which eviction acts on.
//! - `rng` — xorshift state for sampling eviction candidates.
//!
//! Each entry carries its own `expires_at`, so TTL needs no parallel map and no
//! second copy of the key.
//!
//! ## Eviction: approximated LRU by sampling
//!
//! When a shard exceeds its budget it samples `evict_sample` entries at random
//! and drops the one with the oldest `last_access`, repeating until it is back
//! under. This is the approach Redis uses for `allkeys-lru`, and it is chosen
//! for the same reason: true LRU requires mutating shared ordering state on
//! every read, which is precisely the contention a cache cannot afford.
//!
//! Sampling also avoids an entire class of bug. An earlier CLOCK implementation
//! kept a `Vec` ring with a sweep hand, and because `swap_remove` reshuffles
//! entries under that hand, a key could be examined twice in one sweep — its
//! second chance cleared and then consumed — so the *hottest* keys were evicted
//! first. Sampling has no ring, no hand, and no position: there is no ordering
//! state to corrupt.
//!
//! ## Concurrency
//!
//! Reads take a **shared** lock and stamp recency with one relaxed atomic
//! store, so any number of readers on a shard proceed together. Only writes and
//! eviction take the shard exclusively.
//!
//! This matters more than it looks. When recency lived in a plain field,
//! refreshing it needed `&mut Entry`, so every *read* took its shard
//! exclusively — readers of the same shard queued behind each other for the
//! sake of one integer store, and measured throughput went **down** as cores
//! were added (15.9 M ops/s on one thread, 8.2 M on eight). With shared reads
//! and 512 shards the lock is no longer the limit.
//!
//! The byte budget is enforced per shard (`capacity / SHARDS`), so no
//! process-global counter is on the hot path either.
//!
//! ## The memory bound
//!
//! `capacity_bytes` is a **hard** bound, not a target. Three things are charged
//! against it: value bytes, key bytes, and a deliberately over-estimated
//! per-entry overhead ([`ENTRY_OVERHEAD`]). Undercounting any of them is what
//! turns a "bounded" cache into an OOM, so all of them are counted, and a shard
//! over budget drops entries until it is not.

use std::collections::hash_map::Entry as MapEntry;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Upper bound on shard count; the actual count adapts to capacity (see
/// [`shard_count`]). A power of two so routing is a mask.
const MAX_SHARDS: usize = 512;

/// Smallest per-shard budget worth creating, in bytes.
///
/// Sharding trades eviction quality for concurrency: a shard is an independent
/// LRU population, so splitting a small cache too finely leaves each shard with
/// a handful of entries and nothing to choose between when sampling. Taken to
/// its extreme — one entry per shard — every insert evicts its predecessor and
/// recency stops working entirely. Requiring 1 MB per shard keeps every shard
/// large enough for sampling to mean something.
const MIN_SHARD_BYTES: usize = 1024 * 1024;

/// Per-entry RAM overhead charged against the budget beyond the raw key and
/// value bytes: the `HashMap` slot, the `Entry` struct, and the two `Arc`
/// headers. Deliberately an over-estimate — undercounting is what turns a
/// "bounded" cache into an OOM.
const ENTRY_OVERHEAD: usize = 96;

/// One cached value. There is exactly one of these per live key, and it is the
/// only place the value's bytes exist.
struct Entry {
    value: Arc<[u8]>,
    /// Logical access stamp: the value of the engine's access counter when this
    /// entry was last written or read. Eviction picks the entry with the
    /// smallest stamp among those it samples.
    ///
    /// Atomic so that stamping it needs only a *shared* borrow of the entry.
    /// This is what lets reads take `RwLock::read` and run concurrently: with a
    /// plain field, refreshing recency required `&mut`, so every read took the
    /// shard exclusively and readers of the same shard serialized behind each
    /// other for the sake of one integer store.
    ///
    /// A *counter* rather than a wall clock deliberately. Redis stamps seconds,
    /// which is fine over the hours a real cache runs but carries no signal at
    /// all inside one second — every entry ties, and sampled LRU degenerates to
    /// sampled random. A counter orders accesses exactly, at any timescale.
    ///
    /// This single field is the entire recency mechanism — there is no ring, no
    /// hand, and no ordering state shared between entries, so there is nothing
    /// for concurrent eviction and access to corrupt.
    last_access: AtomicU64,
    /// Unix millis after which this entry is dead; `0` = never expires.
    expires_at: u64,
}

impl Entry {
    fn is_expired(&self, now: u64) -> bool {
        self.expires_at != 0 && now >= self.expires_at
    }

    fn touch(&self, stamp: u64) {
        self.last_access.store(stamp, Ordering::Relaxed);
    }

    fn stamp(&self) -> u64 {
        self.last_access.load(Ordering::Relaxed)
    }
}

/// Why the cache dropped a key, passed to the [`EvictionListener`].
///
/// Both reasons mean the same thing to an observer — the key is gone — but they
/// are distinguished because they say different things about the deployment:
/// `Expired` is the TTL working as configured, while `Evicted` means the
/// working set does not fit in `capacity_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalCause {
    /// Dropped by the CLOCK sweep to stay within the memory budget.
    Evicted,
    /// Dropped because its TTL had passed.
    Expired,
}

/// Notified whenever the cache drops a key on its own initiative.
///
/// Eviction and expiry happen inside the engine, so without a hook they are
/// invisible: nothing outside the cache can tell that a key it was told about
/// is now gone. The listener exists so removals can be observed — logged,
/// counted, or acted on — by whoever owns the cache.
///
/// Called while no shard lock is held, so an implementation may do real work
/// without blocking the cache.
pub trait EvictionListener: Send + Sync {
    fn on_removed(&self, key: &[u8], cause: RemovalCause);
}

/// Observable cache stats, surfaced in `/healthz`.
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    /// Entries dropped by the CLOCK sweep to stay within the memory budget.
    pub evictions: u64,
    /// Entries dropped because their TTL had passed.
    pub expired: u64,
    pub keys: u64,
    pub bytes: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// The mutable half of a shard, behind one lock.
struct ShardInner {
    map: HashMap<Arc<[u8]>, Entry>,
    /// Resident bytes: `key.len() + value.len() + ENTRY_OVERHEAD` summed over
    /// this shard's entries. This is the number eviction acts on.
    bytes: usize,
    /// xorshift state for sampling. Per-shard and behind the shard lock, so
    /// sampling needs no shared RNG and no atomics.
    rng: u64,
}

/// Per-shard counters, padded to their own cacheline.
///
/// These were process-global atomics. Every read did `fetch_add` on the same
/// two words (the access clock and the hit counter), and a single shared
/// `fetch_add` measures ~399 M ops/s on one thread but only ~31 M across eight
/// — a 13x collapse to cacheline ping-pong, which put a hard ceiling on read
/// throughput no amount of lock work could lift. Keeping them per-shard means
/// a read touches only lines its own shard owns; the totals are summed on the
/// rare occasions anyone asks for them.
#[repr(align(64))]
#[derive(Default)]
struct ShardCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    expired: AtomicU64,
    /// Monotonic access stamp source for this shard. Recency only ever needs to
    /// be comparable *within* a shard, since eviction samples within one — so
    /// there is no reason to serialize every core on one global clock.
    clock: AtomicU64,
}

struct Shard {
    /// `RwLock`, not `Mutex`: a read is a lookup plus one relaxed atomic store,
    /// which needs no exclusive access, so concurrent readers of the same shard
    /// proceed together instead of queueing.
    inner: RwLock<ShardInner>,
    counters: ShardCounters,
    /// Per-shard RAM budget, enforced locally so the hot path never touches a
    /// process-global counter.
    capacity: usize,
}

/// Pure-RAM cache with a hard memory bound. See the module docs.
pub struct CacheEngine {
    shards: Vec<Shard>,
    /// `shards.len() - 1`; `shards.len()` is always a power of two.
    shard_mask: usize,
    /// How many random entries eviction samples before choosing a victim.
    /// Redis's default is 5; sampling more approaches true LRU at linear cost.
    evict_sample: usize,
    /// Notified on eviction and expiry. `None` until `set_eviction_listener`
    /// installs one (the keyspace does this when a event bus exists).
    on_removed: std::sync::RwLock<Option<Arc<dyn EvictionListener>>>,
}

fn now_millis_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// How many shards a cache of `capacity_bytes` should use: as many as fit while
/// keeping each shard at least [`MIN_SHARD_BYTES`], capped at [`MAX_SHARDS`],
/// rounded down to a power of two so routing is a mask. Always at least 1.
fn shard_count(capacity_bytes: usize) -> usize {
    let by_capacity = (capacity_bytes / MIN_SHARD_BYTES).max(1);
    let n = by_capacity.min(MAX_SHARDS);
    // Round down to a power of two.
    1usize << (usize::BITS - 1 - n.leading_zeros()) as usize
}

fn shard_of(key: &[u8], mask: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) & mask
}

fn entry_ram(key_len: usize, value_len: usize) -> usize {
    key_len + value_len + ENTRY_OVERHEAD
}

/// One step of xorshift64*. Cheap, has no shared state, and is more than random
/// enough to pick eviction candidates.
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545F4914F6CDD1D)
}

impl CacheEngine {
    /// Create a cache bounded to `capacity_bytes` of resident memory.
    ///
    /// The bound is hard: it counts value bytes, key bytes, and per-entry
    /// overhead, and the cache sheds entries rather than exceeding it.
    /// `evict_sample` sets the floor on how much work one CLOCK sweep may do.
    pub fn new(capacity_bytes: usize, evict_sample: usize) -> Self {
        // Split the budget evenly across as many shards as it can support
        // without starving each one of eviction candidates.
        let n_shards = shard_count(capacity_bytes);
        let per_shard = (capacity_bytes / n_shards).max(1);
        let shards = (0..n_shards)
            .map(|i| Shard {
                inner: RwLock::new(ShardInner {
                    map: HashMap::new(),
                    bytes: 0,
                    // Distinct seeds so shards do not sample in lockstep.
                    rng: 0x9E3779B97F4A7C15u64.wrapping_mul(i as u64 + 1) | 1,
                }),
                capacity: per_shard,
                counters: ShardCounters::default(),
            })
            .collect();
        Self {
            shard_mask: n_shards - 1,
            shards,
            evict_sample: evict_sample.max(1),
            on_removed: std::sync::RwLock::new(None),
        }
    }

    /// Install the listener notified when the cache drops a key by eviction or
    /// expiry. Installed once at startup, before any traffic.
    pub fn set_eviction_listener(&self, listener: Arc<dyn EvictionListener>) {
        *self.on_removed.write().unwrap() = Some(listener);
    }

    /// Fan out a batch of removals to the listener. Always called with no shard
    /// lock held, so the listener may publish to the event bus freely.
    fn notify_removed(&self, removed: &[(Arc<[u8]>, RemovalCause)]) {
        if removed.is_empty() {
            return;
        }
        let listener = self.on_removed.read().unwrap().clone();
        if let Some(listener) = listener {
            for (key, cause) in removed {
                listener.on_removed(key, *cause);
            }
        }
    }

    /// Spawn the background TTL sweep, which drops expired entries every few
    /// seconds so dead data never accumulates. Returns a handle the caller
    /// aborts on shutdown.
    ///
    /// Separate from `new` so the engine can be constructed without a Tokio
    /// runtime. Expiry is also enforced on read, so this is reclamation rather
    /// than correctness.
    pub fn spawn_maintenance(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                engine.reap_expired();
            }
        })
    }

    /// Take the next access stamp for `shard`. Relaxed because the counter only
    /// has to be monotonic within its shard to order that shard's accesses — it
    /// guards no other memory, and eviction never compares stamps across shards.
    fn access_clock(&self, shard: &Shard) -> u64 {
        shard.counters.clock.fetch_add(1, Ordering::Relaxed)
    }

    pub fn stats(&self) -> CacheStats {
        // Keys and bytes are summed from the shards rather than mirrored into a
        // parallel global counter: the shard counter is what eviction enforces,
        // so deriving the stat from it means the number reported can never
        // drift from the number acted upon.
        let mut out = CacheStats::default();
        for shard in &self.shards {
            let inner = shard.inner.read().unwrap();
            out.keys += inner.map.len() as u64;
            out.bytes += inner.bytes as u64;
            drop(inner);
            let c = &shard.counters;
            out.hits += c.hits.load(Ordering::Relaxed);
            out.misses += c.misses.load(Ordering::Relaxed);
            out.evictions += c.evictions.load(Ordering::Relaxed);
            out.expired += c.expired.load(Ordering::Relaxed);
        }
        out
    }

    /// Insert or overwrite a key, with an optional expiry (unix millis; `0` =
    /// never). This is the entire write path.
    pub fn insert(&self, key: &[u8], value: &[u8], expires_at: u64) {
        let key: Arc<[u8]> = Arc::from(key);
        let value: Arc<[u8]> = Arc::from(value);
        let added = entry_ram(key.len(), value.len());
        let idx = shard_of(&key, self.shard_mask);
        let shard = &self.shards[idx];
        let now = self.access_clock(shard);

        {
            let mut inner = shard.inner.write().unwrap();
            let freed = match inner.map.entry(key) {
                MapEntry::Occupied(mut occ) => {
                    let old_ram = entry_ram(occ.key().len(), occ.get().value.len());
                    occ.insert(Entry {
                        value,
                        last_access: AtomicU64::new(now),
                        expires_at,
                    });
                    old_ram
                }
                MapEntry::Vacant(vac) => {
                    vac.insert(Entry {
                        value,
                        last_access: AtomicU64::new(now),
                        expires_at,
                    });
                    0
                }
            };
            inner.bytes = inner.bytes + added - freed;
        }

        self.evict_shard(idx);
    }

    /// Look up a key. Expired entries are dropped here rather than served, so
    /// a stale value is never returned even between background sweeps.
    ///
    /// The hit and miss paths take only a **shared** lock, so any number of
    /// readers on a shard proceed at once. Refreshing recency is a relaxed
    /// atomic store through `&Entry`, which is why no exclusive borrow is
    /// needed for what is otherwise a pure read.
    fn lookup(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        let now = now_millis_u64();
        let idx = shard_of(key, self.shard_mask);
        let shard = &self.shards[idx];

        // Fast path under the shared lock.
        {
            let inner = shard.inner.read().unwrap();
            match inner.map.get(key) {
                None => {
                    drop(inner);
                    shard.counters.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                Some(entry) if !entry.is_expired(now) => {
                    // The whole hot path: a hash lookup, one relaxed store, and
                    // an `Arc` clone. No allocation, no copy, no exclusive lock.
                    entry.touch(self.access_clock(shard));
                    let value = Arc::clone(&entry.value);
                    drop(inner);
                    shard.counters.hits.fetch_add(1, Ordering::Relaxed);
                    return Some(value);
                }
                // Expired: fall through and take the write lock to remove it.
                Some(_) => {}
            }
        }

        // Slow path: the entry is expired and must be dropped. Re-check under
        // the write lock, since another reader may have removed it, or a writer
        // may have replaced it with a live value, while the lock was released.
        let owned = {
            let mut inner = shard.inner.write().unwrap();
            match inner.map.get(key) {
                Some(entry) if entry.is_expired(now) => {
                    let value_len = entry.value.len();
                    let owned = inner.map.remove_entry(key).map(|(k, _)| k);
                    inner.bytes = inner.bytes.saturating_sub(entry_ram(key.len(), value_len));
                    owned
                }
                // Replaced by a live value in the gap: serve it.
                Some(entry) => {
                    entry.touch(self.access_clock(shard));
                    let value = Arc::clone(&entry.value);
                    drop(inner);
                    shard.counters.hits.fetch_add(1, Ordering::Relaxed);
                    return Some(value);
                }
                None => None,
            }
        };

        shard.counters.expired.fetch_add(1, Ordering::Relaxed);
        shard.counters.misses.fetch_add(1, Ordering::Relaxed);
        if let Some(k) = owned {
            self.notify_removed(&[(k, RemovalCause::Expired)]);
        }
        None
    }

    /// Remove a key. O(1): there is no eviction ring to keep in step, so a
    /// delete touches nothing but the map and the byte counter.
    fn remove(&self, key: &[u8]) -> bool {
        let idx = shard_of(key, self.shard_mask);
        let mut inner = self.shards[idx].inner.write().unwrap();
        match inner.map.remove(key) {
            Some(entry) => {
                inner.bytes = inner
                    .bytes
                    .saturating_sub(entry_ram(key.len(), entry.value.len()));
                true
            }
            None => false,
        }
    }

    /// Evict from one shard until it is back within budget.
    ///
    /// Approximated LRU by random sampling, the same approach Redis uses for
    /// `allkeys-lru`: rather than maintain a global ordering — which would mean
    /// mutating shared state on every read, exactly the contention the cache
    /// exists to avoid — each round samples `evict_sample` entries at random
    /// and drops the one with the oldest `last_access`. Sampling five gets
    /// close to true LRU; sampling more approaches it at linear cost.
    ///
    /// Expired entries found while sampling are dropped for free, since they
    /// are dead weight regardless of the budget.
    fn evict_shard(&self, idx: usize) {
        let shard = &self.shards[idx];
        let mut inner = shard.inner.write().unwrap();
        if inner.bytes <= shard.capacity {
            return;
        }
        let now_ms = now_millis_u64();
        let mut evicted = 0u64;
        let mut expired = 0u64;
        // Removals are reported once the shard lock is released, so the
        // listener never runs while the shard is locked.
        let mut removed: Vec<(Arc<[u8]>, RemovalCause)> = Vec::new();

        // Bound the work per call so a writer never stalls unboundedly, even if
        // the shard is far over budget.
        let mut rounds = inner.map.len() + self.evict_sample;

        while inner.bytes > shard.capacity && rounds > 0 && !inner.map.is_empty() {
            rounds -= 1;

            // Sample up to `evict_sample` entries and keep the least recently
            // used. `HashMap` has no positional index, so a sample is taken by
            // stepping a random distance into the iterator — O(n) in the worst
            // case, which is why the sample count stays small.
            let len = inner.map.len();
            let mut victim: Option<(Arc<[u8]>, u64)> = None;
            let mut dead: Option<Arc<[u8]>> = None;

            for _ in 0..self.evict_sample.min(len) {
                let skip = (next_rand(&mut inner.rng) as usize) % len;
                let Some((key, entry)) = inner.map.iter().nth(skip) else {
                    continue;
                };
                // An expired entry is strictly better to drop than any live
                // one: take it immediately and skip the comparison.
                if entry.expires_at != 0 && now_ms >= entry.expires_at {
                    dead = Some(Arc::clone(key));
                    break;
                }
                let stamp = entry.stamp();
                match &victim {
                    Some((_, oldest)) if stamp >= *oldest => {}
                    _ => victim = Some((Arc::clone(key), stamp)),
                }
            }

            let (key, cause) = match (dead, victim) {
                (Some(k), _) => (k, RemovalCause::Expired),
                (None, Some((k, _))) => (k, RemovalCause::Evicted),
                // Sampling found nothing (the map emptied underneath us).
                (None, None) => break,
            };

            if let Some(entry) = inner.map.remove(&key) {
                inner.bytes = inner
                    .bytes
                    .saturating_sub(entry_ram(key.len(), entry.value.len()));
                match cause {
                    RemovalCause::Expired => expired += 1,
                    RemovalCause::Evicted => evicted += 1,
                }
                removed.push((key, cause));
            }
        }
        drop(inner);

        if evicted > 0 {
            shard.counters.evictions.fetch_add(evicted, Ordering::Relaxed);
        }
        if expired > 0 {
            shard.counters.expired.fetch_add(expired, Ordering::Relaxed);
        }
        self.notify_removed(&removed);
    }

    /// Drop every expired entry. Run periodically so dead data never
    /// accumulates even in a cache that never reaches its memory budget.
    pub fn reap_expired(&self) {
        let now = now_millis_u64();
        let mut removed: Vec<(Arc<[u8]>, RemovalCause)> = Vec::new();
        for shard in &self.shards {
            let mut expired = 0u64;
            let mut inner = shard.inner.write().unwrap();
            let dead: Vec<Arc<[u8]>> = inner
                .map
                .iter()
                .filter(|(_, e)| e.is_expired(now))
                .map(|(k, _)| Arc::clone(k))
                .collect();
            for key in dead {
                if let Some(entry) = inner.map.remove(&key) {
                    inner.bytes = inner
                        .bytes
                        .saturating_sub(entry_ram(key.len(), entry.value.len()));
                    expired += 1;
                    removed.push((Arc::clone(&key), RemovalCause::Expired));
                }
            }
            drop(inner);
            if expired > 0 {
                shard.counters.expired.fetch_add(expired, Ordering::Relaxed);
            }
        }
        // Outside every shard lock.
        self.notify_removed(&removed);
    }

    /// Write with an explicit expiry (unix millis; `0` = never). Used by the
    /// keyspace so TTL lives inside the entry instead of a parallel map.
    pub fn put_with_expiry(&self, key: &[u8], value: &[u8], expires_at: u64) {
        self.insert(key, value, expires_at);
    }

    /// Zero-copy read. Returns the value's `Arc`, so a hit costs a refcount
    /// bump rather than an allocation plus a copy.
    ///
    /// This is the whole read API: the cache never awaits — a read is a shard
    /// lock and a hash lookup, with no I/O anywhere — so there is no async
    /// variant to choose between. Callers that need an owned `Vec` copy at the
    /// boundary themselves.
    pub fn get_shared(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.lookup(key)
    }

    /// Write a key with no expiry.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.insert(key, value, 0);
    }

    /// Remove a key. Returns whether it was present.
    pub fn delete(&self, key: &[u8]) -> bool {
        self.remove(key)
    }

    /// Number of keys whose TTL is being tracked (for `/healthz`).
    pub fn tracked_ttl_keys(&self) -> usize {
        self.shards
            .iter()
            .map(|s| {
                s.inner
                    .read()
                    .unwrap()
                    .map
                    .values()
                    .filter(|e| e.expires_at != 0)
                    .count()
            })
            .sum()
    }
}

