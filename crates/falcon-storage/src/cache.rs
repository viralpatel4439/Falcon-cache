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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

/// Never fewer shards than this, however small the cache.
///
/// Capacity alone used to decide the count, so a cache under 2 MB collapsed to
/// a single shard — one global lock serializing every writer on the machine,
/// no matter how many cores it had. Four is enough to break that up while
/// leaving each shard a workable sampling population.
const MIN_SHARDS: usize = 4;

/// Shards to aim for per logical CPU.
///
/// Writers pick a shard by key hash, so collisions are a birthday problem:
/// several shards per core keeps the chance that two concurrent writers land
/// on the same one low, without splitting the cache so finely that eviction
/// sampling suffers.
const SHARDS_PER_CORE: usize = 4;

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
    /// Dropped by sampled-LRU eviction to stay within the memory budget.
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

/// Observable cache stats, surfaced in `/health`.
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    /// Reads that found a live entry.
    pub hits: u64,
    /// Reads that found nothing — never written, or expired or evicted since.
    /// A read of an expired entry counts here *and* in `expired`: one is the
    /// caller's view, the other is the reason.
    pub misses: u64,
    /// Entries dropped by sampled-LRU eviction to stay within the memory
    /// budget. A rising count means the working set exceeds `capacity_bytes`.
    pub evictions: u64,
    /// Entries dropped because their TTL had passed, whether noticed on read
    /// or reclaimed by the background sweep.
    pub expired: u64,
    /// Live entries across all shards.
    pub keys: u64,
    /// Bytes charged against the budget: keys, values, and per-entry overhead.
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
    ///
    /// Private, and only ever changed through [`ShardInner::set_bytes`], which
    /// also republishes [`Shard::bytes_hint`]. Keeping the two in step is what
    /// lets the insert path skip the lock, so they must never be updated
    /// separately — hence no direct writes to this field.
    bytes: usize,
    /// Live entries in this shard carrying an expiry (`expires_at != 0`).
    ///
    /// Maintained under the same lock as `map`, and mirrored into
    /// [`Shard::ttl_hint`] so the background sweep can tell — without taking
    /// any lock — that a shard has nothing to expire. A cache used without
    /// TTLs (the default) then costs the sweep nothing at all.
    ttl_keys: usize,
    /// xorshift state for sampling. Per-shard and behind the shard lock, so
    /// sampling needs no shared RNG and no atomics.
    rng: u64,
}

impl ShardInner {
    /// Set this shard's resident byte count and republish the lock-free hint.
    ///
    /// The single write path for `bytes`, so the hint can never drift from the
    /// authoritative count: both move together, inside the write lock the
    /// caller already holds.
    fn set_bytes(&mut self, bytes: usize, shard: &Shard) {
        self.bytes = bytes;
        shard.bytes_hint.store(bytes, Ordering::Release);
    }

    /// Record that an entry carrying an expiry was added (`+1`) or dropped
    /// (`-1`), republishing the lock-free hint. Like [`Self::set_bytes`], this
    /// is the only write path, so the mirror cannot drift.
    fn add_ttl_keys(&mut self, delta: isize, shard: &Shard) {
        if delta == 0 {
            return;
        }
        self.ttl_keys = self.ttl_keys.saturating_add_signed(delta);
        shard.ttl_hint.store(self.ttl_keys, Ordering::Release);
    }
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
    /// Lock-free mirror of `inner.bytes`, republished inside the write lock on
    /// every change.
    ///
    /// Eviction reads this *before* taking the lock, so a write to a shard that
    /// is comfortably under budget — the overwhelmingly common case — pays no
    /// exclusive lock at all. Previously every insert took the write lock just
    /// to read `bytes` and discover there was nothing to do, serializing
    /// writers on the same shard for no reason.
    ///
    /// This is a hint, not the authority: `inner.bytes` remains the number
    /// eviction acts on, and `evict_shard` re-checks it under the lock. A hint
    /// that lags can only defer an eviction to the next write on that shard,
    /// never skip enforcement — and `insert` already released the lock before
    /// calling `evict_shard`, so that window predates this field.
    bytes_hint: AtomicUsize,
    /// Lock-free mirror of `inner.ttl_keys`, republished with it.
    ///
    /// Lets the sweep skip a shard with no expiring entries without taking the
    /// write lock — previously it locked and walked every entry of every shard
    /// on a fixed timer regardless of whether anything could expire.
    ttl_hint: AtomicUsize,
}

/// How to size a [`CacheEngine`].
///
/// Exists so sizing inputs arrive as plain resolved numbers: the engine reads
/// no files, no environment, and no config — whoever constructs it decides.
#[derive(Debug, Clone)]
pub struct CacheOptions {
    /// Hard bound on resident memory.
    pub capacity_bytes: usize,
    /// Entries sampled per eviction round (approximated LRU).
    pub evict_sample: usize,
    /// Logical CPUs to size the shard count for.
    pub parallelism: usize,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            capacity_bytes: 256 * 1024 * 1024,
            evict_sample: 8,
            parallelism: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        }
    }
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
    /// installs one.
    on_removed: std::sync::RwLock<Option<Arc<dyn EvictionListener>>>,
    /// Where the incremental TTL sweep resumes. Only the maintenance task
    /// advances it, so contention is nil.
    sweep_cursor: AtomicUsize,
}

/// Fraction of the shards one sweep tick visits (`shards / N`).
const SWEEP_SHARD_DIVISOR: usize = 8;

/// How the background sweep paces itself.
///
/// The sweep used to run on a fixed 5-second timer whether or not anything
/// could expire. Pacing it against what it actually finds means an idle cache
/// is swept rarely and a cache churning through TTLs is swept promptly, without
/// either being a configured number.
struct SweepPacer {
    delay: std::time::Duration,
    rng: u64,
}

impl SweepPacer {
    const MIN: std::time::Duration = std::time::Duration::from_millis(250);
    const MAX: std::time::Duration = std::time::Duration::from_secs(30);
    const START: std::time::Duration = std::time::Duration::from_secs(1);

    fn new() -> Self {
        Self {
            delay: Self::START,
            // Seeded from the clock so co-deployed nodes don't align.
            rng: now_millis_u64() | 1,
        }
    }

    /// Back off when a sweep found nothing, close in when it found something.
    fn observe(&mut self, reclaimed: usize) {
        self.delay = if reclaimed == 0 {
            (self.delay * 2).min(Self::MAX)
        } else {
            (self.delay / 2).max(Self::MIN)
        };
    }

    /// The next delay, jittered ±25%.
    ///
    /// Without jitter every node started by one deployment sweeps in lockstep,
    /// so their latency blips line up instead of averaging out.
    fn next_delay(&mut self) -> std::time::Duration {
        let spread = self.delay.as_millis() as u64 / 2; // full width = 50%
        let offset = if spread == 0 {
            0
        } else {
            next_rand(&mut self.rng) % spread
        };
        // Centre the window on `delay`: [delay - 25%, delay + 25%].
        self.delay + std::time::Duration::from_millis(offset)
            - std::time::Duration::from_millis(spread / 2)
    }
}

/// Milliseconds since the unix epoch, saturating to `0` if the system clock is
/// set before 1970.
///
/// A pre-epoch clock is a misconfigured machine, not a cache failure: treating
/// it as "time zero" makes every entry look already-expired, which is a
/// degraded but safe cache. Panicking here would take down a node on the write
/// path — this is called on every TTL'd write.
pub fn now_millis_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// How many shards a cache of `capacity_bytes` should use: as many as fit while
/// keeping each shard at least [`MIN_SHARD_BYTES`], capped at [`MAX_SHARDS`],
/// rounded down to a power of two so routing is a mask. Always at least 1.
fn shard_count(capacity_bytes: usize, parallelism: usize) -> usize {
    // What concurrency wants: enough shards that simultaneous writers rarely
    // collide. This is the half that used to be missing — the count was derived
    // from capacity alone, so the same 256 MB cache got the same 256 shards on
    // a 2-core box and a 128-core one.
    let by_cores = (parallelism.max(1) * SHARDS_PER_CORE).next_power_of_two();
    // What eviction quality allows: each shard needs enough bytes to hold a
    // population worth sampling.
    let by_capacity = prev_power_of_two((capacity_bytes / MIN_SHARD_BYTES).max(1));
    // Capacity caps the core-driven number, but never below `MIN_SHARDS` — a
    // tiny cache still must not become one global lock.
    by_cores
        .min(by_capacity.max(MIN_SHARDS))
        .clamp(MIN_SHARDS, MAX_SHARDS)
        .next_power_of_two()
}

/// Largest power of two `<= n`. `n` must be non-zero.
fn prev_power_of_two(n: usize) -> usize {
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
    /// Create a cache bounded to `capacity_bytes` of resident memory, sized for
    /// the current machine's parallelism.
    ///
    /// The bound is hard: it counts value bytes, key bytes, and per-entry
    /// overhead, and the cache sheds entries rather than exceeding it.
    /// `evict_sample` sets the floor on how much work one eviction round does.
    pub fn new(capacity_bytes: usize, evict_sample: usize) -> Self {
        Self::with_options(CacheOptions {
            capacity_bytes,
            evict_sample,
            ..CacheOptions::default()
        })
    }

    /// Create a cache from a fully-specified set of options.
    ///
    /// Prefer this when the caller already knows the machine's parallelism —
    /// `falcon-core` resolves it once at startup and passes it down, so the
    /// engine itself never has to probe the environment.
    pub fn with_options(opts: CacheOptions) -> Self {
        let CacheOptions {
            capacity_bytes,
            evict_sample,
            parallelism,
        } = opts;
        // Split the budget evenly across as many shards as capacity and core
        // count together justify.
        let n_shards = shard_count(capacity_bytes, parallelism);
        let per_shard = (capacity_bytes / n_shards).max(1);
        let shards = (0..n_shards)
            .map(|i| Shard {
                inner: RwLock::new(ShardInner {
                    map: HashMap::new(),
                    bytes: 0,
                    ttl_keys: 0,
                    // Distinct seeds so shards do not sample in lockstep.
                    rng: 0x9E3779B97F4A7C15u64.wrapping_mul(i as u64 + 1) | 1,
                }),
                capacity: per_shard,
                counters: ShardCounters::default(),
                bytes_hint: AtomicUsize::new(0),
                ttl_hint: AtomicUsize::new(0),
            })
            .collect();
        Self {
            shard_mask: n_shards - 1,
            shards,
            evict_sample: evict_sample.max(1),
            on_removed: std::sync::RwLock::new(None),
            sweep_cursor: AtomicUsize::new(0),
        }
    }

    /// How many shards this cache was built with — derived from its capacity
    /// and the machine's core count, so it is worth reporting at startup.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
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
        // A slice of the shards per tick, so one sweep never walks the whole
        // cache in a single pass.
        let per_tick = (engine.shards.len() / SWEEP_SHARD_DIVISOR).max(1);
        let mut pacer = SweepPacer::new();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(pacer.next_delay()).await;
                let reclaimed = engine.reap_some(per_tick);
                pacer.observe(reclaimed);
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
            // TTL delta for this write: +1 if it gains an expiry, -1 if it
            // overwrites one with a pinned entry, 0 otherwise. The old entry's
            // `expires_at` has to be read BEFORE `occ.insert` replaces it.
            let mut ttl_delta = isize::from(expires_at != 0);
            let freed = match inner.map.entry(key) {
                MapEntry::Occupied(mut occ) => {
                    let old_ram = entry_ram(occ.key().len(), occ.get().value.len());
                    ttl_delta -= isize::from(occ.get().expires_at != 0);
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
            let n = inner.bytes + added - freed;
            inner.set_bytes(n, shard);
            inner.add_ttl_keys(ttl_delta, shard);
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
                    let n = inner.bytes.saturating_sub(entry_ram(key.len(), value_len));
                    inner.set_bytes(n, shard);
                    // An expired entry had an expiry by definition.
                    inner.add_ttl_keys(-1, shard);
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
        let shard = &self.shards[shard_of(key, self.shard_mask)];
        let mut inner = shard.inner.write().unwrap();
        match inner.map.remove(key) {
            Some(entry) => {
                let n = inner
                    .bytes
                    .saturating_sub(entry_ram(key.len(), entry.value.len()));
                inner.set_bytes(n, shard);
                inner.add_ttl_keys(-isize::from(entry.expires_at != 0), shard);
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
        // Lock-free rejection of the common case: a shard under budget has no
        // work to do, and reading the hint costs one relaxed load instead of an
        // exclusive lock. `inner.bytes` is still the authority — it is
        // re-checked below once the lock is actually held.
        if shard.bytes_hint.load(Ordering::Acquire) <= shard.capacity {
            return;
        }
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

            // Sample `evict_sample` entries and keep the least recently used.
            //
            // `HashMap` has no positional index, so reaching a random entry
            // means stepping into the iterator. Taking each sample that way
            // independently cost `evict_sample` separate O(n) walks per round —
            // on a large shard, millions of iterator steps under the write
            // lock, and quadratic overall since `rounds` scales with the map.
            //
            // Instead: seek once to a random offset and read a contiguous
            // window of `evict_sample` entries from there. That is one walk per
            // round rather than `evict_sample` of them. Adjacency costs nothing
            // in sample quality — bucket order under SipHash is uncorrelated
            // with insertion time and access pattern, so neighbours are as good
            // as independent draws for picking a cold victim. (Redis samples
            // its hash table essentially the same way.)
            let len = inner.map.len();
            let mut victim: Option<(Arc<[u8]>, u64)> = None;
            let mut dead: Option<Arc<[u8]>> = None;

            let window = self.evict_sample.min(len);
            let start = (next_rand(&mut inner.rng) as usize) % len;
            // Wrap around the end so a window starting near the tail still sees
            // `window` entries rather than being truncated.
            for (key, entry) in inner
                .map
                .iter()
                .skip(start)
                .chain(inner.map.iter())
                .take(window)
            {
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
                let n = inner
                    .bytes
                    .saturating_sub(entry_ram(key.len(), entry.value.len()));
                inner.set_bytes(n, shard);
                inner.add_ttl_keys(-isize::from(entry.expires_at != 0), shard);
                match cause {
                    RemovalCause::Expired => expired += 1,
                    RemovalCause::Evicted => evicted += 1,
                }
                removed.push((key, cause));
            }
        }
        drop(inner);

        if evicted > 0 {
            shard
                .counters
                .evictions
                .fetch_add(evicted, Ordering::Relaxed);
        }
        if expired > 0 {
            shard.counters.expired.fetch_add(expired, Ordering::Relaxed);
        }
        self.notify_removed(&removed);
    }

    /// Drop every expired entry, across every shard.
    ///
    /// The complete, immediate sweep. The background task uses
    /// [`Self::reap_some`] instead, which spreads the same work over several
    /// ticks; this one exists for callers that need the cache fully reaped
    /// before they look at it.
    pub fn reap_expired(&self) {
        let mut removed = Vec::new();
        for idx in 0..self.shards.len() {
            self.reap_shard(idx, &mut removed);
        }
        // Outside every shard lock.
        self.notify_removed(&removed);
    }

    /// Sweep `count` shards starting where the last call stopped, returning how
    /// many entries were reclaimed.
    ///
    /// Sweeping every shard in one pass meant one tick could write-lock the
    /// whole cache in sequence; spreading it over several ticks bounds how much
    /// of the cache a single sweep can hold up at once.
    fn reap_some(&self, count: usize) -> usize {
        let n = self.shards.len();
        let start = self.sweep_cursor.fetch_add(count, Ordering::Relaxed);
        let mut removed = Vec::new();
        for i in 0..count.min(n) {
            self.reap_shard((start.wrapping_add(i)) % n, &mut removed);
        }
        let reclaimed = removed.len();
        self.notify_removed(&removed);
        reclaimed
    }

    /// Drop the expired entries of one shard, appending them to `removed`.
    ///
    /// Returns immediately, without taking the lock, when the shard holds
    /// nothing that can expire — which is the whole cache in the default
    /// configuration, where no key carries a TTL.
    fn reap_shard(&self, idx: usize, removed: &mut Vec<(Arc<[u8]>, RemovalCause)>) {
        let shard = &self.shards[idx];
        if shard.ttl_hint.load(Ordering::Acquire) == 0 {
            return;
        }
        let now = now_millis_u64();
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
                let n = inner
                    .bytes
                    .saturating_sub(entry_ram(key.len(), entry.value.len()));
                inner.set_bytes(n, shard);
                inner.add_ttl_keys(-1, shard);
                expired += 1;
                removed.push((Arc::clone(&key), RemovalCause::Expired));
            }
        }
        drop(inner);
        if expired > 0 {
            shard.counters.expired.fetch_add(expired, Ordering::Relaxed);
        }
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

    /// Number of keys currently carrying an expiry (reported by `/health`).
    ///
    /// Reads the per-shard counters rather than walking entries, so this is
    /// O(shards) — it is served on every `/health` request, and previously
    /// scanned the entire cache to answer.
    pub fn tracked_ttl_keys(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.ttl_hint.load(Ordering::Relaxed))
            .sum()
    }

    /// Count expiring entries by walking every shard.
    ///
    /// The slow, obviously-correct version of [`Self::tracked_ttl_keys`], kept
    /// so tests can assert the maintained counter never drifts from reality.
    #[doc(hidden)]
    pub fn count_ttl_keys_by_scan(&self) -> usize {
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

#[cfg(test)]
mod sweep_pacer_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn backs_off_when_nothing_is_reclaimed() {
        let mut p = SweepPacer::new();
        assert_eq!(p.delay, SweepPacer::START);
        p.observe(0);
        assert_eq!(p.delay, Duration::from_secs(2));
        p.observe(0);
        assert_eq!(p.delay, Duration::from_secs(4));
    }

    #[test]
    fn closes_in_when_something_is_reclaimed() {
        let mut p = SweepPacer::new();
        p.observe(0);
        p.observe(0); // 4s
        p.observe(5);
        assert_eq!(p.delay, Duration::from_secs(2));
        p.observe(5);
        assert_eq!(p.delay, Duration::from_secs(1));
    }

    /// An idle cache must not back off forever, and a busy one must not spin.
    #[test]
    fn delay_stays_within_its_clamps() {
        let mut p = SweepPacer::new();
        for _ in 0..50 {
            p.observe(0);
        }
        assert_eq!(p.delay, SweepPacer::MAX, "idle cache must clamp at MAX");

        for _ in 0..50 {
            p.observe(1);
        }
        assert_eq!(p.delay, SweepPacer::MIN, "busy cache must clamp at MIN");
    }

    /// Jitter must stay inside ±25% — the point is to spread co-deployed nodes
    /// apart, not to occasionally sweep at a wildly wrong cadence.
    #[test]
    fn jitter_stays_within_25_percent_of_the_delay() {
        let mut p = SweepPacer::new();
        let base = p.delay;
        let lo = base - base / 4;
        let hi = base + base / 4;
        for _ in 0..1000 {
            let d = p.next_delay();
            assert!(d >= lo && d <= hi, "{d:?} outside [{lo:?}, {hi:?}]");
        }
    }

    /// Two nodes started at the same moment must not sweep in lockstep; that
    /// alignment is exactly what the jitter exists to prevent.
    #[test]
    fn jitter_actually_varies() {
        let mut p = SweepPacer::new();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            seen.insert(p.next_delay());
        }
        assert!(seen.len() > 1, "next_delay produced a constant");
    }

    /// `next_delay` must not underflow when the delay is at its minimum.
    #[test]
    fn min_delay_does_not_underflow() {
        let mut p = SweepPacer::new();
        for _ in 0..50 {
            p.observe(1);
        }
        assert_eq!(p.delay, SweepPacer::MIN);
        for _ in 0..100 {
            let _ = p.next_delay(); // must not panic
        }
    }
}
