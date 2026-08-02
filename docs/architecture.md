# Falcon Cache — architecture index

Falcon Cache ships as one Rust binary exposing a single product: an in-memory
cache with TTL and a hard memory bound.

| Doc | What it covers |
|-----|----------------|
| **[cache.md](cache.md)** | The cache itself — API surface, the sharded engine, eviction, TTL, and the reasoning behind each choice. **Start here.** |
| **[operations.md](operations.md)** | Running it: probes, health statistics, graceful shutdown, and container sizing. |

For the serve model, protocols, TLS, and auth, see the top-level
[README](../README.md).

---

## The shape of the system

Five crates, each with one job:

```
  falcon-cli      the `falcon` binary: config, serve, and a client
       │
       ├── falcon-api     HTTP/REST + health probes  ─┐
       │                                              ├─▶ falcon-core ──▶ falcon-storage
       └── falcon-wire    binary TCP, pipelined      ─┘   (Node/Keyspace   (CacheEngine)
                                                            + resources)
```

Both protocol layers hold an `Arc<Node>` and go through `Keyspace`; neither
reaches into the engine directly.

### Detection is policy; the engine is mechanism

`falcon-core::resources` is the only code that looks at the machine — cgroup
limits, `/proc/meminfo`, core count. It resolves those into plain numbers, and
`Node::build` hands them to the engine.

`falcon-storage` therefore reads no files, no environment, and no config. It is
a deterministic data structure that does what its `CacheOptions` say, which is
what makes its behaviour testable without reference to the machine the tests run
on.

### One engine, no abstraction over it

`Keyspace` holds an `Arc<CacheEngine>` — a concrete type, not a trait object.

There was previously a `StorageEngine` trait with async methods, so a durable
tier and an object-store tier could be swapped in by config. Both are gone, and
a trait with one implementor is pure indirection: it cost a vtable dispatch per
operation, forced a downcast whenever the keyspace needed the real engine, and
made every call `async` for an engine that never awaits.

The whole path is now **synchronous**. A read is a shard lock plus a hash
lookup; a write is a lock plus an insert. Neither touches disk or the network,
so there is nothing to await — and a pipelined batch of depth N is served
without constructing and polling N futures of pure bookkeeping.

### Sizing itself to the machine

Every sizing decision used to be a compile-time constant or a static default,
which meant the cache behaved identically on a 1 GB container and a 512 GB host.
Now:

- **Capacity** — unset by default; resolved from the cgroup limit, else host
  RAM, taking ~70% and reserving headroom. An explicit setting always wins.
- **Shards** — derived from core count *and* capacity. Capacity alone was the
  old rule, which gave a small cache a single shard (one global lock, however
  many cores) and gave the same count on a 2-core box as a 128-core one.
- **Cleaning** — the TTL sweep paces itself against what it reclaims and skips
  shards with nothing to expire.

### What a write does

```
  put(k, v, ttl?) ──▶ Keyspace       resolves the TTL (per-write, else the
        │                            keyspace default) into an absolute expiry
        ▼
   CacheEngine ──▶ hash to a shard ──▶ insert ──▶ evict if over budget
```

That is the entire write path. There is no write-ahead log, no replication log,
no fsync, and no second copy of the value anywhere.

**TTL lives inside the entry.** Each entry carries its own `expires_at`, rather
than a `key → expiry` map beside the engine. A parallel map would mean a second
copy of every key, two structures to keep in step, and a reaper walking keys the
engine had already dropped. Expiry is enforced on read and reclaimed by a
background sweep.

**Two atomics keep the hot path off the lock.** Each shard mirrors its byte
count and its count of expiring entries outside the `RwLock`, republished inside
it on every change. A write that leaves the shard under budget never takes the
exclusive lock to discover that, and the sweep skips a shard with no expiring
entries without locking it. Both mirrors are hints — the values inside the lock
stay authoritative, and are re-checked there before anything is evicted.

### Removals are observable

The cache drops keys on its own initiative — eviction under memory pressure and
TTL expiry — and those removals do not pass through `delete`. The
`EvictionListener` hook exists so they can still be observed, and it is called
with no shard lock held so an implementation can do real work without blocking
the cache.

### Nothing is durable

The cache writes **nothing** to disk: no data directory, no volume, no files.
A restart starts empty, by design — cache contents are regenerable, so every
durability mechanism is pure cost. This is also why shutdown has nothing to
flush; draining in-flight requests is the whole of it.

The one file Falcon writes is the profile (`~/.falcon/profile.toml`), which is
configuration, not data.
