# Falcon Cache — architecture index

Falcon Cache ships as one Rust binary exposing a single product: an in-memory
cache with TTL and a hard memory bound.

| Doc | What it covers |
|-----|----------------|
| **[cache.md](cache.md)** | The cache itself — API surface, the sharded engine, eviction, TTL, and the reasoning behind each choice. **Start here.** |
| **[operations.md](operations.md)** | Running it: probes, metrics, graceful shutdown, and container sizing. |

For the serve model, protocols, TLS, and auth, see the top-level
[README](../README.md).

---

## The shape of the system

Four crates, each with one job:

```
  falcon-cli      the `falcon` binary: config, serve, and a client
       │
       ├── falcon-api     HTTP/REST + the embedded UI  ─┐
       │                                                ├─▶ falcon-core ──▶ falcon-storage
       └── falcon-wire    binary TCP, pipelined        ─┘   (Node/Keyspace)    (CacheEngine)
```

Both protocol layers hold an `Arc<Node>` and go through `Keyspace`; neither
reaches into the engine directly. `falcon-metrics` is a leaf both use to record
counters.

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
