# Falcon Cache — architecture & rationale

An **in-memory cache with TTL and a hard memory bound**. Everything it holds is
served from RAM at RAM speed, and it never exceeds the memory you give it —
under pressure it evicts the least recently used entries rather than growing.

- **Run:** `falcon serve` · **Keyspace:** `cache`
- **Core code:** [`cache.rs`](../crates/falcon-storage/src/cache.rs)

---

## 1. What it is — API surface

One product = one URL (`/cache`). Key, value, and optional TTL travel in a JSON
body; the operation is the HTTP method. There is no keyspace to name.

**Example — a login session that expires on its own.** Store the session under
its token with a 30-minute TTL; every request reads it back at RAM speed, and it
disappears automatically when it goes stale — no cleanup job, no stale logins.

### CLI
```bash
# session token -> the logged-in user, auto-expiring after 1800s (30 min)
falcon put "session:7f3a9c" '{"user":42,"role":"admin"}' --cache --ttl 1800
falcon get "session:7f3a9c" --cache          # → {"user":42,"role":"admin"}
falcon delete "session:7f3a9c" --cache       # e.g. on logout
```
### HTTP / REST
```bash
curl -X POST localhost:8080/cache -H 'content-type: application/json' \
     -d '{"key":"session:7f3a9c","value":"{\"user\":42,\"role\":\"admin\"}","ttl":1800}'
# → {"ok":true}
curl 'localhost:8080/cache?key=session:7f3a9c'   # → {"value":"{\"user\":42,\"role\":\"admin\"}"}
curl -X DELETE 'localhost:8080/cache?key=session:7f3a9c'
```
The cache is **exact-key lookup only — there is deliberately no scan/list.**
Entries expire and evict, so enumerating a cache would return a racy, partial
snapshot. If you need to list keys, that belongs in a durable store, not here.

Other natural fits: a rate-limit counter (`ratelimit:ip:1.2.3.4`, TTL 60),
a rendered page fragment, or a short-lived API token — anything hot, derived,
and safe to lose. `value` is a string — the client JSON-stringifies numbers or
objects into it and parses them back on read. The UI at `/` shows hit-rate,
hot keys/bytes, evictions, and TTL-tracked keys.

---

## 2. How it's built — the `cache` engine

Pure RAM. No write-ahead log, no replication log, no fsync, no disk spill, and
no second copy of a value anywhere — a cache's contents are regenerable, so
every durability mechanism is pure cost.

```
   put(k,v)                         get(k)
      │                                │
      ▼                                ▼
  ┌──────────────────────────────────────────────┐
  │ 64 shards, each an independently locked map  │
  │   HashMap<Arc<[u8]>, Entry>                  │
  │   Entry { value, last_access, expires_at }   │
  │   bytes  ← what the budget is enforced on    │
  └───────────────────┬──────────────────────────┘
                      │ over budget
                      ▼
        sample N entries at random
        drop the one with the oldest last_access
```

Keys route to a shard by hash. Each shard owns its own `RwLock`, its own byte
budget, and its own counters, so operations on different keys rarely contend.

**Reads take a shared lock.** Recency is an atomic stamp, so refreshing it needs
no exclusive borrow and any number of readers on a shard proceed together.

**Nothing on the read path is process-global.** The access clock and the hit and
miss counters are per shard, padded to their own cacheline, and summed only when
someone asks for stats. This matters more than it sounds: a single shared
`fetch_add` measures ~425 M ops/s on one thread but only ~39 M across eight, so
one global counter alone would cap read throughput regardless of anything else.

**The shard count adapts to capacity** — as many shards as fit while keeping each
at least 1 MB, capped at 512. Shards are independent LRU populations, so
splitting a small cache too finely leaves each with nothing to choose between
when sampling; at one entry per shard, recency stops working altogether.

- **A write** is a hash-table insert. It never touches a disk.
- **A read** is a hash-table lookup, one stamp update, and an `Arc` clone — the
  value bytes are never copied. The wire layer takes that `Arc` and writes it
  straight into the connection's output buffer, so a value is copied exactly
  once on its way out: into the socket buffer.
- **TTL** rides inside the entry, so there is no parallel expiry map and no
  second copy of the key.

### Eviction — approximated LRU by sampling

When a shard exceeds its budget it samples `evict_sample` entries at random and
drops the one with the oldest access stamp, repeating until it is back under.
This is what Redis does for `allkeys-lru`, and for the same reason: true LRU
requires mutating shared ordering state on *every read*, which is exactly the
contention a cache cannot afford. Sampling five entries gets close to true LRU;
sampling more approaches it at linear cost.

Expired entries found while sampling are dropped for free — they are dead weight
regardless of the budget.

### Connection memory

A connection's read and write buffers start at 8 KiB and grow only as deep
pipelining demands, then shrink back once a batch no longer needs the space.

The previous fixed sizing — a 64 KiB `BufReader`, a 64 KiB `BufWriter`, and two
64 KiB `BytesMut` buffers — reserved **256 KiB per connection**, so 64
connections held ~16 MiB of buffers against ~2 MiB of actually cached data. The
process footprint was dominated by empty buffer space, not by the cache.

Each socket read is still guaranteed a 32 KiB window. `read_buf` fills only the
buffer's *spare* capacity and the decoder drains completed frames off the front,
so a buffer left to itself settles at a few hundred spare bytes — and the loop
dispatches as soon as anything decodes, so a small read window turns one
pipelined batch into a reply per request. Writes suffer most, since a SET
request carries its value and its batch is several times larger than the
equivalent GET batch. Measured: reserving the floor is worth **+32% on SET
d=128** and **+21% on SET d=16**.

The two wrapper layers are gone as well, and not only for the memory: `read_buf`
already reads straight into the request buffer, and responses are accumulated in
one contiguous buffer and written as a single slice. Passing that slice through
a `BufWriter` copied the entire batch into a second buffer before the same
single `write` syscall — at depth=128 roughly 17 KiB per batch, about 0.6 GiB/s
of memcpy that bought nothing.

### The pipelined path

At a pipeline depth of 128 the per-operation constant dominates, so the hot ops
avoid everything they can:

- **No allocation per read.** `get_shared` hands back the engine's `Arc<[u8]>`.
  The former path built a `Vec` per GET and then copied it again into the output
  buffer — two allocations and two copies per operation.
- **No future per operation.** The cache never awaits (a read is a lock plus a
  hash lookup), so the entire engine API is synchronous and the wire layer serves
  a batch directly instead of building and polling 128 futures.
- **One keyspace resolution per batch**, not per operation.
- **No refcount traffic for empty fields.** A GET carries no value, so the
  decoder hands it `Bytes::new()` rather than an atomic-refcounted slice.

Measured at the engine, against the previous copying path: **1.35× on reads**
(7.35 M → 9.95 M ops/s) and **1.42× on writes** (9.76 M → 13.86 M ops/s), 128-byte
values, single-threaded.

### Multi-core reads

Concurrent reads scale with cores. Before shared-lock reads and per-shard
counters they did the opposite — throughput *fell* as threads were added:

| Threads | Before | After |
|---:|---:|---:|
| 1 | 17.2 M ops/s | 17.0 M ops/s |
| 2 | 14.1 M | 22.9 M |
| 4 | 16.5 M | 35.2 M |
| 8 | 10.6 M | **37.4 M** |

A benchmark pinned to a single CPU cannot see this; a real multi-core deployment
very much can.

### Removals are observable

Eviction and TTL expiry do not go through `delete`, so without help they would
be invisible from outside the engine: nothing could tell that a key it was told
about is now gone. The engine therefore reports every removal it initiates
through an `EvictionListener`, called with no shard lock held so the handler can
do real work without blocking the cache.

---

## 3. Why it's built this way — the reasoning

**Why no durability at all?** A cache that fsyncs is paying a store's costs for a
cache's guarantees. The previous engine wrote every value to sled *and* cached it
in RAM — twice the memory, three times counting a replication log that
re-encoded key and value on every write and was never truncated. That log grew
without bound until the kernel OOM-killed the process, and the per-write fsync
put ~6 ms on the write path. All of it bought durability a cache does not want.
Falcon already has a durable KV Store for data that must survive.

**Why is the memory bound hard?** Three things are charged against
`capacity-mb`: value bytes, key bytes, and a deliberately over-estimated
per-entry overhead. Undercounting any of them is what turns a "bounded" cache
into an OOM. When a shard is over budget it sheds entries until it is not —
there is no configuration under which the cache grows without limit.

**Why sampling instead of a CLOCK ring?** An earlier implementation kept a `Vec`
ring with a sweep hand. Because `swap_remove` reshuffles entries under that hand,
a key could be examined twice in a single sweep — its second chance cleared and
then immediately consumed — so the *hottest* keys were evicted first. Sampling
has no ring, no hand, and no position: that class of bug cannot occur.

**Why a logical access counter rather than a timestamp?** Redis stamps seconds,
which is fine across the hours a real cache runs but carries no signal at all
within one second — every entry ties and sampled LRU degenerates into sampled
random. A counter orders accesses exactly, at any timescale.

**Why no scan?** Entries expire and evict, so enumerating a cache returns a racy
partial snapshot. Listing belongs in a durable store, not in a cache.

---

## 4. Storage on disk

**None.** The cache writes nothing and reads nothing from disk, and starts empty
after a restart. There is no data directory, no volume, and no on-disk format —
cache contents are regenerable, so every durability mechanism would be pure cost.
Anything that must survive belongs in your system of record, behind the cache.

## 5. TTL
Each entry stores its own `expires_at` **inline**, so TTL costs no second map and
no second copy of the key. An expired entry is dropped the moment it is touched,
is preferred as an eviction victim when sampling finds one, and is swept by a
background task every 5 seconds so dead data never accumulates even in a cache
that stays under its budget. Set a node default with `falcon config set default-ttl <secs>`, or a
per-write TTL via `--ttl` / `?ttl=` (which overrides the default).

## 6. Configuration

| Key | Effect | Why |
|-----|--------|-----|
| `capacity-mb` | **Hard** RAM bound: values + keys + per-entry overhead. Unset = auto-size from detected memory | the cache will not exceed this; leaving it unset is usually right |
| `default-ttl` | default key expiry in seconds (0 = never) | per-write `?ttl=` overrides it |
| `evict_sample` | entries sampled per eviction (default 8; engine config only) | bigger = closer to true LRU, more work per evict |

Set the first two with `falcon config set <key> <value>`, or override either for
one run with `falcon serve --capacity-mb N` / `--default-ttl N`. `falcon config
set capacity-mb auto` hands sizing back to the machine.

## 7. Benchmarks

What changed structurally when the write-through engine was replaced:

| Path | Before (write-through to sled) | Now |
|------|-------------------------------|-----|
| Write | durable commit + fsync + write-ahead-log append | one hash-table insert |
| Read (hit) | map lookup + full value copy | map lookup + `Arc` clone (no copy) |
| Memory per hot key | value in RAM **and** on disk, plus an untruncated log | one copy |

**Measured on an Apple M5 (10 cores, 16 GB, macOS 26), `--release` + LTO.**
Throughput is **concurrent** (8 connections), so it reflects real capacity, not
single-thread. Neither path touches a disk.

| Path | Throughput | p50 | p99 |
|------|-----------:|----:|----:|
| Wire GET, pipeline depth=128 | **8.74 M ops/sec** | 106 µs¹ | 197 µs¹ |
| Wire GET, pipeline depth=16 | **2.32 M ops/sec** | 52 µs¹ | 97 µs¹ |
| Wire GET, depth=1 (no pipeline) | 176 K ops/sec | 44 µs | 80 µs |
| HTTP GET (JSON, 1 req/op) | 69 K ops/sec | 107 µs | 222 µs |
| HTTP PUT (JSON, 1 req/op) | 78 K ops/sec | 96 µs | 187 µs |

¹ *Pipelined rows report **per-batch** latency (batch = `depth` ops); throughput
is aggregate.*

Sustained mixed load (8 s, 64 connections, depth 16, 50 % writes):
**2.98 M ops/sec**, p50 329 µs / p99 650 µs per batch — reported **STABLE (no
latency cliff / queue buildup)**.

Reproduce with:

```bash
cargo build --release -p falcon-cli -p falcon-bench
falcon-bench --skip-writes --pipeline-depths 1,16,128   # read path
falcon-bench --load-test --load-secs 8 --load-conns 64  # sustained load
```

## 8. Single-node by design

Falcon Cache is one process holding one cache. It does not replicate, cluster,
or shard across nodes: it has no ordered log to stream to a follower, and there
is little value in shipping another region's writes into a cache that refills
from its own misses anyway.

Run one cache per region (or per service), each filling from its own misses
against whatever system of record sits behind it. Scale it by giving the process
more memory and more cores — it uses every core it is given.

## 9. Guarantees
- **One copy per key.** The value's bytes exist in exactly one place.
- **Hard memory bound.** Resident memory stays within `capacity-mb` under
  any load; under pressure the cache sheds entries rather than growing.
- **No durability.** Nothing is written to disk and the cache starts empty after
  a restart — by design. Anything that must survive belongs in your system of
  record, with the cache in front of it.
- **Writes never touch disk.** A write is a hash-table insert.
- **Every removal is reported.** Eviction and expiry both notify the
  `EvictionListener`, so removals the client never asked for are still visible.
- Expired keys are dropped on access and swept in the background; per-write TTL
  always wins.
