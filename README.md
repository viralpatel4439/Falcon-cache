# Falcon Cache

An **in-memory cache with TTL and a hard memory bound**, written in Rust.
Everything it holds is served from RAM at RAM speed, and it never exceeds the
memory you give it — under pressure it evicts the least recently used entries
rather than growing.

It runs over two protocols (a pipelined binary TCP protocol and REST) and can
serve both over **TLS** — all configured through the CLI, never environment
variables.

It **sizes itself to the machine it runs on**: given no explicit capacity it
takes a share of the memory the process actually has (the container limit if
there is one, else host RAM), shards for the core count it finds, and paces its
own TTL cleanup against how much there is to clean.

Every crate is `#![forbid(unsafe_code)]` (zero unsafe).

---

## Table of contents

- [Quickstart](#quickstart)
- [Using the cache](#using-the-cache)
- [Configuration (CLI only)](#configuration-cli-only)
- [Storage: pure RAM](#storage-pure-ram)
- [Protocols & TLS](#protocols--tls)
- [Operations](#operations)
- [Benchmarks](#benchmarks)
- [Building & testing](#building--testing)
- [Architecture](#architecture)

---

## Quickstart

Pick whichever you prefer — both give you a node listening on `:8080` (REST)
and `:6380` (binary protocol). There is no data directory, no capacity to pick,
and no config file to write: the node sizes itself to the machine.

### Option A — Docker (nothing to install but Docker)

```bash
docker build -f docker/Dockerfile -t falcon-cache .
docker run -d --name falcon-cache -m 512m -p 8080:8080 -p 6380:6380 falcon-cache
```

`-m 512m` **is** the memory configuration — the cache reads the container's own
limit at startup and sizes itself under it. Or use the shortcut:

```bash
make docker-run     # build, run, and wait until it answers /healthz
```

### Option B — Install the binary

Needs a Rust toolchain ([rustup.rs](https://rustup.rs)); the exact compiler
version is pinned by the repo, so you don't have to pick one.

```bash
make install        # cargo install --path crates/falcon-cli
falcon serve
```

If `falcon: command not found`, add cargo's bin directory to your `PATH`:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

To build without installing, use `make build` and run
`./target/release/falcon serve`.

### Use it

```bash
# The same binary is the client. A login session that expires on its own after
# 30 minutes — the classic use for a cache with TTL:
falcon put "session:7f3a9c" '{"user":42,"role":"admin"}' --ttl 1800
falcon get "session:7f3a9c"      # → {"user":42,"role":"admin"}
falcon del "session:7f3a9c"      # e.g. on logout
falcon status                    # current config
falcon health
```

The client subcommands read the node's address and API key from your profile, so
`falcon config set http-bind 0.0.0.0:9090` moves the client along with the
server. Point them elsewhere with `--addr http://host:port` (and `--api-key`)
when talking to a remote node.

Run `make` on its own to see every available task.

```bash
# …or over plain HTTP — one product = one URL, key+value in a JSON body:
curl -X POST localhost:8080/cache -H 'content-type: application/json' \
     -d '{"key":"session:7f3a9c","value":"{\"user\":42}","ttl":1800}'
curl 'localhost:8080/cache?key=session:7f3a9c'   # → {"value":"{\"user\":42}"}
curl -X DELETE 'localhost:8080/cache?key=session:7f3a9c'
curl localhost:8080/health                       # active product + feature set
```

> **Values are strings.** A value can be a number, string, or object — the
> client JSON-stringifies it into `value` and parses it back on read, so the API
> stays schema-free and you never manage keyspaces, partitions, or offsets.

---

## Using the cache

The cache is **exact-key lookup only — there is deliberately no scan/list.**
Entries expire and evict, so enumerating a cache would return a racy, partial
snapshot. Listing belongs in your system of record.

Natural fits: a session token (as above), a rate-limit counter
(`ratelimit:ip:1.2.3.4`, TTL 60), a rendered page fragment, or a short-lived API
token — anything hot, derived, and safe to lose.

Full detail — the engine, eviction, and the reasoning behind each choice — is in
**[docs/cache.md](docs/cache.md)**.

---

## Configuration (CLI only)

Falcon **never reads environment variables**. All settings live in a single
profile file (`~/.falcon/profile.toml`), written only through:

- the CLI — `falcon config set <key> <value>` / `get` / `list`.

```bash
falcon config set capacity-mb 512        # pin the bound; omit to auto-size
falcon config set capacity-mb auto       # hand sizing back to the machine
falcon config set http-bind 0.0.0.0:9090
falcon config set api-key s3cret
falcon config list                       # every key + current value
falcon status                            # build + settings
```

`falcon serve` loads the profile; its flags (`--http-bind`, `--wire-bind`,
`--capacity-mb`, `--default-ttl`, `--node-id`, `--region`, `--log-level`)
override the profile **for one run**. Order: **profile < serve flags**.
If no profile exists yet, the node simply starts on defaults.

**Concurrency is automatic — there is no thread/worker/core knob.** On start,
Falcon sizes a multi-threaded, work-stealing runtime to the machine: one async
worker per logical CPU. The scheduler work-steals to balance load, so the
runtime adapts to traffic on its own. The chosen worker count is logged at
startup. Every operation is served from RAM with no I/O to await, so nothing is
ever offloaded to a blocking pool.

### Config reference

| Key | Example | Controls |
|-----|---------|----------|
| `capacity-mb` | `512` / `auto` | **Hard** RAM bound. `auto` (the default) derives it from detected memory; an explicit value always wins. |
| `default-ttl` | `300` | Default TTL in seconds for writes that omit one. `0` = never expire. |
| `node.id` | `us-1` | Node identity (used in logs). |
| `region` | `us-east-1` | Region label (display). |
| `http-bind` | `0.0.0.0:8080` | REST address. |
| `wire-bind` | `0.0.0.0:6380` | Binary protocol address (if `wire-enabled`). |
| `wire-enabled` | `true` | Turn the binary protocol on/off. |
| `api-key` | `s3cret` | Shared secret required on every connection. Empty = auth off. |
| `log-level` | `info` | `error`/`warn`/`info`/`debug`/`trace`. |
| `tls-enabled` | `true` | In-process TLS on **both** hops. Off by default. |
| `tls-cert` / `tls-key` | `/path/*.pem` | PEM cert chain + private key. |

Engine-internal tuning (`evict_sample`, `max_value_bytes`, `max_key_bytes`,
`max_connections`, `idle_timeout_secs`) is documented in
[`config/default.toml`](config/default.toml) and reachable via
`falcon serve --config <file>`.

The profile file is written `0600` in a `0700` directory, since it holds the
API key in plaintext.

---

## Storage: pure RAM

The cache is **pure RAM**: no write-ahead log, no fsync, no disk spill, and no
second copy of a value anywhere. It writes nothing to disk and starts empty
after a restart — by design, since cache contents are regenerable. There is no
data directory to configure and no volume to mount.

Anything that must survive a restart belongs in your system of record, with
Falcon in front of it.

---

## Protocols & TLS

Falcon uses the right protocol for each hop rather than one everywhere:

| Hop | Protocol | Why |
|-----|----------|-----|
| client ↔ service (hot path) | binary TCP, pipelined | lowest latency for small ops (µs-scale); one persistent stream |
| client ↔ service (REST) | HTTP/1.1 + HTTP/2 | ubiquitous, browser + curl friendly |

Both hops keep **persistent connections**, so this optimizes the per-op path.

**TLS on both hops (optional, off by default).** Turn it on once and both
listen encrypted:

```bash
falcon config set tls-enabled true
falcon config set tls-cert /path/cert.pem
falcon config set tls-key  /path/key.pem
falcon serve            # HTTPS + binary-over-TLS
```

TLS is terminated **in process** with rustls (pure-Rust, AES-NI accelerated) —
not via an extra proxy hop. On persistent connections the handshake is a
one-time per-connection cost and per-record encryption adds only single-digit
microseconds, so the low-latency hot path is preserved.

### API key (optional auth)

Set `falcon config set api-key "..."` and **every** connection must present it:

- **HTTP/REST**: `Authorization: Bearer <key>` (or `?api_key=<key>`; `/healthz` exempt)
- **Binary wire**: an `AUTH` frame first, before any other op

The key is compared in constant time; when unset, auth is fully off.

---

## Self-tuning

Falcon has no thread, core, shard, or sweep-interval knob, and it does not need
a capacity either. What it can work out from the machine, it does:

| What | How it decides | Override |
|------|----------------|----------|
| **Memory** | ~70% of the ceiling this process actually has — a cgroup limit when containerized, else host RAM — leaving headroom for the runtime, buffers, and allocator slack. | `capacity-mb` |
| **Cores** | One async worker per logical CPU, work-stealing across them. | none — automatic |
| **Shards** | From core count *and* capacity: enough shards that concurrent writers rarely collide, never so many that a shard has too few entries to pick a good eviction victim. | none — automatic |
| **Cleaning** | The TTL sweep paces itself: it backs off when it finds nothing to reclaim, closes in when it does, skips shards holding no expiring entries at all, and jitters so co-deployed nodes don't sweep in lockstep. | none — automatic |

The resolved capacity, its source, the core count, and the shard count are all
logged at startup:

```
cache capacity resolved keyspace=cache capacity_mb=1434 source="cgroup-v2" cores=8
cache sharded for this machine keyspace=cache shards=32
```

Detection failing is not an error — the cache falls back to a 256 MB default and
says so (`source="fallback-default"`). Setting `capacity-mb` explicitly skips
detection entirely.

---

## Operations

Falcon Cache is built to run as a single autoscalable container. Probes live at
`/healthz` (liveness) and `/readyz` (readiness); `/health` returns live cache
statistics as JSON — hit rate, keys, bytes, evictions, and expiries.

Because the cache is pure RAM, **memory is the one thing to size correctly** —
auto-sizing already leaves headroom below the container limit, so the usual
failure — sizing the cache to the whole container and being OOM-killed before it
can evict — does not arise unless you override it. The full runbook is in
**[docs/operations.md](docs/operations.md)**.

---

## Benchmarks

**Measured on an Apple M5 (10 cores, 16 GB, macOS 26), `--release` + LTO.**
Throughput figures are **concurrent** (many connections at once), so they
reflect real capacity, not single-thread. Both reads and writes are served
entirely from RAM; there is no disk I/O on either path.

> These are recorded results, not a reproducible target: the load-testing
> harness that produced them is no longer part of this repo. Treat them as an
> indication of the design's shape — pipelining dominates, and neither path
> touches a disk — and measure your own workload before relying on a number.

| Path | Throughput | p50 | p99 |
|------|-----------:|----:|----:|
| Wire GET, pipeline depth=128 | **8.74 M ops/sec** | 106 µs¹ | 197 µs¹ |
| Wire GET, pipeline depth=16 | **2.32 M ops/sec** | 52 µs¹ | 97 µs¹ |
| Wire GET, depth=1 (no pipeline) | 176 K ops/sec | 44 µs | 80 µs |
| HTTP GET (JSON, 1 req/op) | 69 K ops/sec | 107 µs | 222 µs |
| HTTP PUT (JSON, 1 req/op) | 78 K ops/sec | 96 µs | 187 µs |

¹ *In the pipelined rows, latency percentiles are **per batch** (batch = `depth`
ops); throughput is aggregate.*

Sustained mixed load (8 s, 64 connections, depth 16, 50 % writes):
**2.98 M ops/sec**, p50 329 µs / p99 650 µs per batch, reported **STABLE (no
latency cliff / queue buildup)**.

---

## Building & testing

```bash
make build     # release binary at target/release/falcon
make test      # the whole workspace
make check     # fmt + clippy (-D warnings) + test — run this before committing
```

Or with cargo directly:

```bash
cargo build --release
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

The toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml), so
local builds, CI, and the Docker image all compile with the same compiler.

---

## Architecture

Start with **[docs/architecture.md](docs/architecture.md)** — the crate layout
and the synchronous write path. Then **[docs/cache.md](docs/cache.md)** explains
the cache engine itself and **why** it is built that way, and
**[docs/operations.md](docs/operations.md)** covers running it.

## License

MIT — see [LICENSE](LICENSE).
