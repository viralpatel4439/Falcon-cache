# Operations runbook — running Falcon Cache

The cache is pure RAM: no WAL, no fsync, no disk spill. That removes most of
what usually makes a data service risky to operate, and leaves exactly one thing
that matters — **memory**.

## The one thing to get right: memory

`capacity-mb` is a **hard bound on the entries the cache holds**, enforced
per shard on every write. Over the bound, the cache samples entries and evicts
the least recently used rather than growing.

**By default you do not set it.** Falcon takes ~70% of the memory the process
actually has — the cgroup limit when containerized, host RAM otherwise —
leaving the rest for connection buffers, the HTTP stack, and allocator slack.
Just give the container the memory you want the cache to use and let it size
itself; `docker run -m 2g` is the whole configuration.

Check what it picked in the startup log:

```
cache capacity resolved keyspace=cache capacity_mb=1434 source="cgroup-v2" cores=8
```

`source` tells you which ceiling it found — `cgroup-v2`/`cgroup-v1` (a container
limit), `host-total` (no container limit), `explicit` (you set it), or
`fallback-default` (nothing detectable, 256 MB).

**If you override it**, give the container a ceiling well above the budget, not
equal to it. A limit at or below the cache budget means the kernel OOM-kills the
process before the cache ever gets to evict — turning graceful eviction into a
hard restart. Falcon does not export process RSS; use your runtime's own
container memory metric for that.

Losing the cache is survivable by design (it starts empty and refills from
misses), but a restart loop still shows up as a latency cliff at your origin.

## What to watch (`GET /health`)

There is no Prometheus endpoint: a cache's operational story is small enough to
read straight off the engine, so `/health` returns it as JSON, unauthenticated,
with no scrape configuration to maintain.

```bash
curl -s localhost:8080/health | jq '.keyspaces[0].cache'
```

| Field | Meaning | Watch for |
|-------|---------|-----------|
| `hit_rate` | The cache's entire reason to exist | A falling rate usually means the working set outgrew `capacity-mb`, or TTLs are too short |
| `evictions` | Entries dropped to stay in budget | Sustained growth = the working set no longer fits; raise `capacity-mb` or give the container more memory |
| `expired` | Entries dropped because their TTL passed | Healthy churn, not a problem in itself |
| `keys` / `bytes` | Live entries and the RAM they account for | `bytes` plateauing at the budget is eviction working as designed |
| `ttl_tracked_keys` | Keys carrying an expiry | Zero means the TTL sweep costs nothing at all |

Readiness is a separate signal: `/readyz` returns 503 until startup completes.

## Probes

- **`/healthz`** — liveness. 200 while the process is up. Unauthenticated.
- **`/readyz`** — readiness. 503 until startup completes, then 200. Route
  traffic on this one.

Use liveness to restart and readiness to route: they are deliberately different
signals, and wiring a load balancer to `/healthz` sends traffic to a node that
is up but not yet serving.

## Shutdown

SIGTERM drains in-flight requests and stops accepting new connections. There is
**no final flush to wait on** — the cache holds nothing durable — so shutdown is
as fast as the in-flight requests allow.

## Restart behaviour

A restarted cache starts **empty**, by design. Expect a miss spike and a
corresponding load spike at whatever system of record sits behind the cache;
size that system for the cold-start burst, or stagger restarts across replicas
so they don't all refill at once.

## Not covered (known limits)

These are honest gaps, not guarantees:
- The **number** of concurrent connections is not capped in-process — bound it
  at the deployment layer (LB / `ulimit -n` / k8s limits).
- `capacity-mb` bounds the cached entries, not total process RSS; connection
  buffers and allocator behaviour sit outside it. Auto-sizing's headroom is what
  covers that gap — an explicit `capacity-mb` opts out of it.
- Memory auto-detection is Linux-only (it reads cgroup files and `/proc`). Other
  platforms fall back to the 256 MB default, which the startup log reports as
  `source="fallback-default"`.
