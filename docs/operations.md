# Operations runbook — running Falcon Cache

The cache is pure RAM: no WAL, no fsync, no disk spill. That removes most of
what usually makes a data service risky to operate, and leaves exactly one thing
that matters — **memory**.

## The one thing to get right: memory

`capacity-mb` is a **hard bound on the entries the cache holds**, enforced
per shard on every write. Over the bound, the cache samples entries and evicts
the least recently used rather than growing.

Give the container a ceiling **above** that budget, not equal to it: the process
also holds connection buffers, the HTTP stack, and allocator slack. If the
container limit is at or below the cache budget, the kernel OOM-kills the
process before the cache ever gets to evict — turning a graceful eviction into a
hard restart. A reasonable starting point is a container limit ~2× the cache
budget, then tune against your runtime's own memory metric (Falcon does not
export process RSS itself; use cgroup/container metrics for that).

Losing the cache is survivable by design (it starts empty and refills from
misses), but a restart loop still shows up as a latency cliff at your origin.

## Metrics to watch (`GET /metrics`, Prometheus text)

| Metric | Meaning | Alert |
|--------|---------|-------|
| `falcon_ready` | 1 when the node is serving | `== 0` for more than a few seconds after start |
| `falcon_kv_get_hit_total` / `falcon_kv_get_miss_total` | The hit rate — the cache's entire reason to exist | A falling hit rate usually means the working set outgrew `capacity-mb`, or TTLs are too short |
| `falcon_kv_get_latency_seconds` | Server-observed read latency | Should stay flat; a RAM lookup does not degrade with dataset size |
| `falcon_wire_requests_rejected_total` | Wire writes rejected for exceeding `max_value_bytes` | Nonzero = a client is sending oversized values; check the client, not the server |
| `falcon_wire_idle_timeouts_total` | Connections closed on idle timeout | A steady rise can indicate leaking/half-open clients |

Eviction and hit-rate detail per keyspace is also on `GET /health`
(`keyspaces[].cache` — hit rate, keys, bytes, evictions, expired), which is what
the UI at `/` renders. Sustained growth in `evictions` is the signal that the
working set no longer fits in `capacity-mb`.

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
- `capacity-mb` bounds the cached entries, not total process RSS;
  connection buffers and allocator behaviour sit outside it (hence the headroom
  advice above).
