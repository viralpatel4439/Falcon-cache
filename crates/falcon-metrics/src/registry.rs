//! The Falcon metrics registry: a fixed, statically-named set of counters,
//! gauges, and latency histograms plus a Prometheus text encoder. A fixed
//! schema (rather than a dynamic map) keeps access lock-free — a metric is a
//! plain field, so the hot path never hashes a name or takes a lock.

use crate::histogram::{Histogram, BUCKET_BOUNDS_US};
use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonically increasing counter (relaxed atomic add).
#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    /// Set the counter to an externally-tracked cumulative total. Only valid for
    /// counters mirrored from a monotonic source refreshed on scrape (e.g. the
    /// storage/messaging process-wide totals) — a counter must never decrease.
    pub fn set(&self, v: u64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A value that can go up or down (bytes resident, connections, WAL size).
#[derive(Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    pub fn set(&self, v: u64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// The whole process's metrics. Held behind an `Arc` in shared state and
/// referenced from every hot path. Cheap to construct; all-zero initially.
pub struct Metrics {
    // KV operation counts.
    pub kv_get_total: Counter,
    pub kv_put_total: Counter,
    pub kv_delete_total: Counter,
    pub kv_get_hit_total: Counter,
    pub kv_get_miss_total: Counter,

    // KV operation latency (server-observed, whole op).
    pub kv_get_latency: Histogram,
    pub kv_put_latency: Histogram,
    pub kv_delete_latency: Histogram,

    // Connections / requests.
    pub http_requests_total: Counter,
    pub http_requests_rejected_total: Counter,
    /// Wire-protocol writes rejected for exceeding `max_value_bytes` (E2). Lets
    /// you see a client hitting the cap over the binary protocol, which was
    /// previously invisible (only REST rejections were countable).
    pub wire_requests_rejected_total: Counter,
    pub wire_connections: Gauge,
    /// Wire connections closed because they idled past `wire.idle_timeout_secs`
    /// (F2). A steady rise can indicate leaking/half-open clients.
    pub wire_idle_timeouts_total: Counter,

    // Process readiness (1 = ready). Rendered as a gauge.
    pub ready: Gauge,
}

impl Metrics {
    pub fn new() -> Self {
        let m = Self {
            kv_get_total: Counter::default(),
            kv_put_total: Counter::default(),
            kv_delete_total: Counter::default(),
            kv_get_hit_total: Counter::default(),
            kv_get_miss_total: Counter::default(),
            kv_get_latency: Histogram::new(),
            kv_put_latency: Histogram::new(),
            kv_delete_latency: Histogram::new(),
            http_requests_total: Counter::default(),
            http_requests_rejected_total: Counter::default(),
            wire_requests_rejected_total: Counter::default(),
            wire_connections: Gauge::default(),
            wire_idle_timeouts_total: Counter::default(),
            ready: Gauge::default(),
        };
        m.ready.set(0);
        m
    }

    /// Render the whole registry in Prometheus text exposition format.
    pub fn encode_prometheus(&self) -> String {
        let mut out = String::with_capacity(4096);

        let counters: &[(&str, &str, u64)] = &[
            ("falcon_kv_get_total", "Total KV GET operations", self.kv_get_total.get()),
            ("falcon_kv_put_total", "Total KV PUT operations", self.kv_put_total.get()),
            ("falcon_kv_delete_total", "Total KV DELETE operations", self.kv_delete_total.get()),
            ("falcon_kv_get_hit_total", "GETs that found a value", self.kv_get_hit_total.get()),
            ("falcon_kv_get_miss_total", "GETs that missed", self.kv_get_miss_total.get()),
            ("falcon_http_requests_total", "HTTP requests served", self.http_requests_total.get()),
            ("falcon_http_requests_rejected_total", "HTTP requests rejected (auth/size)", self.http_requests_rejected_total.get()),
            ("falcon_wire_requests_rejected_total", "Wire-protocol writes rejected (value too large)", self.wire_requests_rejected_total.get()),
            ("falcon_wire_idle_timeouts_total", "Wire connections closed on idle timeout", self.wire_idle_timeouts_total.get()),
        ];
        for (name, help, val) in counters {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} counter\n{name} {val}\n"));
        }

        let gauges: &[(&str, &str, u64)] = &[
            ("falcon_wire_connections", "Open binary-protocol connections", self.wire_connections.get()),
            ("falcon_ready", "1 when the node is ready to serve, else 0", self.ready.get()),
        ];
        for (name, help, val) in gauges {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n{name} {val}\n"));
        }

        let hists: &[(&str, &str, &Histogram)] = &[
            ("falcon_kv_get_latency_seconds", "GET latency", &self.kv_get_latency),
            ("falcon_kv_put_latency_seconds", "PUT latency", &self.kv_put_latency),
            ("falcon_kv_delete_latency_seconds", "DELETE latency", &self.kv_delete_latency),
        ];
        for (name, help, hist) in hists {
            encode_histogram(&mut out, name, help, hist);
        }

        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

fn encode_histogram(out: &mut String, name: &str, help: &str, hist: &Histogram) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
    for (bound_us, cum) in hist.cumulative() {
        // Prometheus expects seconds for *_seconds histograms.
        let le = bound_us as f64 / 1_000_000.0;
        out.push_str(&format!("{name}_bucket{{le=\"{le}\"}} {cum}\n"));
    }
    out.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {}\n", hist.count()));
    let sum_seconds = hist.sum_us() as f64 / 1_000_000.0;
    out.push_str(&format!("{name}_sum {sum_seconds}\n"));
    out.push_str(&format!("{name}_count {}\n", hist.count()));
    // Reference the bounds table so the constant stays "used" even if the
    // schema is trimmed; also documents the resolution in the output.
    debug_assert!(!BUCKET_BOUNDS_US.is_empty());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_counters_gauges_and_histograms() {
        let m = Metrics::new();
        m.kv_get_total.add(3);
        m.kv_get_hit_total.add(2);
        m.ready.set(1);
        m.kv_get_latency.observe_us(42);

        let text = m.encode_prometheus();
        assert!(text.contains("falcon_kv_get_total 3"));
        assert!(text.contains("falcon_kv_get_hit_total 2"));
        assert!(text.contains("falcon_ready 1"));
        assert!(text.contains("# TYPE falcon_kv_get_latency_seconds histogram"));
        assert!(text.contains("falcon_kv_get_latency_seconds_count 1"));
        assert!(text.contains("le=\"+Inf\""));
    }
}
