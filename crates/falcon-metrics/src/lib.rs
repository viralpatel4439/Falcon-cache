#![forbid(unsafe_code)]

//! Falcon's zero-dependency metrics core: lock-free atomic counters and
//! gauges plus a fixed-bucket latency histogram, rendered to the Prometheus
//! text exposition format. No external crate, no background thread, no
//! allocation on the hot path — incrementing a counter is one relaxed atomic
//! add, so instrumenting the request path is effectively free.
//!
//! The design goal is **autoscale-grade observability at zero hot-path
//! cost**: an HPA/KEDA autoscaler scrapes `/metrics` and scales on real
//! throughput, latency, and error signals; when nobody scrapes, the only
//! cost is the atomic adds, which are negligible next to the actual work.

mod histogram;
mod registry;

pub use histogram::{Histogram, LatencyTimer};
pub use registry::{Counter, Gauge, Metrics};
