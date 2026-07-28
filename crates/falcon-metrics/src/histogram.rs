//! A fixed-bucket, lock-free latency histogram in the Prometheus cumulative
//! style. Buckets are hard-coded microsecond upper bounds spanning sub-µs to
//! multi-second — the range a KV op latency lives in — so recording an
//! observation is a small linear scan of relaxed atomic adds with no
//! allocation and no lock.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Upper bounds in **microseconds** (cumulative "le" buckets). Chosen to give
/// useful resolution around typical KV latencies (tens of µs to low ms) while
/// still capturing tail outliers up to ~10s.
pub const BUCKET_BOUNDS_US: &[u64] = &[
    10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
    500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

/// A Prometheus-style cumulative histogram. `buckets[i]` counts observations
/// whose value is <= `BUCKET_BOUNDS_US[i]`; a final implicit `+Inf` bucket is
/// `count`. `sum_us` accumulates total observed microseconds.
pub struct Histogram {
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Histogram {
    pub fn new() -> Self {
        Self {
            buckets: BUCKET_BOUNDS_US.iter().map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }

    /// Record one observation in microseconds.
    pub fn observe_us(&self, us: u64) {
        // Increment the first bucket whose bound is >= us (cumulative buckets
        // are rendered as running sums at encode time, so here we bump only
        // the exact bucket the sample falls into).
        let idx = BUCKET_BOUNDS_US
            .iter()
            .position(|&b| us <= b)
            .unwrap_or(BUCKET_BOUNDS_US.len());
        if idx < self.buckets.len() {
            self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
    }

    /// Start timing; observe on drop or via `stop`.
    pub fn start(&self) -> LatencyTimer<'_> {
        LatencyTimer {
            hist: self,
            start: Instant::now(),
        }
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn sum_us(&self) -> u64 {
        self.sum_us.load(Ordering::Relaxed)
    }

    /// Cumulative bucket counts as `(bound_us, cumulative_count)` pairs, in
    /// ascending bound order — the exact shape Prometheus expects for `le`.
    pub fn cumulative(&self) -> Vec<(u64, u64)> {
        let mut running = 0u64;
        BUCKET_BOUNDS_US
            .iter()
            .enumerate()
            .map(|(i, &bound)| {
                running += self.buckets[i].load(Ordering::Relaxed);
                (bound, running)
            })
            .collect()
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII timer: observes the elapsed time into its histogram when dropped, so
/// callers can `let _t = hist.start();` and get an observation on every exit
/// path (including `?` early returns).
pub struct LatencyTimer<'a> {
    hist: &'a Histogram,
    start: Instant,
}

impl LatencyTimer<'_> {
    /// Observe now and consume the timer (explicit alternative to drop).
    pub fn stop(self) {
        // Drop does the work.
    }
}

impl Drop for LatencyTimer<'_> {
    fn drop(&mut self) {
        let us = self.start.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.hist.observe_us(us);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_land_in_right_buckets() {
        let h = Histogram::new();
        h.observe_us(5); // -> <=10
        h.observe_us(30); // -> <=50
        h.observe_us(30);
        h.observe_us(9_999_999); // -> <=10_000_000
        assert_eq!(h.count(), 4);
        assert_eq!(h.sum_us(), 5 + 30 + 30 + 9_999_999);
        let cum = h.cumulative();
        // <=10 has 1, <=50 has 1+2=3 cumulative
        assert_eq!(cum[0], (10, 1));
        assert_eq!(cum[2], (50, 3));
        // The very last bound accumulates everything.
        assert_eq!(cum.last().unwrap().1, 4);
    }

    #[test]
    fn over_max_bound_still_counts_in_total() {
        let h = Histogram::new();
        h.observe_us(u64::MAX); // beyond every bound -> only count/sum move
        assert_eq!(h.count(), 1);
        // Not in any finite bucket, but the implicit +Inf (== count) covers it.
        assert_eq!(h.cumulative().last().unwrap().1, 0);
    }
}
