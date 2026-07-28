use rand::{Rng, SeedableRng};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// One randomized key/value workload, shared verbatim between whichever
/// target (kvstored, Redis) is under test, so both see the exact same key
/// distribution and value sizes.
pub struct Workload {
    pub keys: Vec<String>,
    pub value: Vec<u8>,
}

impl Workload {
    pub fn generate(key_count: usize, value_size: usize) -> Self {
        let keys = (0..key_count).map(|i| format!("bench:key:{i}")).collect();
        let value = vec![b'x'; value_size];
        Self { keys, value }
    }
}

pub struct RunResult {
    pub label: String,
    pub concurrency: usize,
    pub total_ops: usize,
    pub elapsed: Duration,
    pub latencies: Vec<Duration>,
}

impl RunResult {
    pub fn ops_per_sec(&self) -> f64 {
        self.total_ops as f64 / self.elapsed.as_secs_f64()
    }

    pub fn percentile(&self, p: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    pub fn max(&self) -> Duration {
        self.latencies.iter().copied().max().unwrap_or_default()
    }
}

/// Runs `ops_per_worker` sequential operations on each of `concurrency`
/// concurrent workers against the same target, picking a random key from
/// `keys` for each op. `op` is called once per operation and must perform
/// exactly one request against the target being benchmarked.
pub async fn run<F, Fut>(
    label: &str,
    concurrency: usize,
    ops_per_worker: usize,
    keys: Arc<Vec<String>>,
    op: F,
) -> RunResult
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    let op = Arc::new(op);
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(concurrency * ops_per_worker)));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for worker_id in 0..concurrency {
        let op = op.clone();
        let keys = keys.clone();
        let latencies = latencies.clone();
        handles.push(tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::seed_from_u64(worker_id as u64);
            let mut local_latencies = Vec::with_capacity(ops_per_worker);
            for _ in 0..ops_per_worker {
                let key = keys[rng.gen_range(0..keys.len())].clone();
                let op_start = Instant::now();
                op(key).await;
                local_latencies.push(op_start.elapsed());
            }
            latencies.lock().await.extend(local_latencies);
        }));
    }
    for h in handles {
        h.await.expect("worker task panicked");
    }
    let elapsed = start.elapsed();

    RunResult {
        label: label.to_string(),
        concurrency,
        total_ops: concurrency * ops_per_worker,
        elapsed,
        latencies: Arc::try_unwrap(latencies).unwrap().into_inner(),
    }
}
