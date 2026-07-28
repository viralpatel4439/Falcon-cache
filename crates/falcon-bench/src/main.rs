mod target_kvstore;
mod target_wire;
mod workload;

use clap::Parser;
use std::sync::Arc;
use std::time::Instant;
use workload::{run, RunResult, Workload};

/// Benchmarks Falcon Cache: HTTP baseline plus the binary wire protocol with
/// pipelining. Spawns a local falcon node, drives load, and reports ops/sec
/// and latency percentiles.
#[derive(Parser)]
struct Args {
    /// Path to the release falcon binary.
    #[arg(long, default_value = "target/release/falcon")]
    kvstored_bin: String,

    /// Number of distinct keys in the workload.
    #[arg(long, default_value_t = 10_000)]
    key_count: usize,

    /// Value size in bytes.
    #[arg(long, default_value_t = 128)]
    value_size: usize,

    /// Operations per worker per run for the HTTP baseline section.
    #[arg(long, default_value_t = 200)]
    ops_per_worker: usize,

    /// Concurrency levels for the HTTP baseline section.
    #[arg(long, value_delimiter = ',', default_value = "8")]
    concurrency: Vec<usize>,

    /// Pipeline depths to test for the binary wire protocol. 1 = no pipelining.
    #[arg(long, value_delimiter = ',', default_value = "1,16,128")]
    pipeline_depths: Vec<usize>,

    /// Number of concurrent connections in the pipelined section.
    #[arg(long, default_value_t = 8)]
    pipeline_conns: usize,

    /// Total ops per connection in the pipelined section.
    #[arg(long, default_value_t = 20_000)]
    pipeline_ops: usize,

    /// Skip the (durable, slow) SET phases; pre-populate keys and measure
    /// GET throughput only. Useful for read-path benchmarking.
    #[arg(long, default_value_t = false)]
    skip_writes: bool,

    /// Run the sustained-load latency test instead of the standard bench:
    /// hammer the wire protocol at high concurrency for a fixed duration
    /// and report tail latency + stability over time.
    #[arg(long, default_value_t = false)]
    load_test: bool,

    /// Load-test duration in seconds.
    #[arg(long, default_value_t = 20)]
    load_secs: u64,

    /// Load-test concurrent connections.
    #[arg(long, default_value_t = 64)]
    load_conns: usize,

    /// Load-test write ratio (0.0 = all reads, 1.0 = all writes).
    #[arg(long, default_value_t = 0.5)]
    load_write_ratio: f64,

    /// Load-test pipeline depth per connection.
    #[arg(long, default_value_t = 16)]
    load_depth: usize,

}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let workload = Workload::generate(args.key_count, args.value_size);
    let keys = Arc::new(workload.keys);
    let value = Arc::new(workload.value);

    println!("kv-bench: kvstored");
    println!(
        "  keys={} value_size={}B ops_per_worker={}\n",
        keys.len(),
        args.value_size,
        args.ops_per_worker
    );

    use std::io::Write;
    println!("starting kvstored...");
    let _ = std::io::stdout().flush();
    let config_dir = tempdir()?;
    let kvstore =
        target_kvstore::KvStoreHandle::spawn(&args.kvstored_bin, 19_876, &config_dir).await?;
    println!("ready, running workload\n");
    let _ = std::io::stdout().flush();

    if args.load_test {
        run_load_test(&kvstore.wire_addr, &args, &keys, &value).await?;
        return Ok(());
    }

    // --- HTTP baseline (one request per op) ---
    println!("=== HTTP (one request per op) ===");
    print_header();
    for &concurrency in &args.concurrency {
        let client = kvstore.client();
        let base_url = kvstore.base_url.clone();
        let value_c = value.clone();
        let r = run("http PUT", concurrency, args.ops_per_worker, keys.clone(), move |key| {
            let client = client.clone();
            let base_url = base_url.clone();
            let value = value_c.clone();
            async move { target_kvstore::put(&client, &base_url, &key, &value).await }
        })
        .await;
        print_row(&r);

        let client = kvstore.client();
        let base_url = kvstore.base_url.clone();
        let r = run("http GET", concurrency, args.ops_per_worker, keys.clone(), move |key| {
            let client = client.clone();
            let base_url = base_url.clone();
            async move { target_kvstore::get(&client, &base_url, &key).await }
        })
        .await;
        print_row(&r);
    }

    // --- Binary wire protocol, pipelined ---
    println!("\n=== binary wire protocol, pipelined ({} conns, {} ops/conn) ===",
        args.pipeline_conns, args.pipeline_ops);
    print_header();
    if args.skip_writes {
        // Pre-populate keys once (small, quick) so GETs hit, then measure
        // GET only — avoids the multi-minute durable SET phase when we only
        // care about read-path throughput.
        let mut warm = target_wire::WireClient::connect(&kvstore.wire_addr).await?;
        let all: Vec<String> = keys.iter().cloned().collect();
        for chunk in all.chunks(256) {
            warm.pipeline_set(chunk, &value).await?;
        }
    }
    for &depth in &args.pipeline_depths {
        if !args.skip_writes {
            let r = bench_wire(&kvstore.wire_addr, "wire SET", depth, &args, &keys, &value, true).await;
            print_row(&r);
        }
        let r = bench_wire(&kvstore.wire_addr, "wire GET", depth, &args, &keys, &value, false).await;
        print_row(&r);
    }

    print_notes();
    Ok(())
}

/// Drives `pipeline_conns` concurrent wire connections, each pushing
/// batches of `depth` ops until it has done `pipeline_ops` total. Reports
/// aggregate ops/sec and per-batch latency.
async fn bench_wire(
    addr: &str,
    label: &str,
    depth: usize,
    args: &Args,
    keys: &Arc<Vec<String>>,
    value: &Arc<Vec<u8>>,
    is_set: bool,
) -> RunResult {
    let label = format!("{label} d={depth}");
    let start = Instant::now();
    let mut handles = Vec::new();
    for conn_id in 0..args.pipeline_conns {
        let addr = addr.to_string();
        let keys = keys.clone();
        let value = value.clone();
        let ops = args.pipeline_ops;
        handles.push(tokio::spawn(async move {
            let mut client = target_wire::WireClient::connect(&addr).await.expect("wire connect");
            let mut latencies = Vec::new();
            let mut done = 0usize;
            let mut idx = conn_id * 7;
            while done < ops {
                let n = depth.min(ops - done);
                let batch: Vec<String> = (0..n).map(|_| { idx = (idx + 1) % keys.len(); keys[idx].clone() }).collect();
                let t = Instant::now();
                if is_set {
                    client.pipeline_set(&batch, &value).await.expect("wire set");
                } else {
                    client.pipeline_get(&batch).await.expect("wire get");
                }
                latencies.push(t.elapsed());
                done += n;
            }
            latencies
        }));
    }
    let mut all_latencies = Vec::new();
    for h in handles {
        all_latencies.extend(h.await.unwrap());
    }
    RunResult {
        label,
        concurrency: args.pipeline_conns,
        total_ops: args.pipeline_conns * args.pipeline_ops,
        elapsed: start.elapsed(),
        latencies: all_latencies,
    }
}

/// Sustained-load latency test: `load_conns` connections each pipeline a
/// mix of GET/SET (per `load_write_ratio`) at depth `load_depth`, for
/// `load_secs` seconds. Reports tail latency (per batch of `depth` ops) and
/// per-second windows so a latency cliff / queue buildup is visible.
async fn run_load_test(
    addr: &str,
    args: &Args,
    keys: &Arc<Vec<String>>,
    value: &Arc<Vec<u8>>,
) -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    println!(
        "=== sustained load: {}s, {} conns, depth {}, write_ratio {:.2} ===",
        args.load_secs, args.load_conns, args.load_depth, args.load_write_ratio
    );
    // Pre-populate so reads hit.
    {
        let mut warm = target_wire::WireClient::connect(addr).await?;
        let all: Vec<String> = keys.iter().cloned().collect();
        for chunk in all.chunks(256) {
            warm.pipeline_set(chunk, value).await?;
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    // Per-second op counts, to show throughput stability over time.
    let per_sec = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));

    let mut handles = Vec::new();
    for conn_id in 0..args.load_conns {
        let addr = addr.to_string();
        let keys = keys.clone();
        let value = value.clone();
        let stop = stop.clone();
        let total_ops = total_ops.clone();
        let depth = args.load_depth;
        let write_ratio = args.load_write_ratio;
        handles.push(tokio::spawn(async move {
            let mut client = target_wire::WireClient::connect(&addr).await.expect("connect");
            let mut latencies: Vec<Duration> = Vec::new();
            let mut idx = conn_id * 131;
            // Deterministic per-conn write/read decision.
            let mut rng = conn_id as u64 ^ 0x2545f4914f6cdd1d;
            while !stop.load(Ordering::Relaxed) {
                rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                let is_write = (rng % 1000) as f64 / 1000.0 < write_ratio;
                let batch: Vec<String> = (0..depth)
                    .map(|_| { idx = (idx + 1) % keys.len(); keys[idx].clone() })
                    .collect();
                let t = Instant::now();
                if is_write {
                    if client.pipeline_set(&batch, &value).await.is_err() { break; }
                } else if client.pipeline_get(&batch).await.is_err() { break; }
                latencies.push(t.elapsed());
                total_ops.fetch_add(depth as u64, Ordering::Relaxed);
            }
            latencies
        }));
    }

    // Sample throughput each second while the test runs.
    let sampler = {
        let stop = stop.clone();
        let total_ops = total_ops.clone();
        let per_sec = per_sec.clone();
        tokio::spawn(async move {
            let mut last = 0u64;
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = total_ops.load(Ordering::Relaxed);
                per_sec.lock().unwrap().push(now - last);
                last = now;
            }
        })
    };

    tokio::time::sleep(Duration::from_secs(args.load_secs)).await;
    stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;

    let mut all_latencies = Vec::new();
    for h in handles {
        if let Ok(mut ls) = h.await {
            all_latencies.append(&mut ls);
        }
    }
    all_latencies.sort_unstable();

    let pct = |p: f64| -> Duration {
        if all_latencies.is_empty() { return Duration::ZERO; }
        let i = ((all_latencies.len() as f64 - 1.0) * p).round() as usize;
        all_latencies[i.min(all_latencies.len() - 1)]
    };
    let total = total_ops.load(Ordering::Relaxed);
    let ops_sec = total as f64 / args.load_secs as f64;

    println!("\ntotal ops: {total}  |  throughput: {:.0} ops/sec", ops_sec);
    println!("per-batch latency (batch = {} ops):", args.load_depth);
    println!("  p50={}  p95={}  p99={}  p99.9={}  max={}",
        fmt_dur(pct(0.50)), fmt_dur(pct(0.95)), fmt_dur(pct(0.99)),
        fmt_dur(pct(0.999)), fmt_dur(pct(1.0)));

    // Per-second throughput: stable = no cliff / queue buildup.
    let windows = per_sec.lock().unwrap().clone();
    if !windows.is_empty() {
        let min = *windows.iter().min().unwrap();
        let max = *windows.iter().max().unwrap();
        let avg = windows.iter().sum::<u64>() / windows.len() as u64;
        println!("per-second throughput: min={min} avg={avg} max={max} ops/sec");
        let stable = min as f64 >= avg as f64 * 0.5; // no >2x dip
        println!("stability: {}",
            if stable { "STABLE (no latency cliff / queue buildup)" }
            else { "UNSTABLE — throughput dipped >2x in some window" });
    }
    Ok(())
}

fn print_header() {
    println!(
        "{:<14} {:>5} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "target", "conc", "ops/sec", "p50", "p95", "p99", "max"
    );
    println!("{}", "-".repeat(80));
}

fn print_row(r: &RunResult) {
    use std::io::Write;
    println!(
        "{:<14} {:>5} {:>12.0} {:>10} {:>10} {:>10} {:>10}",
        r.label,
        r.concurrency,
        r.ops_per_sec(),
        fmt_dur(r.percentile(0.50)),
        fmt_dur(r.percentile(0.95)),
        fmt_dur(r.percentile(0.99)),
        fmt_dur(r.max()),
    );
    let _ = std::io::stdout().flush();
}

fn fmt_dur(d: std::time::Duration) -> String {
    if d.as_millis() >= 1 {
        format!("{}ms", d.as_millis())
    } else {
        format!("{}us", d.as_micros())
    }
}

fn print_notes() {
    println!();
    println!("Notes:");
    println!("  - HTTP section is one request per op (JSON body); the wire section pipelines");
    println!("    d ops per round-trip over the binary protocol on a persistent TCP connection.");
    println!("  - In the pipelined section p50/p95/p99 are PER-BATCH latencies (batch = d ops);");
    println!("    ops/sec is the aggregate throughput.");
    println!("  - Both reads and writes are served entirely from RAM — the cache tier has");
    println!("    no WAL and no disk I/O on either path.");
}

fn tempdir() -> anyhow::Result<std::path::PathBuf> {
    let dir = std::env::temp_dir().join(format!("kv-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}
