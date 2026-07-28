//! `falcon serve` — run a node from the profile.
//!
//! Configuration comes ONLY from the profile file (written by `falcon config`
//! or the web UI). Falcon reads no environment variables. The `serve` flags may
//! override individual fields for a single run, but the profile remains the
//! durable source of truth. If no profile exists yet, the node starts on
//! defaults rather than refusing to run.
//!
//! Concurrency is **automatic**. Falcon builds a multi-threaded, work-stealing
//! Tokio runtime sized to the machine — there is no thread/worker/core knob to
//! tune. The async worker pool gets one thread per logical CPU, and the
//! scheduler load-balances tasks across workers by work-stealing, so the
//! runtime adapts to load on its own rather than to a fixed setting.

use crate::cli::ServeArgs;
use falcon_core::{Config, Node, Profile};
use std::path::PathBuf;
use std::sync::Arc;

/// How Falcon auto-sized the runtime for this machine. Logged at startup so the
/// chosen concurrency is transparent even though it isn't configurable.
#[derive(Clone, Copy)]
struct RuntimePlan {
    /// Async worker threads = logical CPUs. One per core → all cores usable.
    workers: usize,
}

impl RuntimePlan {
    /// Derive the plan purely from the hardware — no user input.
    fn detect() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self { workers }
    }

    /// Build the multi-threaded runtime this plan describes.
    ///
    /// No blocking-pool sizing: the cache serves every operation from RAM, so
    /// nothing is ever offloaded to `spawn_blocking` and the default pool is
    /// left untouched.
    fn build(self) -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(self.workers)
            .thread_name("falcon-worker")
            .build()
    }
}

pub fn run(profile_flag: &Option<String>, args: ServeArgs) -> anyhow::Result<()> {
    // Advanced/testing path: a full engine config file bypasses the profile.
    if let Some(cfg_path) = args.config.clone() {
        return run_from_config_file(&cfg_path, &args);
    }

    let profile_path: PathBuf = profile_flag
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(falcon_core::default_profile_path);

    // A missing profile is not an error: there is one product and it has
    // working defaults, so an unconfigured node still starts.
    let profile = Profile::load_or_default(&profile_path)?;

    init_tracing(&args.log_level);

    // Select the rustls crypto provider once, before any TLS listener is built.
    falcon_core::tls::init_crypto_provider();

    let config = build_config(&profile, &args);

    let plan = RuntimePlan::detect();
    let runtime = plan.build()?;
    runtime.block_on(async move { serve(config, profile_path, plan).await })
}

/// Serve from a full engine config TOML (the `--config` escape hatch). Used by
/// the benchmark harness and advanced setups.
fn run_from_config_file(cfg_path: &str, args: &ServeArgs) -> anyhow::Result<()> {
    init_tracing(&args.log_level);
    falcon_core::tls::init_crypto_provider();

    let mut config = Config::from_file(std::path::Path::new(cfg_path))?;
    apply_overrides(&mut config, args);

    let plan = RuntimePlan::detect();
    let runtime = plan.build()?;
    let profile_path = falcon_core::default_profile_path();
    runtime.block_on(async move { serve(config, profile_path, plan).await })
}

fn init_tracing(log_level: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_level))
        .init();
}

/// Turn the profile into a runtime `Config`, then apply any one-run `serve`
/// flag overrides. Order: profile < serve flags. No environment layer.
fn build_config(profile: &Profile, args: &ServeArgs) -> Config {
    let mut config = profile.to_config();
    apply_overrides(&mut config, args);
    config
}

/// One-run `serve` flag overrides, applied on top of whichever source the
/// config came from (profile or `--config` file).
fn apply_overrides(config: &mut Config, args: &ServeArgs) {
    if let Some(v) = &args.http_bind {
        config.http.bind = v.clone();
    }
    if let Some(v) = &args.wire_bind {
        config.wire.bind = v.clone();
    }
    if args.wire_enabled {
        config.wire.enabled = true;
    }
    if args.wire_disabled {
        config.wire.enabled = false;
    }
    if let Some(v) = &args.node_id {
        config.node.id = v.clone();
    }
    if let Some(v) = &args.region {
        config.node.region = v.clone();
    }
    if let Some(mb) = args.capacity_mb {
        for ks in &mut config.keyspaces {
            ks.cache_capacity_mb = mb;
        }
    }
    if let Some(ttl) = args.default_ttl {
        for ks in &mut config.keyspaces {
            ks.default_ttl_secs = ttl;
        }
    }
}

async fn serve(config: Config, profile_path: PathBuf, plan: RuntimePlan) -> anyhow::Result<()> {
    let capacity_mb: usize = config.keyspaces.iter().map(|k| k.cache_capacity_mb).sum();
    tracing::info!(
        node_id = %config.node.id,
        region = %config.node.region,
        capacity_mb,
        worker_threads = plan.workers,
        "starting Falcon Cache (auto-sized runtime: one async worker per core)"
    );

    let node = Arc::new(Node::build(config.clone())?);

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    if config.wire.enabled {
        let wire_bind: std::net::SocketAddr = config.wire.bind.parse()?;
        let wire_node = node.clone();
        let mut wire_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let signal = async move {
                let _ = wire_shutdown.recv().await;
            };
            if let Err(e) = falcon_wire::serve_with_shutdown(wire_node, wire_bind, signal).await {
                tracing::error!(error = %e, "wire server exited");
            }
        });
    }

    node.set_ready(true);

    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        falcon_core::shutdown_signal().await;
        let _ = signal_tx.send(());
    });

    let bind: std::net::SocketAddr = config.http.bind.parse()?;
    tracing::info!(%bind, "cache UI at http://{bind}/  ·  metrics at /metrics");
    let mut http_shutdown = shutdown_tx.subscribe();
    let http_signal = async move {
        let _ = http_shutdown.recv().await;
    };
    falcon_api::serve_with_shutdown(node.clone(), bind, profile_path, http_signal).await?;

    // Nothing to flush: the cache holds no durable state. Draining in-flight
    // requests (above) is the whole of graceful shutdown.
    node.set_ready(false);
    tracing::info!("drained in-flight requests; exiting cleanly");
    Ok(())
}
