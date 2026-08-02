//! `falcon serve` — run a node from the profile.
//!
//! Configuration comes ONLY from the profile file (written by `falcon config`).
//! Falcon reads no environment variables. The `serve` flags may override
//! individual fields for a single run, but the profile remains the durable
//! source of truth. If no profile exists yet, the node starts on defaults —
//! including a capacity sized to the machine — rather than refusing to run.
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
    ///
    /// Uses the same core-count probe the cache engine is sized from, so the
    /// worker count and the shard count cannot disagree about the machine.
    fn detect() -> Self {
        Self {
            workers: falcon_core::available_parallelism(),
        }
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
    runtime.block_on(async move { serve(config, plan).await })
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
    runtime.block_on(async move { serve(config, plan).await })
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
            ks.cache_capacity_mb = Some(mb);
        }
    }
    if let Some(ttl) = args.default_ttl {
        for ks in &mut config.keyspaces {
            ks.default_ttl_secs = ttl;
        }
    }
}

async fn serve(config: Config, plan: RuntimePlan) -> anyhow::Result<()> {
    // The per-keyspace capacity that was actually resolved is logged by
    // `Node::build`, which is where auto-sizing happens.
    tracing::info!(
        node_id = %config.node.id,
        region = %config.node.region,
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
    tracing::info!(%bind, "HTTP API at http://{bind}/cache  ·  health at /healthz");
    let mut http_shutdown = shutdown_tx.subscribe();
    let http_signal = async move {
        let _ = http_shutdown.recv().await;
    };
    falcon_api::serve_with_shutdown(node.clone(), bind, http_signal).await?;

    // Nothing to flush: the cache holds no durable state. Draining in-flight
    // requests (above) is the whole of graceful shutdown.
    node.set_ready(false);
    tracing::info!("drained in-flight requests; exiting cleanly");
    Ok(())
}

#[cfg(test)]
mod override_tests {
    use super::*;

    /// A `serve` invocation with no flags — the baseline every test varies from.
    fn no_flags() -> ServeArgs {
        ServeArgs {
            config: None,
            http_bind: None,
            wire_bind: None,
            wire_enabled: false,
            wire_disabled: false,
            capacity_mb: None,
            default_ttl: None,
            node_id: None,
            region: None,
            log_level: "info".to_string(),
        }
    }

    #[test]
    fn absent_flags_leave_the_profile_untouched() {
        // The common case: `falcon serve` with no flags must not silently
        // rewrite settings the operator configured in the profile.
        let mut config = Config::default();
        config.node.id = "from-profile".into();
        config.http.bind = "10.0.0.1:9999".into();
        let before = config.clone();

        apply_overrides(&mut config, &no_flags());

        assert_eq!(config.node.id, before.node.id);
        assert_eq!(config.http.bind, before.http.bind);
        assert_eq!(config.wire.enabled, before.wire.enabled);
    }

    #[test]
    fn flags_override_profile_values() {
        let mut config = Config::default();
        config.node.id = "from-profile".into();

        let args = ServeArgs {
            http_bind: Some("127.0.0.1:1111".into()),
            wire_bind: Some("127.0.0.1:2222".into()),
            node_id: Some("from-flag".into()),
            region: Some("eu-west-1".into()),
            ..no_flags()
        };
        apply_overrides(&mut config, &args);

        assert_eq!(config.http.bind, "127.0.0.1:1111");
        assert_eq!(config.wire.bind, "127.0.0.1:2222");
        assert_eq!(config.node.id, "from-flag");
        assert_eq!(config.node.region, "eu-west-1");
    }

    #[test]
    fn wire_can_be_enabled_and_disabled_for_one_run() {
        let mut config = Config::default();
        config.wire.enabled = false;
        apply_overrides(
            &mut config,
            &ServeArgs {
                wire_enabled: true,
                ..no_flags()
            },
        );
        assert!(config.wire.enabled);

        let mut config = Config::default();
        config.wire.enabled = true;
        apply_overrides(
            &mut config,
            &ServeArgs {
                wire_disabled: true,
                ..no_flags()
            },
        );
        assert!(!config.wire.enabled);
    }

    /// `--capacity-mb` and `--default-ttl` are node-level flags that fan out to
    /// every keyspace, so a config declaring several must have them all set.
    #[test]
    fn capacity_and_ttl_apply_to_every_keyspace() {
        let mut config = Config::default();
        config.keyspaces.push(falcon_core::KeyspaceConfig {
            name: "second".into(),
            cache_capacity_mb: None,
            evict_sample: 8,
            default_ttl_secs: 0,
        });

        apply_overrides(
            &mut config,
            &ServeArgs {
                capacity_mb: Some(512),
                default_ttl: Some(300),
                ..no_flags()
            },
        );

        assert!(!config.keyspaces.is_empty());
        for ks in &config.keyspaces {
            assert_eq!(ks.cache_capacity_mb, Some(512), "{}", ks.name);
            assert_eq!(ks.default_ttl_secs, 300, "{}", ks.name);
        }
    }

    /// Omitting `--capacity-mb` must leave `None` in place: `None` is what
    /// selects auto-sizing, and overwriting it with a number would silently
    /// opt the node out of detection.
    #[test]
    fn omitting_capacity_preserves_auto_sizing() {
        let mut config = Config::default();
        for ks in &mut config.keyspaces {
            ks.cache_capacity_mb = None;
        }

        apply_overrides(&mut config, &no_flags());

        for ks in &config.keyspaces {
            assert_eq!(
                ks.cache_capacity_mb, None,
                "auto-sizing must survive a flagless serve"
            );
        }
    }

    #[test]
    fn runtime_plan_allocates_at_least_one_worker() {
        // Sized from the machine, so assert the invariant rather than a number.
        let plan = RuntimePlan::detect();
        assert!(plan.workers >= 1);
        assert_eq!(plan.workers, falcon_core::available_parallelism());
    }
}
