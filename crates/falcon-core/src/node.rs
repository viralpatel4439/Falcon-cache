use crate::config::Config;
use crate::keyspace::Keyspace;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Composition root: owns one [`Keyspace`] per configured keyspace, built once
/// at startup from [`Config`]. `falcon-api` and `falcon-wire` both hold an
/// `Arc<Node>` and never touch the cache engine directly.
pub struct Node {
    config: Config,
    keyspaces: HashMap<String, Keyspace>,
    /// Whether this node is ready to serve traffic — the state `/readyz`
    /// reports. Set once startup completes and cleared on shutdown, so an
    /// orchestrator stops routing to a draining node.
    ready: AtomicBool,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("unknown keyspace '{0}'")]
    UnknownKeyspace(String),
}

impl Node {
    pub fn build(config: Config) -> Result<Self, NodeError> {
        let mut keyspaces = HashMap::new();
        let parallelism = crate::resources::available_parallelism();
        // Auto-sized keyspaces share one budget rather than each claiming a
        // share of total memory — two keyspaces both auto-sizing would
        // otherwise commit well over what the machine has.
        let auto_count = config
            .keyspaces
            .iter()
            .filter(|k| k.cache_capacity_mb.is_none())
            .count()
            .max(1);

        for ks_cfg in &config.keyspaces {
            let resolved = crate::resources::resolve_capacity(ks_cfg.cache_capacity_mb);
            let capacity_bytes = if ks_cfg.cache_capacity_mb.is_none() {
                resolved.bytes / auto_count as u64
            } else {
                resolved.bytes
            } as usize;

            // Say what was chosen and why. An auto-sizing cache that keeps that
            // to itself is an operational trap: after an OOM-kill the first
            // question is what the cache thought it had.
            tracing::info!(
                keyspace = %ks_cfg.name,
                capacity_mb = capacity_bytes / (1024 * 1024),
                source = resolved.source.as_str(),
                cores = parallelism,
                "cache capacity resolved"
            );

            let engine = Arc::new(falcon_storage::CacheEngine::with_options(
                falcon_storage::CacheOptions {
                    capacity_bytes,
                    evict_sample: ks_cfg.evict_sample,
                    parallelism,
                },
            ));
            tracing::info!(
                keyspace = %ks_cfg.name,
                shards = engine.shard_count(),
                "cache sharded for this machine"
            );
            // Reclaims expired entries in the background. Expiry is also
            // enforced on read, so this bounds memory rather than correctness:
            // without it, a key written with a TTL and never read again would
            // hold its bytes until eviction pressure happened to reach it.
            engine.spawn_maintenance();
            keyspaces.insert(
                ks_cfg.name.clone(),
                Keyspace::new(ks_cfg.name.clone(), engine, ks_cfg.default_ttl_secs),
            );
        }

        Ok(Self {
            config,
            keyspaces,
            ready: AtomicBool::new(false),
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn keyspace(&self, name: &str) -> Option<&Keyspace> {
        self.keyspaces.get(name)
    }

    pub fn keyspace_names(&self) -> impl Iterator<Item = &str> {
        self.keyspaces.keys().map(|s| s.as_str())
    }

    pub fn require_keyspace(&self, name: &str) -> Result<&Keyspace, NodeError> {
        self.keyspace(name)
            .ok_or_else(|| NodeError::UnknownKeyspace(name.to_string()))
    }

    /// Mark the node ready (or not) to serve traffic — drives `/readyz`.
    /// Called once startup finishes, and again as shutdown begins.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// Whether the node is ready to serve traffic.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
}

/// Resolves when the process receives SIGTERM (k8s/docker stop) or Ctrl-C
/// (SIGINT). The single shutdown trigger for graceful drain.
///
/// The cache holds nothing durable, so shutdown has nothing to flush — this
/// exists so in-flight requests finish rather than being cut off mid-response.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl-C, shutting down gracefully"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down gracefully"),
    }
}
