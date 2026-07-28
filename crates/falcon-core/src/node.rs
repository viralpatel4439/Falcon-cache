use crate::config::Config;
use crate::keyspace::Keyspace;
use falcon_metrics::Metrics;
use std::collections::HashMap;
use std::sync::Arc;

/// Composition root: owns one [`Keyspace`] per configured keyspace, built once
/// at startup from [`Config`]. `falcon-api` and `falcon-wire` both hold an
/// `Arc<Node>` and never touch the cache engine directly.
pub struct Node {
    config: Config,
    keyspaces: HashMap<String, Keyspace>,
    metrics: Arc<Metrics>,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("unknown keyspace '{0}'")]
    UnknownKeyspace(String),
}

impl Node {
    pub fn build(config: Config) -> Result<Self, NodeError> {
        let mut keyspaces = HashMap::new();
        for ks_cfg in &config.keyspaces {
            let capacity_bytes = ks_cfg.cache_capacity_mb * 1024 * 1024;
            let engine = Arc::new(falcon_storage::CacheEngine::new(
                capacity_bytes,
                ks_cfg.evict_sample,
            ));
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
            metrics: Arc::new(Metrics::new()),
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The process metrics registry — shared with the HTTP/wire servers so
    /// every request path records into the same counters/histograms that
    /// `/metrics` renders.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
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

    /// Mark the node ready (or not) to serve traffic — drives `/readyz` and
    /// the `falcon_ready` gauge. Called once startup finishes.
    pub fn set_ready(&self, ready: bool) {
        self.metrics.ready.set(if ready { 1 } else { 0 });
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
