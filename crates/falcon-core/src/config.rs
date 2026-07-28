use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    #[serde(default = "default_node_id")]
    pub id: String,
    #[serde(default = "default_region")]
    pub region: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: default_node_id(),
            region: default_region(),
        }
    }
}

fn default_node_id() -> String {
    "node-1".to_string()
}
fn default_region() -> String {
    "local".to_string()
}

/// Optional transport TLS, shared by both server hops (HTTP and the binary
/// wire protocol). When enabled, both listen with TLS so client↔service
/// traffic is encrypted. Off by default (zero cost).
///
/// TLS is terminated *in process* with rustls (pure-Rust, AES-NI accelerated),
/// so on persistent connections — which Falcon uses everywhere — the handshake
/// is a one-time per-connection cost and per-record encryption adds only single
/// -digit microseconds, keeping the per-op hot path fast.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// PEM certificate chain file path.
    #[serde(default)]
    pub cert_file: String,
    /// PEM private key file path.
    #[serde(default)]
    pub key_file: String,
}

impl TlsConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.cert_file.is_empty() && !self.key_file.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_http_bind")]
    pub bind: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: default_http_bind(),
        }
    }
}

fn default_http_bind() -> String {
    "0.0.0.0:8080".to_string()
}

/// Optional shared-secret API key. When empty (default), auth is OFF and no
/// checks run anywhere — zero overhead. When set, EVERY client on every
/// path must present the matching key: HTTP (`Authorization: Bearer` or
/// `?api_key=`) and the binary wire protocol (an AUTH frame first).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    /// The shared API key.
    #[serde(default)]
    pub api_key: String,
}

impl AuthConfig {
    pub fn is_enabled(&self) -> bool {
        !self.api_key.is_empty()
    }

    /// The configured key (empty = auth off).
    pub fn key(&self) -> &str {
        &self.api_key
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_wire_bind")]
    pub bind: String,
    /// Close a connection that sends nothing for this many seconds. Prevents an
    /// idle/half-open/slowloris client from holding a task + socket forever.
    /// `0` disables the timeout (connections may idle indefinitely).
    #[serde(default = "default_wire_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
}

impl Default for WireConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            bind: default_wire_bind(),
            idle_timeout_secs: default_wire_idle_timeout_secs(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_wire_bind() -> String {
    "0.0.0.0:6380".to_string()
}
fn default_wire_idle_timeout_secs() -> u64 {
    300 // 5 minutes: generous for persistent clients, bounds truly-idle sockets
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Max accepted value/body size in bytes (anti-OOM). A PUT larger than
    /// this is rejected with 413. Default 64 MiB; set 0 to disable the cap.
    #[serde(default = "default_max_value_bytes")]
    pub max_value_bytes: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_value_bytes: default_max_value_bytes(),
        }
    }
}

fn default_max_value_bytes() -> usize {
    64 * 1024 * 1024
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyspaceConfig {
    pub name: String,
    /// Max RAM (MB) this cache may hold.
    ///
    /// A **hard** bound on resident memory — values, keys, and per-entry
    /// overhead all count against it, and the cache evicts rather than exceed
    /// it.
    #[serde(default = "default_cache_capacity_mb")]
    pub cache_capacity_mb: usize,
    /// How many entries eviction samples before choosing a victim. Bigger is
    /// closer to true LRU at linear cost; Redis's equivalent default is 5.
    #[serde(default = "default_evict_sample")]
    pub evict_sample: usize,
    /// Default time-to-live for keys in this keyspace, in seconds. 0 = no
    /// expiry (default). A per-write TTL (via the API) overrides this.
    #[serde(default)]
    pub default_ttl_secs: u64,
}

fn default_cache_capacity_mb() -> usize {
    256
}
fn default_evict_sample() -> usize {
    8
}

impl KeyspaceConfig {
    pub fn default_keyspace() -> Self {
        Self {
            // `cache` is the one keyspace this product owns — the name the
            // `/cache` route and the wire protocol's empty-keyspace default
            // both resolve to.
            name: "cache".to_string(),
            cache_capacity_mb: default_cache_capacity_mb(),
            evict_sample: default_evict_sample(),
            default_ttl_secs: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub wire: WireConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default = "default_keyspaces", rename = "keyspace")]
    pub keyspaces: Vec<KeyspaceConfig>,
}

fn default_keyspaces() -> Vec<KeyspaceConfig> {
    vec![KeyspaceConfig::default_keyspace()]
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            http: HttpConfig::default(),
            wire: WireConfig::default(),
            storage: StorageConfig::default(),
            keyspaces: default_keyspaces(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("io error reading config: {0}")]
    Io(#[from] std::io::Error),
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(s)?)
    }

    pub fn from_file(path: &std::path::Path) -> Result<Self, ConfigError> {
        let s = std::fs::read_to_string(path)?;
        Self::from_toml_str(&s)
    }
}

#[cfg(test)]
mod cache_config_tests {
    use super::*;

    #[test]
    fn cache_capacity_mb_loads() {
        let toml = r#"
            [[keyspace]]
            name = "cache"
            cache_capacity_mb = 128
        "#;
        let cfg: Config = toml::from_str(toml).expect("profile must parse");
        assert_eq!(cfg.keyspaces[0].cache_capacity_mb, 128);
    }

    #[test]
    fn defaults_give_one_cache_keyspace() {
        let cfg = Config::default();
        assert_eq!(cfg.keyspaces.len(), 1);
        assert_eq!(cfg.keyspaces[0].name, "cache");
    }
}
