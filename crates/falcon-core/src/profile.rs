//! The Falcon profile — the *only* way a node is configured.
//!
//! Falcon does not read environment variables. A node's entire configuration
//! lives in a single TOML profile file that is written and edited exclusively
//! through the CLI (`falcon config set`). At startup `falcon serve` loads this
//! file and nothing else; CLI flags to `serve` may override individual fields
//! for one run, but the durable source of truth is always the profile.

use crate::config::{Config, KeyspaceConfig};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Default profile location: `~/.falcon/profile.toml`. Overridable per-invocation
/// with `--profile <path>` (a flag, never an env var).
pub fn default_profile_path() -> PathBuf {
    let base = home_dir().unwrap_or_else(|| PathBuf::from("."));
    base.join(".falcon").join("profile.toml")
}

/// Minimal `$HOME` resolution without pulling in a crate. We read the process
/// environment for HOME here only to *locate* the profile file — this is not
/// configuration (no Falcon setting is taken from the environment).
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Restrict the profile file to owner-only (`0600`).
///
/// The profile stores `api_key` in plaintext. `std::fs::write` creates with
/// `0644` by default, which would publish the shared secret to every local
/// user on the machine.
#[cfg(unix)]
fn restrict_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

/// Restrict the profile directory to owner-only (`0700`).
///
/// A `0600` file inside a world-readable directory still leaks its name and
/// lets another user replace it, so the directory is tightened too.
#[cfg(unix)]
fn restrict_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

/// Windows has no mode bits; ACL inheritance from the user profile directory
/// is the platform-appropriate protection, so this is a no-op there.
#[cfg(not(unix))]
fn restrict_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn restrict_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// The node identity + network settings a profile carries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileNode {
    #[serde(default = "default_node_id")]
    pub id: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_http_bind")]
    pub http_bind: String,
    #[serde(default = "default_wire_bind")]
    pub wire_bind: String,
    #[serde(default)]
    pub wire_enabled: bool,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Max RAM (MB) the cache may hold. A hard bound: the cache evicts rather
    /// than exceed it.
    ///
    /// Unset (the default) means auto-size from the memory this process
    /// actually has. Skipped when serializing so an unconfigured profile stays
    /// auto after a save rather than freezing today's detected number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_mb: Option<usize>,
    /// Default TTL in seconds applied to writes that don't carry one.
    /// `0` = entries never expire.
    #[serde(default)]
    pub default_ttl_secs: u64,
    /// Enable in-process TLS on every server hop (HTTP and wire).
    #[serde(default)]
    pub tls_enabled: bool,
    /// PEM certificate chain file (required when `tls_enabled`).
    #[serde(default)]
    pub tls_cert: String,
    /// PEM private key file (required when `tls_enabled`).
    #[serde(default)]
    pub tls_key: String,
}

impl Default for ProfileNode {
    fn default() -> Self {
        Self {
            id: default_node_id(),
            region: default_region(),
            http_bind: default_http_bind(),
            wire_bind: default_wire_bind(),
            wire_enabled: false,
            api_key: String::new(),
            log_level: default_log_level(),
            capacity_mb: None,
            default_ttl_secs: 0,
            tls_enabled: false,
            tls_cert: String::new(),
            tls_key: String::new(),
        }
    }
}

// Re-exported from `config` rather than redefined: the profile and the runtime
// config must describe the same node, and duplicated literals drift.
use crate::config::{default_http_bind, default_node_id, default_region, default_wire_bind};

fn default_log_level() -> String {
    "info".into()
}

/// The full profile: the node's settings. Falcon Cache is a single product,
/// so there is nothing to select — the profile is purely configuration.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Profile {
    #[serde(default)]
    pub node: ProfileNode,
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("no profile found at {0} — run `falcon config set` to create one")]
    NotFound(PathBuf),
    #[error("failed to parse profile: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("failed to serialize profile: {0}")]
    Serialize(#[from] toml::ser::Error),
    #[error("io error on profile file: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown config key '{0}'")]
    UnknownKey(String),
    #[error("invalid value for '{key}': {reason}")]
    InvalidValue { key: String, reason: String },
}

impl Profile {
    /// Load a profile from disk, or a friendly NotFound if absent.
    pub fn load(path: &Path) -> Result<Self, ProfileError> {
        if !path.exists() {
            return Err(ProfileError::NotFound(path.to_path_buf()));
        }
        let s = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&s)?)
    }

    /// Load if present, else a default profile. Used by `config set` and
    /// `serve` so a node runs with sane defaults before anything is written.
    pub fn load_or_default(path: &Path) -> Result<Self, ProfileError> {
        match Self::load(path) {
            Ok(p) => Ok(p),
            Err(ProfileError::NotFound(_)) => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Persist the profile, creating the parent directory if needed.
    ///
    /// The file holds the API key in plaintext, so on Unix it is written
    /// `0600` and its directory `0700` — the default `0644` would leave the
    /// shared secret readable by every local user.
    pub fn save(&self, path: &Path) -> Result<(), ProfileError> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
            restrict_dir(dir)?;
        }
        let s = toml::to_string_pretty(self)?;
        std::fs::write(path, s)?;
        restrict_file(path)?;
        Ok(())
    }

    /// Set a dotted config key to a string value (the CLI/UI write path).
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), ProfileError> {
        let bad = |reason: &str| ProfileError::InvalidValue {
            key: key.to_string(),
            reason: reason.to_string(),
        };
        match key {
            "node.id" | "id" => self.node.id = value.to_string(),
            "node.region" | "region" => self.node.region = value.to_string(),
            "http-bind" | "http_bind" | "node.http_bind" => self.node.http_bind = value.to_string(),
            "wire-bind" | "wire_bind" | "node.wire_bind" => self.node.wire_bind = value.to_string(),
            "wire-enabled" | "wire_enabled" => {
                self.node.wire_enabled =
                    parse_bool(value).map_err(|_| bad("expected true/false"))?
            }
            "api-key" | "api_key" | "auth.api_key" => self.node.api_key = value.to_string(),
            "log-level" | "log_level" => self.node.log_level = value.to_string(),
            "capacity-mb" | "capacity_mb" => {
                // `auto` hands sizing back to detection — without it there
                // would be no way to undo an explicit value.
                self.node.capacity_mb = if value.trim().eq_ignore_ascii_case("auto") {
                    None
                } else {
                    Some(
                        value
                            .trim()
                            .parse()
                            .map_err(|_| bad("expected a whole number of megabytes, or 'auto'"))?,
                    )
                }
            }
            "default-ttl" | "default_ttl_secs" | "default-ttl-secs" => {
                self.node.default_ttl_secs = value
                    .trim()
                    .parse()
                    .map_err(|_| bad("expected a whole number of seconds (0 = no expiry)"))?
            }
            "tls-enabled" | "tls_enabled" => {
                self.node.tls_enabled = parse_bool(value).map_err(|_| bad("expected true/false"))?
            }
            "tls-cert" | "tls_cert" => self.node.tls_cert = value.to_string(),
            "tls-key" | "tls_key" => self.node.tls_key = value.to_string(),
            other => return Err(ProfileError::UnknownKey(other.to_string())),
        }
        Ok(())
    }

    /// Read a dotted config key back as a display string (the `config get` path).
    pub fn get(&self, key: &str) -> Option<String> {
        Some(match key {
            "node.id" | "id" => self.node.id.clone(),
            "node.region" | "region" => self.node.region.clone(),
            "http-bind" | "http_bind" | "node.http_bind" => self.node.http_bind.clone(),
            "wire-bind" | "wire_bind" | "node.wire_bind" => self.node.wire_bind.clone(),
            "wire-enabled" | "wire_enabled" => self.node.wire_enabled.to_string(),
            "api-key" | "api_key" | "auth.api_key" => self.node.api_key.clone(),
            "log-level" | "log_level" => self.node.log_level.clone(),
            "capacity-mb" | "capacity_mb" => match self.node.capacity_mb {
                Some(mb) => mb.to_string(),
                None => "auto".to_string(),
            },
            "default-ttl" | "default_ttl_secs" | "default-ttl-secs" => {
                self.node.default_ttl_secs.to_string()
            }
            "tls-enabled" | "tls_enabled" => self.node.tls_enabled.to_string(),
            "tls-cert" | "tls_cert" => self.node.tls_cert.clone(),
            "tls-key" | "tls_key" => self.node.tls_key.clone(),
            _ => return None,
        })
    }

    /// All settable keys with their current values, for `config list` / the UI.
    pub fn entries(&self) -> Vec<(&'static str, String)> {
        [
            "node.id",
            "node.region",
            "http-bind",
            "wire-bind",
            "wire-enabled",
            "api-key",
            "log-level",
            "capacity-mb",
            "default-ttl",
            "tls-enabled",
            "tls-cert",
            "tls-key",
        ]
        .into_iter()
        .map(|k| (k, self.get(k).unwrap_or_default()))
        .collect()
    }

    /// Materialise the runtime [`Config`] this profile describes: the single
    /// pure-RAM `cache` keyspace plus the node's network settings.
    pub fn to_config(&self) -> Config {
        let mut cfg = Config::default();
        cfg.node.id = self.node.id.clone();
        cfg.node.region = self.node.region.clone();
        cfg.http.bind = self.node.http_bind.clone();
        cfg.wire.enabled = self.node.wire_enabled;
        cfg.wire.bind = self.node.wire_bind.clone();
        cfg.auth.api_key = self.node.api_key.clone();
        cfg.tls = crate::config::TlsConfig {
            enabled: self.node.tls_enabled,
            cert_file: self.node.tls_cert.clone(),
            key_file: self.node.tls_key.clone(),
        };
        cfg.keyspaces = vec![KeyspaceConfig {
            cache_capacity_mb: self.node.capacity_mb,
            default_ttl_secs: self.node.default_ttl_secs,
            ..KeyspaceConfig::default_keyspace()
        }];
        cfg
    }
}

fn parse_bool(s: &str) -> Result<bool, ()> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_roundtrip() {
        let mut p = Profile::default();
        p.set("region", "us-east-1").unwrap();
        p.set("http-bind", "0.0.0.0:9000").unwrap();
        assert_eq!(p.get("region").unwrap(), "us-east-1");
        assert_eq!(p.get("http-bind").unwrap(), "0.0.0.0:9000");
    }

    #[test]
    fn unknown_key_errors() {
        let mut p = Profile::default();
        assert!(matches!(
            p.set("nope", "x"),
            Err(ProfileError::UnknownKey(_))
        ));
    }

    #[test]
    fn profile_builds_single_cache_keyspace() {
        let p = Profile::default();
        let cfg = p.to_config();
        assert_eq!(cfg.keyspaces.len(), 1);
        assert_eq!(cfg.keyspaces[0].name, "cache");
    }

    #[test]
    fn capacity_and_ttl_plumb_into_config() {
        let mut p = Profile::default();
        p.set("capacity-mb", "512").unwrap();
        p.set("default-ttl", "60").unwrap();
        let cfg = p.to_config();
        assert_eq!(cfg.keyspaces[0].cache_capacity_mb, Some(512));
        assert_eq!(cfg.keyspaces[0].default_ttl_secs, 60);
    }

    #[test]
    fn capacity_rejects_non_numeric() {
        let mut p = Profile::default();
        assert!(matches!(
            p.set("capacity-mb", "lots"),
            Err(ProfileError::InvalidValue { .. })
        ));
    }

    #[test]
    fn tls_config_plumbs_through() {
        let mut p = Profile::default();
        p.set("tls-enabled", "true").unwrap();
        p.set("tls-cert", "/etc/falcon/cert.pem").unwrap();
        p.set("tls-key", "/etc/falcon/key.pem").unwrap();
        let cfg = p.to_config();
        assert!(cfg.tls.is_enabled());
        assert_eq!(cfg.tls.cert_file, "/etc/falcon/cert.pem");
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("falcon-prof-{}", std::process::id()));
        let path = dir.join("profile.toml");
        let mut p = Profile::default();
        p.set("region", "eu-west-1").unwrap();
        p.save(&path).unwrap();
        let loaded = Profile::load(&path).unwrap();
        assert_eq!(loaded.node.region, "eu-west-1");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
