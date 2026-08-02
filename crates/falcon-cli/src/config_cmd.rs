//! `falcon config` and `falcon status` — the whole config path.
//!
//! Falcon reads no environment variables: every setting lives in the profile
//! file, written here.

use crate::cli::ConfigCmd;
use falcon_core::Profile;
use std::path::PathBuf;

pub fn profile_path(flag: &Option<String>) -> PathBuf {
    flag.clone()
        .map(PathBuf::from)
        .unwrap_or_else(falcon_core::default_profile_path)
}

/// Keys whose values are secrets and must not be printed in full.
fn is_secret(key: &str) -> bool {
    key == "api-key"
}

fn redact(key: &str, value: &str) -> String {
    if is_secret(key) && !value.is_empty() {
        "••••••".to_string()
    } else {
        value.to_string()
    }
}

/// `falcon status` — what this node is configured to do.
pub fn status(profile_flag: &Option<String>) -> anyhow::Result<()> {
    let path = profile_path(profile_flag);
    let profile = Profile::load_or_default(&path)?;

    println!("Falcon Cache {}", env!("CARGO_PKG_VERSION"));
    println!("  low-latency, memory-bounded RAM cache with TTL");
    println!();
    if path.exists() {
        println!("Profile: {}", path.display());
    } else {
        println!(
            "Profile: {} (not created yet — using defaults)",
            path.display()
        );
    }
    println!();
    println!("Settings:");
    for (key, value) in profile.entries() {
        println!("  {key:<16} {}", redact(key, &value));
    }
    Ok(())
}

/// `falcon config set|get|list`.
pub fn config(profile_flag: &Option<String>, cmd: ConfigCmd) -> anyhow::Result<()> {
    let path = profile_path(profile_flag);
    match cmd {
        ConfigCmd::Set { key, value } => {
            let mut profile = Profile::load_or_default(&path)?;
            profile.set(&key, &value)?;
            profile.save(&path)?;
            println!("{key} = {}", redact(&key, &value));
            println!("saved to {}", path.display());
            println!("restart `falcon serve` for it to take effect");
            Ok(())
        }
        ConfigCmd::Get { key } => {
            let profile = Profile::load_or_default(&path)?;
            match profile.get(&key) {
                // `get` of a single key prints it verbatim: the user asked for
                // this exact value, and redacting it would make the command
                // useless for the one case it exists to serve.
                Some(value) => {
                    println!("{value}");
                    Ok(())
                }
                None => anyhow::bail!("unknown config key '{key}' (see `falcon config list`)"),
            }
        }
        ConfigCmd::List => {
            let profile = Profile::load_or_default(&path)?;
            for (key, value) in profile.entries() {
                println!("{key:<16} {}", redact(key, &value));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_is_the_only_secret() {
        assert!(is_secret("api-key"));
        for key in ["node.id", "region", "http-bind", "capacity-mb", "log-level"] {
            assert!(!is_secret(key), "{key} must not be treated as a secret");
        }
    }

    /// `list` and `status` print every setting, so a configured key must never
    /// appear in full there — that output routinely ends up in bug reports,
    /// screenshots, and CI logs.
    #[test]
    fn a_configured_api_key_is_never_printed_in_full() {
        let redacted = redact("api-key", "super-secret-value");
        assert_eq!(redacted, "••••••");
        assert!(!redacted.contains("super-secret-value"));
    }

    /// An unset key must render as empty rather than as dots: "auth is off" and
    /// "auth is on and I am hiding the value" have to be distinguishable.
    #[test]
    fn an_empty_api_key_is_shown_as_empty_not_redacted() {
        assert_eq!(redact("api-key", ""), "");
    }

    #[test]
    fn non_secret_values_pass_through_unchanged() {
        assert_eq!(redact("region", "us-east-1"), "us-east-1");
        assert_eq!(redact("http-bind", "0.0.0.0:8080"), "0.0.0.0:8080");
        // A non-secret key that merely looks secret is still not redacted.
        assert_eq!(redact("node.id", "api-key"), "api-key");
    }

    #[test]
    fn an_explicit_profile_flag_wins_over_the_default_path() {
        let explicit = profile_path(&Some("/tmp/custom-profile.toml".to_string()));
        assert_eq!(explicit, PathBuf::from("/tmp/custom-profile.toml"));

        let default = profile_path(&None);
        assert!(default.ends_with("profile.toml"));
    }
}
