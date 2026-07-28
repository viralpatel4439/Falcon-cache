//! `falcon config` and `falcon status` — the whole config path.
//!
//! Falcon reads no environment variables: every setting lives in the profile
//! file, written here.

use crate::cli::ConfigCmd;
use falcon_core::Profile;
use std::path::PathBuf;

fn profile_path(flag: &Option<String>) -> PathBuf {
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
        println!("Profile: {} (not created yet — using defaults)", path.display());
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
