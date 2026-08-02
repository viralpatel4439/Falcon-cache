//! Client subcommands: talk to a running Falcon node over its HTTP API.
//! Synchronous (blocking reqwest) — these are one-shot CLI commands, so a full
//! async runtime would be overkill.

use crate::cli::{ClientArgs, KeyArgs, PutArgs};
use anyhow::{bail, Context, Result};
use falcon_core::Profile;

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::new()
}

/// Where the client should talk, and with what credential.
///
/// Both fall back to the profile, so `falcon config set http-bind
/// 0.0.0.0:9090` moves the client along with the server. Hardcoding
/// `127.0.0.1:8080` here meant that changing the port left `falcon get`
/// unable to reach the very node it had just configured.
struct Target {
    addr: String,
    api_key: Option<String>,
}

/// Turn a *listen* address into one a client can dial.
///
/// `0.0.0.0` means "every interface" to a server and is not a dialable
/// destination, so a node bound there — which is the default, and what the
/// Docker image bakes in — is reached over loopback.
fn dialable(bind: &str) -> String {
    let host_port = match bind.rsplit_once(':') {
        Some((host, port)) if host.is_empty() || host == "0.0.0.0" || host == "[::]" => {
            format!("127.0.0.1:{port}")
        }
        _ => bind.to_string(),
    };
    format!("http://{host_port}")
}

impl Target {
    fn resolve(profile_flag: &Option<String>, c: &ClientArgs) -> Self {
        let profile = crate::config_cmd::profile_path(profile_flag);
        let profile = Profile::load_or_default(&profile).unwrap_or_default();

        let addr = c
            .addr
            .clone()
            .unwrap_or_else(|| dialable(&profile.node.http_bind));

        let api_key = c
            .api_key
            .clone()
            .or_else(|| Some(profile.node.api_key.clone()).filter(|k| !k.is_empty()));

        Self { addr, api_key }
    }
}

/// Attach the API key (if set) as a Bearer header.
fn auth(req: reqwest::blocking::RequestBuilder, t: &Target) -> reqwest::blocking::RequestBuilder {
    match &t.api_key {
        Some(k) => req.bearer_auth(k),
        None => req,
    }
}

/// Read a value from an Option arg or, if None, from stdin (as a UTF-8 string).
fn value_or_stdin(arg: Option<String>) -> Result<String> {
    match arg {
        Some(s) => Ok(s),
        None => {
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)
                .context("reading stdin")?;
            Ok(buf)
        }
    }
}

/// The cache is the only product here, so every key verb targets `/cache`.
const CACHE_ROOT: &str = "/cache";

pub fn get(profile: &Option<String>, a: KeyArgs) -> Result<()> {
    let t = Target::resolve(profile, &a.client);
    let url = format!("{}{}?key={}", t.addr, CACHE_ROOT, a.key);
    let resp = auth(client().get(&url), &t).send()?;
    if resp.status() == 404 {
        bail!("key not found");
    }
    let body: serde_json::Value = resp.error_for_status()?.json()?;
    println!("{}", body["value"].as_str().unwrap_or(""));
    Ok(())
}

pub fn put(profile: &Option<String>, a: PutArgs) -> Result<()> {
    let t = Target::resolve(profile, &a.client);
    let value = value_or_stdin(a.value)?;
    let url = format!("{}{}", t.addr, CACHE_ROOT);
    let mut req = serde_json::json!({ "key": a.key, "value": value });
    if let Some(ttl) = a.ttl {
        req["ttl"] = serde_json::json!(ttl);
    }
    auth(client().post(&url).json(&req), &t)
        .send()?
        .error_for_status()?;
    println!("OK");
    Ok(())
}

pub fn del(profile: &Option<String>, a: KeyArgs) -> Result<()> {
    let t = Target::resolve(profile, &a.client);
    let url = format!("{}{}?key={}", t.addr, CACHE_ROOT, a.key);
    auth(client().delete(&url), &t).send()?.error_for_status()?;
    println!("OK");
    Ok(())
}

pub fn health(profile: &Option<String>, c: ClientArgs) -> Result<()> {
    let t = Target::resolve(profile, &c);
    let body: serde_json::Value = auth(client().get(format!("{}/healthz", t.addr)), &t)
        .send()?
        .error_for_status()?
        .json()?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bug this guards: the client used to hardcode `127.0.0.1:8080`, so
    /// `falcon config set http-bind 0.0.0.0:9090` left `falcon get` unable to
    /// reach the very node it had just configured.
    #[test]
    fn wildcard_binds_become_loopback() {
        assert_eq!(dialable("0.0.0.0:9090"), "http://127.0.0.1:9090");
        assert_eq!(dialable("0.0.0.0:8080"), "http://127.0.0.1:8080");
        assert_eq!(dialable("[::]:8080"), "http://127.0.0.1:8080");
        assert_eq!(dialable(":8080"), "http://127.0.0.1:8080");
    }

    #[test]
    fn concrete_binds_are_dialed_as_written() {
        assert_eq!(dialable("127.0.0.1:8080"), "http://127.0.0.1:8080");
        assert_eq!(dialable("10.0.0.5:9090"), "http://10.0.0.5:9090");
        assert_eq!(
            dialable("cache.internal:8080"),
            "http://cache.internal:8080"
        );
    }
}
