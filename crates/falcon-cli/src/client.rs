//! Client subcommands: talk to a running Falcon node over its HTTP API.
//! Synchronous (blocking reqwest) — these are one-shot CLI commands, so a full
//! async runtime would be overkill.

use crate::cli::{ClientArgs, KeyArgs, PutArgs};
use anyhow::{bail, Context, Result};

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::new()
}

/// Attach the API key (if set) as a Bearer header.
fn auth(
    req: reqwest::blocking::RequestBuilder,
    c: &ClientArgs,
) -> reqwest::blocking::RequestBuilder {
    match &c.api_key {
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

pub fn get(a: KeyArgs) -> Result<()> {
    let url = format!("{}{}?key={}", a.client.addr, CACHE_ROOT, a.key);
    let resp = auth(client().get(&url), &a.client).send()?;
    if resp.status() == 404 {
        bail!("key not found");
    }
    let body: serde_json::Value = resp.error_for_status()?.json()?;
    println!("{}", body["value"].as_str().unwrap_or(""));
    Ok(())
}

pub fn put(a: PutArgs) -> Result<()> {
    let value = value_or_stdin(a.value)?;
    let url = format!("{}{}", a.client.addr, CACHE_ROOT);
    let mut req = serde_json::json!({ "key": a.key, "value": value });
    if let Some(ttl) = a.ttl {
        req["ttl"] = serde_json::json!(ttl);
    }
    auth(client().post(&url).json(&req), &a.client)
        .send()?
        .error_for_status()?;
    println!("OK");
    Ok(())
}

pub fn del(a: KeyArgs) -> Result<()> {
    let url = format!("{}{}?key={}", a.client.addr, CACHE_ROOT, a.key);
    auth(client().delete(&url), &a.client).send()?.error_for_status()?;
    println!("OK");
    Ok(())
}

pub fn health(c: ClientArgs) -> Result<()> {
    let body: serde_json::Value =
        auth(client().get(format!("{}/healthz", c.addr)), &c).send()?.error_for_status()?.json()?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

pub fn metrics(c: ClientArgs) -> Result<()> {
    let text = auth(client().get(format!("{}/metrics", c.addr)), &c)
        .send()?
        .error_for_status()?
        .text()?;
    print!("{text}");
    Ok(())
}
