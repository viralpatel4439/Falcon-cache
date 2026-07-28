use anyhow::{bail, Context, Result};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

pub struct KvStoreHandle {
    child: Child,
    pub base_url: String,
    pub wire_addr: String,
}

impl KvStoreHandle {
    /// Spawns the release `falcon` binary, serving both HTTP (`port`) and the
    /// binary wire protocol (`port+1`) — the config a real user gets out of the
    /// box. `config_dir` only holds the generated config file; the cache itself
    /// writes nothing to disk.
    pub async fn spawn(binary_path: &str, port: u16, config_dir: &std::path::Path) -> Result<Self> {
        Self::spawn_with_config(
            binary_path,
            port,
            config_dir,
            "[[keyspace]]\nname = \"cache\"\n",
        )
        .await
    }

    /// Spawn falcon with a full config string appended to the standard
    /// bind header.
    pub async fn spawn_with_config(
        binary_path: &str,
        port: u16,
        config_dir: &std::path::Path,
        config_body: &str,
    ) -> Result<Self> {
        std::fs::create_dir_all(config_dir)?;
        let bind = format!("127.0.0.1:{port}");
        let wire_addr = format!("127.0.0.1:{}", port + 1);
        let cfg_path = config_dir.join("bench-config.toml");
        let header =
            format!("[http]\nbind = \"{bind}\"\n[wire]\nbind = \"{wire_addr}\"\n");
        std::fs::write(&cfg_path, format!("{header}{config_body}"))?;

        let child = Command::new(binary_path)
            .arg("serve")
            .arg("--config")
            .arg(&cfg_path)
            .arg("--log-level").arg("warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to spawn falcon")?;

        let base_url = format!("http://{bind}");
        wait_for_ready(&base_url).await?;
        Ok(Self {
            child,
            base_url,
            wire_addr,
        })
    }

    pub fn client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .pool_max_idle_per_host(256)
            .build()
            .expect("failed to build reqwest client")
    }
}

impl Drop for KvStoreHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn wait_for_ready(base_url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    for _ in 0..50 {
        if let Ok(resp) = client.get(format!("{base_url}/healthz")).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("falcon did not become ready at {base_url} in time")
}

pub async fn put(client: &reqwest::Client, base_url: &str, key: &str, value: &[u8]) {
    let resp = client
        .post(format!("{base_url}/cache"))
        .json(&serde_json::json!({ "key": key, "value": String::from_utf8_lossy(value) }))
        .send()
        .await
        .expect("falcon PUT request failed");
    assert!(resp.status().is_success(), "falcon PUT returned {}", resp.status());
}

pub async fn get(client: &reqwest::Client, base_url: &str, key: &str) {
    let resp = client
        .get(format!("{base_url}/cache?key={key}"))
        .send()
        .await
        .expect("falcon GET request failed");
    // 404 is expected for a GET issued before that key has been PUT during
    // this run (or after the cache evicted it); only treat non-404 failures
    // as a real error.
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 404,
        "falcon GET returned {}",
        resp.status()
    );
}
