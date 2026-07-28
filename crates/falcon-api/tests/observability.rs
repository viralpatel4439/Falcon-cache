//! Integration tests for the production/autoscale surface: /readyz gating,
//! body-size limits, and that the probes bypass auth.

use falcon_core::{Config, Node};
use std::sync::Arc;

async fn start(config: Config) -> (std::net::SocketAddr, Arc<Node>) {
    let node = Arc::new(Node::build(config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = falcon_api::router(node.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    (addr, node)
}

fn base_config() -> Config {
    Config::default()
}

#[tokio::test]
async fn readyz_reflects_ready_flag() {
    let (addr, node) = start(base_config()).await;
    let client = reqwest::Client::new();

    // Not ready until set.
    let resp = client.get(format!("http://{addr}/readyz")).send().await.unwrap();
    assert_eq!(resp.status(), 503);

    node.set_ready(true);
    let resp = client.get(format!("http://{addr}/readyz")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    node.set_ready(false);
    let resp = client.get(format!("http://{addr}/readyz")).send().await.unwrap();
    assert_eq!(resp.status(), 503);
}

#[tokio::test]
async fn body_limit_rejects_oversized_put() {
    let mut config = base_config();
    config.storage.max_value_bytes = 1024; // 1 KiB cap
    let (addr, _node) = start(config).await;
    let client = reqwest::Client::new();

    // Under the limit: OK.
    let ok = client
        .post(format!("http://{addr}/cache"))
        .json(&serde_json::json!({"key":"small","value":"x".repeat(512)}))
        .send()
        .await
        .unwrap();
    assert!(ok.status().is_success());

    // Over the limit: 413 Payload Too Large.
    let too_big = client
        .post(format!("http://{addr}/cache"))
        .json(&serde_json::json!({"key":"big","value":"x".repeat(4096)}))
        .send()
        .await
        .unwrap();
    assert_eq!(too_big.status(), 413);
}

#[tokio::test]
async fn probes_bypass_auth() {
    let mut config = base_config();
    config.auth.api_key = "s3cret".to_string();
    let (addr, _node) = start(config).await;
    let client = reqwest::Client::new();

    // No token, but the probes must still answer (orchestrators hold no key).
    assert_eq!(
        client.get(format!("http://{addr}/healthz")).send().await.unwrap().status(),
        200
    );
    // /readyz answers without auth (503 = not-ready, NOT 401 = unauthorized).
    assert_eq!(
        client.get(format!("http://{addr}/readyz")).send().await.unwrap().status(),
        503
    );
    // A real cache route still requires auth.
    assert_eq!(
        client.get(format!("http://{addr}/cache?key=x")).send().await.unwrap().status(),
        401
    );
}

