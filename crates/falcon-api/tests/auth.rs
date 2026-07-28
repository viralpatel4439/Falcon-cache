use falcon_core::{Config, Node};
use std::sync::Arc;

async fn start(config: Config) -> std::net::SocketAddr {
    let node = Arc::new(Node::build(config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = falcon_api::router(node);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    addr
}

fn config_with_token(token: &str) -> Config {
    let mut config = Config::default();
    config.auth.api_key = token.to_string();
    config
}

#[tokio::test]
async fn auth_off_by_default_allows_everything() {
    // token empty -> auth off
    let addr = start(Config::default()).await;
    let client = reqwest::Client::new();
    let resp = client.post(format!("http://{addr}/cache")).json(&serde_json::json!({"key":"k","value":"v"})).send().await.unwrap();
    assert!(resp.status().is_success());
}

#[tokio::test]
async fn auth_on_rejects_missing_and_wrong_token() {
    let addr = start(config_with_token("s3cret")).await;
    let client = reqwest::Client::new();

    // No token -> 401.
    let resp = client.get(format!("http://{addr}/cache?key=k")).send().await.unwrap();
    assert_eq!(resp.status(), 401);

    // Wrong token -> 401.
    let resp = client
        .get(format!("http://{addr}/cache?key=k"))
        .bearer_auth("wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn auth_on_allows_correct_token_and_healthz_is_exempt() {
    let addr = start(config_with_token("s3cret")).await;
    let client = reqwest::Client::new();

    // healthz works without a token (liveness probes).
    let resp = client.get(format!("http://{addr}/healthz")).send().await.unwrap();
    assert!(resp.status().is_success());

    // Correct token -> allowed.
    let resp = client
        .post(format!("http://{addr}/cache"))
        .bearer_auth("s3cret")
        .json(&serde_json::json!({"key":"k","value":"v"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
}

#[tokio::test]
async fn api_key_via_query_param_works() {
    // The query-param fallback (for browser clients that can't set headers).
    // Correct key in ?api_key= is accepted; wrong/missing is 401.
    let addr = start(config_with_token("s3cret")).await;
    let client = reqwest::Client::new();

    let ok = client
        .post(format!("http://{addr}/cache?api_key=s3cret"))
        .json(&serde_json::json!({"key":"k","value":"v"}))
        .send()
        .await
        .unwrap();
    assert!(ok.status().is_success(), "valid ?api_key should be accepted");

    let bad = client
        .get(format!("http://{addr}/cache?key=k&api_key=wrong"))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 401, "wrong ?api_key must be rejected");

    let missing = client.get(format!("http://{addr}/cache?key=k")).send().await.unwrap();
    assert_eq!(missing.status(), 401);
}

