//! Cross-protocol handling of values that are not valid UTF-8.
//!
//! The binary protocol accepts arbitrary bytes; REST answers in JSON, whose
//! strings must be valid UTF-8. The two meet whenever a value is written over
//! the wire and read back over HTTP.
//!
//! This used to go through `String::from_utf8_lossy`, which replaced every
//! invalid byte with U+FFFD and returned the result under a 200 — the client
//! received corrupted data with no indication anything had happened. Such a
//! value is now returned base64-encoded with an explicit `encoding` field.

use bytes::BytesMut;
use falcon_core::{Config, Node};
use falcon_wire::{encode_request, OP_GET, OP_SET, STATUS_OK};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct TestServer {
    wire_addr: std::net::SocketAddr,
    http_addr: std::net::SocketAddr,
}

async fn start_server() -> TestServer {
    let node = Arc::new(Node::build(Config::default()).unwrap());

    let wire_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wire_addr = wire_listener.local_addr().unwrap();
    let wire_node = node.clone();
    tokio::spawn(async move {
        let _ = falcon_wire::serve_with_listener(wire_node, wire_listener).await;
    });

    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let app = falcon_api::router(node);
    tokio::spawn(async move {
        axum::serve(http_listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    TestServer {
        wire_addr,
        http_addr,
    }
}

async fn read_response(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut body = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut body).await.unwrap();
    }
    (header[0], body)
}

async fn wire_set(conn: &mut TcpStream, key: &[u8], value: &[u8]) {
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", key, value);
    conn.write_all(&out).await.unwrap();
    let (status, _) = read_response(conn).await;
    assert_eq!(status, STATUS_OK);
}

/// The wire protocol itself must be byte-transparent: what goes in comes out.
#[tokio::test]
async fn wire_round_trip_preserves_arbitrary_bytes_exactly() {
    let server = start_server().await;
    let mut conn = TcpStream::connect(server.wire_addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    // Every byte value, including NULs and lone continuation bytes.
    let payload: Vec<u8> = (0..=255).collect();
    wire_set(&mut conn, b"binary:all", &payload).await;

    let mut out = BytesMut::new();
    encode_request(&mut out, OP_GET, b"", b"binary:all", b"");
    conn.write_all(&out).await.unwrap();
    let (status, got) = read_response(&mut conn).await;

    assert_eq!(status, STATUS_OK);
    assert_eq!(got, payload, "the wire protocol must be byte-transparent");
}

/// The regression test: a non-UTF-8 value read over REST must come back
/// base64-encoded and flagged, never silently mangled.
#[tokio::test]
async fn non_utf8_value_written_over_wire_is_base64_over_rest() {
    let server = start_server().await;
    let mut conn = TcpStream::connect(server.wire_addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    // 0xFF/0xFE/0xFD are not valid UTF-8 in any position.
    wire_set(&mut conn, b"binary:key", &[0xff, 0xfe, 0xfd]).await;

    let body: serde_json::Value =
        reqwest::get(format!("http://{}/cache?key=binary:key", server.http_addr))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

    assert_eq!(
        body["encoding"], "base64",
        "a non-UTF-8 value must be flagged as base64: {body}"
    );
    assert_eq!(body["value"], "//79");
    assert!(
        !body["value"].as_str().unwrap().contains('\u{FFFD}'),
        "the value must not contain replacement characters"
    );
}

/// The common case must be untouched: a UTF-8 value carries no `encoding`
/// field, so existing clients see exactly what they saw before.
#[tokio::test]
async fn utf8_value_written_over_wire_is_plain_over_rest() {
    let server = start_server().await;
    let mut conn = TcpStream::connect(server.wire_addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    wire_set(&mut conn, b"text:key", br#"{"user":42}"#).await;

    let body: serde_json::Value =
        reqwest::get(format!("http://{}/cache?key=text:key", server.http_addr))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

    assert_eq!(body["value"], r#"{"user":42}"#);
    assert!(
        body.get("encoding").is_none(),
        "a UTF-8 value must not carry an encoding field: {body}"
    );
}

/// Multi-byte UTF-8 (emoji, CJK) is valid and must stay a plain string rather
/// than being needlessly base64'd.
#[tokio::test]
async fn multibyte_utf8_is_not_treated_as_binary() {
    let server = start_server().await;
    let mut conn = TcpStream::connect(server.wire_addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    let text = "héllo 世界 🚀";
    wire_set(&mut conn, b"utf8:key", text.as_bytes()).await;

    let body: serde_json::Value =
        reqwest::get(format!("http://{}/cache?key=utf8:key", server.http_addr))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

    assert_eq!(body["value"], text);
    assert!(body.get("encoding").is_none());
}
