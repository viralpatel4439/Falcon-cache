use bytes::BytesMut;
use falcon_core::{Config, Node};
use falcon_wire::{encode_request, OP_AUTH, OP_SET, STATUS_OK, STATUS_UNAUTHORIZED};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn start(config: Config) -> std::net::SocketAddr {
    let node = Arc::new(Node::build(config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = falcon_wire::serve_with_listener(node, listener).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    addr
}

async fn read_status(stream: &mut TcpStream) -> u8 {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if len > 0 {
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await.unwrap();
    }
    header[0]
}

fn config_with_token(token: &str) -> Config {
    let mut config = Config::default();
    config.auth.api_key = token.to_string();
    config
}

#[tokio::test]
async fn wire_requires_auth_before_ops_when_enabled() {
    let addr = start(config_with_token("t0ken")).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    // A SET before AUTH is rejected.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", b"k", b"v");
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_status(&mut conn).await, STATUS_UNAUTHORIZED);

    // Wrong token AUTH -> unauthorized.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_AUTH, b"", b"", b"wrong");
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_status(&mut conn).await, STATUS_UNAUTHORIZED);

    // Correct token AUTH -> OK, then SET works.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_AUTH, b"", b"", b"t0ken");
    encode_request(&mut out, OP_SET, b"", b"k", b"v");
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_status(&mut conn).await, STATUS_OK); // auth ok
    assert_eq!(read_status(&mut conn).await, STATUS_OK); // set ok
}

#[tokio::test]
async fn wire_no_auth_needed_when_disabled() {
    // token empty -> auth off
    let addr = start(Config::default()).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", b"k", b"v");
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_status(&mut conn).await, STATUS_OK);
}
