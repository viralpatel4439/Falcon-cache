//! E2: the wire (TCP) protocol must honor the configured `max_value_bytes`, not
//! just the REST layer. An oversized write is rejected with BadRequest before it
//! reaches the storage/messaging write path.

use bytes::BytesMut;
use falcon_core::{Config, Node};
use falcon_wire::{encode_request, OP_SET, STATUS_BAD_REQUEST, STATUS_OK};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct Frame {
    status: u8,
}

async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if len > 0 {
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await.unwrap();
    }
    Frame { status: header[0] }
}

async fn start_server(max_value_bytes: usize) -> std::net::SocketAddr {
    start_server_cfg(max_value_bytes, 300).await
}

async fn start_server_cfg(max_value_bytes: usize, idle_timeout_secs: u64) -> std::net::SocketAddr {
    let mut config = Config::default();
    config.storage.max_value_bytes = max_value_bytes;
    config.wire.idle_timeout_secs = idle_timeout_secs;
    let node = Arc::new(Node::build(config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = falcon_wire::serve_with_listener(node, listener).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    addr
}

#[tokio::test]
async fn oversized_wire_set_is_rejected() {
    // Cap values at 1 KiB.
    let addr = start_server(1024).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    // A value within the cap is accepted.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", b"ok", &vec![b'x'; 512]);
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_frame(&mut conn).await.status, STATUS_OK);

    // A value over the cap is rejected with BadRequest — it never reaches storage.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", b"big", &vec![b'x'; 4096]);
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(
        read_frame(&mut conn).await.status,
        STATUS_BAD_REQUEST,
        "an oversized wire value must be rejected by the configured cap"
    );
}

#[tokio::test]
async fn idle_connection_is_closed_after_timeout() {
    // F2: a client that connects and never sends must be dropped after the idle
    // timeout, not held forever. Use a 1-second timeout for a fast test.
    let addr = start_server_cfg(0, 1).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    // Send nothing. After the idle timeout the server closes the connection, so
    // a read returns 0 bytes (EOF). Give it a little margin over the 1s timeout.
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), conn.read(&mut buf))
        .await
        .expect("server should close the idle connection well within 5s")
        .expect("read after server close");
    assert_eq!(n, 0, "server must close an idle connection (EOF)");
}
