use bytes::BytesMut;
use falcon_core::{Config, Node};
use falcon_wire::{
    encode_request, OP_DEL, OP_GET, OP_PING, OP_SET, STATUS_NOT_FOUND, STATUS_OK, STATUS_PONG,
};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct TestServer {
    wire_addr: std::net::SocketAddr,
    http_addr: std::net::SocketAddr,
}

async fn start_server() -> TestServer {
    let node = Arc::new(Node::build(Config::default()).unwrap());

    // Wire server on an ephemeral port (bind first, then serve — no race).
    let wire_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wire_addr = wire_listener.local_addr().unwrap();
    let wire_node = node.clone();
    tokio::spawn(async move {
        let _ = falcon_wire::serve_with_listener(wire_node, wire_listener).await;
    });

    // HTTP server on an ephemeral port, sharing the same Node.
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let app = falcon_api::router(node);
    tokio::spawn(async move {
        axum::serve(http_listener, app).await.unwrap();
    });

    // Give the wire server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    TestServer {
        wire_addr,
        http_addr,
    }
}

struct Resp {
    status: u8,
    value: Vec<u8>,
}

async fn read_response(stream: &mut TcpStream) -> Resp {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    let status = header[0];
    let val_len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut value = vec![0u8; val_len];
    if val_len > 0 {
        stream.read_exact(&mut value).await.unwrap();
    }
    Resp { status, value }
}

#[tokio::test]
async fn pipelined_set_get_del_over_wire() {
    let server = start_server().await;
    let mut stream = TcpStream::connect(server.wire_addr).await.unwrap();
    stream.set_nodelay(true).unwrap();

    // Pipeline: PING, SET foo=bar, GET foo, DEL foo, GET foo — all sent
    // before reading any reply.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_PING, b"", b"", b"");
    encode_request(&mut out, OP_SET, b"", b"foo", b"bar");
    encode_request(&mut out, OP_GET, b"", b"foo", b"");
    encode_request(&mut out, OP_DEL, b"", b"foo", b"");
    encode_request(&mut out, OP_GET, b"", b"foo", b"");
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();

    // Responses come back in request order.
    let ping = read_response(&mut stream).await;
    assert_eq!(ping.status, STATUS_PONG);

    let set = read_response(&mut stream).await;
    assert_eq!(set.status, STATUS_OK);

    let get1 = read_response(&mut stream).await;
    assert_eq!(get1.status, STATUS_OK);
    assert_eq!(get1.value, b"bar");

    let del = read_response(&mut stream).await;
    assert_eq!(del.status, STATUS_OK);

    let get2 = read_response(&mut stream).await;
    assert_eq!(get2.status, STATUS_NOT_FOUND);
    assert!(get2.value.is_empty());
}

#[tokio::test]
async fn value_written_over_wire_is_visible_via_http() {
    let server = start_server().await;
    let mut stream = TcpStream::connect(server.wire_addr).await.unwrap();
    stream.set_nodelay(true).unwrap();

    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", b"shared", b"cross-protocol");
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
    let set = read_response(&mut stream).await;
    assert_eq!(set.status, STATUS_OK);

    // Same Node underneath, so the HTTP API must see it.
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/cache?key=shared", server.http_addr))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["value"], "cross-protocol");
}

#[tokio::test]
async fn deep_pipeline_preserves_order() {
    let server = start_server().await;
    let mut stream = TcpStream::connect(server.wire_addr).await.unwrap();
    stream.set_nodelay(true).unwrap();

    // Pipeline 500 SETs to distinct keys, then 500 GETs, all before reading.
    const N: usize = 500;
    let mut out = BytesMut::new();
    for i in 0..N {
        encode_request(
            &mut out,
            OP_SET,
            b"",
            format!("k{i}").as_bytes(),
            format!("v{i}").as_bytes(),
        );
    }
    for i in 0..N {
        encode_request(&mut out, OP_GET, b"", format!("k{i}").as_bytes(), b"");
    }
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();

    for _ in 0..N {
        assert_eq!(read_response(&mut stream).await.status, STATUS_OK);
    }
    for i in 0..N {
        let r = read_response(&mut stream).await;
        assert_eq!(r.status, STATUS_OK);
        assert_eq!(
            r.value,
            format!("v{i}").into_bytes(),
            "GET k{i} returned wrong value"
        );
    }
}

/// A batch far larger than a connection's initial buffers must still come back
/// complete and in order.
///
/// The request/response loop no longer wraps the socket in `BufReader`/
/// `BufWriter` — responses are accumulated in one buffer and written directly,
/// and both buffers start small and grow on demand. This exercises that growth
/// with a batch whose response (256 × 1 KiB ≈ 264 KiB) is ~33× the 8 KiB the
/// connection starts with, and which cannot fit in a single socket write.
#[tokio::test]
async fn deep_pipeline_with_large_values_round_trips() {
    let server = start_server().await;
    let mut stream = TcpStream::connect(server.wire_addr).await.unwrap();
    stream.set_nodelay(true).unwrap();

    const N: usize = 256;
    let value = vec![b'x'; 1024];

    // Write N SETs as one pipelined batch.
    let mut req = BytesMut::new();
    for i in 0..N {
        encode_request(&mut req, OP_SET, b"", format!("k{i}").as_bytes(), &value);
    }
    stream.write_all(&req).await.unwrap();
    let mut acks = vec![0u8; N * 5];
    stream.read_exact(&mut acks).await.unwrap();
    for i in 0..N {
        assert_eq!(acks[i * 5], STATUS_OK, "SET {i} must be acked");
    }

    // Read them all back in one batch and check every value survives intact.
    let mut req = BytesMut::new();
    for i in 0..N {
        encode_request(&mut req, OP_GET, b"", format!("k{i}").as_bytes(), b"");
    }
    stream.write_all(&req).await.unwrap();

    let mut resp = vec![0u8; N * (5 + value.len())];
    stream.read_exact(&mut resp).await.unwrap();
    for i in 0..N {
        let at = i * (5 + value.len());
        assert_eq!(resp[at], STATUS_OK, "GET {i} must hit");
        let len = u32::from_le_bytes(resp[at + 1..at + 5].try_into().unwrap()) as usize;
        assert_eq!(len, value.len(), "GET {i} length");
        assert_eq!(&resp[at + 5..at + 5 + len], &value[..], "GET {i} payload");
    }

    // The connection is still usable after the buffers were grown and reclaimed.
    let mut req = BytesMut::new();
    encode_request(&mut req, OP_PING, b"", b"", b"");
    stream.write_all(&req).await.unwrap();
    let mut pong = [0u8; 5];
    stream.read_exact(&mut pong).await.unwrap();
    assert_eq!(pong[0], STATUS_PONG);
}
