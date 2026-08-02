//! The wire protocol must honor `max_key_bytes` the same way it honors
//! `max_value_bytes`.
//!
//! Keys live for the entry's whole lifetime and are charged against the cache
//! budget exactly like values, so an uncapped key is the same memory risk as an
//! uncapped value. Before this cap existed, only value length was checked — a
//! 64 MiB key carrying a one-byte value passed every guard and was admitted.

use bytes::BytesMut;
use falcon_core::{Config, Node};
use falcon_wire::{encode_request, OP_DEL, OP_GET, OP_SET, STATUS_BAD_REQUEST, STATUS_OK};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

async fn start_server(max_key_bytes: usize) -> std::net::SocketAddr {
    let mut config = Config::default();
    config.storage.max_key_bytes = max_key_bytes;
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
async fn oversized_key_is_rejected_on_set() {
    let addr = start_server(64).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    // Within the cap: accepted.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", &[b'k'; 32], b"v");
    conn.write_all(&out).await.unwrap();
    assert_eq!(read_status(&mut conn).await, STATUS_OK);

    // The exact case the cap exists for: a huge key with a tiny value, which
    // the value cap alone would wave through.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", &vec![b'k'; 4096], b"v");
    conn.write_all(&out).await.unwrap();
    assert_eq!(
        read_status(&mut conn).await,
        STATUS_BAD_REQUEST,
        "an oversized key must be rejected even when the value is tiny"
    );
}

#[tokio::test]
async fn oversized_key_is_rejected_on_reads_and_deletes_too() {
    // A GET or DEL for an oversized key is just as malformed as a SET, and
    // rejecting it early keeps the engine from hashing megabytes.
    let addr = start_server(64).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    for op in [OP_GET, OP_DEL] {
        let mut out = BytesMut::new();
        encode_request(&mut out, op, b"", &vec![b'k'; 4096], b"");
        conn.write_all(&out).await.unwrap();
        assert_eq!(
            read_status(&mut conn).await,
            STATUS_BAD_REQUEST,
            "op {op:#x} with an oversized key must be rejected"
        );
    }
}

/// The cap must hold inside a pipelined batch, which takes the fast path and
/// bypasses `dispatch` entirely — the same reasoning that applies to values.
#[tokio::test]
async fn oversized_key_is_rejected_inside_a_pipelined_batch() {
    let addr = start_server(64).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", b"small-1", b"v");
    encode_request(&mut out, OP_SET, b"", &vec![b'k'; 4096], b"v"); // must be rejected
    encode_request(&mut out, OP_SET, b"", b"small-2", b"v");
    conn.write_all(&out).await.unwrap();

    // Responses come back in request order, so the rejection must be the middle
    // one and the surrounding writes must still succeed.
    assert_eq!(read_status(&mut conn).await, STATUS_OK);
    assert_eq!(
        read_status(&mut conn).await,
        STATUS_BAD_REQUEST,
        "the oversized key must be rejected on the pipelined fast path"
    );
    assert_eq!(read_status(&mut conn).await, STATUS_OK);
}

#[tokio::test]
async fn zero_disables_the_key_cap() {
    // `0` means "no limit", matching how max_value_bytes behaves.
    let addr = start_server(0).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", &vec![b'k'; 200_000], b"v");
    conn.write_all(&out).await.unwrap();
    assert_eq!(
        read_status(&mut conn).await,
        STATUS_OK,
        "max_key_bytes=0 must disable the cap"
    );
}
