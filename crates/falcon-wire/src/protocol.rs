//! Wire protocol constants and response encoding.
//!
//! A lean, length-delimited binary protocol over a persistent TCP stream,
//! designed for pipelining: because every field is length-prefixed, a
//! reader can walk a stream of concatenated requests and a client can send
//! many requests back-to-back without waiting for replies.
//!
//! Request frame (little-endian):
//!   [op:u8][flags:u8][keyspace_len:u16][keyspace][key_len:u32][key][val_len:u32][val]
//! Response frame:
//!   [status:u8][val_len:u32][val]
//!
//! One TCP connection is a single strictly-ordered stream; responses are
//! written back in request order, so no request IDs are needed (same model
//! as Redis RESP).

use bytes::{BufMut, Bytes, BytesMut};

// KV opcodes.
pub const OP_PING: u8 = 0x00;
pub const OP_GET: u8 = 0x01;
pub const OP_SET: u8 = 0x02;
pub const OP_DEL: u8 = 0x03;
/// Authenticate a connection: value = token. Required as the first frame
/// when auth is enabled; a no-op (always OK) when auth is off.
pub const OP_AUTH: u8 = 0x05;

// Status codes. The numeric values are those of the multi-product protocol,
// left unchanged so an existing client library keeps decoding this server
// correctly. The codes only a messaging product could return are simply never
// emitted here.
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_NOT_FOUND: u8 = 0x01;
pub const STATUS_BAD_REQUEST: u8 = 0x02;
pub const STATUS_UNKNOWN_KEYSPACE: u8 = 0x03;
/// Reserved, never emitted. A pure-RAM cache has no I/O to fail: there is no
/// disk write to error, no replica to lose, and no downstream to time out.
/// The number stays claimed so a future status cannot reuse `0x04` and change
/// what an existing client decodes.
pub const STATUS_SERVER_ERROR: u8 = 0x04;
pub const STATUS_PONG: u8 = 0x05;
pub const STATUS_UNAUTHORIZED: u8 = 0x0a; // auth required or token mismatch

/// Reject absurd/hostile frame sizes rather than allocating for them.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// A decoded request. `key`/`value`/`keyspace` are zero-copy views into
/// the connection's read buffer.
#[derive(Debug, Clone)]
pub struct Request {
    pub op: u8,
    pub flags: u8,
    pub keyspace: Bytes, // empty => default keyspace
    pub key: Bytes,
    pub value: Bytes, // empty for GET/DEL/PING
}

/// A response to encode back to the client.
#[derive(Debug, Clone)]
pub enum Response {
    /// SET/DEL success.
    Ok,
    /// GET hit whose bytes the engine already owns behind an `Arc`.
    ///
    /// Encoding writes the slice straight into the connection's output buffer,
    /// so the value is copied exactly once — into the socket buffer — instead of
    /// twice (engine → `Vec`, `Vec` → output buffer). At a pipeline depth of
    /// 128 that removes 128 allocations and 128 copies per batch.
    ///
    /// There is deliberately no owned `Value(Vec<u8>)` counterpart: every hit
    /// comes from the engine already behind an `Arc`, so an owned variant could
    /// only ever add a copy.
    ValueShared(std::sync::Arc<[u8]>),
    /// GET miss.
    NotFound,
    /// PING reply.
    Pong,
    /// Malformed request, or one exceeding a configured size cap.
    BadRequest,
    /// The named keyspace does not exist on this node.
    UnknownKeyspace,
    /// Auth required, or token mismatch.
    Unauthorized,
}

/// Appends a request frame to `out` (little-endian). Public so clients
/// (e.g. the benchmark harness) can build pipelined requests without
/// re-implementing the framing.
pub fn encode_request(out: &mut BytesMut, op: u8, keyspace: &[u8], key: &[u8], value: &[u8]) {
    out.put_u8(op);
    out.put_u8(0); // flags
    out.put_u16_le(keyspace.len() as u16);
    out.put_slice(keyspace);
    out.put_u32_le(key.len() as u32);
    out.put_slice(key);
    out.put_u32_le(value.len() as u32);
    out.put_slice(value);
}

impl Response {
    /// Appends this response's frame to `out` (little-endian).
    pub fn encode(&self, out: &mut BytesMut) {
        match self {
            Response::Ok => {
                out.put_u8(STATUS_OK);
                out.put_u32_le(0);
            }
            Response::ValueShared(v) => {
                out.put_u8(STATUS_OK);
                out.put_u32_le(v.len() as u32);
                out.put_slice(v);
            }
            Response::NotFound => {
                out.put_u8(STATUS_NOT_FOUND);
                out.put_u32_le(0);
            }
            Response::Pong => {
                out.put_u8(STATUS_PONG);
                out.put_u32_le(0);
            }
            Response::BadRequest => {
                out.put_u8(STATUS_BAD_REQUEST);
                out.put_u32_le(0);
            }
            Response::UnknownKeyspace => {
                out.put_u8(STATUS_UNKNOWN_KEYSPACE);
                out.put_u32_le(0);
            }
            Response::Unauthorized => {
                out.put_u8(STATUS_UNAUTHORIZED);
                out.put_u32_le(0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes_are_distinct() {
        let all = [
            STATUS_OK,
            STATUS_NOT_FOUND,
            STATUS_BAD_REQUEST,
            STATUS_UNKNOWN_KEYSPACE,
            STATUS_SERVER_ERROR,
            STATUS_PONG,
            STATUS_UNAUTHORIZED,
        ];
        let mut sorted = all.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), all.len(), "status codes must be unique");
    }

    #[test]
    fn ok_and_value_frames_carry_their_length_prefix() {
        // The framing contract the client decoder relies on: status byte, then
        // a 4-byte LE length, then exactly that many payload bytes.
        let mut out = BytesMut::new();
        Response::Ok.encode(&mut out);
        assert_eq!(out[0], STATUS_OK);
        assert_eq!(u32::from_le_bytes([out[1], out[2], out[3], out[4]]), 0);
        assert_eq!(out.len(), 5);

        let mut out = BytesMut::new();
        Response::ValueShared(std::sync::Arc::from(&b"hello"[..])).encode(&mut out);
        assert_eq!(out[0], STATUS_OK);
        assert_eq!(u32::from_le_bytes([out[1], out[2], out[3], out[4]]), 5);
        assert_eq!(&out[5..], b"hello");
    }
}
