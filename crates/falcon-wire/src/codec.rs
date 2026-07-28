//! Incremental, zero-copy request decoder.

use crate::protocol::{Request, MAX_FRAME};
use bytes::{Bytes, BytesMut};

#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// A length field exceeded `MAX_FRAME`, or the frame is otherwise
    /// unparseable — the connection should be closed.
    Malformed,
}

const HEADER_MIN: usize = 1 + 1 + 2; // op + flags + keyspace_len

/// Try to parse ONE request from the front of `buf`. On success, advances
/// `buf` past the frame and returns the request (with key/value/keyspace as
/// zero-copy `Bytes` views). Returns `Ok(None)` when more bytes are needed
/// (a partial frame — the caller should read more and retry).
pub fn decode_one(buf: &mut BytesMut) -> Result<Option<Request>, DecodeError> {
    if buf.len() < HEADER_MIN {
        return Ok(None);
    }

    // Peek without consuming, so a partial frame leaves `buf` untouched.
    let op = buf[0];
    let flags = buf[1];
    let keyspace_len = u16::from_le_bytes([buf[2], buf[3]]) as usize;
    if keyspace_len > MAX_FRAME {
        return Err(DecodeError::Malformed);
    }

    let mut pos = HEADER_MIN;
    if buf.len() < pos + keyspace_len + 4 {
        return Ok(None);
    }
    let keyspace_start = pos;
    pos += keyspace_len;

    let key_len = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
    pos += 4;
    if key_len > MAX_FRAME {
        return Err(DecodeError::Malformed);
    }
    if buf.len() < pos + key_len + 4 {
        return Ok(None);
    }
    let key_start = pos;
    pos += key_len;

    let val_len = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
    pos += 4;
    if val_len > MAX_FRAME {
        return Err(DecodeError::Malformed);
    }
    if buf.len() < pos + val_len {
        return Ok(None);
    }
    let val_start = pos;
    let frame_end = pos + val_len;

    // The whole frame is present. Split it off as an owned `Bytes` block, then
    // slice zero-copy views into it.
    //
    // Empty fields get `Bytes::new()` rather than a slice of the frame. A slice
    // is an atomic refcount bump on creation and another on drop, and a GET
    // carries an empty value and (usually) an empty keyspace — so at a pipeline
    // depth of 128 this avoids a few hundred atomic operations per batch for
    // views that address zero bytes anyway.
    let frame = buf.split_to(frame_end).freeze();
    let keyspace = if keyspace_len == 0 {
        Bytes::new()
    } else {
        frame.slice(keyspace_start..keyspace_start + keyspace_len)
    };
    let value = if val_len == 0 {
        Bytes::new()
    } else {
        frame.slice(val_start..val_start + val_len)
    };
    let key = frame.slice(key_start..key_start + key_len);

    Ok(Some(Request {
        op,
        flags,
        keyspace,
        key,
        value,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{encode_request, OP_GET, OP_SET};
    use bytes::BytesMut;

    #[test]
    fn round_trip_single_request() {
        let mut buf = BytesMut::new();
        encode_request(&mut buf, OP_SET, b"", b"foo", b"bar");
        let req = decode_one(&mut buf).unwrap().unwrap();
        assert_eq!(req.op, OP_SET);
        assert!(req.keyspace.is_empty());
        assert_eq!(&req.key[..], b"foo");
        assert_eq!(&req.value[..], b"bar");
        assert!(buf.is_empty(), "decoder must consume the whole frame");
    }

    #[test]
    fn round_trip_with_keyspace() {
        let mut buf = BytesMut::new();
        encode_request(&mut buf, OP_GET, b"sessions", b"k", b"");
        let req = decode_one(&mut buf).unwrap().unwrap();
        assert_eq!(&req.keyspace[..], b"sessions");
        assert_eq!(&req.key[..], b"k");
        assert!(req.value.is_empty());
    }

    #[test]
    fn walks_multiple_concatenated_requests() {
        // Pipelining: several requests back-to-back in one buffer.
        let mut buf = BytesMut::new();
        encode_request(&mut buf, OP_SET, b"", b"a", b"1");
        encode_request(&mut buf, OP_SET, b"", b"b", b"2");
        encode_request(&mut buf, OP_GET, b"", b"a", b"");

        let r1 = decode_one(&mut buf).unwrap().unwrap();
        assert_eq!(&r1.key[..], b"a");
        let r2 = decode_one(&mut buf).unwrap().unwrap();
        assert_eq!(&r2.key[..], b"b");
        let r3 = decode_one(&mut buf).unwrap().unwrap();
        assert_eq!(r3.op, OP_GET);
        assert_eq!(&r3.key[..], b"a");
        assert!(decode_one(&mut buf).unwrap().is_none());
    }

    #[test]
    fn partial_frame_returns_none_without_consuming() {
        let mut full = BytesMut::new();
        encode_request(&mut full, OP_SET, b"", b"foo", b"barbaz");

        // Feed only the first few bytes.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&full[..5]);
        assert!(decode_one(&mut buf).unwrap().is_none());
        let len_before = buf.len();

        // Feed the rest; now it parses, and the earlier bytes weren't lost.
        buf.extend_from_slice(&full[5..]);
        assert!(buf.len() > len_before);
        let req = decode_one(&mut buf).unwrap().unwrap();
        assert_eq!(&req.value[..], b"barbaz");
    }

    #[test]
    fn oversized_length_is_malformed() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[OP_SET, 0]); // op, flags
        buf.extend_from_slice(&0u16.to_le_bytes()); // keyspace_len = 0
        buf.extend_from_slice(&(u32::MAX).to_le_bytes()); // absurd key_len
        assert!(matches!(decode_one(&mut buf), Err(DecodeError::Malformed)));
    }

    #[test]
    fn arbitrary_garbage_never_panics() {
        // Fuzz-style: throw many random byte sequences at the decoder and
        // assert it only ever returns Ok(None) (need more) / Ok(Some) /
        // Err(Malformed) — never panics, never hangs, never over-reads.
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..5000 {
            let len = (rng() % 64) as usize;
            let mut buf = BytesMut::new();
            for _ in 0..len {
                buf.extend_from_slice(&[(rng() & 0xff) as u8]);
            }
            // Must return without panicking regardless of content.
            let _ = decode_one(&mut buf);
        }
    }

    #[test]
    fn truncated_after_valid_frame_leaves_remainder() {
        // A complete frame followed by a partial one: decode the first,
        // then get None (not a panic) on the truncated tail.
        let mut buf = BytesMut::new();
        encode_request(&mut buf, OP_SET, b"", b"k", b"v");
        buf.extend_from_slice(&[OP_GET, 0, 0, 0]); // truncated next header
        let first = decode_one(&mut buf).unwrap().unwrap();
        assert_eq!(&first.key[..], b"k");
        assert!(decode_one(&mut buf).unwrap().is_none());
    }
}
