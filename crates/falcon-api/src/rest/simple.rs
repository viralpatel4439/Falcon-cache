//! The REST surface: one product, one URL root.
//!
//! A user never sees Falcon's internal concepts. They send a small JSON body
//! and pick the operation with the HTTP method:
//!
//! ```text
//!   POST   /cache        { "key": "...", "value": "...", "ttl": 300 }  -> { "ok": true }
//!   GET    /cache?key=...                                              -> { "value": "..." }
//!   DELETE /cache?key=...                                              -> { "ok": true }
//! ```
//!
//! There is deliberately **no scan/list**. A cache is exact-key lookup: its
//! entries expire and evict, so enumerating one returns a racy, partial
//! snapshot. Listing belongs in your system of record.
//!
//! `value` is always a string on the wire — the client JSON-stringifies whatever
//! it has (number, string, object) before sending and parses it back on read, so
//! Falcon stores the value verbatim and stays schema-free.
//!
//! A value written over the *binary* protocol may be arbitrary bytes and so may
//! not be valid UTF-8. Reading such a value over REST returns it base64-encoded
//! with an explicit `"encoding": "base64"` field, because silently substituting
//! replacement characters would return corrupted data under a success status.

use crate::rest::error::ApiError;
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// The single, fixed keyspace the cache owns on a node. Users never name it —
// the `/cache` route already knows which product it is.
pub const CACHE_KEYSPACE: &str = "cache";

// ---------- request / response bodies ----------

#[derive(Deserialize)]
pub struct KvWrite {
    pub key: String,
    /// The value as a string (client JSON-stringifies anything into it).
    pub value: String,
    /// Optional time-to-live in seconds. Omit to use the node's default.
    #[serde(default)]
    pub ttl: Option<u64>,
}

#[derive(Serialize)]
pub struct Ok {
    pub ok: bool,
}
fn ok() -> Json<Ok> {
    Json(Ok { ok: true })
}

#[derive(Serialize)]
pub struct ValueResponse {
    pub value: String,
    /// Present only when `value` is base64 — see [`ValueResponse::new`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<&'static str>,
}

impl ValueResponse {
    /// Render a stored value as JSON.
    ///
    /// The wire protocol accepts arbitrary bytes but JSON strings must be valid
    /// UTF-8, so a value written over the wire can be unrepresentable here.
    /// Such a value is returned base64-encoded with an explicit `encoding`
    /// field rather than passed through `from_utf8_lossy`, which would replace
    /// every invalid byte with U+FFFD and hand the client corrupted data while
    /// reporting success.
    fn new(value: &[u8]) -> Self {
        match std::str::from_utf8(value) {
            Ok(s) => Self {
                value: s.to_string(),
                encoding: None,
            },
            Err(_) => Self {
                value: base64_encode(value),
                encoding: Some("base64"),
            },
        }
    }
}

/// Standard base64 (RFC 4648, with padding).
///
/// Hand-rolled to keep this crate's dependency list honest: one 20-line
/// encoder on a path that only non-UTF-8 values reach does not justify a
/// dependency.
fn base64_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        let idx = [
            (n >> 18) & 0x3F,
            (n >> 12) & 0x3F,
            (n >> 6) & 0x3F,
            n & 0x3F,
        ];
        // A 1-byte tail encodes to 2 chars + "==", a 2-byte tail to 3 + "=".
        let keep = chunk.len() + 1;
        for (i, &c) in idx.iter().enumerate() {
            out.push(if i < keep {
                ALPHABET[c as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

fn key_param(params: &HashMap<String, String>) -> Result<String, ApiError> {
    params
        .get("key")
        .filter(|k| !k.is_empty())
        .cloned()
        .ok_or_else(|| ApiError::BadRequest("missing ?key=".into()))
}

// ---------- Cache route handlers ----------

/// `POST /cache` — write a key.
pub async fn cache_write(
    State(state): State<AppState>,
    Json(body): Json<KvWrite>,
) -> Result<Json<Ok>, ApiError> {
    let ks = state.node.require_keyspace(CACHE_KEYSPACE)?;
    ks.put_with_ttl(body.key.as_bytes(), body.value.as_bytes(), body.ttl);
    Ok(ok())
}

/// `GET /cache?key=` — read a key. 404 on a miss.
pub async fn cache_read(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ValueResponse>, ApiError> {
    let key = key_param(&params)?;
    let ks = state.node.require_keyspace(CACHE_KEYSPACE)?;
    match ks.get(key.as_bytes()) {
        Some(value) => Ok(Json(ValueResponse::new(&value))),
        None => Err(ApiError::NotFound),
    }
}

/// `DELETE /cache?key=` — remove a key. Idempotent: deleting an absent key
/// still reports success, since the caller's intent (the key is gone) holds
/// either way.
pub async fn cache_delete(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Ok>, ApiError> {
    let key = key_param(&params)?;
    let ks = state.node.require_keyspace(CACHE_KEYSPACE)?;
    ks.delete(key.as_bytes());
    Ok(ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 4648 §10 test vectors, plus the all-bytes case. Checked against a
    /// reference implementation — a hand-rolled encoder is only as trustworthy
    /// as the vectors pinning it.
    #[test]
    fn base64_matches_rfc4648_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        // Exercises the high bits of the alphabet ('+' and '/').
        assert_eq!(base64_encode(&[0xff, 0xfe, 0xfd]), "//79");
    }

    #[test]
    fn base64_covers_every_byte_value() {
        let all: Vec<u8> = (0..=255).collect();
        let encoded = base64_encode(&all);
        assert!(encoded.starts_with("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"));
        assert!(encoded.ends_with("+/w=="));
        assert_eq!(encoded.len(), 344); // 256 bytes -> ceil(256/3)*4
    }

    #[test]
    fn utf8_values_are_returned_verbatim_without_an_encoding_field() {
        let r = ValueResponse::new(b"{\"user\":42}");
        assert_eq!(r.value, "{\"user\":42}");
        assert_eq!(r.encoding, None);
    }

    /// The regression this guards: `from_utf8_lossy` used to turn these bytes
    /// into U+FFFD and report success, so a value written over the wire came
    /// back over REST silently corrupted.
    #[test]
    fn non_utf8_values_are_base64_not_replacement_chars() {
        let r = ValueResponse::new(&[0xff, 0xfe, 0xfd]);
        assert_eq!(r.encoding, Some("base64"));
        assert_eq!(r.value, "//79");
        assert!(!r.value.contains('\u{FFFD}'));
    }
}
