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
        Some(value) => Ok(Json(ValueResponse {
            value: String::from_utf8_lossy(&value).to_string(),
        })),
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
