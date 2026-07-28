//! Per-connection read/dispatch/write loop — the pipelining core.

use crate::codec::{decode_one, DecodeError};
use crate::protocol::{Request, Response, OP_AUTH, OP_DEL, OP_GET, OP_PING, OP_SET};
use bytes::BytesMut;
use falcon_core::Node;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Starting size of a connection's read and write buffers.
///
/// Deliberately small. Buffers grow on demand, so a connection that pipelines
/// deeply pays for the space it actually uses, while the far more common
/// shallow connection never reserves 64 KiB it will not touch. With 64
/// connections the old fixed sizing reserved ~16 MiB of buffers against ~2 MiB
/// of cached data — the buffers, not the cache, dominated the process
/// footprint.
const INITIAL_BUF: usize = 8 * 1024;

/// A connection's buffers are released back to [`INITIAL_BUF`] once they exceed
/// this and the traffic no longer needs the space. Without it, one deep batch
/// would inflate a long-lived connection permanently.
const BUF_RECLAIM_ABOVE: usize = 256 * 1024;

/// Minimum spare capacity guaranteed to each socket read.
///
/// `read_buf` fills only the buffer's *spare* capacity, and the decoder drains
/// completed frames off the front, so a `BytesMut` left to its own devices can
/// settle at a few hundred spare bytes. A read window that small turns one
/// pipelined batch into one syscall per request: the loop reads a fragment,
/// decodes a single frame, and — since it dispatches as soon as anything
/// decodes — replies to that one request before reading again. Deep *writes*
/// suffer most, because a SET request carries its value and so a batch is far
/// larger than the equivalent GET batch.
///
/// Reserving a floor before every read keeps a whole batch arriving in one or
/// two syscalls, which is what the removed `BufReader` used to provide.
const MIN_READ_SPACE: usize = 32 * 1024;
/// The keyspace a request falls back to when it sends an empty keyspace field.
/// A cache node builds exactly one keyspace — `cache` — so an unqualified wire
/// request targets it.
const DEFAULT_KEYSPACE: &str = "cache";

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Handle one connection over any byte stream — a plain `TcpStream` or a
/// `tokio_rustls` TLS stream. Generic so the wire protocol runs identically
/// with or without transport encryption.
pub async fn handle_conn<S>(node: Arc<Node>, stream: S) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    // No `BufReader`/`BufWriter`. Both would be pure overhead here: `read_buf`
    // already reads straight into `in_buf`, and responses are accumulated in
    // `out_buf` and written as one contiguous slice — handing that to a
    // `BufWriter` copies the whole batch into a second buffer before the same
    // single syscall. At depth=128 that was ~17 KiB memcpy'd per batch, about
    // 0.6 GiB/s of copying that bought nothing.
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut in_buf = BytesMut::with_capacity(INITIAL_BUF);
    let mut out_buf = BytesMut::with_capacity(INITIAL_BUF);

    // Auth gate: when a token is configured, the connection must present a
    // matching AUTH frame before any other op is honored. Off by default
    // (`authed` starts true), so the fast path pays nothing.
    let auth = &node.config().auth;
    let mut authed = !auth.is_enabled();

    // Idle timeout: close a connection that sends nothing for this long, so an
    // idle/half-open/slowloris client can't hold a task + socket forever. `0`
    // disables it.
    let idle_timeout = match node.config().wire.idle_timeout_secs {
        0 => None,
        secs => Some(std::time::Duration::from_secs(secs)),
    };

    loop {
        // 1. Drain every fully-buffered request into a pipeline batch.
        let mut batch: Vec<Request> = Vec::new();
        loop {
            match decode_one(&mut in_buf) {
                Ok(Some(req)) => batch.push(req),
                Ok(None) => break, // need more bytes
                Err(DecodeError::Malformed) => {
                    out_buf.clear();
                    Response::BadRequest.encode(&mut out_buf);
                    let _ = writer.write_all(&out_buf).await;
                    return Ok(());
                }
            }
        }

        // 2. Nothing decoded yet — read more (the only awaited blocking point).
        if batch.is_empty() {
            // Guarantee a useful read window; see `MIN_READ_SPACE`.
            if in_buf.capacity() - in_buf.len() < MIN_READ_SPACE {
                in_buf.reserve(MIN_READ_SPACE);
            }
            let n = match idle_timeout {
                Some(dur) => match tokio::time::timeout(dur, reader.read_buf(&mut in_buf)).await {
                    Ok(r) => r?,
                    Err(_) => {
                        // Idle too long: close the connection.
                            return Ok(());
                    }
                },
                None => reader.read_buf(&mut in_buf).await?,
            };
            if n == 0 {
                return Ok(()); // client closed the connection
            }
            continue;
        }

        // 3. Dispatch the batch sequentially, encoding responses in request
        //    order. (Measured: concurrent fan-out via join_all was *slower*
        //    here — each GET is a sub-microsecond DashMap lookup with no
        //    real await to overlap, so the future-vec bookkeeping cost more
        //    than it saved. Sequential wins.) A SUBSCRIBE converts the
        //    connection into a push stream and never returns.
        out_buf.clear();
        // Keyspace resolution memoized across the batch; see the fast path
        // below. Almost every real batch uses one keyspace throughout.
        let mut cached_ks: Option<&falcon_core::Keyspace> = None;
        let mut cached_ks_bytes: Option<bytes::Bytes> = None;
        let max_value = node.config().storage.max_value_bytes;
        for req in batch.iter() {
            // Auth gate: before authentication, only an AUTH frame is honored.
            if req.op == OP_AUTH {
                if constant_time_eq(&req.value, auth.api_key.as_bytes()) {
                    authed = true;
                    Response::Ok.encode(&mut out_buf);
                } else {
                    Response::Unauthorized.encode(&mut out_buf);
                }
                continue;
            }
            if !authed {
                Response::Unauthorized.encode(&mut out_buf);
                continue;
            }
            // Fast path for the pipelined KV ops. The keyspace is resolved
            // once per run of identical keyspace bytes rather than per request,
            // and an engine that can answer without awaiting (the cache) does
            // so directly — no future is built or polled for it.
            // The value-size cap is an anti-OOM guard, so it must hold on this
            // path exactly as it does in `dispatch` — otherwise a client could
            // pipeline oversized values straight past it.
            if matches!(req.op, OP_GET | OP_SET | OP_DEL)
                && !(max_value > 0 && req.op == OP_SET && req.value.len() > max_value)
            {
                if cached_ks_bytes.as_deref() != Some(&req.keyspace[..]) {
                    cached_ks = name_str(&req.keyspace, DEFAULT_KEYSPACE)
                        .and_then(|name| node.keyspace(name));
                    cached_ks_bytes = Some(req.keyspace.clone());
                }
                match cached_ks {
                    Some(ks) => dispatch_kv_on(ks, req).encode(&mut out_buf),
                    None => Response::UnknownKeyspace.encode(&mut out_buf),
                }
                continue;
            }
            dispatch(&node, req).encode(&mut out_buf);
        }
        // 4. One write syscall per batch, straight from the response buffer —
        //    no intermediate copy.
        writer.write_all(&out_buf).await?;

        // 5. Release the space a deep batch claimed, so a connection that
        //    pipelines hard once does not hold the memory forever. `in_buf` is
        //    only shrunk when empty, since it may hold a partial frame.
        if out_buf.capacity() > BUF_RECLAIM_ABOVE {
            out_buf = BytesMut::with_capacity(INITIAL_BUF);
        }
        if in_buf.is_empty() && in_buf.capacity() > BUF_RECLAIM_ABOVE {
            in_buf = BytesMut::with_capacity(INITIAL_BUF);
        }
    }
}

/// Borrow the keyspace/topic/queue name from the request bytes without
/// allocating; empty means the default keyspace.
fn name_str<'a>(bytes: &'a [u8], default: &'a str) -> Option<&'a str> {
    if bytes.is_empty() {
        Some(default)
    } else {
        std::str::from_utf8(bytes).ok()
    }
}

fn dispatch(node: &Arc<Node>, req: &Request) -> Response {
    // Enforce the configured value-size cap on every write-bearing op, matching
    // the REST layer's DefaultBodyLimit. The wire codec's MAX_FRAME is a hard
    // ceiling independent of config; this honors a LOWER operator-set limit so a
    // client can't pipeline oversized values straight into the write path.
    let max_value = node.config().storage.max_value_bytes;
    if max_value > 0 && req.op == OP_SET && req.value.len() > max_value
    {
        return Response::BadRequest;
    }
    match req.op {
        OP_PING => Response::Pong,
        OP_GET | OP_SET | OP_DEL => dispatch_kv(node, req),
        _ => Response::BadRequest,
    }
}

fn dispatch_kv(node: &Node, req: &Request) -> Response {
    let ks_name = match name_str(&req.keyspace, DEFAULT_KEYSPACE) {
        Some(s) => s,
        None => return Response::BadRequest,
    };
    let ks = match node.keyspace(ks_name) {
        Some(ks) => ks,
        None => return Response::UnknownKeyspace,
    };
    dispatch_kv_on(ks, req)
}

/// Serve one KV op against an already-resolved keyspace.
///
/// Split out so a pipelined batch resolves the keyspace once rather than
/// hashing the same name for every operation in the batch.
///
/// Synchronous, and deliberately so: every cache operation is a shard lock plus
/// a hash operation with no I/O to await, so a pipelined batch of depth N is
/// served without constructing and polling N futures of pure bookkeeping.
fn dispatch_kv_on(ks: &falcon_core::Keyspace, req: &Request) -> Response {
    match req.op {
        OP_GET => match ks.get(&req.key) {
            Some(v) => Response::ValueShared(v),
            None => Response::NotFound,
        },
        OP_SET => {
            ks.put(&req.key, &req.value);
            Response::Ok
        }
        OP_DEL => {
            ks.delete(&req.key);
            Response::Ok
        }
        _ => Response::BadRequest,
    }
}

