#![forbid(unsafe_code)]

//! Lean binary TCP protocol server with pipelining, for Redis-competitive
//! throughput.
//!
//! A pure front-end over [`falcon_core::Node`]: it calls the same [`Keyspace`]
//! methods the HTTP API does, so both protocols see one cache with identical
//! TTL, eviction, and capacity semantics. The wire protocol exists for the hot
//! path — one persistent stream, many pipelined ops per round trip — while REST
//! exists for reach.
//!
//! [`Keyspace`]: falcon_core::Keyspace

mod codec;
mod conn;
pub mod protocol;

pub use protocol::{
    encode_request, Request, Response, OP_AUTH, OP_DEL, OP_GET, OP_PING, OP_SET,
    STATUS_BAD_REQUEST, STATUS_NOT_FOUND, STATUS_OK, STATUS_PONG, STATUS_UNAUTHORIZED,
};

use falcon_core::Node;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Build the accept-limiting semaphore for a node, or `None` when the operator
/// disabled the cap with `0`.
///
/// Each connection holds a task plus read and write buffers that grow with
/// pipeline depth, so without this the number of clients decides the process's
/// memory ceiling — the one thing a cache with a hard memory bound must not
/// concede.
fn connection_limiter(node: &Node) -> Option<Arc<Semaphore>> {
    match node.config().wire.max_connections {
        0 => None,
        n => Some(Arc::new(Semaphore::new(n))),
    }
}

/// Wait for a connection slot. `None` means no cap is configured.
///
/// Backpressure rather than rejection: at the cap, `accept` simply stops until
/// a live connection finishes. The pending client waits in the kernel's accept
/// queue instead of being handed a socket the server has no budget to serve.
async fn acquire_slot(limiter: &Option<Arc<Semaphore>>) -> Option<OwnedSemaphorePermit> {
    match limiter {
        // The semaphore is never closed, so acquiring cannot fail.
        Some(sem) => sem.clone().acquire_owned().await.ok(),
        None => None,
    }
}

pub async fn serve(node: Arc<Node>, bind: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    serve_with_listener(node, listener).await
}

/// Like `serve`, but stops accepting new connections when `shutdown`
/// resolves (graceful drain on SIGTERM). In-flight connections finish on
/// their own; the process's final flush covers durability.
pub async fn serve_with_shutdown<F>(
    node: Arc<Node>,
    bind: SocketAddr,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let listener = TcpListener::bind(bind).await?;
    // Optional transport TLS (shared loader). Persistent connections make the
    // handshake a one-time per-connection cost; per-op stays fast.
    let tls = falcon_core::tls::load_server_config(&node.config().tls)?
        .map(tokio_rustls::TlsAcceptor::from);
    if let Ok(addr) = listener.local_addr() {
        if tls.is_some() {
            tracing::info!(bind = %addr, "binary wire server listening [TLS] (graceful shutdown enabled)");
        } else {
            tracing::info!(bind = %addr, "binary wire server listening (graceful shutdown enabled)");
        }
    }
    let limiter = connection_limiter(&node);
    tokio::pin!(shutdown);
    loop {
        // Take the slot before accepting, so at the cap the pending client
        // waits in the kernel accept queue rather than being handed a socket.
        let permit = acquire_slot(&limiter).await;
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("wire server draining on shutdown signal");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _peer) = accepted?;
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::debug!(?e, "failed to set TCP_NODELAY");
                }
                let node = node.clone();
                let tls = tls.clone();
                tokio::spawn(async move {
                    // Held for the connection's lifetime; released on drop.
                    let _permit = permit;
                    match tls {
                        Some(acceptor) => match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                if let Err(e) = conn::handle_conn(node, tls_stream).await {
                                    tracing::debug!(?e, "wire connection ended with error");
                                }
                            }
                            Err(e) => tracing::debug!(?e, "wire TLS handshake failed"),
                        },
                        None => {
                            if let Err(e) = conn::handle_conn(node, stream).await {
                                tracing::debug!(?e, "wire connection ended with error");
                            }
                        }
                    }
                });
            }
        }
    }
}

/// Serve on an already-bound listener. Useful for tests that need to know
/// the ephemeral port before the server starts accepting.
pub async fn serve_with_listener(node: Arc<Node>, listener: TcpListener) -> std::io::Result<()> {
    if let Ok(addr) = listener.local_addr() {
        tracing::info!(bind = %addr, "binary wire server listening");
    }
    let limiter = connection_limiter(&node);
    loop {
        let permit = acquire_slot(&limiter).await;
        let (stream, _peer) = listener.accept().await?;
        // Disable Nagle: pipelined replies must flush immediately, otherwise
        // the kernel coalesces small writes and destroys pipeline latency.
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!(?e, "failed to set TCP_NODELAY");
        }
        let node = node.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = conn::handle_conn(node, stream).await {
                tracing::debug!(?e, "wire connection ended with error");
            }
        });
    }
}
