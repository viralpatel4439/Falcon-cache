#![forbid(unsafe_code)]

//! Lean binary TCP protocol server with pipelining, for Redis-competitive
//! throughput. A pure front-end over `kv-core::Node` — it calls the same
//! `Keyspace` methods the HTTP API does, so replication, subscriptions,
//! group-commit durability, and per-key ordering are all inherited.

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
    tokio::pin!(shutdown);
    loop {
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
    loop {
        let (stream, _peer) = listener.accept().await?;
        // Disable Nagle: pipelined replies must flush immediately, otherwise
        // the kernel coalesces small writes and destroys pipeline latency.
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!(?e, "failed to set TCP_NODELAY");
        }
        let node = node.clone();
        tokio::spawn(async move {
            if let Err(e) = conn::handle_conn(node, stream).await {
                tracing::debug!(?e, "wire connection ended with error");
            }
        });
    }
}