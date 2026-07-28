use crate::rest::{handlers, simple};
use crate::state::AppState;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use falcon_core::Node;
use std::net::SocketAddr;
use std::sync::Arc;

/// Build the router for a node: the cache itself plus the health probes.
///
/// Configuration is a CLI-only path (`falcon config`, which edits the profile
/// file in process), so there is no config endpoint here — and no UI to serve.
pub fn router(node: Arc<Node>) -> Router {
    let state = AppState { node };

    let max_body = state.node.config().storage.max_value_bytes;

    let mut app = Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/health", get(handlers::health))
        // Falcon Cache — POST /cache {key,value,ttl?} · GET/DELETE /cache?key=
        // No scan: a cache is exact-key lookup by design. Entries expire and
        // are evicted, so enumerating one returns a racy, partial snapshot.
        .route(
            "/cache",
            get(simple::cache_read)
                .post(simple::cache_write)
                .delete(simple::cache_delete),
        );

    // Anti-OOM: cap request body size so a single huge PUT can't exhaust
    // memory. 0 disables the cap. Applied before handlers run.
    if max_body > 0 {
        app = app.layer(axum::extract::DefaultBodyLimit::max(max_body));
    }

    // Only attach the auth layer when a token is configured — zero cost
    // (not even a layer in the stack) when auth is off.
    if state.node.config().auth.is_enabled() {
        app = app.layer(middleware::from_fn_with_state(state.clone(), auth_middleware));
    }

    app.with_state(state)
}

/// Rejects requests without the API key. The key may be presented as an
/// `Authorization: Bearer <key>` header (preferred — not logged) or an
/// `api_key=<key>` query parameter (fallback for clients that cannot set
/// request headers).
///
/// The query-param form is only as safe as the transport: use TLS so the URL
/// isn't sniffable, and note URLs may appear in proxy/access logs.
/// The probes are always exempt so orchestrators work unauthenticated.
async fn auth_middleware(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // The probes are always unauthenticated so liveness/readiness checks work
    // without distributing a key to the orchestrator. `/health` carries only
    // aggregate counters (hit rate, key count, bytes) — never key or value data.
    let path = req.uri().path();
    if matches!(path, "/healthz" | "/readyz" | "/health") {
        return Ok(next.run(req).await);
    }
    let token = &state.node.config().auth.api_key;

    // 1. Authorization: Bearer <key>
    let header_key = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    // 2. ?api_key=<key> query param (browser fallback).
    let query_key = req.uri().query().and_then(|q| {
        q.split('&')
            .find_map(|kv| kv.strip_prefix("api_key="))
    });

    let presented = header_key.or(query_key).unwrap_or("");
    if constant_time_eq(presented.as_bytes(), token.as_bytes()) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Length-independent-ish constant-time comparison to avoid leaking the
/// token via response timing.
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

pub async fn serve(node: Arc<Node>, bind: SocketAddr) -> std::io::Result<()> {
    let app = router(node);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "HTTP server listening");
    axum::serve(listener, app).await
}

/// Like `serve`, but stops accepting new connections and drains in-flight
/// requests when `shutdown` resolves — the graceful path for SIGTERM during
/// an autoscale/rollout. The cache holds nothing durable, so draining is the
/// whole of shutdown.
pub async fn serve_with_shutdown<F>(
    node: Arc<Node>,
    bind: SocketAddr,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    // Load TLS once (shared loader) before building the app so a cert error
    // fails fast at startup rather than mid-serve.
    let tls = falcon_core::tls::load_server_config(&node.config().tls)?;
    let app = router(node);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    match tls {
        None => {
            tracing::info!(%bind, "HTTP server listening (graceful shutdown enabled)");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await
        }
        Some(tls) => {
            tracing::info!(%bind, "HTTPS server listening [TLS] (graceful shutdown enabled)");
            serve_tls(listener, app, tls, shutdown).await
        }
    }
}

/// Serve the axum app over rustls. Each accepted TCP connection is TLS-wrapped
/// with `tokio-rustls`, then handed to hyper with HTTP/1 + HTTP/2 auto-detect.
/// The TLS handshake is per-connection (Falcon uses persistent connections), so
/// the per-request cost is just AES-NI-accelerated record encryption (µs).
async fn serve_tls<F>(
    listener: tokio::net::TcpListener,
    app: Router,
    tls: std::sync::Arc<rustls::ServerConfig>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnBuilder;
    use tower::Service;

    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let (stream, _peer) = tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok(v) => v,
                Err(e) => { tracing::warn!(error = %e, "accept failed"); continue; }
            },
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(error = %e, "TLS handshake failed");
                    return;
                }
            };
            // Adapt the tower Service (axum Router) into a hyper service.
            let svc = hyper::service::service_fn(move |req| {
                let mut app = app.clone();
                async move { app.call(req).await }
            });
            if let Err(e) = ConnBuilder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(tls_stream), svc)
                .await
            {
                tracing::debug!(error = %e, "TLS connection error");
            }
        });
    }
    Ok(())
}
