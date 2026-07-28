//! Shared TLS loading for every server hop.
//!
//! One place loads the operator's PEM cert + key into a rustls `ServerConfig`,
//! so the HTTP, wire, and gRPC servers all present the same identity with the
//! same, audited path. rustls is pure-Rust and uses AES-NI, so on Falcon's
//! persistent connections the only real cost is a one-time handshake per
//! connection — the per-op hot path stays microsecond-fast.

use crate::config::TlsConfig;
use std::io;
use std::sync::Arc;

/// Install the process-wide rustls crypto provider (ring). rustls 0.23 requires
/// exactly one provider be selected before any TLS is built; call this once at
/// startup. Idempotent — a second call is a harmless no-op.
pub fn init_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Load a rustls `ServerConfig` (no client-auth) from the configured PEM files.
/// Returns `Ok(None)` when TLS is not enabled, so callers can `if let Some(..)`.
pub fn load_server_config(cfg: &TlsConfig) -> io::Result<Option<Arc<rustls::ServerConfig>>> {
    if !cfg.is_enabled() {
        return Ok(None);
    }
    let certs = load_certs(&cfg.cert_file)?;
    let key = load_key(&cfg.key_file)?;
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(Some(Arc::new(server_config)))
}

fn load_certs(path: &str) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let data = std::fs::read(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading TLS cert '{path}': {e}")))?;
    let mut reader = io::BufReader::new(&data[..]);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader).collect::<Result<_, _>>()?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates found in '{path}'"),
        ));
    }
    Ok(certs)
}

fn load_key(path: &str) -> io::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let data = std::fs::read(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading TLS key '{path}': {e}")))?;
    let mut reader = io::BufReader::new(&data[..]);
    rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key found in '{path}'"),
        )
    })
}
