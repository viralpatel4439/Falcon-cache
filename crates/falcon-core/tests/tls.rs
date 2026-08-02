//! TLS config loading.
//!
//! TLS is a headline feature configured by pointing at two PEM files, so the
//! failure modes that matter are the operator ones: a path that doesn't exist,
//! a file that isn't PEM, a cert/key pair that don't match. Each returns an
//! error rather than starting a listener that cannot complete a handshake.

use falcon_core::config::TlsConfig;
use falcon_core::tls::{init_crypto_provider, load_server_config};

/// Write a self-signed cert + key pair into `dir`, returning their paths.
fn write_cert_pair(dir: &std::path::Path, name: &str) -> (String, String) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("generate self-signed cert");
    let cert_path = dir.join(format!("{name}-cert.pem"));
    let key_path = dir.join(format!("{name}-key.pem"));
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    (
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
    )
}

fn enabled(cert: &str, key: &str) -> TlsConfig {
    TlsConfig {
        enabled: true,
        cert_file: cert.to_string(),
        key_file: key.to_string(),
    }
}

#[test]
fn disabled_tls_loads_nothing_and_does_not_touch_the_filesystem() {
    // The default path for the overwhelming majority of deployments: TLS off
    // means no file access at all, even if paths are set.
    let cfg = TlsConfig {
        enabled: false,
        cert_file: "/nonexistent/cert.pem".into(),
        key_file: "/nonexistent/key.pem".into(),
    };
    assert!(load_server_config(&cfg).unwrap().is_none());
}

#[test]
fn valid_cert_and_key_build_a_server_config() {
    init_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = write_cert_pair(dir.path(), "valid");

    let loaded = load_server_config(&enabled(&cert, &key)).expect("should load");
    assert!(loaded.is_some(), "enabled TLS with a valid pair must load");
}

#[test]
fn missing_cert_file_is_an_error_naming_the_path() {
    init_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (_, key) = write_cert_pair(dir.path(), "missing");
    let absent = dir.path().join("does-not-exist.pem");

    let err = load_server_config(&enabled(&absent.to_string_lossy(), &key))
        .expect_err("a missing cert must not start a TLS listener");
    assert!(
        err.to_string().contains("does-not-exist.pem"),
        "error should name the offending path, got: {err}"
    );
}

#[test]
fn missing_key_file_is_an_error_naming_the_path() {
    init_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (cert, _) = write_cert_pair(dir.path(), "missingkey");
    let absent = dir.path().join("no-key-here.pem");

    let err = load_server_config(&enabled(&cert, &absent.to_string_lossy()))
        .expect_err("a missing key must not start a TLS listener");
    assert!(
        err.to_string().contains("no-key-here.pem"),
        "error should name the offending path, got: {err}"
    );
}

/// Guards the empty-file rejection: an empty file reads successfully, so
/// without an explicit check it would produce an empty cert chain and a
/// listener that fails every handshake instead of failing at startup.
#[test]
fn empty_cert_file_is_rejected_rather_than_yielding_an_empty_chain() {
    init_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (_, key) = write_cert_pair(dir.path(), "empty");
    let empty = dir.path().join("empty-cert.pem");
    std::fs::write(&empty, "").unwrap();

    let err = load_server_config(&enabled(&empty.to_string_lossy(), &key))
        .expect_err("an empty cert file must be rejected");
    assert!(
        err.to_string().contains("no certificates found"),
        "expected a 'no certificates' error, got: {err}"
    );
}

#[test]
fn cert_file_containing_no_pem_is_rejected() {
    init_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (_, key) = write_cert_pair(dir.path(), "garbage");
    let garbage = dir.path().join("not-pem.txt");
    std::fs::write(&garbage, "this is not a certificate\n").unwrap();

    assert!(
        load_server_config(&enabled(&garbage.to_string_lossy(), &key)).is_err(),
        "a non-PEM cert file must be rejected"
    );
}

#[test]
fn empty_key_file_is_rejected() {
    init_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (cert, _) = write_cert_pair(dir.path(), "emptykey");
    let empty = dir.path().join("empty-key.pem");
    std::fs::write(&empty, "").unwrap();

    let err = load_server_config(&enabled(&cert, &empty.to_string_lossy()))
        .expect_err("an empty key file must be rejected");
    assert!(
        err.to_string().contains("no private key found"),
        "expected a 'no private key' error, got: {err}"
    );
}

/// A cert and key from two different pairs are individually well-formed, so
/// only rustls's own pairing check catches this. Getting it at startup is the
/// difference between a failed boot and a listener that 100% fails handshakes.
#[test]
fn mismatched_cert_and_key_are_rejected() {
    init_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (cert_a, _) = write_cert_pair(dir.path(), "pair-a");
    let (_, key_b) = write_cert_pair(dir.path(), "pair-b");

    assert!(
        load_server_config(&enabled(&cert_a, &key_b)).is_err(),
        "a cert and key from different pairs must be rejected"
    );
}

#[test]
fn init_crypto_provider_is_idempotent() {
    // Called once per server hop at startup; a second call must be a no-op
    // rather than a panic.
    init_crypto_provider();
    init_crypto_provider();
    init_crypto_provider();
}
