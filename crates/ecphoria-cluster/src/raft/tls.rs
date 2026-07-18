//! TLS for the inter-node Raft gRPC transport (encryption in transit + optional mutual TLS).
//!
//! Builds tonic server/client TLS configs from PEM files referenced by [`RaftTlsConfig`]. When a CA
//! is provided, the server requires client certs (mTLS) and clients verify peers against it.

use std::fs;

use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

use crate::config::RaftTlsConfig;

fn read_pem(path: &str, what: &str) -> crate::Result<Vec<u8>> {
    fs::read(path).map_err(|e| crate::Error::Coordination(format!("read TLS {what} '{path}': {e}")))
}

/// rustls 0.23 requires a process-wide `CryptoProvider`; install the ring provider once. Idempotent
/// (a second install is ignored) so it's safe to call from every TLS-config build.
fn ensure_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Server-side TLS: present this node's identity; require + verify client certs when a CA is set.
pub fn server_tls(cfg: &RaftTlsConfig) -> crate::Result<ServerTlsConfig> {
    ensure_crypto_provider();
    let cert = read_pem(&cfg.cert_path, "cert")?;
    let key = read_pem(&cfg.key_path, "key")?;
    let mut s = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));
    if let Some(ca) = &cfg.ca_path {
        s = s.client_ca_root(Certificate::from_pem(read_pem(ca, "ca")?));
    }
    Ok(s)
}

/// Client-side TLS: trust the configured CA (if any) and present this node's identity (mTLS).
pub fn client_tls(cfg: &RaftTlsConfig) -> crate::Result<ClientTlsConfig> {
    ensure_crypto_provider();
    let mut c = ClientTlsConfig::new().domain_name(cfg.domain.clone());
    if let Some(ca) = &cfg.ca_path {
        c = c.ca_certificate(Certificate::from_pem(read_pem(ca, "ca")?));
    }
    let cert = read_pem(&cfg.cert_path, "cert")?;
    let key = read_pem(&cfg.key_path, "key")?;
    c = c.identity(Identity::from_pem(cert, key));
    Ok(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Generate a self-signed cert/key for `domain`, write them to `dir`, return (cert, key, ca) paths.
    fn write_certs(dir: &std::path::Path, domain: &str) -> RaftTlsConfig {
        let cert = rcgen::generate_simple_self_signed(vec![domain.to_string()]).unwrap();
        let cert_pem = cert.cert.pem();
        let key_pem = cert.key_pair.serialize_pem();
        let cert_path = dir.join("node.pem");
        let key_path = dir.join("node.key");
        let ca_path = dir.join("ca.pem");
        fs::write(&cert_path, &cert_pem).unwrap();
        fs::write(&key_path, &key_pem).unwrap();
        // Self-signed: the cert is its own CA for this test.
        fs::write(&ca_path, &cert_pem).unwrap();
        RaftTlsConfig {
            cert_path: cert_path.to_string_lossy().into(),
            key_path: key_path.to_string_lossy().into(),
            ca_path: Some(ca_path.to_string_lossy().into()),
            domain: domain.into(),
        }
    }

    #[test]
    fn builds_server_and_client_tls_from_pem() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_certs(dir.path(), "ecphoria");
        // Both directions construct successfully from real PEMs (cert loading + mTLS identity/CA).
        server_tls(&cfg).expect("server TLS config");
        client_tls(&cfg).expect("client TLS config");
    }

    #[test]
    fn missing_cert_is_an_error() {
        let cfg = RaftTlsConfig {
            cert_path: "/nonexistent/cert.pem".into(),
            key_path: "/nonexistent/key.pem".into(),
            ca_path: None,
            domain: "ecphoria".into(),
        };
        assert!(server_tls(&cfg).is_err());
        assert!(client_tls(&cfg).is_err());
    }
}
