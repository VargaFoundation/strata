use serde::Deserialize;

/// TLS for the inter-node Raft gRPC transport. When set, the server presents `cert`/`key`; when
/// `ca` is also set, peers are verified against it (mutual TLS) and clients trust it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RaftTlsConfig {
    /// PEM path to this node's certificate.
    pub cert_path: String,
    /// PEM path to this node's private key.
    pub key_path: String,
    /// PEM path to the CA that signs peer certs (enables mTLS + client trust). Optional.
    pub ca_path: Option<String>,
    /// SNI/domain name the cert is issued for (client-side verification). Default "strata".
    #[serde(default = "default_tls_domain")]
    pub domain: String,
}

fn default_tls_domain() -> String {
    "strata".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: u64,
    pub listen: String,
    pub peers: Vec<String>,
    /// Directory for persistent Raft log storage. Use ":memory:" for in-memory (testing).
    pub data_dir: String,
    /// Shared secret authenticating inter-node Raft RPCs. When set, every node must present it
    /// (Bearer token on the gRPC transport) or its RPCs are rejected — prevents an unauthorized
    /// node from injecting AppendEntries/Vote and corrupting the cluster. `None` = no auth.
    #[serde(default)]
    pub secret: Option<String>,
    /// TLS for the Raft transport (encryption in transit + optional mTLS). `None` = cleartext
    /// HTTP/2 (rely on a service mesh / private network for confidentiality).
    #[serde(default)]
    pub tls: Option<RaftTlsConfig>,
    /// Number of write shards (independent Raft groups). 1 = single group (today's default). >1 is
    /// the consistent-hash routing foundation for horizontal write scaling.
    #[serde(default = "default_shards")]
    pub shards: usize,
    /// This pod's 0-based shard index (only meaningful when `shards > 1`).
    #[serde(default)]
    pub shard_index: usize,
    /// Comma-separated base URLs of every shard's HTTP gateway, indexed by shard. Used by the
    /// gateway to reverse-proxy a request to its tenant's owning shard. Stored as a String (split in
    /// Rust) — the `config` crate's env→Vec parsing is fragile and can silently drop the whole config.
    #[serde(default)]
    pub shard_base_urls: String,
}

fn default_shards() -> usize {
    1
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 1,
            listen: "0.0.0.0:9433".into(),
            peers: vec![],
            data_dir: "./data/raft".into(),
            secret: None,
            tls: None,
            shards: 1,
            shard_index: 0,
            shard_base_urls: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_cluster_config() {
        let config = ClusterConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.node_id, 1);
        assert_eq!(config.listen, "0.0.0.0:9433");
        assert!(config.peers.is_empty());
    }

    #[test]
    fn deserialize_from_toml() {
        let toml_str = r#"
            enabled = true
            node_id = 3
            listen = "10.0.0.1:9433"
            peers = ["10.0.0.2:9433", "10.0.0.3:9433"]
        "#;
        let config: ClusterConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(config.node_id, 3);
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.peers[0], "10.0.0.2:9433");
    }

    #[test]
    fn deserialize_empty_uses_defaults() {
        let config: ClusterConfig = toml::from_str("").unwrap();
        assert!(!config.enabled);
        assert_eq!(config.node_id, 1);
    }

    #[test]
    fn config_is_clone() {
        let config = ClusterConfig {
            enabled: true,
            node_id: 5,
            listen: "localhost:9433".into(),
            peers: vec!["peer1:9433".into()],
            data_dir: "/tmp/raft".into(),
            secret: None,
            tls: None,
            shards: 1,
            shard_index: 0,
            shard_base_urls: String::new(),
        };
        let cloned = config.clone();
        assert_eq!(cloned.node_id, 5);
        assert_eq!(cloned.peers.len(), 1);
    }
}
