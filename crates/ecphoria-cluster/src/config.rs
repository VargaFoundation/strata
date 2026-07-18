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
    /// SNI/domain name the cert is issued for (client-side verification). Default "ecphoria".
    #[serde(default = "default_tls_domain")]
    pub domain: String,
}

fn default_tls_domain() -> String {
    "ecphoria".into()
}

/// Deserialize `peers` from either a sequence (TOML array) or a comma-separated string (env var).
fn de_string_or_seq<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{SeqAccess, Visitor};
    use std::fmt;

    struct StringOrSeq;
    impl<'de> Visitor<'de> for StringOrSeq {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a comma-separated string or a sequence of strings")
        }
        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E> {
            Ok(s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect())
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(x) = seq.next_element::<String>()? {
                out.push(x);
            }
            Ok(out)
        }
    }
    deserializer.deserialize_any(StringOrSeq)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: u64,
    pub listen: String,
    /// Full voter membership as `id@addr`. Accepts a TOML array **or** a comma-separated string, so
    /// it works via `ECPHORIA_CLUSTER__PEERS="1@http://a:9433,2@http://b:9433"` — the `config` crate
    /// cannot deserialize a plain env string into a `Vec` on its own (it errors, and with
    /// `unwrap_or_default` that silently wipes the whole config). This custom deserializer fixes it.
    #[serde(deserialize_with = "de_string_or_seq")]
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
    fn peers_from_comma_separated_string() {
        // The env-var shape (`ECPHORIA_CLUSTER__PEERS="1@..,2@.."`): a single comma-separated string.
        // Previously this failed to deserialize into Vec and wiped the whole config.
        let config: ClusterConfig =
            toml::from_str(r#"peers = "1@http://a:9433, 2@http://b:9433, 3@http://c:9433""#)
                .unwrap();
        assert_eq!(config.peers.len(), 3);
        assert_eq!(config.peers[0], "1@http://a:9433");
        assert_eq!(config.peers[2], "3@http://c:9433");
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
