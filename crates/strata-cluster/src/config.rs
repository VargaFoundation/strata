use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: u64,
    pub listen: String,
    pub peers: Vec<String>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 1,
            listen: "0.0.0.0:9433".into(),
            peers: vec![],
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
        };
        let cloned = config.clone();
        assert_eq!(cloned.node_id, 5);
        assert_eq!(cloned.peers.len(), 1);
    }
}
