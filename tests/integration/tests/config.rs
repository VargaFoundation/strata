//! Integration tests: configuration and cross-crate types.

use ecphoria_core::CoreConfig;

#[test]
fn core_config_from_toml() {
    let toml_str = r#"
        [storage]
        data_dir = "/var/ecphoria"
        engine = "s3"

        [storage.s3]
        endpoint = "http://minio:9000"
        bucket = "ecphoria-prod"
        region = "eu-west-1"

        [embedding]
        provider = "openai"
        model = "text-embedding-3-small"
        dimension = 1536
        batch_size = 128

        [query]
        max_rows = 100
        timeout_ms = 5000
    "#;
    let config: CoreConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.storage.data_dir, "/var/ecphoria");
    assert_eq!(config.storage.engine, "s3");
    assert_eq!(config.storage.s3.endpoint, "http://minio:9000");
    assert_eq!(config.storage.s3.bucket, "ecphoria-prod");
    assert_eq!(config.storage.s3.region, "eu-west-1");
    assert_eq!(config.embedding.provider, "openai");
    assert_eq!(config.embedding.model, "text-embedding-3-small");
    assert_eq!(config.embedding.dimension, 1536);
    assert_eq!(config.embedding.batch_size, 128);
    assert_eq!(config.query.max_rows, 100);
    assert_eq!(config.query.timeout_ms, 5000);
    // Defaults for unspecified fields
    assert_eq!(config.memory.episodic.default_retention_days, 365);
    assert_eq!(config.memory.semantic.metric, "cosine");
}

#[test]
fn gateway_config_from_toml() {
    let toml_str = r#"
        listen = "0.0.0.0:9000"
        pg_listen = "0.0.0.0:5433"
        grpc_listen = "0.0.0.0:50051"
        mcp_enabled = false
        llm_proxy_enabled = true
        auth_enabled = true
    "#;
    let config: ecphoria_gateway::server::GatewayConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.listen, "0.0.0.0:9000");
    assert_eq!(config.pg_listen, "0.0.0.0:5433");
    assert!(!config.mcp_enabled);
    assert!(config.llm_proxy_enabled);
    assert!(config.auth_enabled);
}

#[test]
fn cluster_config_from_toml() {
    let toml_str = r#"
        enabled = true
        node_id = 2
        listen = "10.0.0.2:9433"
        peers = ["10.0.0.1:9433", "10.0.0.3:9433"]
    "#;
    let config: ecphoria_cluster::ClusterConfig = toml::from_str(toml_str).unwrap();
    assert!(config.enabled);
    assert_eq!(config.node_id, 2);
    assert_eq!(config.peers.len(), 2);
}

#[test]
fn cluster_coordinator_single_node() {
    let config = ecphoria_cluster::ClusterConfig::default();
    assert!(!config.enabled);
    let coord = ecphoria_cluster::ClusterCoordinator::new(config);
    assert!(coord.is_leader());
    assert_eq!(coord.leader_id(), Some(1));
}

#[test]
fn mcp_tools_unique_names() {
    let tools = ecphoria_gateway::mcp::tools::list_tools();
    let mut names: Vec<String> = tools.iter().map(|t| t.name.clone()).collect();
    let original_len = names.len();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), original_len, "duplicate MCP tool names");
}

#[test]
fn mcp_resources_unique_uris() {
    let resources = ecphoria_gateway::mcp::resources::list_resources();
    let mut uris: Vec<String> = resources.iter().map(|r| r.uri.clone()).collect();
    let original_len = uris.len();
    uris.sort();
    uris.dedup();
    assert_eq!(uris.len(), original_len, "duplicate MCP resource URIs");
}
