//! Gateway server — starts all protocol listeners.

use std::sync::Arc;

use strata_core::StrataEngine;

use crate::Result;

/// Configuration for the gateway.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct GatewayConfig {
    pub listen: String,
    pub pg_listen: String,
    pub grpc_listen: String,
    pub mcp_enabled: bool,
    pub llm_proxy_enabled: bool,
    pub auth_enabled: bool,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8432".into(),
            pg_listen: "0.0.0.0:5432".into(),
            grpc_listen: "0.0.0.0:9432".into(),
            mcp_enabled: true,
            llm_proxy_enabled: false,
            auth_enabled: false,
        }
    }
}

/// The gateway server — owns all protocol listeners.
pub struct GatewayServer {
    _engine: Arc<StrataEngine>,
    _config: GatewayConfig,
}

impl GatewayServer {
    /// Start all protocol listeners.
    pub async fn start(engine: Arc<StrataEngine>, config: GatewayConfig) -> Result<Self> {
        tracing::info!(listen = %config.listen, "Gateway starting");

        // TODO: spawn REST, PG wire, gRPC, MCP, LLM proxy listeners

        Ok(Self {
            _engine: engine,
            _config: config,
        })
    }

    /// Gracefully shut down all listeners.
    pub async fn shutdown(self) -> Result<()> {
        tracing::info!("Gateway shutting down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn gateway_lifecycle() {
        let engine = Arc::new(
            StrataEngine::new(strata_core::CoreConfig::default())
                .await
                .unwrap(),
        );
        let gateway = GatewayServer::start(engine, GatewayConfig::default())
            .await
            .unwrap();
        gateway.shutdown().await.unwrap();
    }

    #[test]
    fn default_gateway_config() {
        let config = GatewayConfig::default();
        assert_eq!(config.listen, "0.0.0.0:8432");
        assert_eq!(config.pg_listen, "0.0.0.0:5432");
        assert_eq!(config.grpc_listen, "0.0.0.0:9432");
        assert!(config.mcp_enabled);
        assert!(!config.llm_proxy_enabled);
        assert!(!config.auth_enabled);
    }

    #[test]
    fn gateway_config_deserialize_from_toml() {
        let toml_str = r#"
            listen = "127.0.0.1:9000"
            pg_listen = "127.0.0.1:5433"
            mcp_enabled = false
            auth_enabled = true
        "#;
        let config: GatewayConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.listen, "127.0.0.1:9000");
        assert_eq!(config.pg_listen, "127.0.0.1:5433");
        assert!(!config.mcp_enabled);
        assert!(config.auth_enabled);
    }

    #[test]
    fn gateway_config_partial_deserialize() {
        let config: GatewayConfig = toml::from_str("").unwrap();
        assert_eq!(config.listen, "0.0.0.0:8432");
        assert!(config.mcp_enabled);
    }

    #[tokio::test]
    async fn gateway_with_custom_config() {
        let engine = Arc::new(
            StrataEngine::new(strata_core::CoreConfig::default())
                .await
                .unwrap(),
        );
        let config = GatewayConfig {
            listen: "127.0.0.1:0".into(),
            pg_listen: "127.0.0.1:0".into(),
            grpc_listen: "127.0.0.1:0".into(),
            mcp_enabled: false,
            llm_proxy_enabled: false,
            auth_enabled: false,
        };
        let gateway = GatewayServer::start(engine, config).await.unwrap();
        gateway.shutdown().await.unwrap();
    }
}
