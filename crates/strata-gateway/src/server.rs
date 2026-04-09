//! Gateway server — starts all protocol listeners.

use std::sync::Arc;

use strata_core::StrataEngine;
use tokio::net::TcpListener;

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
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl GatewayServer {
    /// Start all protocol listeners.
    pub async fn start(engine: Arc<StrataEngine>, config: GatewayConfig) -> Result<Self> {
        let listen_addr = config.listen.clone();

        // Build REST router with engine state
        let app = crate::rest::router_with_engine(engine.clone());

        // Bind TCP listener
        let listener = TcpListener::bind(&listen_addr)
            .await
            .map_err(|e| crate::Error::Bind(format!("failed to bind {listen_addr}: {e}")))?;

        let local_addr = listener
            .local_addr()
            .map_err(|e| crate::Error::Bind(e.to_string()))?;

        tracing::info!(%local_addr, "HTTP server listening");

        // Spawn HTTP server with graceful shutdown
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        // Start PG wire protocol server
        let pg_addr = config.pg_listen.clone();
        if let Err(e) = crate::pg_wire::handler::start_pg_wire(&pg_addr, engine.clone()).await {
            tracing::warn!(%pg_addr, error = %e, "failed to start PG wire server (non-fatal)");
        }

        Ok(Self {
            _engine: engine,
            _config: config,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Gracefully shut down all listeners.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        tracing::info!("Gateway shut down");
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
        // Use port 0 so OS picks a free port
        let config = GatewayConfig {
            listen: "127.0.0.1:0".into(),
            ..Default::default()
        };
        let gateway = GatewayServer::start(engine, config).await.unwrap();
        gateway.shutdown().await.unwrap();
    }

    #[test]
    fn default_gateway_config() {
        let config = GatewayConfig::default();
        assert_eq!(config.listen, "0.0.0.0:8432");
        assert_eq!(config.pg_listen, "0.0.0.0:5432");
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
        assert!(!config.mcp_enabled);
        assert!(config.auth_enabled);
    }

    #[test]
    fn gateway_config_partial_deserialize() {
        let config: GatewayConfig = toml::from_str("").unwrap();
        assert_eq!(config.listen, "0.0.0.0:8432");
        assert!(config.mcp_enabled);
    }
}
