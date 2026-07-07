//! Gateway server — starts all protocol listeners.

use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusHandle;
use strata_cluster::ClusterCoordinator;
use strata_core::StrataEngine;
use tokio::net::TcpListener;

use crate::Result;

/// Configuration for the gateway.
#[derive(Clone, serde::Deserialize)]
#[serde(default)]
pub struct GatewayConfig {
    pub listen: String,
    pub pg_listen: String,
    pub grpc_listen: String,
    pub mcp_enabled: bool,
    pub llm_proxy_enabled: bool,
    pub auth_enabled: bool,
    pub max_pg_connections: usize,
    /// API keys that are allowed to access the server (when auth_enabled = true).
    pub api_keys: Vec<String>,
    /// HMAC-SHA256 secret for JWT token validation.
    #[serde(default)]
    pub jwt_secret: Option<String>,
    /// Allowed CORS origins. Empty = permissive (dev only).
    #[serde(default)]
    pub cors_origins: Vec<String>,
    /// Maximum requests per second per API key (token bucket). 0 = unlimited.
    #[serde(default)]
    pub rate_limit_per_key: u32,
    /// OIDC configuration for federated SSO authentication.
    #[serde(default)]
    pub oidc: crate::auth::oidc::OidcConfig,
    /// Durable audit-log path (file-backed DuckDB). Empty/`:memory:` = in-memory (non-durable).
    #[serde(default)]
    pub audit_db_path: String,
    /// HMAC-SHA256 secret for verifying incoming webhook signatures (GitHub-style
    /// `X-Hub-Signature-256`). When set, unsigned/mis-signed webhooks are rejected.
    #[serde(default)]
    pub webhook_secret: Option<String>,
}

impl std::fmt::Debug for GatewayConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayConfig")
            .field("listen", &self.listen)
            .field("pg_listen", &self.pg_listen)
            .field("grpc_listen", &self.grpc_listen)
            .field("mcp_enabled", &self.mcp_enabled)
            .field("llm_proxy_enabled", &self.llm_proxy_enabled)
            .field("auth_enabled", &self.auth_enabled)
            .field("max_pg_connections", &self.max_pg_connections)
            .field("api_keys", &format!("[{} keys]", self.api_keys.len()))
            .field("jwt_secret", &self.jwt_secret.as_ref().map(|_| "***"))
            .field("cors_origins", &self.cors_origins)
            .field("rate_limit_per_key", &self.rate_limit_per_key)
            .field("oidc_enabled", &self.oidc.enabled)
            .field("audit_db_path", &self.audit_db_path)
            .finish()
    }
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
            max_pg_connections: 256,
            api_keys: vec![],
            jwt_secret: None,
            cors_origins: vec![],
            rate_limit_per_key: 0,
            oidc: crate::auth::oidc::OidcConfig::default(),
            audit_db_path: "./data/audit.duckdb".into(),
            webhook_secret: None,
        }
    }
}

/// The gateway server — owns all protocol listeners.
pub struct GatewayServer {
    _engine: Arc<StrataEngine>,
    _config: GatewayConfig,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pg_handle: Option<crate::pg_wire::handler::PgWireHandle>,
    grpc_handle: Option<crate::grpc::service::GrpcHandle>,
}

impl GatewayServer {
    /// Start all protocol listeners.
    ///
    /// If a `PrometheusHandle` is provided, a `/metrics` endpoint is exposed.
    pub async fn start(
        engine: Arc<StrataEngine>,
        config: GatewayConfig,
        prometheus: Option<PrometheusHandle>,
        coordinator: Option<Arc<tokio::sync::RwLock<ClusterCoordinator>>>,
    ) -> Result<Self> {
        let listen_addr = config.listen.clone();

        // Build REST router with engine state and optional auth
        let auth_state = if config.auth_enabled {
            let state = if config.oidc.enabled {
                crate::auth::middleware::AuthState::with_oidc(
                    config.api_keys.clone(),
                    config.jwt_secret.clone(),
                    config.oidc.clone(),
                    config.rate_limit_per_key,
                )
            } else {
                crate::auth::middleware::AuthState::new(
                    config.api_keys.clone(),
                    config.jwt_secret.clone(),
                    config.rate_limit_per_key,
                )
            };
            // Make the audit log durable (file-backed) for compliance.
            let state = state.with_audit_path(&config.audit_db_path);
            // Reject a weak JWT secret (HS256 needs ≥32 bytes of entropy).
            if let Some(secret) = &config.jwt_secret {
                if secret.len() < 32 {
                    return Err(crate::Error::Auth(format!(
                        "jwt_secret is too short ({} bytes); HS256 requires at least 32 bytes",
                        secret.len()
                    )));
                }
            }
            // Fail CLOSED: refuse to start unauthenticated when auth was explicitly requested,
            // rather than silently disabling it (a dangerous misconfiguration otherwise).
            if state.is_empty() {
                return Err(crate::Error::Auth(
                    "auth_enabled=true but no api_keys, jwt_secret, or OIDC configured — refusing \
                     to start (fail-closed). Configure a credential or set auth_enabled=false."
                        .into(),
                ));
            }
            tracing::info!(
                api_keys = config.api_keys.len(),
                jwt = config.jwt_secret.is_some(),
                oidc = config.oidc.enabled,
                rate_limit = config.rate_limit_per_key,
                "Authentication enabled"
            );
            Some(state)
        } else {
            None
        };

        // Build cluster state for leader-forwarding middleware
        let cluster_state =
            coordinator
                .as_ref()
                .map(|coord| crate::cluster::leader_forward::ClusterState {
                    coordinator: coord.clone(),
                });

        // Build shard-routing state from the cluster config (the router gates on shards > 1).
        let shard_state = match coordinator.as_ref() {
            Some(coord) => {
                let c = coord.read().await;
                Some(crate::cluster::shard_route::ShardRoutingState {
                    router: std::sync::Arc::new(c.shard_router()),
                    my_shard: c.shard_index(),
                    base_urls: std::sync::Arc::new(c.shard_base_urls()),
                    http: reqwest::Client::new(),
                })
            }
            None => None,
        };

        // gRPC + PG wire share the same auth state (tenant scoping + token validation) and shard routing.
        let grpc_auth = auth_state.clone();
        let grpc_shard = shard_state.clone();
        let pg_auth = auth_state.clone();
        let pg_shard = shard_state.clone();
        // Auth for the cluster admin routes (they are mounted after the global layers below, so
        // they need auth applied explicitly rather than inheriting it).
        let raft_auth = auth_state.clone();

        let mut app = crate::rest::router_with_engine_and_auth(
            engine.clone(),
            auth_state,
            cluster_state,
            shard_state,
            &config,
        );

        if let Some(handle) = prometheus {
            app = app.route(
                "/metrics",
                axum::routing::get(move || {
                    let h = handle.clone();
                    async move { h.render() }
                }),
            );
        }

        // Mount cluster admin endpoints if cluster mode is active. The hot-path Raft RPCs
        // (AppendEntries/Vote/InstallSnapshot) are served over gRPC by the coordinator on the
        // Raft port — only the low-traffic admin routes live on the HTTP app.
        if let Some(ref coord) = coordinator {
            let coord_read = coord.read().await;
            if let Some(raft_instance) = coord_read.raft() {
                let mut raft_router =
                    crate::cluster::raft_routes::raft_router(Arc::new(raft_instance.clone()));
                // These routes change Raft cluster membership — they must NEVER be reachable
                // unauthenticated. Because the router is merged after the global layers, apply auth
                // here explicitly; `require_auth` gates `/cluster/*` on the Admin role (see
                // auth::middleware). When auth is disabled they stay open (dev posture, same as the
                // rest of the API) — bind :8432 to a trusted network in that case.
                if let Some(state) = raft_auth {
                    raft_router = raft_router.route_layer(axum::middleware::from_fn_with_state(
                        state,
                        crate::auth::middleware::require_auth,
                    ));
                }
                app = app.merge(raft_router);
                tracing::info!("Cluster admin endpoints mounted (/cluster/status, /cluster/*)");
            }
        }

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
        let max_pg = config.max_pg_connections;
        let pg_handle = match crate::pg_wire::handler::start_pg_wire(
            &pg_addr,
            engine.clone(),
            max_pg,
            pg_auth,
            pg_shard,
        )
        .await
        {
            Ok(handle) => Some(handle),
            Err(e) => {
                tracing::warn!(%pg_addr, error = %e, "failed to start PG wire server (non-fatal)");
                None
            }
        };

        // Start gRPC server
        let grpc_addr = config.grpc_listen.clone();
        let grpc_handle = match crate::grpc::service::start_grpc(
            &grpc_addr,
            engine.clone(),
            grpc_auth,
            grpc_shard,
        )
        .await
        {
            Ok(handle) => Some(handle),
            Err(e) => {
                tracing::warn!(%grpc_addr, error = %e, "failed to start gRPC server (non-fatal)");
                None
            }
        };

        Ok(Self {
            _engine: engine,
            _config: config,
            shutdown_tx: Some(shutdown_tx),
            pg_handle,
            grpc_handle,
        })
    }

    /// Gracefully shut down all listeners.
    ///
    /// Signals HTTP, PG wire, and gRPC servers to stop accepting new connections,
    /// then waits up to 10 seconds for in-flight connections to drain.
    pub async fn shutdown(mut self) -> Result<()> {
        let drain_timeout = std::time::Duration::from_secs(10);

        // Signal HTTP server
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Signal gRPC server (uses tonic's graceful shutdown)
        if let Some(handle) = self.grpc_handle.take() {
            handle.shutdown();
        }

        // Drain PG wire connections with timeout
        if let Some(handle) = self.pg_handle.take() {
            handle.shutdown(drain_timeout).await;
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
        let gateway = GatewayServer::start(
            engine,
            config,
            None,
            None::<Arc<tokio::sync::RwLock<ClusterCoordinator>>>,
        )
        .await
        .unwrap();
        gateway.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn auth_enabled_without_credentials_fails_closed() {
        let engine = Arc::new(
            StrataEngine::new(strata_core::CoreConfig::default())
                .await
                .unwrap(),
        );
        let config = GatewayConfig {
            listen: "127.0.0.1:0".into(),
            auth_enabled: true, // but no api_keys / jwt_secret / OIDC
            ..Default::default()
        };
        let result = GatewayServer::start(
            engine,
            config,
            None,
            None::<Arc<tokio::sync::RwLock<ClusterCoordinator>>>,
        )
        .await;
        assert!(result.is_err(), "must refuse to start (fail-closed)");
    }

    #[tokio::test]
    async fn weak_jwt_secret_is_rejected() {
        let engine = Arc::new(
            StrataEngine::new(strata_core::CoreConfig::default())
                .await
                .unwrap(),
        );
        let config = GatewayConfig {
            listen: "127.0.0.1:0".into(),
            auth_enabled: true,
            jwt_secret: Some("short".into()), // < 32 bytes
            ..Default::default()
        };
        let result = GatewayServer::start(
            engine,
            config,
            None,
            None::<Arc<tokio::sync::RwLock<ClusterCoordinator>>>,
        )
        .await;
        assert!(result.is_err(), "weak jwt_secret must be rejected");
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
