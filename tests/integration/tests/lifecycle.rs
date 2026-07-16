//! Integration tests: engine and gateway lifecycle.

use std::sync::Arc;

use strata_core::{CoreConfig, StrataEngine};
use strata_gateway::server::{GatewayConfig, GatewayServer};

#[tokio::test]
async fn engine_starts_and_stops() {
    let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn full_stack_lifecycle() {
    let engine = Arc::new(StrataEngine::new(CoreConfig::default()).await.unwrap());
    let config = GatewayConfig {
        listen: "127.0.0.1:0".into(),
        pg_listen: "127.0.0.1:0".into(),
        grpc_listen: "127.0.0.1:0".into(),
        ..Default::default()
    };
    let gateway = GatewayServer::start(engine.clone(), config, None, None)
        .await
        .unwrap();

    // Shutdown gateway (signals background task to stop)
    gateway.shutdown().await.unwrap();

    // Give the background task time to release Arc
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(engine);
}

#[tokio::test]
async fn gateway_with_all_features_disabled() {
    let engine = Arc::new(StrataEngine::new(CoreConfig::default()).await.unwrap());
    let config = GatewayConfig {
        listen: "127.0.0.1:0".into(),
        pg_listen: "127.0.0.1:0".into(),
        grpc_listen: "127.0.0.1:0".into(),
        mcp_enabled: false,
        llm_proxy_enabled: false,
        auth_enabled: false,
        ..Default::default()
    };

    let gateway = GatewayServer::start(engine.clone(), config, None, None)
        .await
        .unwrap();
    gateway.shutdown().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(engine);
}
