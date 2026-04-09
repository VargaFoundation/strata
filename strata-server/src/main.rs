use std::sync::Arc;

mod banner;
mod config;
mod signals;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,strata=debug".parse().unwrap()),
        )
        .init();

    banner::print();

    let server_config = config::load()?;

    let engine = Arc::new(strata_core::StrataEngine::new(server_config.core).await?);

    let gateway =
        strata_gateway::GatewayServer::start(engine.clone(), server_config.gateway).await?;

    signals::wait_for_shutdown().await;

    gateway.shutdown().await?;
    Arc::try_unwrap(engine)
        .map_err(|_| anyhow::anyhow!("engine still has active references"))?
        .shutdown()
        .await?;

    tracing::info!("Strata shutdown complete");
    Ok(())
}
