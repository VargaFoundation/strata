use std::sync::Arc;

use tokio::sync::RwLock;

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

    // Install Prometheus metrics recorder
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    banner::print();

    let mut server_config = config::load()?;

    // Support the `_FILE` secret convention for the Raft shared secret (like other secrets), so it
    // can be mounted from a Docker/K8s secret file instead of a plaintext env var. Only fills in
    // when not already set via TOML or the direct `STRATA_CLUSTER__SECRET` env.
    if server_config.cluster.secret.is_none() {
        let s = strata_core::config::resolve_secret("STRATA_CLUSTER__SECRET");
        if !s.is_empty() {
            server_config.cluster.secret = Some(s);
        }
    }

    let engine = Arc::new(strata_core::StrataEngine::new(server_config.core).await?);

    // Start background tiering manager (retention + TTL cleanup)
    let (tiering_mgr, tiering_handle) = strata_core::storage::tiering::TieringManager::new(3600);
    tokio::spawn(tiering_mgr.run(engine.clone()));

    // Start Raft cluster if enabled
    let coordinator = Arc::new(RwLock::new(strata_cluster::ClusterCoordinator::new(
        server_config.cluster.clone(),
    )));

    if server_config.cluster.enabled {
        let mut coord = coordinator.write().await;
        coord.start_raft(engine.clone()).await?;
        drop(coord);
        // Route the agent driver's run/step writes through Raft so runs started via /agents/run
        // (and their traces) replicate and survive leader failover.
        engine.set_run_replicator(Arc::new(strata_cluster::CoordinatorRunReplicator::new(
            coordinator.clone(),
        )));
    }

    let cluster_handle = if server_config.cluster.enabled {
        Some(coordinator.clone())
    } else {
        None
    };

    // Run dispatcher: on the leader (or single-node), periodically resume agent runs orphaned by a
    // crash / leader failover — the durable-execution recovery loop. No-op without a completion
    // provider or when this node isn't the leader.
    {
        let engine = engine.clone();
        let coord = cluster_handle.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(15));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let is_leader = match &coord {
                    Some(c) => c.read().await.is_leader(),
                    None => true,
                };
                if is_leader {
                    if let Err(e) = engine.run_dispatch_once(60, 5).await {
                        tracing::warn!(error = %e, "run dispatch tick failed");
                    }
                }
            }
        });
    }

    let gateway = strata_gateway::GatewayServer::start(
        engine.clone(),
        server_config.gateway.clone(),
        Some(prometheus_handle),
        cluster_handle,
    )
    .await?;

    banner::print_ready(&server_config.gateway, engine.config());

    signals::wait_for_shutdown().await;

    tiering_handle.shutdown();
    coordinator.write().await.shutdown().await?;
    gateway.shutdown().await?;
    Arc::try_unwrap(engine)
        .map_err(|_| anyhow::anyhow!("engine still has active references"))?
        .shutdown()
        .await?;

    tracing::info!("Strata shutdown complete");
    Ok(())
}
