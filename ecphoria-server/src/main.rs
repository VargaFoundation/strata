use std::sync::Arc;

use tokio::sync::RwLock;

/// Use mimalloc as the global allocator. DuckDB allocates heavily and the musl allocator (Alpine
/// runtime image) is notably slow under multi-threaded contention; mimalloc removes that penalty.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod banner;
mod config;
mod signals;
mod telemetry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let telemetry = telemetry::init();

    // Install Prometheus metrics recorder
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    describe_metrics();

    banner::print();

    let mut server_config = config::load()?;

    // Support the `_FILE` secret convention for the Raft shared secret (like other secrets), so it
    // can be mounted from a Docker/K8s secret file instead of a plaintext env var. Only fills in
    // when not already set via TOML or the direct `ECPHORIA_CLUSTER__SECRET` env.
    if server_config.cluster.secret.is_none() {
        let s = ecphoria_core::config::resolve_secret("ECPHORIA_CLUSTER__SECRET");
        if !s.is_empty() {
            server_config.cluster.secret = Some(s);
        }
    }

    let engine = Arc::new(ecphoria_core::EcphoriaEngine::new(server_config.core).await?);

    // Start background tiering manager (retention + TTL cleanup)
    let (tiering_mgr, tiering_handle) = ecphoria_core::storage::tiering::TieringManager::new(3600);
    tokio::spawn(tiering_mgr.run(engine.clone()));

    // Background reindex: embed episodic events that were appended without a vector. The Raft apply
    // path appends deterministically and leaves embedding to this local, best-effort loop, so each
    // node builds its own vector index without a non-deterministic external call inside apply (this
    // also repairs events left unembedded by a transient provider outage during inline ingest).
    {
        let engine = engine.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tick.tick().await;
                match engine.reindex_unembedded(1000).await {
                    Ok(n) if n > 0 => tracing::debug!(reindexed = n, "background reindex"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "background reindex failed"),
                }
            }
        });
    }

    // Start Raft cluster if enabled
    let coordinator = Arc::new(RwLock::new(ecphoria_cluster::ClusterCoordinator::new(
        server_config.cluster.clone(),
    )));

    if server_config.cluster.enabled {
        let mut coord = coordinator.write().await;
        coord.start_raft(engine.clone()).await?;
        drop(coord);
        // Route the agent driver's run/step writes through Raft so runs started via /agents/run
        // (and their traces) replicate and survive leader failover.
        engine.set_run_replicator(Arc::new(ecphoria_cluster::CoordinatorRunReplicator::new(
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

    // Background consolidation: on the leader, periodically forget decayed memories (the
    // "sleep-time" forgetting job). Off unless `memory.cognition.decay_interval_secs > 0`. In
    // cluster mode the forget-set is computed on the leader and replicated via `MemoryExpire`, so
    // every node forgets the identical rows (no failover divergence); single-node applies locally.
    {
        let decay_interval = engine.config().memory.cognition.decay_interval_secs;
        if decay_interval > 0 {
            let engine = engine.clone();
            let coord = cluster_handle.clone();
            tokio::spawn(async move {
                let mut tick =
                    tokio::time::interval(std::time::Duration::from_secs(decay_interval));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    let is_leader = match &coord {
                        Some(c) => c.read().await.is_leader(),
                        None => true,
                    };
                    if !is_leader {
                        continue;
                    }
                    match &coord {
                        // Cluster: compute the forget-set on the leader, replicate via Raft.
                        Some(c) => match engine.memory_decay_plan().await {
                            Ok(ids) if !ids.is_empty() => {
                                let req =
                                    ecphoria_cluster::raft::types::AppRequest::MemoryExpire { ids };
                                if let Err(e) = c.read().await.client_write(req).await {
                                    tracing::warn!(error = %e, "decay replication failed");
                                }
                            }
                            Ok(_) => {}
                            Err(e) => tracing::warn!(error = %e, "decay plan failed"),
                        },
                        // Single node: apply locally.
                        None => {
                            if let Err(e) = engine.memory_enforce_decay().await {
                                tracing::warn!(error = %e, "memory decay tick failed");
                            }
                        }
                    }
                }
            });
        }
    }

    let gateway = ecphoria_gateway::GatewayServer::start(
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

    tracing::info!("Ecphoria shutdown complete");
    telemetry.shutdown();
    Ok(())
}

/// Register HELP/TYPE metadata (and units) for the metrics Ecphoria emits, so `/metrics` is
/// self-describing for Prometheus scraping and Grafana. Descriptions are attached to the recorder
/// once at startup; the values are recorded lazily at the emission sites across the crates.
fn describe_metrics() {
    use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

    // Counters.
    describe_counter!(
        "ecphoria_episodic_events_ingested_total",
        "Episodic events ingested."
    );
    describe_counter!(
        "ecphoria_episodic_queries_total",
        "SQL queries executed against episodic memory."
    );
    describe_counter!(
        "ecphoria_rest_requests_total",
        "REST requests, labelled by endpoint."
    );
    describe_counter!(
        "ecphoria_llm_cache_hits_total",
        "LLM-proxy semantic response-cache hits."
    );
    describe_counter!(
        "ecphoria_llm_cache_misses_total",
        "LLM-proxy semantic response-cache misses."
    );
    describe_counter!(
        "ecphoria_memory_embed_failures_total",
        "Embedding failures, labelled by op (ingest|query); each degrades search to BM25-only."
    );
    describe_counter!("ecphoria_runs_created_total", "Agent runs created.");
    describe_counter!(
        "ecphoria_runs_completed_total",
        "Agent runs that reached a terminal status, labelled by status."
    );
    describe_counter!(
        "ecphoria_run_steps_total",
        "Agent run steps, labelled by type."
    );
    describe_counter!(
        "ecphoria_retention_events_deleted_total",
        "Events deleted by retention enforcement."
    );
    describe_counter!("ecphoria_state_expired_total", "State keys expired by TTL.");
    describe_counter!(
        "ecphoria_raft_leader_changes_total",
        "Raft leadership changes observed by this node."
    );

    // Histograms (seconds).
    describe_histogram!(
        "ecphoria_episodic_append_duration_seconds",
        Unit::Seconds,
        "Episodic append latency."
    );
    describe_histogram!(
        "ecphoria_episodic_query_duration_seconds",
        Unit::Seconds,
        "Episodic SQL query latency."
    );
    describe_histogram!(
        "ecphoria_rest_request_duration_seconds",
        Unit::Seconds,
        "REST request latency, labelled by endpoint."
    );
    describe_histogram!(
        "ecphoria_raft_snapshot_build_duration_seconds",
        Unit::Seconds,
        "Raft snapshot build latency."
    );
    describe_histogram!(
        "ecphoria_raft_snapshot_install_duration_seconds",
        Unit::Seconds,
        "Raft snapshot install latency."
    );

    // Gauges (Raft state).
    describe_gauge!(
        "ecphoria_raft_is_leader",
        "1 if this node is the Raft leader, else 0."
    );
    describe_gauge!("ecphoria_raft_term", "Current Raft term.");
    describe_gauge!(
        "ecphoria_raft_last_log_index",
        "Index of the last Raft log entry."
    );
    describe_gauge!(
        "ecphoria_raft_last_applied_index",
        "Index of the last applied Raft log entry."
    );
    describe_gauge!(
        "ecphoria_raft_replication_lag",
        "Replication lag (last_log_index − last_applied_index)."
    );
}
