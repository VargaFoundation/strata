# ecphoria-server

## Responsibility

Main binary entry point. Thin wiring layer: loads config, constructs
`EcphoriaEngine`, starts the `ClusterCoordinator` (if cluster mode enabled),
starts the `GatewayServer`, and waits for shutdown signal.

## Internal Architecture

```
src/
  main.rs       Init tracing → Prometheus recorder → load config → build Engine
                → start Coordinator (if cluster.enabled) → start Gateway → wait shutdown
  config.rs     ServerConfig: layers defaults → ecphoria.toml → env ECPHORIA_*
  signals.rs    Graceful shutdown (Ctrl+C + SIGTERM)
  banner.rs     ASCII art + version on startup
```

## Startup Sequence

1. Initialize `tracing-subscriber` with env filter
2. Install Prometheus metrics recorder (`PrometheusBuilder`)
3. Print ASCII banner
4. Load `ServerConfig` (TOML + env vars)
5. Create `EcphoriaEngine` (core)
6. Create `ClusterCoordinator`; if `cluster.enabled`, start Raft instance
7. Start `GatewayServer` (HTTP, PG wire, gRPC, Raft RPC endpoints)
8. Wait for shutdown signal (Ctrl+C or SIGTERM)
9. Shutdown: coordinator → gateway → engine

Minimal logic here — all business logic lives in ecphoria-core, ecphoria-cluster, and ecphoria-gateway.
