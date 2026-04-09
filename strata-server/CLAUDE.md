# strata-server

## Responsibility

Main binary entry point. Thin wiring layer: loads config, constructs
`StrataEngine` and `GatewayServer`, waits for shutdown signal.

## Internal Architecture

```
src/
  main.rs       Init tracing → load config → build Engine → start Gateway → wait shutdown
  config.rs     ServerConfig: layers defaults → strata.toml → env STRATA_*
  signals.rs    Graceful shutdown (Ctrl+C + SIGTERM)
  banner.rs     ASCII art + version on startup
```

Minimal logic here — all business logic lives in strata-core and strata-gateway.
