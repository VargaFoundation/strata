# strata-gateway

## Responsibility

Protocol layer. Translates external protocols (PostgreSQL wire, REST, gRPC, MCP,
LLM proxy) into calls on `strata_core::StrataEngine`. Also handles authentication.

## Public API

- `GatewayServer::start(engine, config)` — starts all protocol listeners
- `GatewayServer::shutdown()` — graceful shutdown

## REST Routes

- `GET  /health` — health check
- `POST /api/v1/query` — SQL query execution
- `POST /api/v1/ingest` — event ingestion
- `POST /api/v1/search` — semantic search
- `GET  /mcp` — MCP SSE endpoint
- `POST /mcp` — MCP message endpoint
- `POST /v1/chat/completions` — LLM proxy (OpenAI-compatible)

## Internal Architecture

```
src/
  lib.rs           GatewayServer, re-exports
  error.rs         GatewayError (wraps CoreError + protocol errors)
  server.rs        GatewayServer + GatewayConfig
  pg_wire/         PostgreSQL wire protocol (pgwire crate)
  rest/            REST API (axum): routes, handlers, models
  grpc/            gRPC server (tonic) — stub pending proto
  mcp/             MCP server: transport (SSE), resources, tools, prompts
  llm_proxy/       OpenAI-compatible proxy: router, providers, cache
  auth/            Authentication: api_key, jwt, middleware
```

## Testing

- `cargo test -p strata-gateway`
- Handler tests: construct engine with default config, test handler functions
- MCP tests: HTTP client to verify SSE stream and tool calls
