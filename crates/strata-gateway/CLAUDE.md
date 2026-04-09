# strata-gateway

## Responsibility

Protocol layer. Translates external protocols (PostgreSQL wire, REST, gRPC, MCP,
LLM proxy) into calls on `strata_core::StrataEngine`. Also handles authentication.

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| REST API | **Working** | axum router with health/query/ingest/search/state endpoints |
| HTTP Server | **Working** | Binds port, graceful shutdown via oneshot channel |
| PG Wire | **Working** | pgwire SimpleQuery + ExtendedQuery, routes SQL to engine DuckDB |
| MCP Server | **Working** | JSON-RPC at /mcp: initialize, tools/list, tools/call, resources/list |
| MCP tools | **Working** | query, ingest, get_state, set_state callable via tools/call |
| gRPC | Stub | tonic service skeleton |
| LLM proxy | Stub | Router/providers/cache skeletons |
| Auth | Stub | API key/JWT types defined, middleware not wired |

## Public API

- `GatewayServer::start(engine, config)` — binds HTTP + PG wire ports, starts serving
- `GatewayServer::shutdown()` — graceful shutdown
- `rest::router()` — stateless router for testing
- `rest::router_with_engine(engine)` — full router with engine state + MCP endpoint

## REST Routes

| Method | Path | Handler | Status |
|--------|------|---------|--------|
| GET | `/health` | health check | **Working** |
| POST | `/api/v1/query` | SQL query via DuckDB | **Working** |
| POST | `/api/v1/ingest` | event ingestion | **Working** |
| POST | `/api/v1/search` | semantic vector search | **Working** |
| GET | `/api/v1/state/{agent_id}/{key}` | get state | **Working** |
| PUT | `/api/v1/state/{agent_id}/{key}` | set state | **Working** |
| POST | `/mcp` | MCP JSON-RPC endpoint | **Working** |

## PG Wire Protocol

Clients connect with `psql -h localhost -p 5432` (no auth required).
SQL is routed to the engine's DuckDB. Results returned as VARCHAR columns.

## MCP Protocol

POST JSON-RPC to `/mcp`. Supported methods:
- `initialize` — server capabilities
- `tools/list` — list available tools with schemas
- `tools/call` — execute a tool (query, ingest, get_state, set_state)
- `resources/list` — list memory resources
- `prompts/list` — list prompt templates
- `ping` — health check

## Testing

- `cargo test -p strata-gateway` (32 tests)
- Integration tests in `tests/integration/` test full router
- Gateway lifecycle tests verify start/shutdown with port 0
