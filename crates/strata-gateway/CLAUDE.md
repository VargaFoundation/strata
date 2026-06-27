# strata-gateway

## Responsibility

Protocol layer. Translates external protocols (PostgreSQL wire, REST, gRPC, MCP,
LLM proxy) into calls on `strata_core::StrataEngine`. Also handles authentication
(API keys, JWT HS256, OIDC RS256), leader forwarding (cluster mode), and Raft RPC routing.

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| REST API | **Working** | axum router with health/query/ingest/search/state/webhook/session/retention endpoints, 16MB body limit, 10K event batch limit, Prometheus metrics per endpoint, X-Request-Id correlation |
| HTTP Server | **Working** | Binds port, graceful shutdown, 30s timeout (TimeoutLayer), CORS (restrictive when auth enabled), tracing |
| PG Wire | **Working** | pgwire SimpleQuery + ExtendedQuery, full type mapping (20+ DuckDB types -> PG OIDs), connection limit (Semaphore, default 256) |
| MCP Server | **Working** | **Streamable HTTP** at /mcp: POST (JSON-RPC; initialize returns `Mcp-Session-Id`) + GET (server→client SSE keep-alive) → native Claude Code **and** Claude Desktop. initialize, tools/list (15 tools), tools/call, resources/list, prompts/list. Authenticated when `auth_enabled`. See docs/connect-claude.md |
| MCP tools | **Working** | query, ingest, get_state, set_state, search, embed, start/end/recall_session + **memory tools**: add_memory, search_memory, get_memories, memory_history, delete_memory, remember |
| gRPC | **Working** | tonic Query/Ingest/Search/State/Health RPCs, 16MB max message. **Typed payloads** (`google.protobuf.Struct`/`Value` for rows/events/metadata/state values — not JSON-in-string). **Authenticated** (Bearer JWT in metadata) + **tenant-scoped** when auth_enabled |
| LLM proxy | **Working** | /v1/chat/completions with auto-RAG (semantic + episodic + **tenant-scoped user memories**), semantic response cache, multi-provider (OpenAI/Ollama/Anthropic), **multi-turn tool-use** (assistant `tool_calls` + `role:"tool"` results → Anthropic `tool_use`/`tool_result`), **SSE streaming** incl. tool-call deltas |
| Auth | **Working** | API key, JWT HS256, OIDC RS256 (JWKS), RBAC, per-key rate limiting, **durable (file-backed) audit log**, and **tenant isolation enforced on all read paths** (SQL/memories/semantic/state/schema/sessions) across REST + MCP + proxy + gRPC |
| OIDC | **Working** | RS256 JWKS validation, configurable issuer/audience/role_claim, auto-refresh with TTL cache |
| Cluster routes | **Working** | /raft/append, /raft/vote, /raft/snapshot (inter-node RPC), /cluster/status |
| Leader forwarding | **Working** | Middleware returns 307 redirect for writes on follower nodes, serves reads locally |
| Prometheus | **Working** | /metrics endpoint with Raft metrics, LLM cache hit/miss counters |

## REST Routes

| Method | Path | Handler | Auth | Status |
|--------|------|---------|------|--------|
| GET | `/health` | health check | No | **Working** |
| GET | `/ready` | readiness probe | No | **Working** |
| GET | `/metrics` | Prometheus metrics | No | **Working** |
| POST | `/api/v1/query` | SQL query via DuckDB | Yes* | **Working** |
| POST | `/api/v1/ingest` | event ingestion (tenant-aware) | Yes* | **Working** |
| POST | `/api/v1/webhook/{source}` | webhook ingestion (HMAC-verified if `webhook_secret` set) | Yes* | **Working** |
| POST | `/api/v1/search` | semantic vector search | Yes* | **Working** |
| POST | `/api/v1/embed-and-search` | embed text + search | Yes* | **Working** |
| GET | `/api/v1/state/{agent_id}/{key}` | get state | Yes* | **Working** |
| PUT | `/api/v1/state/{agent_id}/{key}` | set state | Yes* | **Working** |
| GET | `/api/v1/state/{agent_id}/watch` | WebSocket state watcher | Yes* | **Working** |
| POST | `/api/v1/sessions` | start session | Yes* | **Working** |
| POST | `/api/v1/sessions/{id}/end` | end session | Yes* | **Working** |
| GET | `/api/v1/sessions/{id}/recall` | recall session events | Yes* | **Working** |
| GET | `/api/v1/schema/sources` | list event sources | Yes* | **Working** |
| GET | `/api/v1/schema/agents` | list agent IDs | Yes* | **Working** |
| POST | `/api/v1/admin/retention` | enforce retention | Yes* | **Working** |
| GET/PUT | `/api/v1/admin/retention/policies` | CRUD retention policies | Yes* | **Working** |
| POST | `/api/v1/admin/backup` | trigger backup | Yes* | **Working** |
| DELETE | `/api/v1/admin/tenants/{tenant_id}` | GDPR erasure (all stores) | Yes* (admin) | **Working** |
| GET | `/api/v1/admin/audit` | query audit log | Yes* | **Working** |
| POST | `/mcp` | MCP JSON-RPC (Streamable HTTP; session id) | No* | **Working** |
| GET | `/mcp` | MCP server→client SSE stream | No* | **Working** |
| POST | `/v1/chat/completions` | LLM proxy with auto-RAG + cache + SSE streaming | No* | **Working** |
| GET | `/cluster/status` | Raft cluster metrics | No | **Working** |
| POST | `/raft/append` | Raft AppendEntries RPC | No | **Working** |
| POST | `/raft/vote` | Raft RequestVote RPC | No | **Working** |
| POST | `/raft/snapshot` | Raft InstallSnapshot RPC | No | **Working** |

*Auth required only when `gateway.auth_enabled = true`.

## Testing

- `cargo test -p strata-gateway` (96 tests, incl. gRPC tenant isolation + proxy tool-use translation)
- Integration tests in `tests/integration/` (12 tests covering sessions, schema, MCP, batch limits, retention)
- Gateway lifecycle tests verify start/shutdown with port 0
- Auth middleware tests verify API key, JWT, RBAC, rate limiting
- OIDC tests verify JWKS URI construction, config deserialization
- Semantic cache tests verify exact-match and vector similarity
