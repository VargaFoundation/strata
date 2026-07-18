# API Reference

Ecphoria exposes multiple protocol interfaces. All access the same underlying engine.

## REST API

Base URL: `http://localhost:8432`

### Authentication

When `gateway.auth_enabled = true`, all `/api/v1/*` endpoints require a Bearer token:

```
Authorization: Bearer <api-key>
```

The API key must match one of the keys in `gateway.api_keys`. Health, metrics, MCP, and cluster endpoints are unauthenticated.

### Health Check

```
GET /health
```

Response:
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

### Prometheus Metrics

```
GET /metrics
```

Returns Prometheus text format with counters and histograms:
- `ecphoria_episodic_events_ingested_total` — total events ingested
- `ecphoria_episodic_append_duration_seconds` — ingest latency histogram
- `ecphoria_episodic_queries_total` — total SQL queries executed
- `ecphoria_episodic_query_duration_seconds` — query latency histogram
- `ecphoria_rest_requests_total{endpoint="..."}` — REST requests by endpoint
- `ecphoria_rest_request_duration_seconds{endpoint="..."}` — REST latency by endpoint

### Query

Execute read-only SQL against the episodic store. Only SELECT statements are allowed (enforced by SQL parser). Results are capped at `query.max_rows` (default 10,000).

```
POST /api/v1/query
Content-Type: application/json
```

Request:
```json
{
  "sql": "SELECT * FROM episodic WHERE source = 'my-app' ORDER BY ts DESC LIMIT 10"
}
```

Response:
```json
{
  "rows": [...],
  "count": 10
}
```

### Ingest

Ingest events into episodic memory. When an embedding provider is configured, events are automatically embedded and indexed in semantic memory (batched by `embedding.batch_size`).

```
POST /api/v1/ingest
Content-Type: application/json
```

Request:
```json
{
  "source": "my-app",
  "events": [
    {
      "event_type": "user.signup",
      "user_id": "u1",
      "plan": "pro"
    }
  ]
}
```

Response:
```json
{
  "ingested": 1
}
```

### Webhook Ingest

Ingest from third-party webhook providers (GitHub, Sentry, Slack, PagerDuty).

```
POST /api/v1/webhook/{source}
Content-Type: application/json
```

The payload is normalized into Ecphoria events based on the source.

### Semantic Search

Search across semantic memory by vector similarity.

```
POST /api/v1/search
Content-Type: application/json
```

Request:
```json
{
  "query": "frustrated customer billing issue",
  "vector": [0.1, 0.2, ...],
  "k": 5
}
```

Response:
```json
{
  "results": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "content": "Customer complained about billing...",
      "metadata": {"source": "support", "event_type": "ticket.created"},
      "score": 0.92
    }
  ]
}
```

### Agent State

Read and write per-agent key-value state.

```
GET /api/v1/state/{agent_id}/{key}
```

Response:
```json
{
  "agent_id": "support-bot",
  "key": "mood",
  "value": "happy",
  "version": 3
}
```

```
PUT /api/v1/state/{agent_id}/{key}
Content-Type: application/json

"happy"
```

Response:
```json
{
  "version": 4
}
```

## Cluster Endpoints

Available when `cluster.enabled = true`.

### Cluster Status

```
GET /cluster/status
```

Response:
```json
{
  "node_id": 1,
  "state": "Leader",
  "current_leader": 1,
  "current_term": 3,
  "last_log_index": 42,
  "last_applied": 42,
  "membership": "..."
}
```

### Leader Forwarding

When a write request (POST, PUT, DELETE) arrives at a follower node, it returns a 307 redirect:

```json
{
  "error": "not_leader",
  "leader_id": 1,
  "message": "This node is not the leader. Retry on the leader node."
}
```

GET requests (reads) are always served locally from the follower's engine for low-latency reads.

### Raft RPC Endpoints (Internal)

These are used for inter-node Raft communication and should not be called by clients:

- `POST /raft/append` — AppendEntries RPC
- `POST /raft/vote` — RequestVote RPC
- `POST /raft/snapshot` — InstallSnapshot RPC

## PostgreSQL Wire Protocol

Ecphoria speaks the PostgreSQL wire protocol on port 5432. Connect with any PostgreSQL client.

```bash
psql -h localhost -p 5432
```

Connection limit is configurable via `gateway.max_pg_connections` (default 256). Excess connections are rejected.

### Supported SQL

Only SELECT queries are allowed via the SQL validation layer:

```sql
-- Query events
SELECT * FROM episodic
WHERE source = 'app'
ORDER BY ts DESC
LIMIT 100;

-- Count events
SELECT count(*) FROM episodic WHERE source = 'app';

-- Filter by time range
SELECT * FROM episodic
WHERE ts >= '2026-01-01T00:00:00Z'::TIMESTAMPTZ
ORDER BY ts ASC;

-- Aggregate by source
SELECT source, count(*) as cnt
FROM episodic
GROUP BY source
ORDER BY cnt DESC;
```

## MCP (Model Context Protocol)

Ecphoria includes a built-in MCP server at `/mcp` using Streamable HTTP (SSE) transport.

### Configuration

Add to Claude Desktop, VS Code, Cursor, or any MCP client:

```json
{
  "mcpServers": {
    "ecphoria": {
      "url": "http://localhost:8432/mcp"
    }
  }
}
```

### Resources

| URI | Description |
|-----|-------------|
| `ecphoria://episodic` | Append-only event store |
| `ecphoria://semantic` | Vector embedding store |
| `ecphoria://state` | Agent key-value state |

### Tools

| Tool | Description | Parameters |
|------|-------------|------------|
| `query` | Execute SQL query | `sql` (string) |
| `ingest` | Ingest events | `source` (string), `events` (array) |
| `search` | Semantic search | `query` (string), `k` (number) |
| `get_state` | Get agent state | `agent_id` (string), `key` (string) |
| `set_state` | Set agent state | `agent_id` (string), `key` (string), `value` (any) |
| `embed` | Compute embedding | `text` (string) |

### Prompts

| Prompt | Description |
|--------|-------------|
| `analyze_events` | Analyze recent events for a given source |
| `summarize_state` | Summarize current agent state |

## LLM Proxy

OpenAI-compatible endpoint with automatic RAG enrichment from episodic memory.

```
POST /v1/chat/completions
Content-Type: application/json
```

Request (same as OpenAI API):
```json
{
  "model": "claude-sonnet-4-20250514",
  "messages": [
    {"role": "user", "content": "What happened with customer u1?"}
  ]
}
```

Ecphoria automatically:
1. Extracts the last user message
2. Queries episodic memory for recent relevant events
3. Prepends context to the system prompt
4. Determines the LLM provider from the model name
5. Forwards the enriched request to the provider

Provider detection:
- `gpt-*`, `o1-*`, `o3-*` → OpenAI
- `claude-*` → Anthropic
- All others → Ollama (local inference)

## CLI Commands

```bash
ecphoria status                          # Server health check
ecphoria query "SELECT ..."              # Execute SQL
ecphoria ingest --source X --file Y      # Bulk ingest from file
ecphoria export --entity ID              # GDPR data export
ecphoria backup --target s3://...        # Trigger backup
ecphoria restore --from s3://...         # Restore from backup
```

### Global Options

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--url` | `ECPHORIA_URL` | `http://localhost:8432` | Server URL |

## Error Responses

All endpoints return errors in this format:

```json
{
  "error": "description of what went wrong"
}
```

HTTP status codes:
- `200` — Success
- `307` — Temporary Redirect (follower node, retry on leader)
- `400` — Bad request (malformed JSON, invalid parameters)
- `401` — Unauthorized (missing or invalid API key)
- `403` — Forbidden (insufficient permissions)
- `404` — Not found
- `408` — Request Timeout (query exceeded `query.timeout_ms`)
- `422` — Unprocessable entity (valid JSON, invalid semantics — e.g., non-SELECT SQL)
- `500` — Internal server error
- `503` — Service Unavailable (no leader elected yet)
- `504` — Gateway Timeout (request exceeded 30s HTTP timeout)
