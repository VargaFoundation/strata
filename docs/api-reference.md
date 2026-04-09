# API Reference

Strata exposes multiple protocol interfaces. All access the same underlying engine.

## REST API

Base URL: `http://localhost:8432`

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

### Query

Execute SQL against the context lake.

```
POST /api/v1/query
Content-Type: application/json
```

Request:
```json
{
  "sql": "SELECT * FROM episodic WHERE source = 'my-app' LIMIT 10"
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

Ingest events into episodic memory.

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
      "payload": {"user_id": "u1", "plan": "pro"},
      "timestamp": "2026-04-09T10:00:00Z"
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

### Semantic Search

Search across semantic memory by natural language query.

```
POST /api/v1/search
Content-Type: application/json
```

Request:
```json
{
  "query": "frustrated customer billing issue",
  "k": 5
}
```

Response:
```json
{
  "results": [
    {
      "entry": {
        "id": "...",
        "content": "Customer complained about billing...",
        "metadata": {}
      },
      "score": 0.92
    }
  ]
}
```

## PostgreSQL Wire Protocol

Strata speaks the PostgreSQL wire protocol on port 5432. Connect with any PostgreSQL client.

```bash
psql -h localhost -p 5432
```

### Supported SQL

```sql
-- Insert events
INSERT INTO episodic (source, event_type, payload)
VALUES ('app', 'click', '{"page": "/home"}');

-- Query events
SELECT * FROM episodic
WHERE source = 'app' AND timestamp > '2026-01-01'
ORDER BY timestamp DESC
LIMIT 100;

-- Semantic search via SQL function
SELECT * FROM strata_search('billing problem', 5);

-- Agent state
SELECT * FROM state WHERE agent_id = 'my-bot';
SET state.my_bot.status = '{"active": true}';

-- Embedding via SQL function
SELECT embed('Hello world');

-- Cosine similarity
SELECT cosine_similarity(embed('query'), embedding) AS score
FROM semantic
ORDER BY score DESC
LIMIT 10;
```

## MCP (Model Context Protocol)

Strata includes a built-in MCP server at `/mcp` using Streamable HTTP (SSE) transport.

### Configuration

```json
{
  "mcpServers": {
    "strata": {
      "url": "http://localhost:8432/mcp"
    }
  }
}
```

### Resources

| URI | Description |
|-----|-------------|
| `strata://episodic` | Append-only event store |
| `strata://semantic` | Vector embedding store |
| `strata://state` | Agent key-value state |

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

OpenAI-compatible endpoint with automatic RAG enrichment.

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

Strata automatically:
1. Embeds the user message
2. Searches semantic memory for relevant context
3. Prepends context to the system prompt
4. Forwards to the configured LLM provider
5. Caches the response by semantic similarity

Enable with `gateway.llm_proxy_enabled = true` in configuration.

## CLI Commands

```
strata status                          # Server health check
strata query "SELECT ..."              # Execute SQL
strata ingest --source X --file Y      # Bulk ingest from file
strata export --entity ID              # GDPR data export
strata backup --target s3://...        # Trigger backup
strata restore --from s3://...         # Restore from backup
```

### Global Options

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--url` | `STRATA_URL` | `http://localhost:8432` | Server URL |

## Error Responses

All endpoints return errors in this format:

```json
{
  "error": "description of what went wrong"
}
```

HTTP status codes:
- `200` — Success
- `400` — Bad request (malformed JSON, invalid parameters)
- `401` — Unauthorized (missing or invalid credentials)
- `403` — Forbidden (insufficient permissions)
- `404` — Not found
- `415` — Unsupported media type (missing Content-Type header)
- `422` — Unprocessable entity (valid JSON, invalid semantics)
- `500` — Internal server error
