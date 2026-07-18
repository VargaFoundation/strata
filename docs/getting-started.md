# Getting Started

Get Ecphoria running in under a minute.

## Prerequisites

- Docker (for quick start) or Rust 1.82+ (for building from source)
- Any PostgreSQL client (psql, DBeaver, etc.) for querying

## Quick Start with Docker

```bash
docker run -d --name ecphoria \
  -p 5432:5432 -p 8432:8432 \
  -v ecphoria-data:/data \
  -e ECPHORIA_MEMORY__EPISODIC__DB_PATH=/data/episodic.duckdb \
  ghcr.io/vargafoundation/ecphoria:latest
```

Verify it's running:

```bash
curl http://localhost:8432/health
# {"status":"ok","version":"0.1.0"}
```

## Connect with PostgreSQL

```bash
psql -h localhost -p 5432
```

```sql
-- Query events
SELECT * FROM episodic WHERE source = 'my-app' LIMIT 10;

-- Count by source
SELECT source, count(*) FROM episodic GROUP BY source;
```

## Ingest Events via REST

```bash
curl -X POST http://localhost:8432/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "my-app",
    "events": [
      {"event_type": "user.signup", "user_id": "u1", "plan": "pro"},
      {"event_type": "page.view", "user_id": "u1", "page": "/dashboard"}
    ]
  }'
# {"ingested":2}
```

## Full Stack with Docker Compose

For a complete dev environment with S3-compatible storage and local embeddings:

```bash
git clone https://github.com/VargaFoundation/ecphoria.git
cd ecphoria
docker compose up -d
```

This starts:
- **Ecphoria** on ports 5432 (PG wire) and 8432 (HTTP)
- **MinIO** on port 9000 (S3 API) and 9001 (console)
- **Ollama** on port 11434 (embedding model)

Pull the embedding model on first run:

```bash
docker exec ecphoria-ollama-1 ollama pull nomic-embed-text
```

Events ingested via `/api/v1/ingest` are now automatically embedded and searchable via `/api/v1/search`.

## Cluster Mode (3-Node HA)

Test a Raft cluster locally:

```bash
docker compose -f deploy/docker-compose.cluster.yml up -d
```

Check cluster health:

```bash
curl http://localhost:8432/cluster/status
# {"node_id":1,"state":"Leader","current_leader":1,...}
```

All three nodes share the same data via Raft consensus. Reads are served by any node; writes go through the leader.

For Kubernetes deployment, see the [Helm chart](../deploy/helm/ecphoria/):

```bash
helm install ecphoria deploy/helm/ecphoria/ --set replicaCount=3
```

## Build from Source

```bash
git clone https://github.com/VargaFoundation/ecphoria.git
cd ecphoria
cargo build --release
```

Run the server:

```bash
./target/release/ecphoria-server
```

The server reads configuration from `ecphoria.toml` and environment variables prefixed with `ECPHORIA_`.

## MCP Integration

Ecphoria includes a built-in MCP server. Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "ecphoria": {
      "url": "http://localhost:8432/mcp"
    }
  }
}
```

Available MCP tools:
- `query` — Execute SQL against Ecphoria
- `ingest` — Ingest events into episodic memory
- `search` — Semantic search across stored knowledge
- `get_state` / `set_state` — Read/write agent state
- `embed` — Compute vector embeddings

## CLI Usage

The `ecphoria` CLI communicates with the server over HTTP:

```bash
# Check server status
ecphoria status

# Execute a SQL query
ecphoria query "SELECT count(*) FROM episodic"

# Ingest from a file
ecphoria ingest --source my-app --file events.json
```

Set `ECPHORIA_URL` to point at a remote server:

```bash
export ECPHORIA_URL=http://ecphoria.internal:8432
ecphoria status
```

## Monitoring

Ecphoria exposes Prometheus-compatible metrics:

```bash
curl http://localhost:8432/metrics
```

Key metrics:
- `ecphoria_episodic_events_ingested_total` — total events ingested
- `ecphoria_episodic_queries_total` — total queries executed
- `ecphoria_episodic_append_duration_seconds` — ingest latency
- `ecphoria_episodic_query_duration_seconds` — query latency

## Configuration

Ecphoria loads configuration in this order (later sources override earlier):

1. Built-in defaults
2. `ecphoria.toml` (in working directory)
3. Environment variables with `ECPHORIA_` prefix

See [deployment.md](deployment.md) for the full configuration reference.

## Next Steps

- [Architecture](architecture.md) — understand Ecphoria's internals
- [API Reference](api-reference.md) — full REST, PG wire, MCP, and cluster documentation
- [Deployment](deployment.md) — production deployment, Kubernetes Helm chart, and configuration
- [Contributing](contributing.md) — how to contribute to Ecphoria
