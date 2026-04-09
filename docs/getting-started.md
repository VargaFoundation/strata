# Getting Started

Get Strata running in under a minute.

## Prerequisites

- Docker (for quick start) or Rust 1.82+ (for building from source)
- Any PostgreSQL client (psql, DBeaver, etc.) for querying

## Quick Start with Docker

```bash
docker run -d --name strata \
  -p 5432:5432 -p 8432:8432 \
  -v strata-data:/data \
  ghcr.io/vargafoundation/strata:latest
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
-- Ingest an event
INSERT INTO episodic (source, event_type, payload)
VALUES ('my-app', 'user.signup', '{"user_id": "u1", "plan": "pro"}');

-- Semantic search
SELECT * FROM strata_search('frustrated customer billing issue', 5);

-- Read agent state
SELECT * FROM state WHERE agent_id = 'support-bot';
```

## Full Stack with Docker Compose

For a complete dev environment with S3-compatible storage and local embeddings:

```bash
git clone https://github.com/VargaFoundation/strata.git
cd strata
docker compose up -d
```

This starts:
- **Strata** on ports 5432 (PG wire) and 8432 (HTTP)
- **MinIO** on port 9000 (S3 API) and 9001 (console)
- **Ollama** on port 11434 (embedding model)

Pull the embedding model on first run:

```bash
docker exec strata-ollama-1 ollama pull nomic-embed-text
```

## Build from Source

```bash
git clone https://github.com/VargaFoundation/strata.git
cd strata
cargo build --release
```

Run the server:

```bash
./target/release/strata-server
```

The server reads configuration from `strata.toml` and environment variables prefixed with `STRATA_`.

## MCP Integration

Strata includes a built-in MCP server. Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "strata": {
      "url": "http://localhost:8432/mcp"
    }
  }
}
```

Available MCP tools:
- `query` — Execute SQL against Strata
- `ingest` — Ingest events into episodic memory
- `search` — Semantic search across stored knowledge
- `get_state` / `set_state` — Read/write agent state
- `embed` — Compute vector embeddings

## CLI Usage

The `strata` CLI communicates with the server over HTTP:

```bash
# Check server status
strata status

# Execute a SQL query
strata query "SELECT count(*) FROM episodic"

# Ingest from a file
strata ingest --source my-app --file events.json

# GDPR export
strata export --entity user-123

# Backup
strata backup --target s3://my-bucket/backups/
```

Set `STRATA_URL` to point at a remote server:

```bash
export STRATA_URL=http://strata.internal:8432
strata status
```

## Configuration

Strata loads configuration in this order (later sources override earlier):

1. Built-in defaults
2. `strata.toml` (in working directory)
3. Environment variables with `STRATA_` prefix

See [deployment.md](deployment.md) for the full configuration reference.

## Next Steps

- [Architecture](architecture.md) — understand Strata's internals
- [API Reference](api-reference.md) — full REST, PG wire, and MCP documentation
- [Deployment](deployment.md) — production deployment and configuration
- [Contributing](contributing.md) — how to contribute to Strata
