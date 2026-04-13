<h1 align="center">Strata</h1>
<p align="center">
  <strong>Give your AI agents persistent memory.</strong><br>
  Episodic, semantic, and state — in a single binary.
</p>

<p align="center">
  <a href="https://github.com/VargaFoundation/strata/actions/workflows/ci.yml"><img src="https://github.com/VargaFoundation/strata/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/VargaFoundation/strata/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
</p>

---

AI agents lose their memory every time you restart them. Strata fixes that.

It's an open-source **context lake** — a unified data layer that combines three
types of memory in a single Rust binary:

| Memory | What it stores | Backend |
|--------|---------------|---------|
| **Episodic** | Events, logs, actions | DuckDB (columnar SQL) |
| **Semantic** | Embeddings, knowledge | USearch (HNSW vectors) |
| **State** | Live agent state, decisions | SQLite + DashMap (KV with TTL) |

All three are queryable with SQL via PostgreSQL wire protocol. Works with
`psql`, DBeaver, Metabase, Grafana. MCP-native for Claude, Cursor, VS Code.

## Quick Start

```bash
docker run -d --name strata \
  -p 5432:5432 -p 8432:8432 \
  -v strata-data:/data \
  ghcr.io/vargafoundation/strata:latest
```

That's it. Connect with `psql`:

```bash
psql -h localhost -p 5432
```

```sql
SELECT * FROM episodic WHERE source = 'my-app' ORDER BY ts DESC LIMIT 10;
```

Or use the REST API:

```bash
# Ingest events (auto-embedded when Ollama/OpenAI is configured)
curl -X POST localhost:8432/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{"source":"my-app","events":[{"event_type":"user.signup","user_id":"u1"}]}'

# Search by natural language
curl -X POST localhost:8432/api/v1/embed-and-search \
  -d '{"text":"frustrated customer billing issue","k":5}'

# Agent state
curl localhost:8432/api/v1/state/support-bot/mood
```

## Why Strata?

You're building AI agents. They need memory. Today you're wiring together
Postgres + pgvector + Redis + an embedding service + glue code.

Strata replaces all of that:

```
Before:  PostgreSQL + pgvector + Redis + embedding API + glue code
After:   docker run ghcr.io/vargafoundation/strata
```

| | DIY stack | Strata |
|---|---|---|
| Services to deploy | 3+ | 1 |
| Memory types | Build yourself | Episodic + Semantic + State |
| Auto-embedding | Build yourself | Configure provider, done |
| MCP for Claude | Not available | Built-in |
| PostgreSQL compatible | It IS Postgres | Wire protocol |
| Self-hosted / GDPR | Manage 3 services | 1 binary, data stays local |
| Clustering / HA | Complex | Raft built-in |

## Python SDK

```bash
pip install strata-client
```

```python
from strata import StrataClient

async with StrataClient("http://localhost:8432") as client:
    # Ingest events
    await client.ingest("my-app", [
        {"event_type": "user.signup", "user_id": "u1", "plan": "pro"}
    ])

    # Search by text (auto-embeds via configured provider)
    results = await client.find("frustrated customer", k=5)

    # Agent state with TTL
    await client.state_set("bot-1", "mood", "happy")

    # Query with fluent API (no raw SQL)
    events = await client.events(source="my-app", limit=10)
```

## LangChain

```python
from langchain_strata import StrataRetriever

retriever = StrataRetriever(url="http://localhost:8432", k=5)
docs = retriever.get_relevant_documents("billing issue")
```

## MCP

Built-in MCP server. Add to Claude Desktop, Cursor, or VS Code:

```json
{
  "mcpServers": {
    "strata": {
      "url": "http://localhost:8432/mcp"
    }
  }
}
```

## Features

- **PostgreSQL wire protocol** — psql, DBeaver, Metabase, Grafana
- **Three unified memories** — episodic + semantic + state in one binary
- **MCP server** — native Claude, Cursor, VS Code integration
- **Auto-embedding** — Ollama or OpenAI, events embedded on ingest
- **Text search** — `embed-and-search`: text in, results out, one call
- **State watchers** — WebSocket real-time notifications
- **Event tracing** — parent_id, trace_id, tags for causal chains
- **Self-hosted** — data never leaves your servers
- **GDPR-ready** — retention policies, backup/restore, data locality
- **Single binary** — Docker, Compose, Kubernetes
- **Raft clustering** — 3-node HA, leader forwarding, follower reads
- **Prometheus metrics** — `/metrics` for observability
- **LLM proxy** — OpenAI-compatible `/v1/chat/completions` with auto-RAG
- **Python SDK** — async client with query builder + LangChain integration

## Deployment

| Mode | Command | Best for |
|------|---------|----------|
| Docker | `docker run ghcr.io/vargafoundation/strata` | Dev, small prod |
| Compose | `docker compose up` | Teams, full stack |
| Cluster | `docker compose -f deploy/docker-compose.cluster.yml up` | HA testing |
| Kubernetes | `helm install strata deploy/helm/strata/` | Production HA |

## Full Dev Stack

```bash
git clone https://github.com/VargaFoundation/strata.git
cd strata
docker compose up -d
```

Starts Strata + MinIO (S3 storage) + Ollama (local embeddings).

## Project Structure

```
crates/
  strata-core/       Core engine: memories, query, storage, ingest, embedding
  strata-gateway/    Protocols: PostgreSQL wire, REST, gRPC, MCP, LLM proxy
  strata-cluster/    Raft consensus, replication, leader forwarding
  strata-cli/        CLI admin tool
strata-server/       Main binary
sdk/python/          Python SDK + LangChain integration
deploy/              Helm chart, Docker Compose configs
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Follow the conventions in `CLAUDE.md`
4. Run `cargo fmt`, `cargo clippy`, `cargo test`
5. Open a pull request

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
