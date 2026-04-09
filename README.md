<h1 align="center">Strata</h1>
<p align="center">
  <strong>The open-source context lake for AI agents.</strong><br>
  Deploy in 30 seconds. Scale to millions. Keep your data on your servers.
</p>

<p align="center">
  <a href="https://github.com/VargaFoundation/strata/actions/workflows/ci.yml"><img src="https://github.com/VargaFoundation/strata/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/VargaFoundation/strata/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
</p>

---

Strata is a **context lake** — a unified data layer that gives your AI agents a
shared, real-time understanding of reality. It combines three types of memory in
a single Rust binary:

- **Episodic Memory** — What happened (events, logs, actions)
- **Semantic Memory** — What it means (embeddings, entities, relationships)
- **State Memory** — Where things stand (live agent state, features, decisions)

All three are queried in a single ACID transaction. PostgreSQL wire-compatible,
MCP-native, self-hosted.

## Quick Start

```bash
docker run -d --name strata \
  -p 5432:5432 -p 8432:8432 \
  -v strata-data:/data \
  ghcr.io/vargafoundation/strata:latest
```

Connect with any PostgreSQL client:

```bash
psql -h localhost -p 5432
```

```sql
-- Ingest an event
INSERT INTO episodic (source, event_type, payload)
VALUES ('my-app', 'user.signup', '{"user_id": "u1", "plan": "pro"}');

-- Semantic search
SELECT * FROM strata_search('frustrated customer billing issue', 5);

-- Agent state
SELECT * FROM state WHERE agent_id = 'support-bot';
```

## Full Dev Stack

```bash
git clone https://github.com/VargaFoundation/strata.git
cd strata
docker compose up -d
```

Starts Strata + MinIO (S3-compatible storage) + Ollama (local embeddings).

## Build from Source

```bash
git clone https://github.com/VargaFoundation/strata.git
cd strata
cargo build --release
./target/release/strata-server
```

## MCP Integration

Strata includes a built-in MCP server. Add to Claude Desktop or any MCP client:

```json
{
  "mcpServers": {
    "strata": {
      "url": "http://localhost:8432/mcp"
    }
  }
}
```

## Project Structure

```
crates/
  strata-core/       Core engine: memories, query, storage, ingest, embedding
  strata-gateway/    Protocol layer: PostgreSQL wire, REST, gRPC, MCP, LLM proxy
  strata-cluster/    Distributed mode: Raft consensus, replication
  strata-cli/        CLI admin tool
strata-server/       Main binary entry point
```

## Features

- **PostgreSQL wire protocol** — works with psql, DBeaver, Metabase, Grafana
- **Three unified memories** — episodic + semantic + state in one transaction
- **MCP server built-in** — native integration with Claude, Cursor, VS Code
- **Hybrid search** — SQL + vector similarity + metadata filters
- **Self-hosted** — your data never leaves your servers
- **GDPR-native** — built-in erasure, export, audit, retention policies
- **Single binary** — Docker, Compose, Kubernetes
- **Rust-powered** — sub-10ms queries, minimal resource usage
- **LLM proxy** — OpenAI-compatible endpoint with automatic RAG

## Deployment

| Mode | Command | Best for |
|------|---------|----------|
| Docker | `docker run ghcr.io/vargafoundation/strata` | Dev, small prod |
| Compose | `docker compose up` | Teams, medium prod |
| Kubernetes | `helm install strata` | Enterprise, HA |

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Follow the conventions in `CLAUDE.md`
4. Ensure `cargo fmt`, `cargo clippy`, and `cargo test` pass
5. Open a pull request

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
