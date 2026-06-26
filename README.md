<h1 align="center">Strata</h1>
<p align="center">
  <strong>The open-source memory engine for AI agents — self-hostable and benchmarkable.</strong><br>
  Bi-temporal memories with dedup, contradiction resolution, and hybrid search — in a single Rust binary.
</p>

<p align="center">
  <a href="https://github.com/VargaFoundation/strata/actions/workflows/ci.yml"><img src="https://github.com/VargaFoundation/strata/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/VargaFoundation/strata/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
</p>

---

AI agents lose their memory every time you restart them. Strata fixes that — and
unlike a bare vector DB, it does the *intelligent* part of memory for you.

Most "agent memory" intelligence (Mem0, Zep) lives behind a cloud API or a paywalled
graph. Strata is the **genuinely open, self-hostable, benchmarkable** alternative:
the smarts run in your own single Rust binary, on your own infrastructure.

### Memory intelligence (the part that's hard)

- **First-class, bi-temporal memories** — every fact has `valid_from`/`valid_to`, so you
  can ask *"what did we believe at time T?"* in plain SQL.
- **Contradiction resolution** — a newer fact about the same subject **supersedes** the old
  one (the old one is kept as history), deterministically, no LLM required.
- **Deduplication & consolidation** — near-duplicate facts merge instead of piling up.
- **Hybrid retrieval** — deterministic BM25 fused with vector search via Reciprocal Rank
  Fusion. Works (and ranks well) even with no embedding provider configured.
- **Decay-based forgetting** — low-value memories fade by time-decayed importance; nothing
  is silently hard-deleted.
- **Opt-in LLM extraction** — `remember("…raw conversation…")` distills atomic facts when a
  completion provider is configured; otherwise stores the text deterministically.
- **Mem0-compatible API + MCP-native** — drop-in `memories` REST endpoints and memory tools
  for Claude / Cursor / any MCP client.
- **Benchmarkable** — `cargo run -p strata-core --example locomo_eval` runs a LoCoMo-style eval
  reporting **recall@{1,3,5}, MRR, and ingest/query p50/p95**. Runs offline (pure-Rust BM25) out
  of the box; point it at a real export with a provider for hybrid numbers:
  `LOCOMO_PATH=your-locomo.json STRATA_EMBEDDING__PROVIDER=ollama cargo run -p strata-core --example locomo_eval`.
  We don't publish leaderboard numbers we can't reproduce — run it on your data and see.

### Built on a unified three-store substrate

| Memory | What it stores | Backend | Query |
|--------|---------------|---------|-------|
| **Episodic** | Events, logs, actions | DuckDB (columnar SQL) | Full SQL |
| **Semantic** | Embeddings, knowledge | USearch (HNSW vectors) | k-NN similarity |
| **State** | Live agent state, decisions | SQLite + DashMap (KV with TTL) | Get/Set/CAS |

PostgreSQL wire-compatible. MCP-native. Self-hosted. Raft-clustered for HA.

## Quick Start (3 minutes)

**1. Start Strata** (10 seconds)
```bash
docker run -d --name strata \
  -p 5432:5432 -p 8432:8432 \
  -v strata-data:/data \
  ghcr.io/vargafoundation/strata:latest
```

**2. Ingest events** (30 seconds)
```bash
curl -X POST localhost:8432/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "support-bot",
    "events": [
      {"event_type": "customer.contact", "customer_id": "cust_42", "message": "Double charged for order #1234", "sentiment": "frustrated"},
      {"event_type": "customer.contact", "customer_id": "cust_43", "message": "How do I upgrade my plan?", "sentiment": "neutral"}
    ]
  }'
```

**3. Query with SQL** (30 seconds)
```bash
psql -h localhost -p 5432 -c \
  "SELECT source, event_type, payload->>'customer_id' as customer, ts
   FROM episodic ORDER BY ts DESC LIMIT 10;"
```

**4. Search by meaning** (30 seconds)
```bash
curl -X POST localhost:8432/api/v1/embed-and-search \
  -H 'Content-Type: application/json' \
  -d '{"text": "angry customer billing issue", "k": 3}'
```

**5. Agent state** (30 seconds)
```bash
# Set state
curl -X PUT localhost:8432/api/v1/state/support-bot/context \
  -H 'Content-Type: application/json' \
  -d '{"topic": "billing", "active_tickets": 2, "priority": "high"}'

# Read state
curl localhost:8432/api/v1/state/support-bot/context
```

**6. Remember & recall facts** (the cognition layer)
```bash
# Remember a fact about a user (deduped; contradictions supersede the old fact)
curl -X POST localhost:8432/api/v1/memories \
  -H 'Content-Type: application/json' \
  -d '{"user_id": "cust_42", "subject": "plan", "content": "On the Pro plan"}'

# Later, a contradicting fact — the old one is superseded, not overwritten
curl -X POST localhost:8432/api/v1/memories \
  -H 'Content-Type: application/json' \
  -d '{"user_id": "cust_42", "subject": "plan", "content": "Upgraded to Enterprise"}'

# Hybrid search over that user's memories
curl -X POST localhost:8432/api/v1/memories/search \
  -H 'Content-Type: application/json' \
  -d '{"user_id": "cust_42", "query": "what plan are they on?"}'

# Full bi-temporal history of a memory (every superseded version)
curl localhost:8432/api/v1/memories/<id>/history
```

All three memory types plus the cognition layer, running and queryable in under 3 minutes.

→ **Connecting Claude?** See [docs/connect-claude.md](docs/connect-claude.md) — REST + tool-use
(recommended), MCP (Claude Code native / Claude Desktop via `mcp-remote`), or the auto-RAG proxy.

## Why Strata?

You're building AI agents. They need memory. Today you're wiring together
Postgres + pgvector + Redis + an embedding service + glue code.

Strata replaces all of that:

```
Before:  PostgreSQL + pgvector + Redis + embedding API + glue code
After:   docker run ghcr.io/vargafoundation/strata
```

### Comparison

| | DIY stack | Strata | Mem0 | Zep | Pinecone |
|---|---|---|---|---|---|
| Services to deploy | 3+ | **1** | Cloud/Self-hosted | Cloud/Self-hosted | Cloud only |
| Open source | Varies | **Apache 2.0** | Partial | Partial | No |
| Self-hosted | Complex | **Docker/K8s** | Limited | Complex (Neo4j) | No |
| Memory types | Build yourself | **Episodic + Semantic + State** | Conversations | Knowledge graph | Vectors only |
| PostgreSQL compatible | It IS Postgres | **Wire protocol** | No | No | No |
| MCP native | No | **Built-in** | No | No | No |
| Auto-embedding | Build yourself | **Configure provider, done** | Yes | Yes | No |
| GDPR / data locality | Manage 3 services | **1 binary, data stays local** | Cloud | Cloud | Cloud |
| Clustering / HA | Complex | **Raft built-in** | Managed | Managed | Managed |
| LLM proxy with auto-RAG | No | **Built-in** | No | No | No |

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

## TypeScript SDK

```bash
npm install @strata/client
```

```typescript
import { StrataClient } from '@strata/client';

const client = new StrataClient('http://localhost:8432');

await client.ingest('my-app', [
  { event_type: 'user.signup', user_id: 'u1' }
]);

const results = await client.search('billing issue', { k: 5 });
const state = await client.stateGet('bot-1', 'context');
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

Your AI agents can then query, ingest, and manage state directly via MCP tools.

## LLM Proxy (Auto-RAG)

OpenAI-compatible endpoint that automatically enriches prompts with relevant
context from Strata's memory stores:

```bash
curl -X POST localhost:8432/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-4",
    "messages": [{"role": "user", "content": "What happened with customer 42?"}]
  }'
```

Strata automatically:
1. Embeds the user query
2. Searches semantic memory for relevant knowledge
3. Pulls recent episodic events
4. Injects context into the system message
5. Forwards to your LLM provider (OpenAI, Anthropic, Ollama)

## Features

- **PostgreSQL wire protocol** — psql, DBeaver, Metabase, Grafana
- **Three unified memories** — episodic + semantic + state in one binary
- **MCP server** — native Claude, Cursor, VS Code integration
- **LLM proxy** — OpenAI-compatible `/v1/chat/completions` with auto-RAG
- **Auto-embedding** — Ollama or OpenAI, events embedded on ingest
- **Text search** — `embed-and-search`: text in, results out, one call
- **State watchers** — WebSocket real-time notifications
- **Event tracing** — parent_id, trace_id, tags for causal chains
- **Auth & RBAC** — API keys, JWT, role-based access (admin/writer/reader/agent)
- **Self-hosted** — data never leaves your servers
- **GDPR-ready** — retention policies, backup/restore, data locality
- **Single binary** — Docker, Compose, Kubernetes
- **Raft clustering** — 3-node HA, leader forwarding, follower reads
- **Prometheus metrics** — `/metrics` for observability
- **gRPC API** — high-performance alternative to REST
- **Python, TypeScript, Go SDKs** — async clients with retry logic

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
sdk/
  python/            Python SDK + LangChain/LlamaIndex integrations
  typescript/        TypeScript SDK
  go/                Go SDK
deploy/              Helm chart, Docker Compose configs
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Follow the conventions in `CLAUDE.md`
4. Run `cargo fmt`, `cargo clippy`, `cargo test`
5. Open a pull request

See [Contributing Guide](docs/contributing.md) for details.

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
