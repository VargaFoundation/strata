# Why We Built Ecphoria: A Context Lake for AI Agents

AI agents are getting smarter every month. They can reason, plan, use tools, and hold long conversations. But most of them have the memory of a goldfish.

Every session starts from zero. The agent that helped you debug a production issue yesterday has no idea it happened today. The support bot that resolved a billing dispute for a customer can't recall the resolution when the same customer comes back. Three agents collaborating on a task can't share what they've learned — each one maintains its own fragile in-memory state that vanishes when the process restarts.

**AI agents need a memory layer.** Not a bolted-on afterthought — a purpose-built data layer that understands the three types of memory agents actually need.

## The Duct-Tape Stack

Most teams solve this by stitching together existing tools:

**PostgreSQL + pgvector + Redis.** Store events in Postgres, embeddings in pgvector, hot state in Redis. It works, but now you're running three services, writing glue code to keep them in sync, and designing schemas that were never meant for agent workloads. Your "quick agent prototype" now has a Docker Compose file with 5 services and a migration folder.

**Mem0 / Zep.** Purpose-built memory layers — promising, but cloud-first. Self-hosted options are limited or lagging. You can't `psql` into your data. You can't connect Grafana. Your agent's memory is behind someone else's API, and you're one pricing change away from a rewrite.

**Vector databases (Pinecone, Weaviate, Qdrant).** Excellent at semantic search. But search is only one piece of the puzzle. Agents also need an event timeline (what happened, in order), a state store (what does the agent know right now), and analytical queries (how many tickets did we auto-resolve this week). A vector DB gives you one of these. You still need the rest.

**Custom solutions.** We've talked to teams that spent months building their own agent memory stack. They all built roughly the same thing — an event store, a vector index, a KV cache — with different trade-offs and different bugs. It's undifferentiated engineering.

## What Ecphoria Does

Ecphoria is an open-source context lake that unifies the three types of AI memory in a single Rust binary:

**Episodic memory** — *What happened?* An append-only event store powered by DuckDB. Every agent interaction, tool call, decision, and observation is recorded with timestamps, sources, and structured payloads. Query it with full SQL:

```bash
curl -X POST http://localhost:8432/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT source, event_type, COUNT(*) FROM episodic GROUP BY 1, 2"}'
```

**Semantic memory** — *What's relevant?* A vector search index powered by USearch HNSW. Ingest events, and Ecphoria auto-embeds them (via Ollama or OpenAI). Search by meaning, not keywords:

```bash
curl -X POST http://localhost:8432/api/v1/embed-and-search \
  -H 'Content-Type: application/json' \
  -d '{"text": "customer billing dispute", "k": 5}'
```

**State memory** — *What does the agent know right now?* A versioned key-value store backed by SQLite. Agents read and write their current context — mood, topic, decisions, configuration — with atomic updates:

```bash
curl -X PUT http://localhost:8432/api/v1/state/my-agent/context \
  -H 'Content-Type: application/json' \
  -d '{"topic": "billing", "mood": "focused", "decisions": 7}'
```

All three share the same process, the same config, the same API. One `docker run`:

```bash
docker run -p 8432:8432 -p 5432:5432 ghcr.io/varga-foundation/ecphoria:latest
```

## What Makes It Different

**PostgreSQL wire-compatible.** Ecphoria speaks the Postgres protocol on port 5432. Connect with `psql`, Grafana, DBeaver, or any BI tool. Run analytical SQL directly against your agent's event history — no ETL, no export.

**MCP-native.** Claude and other MCP-enabled agents can use Ecphoria directly as a tool server. Add it to your `claude_desktop_config.json` and your agent gets `query`, `ingest`, `search`, and `state` tools out of the box.

**Single binary, batteries included.** No Docker Compose with 5 services. No migration scripts. No external databases. DuckDB, USearch, and SQLite are embedded in one ~60MB Rust binary. REST, gRPC, PG wire, MCP, Prometheus metrics — all from one process.

**Raft clustering for production.** When you need HA, add two more nodes and Ecphoria forms a Raft cluster with automatic leader election, write replication, and leader forwarding. Scale from laptop to production without changing your application code.

## Get Started

Try it in 30 seconds:

```bash
# Start Ecphoria
docker run -d -p 8432:8432 -p 5432:5432 ghcr.io/varga-foundation/ecphoria:latest

# Ingest an event
curl -X POST http://localhost:8432/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{"source": "my-agent", "events": [{"event_type": "hello", "payload": {"message": "world"}}]}'

# Query it back
curl -X POST http://localhost:8432/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM episodic"}'
```

Then explore the [examples](../../examples/) — a Claude agent with persistent memory, a multi-agent support system, a Grafana dashboard, a LangChain RAG pipeline.

Ecphoria is open source under the Apache 2.0 license. Star the repo, try it out, and tell us what you build.

[GitHub](https://github.com/varga-foundation/ecphoria) | [Documentation](https://docs.ecphoria.dev) | [Discord](https://discord.gg/ecphoria)
