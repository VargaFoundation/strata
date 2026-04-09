# Architecture

This document describes Strata's internal architecture, crate structure, data model, and key design decisions.

> **Current status**: All three memory stores are functional — DuckDB-backed episodic store,
> USearch HNSW semantic store, SQLite+DashMap state store. The gateway serves REST API,
> PostgreSQL wire protocol (psql-compatible), and MCP JSON-RPC (tools/call for query/ingest/state).
> Embedding providers (Ollama, OpenAI) are implemented. See each crate's CLAUDE.md for details.

## System Overview

Strata is a **context lake** — a unified data layer for AI agents that combines three types of memory in a single Rust binary:

```
                         ┌──────────────────────────────────┐
                         │           strata-server           │
                         │  (config loading, signal handling) │
                         └──────────┬───────────────────────┘
                                    │
                         ┌──────────▼───────────────────────┐
                         │         strata-gateway            │
                         │                                    │
                         │  ┌─────────┐  ┌──────┐  ┌──────┐ │
  Clients ──────────────▶│  │ PG Wire │  │ REST │  │ MCP  │ │
  (psql, apps, Claude)   │  └────┬────┘  └──┬───┘  └──┬───┘ │
                         │       │          │         │      │
                         │  ┌────┴──────────┴─────────┴────┐ │
                         │  │    Auth Middleware             │ │
                         │  └──────────────┬────────────────┘ │
                         └─────────────────┼─────────────────┘
                                           │
                         ┌─────────────────▼─────────────────┐
                         │          strata-core               │
                         │                                     │
                         │  ┌───────────┐ ┌──────────────────┐│
                         │  │  Query    │ │  Ingest Pipeline  ││
                         │  │  Engine   │ │                    ││
                         │  └─────┬─────┘ └────────┬──────────┘│
                         │        │                │           │
                         │  ┌─────▼────────────────▼─────────┐│
                         │  │        Memory Manager           ││
                         │  │                                  ││
                         │  │ ┌──────────┐ ┌────────┐ ┌─────┐││
                         │  │ │ Episodic │ │Semantic│ │State│ ││
                         │  │ │  (WAL)   │ │(HNSW)  │ │(KV) │ ││
                         │  │ └──────────┘ └────────┘ └─────┘ ││
                         │  └──────────────────────────────────┘│
                         │                                      │
                         │  ┌──────────────────────────────────┐│
                         │  │       Storage Backends            ││
                         │  │  Local FS  │  S3/MinIO  │ Tiering ││
                         │  └──────────────────────────────────┘│
                         └──────────────────────────────────────┘
```

## Crate Structure

Strata is organized as a Cargo workspace with five crates:

### `strata-core`

The engine. Contains all business logic with zero knowledge of transport protocols.

| Module | Purpose |
|--------|---------|
| `memory::episodic` | WAL-based append-only event store |
| `memory::semantic` | USearch HNSW vector index + metadata |
| `memory::state` | Transactional KV store with MVCC (SQLite + DashMap) |
| `query::planner` | Routes SQL to DuckDB, vector search, or hybrid |
| `query::executor` | Executes query plans against memory stores |
| `query::functions` | Custom SQL UDFs: `embed()`, `cosine_similarity()`, `strata_search()` |
| `storage` | `StorageBackend` trait + local/S3 implementations |
| `storage::tiering` | Hot/warm/cold data movement between tiers |
| `ingest::pipeline` | Event ingestion → episodic store → auto-embedding → semantic index |
| `embedding` | `EmbeddingProvider` trait + Ollama/OpenAI implementations |
| `materialized` | Incremental materialized views over DuckDB |

### `strata-gateway`

Protocol layer. Translates external protocols into calls on `strata-core::StrataEngine`.

| Module | Purpose |
|--------|---------|
| `pg_wire` | PostgreSQL wire protocol via `pgwire` crate |
| `rest` | REST API via axum (`/health`, `/api/v1/*`) |
| `grpc` | gRPC server via tonic (proto pending) |
| `mcp` | MCP server — SSE transport, tools, resources, prompts |
| `llm_proxy` | OpenAI-compatible `/v1/chat/completions` with auto-RAG |
| `auth` | API key, JWT, middleware layer |

### `strata-cluster`

Distributed mode (Phase 3). Implements Raft consensus via `openraft`.

| Module | Purpose |
|--------|---------|
| `raft` | Raft state machine, log store, network transport |
| `replication` | WAL segment shipping, snapshot transfer |
| `coordinator` | Leader election awareness, request routing |

### `strata-cli`

CLI admin tool. Communicates with the server via HTTP. Binary name: `strata`.

### `strata-server`

Main binary. Thin wiring layer: config → engine → gateway → signal handling.

## Dependency Graph

```
strata-server (binary)
  ├── strata-core
  ├── strata-gateway → strata-core
  └── strata-cluster → strata-core

strata-cli (binary)
  └── strata-core (shared types)
```

**Rule**: dependencies flow downward. `strata-gateway` and `strata-cluster` never depend on each other. `strata-core` has zero knowledge of the protocol or cluster layers.

## Data Model

### Three Memory Types

**Episodic Memory** — What happened.
- Append-only event store backed by a write-ahead log (WAL)
- Each event has: `id`, `source`, `event_type`, `payload` (JSON), `timestamp`
- Indexed and queryable via DuckDB for time-range and SQL analytics
- Retention policies with configurable TTL

**Semantic Memory** — What it means.
- Vector embeddings stored in a USearch HNSW index
- Each entry has: `id`, `content`, `embedding` (f32 vector), `metadata` (JSON)
- Supports k-nearest-neighbor search with cosine, L2, or inner product metrics
- Auto-populated from episodic events via the ingestion pipeline

**State Memory** — Where things stand.
- Transactional key-value store with MVCC (multi-version concurrency control)
- Each entry has: `agent_id`, `key`, `value` (JSON), `version`
- Supports compare-and-swap (CAS) for lock-free coordination
- Hot cache via DashMap, persistent storage via SQLite

### Query Pipeline

```
SQL query
    │
    ▼
QueryPlanner ──────────────────┐
    │                          │
    ├── Pure SQL ──► DuckDB    │
    ├── Vector Search ──► USearch
    └── Hybrid ──► DuckDB + USearch
    │
    ▼
QueryExecutor
    │
    ▼
Results (JSON rows)
```

### Ingestion Pipeline

```
Events (HTTP/WebSocket/gRPC/Webhook)
    │
    ▼
IngestPipeline
    │
    ├── Validate & normalize
    ├── Write to EpisodicStore (WAL)
    ├── Auto-embed via EmbeddingProvider
    └── Upsert to SemanticStore (HNSW)
```

## Storage Tiers

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│    Hot       │     │    Warm     │     │    Cold     │
│  (Local FS)  │────▶│  (Local FS)  │────▶│  (S3/MinIO) │
│  < 7 days    │     │  7-30 days   │     │  > 30 days   │
└─────────────┘     └─────────────┘     └─────────────┘
```

The `TieringManager` runs periodic passes to move data between tiers based on configurable age policies.

## Key Technology Choices

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Language | Rust | Performance, safety, single binary, no runtime |
| Analytics SQL | DuckDB (embedded) | Columnar, zero-config, MIT license |
| Vector Index | USearch | HNSW, 10x more compact than FAISS, Rust bindings |
| State Storage | SQLite (via rusqlite) | ACID, embedded, battle-tested |
| Object Storage | S3/MinIO (via aws-sdk-s3) | Standard, tiered, cost-effective |
| Consensus | openraft | Raft in Rust, well-maintained |
| PG Protocol | pgwire | PostgreSQL wire protocol in Rust |
| HTTP Framework | axum | Async, tower-compatible, high performance |
| gRPC | tonic | HTTP/2, codegen from proto |
| Config | TOML + env vars | Convention over configuration |

## Thread Model

Strata runs on the Tokio multi-threaded runtime. The engine (`StrataEngine`) is `Send + Sync` and wrapped in `Arc` for shared ownership across protocol handlers.

Each protocol listener (REST, PG wire, gRPC, MCP) runs as a separate Tokio task. All handlers share the same engine instance.

## Security Model

Authentication is handled at the gateway layer:
- **API Keys**: Simple shared-secret for machine-to-machine
- **JWT**: Stateless token-based for user sessions
- **RBAC**: Four roles — Admin, Writer, Reader, Agent

All auth is enforced via Tower middleware before requests reach the engine.
