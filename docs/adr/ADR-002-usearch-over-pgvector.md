# ADR-002: USearch over pgvector

**Status**: Accepted  
**Date**: 2024-06-20  
**Author**: Ecphoria Core Team

## Context

Ecphoria's semantic memory stores vector embeddings alongside event metadata, enabling agents to search for relevant past experiences by meaning rather than keywords. We need a vector search engine that:

- Runs embedded (no external service)
- Supports HNSW approximate nearest neighbor search
- Handles upsert and delete (not just batch-build-once)
- Persists indexes to disk
- Has Rust bindings or a C FFI
- Works within a single-binary architecture

### Alternatives Considered

**pgvector**: The default choice for Postgres-based stacks. Excellent if you're already running Postgres, but Ecphoria doesn't use Postgres — we use DuckDB for episodic and SQLite for state. Adding a full Postgres dependency solely for vector search contradicts the single-binary goal.

**Qdrant / Milvus / Weaviate**: Purpose-built vector databases with rich feature sets (filtering, multi-tenancy, replication). But they all run as separate services with their own ports, configs, and upgrade cycles. They solve a harder problem than we need — Ecphoria's semantic store is one component within a larger system, not a standalone service.

**FAISS**: Meta's vector search library. High performance, battle-tested. However, the primary interface is C++ with Python bindings. Rust bindings exist but are community-maintained and lag behind. Index types are build-once (no incremental updates without rebuilding), and there's no built-in persistence.

**Annoy (Spotify)**: Simple, memory-mapped, fast reads. But indexes are immutable after building — no upserts or deletes. Unsuitable for a continuously-updating memory system.

## Decision

Use **USearch** for the semantic memory vector index.

USearch is a single-file C library for HNSW vector search with official Rust bindings (`usearch` crate). It supports:

- Multiple distance metrics (cosine, L2, inner product)
- Incremental upsert and delete operations
- Persistent save/load of the index file
- Low memory footprint via quantization options
- Thread-safe concurrent reads

Implementation details:

- **EntryMetadata pattern**: We store `(id, source, event_type, content, timestamp)` in a separate map, not in the index itself. This avoids duplicating large text data inside the HNSW graph, reducing memory footprint significantly.
- **Post-filtering**: USearch doesn't support native metadata filtering. We over-fetch by 2x and filter in application code. For Ecphoria's scale (thousands to low millions of vectors per node), this is efficient enough.
- **Persistence**: The index is saved to disk on shutdown and loaded on startup. Configurable via `memory.semantic.index_dir`.
- **Blocking operations**: Index operations (search, upsert, save) are wrapped in `spawn_blocking` to avoid blocking the Tokio runtime.

## Consequences

### Positive

- **Truly embedded**: Single C file compiled into the binary. No external service, no network hop for search, no separate process to manage.
- **Incremental updates**: Unlike FAISS/Annoy, USearch supports upsert and delete without rebuilding the entire index. Critical for a continuously-ingesting memory system.
- **Low memory**: The EntryMetadata pattern means we don't duplicate event content inside the vector index. Memory usage scales with vector count, not content size.
- **Fast HNSW**: Sub-millisecond search over hundreds of thousands of vectors. More than sufficient for agent memory workloads.
- **Clean Rust bindings**: Official `usearch` crate, actively maintained by the USearch team.

### Negative

- **No native filtering**: Metadata filtering must happen in application code (post-filter). For high-cardinality filters with large indexes, this means over-fetching. At Ecphoria's target scale this is acceptable, but it won't match the efficiency of Qdrant's native filtered search.
- **Less ecosystem tooling**: pgvector has a large ecosystem (ORMs, migration tools, monitoring). USearch is more niche — no pgAdmin equivalent for inspecting indexes.
- **Manual index lifecycle**: We manage save/load/compaction ourselves. There's no WAL or automatic crash recovery for the index — if the process crashes before saving, recent vectors may be lost. Mitigated by periodic saves and Raft replication in cluster mode.
