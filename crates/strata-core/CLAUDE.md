# strata-core

## Responsibility

Core engine crate. Contains all business logic: three memory stores (episodic,
semantic, state), query planning/execution, storage backends, ingestion pipeline,
and embedding providers. Has ZERO knowledge of protocols (HTTP, PG wire, gRPC).

## Public API Surface

- `StrataEngine` — main entry point, owns all subsystems
- `EpisodicStore` — append-only event storage (WAL-based)
- `SemanticStore` — vector storage + HNSW index (USearch)
- `StateStore` — transactional KV with MVCC
- `QueryPlanner` / `QueryExecutor` — SQL query processing (via DuckDB)
- `IngestPipeline` — event ingestion + auto-embedding
- `EmbeddingProvider` (trait) — pluggable embedding backends
- `StorageBackend` (trait) — pluggable storage backends

## Internal Architecture

```
src/
  lib.rs               Re-exports, pub use Engine, Error, Result
  error.rs             Error enum (thiserror)
  config.rs            CoreConfig + nested config structs
  engine.rs            StrataEngine — owns all subsystems
  memory/
    mod.rs             MemoryManager (cross-memory coordination)
    episodic.rs        WAL + time-indexed append-only store
    semantic.rs        USearch HNSW + metadata storage
    state.rs           KV MVCC (rusqlite + DashMap hot cache)
  query/
    mod.rs             Re-exports
    planner.rs         SQL → QueryPlan enum
    executor.rs        Plan execution across memories
    functions.rs       Custom SQL functions for DuckDB
  storage/
    mod.rs             StorageBackend trait
    local.rs           Local filesystem
    s3.rs              S3/MinIO via aws-sdk-s3
    tiering.rs         Hot/warm/cold data movement
  ingest/
    mod.rs             Re-exports
    pipeline.rs        IngestPipeline: receive → store → embed
    webhook.rs         Webhook schema normalization
  embedding/
    mod.rs             Re-exports
    provider.rs        EmbeddingProvider trait
    ollama.rs          Ollama HTTP client
    openai.rs          OpenAI API client
  materialized.rs      Incremental materialized view engine
```

## Testing

- Unit tests: `cargo test -p strata-core`
- Mock storage in tests: use temp directories with `LocalStorage`
- Mock embedding: create a test provider returning fixed-dimension zero vectors
- All memory stores have isolated tests

## Key Design Rules

- This crate must compile and test without any network access
- All public methods return `Result<T, Error>`
- Storage backends implement `StorageBackend` trait
- `StrataEngine` is Send + Sync (all state protected by Arc or lock-free structures)
