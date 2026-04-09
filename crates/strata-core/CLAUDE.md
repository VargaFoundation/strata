# strata-core

## Responsibility

Core engine crate. Contains all business logic: three memory stores (episodic,
semantic, state), query execution, storage backends, ingestion pipeline,
and embedding providers. Has ZERO knowledge of protocols (HTTP, PG wire, gRPC).

## Implementation Status

| Component | Status | Backend |
|-----------|--------|---------|
| `EpisodicStore` | **Working** | DuckDB in-memory, INSERT/SELECT/COUNT, time-range + source queries |
| `SemanticStore` | **Working** | USearch HNSW, upsert/search/delete, cosine similarity, DashMap metadata |
| `StateStore` | **Working** | rusqlite + DashMap hot cache, full CRUD + CAS + list_keys |
| `LocalStorage` | **Working** | tokio::fs, put/get/delete/list with tempfile tests |
| `IngestPipeline` | **Working** | Validates and appends to EpisodicStore |
| `StrataEngine` | **Working** | Wires all 3 memories + ingest, exposes full public API |
| `OllamaProvider` | **Working** | HTTP POST to Ollama /api/embed |
| `OpenAiProvider` | **Working** | HTTP POST to OpenAI /v1/embeddings |
| `S3Storage` | Stub | aws-sdk-s3 integration pending |
| `QueryPlanner` | Stub | SQL parsing/routing pending |
| `MaterializedViews` | Stub | DuckDB views pending |

## Public API Surface

### StrataEngine (main entry point)

**Episodic Memory:**
- `ingest(events)` → stores in DuckDB via pipeline
- `query_sql(sql)` → executes raw SQL, returns JSON rows
- `query_by_source(source, limit)` → filtered event query
- `event_count()` → total event count

**Semantic Memory:**
- `semantic_upsert(entry)` → add/update vector entry
- `semantic_search(vector, k)` → k-NN search, returns scored results
- `semantic_delete(id)` → remove entry
- `semantic_count()` → entry count

**State Memory:**
- `state_get(agent_id, key)` → get value
- `state_set(agent_id, key, value)` → set value, returns version
- `state_delete(agent_id, key)` → delete key
- `state_list_keys(agent_id)` → list all keys

## Testing

- Unit tests: `cargo test -p strata-core` (83 tests)
- LocalStorage tests use `tempfile::TempDir` for isolation
- EpisodicStore tests use in-memory DuckDB
- SemanticStore tests use in-memory USearch with dimension=4 for speed
- StateStore tests use in-memory SQLite
- All tests run without network access
