# strata-core

## Responsibility

Core engine crate. Contains all business logic: three memory stores (episodic,
semantic, state), query execution, storage backends, ingestion pipeline,
and embedding providers. Has ZERO knowledge of protocols (HTTP, PG wire, gRPC)
or clustering (Raft).

## Implementation Status

| Component | Status | Backend |
|-----------|--------|---------|
| `EpisodicStore` | **Working** | DuckDB file-backed or in-memory, connection pool (4 readers via try_clone), batch transactions, typed schema (TIMESTAMPTZ, JSON), SQL injection protection (sqlparser SELECT whitelist), configurable max_rows pagination, session support (sessions table), tenant_id column for multi-tenancy |
| `SemanticStore` | **Working** | USearch HNSW, upsert/search/delete, cosine similarity, persistent save/load to disk, memory-efficient EntryMetadata (no vector duplication in DashMap), `load_from()` for Raft snapshot restore |
| `StateStore` | **Working** | rusqlite + DashMap hot cache, full CRUD + CAS + list_keys, race-safe cache population, WAL mode + NORMAL sync for crash safety, TTL expiry |
| `LocalStorage` | **Working** | tokio::fs, put/get/delete/list with tempfile tests |
| `IngestPipeline` | **Working** | Validates -> appends to EpisodicStore -> auto-embed (batched by config.batch_size) -> upsert to SemanticStore. Embedding failures are non-fatal |
| `StrataEngine` | **Working** | Wires all 3 memories + ingest + embedding provider (auto-instantiated from config), async query_sql with spawn_blocking + timeout, session lifecycle, tenant-aware ingest, per-source retention policies |
| `OllamaProvider` | **Working** | HTTP POST to Ollama /api/embed, auto-wired from config |
| `OpenAiProvider` | **Working** | HTTP POST to OpenAI /v1/embeddings, auto-wired from config |
| `S3Storage` | **Working** | aws-sdk-s3, put/get/delete/list, MinIO-compatible |
| `MaterializedViews` | **Working** | DuckDB CREATE TABLE AS, refresh, drop, list, SQL-injection-safe (name validation + SELECT whitelist) |
| `QueryPlanner` | **Working** | Routes SQL to DuckDB, intercepts strata_search() and strata_state() calls, hybrid query rewriting via CTE injection for JOINs/subqueries |
| `TenantContext` | **Working** | Struct for multi-tenancy scoping, `resolve_secret()` helper for _FILE convention |

## Public API Surface

### StrataEngine (main entry point)

**Episodic Memory:**
- `ingest(events)` -> stores in DuckDB via pipeline, auto-embeds if provider configured
- `ingest_for_tenant(events, tenant)` -> tenant-scoped ingest (tags events with tenant_id)
- `query_sql(sql)` -> async, spawn_blocking, timeout, max_rows limit, SELECT-only
- `query_by_source(source, limit)` -> filtered event query
- `event_count()` -> total event count

**Semantic Memory:**
- `semantic_upsert(entry)` -> add/update vector entry
- `semantic_search(vector, k)` -> k-NN search, returns scored EntryMetadata results
- `semantic_search_filtered(vector, k, source, event_type)` -> filtered search
- `semantic_delete(id)` -> remove entry
- `semantic_count()` -> entry count
- `embed_text(text)` -> embed via configured provider
- `embed_and_search(text, k, source, event_type)` -> embed + search in one call

**State Memory:**
- `state_get(agent_id, key)` -> get value (cache-first, fallback to SQLite)
- `state_set(agent_id, key, value)` -> set value, returns version
- `state_delete(agent_id, key)` -> delete key
- `state_list_keys(agent_id)` -> list all keys (limited)
- `state_subscribe()` -> broadcast receiver for change notifications

**Sessions:**
- `session_start(session_id, agent_id, parent, metadata)` -> start conversation session
- `session_end(session_id, summary)` -> end session with optional summary
- `session_get(session_id)` -> get session details
- `session_list(agent_id, limit)` -> list sessions for an agent
- `session_recall(session_id)` -> recall all events in a session

**Retention:**
- `enforce_retention()` -> delete old events (per-source policies + default)
- `retention_policies()` -> list per-source policies
- `set_retention_policy(source, days)` -> set per-source retention
- `remove_retention_policy(source)` -> remove per-source policy

**Config:**
- `resolve_secret(name)` -> read secret from env var or _FILE path
- `TenantContext::new(tenant_id)` -> create tenant context for scoped operations

## Testing

- Unit tests: `cargo test -p strata-core` (112 tests)
- LocalStorage tests use `tempfile::TempDir` for isolation
- EpisodicStore tests use both in-memory and file-backed DuckDB
- SemanticStore tests use in-memory USearch with dimension=4 for speed, plus save/load persistence test
- StateStore tests use in-memory SQLite with WAL mode
- All tests run without network access
