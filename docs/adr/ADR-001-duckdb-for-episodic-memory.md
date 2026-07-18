# ADR-001: DuckDB for Episodic Memory

**Status**: Accepted  
**Date**: 2024-06-15  
**Author**: Ecphoria Core Team

## Context

Ecphoria's episodic memory stores time-ordered events from AI agents — user interactions, tool calls, decisions, observations. The access pattern is append-heavy with analytical reads: agents rarely update past events but frequently query them with filters, aggregations, window functions, and time-range scans.

We need an embedded database (no external process) that handles:

- High-throughput appends (batched inserts)
- Analytical queries over millions of rows (GROUP BY, window functions, CTEs)
- Native JSON support (event payloads are semi-structured)
- TIMESTAMPTZ for global time-series data
- Zero operational overhead — no background vacuuming, compaction, or tuning

### Alternatives Considered

**SQLite**: Battle-tested embedded database, but designed for OLTP. Analytical queries on large datasets are slow — no columnar storage, no vectorized execution. JSON support exists but is bolted on. Would require a separate analytics layer.

**Embedded PostgreSQL**: Feature-rich but enormous dependency (~50MB), requires background processes (autovacuum, WAL writer), and is not designed for embedding. Initialization is slow and complex.

**Custom storage engine**: Maximum control but months of engineering for table stakes (query planning, type system, serialization). Not our core differentiator.

**ClickHouse/TimescaleDB**: Excellent analytics but require running as separate servers. Defeats the single-binary goal.

## Decision

Use **DuckDB** as the storage engine for episodic memory.

DuckDB is an embedded OLAP database with a columnar storage engine, vectorized query execution, and full SQL support. It runs in-process with zero external dependencies.

Implementation details:

- **Connection pool**: 1 writer + 4 reader connections via `spawn_blocking` to avoid blocking the Tokio runtime
- **File-backed or in-memory**: Configurable per deployment (`:memory:` for tests, file path for production)
- **Schema**: `episodic` table with `id UUID, source TEXT, event_type TEXT, payload JSON, ts TIMESTAMPTZ, parent_id UUID, trace_id TEXT, tags TEXT[]`
- **Batch inserts**: Events are appended in transactions for durability and throughput
- **SQL whitelist**: Only SELECT queries allowed from external clients (enforced via `sqlparser`)

## Consequences

### Positive

- **Zero operational overhead**: No external process, no background maintenance, no connection strings to configure
- **Fast analytical queries**: Columnar storage with vectorized execution handles millions of events efficiently
- **Standard SQL**: DuckDB supports a rich SQL dialect including JSON functions, window functions, CTEs, and TIMESTAMPTZ — no custom query language needed
- **Small binary impact**: DuckDB adds ~20MB to the binary, acceptable for the functionality it provides
- **Familiar interface**: The PG wire protocol exposes DuckDB directly — `psql`, Grafana, and any PG-compatible tool works out of the box

### Negative

- **Single-writer model**: DuckDB supports one concurrent writer, requiring a serialized write path. Mitigated by batching and the Raft consensus layer in cluster mode
- **Not designed for OLTP**: Point lookups by primary key are slower than SQLite/Postgres B-tree. Acceptable because episodic memory is read-mostly with analytical access patterns
- **Evolving project**: DuckDB is younger than SQLite or Postgres. API stability is good but not yet battle-tested at the same scale. Mitigated by pinning versions and wrapping behind our own storage trait
