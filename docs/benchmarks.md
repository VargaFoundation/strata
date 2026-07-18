# Benchmarks

Criterion microbenchmarks for the hot paths. Run locally:

```bash
cargo bench -p ecphoria-core --bench engine
# quick pass: cargo bench -p ecphoria-core --bench engine -- --sample-size 10 --measurement-time 2
```

CI runs these on every PR (`.github/workflows/bench.yml`) and posts a comparison.

## Reference numbers

Indicative medians from one developer machine (WSL2, in-memory DuckDB/USearch/SQLite, optimized
`bench` profile, no embedding provider). **Absolute values are machine-specific** — use them for
relative/regression comparison, not as a spec.

| Benchmark | What it measures | Median |
|-----------|------------------|--------|
| `ingest_100_events` | Batch append of 100 **fresh** events to episodic (DuckDB Appender fast path) | ~5.9 ms |
| `query_select_100` | `SELECT … FROM episodic ORDER BY ts DESC LIMIT 100` over 500 rows | ~1.5 ms |
| `state_set_get` | One KV set + get (SQLite + hot cache) | ~44 µs |
| `semantic_search_k10` | HNSW k-NN over 200 vectors (768-d) | ~40 µs |
| `memory_add` | Cognition add: dedup / contradiction / importance + insert | ~9.4 ms |
| `memory_search_hybrid_k5` | Hybrid BM25 + recency/importance re-rank over 500 memories | ~5.1 ms |
| `graph_neighbors_hub` | 1-hop neighborhood of a 1000-edge hub entity | ~2.7 ms |

## Notes

- The semantic search path (USearch HNSW) is the fastest hot path (~40 µs) — vector retrieval is not
  the bottleneck.
- **Ingest uses the DuckDB Appender fast path** when no event in the batch carries an
  `idempotency_key` (~72× faster than row INSERTs: 429 ms → 5.9 ms / 100 events). Batches that need
  idempotency dedup fall back to `INSERT OR IGNORE` (the Appender has no conflict handling).
- `memory_add` is still row-insert bound (cognition does a contradiction-check SELECT + insert).
- With an embedding provider configured, `ingest` and `memory_add` additionally pay the provider's
  embedding latency (network-bound, not measured here).
- The benchmark suite covers the flagship cognition + graph paths so regressions surface in CI.
