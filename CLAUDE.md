# Strata — Claude Code Agent Guide

## What is Strata?

Strata is an open-source context lake for AI agents — a unified data layer combining
episodic memory (events), semantic memory (embeddings), and state memory (live key-value)
in a single Rust binary. PostgreSQL wire-compatible, MCP-native, deployable via Docker.

## Architecture Overview

Single binary (`strata-server`) with embedded DuckDB (analytics), USearch (vector HNSW),
custom WAL (append-only events), and SQLite B-tree (state KV). Exposes PostgreSQL wire
protocol on port 5432 and HTTP (REST + MCP + LLM proxy) on port 8432.

## Workspace Structure

```
Cargo.toml                 Workspace root
├── crates/
│   ├── strata-core/       Core engine: memories, query, storage, ingest, embedding
│   ├── strata-gateway/    Protocol layer: pg_wire, REST, gRPC, MCP, LLM proxy, auth
│   ├── strata-cluster/    Distributed: Raft consensus, replication, coordination
│   └── strata-cli/        CLI admin tool (binary: `strata`)
├── strata-server/         Main binary: `strata-server`
├── deploy/                Docker, Helm charts, K8s manifests
├── tests/                 Integration tests
└── docs/                  Documentation
```

## Crate Dependency Graph

```
strata-server (bin)
  ├── strata-gateway → strata-core
  ├── strata-cluster → strata-core
  └── strata-core

strata-cli (bin)
  └── strata-core (shared types only; talks to server via HTTP)
```

**Rule**: dependencies go DOWN. Never sideways (gateway must not depend on cluster).
Core has zero knowledge of gateway or cluster.

## Build & Test Commands

```bash
cargo fmt --all                                         # Format
cargo fmt --all -- --check                              # Check format (CI)
cargo clippy --workspace --all-targets -- -D warnings   # Lint
cargo test --workspace                                  # All tests
cargo test -p strata-core                               # Single crate tests
cargo build --release                                   # Release build
cargo run --bin strata-server                           # Run server
cargo run --bin strata -- status                        # Run CLI
```

## Coding Conventions

- **Error handling**: `thiserror` for library errors, `anyhow` only in binaries.
  Every crate has its own `Error` enum in `error.rs`. Propagate with `?`.
- **Async runtime**: Tokio (multi-thread). All public async APIs use `async fn`.
- **Logging**: `tracing` crate. Use `#[instrument]` on public functions.
  Levels: error (broken), warn (degraded), info (lifecycle), debug (flow), trace (data).
- **Configuration**: `serde` + TOML deserialization. Env var overrides via `STRATA_` prefix.
  Nested keys use double underscore: `STRATA_STORAGE__ENGINE=s3`.
- **Testing**: Unit tests in `#[cfg(test)] mod tests` at bottom of each file.
  Integration tests in `tests/` directory. Use `#[tokio::test]` for async tests.
- **Naming**: snake_case for functions/variables, PascalCase for types, SCREAMING_SNAKE for constants.
- **Dependencies**: Workspace-level version pinning in root Cargo.toml. Crates use `dep.workspace = true`.

## Adding a New Feature

1. Identify which crate owns the feature (core engine vs protocol vs cluster).
2. If touching the public API of `strata-core`, update both core and gateway.
3. Add types/structs in a new module or extend existing module.
4. Write unit tests first (`cargo test -p <crate>`).
5. If adding an API endpoint, add it in `strata-gateway/src/rest/` and document the route.
6. If adding a CLI command, add it in `strata-cli/src/commands/`.
7. Run `cargo clippy` and `cargo fmt` before considering work done.

## Key Dependencies

| Crate | Purpose | Used in |
|-------|---------|---------|
| tokio | Async runtime | All |
| axum | HTTP framework | gateway |
| pgwire | PostgreSQL wire protocol | gateway |
| duckdb | Embedded analytics SQL | core |
| usearch | HNSW vector index | core |
| rusqlite | Embedded SQLite for state KV | core |
| openraft | Raft consensus | cluster |
| serde/serde_json | Serialization | All |
| tracing | Structured logging | All |
| thiserror | Error derive | All libs |
| clap | CLI argument parsing | cli, server |
| reqwest | HTTP client | cli, core (embedding) |

## Environment Variables

All prefixed with `STRATA_`. Nested keys use `__`. Examples:
- `STRATA_STORAGE__DATA_DIR` — data directory (default: `./data`)
- `STRATA_STORAGE__ENGINE` — `local` or `s3`
- `STRATA_GATEWAY__LISTEN` — HTTP listen address (default: `0.0.0.0:8432`)
- `STRATA_GATEWAY__PG_LISTEN` — PG wire listen address (default: `0.0.0.0:5432`)
- `STRATA_EMBEDDING__PROVIDER` — `ollama` or `openai`
- `STRATA_EMBEDDING__OLLAMA_URL` — Ollama URL (default: `http://localhost:11434`)

## Parallel Development Guidelines

Each crate is designed for independent development by different agents:

- **strata-core**: No network dependencies for unit tests. Mock storage and embedding.
- **strata-gateway**: Depends on core via struct interfaces. Can mock engine for testing.
- **strata-cluster**: Depends on core. Can be tested with in-memory Raft.
- **strata-cli**: Pure HTTP client — test against mock HTTP server.
- **strata-server**: Thin wiring layer, minimal logic.

When working on a crate, read that crate's `CLAUDE.md` for specific guidance.
