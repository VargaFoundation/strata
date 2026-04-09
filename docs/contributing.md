# Contributing to Strata

## Development Setup

### Prerequisites

- Rust 1.82+ (install via [rustup](https://rustup.rs))
- cmake, g++ (for native dependencies: DuckDB, USearch)
- Docker (optional, for integration testing)

### Clone and Build

```bash
git clone https://github.com/VargaFoundation/strata.git
cd strata
cargo build
```

First build takes several minutes (DuckDB and USearch compile from source).

### Run Tests

```bash
cargo test --workspace
```

### Run the Server

```bash
cargo run --bin strata-server
```

## Project Structure

```
Cargo.toml                 Workspace root
├── crates/
│   ├── strata-core/       Core engine (no protocol knowledge)
│   ├── strata-gateway/    Protocol layer (REST, PG wire, MCP, etc.)
│   ├── strata-cluster/    Distributed mode (Raft consensus)
│   └── strata-cli/        CLI admin tool
├── strata-server/         Main binary
├── tests/integration/     Integration tests
├── docs/                  Documentation
└── deploy/                Docker, Helm charts
```

Each crate has its own `CLAUDE.md` with specific guidance.

## Coding Conventions

### Error Handling

- Library crates: use `thiserror` with a crate-specific `Error` enum in `error.rs`
- Binary crates: use `anyhow` for top-level error handling
- Propagate errors with `?` — no `.unwrap()` in library code

### Async

- Tokio multi-threaded runtime
- All public async APIs use `async fn`
- Use `#[tokio::test]` for async tests

### Logging

- Use the `tracing` crate (not `log`)
- Levels: `error` (broken), `warn` (degraded), `info` (lifecycle), `debug` (flow), `trace` (data)
- Add `#[instrument]` to public functions

### Configuration

- TOML deserialization with `serde`
- All config structs derive `Debug, Clone, Deserialize` and implement `Default`
- Environment variable overrides via `STRATA_` prefix

### Testing

- Unit tests in `#[cfg(test)] mod tests` at the bottom of each file
- Integration tests in `tests/integration/`
- Run single crate: `cargo test -p strata-core`
- Run all: `cargo test --workspace`

### Naming

- `snake_case` for functions, variables, modules
- `PascalCase` for types, traits, enums
- `SCREAMING_SNAKE_CASE` for constants
- Crate prefix: `strata-` (Cargo) / `strata_` (Rust)

## Making Changes

### Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Make your changes
4. Ensure all checks pass:
   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```
5. Commit with a conventional commit message
6. Open a pull request

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(core): add time-range deletion for episodic store
fix(gateway): handle empty body in ingest endpoint
docs: add MCP configuration examples
test(integration): add REST API error handling tests
refactor(core): extract storage trait into separate module
```

### Crate Ownership

When making changes, identify which crate owns the feature:

| Change | Crate |
|--------|-------|
| New memory operation | `strata-core` |
| New REST endpoint | `strata-gateway` |
| New CLI command | `strata-cli` |
| Consensus logic | `strata-cluster` |
| Config loading | `strata-server` |
| New protocol handler | `strata-gateway` |

If a change touches the public API of `strata-core`, update both `strata-core` and `strata-gateway`.

### Adding a New Feature

1. Identify the owning crate
2. Add types/structs in the appropriate module
3. Write unit tests
4. If adding an API endpoint, add it in `strata-gateway/src/rest/`
5. If adding a CLI command, add it in `strata-cli/src/commands/`
6. Update the relevant `CLAUDE.md`
7. Run all checks before submitting

## Architecture Decisions

See [architecture.md](architecture.md) for detailed design documentation.

Key principles:
- **Dependencies flow downward**: core → gateway → server (never sideways)
- **Core has no protocol knowledge**: `strata-core` knows nothing about HTTP, gRPC, or MCP
- **Single binary**: everything compiles into one executable
- **Convention over configuration**: sensible defaults, minimal required config
