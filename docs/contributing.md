# Contributing to Ecphoria

## Development Setup

### Prerequisites

- Rust 1.82+ (install via [rustup](https://rustup.rs))
- cmake, g++ (for native dependencies: DuckDB, USearch)
- Docker (optional, for integration testing)

### Clone and Build

```bash
git clone https://github.com/VargaFoundation/ecphoria.git
cd ecphoria
cargo build
```

First build takes several minutes (DuckDB and USearch compile from source).

### Disk usage / build artifacts

Ecphoria links heavy native dependencies (bundled DuckDB, ONNX Runtime via the optional embedding
features, the AWS SDK, protobuf). `target/` grows fast, and across many builds + feature
combinations + the separate `ops/operator` workspace it can accumulate to **hundreds of GB** and
saturate a disk. Two guardrails keep this in check:

- **Smaller builds (automatic):** the root `Cargo.toml` dev profile drops debug info from
  *dependencies* (`[profile.dev.package."*"] debug = false`) and uses `split-debuginfo = "unpacked"`.
  Your own crates keep `line-tables-only` so backtraces still show file:line. A clean full build is
  a few GB and the server binary ~120 MB (vs ~1 GB unstripped).
- **Auto-clean over a cap:** build through the `Makefile` — `make build` / `make test` / `make check`
  run `scripts/target-guard.sh` first, which reclaims space (via `cargo sweep` if installed, else
  `cargo clean`) whenever `target/` exceeds a cap (default 40 GB, override with `CAP_GB=…`).
  - `make disk` reports current `target/` size (non-zero exit if over cap).
  - `make clean` wipes both workspaces' `target/`; `make clean-all` also removes stray test `data/` dirs.
  - Optional but recommended: `cargo install cargo-sweep` so the guard prunes only *stale* artifacts
    instead of doing a full clean.

### Run Tests

```bash
cargo test --workspace
```

### Run the Server

```bash
cargo run --bin ecphoria-server
```

## Project Structure

```
Cargo.toml                 Workspace root
├── crates/
│   ├── ecphoria-core/       Core engine (no protocol knowledge)
│   ├── ecphoria-gateway/    Protocol layer (REST, PG wire, MCP, etc.)
│   ├── ecphoria-cluster/    Distributed mode (Raft consensus)
│   └── ecphoria-cli/        CLI admin tool
├── ecphoria-server/         Main binary
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
- Environment variable overrides via `ECPHORIA_` prefix

### Testing

- Unit tests in `#[cfg(test)] mod tests` at the bottom of each file
- Integration tests in `tests/integration/`
- Run single crate: `cargo test -p ecphoria-core`
- Run all: `cargo test --workspace`

### Naming

- `snake_case` for functions, variables, modules
- `PascalCase` for types, traits, enums
- `SCREAMING_SNAKE_CASE` for constants
- Crate prefix: `ecphoria-` (Cargo) / `ecphoria_` (Rust)

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
| New memory operation | `ecphoria-core` |
| New REST endpoint | `ecphoria-gateway` |
| New CLI command | `ecphoria-cli` |
| Consensus logic | `ecphoria-cluster` |
| Config loading | `ecphoria-server` |
| New protocol handler | `ecphoria-gateway` |

If a change touches the public API of `ecphoria-core`, update both `ecphoria-core` and `ecphoria-gateway`.

### Adding a New Feature

1. Identify the owning crate
2. Add types/structs in the appropriate module
3. Write unit tests
4. If adding an API endpoint, add it in `ecphoria-gateway/src/rest/`
5. If adding a CLI command, add it in `ecphoria-cli/src/commands/`
6. Update the relevant `CLAUDE.md`
7. Run all checks before submitting

## Architecture Decisions

See [architecture.md](architecture.md) for detailed design documentation.

Key principles:
- **Dependencies flow downward**: core → gateway → server (never sideways)
- **Core has no protocol knowledge**: `ecphoria-core` knows nothing about HTTP, gRPC, or MCP
- **Single binary**: everything compiles into one executable
- **Convention over configuration**: sensible defaults, minimal required config

## Developer Certificate of Origin (DCO)

Contributions are accepted under the [DCO](https://developercertificate.org/): by
signing off, you certify that you wrote the change (or have the right to submit it) under
the project's Apache-2.0 license. Sign off every commit with `-s`:

```bash
git commit -s -m "your message"
```

This appends a `Signed-off-by: Your Name <you@example.com>` trailer. PRs whose commits
are not signed off will be asked to amend. This keeps the provenance of external
contributions unambiguous — important for an association-backed project.
