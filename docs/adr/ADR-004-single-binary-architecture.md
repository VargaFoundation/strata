# ADR-004: Single Binary Architecture

**Status**: Accepted  
**Date**: 2024-06-10  
**Author**: Ecphoria Core Team

## Context

AI agent infrastructure is already complex. A typical agent stack includes an LLM provider, a vector database, a cache, a message queue, an event store, and an orchestration layer. Each service has its own deployment, configuration, monitoring, and upgrade lifecycle.

Adding a "context lake" to this stack should reduce complexity, not increase it. The deployment experience matters as much as the feature set — if Ecphoria requires a Docker Compose file with 5 services just to get started, most developers will close the tab.

We need an architecture that:

- Gets from zero to working in one command (`docker run` or a single binary)
- Minimizes operational surface area (ports, configs, dependencies)
- Allows the subsystems (episodic, semantic, state) to share resources efficiently
- Still supports distributed deployment for production HA

### Alternatives Considered

**Microservices**: Separate services for episodic (DuckDB), semantic (vector search), state (KV store), and gateway (protocol routing). Maximum flexibility — each service scales independently. But the operational cost is high: 4+ containers, inter-service networking, distributed tracing between services, and a Docker Compose file before anything works. The "5-minute quickstart" becomes a "30-minute infrastructure setup."

**Plugin architecture**: A core binary with loadable plugins for each storage backend. Modular and extensible. But plugin interfaces are hard to stabilize, version compatibility between core and plugins creates a testing matrix, and the deployment story is fragile (missing plugin DLL = cryptic crash at startup).

**Shared libraries**: Core functionality in a library, with thin binaries for each protocol. Similar to microservices but linked at compile time. Still requires multiple processes and inter-process communication for the subsystems to talk to each other.

## Decision

Build Ecphoria as a **single binary** (`ecphoria-server`) that embeds all three storage engines and exposes all protocols from one process.

The binary includes:

- **DuckDB** (embedded) — episodic memory
- **USearch** (embedded) — semantic memory / vector search
- **SQLite** (embedded) — state KV store
- **axum** — HTTP server (REST API, MCP, LLM proxy, Prometheus metrics)
- **pgwire** — PostgreSQL wire protocol server
- **tonic** — gRPC server
- **openraft** — Raft consensus (when cluster mode is enabled)

All subsystems share the same Tokio runtime, the same configuration file, and the same process lifecycle (graceful shutdown signal propagates to all components).

## Consequences

### Positive

- **`docker run` and done**: One image, one container, four ports (HTTP, PG wire, gRPC, Raft). A developer can have a working context lake in under 30 seconds. No Docker Compose, no dependency graph, no waiting for health checks between services.
- **Shared memory**: The episodic store, semantic index, and state cache share the same address space. The ingest pipeline can embed events and upsert vectors without serialization or network hops. This is a meaningful performance advantage for the embed-and-search path.
- **Single configuration**: One TOML file (or environment variables) configures everything. No per-service configs, no service discovery, no config sync.
- **Simple deployment**: Kubernetes Helm chart deploys a single StatefulSet. No operator, no sidecar, no init container (beyond optional Ollama for embeddings). Upgrades are a rolling restart.
- **Unified observability**: One `/metrics` endpoint, one log stream, one set of traces. No need to correlate across services to debug a slow query.

### Negative

- **Vertical scaling limits**: A single process can only use one machine's resources. For workloads exceeding one node's capacity, cluster mode (Raft) distributes the load, but sharding is not yet implemented. Very large deployments may need a different architecture.
- **All-or-nothing updates**: Upgrading the vector search engine also upgrades the SQL engine and the KV store. There's no way to update one component independently. Mitigated by semantic versioning and comprehensive integration tests.
- **Larger binary**: Embedding three storage engines plus four protocol servers produces a ~60MB binary. Acceptable for a server-side tool but larger than a typical microservice.
- **Memory contention**: DuckDB, USearch, and SQLite all manage their own memory. Under heavy load, they could compete for the same physical memory. Mitigated by configuring memory limits per subsystem and monitoring via Prometheus metrics.
- **Blast radius**: A bug in any subsystem (e.g., a DuckDB crash) takes down the entire process. Mitigated by running multiple replicas in cluster mode — Raft ensures other nodes continue serving while the crashed node restarts.
