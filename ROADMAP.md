# Roadmap

Ecphoria is an open-source agentic memory platform. This roadmap is directional, not a
commitment — priorities shift with feedback. File an issue to propose or reprioritize.

## Versioning & stability

Ecphoria is **pre-1.0 (`0.x`)**: minor versions may contain breaking changes to the API,
config, wire/Raft formats, and on-disk layout. We call out breaking changes in the
release notes. **API stability (SemVer with a deprecation policy) begins at `1.0`.**

## Now (shipping on `main`)

- **Secure by default** — refuses to start unauthenticated on a public bind; hashed API
  keys; per-vendor webhook signatures; SSRF-guarded tool gateway; `ecphoria doctor`.
- **Memory substrate** — bi-temporal memories, contradiction resolution, dedup, hybrid
  retrieval (BM25 + vector), decay, knowledge graph.
- **Cognition APIs** — provenance, feedback loop, CDC stream, HITL contradiction review,
  session distillation, semantic-cluster consolidation, cross-scope sharing (tenant-strict grants).
- **Protocols** — PostgreSQL wire (+TLS), REST, gRPC, MCP (incl. graph tools),
  LLM proxy: OpenAI `/v1/chat/completions` + `/v1/embeddings`, Anthropic `/v1/messages`.
- **Runtime** — durable agent runs, HITL approvals, DAG workflows, triggers, dispatcher.
- **Ops** — Docker/Compose/Helm, Raft HA, sharding + operator, cosign/SBOM releases.
- **Import** — Obsidian vault → memories + graph edges.

## Next (targeted)

- **In-process embeddings** (fastembed/ONNX) so the single binary needs no Ollama sidecar.
- **Embedded admin console** — memory browser, bi-temporal timeline, graph view,
  contradiction queue, key management.
- **Encryption at rest** — per-tenant envelope keys (KMS/age) for the on-disk stores.
- **ReBAC authz backend** — a pluggable policy backend (e.g. SpiceDB) on top of the grants
  primitive, for richer team/role-based sharing.
- **Published benchmarks** — reproducible LoCoMo baseline with the exact recipe.

## Later / exploring

- Advanced consolidation ("sleep-time" episodic→semantic compression).
- More importers (Mem0, Zep, Notion), outbound CDC to NATS/webhooks.
- OTLP traces/metrics export alongside Prometheus.

## Non-goals

- Being a general-purpose database — Ecphoria is a memory platform for agents.
- A hosted/managed offering in this repository (self-hosted first).
