# Strata Threat Model

A 10-minute read for an architect evaluating Strata: what it is trusted to hold, where the trust
boundaries are, what it **guarantees**, and — just as important — what it **does not**. Pairs with
[security.md](security.md) (the how-to for each control).

## What Strata protects

Strata stores an AI agent's memory for one or many tenants:

- **Episodic** events (DuckDB) — logs, actions, webhook payloads.
- **Semantic** vectors (USearch) — embeddings for similarity search.
- **State** (SQLite) — live agent key/value state.
- **Memories** (bi-temporal cognition) — distilled facts, their history, and a knowledge graph.
- **Runs** — durable agent executions, traces, HITL approvals.

**Assets:** (1) per-tenant data confidentiality & integrity; (2) Raft cluster integrity; (3) secrets
(API keys, JWT secret, provider keys, cluster secret).

## Trust boundaries

```
             ┌──────────────────────── UNTRUSTED ────────────────────────┐
  clients →  │ REST :8432   gRPC :9432   PG-wire :5432   MCP   LLM proxy  │
  vendors →  │ webhooks                                                   │
             └───────────────┬───────────────────────────────────────────┘
                             │  (A) authentication + RBAC + tenant scoping
             ┌───────────────▼──────────── TRUSTED (in-process) ─────────┐
             │ StrataEngine: episodic · semantic · state · memories · runs│
             │  (B) SQL AST tenant-rewrite   (C) outbound SSRF guard      │
             └───────────────┬───────────────────────────────────────────┘
                             │  (D) Raft shared-secret auth (cleartext H2)
             ┌───────────────▼──────────── PEER NODES ───────────────────┐
             │ other Strata replicas (same fleet)                         │
             └───────────────────────────────────────────────────────────┘
   outbound: embedding/LLM providers, downstream MCP servers  ← (C)
```

- **(A) Client → gateway.** Every `/api/v1/*`, `/mcp`, `/v1/chat/completions` request needs a Bearer
  token when `auth_enabled`. RBAC (admin/writer/reader/agent) and tenant scoping are derived from the
  token, not from request arguments — a client cannot widen its own scope. The server **refuses to
  start unauthenticated on a non-loopback bind** unless `allow_insecure=true`.
- **(B) Tenant isolation.** Read paths are rewritten to per-tenant views via the SQL AST; internal
  views can't be addressed by name. This rewriter is the isolation boundary — treated as
  security-critical.
- **(C) Engine → outbound.** The tool gateway resolves and range-checks every downstream URL before
  connecting (SSRF guard); embedding/LLM providers are operator-configured.
- **(D) Node → node.** Raft RPCs require a shared secret (constant-time checked). Transport is
  cleartext HTTP/2 — confidentiality relies on a mesh/mTLS or a private network.

## What Strata guarantees

- **No unauthenticated public exposure by accident** — fail-closed startup guard.
- **Authn/authz** on REST **and** gRPC; `/admin/*` and `/cluster/*` are Admin-only.
- **Tenant isolation** on all read paths (SQL, memories, semantic, state, sessions), with regression
  tests in `tests/integration/tests/tenant_isolation.rs`.
- **Credential hygiene:** API keys hashed at rest (SHA-256) + constant-time compare; JWT secret
  minimum length enforced; `_FILE` secret mounting.
- **Deterministic replication:** non-deterministic work (embeddings, UUIDs, timestamps) runs once on
  the leader; only materialized rows go through Raft, so replicas converge (no failover divergence).
- **Bi-temporal integrity:** memory contradictions **and** semantic-dedup merges supersede (never
  hard-overwrite), so `history` / `as-of T` are exact on every write path.
- **Backup integrity:** manifest with SHA-256 checksums, verified before restore.
- **Prompt-injection containment:** retrieved memory is injected as delimited untrusted **user**-turn
  data, never as a system instruction; response cache scoped by (tenant, user, context).

## What Strata does NOT guarantee (know these)

- **PG wire (:5432) is unauthenticated at the protocol layer** — the password *is* the API key, but
  there is **no TLS on that listener today**, so the key crosses the network in cleartext. Bind it to
  loopback / a private subnet, or front it with TLS. Do not expose :5432 publicly.
- **Raft transport is cleartext** (authenticated, not encrypted). Use a mesh/mTLS or private network
  for inter-node confidentiality.
- **Backups are not strictly point-in-time** across all four stores (no global write barrier). Fine
  for Raft restore (log replay); for standalone DR, quiesce writes or snapshot the volume.
- **No encryption at rest** — DuckDB/SQLite/USearch files are plaintext on disk. Use encrypted
  volumes / disk-level encryption for data-at-rest requirements.
- **No cross-scope sharing / ACLs** — scoping is exact-match isolation `(tenant,user,agent,session)`.
  There is no "share this memory with a team" primitive yet.
- **The semantic response cache is per-node** in a cluster — nodes may briefly serve answers built
  from slightly different memory states (bounded by TTL).
- **Prompt-injection is contained, not eliminated** — framing untrusted context caps its authority at
  user rank, but a model may still be influenced. Don't grant an agent capabilities its memory
  sources shouldn't be able to trigger.

<a name="memory-poisoning"></a>
## Attack chain to keep in mind: memory poisoning

The highest-leverage end-to-end attack is **indirect**:

```
forge/ingest an event  →  distilled into a "memory"  →  auto-RAG injects it  →  influences a
(webhook or writer)        (cognition layer)              (LLM proxy)             future completion
```

Controls along the chain: **webhook signature verification** (per vendor, fail-closed) stops forged
events at ingress; **tenant scoping** limits blast radius to one tenant; **untrusted-data framing** in
auto-RAG caps the injected content's authority; **RBAC** limits who can write at all. Enable all four
in production.

## Surfaces & controls (quick map)

| Surface | Boundary | Primary control |
|---|---|---|
| REST/MCP/proxy :8432 | (A) | Bearer auth + RBAC + tenant scope; fail-closed startup |
| gRPC :9432 | (A) | Bearer JWT + RBAC + tenant scope |
| PG wire :5432 | (A) | password=API key → tenant scope; **no TLS — keep private** |
| Webhooks | (A) | per-vendor HMAC signatures; `webhook_require_signature` |
| SQL execution | (B) | AST rewrite to per-tenant view; SELECT-only whitelist |
| Tool gateway (outbound) | (C) | SSRF range-check per call; no redirects |
| Raft :9433 | (D) | shared-secret auth; cleartext H2 (mesh/mTLS for confidentiality) |
| Auto-RAG (LLM proxy) | in-proc | untrusted-data framing; scoped response cache |
| Backup/restore | in-proc | manifest SHA-256 verification |

## Reporting

See the repository's security policy for coordinated disclosure. Do not file exploitable issues in
the public tracker.
