# Security & Hardening

This guide covers Strata's security model and the knobs to harden a deployment. Strata is built
for authorized, self-hosted use; the defaults favor a quick local start, so **production deployments
must enable the controls below.**

## Threat model in one line

Strata holds an agent's memory (events, vectors, state, distilled memories) for one or many
tenants. The assets to protect are: (1) per-tenant data confidentiality/integrity, (2) the Raft
cluster's integrity, (3) secrets (JWT/API keys, provider keys).

## Authentication & authorization

- **Enable auth** (`gateway.auth_enabled = true`). Strata then requires a Bearer token on
  `/api/v1/*`, `/mcp`, and `/v1/chat/completions`. It supports **API keys**, **JWT (HS256)**, and
  **OIDC (RS256/JWKS)**, with RBAC roles (admin/writer/reader/agent) and a per-key token-bucket
  rate limiter (`gateway.rate_limit_per_key`).
- **Fail-closed:** with `auth_enabled = true` but no api_keys/jwt_secret/OIDC configured, the server
  **refuses to start** (it will not silently run unauthenticated). A `jwt_secret` shorter than 32
  bytes is rejected.
- **PostgreSQL wire (:5432)** uses a no-op startup handler — it is **not authenticated**. Do **not**
  expose :5432 outside a trusted network (bind it to localhost or a private subnet / NetworkPolicy).
- `/health`, `/ready`, `/metrics` are intentionally unauthenticated (probes / Prometheus scraping);
  restrict them with network policy.

## Multi-tenant isolation

Every read path is tenant-scoped when the caller presents a tenant-scoped JWT: SQL queries are
rewritten to a per-tenant view, `strata_state()`/`strata_search()` are namespaced/filtered, memory
get/delete/history are 404-on-mismatch, semantic search is tenant-filtered, and the LLM proxy scopes
memory-RAG by the authenticated tenant. gRPC RPCs are likewise tenant-scoped. Cross-tenant leak
tests live in `tests/integration/tests/tenant_isolation.rs`.

## Secrets

- Provider/API secrets support the **`_FILE` convention** (e.g. `ANTHROPIC_API_KEY_FILE`,
  `OPENAI_API_KEY_FILE`, and core secrets via `resolve_secret`) so you can mount Kubernetes/Docker
  secrets as files instead of plaintext env vars.
- `jwt_secret` and `api_keys` come from config/env; never commit them. Redacted in debug output.

## Webhook ingestion

Set `gateway.webhook_secret` to require a GitHub-style `X-Hub-Signature-256: sha256=<hmac>`
(HMAC-SHA256 over the raw body, constant-time verified) on `POST /api/v1/webhook/{source}`.
Unsigned/mis-signed webhooks are rejected (401). With no secret set, signatures are not checked —
only do that on a trusted network.

## Cluster (Raft) security

- **Inter-node authentication:** set `STRATA_CLUSTER__SECRET` to require a shared Bearer token on
  every Raft RPC. A node without the token is rejected (constant-time check), so an unauthorized
  node cannot inject AppendEntries/Vote and corrupt the log/state machine. **Set this for any
  multi-node deployment.**
- **Encryption in transit:** the Raft gRPC transport is HTTP/2 cleartext. For confidentiality, run
  inter-node traffic over a **service mesh / mTLS** (Istio, Linkerd) or a private network. (App-level
  TLS is a future option; the shared secret above gives authentication today.)
- The Raft log + wire format are binary (MessagePack). On a version upgrade that changes the format,
  **wipe each node's Raft data dir** (the log rebuilds from the leader/snapshot).

## Data lifecycle / compliance

- **Right to be forgotten:** `DELETE /api/v1/admin/tenants/{tenant_id}` (admin only) erases a
  tenant's data across **all** stores — episodic events + sessions, memories + their vectors, agent
  state, and event embeddings — returning a per-store summary.
- **Durable audit log:** set `gateway.audit_db_path` to a file path (default is file-backed) so the
  auth audit trail survives restarts. `:memory:`/empty = non-durable.
- **Retention:** per-source episodic retention + state TTL; memory decay-based forgetting.

## Resource limits

- Body limit 16 MB; per-batch ingest cap 10k events; SQL `query.max_rows` cap (default 10k), also
  applied to `memory_all`/`session_list`/`query_by_source` to prevent unbounded result sets.
- Connection limits on PG wire (semaphore) and a configurable DuckDB read pool
  (`...__READ_POOL_SIZE`).

## Deployment hardening checklist (Kubernetes)

- [ ] `auth_enabled=true` with real credentials (or OIDC); strong `jwt_secret` (≥32 bytes).
- [ ] `STRATA_CLUSTER__SECRET` set on every node (multi-node).
- [ ] `webhook_secret` set if using webhooks.
- [ ] Secrets mounted via `_FILE` / k8s Secrets, not plaintext env.
- [ ] `:5432` (PG wire) and `:9433` (Raft) NOT exposed publicly; NetworkPolicy restricting pod-to-pod
      and ingress.
- [ ] Pod `securityContext`: `runAsNonRoot`, drop ALL capabilities, `readOnlyRootFilesystem` where
      possible; resource requests/limits set.
- [ ] mTLS via mesh for inter-node + client traffic (or TLS-terminating ingress).
- [ ] Durable `audit_db_path` on a PVC; CI runs the RUSTSEC `cargo-audit` job.
