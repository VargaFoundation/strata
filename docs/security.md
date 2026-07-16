# Security & Hardening

This guide covers Strata's security model and the knobs to harden a deployment. Strata is built
for authorized, self-hosted use. It is **secure by default in the one way that matters most**: it
**refuses to start unauthenticated on a non-loopback interface** (bind loopback, enable auth, or set
`gateway.allow_insecure=true` to accept the risk explicitly). See [threat-model.md](threat-model.md)
for the trust boundaries and the guarantees Strata does / does not make; **production deployments
must still enable the controls below.**

## Threat model in one line

Strata holds an agent's memory (events, vectors, state, distilled memories) for one or many
tenants. The assets to protect are: (1) per-tenant data confidentiality/integrity, (2) the Raft
cluster's integrity, (3) secrets (JWT/API keys, provider keys).

## Authentication & authorization

- **Enable auth** (`gateway.auth_enabled = true`). Strata then requires a Bearer token on
  `/api/v1/*`, `/mcp`, and `/v1/chat/completions`. It supports **API keys**, **JWT (HS256)**, and
  **OIDC (RS256/JWKS)**, with RBAC roles (admin/writer/reader/agent).
- **RBAC is enforced on both REST and gRPC.** A Reader token may read but not write on either
  protocol (writes → `403` on REST, `PermissionDenied` on gRPC); `/admin/*` requires the Admin role.
- **API keys can be scoped.** An `api_keys` entry may be `"<key>"` (Writer, no tenant — legacy),
  `"<key>@<tenant>"`, or `"<key>@<tenant>:<role>"`. The client always presents just the secret
  (the part before `@`); the tenant and role are bound to it server-side. Prefer JWT/OIDC for
  per-user tenancy; scoped API keys suit fixed service identities.
- **API keys are hashed at rest.** Keys are held only as SHA-256 digests and compared in constant
  time; a presented token never hits a timing-variable lookup. Provide keys pre-hashed as
  `"sha256:<64-hex>@<tenant>:<role>"` so **no plaintext credential sits in your config/secrets**
  (`KEY=$(openssl rand -hex 32); echo -n "$KEY" | sha256sum`). Plaintext entries still work (hashed
  at load).
- **Rate limiting** is a token bucket keyed per **(identity, tenant)** (`gateway.rate_limit_per_key`),
  so one noisy tenant on a shared key can't exhaust another's budget.
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
memory-RAG by the authenticated tenant. gRPC RPCs are likewise tenant-scoped **and RBAC-enforced**.
Cross-tenant leak tests live in `tests/integration/tests/tenant_isolation.rs`.

The **audit log** records each request's tenant and client IP (`X-Forwarded-For`/`X-Real-IP`);
`GET /api/v1/admin/audit?tenant=<t>&since=<iso>` filters by tenant (and aggregates across shards).

## Secrets

- Provider/API secrets support the **`_FILE` convention** (e.g. `ANTHROPIC_API_KEY_FILE`,
  `OPENAI_API_KEY_FILE`, and core secrets via `resolve_secret`) so you can mount Kubernetes/Docker
  secrets as files instead of plaintext env vars.
- `jwt_secret` and `api_keys` come from config/env; never commit them. Redacted in debug output.

## Webhook ingestion

Webhook signatures are verified **per vendor**, over the raw body, constant-time:

- **GitHub** (and unknown sources): `X-Hub-Signature-256: sha256=<hmac>`.
- **Slack**: `X-Slack-Signature: v0=<hmac>` over `v0:{ts}:{body}`, with a ±5-minute replay window on
  `X-Slack-Request-Timestamp`.
- **Sentry**: `Sentry-Hook-Signature: <hmac>` (raw hex).
- **PagerDuty**: `X-PagerDuty-Signature: v1=<hmac>[,…]` (any match passes).

Configure secrets per source (`gateway.webhook_secrets = ["github=…","slack=…"]`) and/or a global
`gateway.webhook_secret` fallback. Set `gateway.webhook_require_signature = true` to **fail closed** —
reject any webhook to a source with no configured secret. This matters because webhook events flow
into episodic memory → memory distillation → auto-RAG; an unauthenticated forger could otherwise
**poison memory** (see [threat-model.md](threat-model.md#memory-poisoning)).

## Prompt injection / auto-RAG

The LLM proxy retrieves memories/events and injects them into the completion. Retrieved content
originates from arbitrary prior ingestion and is therefore **untrusted**: it is injected into the
**user turn** inside a delimited `<strata_retrieved_context>` block with a framing instruction to
treat it as data — **never** into the system message, so an ingested "ignore previous instructions…"
cannot gain instruction rank. The response cache is keyed by **(tenant, user, retrieved-context
fingerprint)**, so a RAG-augmented answer is never replayed across users or after the underlying
memories change; vector-similarity cache hits are opt-in (`gateway.llm_cache_similarity`).

## Tool gateway (SSRF)

Registering a downstream MCP server validates the URL shape; **each outbound tool call** then
resolves the host and rejects blocked address ranges (SSRF guard) *before* connecting, and does not
follow redirects. Loopback/private/CGNAT/ULA targets are blocked unless
`gateway.tool_gateway_allow_private_networks = true`; **link-local (incl. the 169.254.169.254 cloud-
metadata endpoint), unspecified and multicast are always blocked.**

## Backup integrity

`backup()` writes a `manifest.json` with per-artifact SHA-256 checksums, store counts, the Strata
version and a capture timestamp; `restore_from_backup()` **verifies the checksums first** and
refuses a corrupted/truncated backup. Note the four store exports are not wrapped in a single global
write barrier, so a backup taken under concurrent writes may be fuzzy at the edges — for a strict
point-in-time image, quiesce writes or snapshot the data volume (Raft restore replays the log to a
consistent point regardless).

## Supply chain

Release images are **signed with cosign (keyless, GitHub OIDC)** and carry a **SLSA build-provenance
attestation** plus an **SPDX SBOM** attestation (also attached to the GitHub Release). Verify before
deploying:

```bash
# Signature — issuer is GitHub Actions, identity is this repo's release workflow.
cosign verify ghcr.io/vargafoundation/strata:<tag> \
  --certificate-identity-regexp 'https://github.com/VargaFoundation/strata/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

# SBOM attestation.
cosign verify-attestation --type spdxjson ghcr.io/vargafoundation/strata:<tag> \
  --certificate-identity-regexp 'https://github.com/VargaFoundation/strata/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

# Build provenance (SLSA).
gh attestation verify oci://ghcr.io/vargafoundation/strata:<tag> --repo VargaFoundation/strata
```

CI also runs `cargo-audit` (RUSTSEC advisories) and `cargo-deny` (license/ban policy) on every build.

## Cluster (Raft) security

- **Inter-node authentication:** set `STRATA_CLUSTER__SECRET` to require a shared Bearer token on
  every Raft RPC. A node without the token is rejected (constant-time check), so an unauthorized
  node cannot inject AppendEntries/Vote and corrupt the log/state machine. **Set this for any
  multi-node deployment.** It supports the **`_FILE` convention** (`STRATA_CLUSTER__SECRET_FILE`),
  so you can mount it from a Kubernetes/Docker secret instead of a plaintext env var.
- **Encryption in transit:** the Raft gRPC transport is HTTP/2 cleartext. For confidentiality, run
  inter-node traffic over a **service mesh / mTLS** (Istio, Linkerd) or a private network. (App-level
  TLS is a future option; the shared secret above gives authentication today.)
- The Raft log + wire format are binary (MessagePack). On a version upgrade that changes the format,
  **wipe each node's Raft data dir** (the log rebuilds from the leader/snapshot).
- **Certificate rotation:** tonic builds its TLS config **once at startup**, so rotating the Raft
  certs is a **rolling-restart** concern, not in-process hot reload. Recommended: cert-manager renews
  the TLS Secret + [Stakater Reloader](https://github.com/stakater/Reloader) restarts the pods on
  Secret change. The chart exposes `podAnnotations` for this (set `reloader.stakater.com/auto: "true"`).

## Sharded mode (multi-shard write scaling)

- Writes route **by tenant** to the owning shard; a tenant's data lives entirely on one shard.
  **REST + MCP + the LLM proxy** are shard-routed at the gateway. **gRPC (:9432) and the PostgreSQL
  wire (:5432) are NOT shard-routed** — connect those clients to the owning shard's service directly
  (or route by tenant at a gRPC/L4-aware load balancer). Admin endpoints are served locally;
  `/admin/audit` aggregates across shards.
- The shard ring (`cluster.shards`) and per-shard URLs (`shard_base_urls`) must be **uniform across
  the fleet** so every pod hashes tenants to the same shard. The Helm chart sets these.

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
- [ ] API keys provided **pre-hashed** (`sha256:<hex>@tenant:role`) so no plaintext secret at rest.
- [ ] `STRATA_CLUSTER__SECRET` set on every node (multi-node).
- [ ] Webhook secrets set per source (`webhook_secrets`) and `webhook_require_signature=true`.
- [ ] `tool_gateway_allow_private_networks` left **false** unless downstream MCP servers are on a
      trusted private network.
- [ ] Secrets mounted via `_FILE` / k8s Secrets, not plaintext env.
- [ ] `:5432` (PG wire) and `:9433` (Raft) NOT exposed publicly; NetworkPolicy restricting pod-to-pod
      and ingress. (PG wire carries the API key as the password — **needs TLS or a private network**.)
- [ ] Pod `securityContext`: `runAsNonRoot`, drop ALL capabilities, `readOnlyRootFilesystem` where
      possible; resource requests/limits set.
- [ ] mTLS via mesh for inter-node + client traffic (or TLS-terminating ingress).
- [ ] `allow_insecure` is **false** (default); the server refuses unauthenticated public binds.
- [ ] Durable `audit_db_path` on a PVC; CI runs the RUSTSEC `cargo-audit` job.
- [ ] Restore drills: confirm `restore_from_backup` passes manifest verification on your backups.
