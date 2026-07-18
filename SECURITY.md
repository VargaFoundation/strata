# Security Policy

## Supported versions

Ecphoria is pre-1.0 (`0.x`). Security fixes land on `main` and in the latest tagged
release. Until 1.0, only the most recent minor version receives fixes.

## Reporting a vulnerability

**Please do not open a public issue for a security vulnerability.**

Report privately via GitHub's [private vulnerability reporting](https://github.com/VargaFoundation/ecphoria/security/advisories/new)
(Security → Report a vulnerability), or email the maintainers listed in
[CODEOWNERS](.github/CODEOWNERS).

Include, where possible:

- affected component (memory substrate, gateway/protocol, cluster, CLI, an SDK)
- version / commit, and configuration relevant to the issue (auth mode, cluster, sharding)
- a reproduction or proof-of-concept, and the impact you observed

We aim to acknowledge within **3 business days** and to agree a disclosure timeline
with you. We credit reporters in the release notes unless you prefer to stay anonymous.

## Scope

In scope: the Ecphoria server (memory substrate, REST/gRPC/PG-wire/MCP/LLM-proxy), the
cluster/Raft layer, the CLI, the operator, and the official SDKs.

Out of scope: issues that require a misconfiguration Ecphoria explicitly warns against
(e.g. running with `gateway.allow_insecure=true` on a public network — see
[docs/threat-model.md](docs/threat-model.md)), third-party dependencies (report upstream;
we track advisories via `cargo-audit` in CI), and self-inflicted denial of service.

## Hardening

Before deploying, read [docs/threat-model.md](docs/threat-model.md) and
[docs/security.md](docs/security.md), and run `ecphoria doctor` to lint your config.
Release container images are signed (cosign) and ship an SBOM + SLSA provenance;
verification steps are in `docs/security.md`.
