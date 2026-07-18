# Ecphoria sharding operator

A Kubernetes controller (kube-rs) that reconciles a `EcphoriaShardPlan` custom resource: it keeps the
number of shard StatefulSets equal to `spec.shards` and, after scaling, drives per-tenant data moves
so each tenant lives on its consistent-hash-owning shard (by calling each shard's
`POST /api/v1/admin/rebalance`).

## Status (read this)

- Kept **outside the Cargo workspace** (its own empty `[workspace]` table) — heavy k8s deps, and its
  runtime can only be exercised against a real cluster.
- **What IS verified:** this crate **compiles** (kube 0.95 / k8s-openapi 0.23), `clippy` is clean, and
  the decision logic (`reconcile_moves`) has a passing unit test mirroring the workspace's unit-tested
  `ecphoria_cluster::{reconcile_plan, scale_plan}` (on main).
- **What IS now implemented (live apply loop):** scale-**up** creates the new shard StatefulSets by
  cloning `<release>-shard-0`'s spec and setting each one's `ECPHORIA_CLUSTER__SHARD_INDEX`
  (server-side apply); scale-**down** deletes the drained shard StatefulSets; tenant **rebalance
  moves** are driven via each shard's `POST /api/v1/admin/rebalance`. The order is safe — up:
  create-then-move; down: **drain-then-delete** (never lose data).
- **Verified end-to-end on a real cluster (Docker Desktop, k8s 1.34):** applying a `EcphoriaShardPlan`
  with `shards: 2` made the controller clone `<release>-shard-0` into `<release>-shard-1` (with
  `ECPHORIA_CLUSTER__SHARD_INDEX=1`) within ~1s; patching back to `shards: 1` deleted the drained
  StatefulSet. Only pod rollout timing depends on your cluster/images.

## Build / run

```bash
cd ops/operator
cargo build --release
# Install the CRD (the operator can emit it):
./target/release/ecphoria-operator --crd | kubectl apply -f -
# Run the controller — in-cluster (pod ServiceAccount) or with a local kubeconfig:
./target/release/ecphoria-operator
```

Apply the CRD + a plan (sketch):

```yaml
apiVersion: ecphoria.io/v1
kind: EcphoriaShardPlan
metadata:
  name: prod
spec:
  shards: 4
  release: ecphoria
  shard_base_urls:                       # snake_case — matches the CRD (`ecphoria-operator --crd`)
    - http://ecphoria-shard-0-headless:8432
    - http://ecphoria-shard-1-headless:8432
    - http://ecphoria-shard-2-headless:8432
    - http://ecphoria-shard-3-headless:8432
  admin_token_secret:                    # preferred over an inline admin_token
    name: ecphoria-admin
    key: admin-token
```

Tenants are discovered from shard 0 via `SELECT DISTINCT tenant_id` (falling back to a
`ecphoria.io/tenants` annotation on the plan if the query is unavailable).

## Deploy to Kubernetes

Manifests live in [`deploy/`](deploy/): the CRD, a `ecphoria-system` Namespace, a least-privilege
ServiceAccount + ClusterRole + binding (watch `EcphoriaShardPlan`; create/patch/delete shard
StatefulSets; read Secrets; hold a leader-election Lease; emit events), and the controller Deployment
(hardened securityContext, `RollingUpdate`). It is **leader-elected** (a `coordination.k8s.io` Lease),
so bumping `replicas` for HA is safe — only the lease holder reconciles.

```bash
# 1) Build + push the operator image (standalone crate; build context is ops/operator):
docker build -t ghcr.io/vargafoundation/ecphoria-operator:latest ops/operator
docker push ghcr.io/vargafoundation/ecphoria-operator:latest

# 2) Apply the CRD + RBAC + controller in one shot:
kubectl apply -k ops/operator/deploy

# 3) Apply a EcphoriaShardPlan (see the sketch above) into the namespace with your shard StatefulSets.
```

The CRD in `deploy/crd.yaml` is the static equivalent of `ecphoria-operator --crd`; regenerate it from
the binary if `ShardPlanSpec` changes.

## Production-readiness — status

- **Leader election** (`coordination.k8s.io` Lease) — **done**, verified end-to-end on a live cluster
  (a second replica waits; on the holder's crash, another acquires after the TTL). Race-safe via
  `replace` + resourceVersion.
- **`admin_token` from a Secret** (`admin_token_secret`) — **done** (RBAC grants `secrets: get`).
- **Tenant discovery via SQL** (`SELECT DISTINCT tenant_id`) — **done** (annotation is the fallback).
- **Graceful Lease release on `SIGTERM`** — **done**, verified live: on the holder's SIGTERM a standby
  took over in ~6 s (vs the 15 s TTL).
- **Kubernetes Events** for reconcile outcomes (`ScaledUp` / `ScaledDown` / `Reconciled`) — **done**,
  visible in `kubectl describe ecphoriashardplan` / `kubectl get events`.
