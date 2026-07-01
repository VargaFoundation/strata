# Strata sharding operator

A Kubernetes controller (kube-rs) that reconciles a `StrataShardPlan` custom resource: it keeps the
number of shard StatefulSets equal to `spec.shards` and, after scaling, drives per-tenant data moves
so each tenant lives on its consistent-hash-owning shard (by calling each shard's
`POST /api/v1/admin/rebalance`).

## Status (read this)

- Kept **outside the Cargo workspace** (its own empty `[workspace]` table) — heavy k8s deps, and its
  runtime can only be exercised against a real cluster.
- **What IS verified:** this crate **compiles** (kube 0.95 / k8s-openapi 0.23), `clippy` is clean, and
  the decision logic (`reconcile_moves`) has a passing unit test mirroring the workspace's unit-tested
  `strata_cluster::{reconcile_plan, scale_plan}` (on main).
- **What IS now implemented (live apply loop):** scale-**up** creates the new shard StatefulSets by
  cloning `<release>-shard-0`'s spec and setting each one's `STRATA_CLUSTER__SHARD_INDEX`
  (server-side apply); scale-**down** deletes the drained shard StatefulSets; tenant **rebalance
  moves** are driven via each shard's `POST /api/v1/admin/rebalance`. The order is safe — up:
  create-then-move; down: **drain-then-delete** (never lose data).
- **Verified end-to-end on a real cluster (Docker Desktop, k8s 1.34):** applying a `StrataShardPlan`
  with `shards: 2` made the controller clone `<release>-shard-0` into `<release>-shard-1` (with
  `STRATA_CLUSTER__SHARD_INDEX=1`) within ~1s; patching back to `shards: 1` deleted the drained
  StatefulSet. Only pod rollout timing depends on your cluster/images.

## Build / run

```bash
cd ops/operator
cargo build --release
# Install the CRD (the operator can emit it):
./target/release/strata-operator --crd | kubectl apply -f -
# Run the controller — in-cluster (pod ServiceAccount) or with a local kubeconfig:
./target/release/strata-operator
```

Apply the CRD + a plan (sketch):

```yaml
apiVersion: strata.io/v1
kind: StrataShardPlan
metadata:
  name: prod
  annotations:
    strata.io/tenants: "tenant-a,tenant-b,tenant-c"   # or have the operator discover via SQL
spec:
  shards: 4
  release: strata
  shardBaseUrls:
    - http://strata-shard-0-headless:8432
    - http://strata-shard-1-headless:8432
    - http://strata-shard-2-headless:8432
    - http://strata-shard-3-headless:8432
  adminToken: "<bearer>"   # use a Secret ref in a production build
```

## Remaining work for production

- Render + server-side-apply the shard StatefulSets/Services on scale-up (reuse the Helm template),
  and delete drained shards on scale-down.
- Discover tenants from the cluster (`SELECT DISTINCT tenant_id`) instead of an annotation.
- Read `adminToken` from a Secret; add RBAC + leader election.
