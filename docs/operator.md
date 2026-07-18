# Ecphoria rebalancing operator (design)

Scaling write throughput means changing the number of shards (independent Raft groups). When the
shard count changes, a fraction of tenants must move to a different shard. A Kubernetes **operator**
automates this. This document describes its design and what Ecphoria already provides.

## What Ecphoria provides (built + tested)

- **Routing:** the gateway routes each request to its tenant's shard at runtime
  (`crates/ecphoria-gateway/src/cluster/shard_route.rs`); admin is served locally; `/admin/audit`
  scatter-gathers across shards.
- **Reconcile brain (pure, unit-tested):** `ecphoria_cluster::reconcile_plan(desired, actual, tenants)`
  returns a `ReconcilePlan { scale_to, moves: Vec<ShardMove> }` — the shard count to scale to and the
  exact tenant→shard movements (consistent hashing keeps the set small). This is the operator's core
  decision, testable without a cluster.
- **Data movement primitive:** `engine.migrate_tenant_memories_to(dest, tenant)` moves a tenant's
  memories between shard engines (and removes only those memories from the source — not its episodic
  events/state).
- **Helm:** `sharding.enabled` renders N StatefulSets `…-shard-<i>` + per-shard headless services,
  with `ECPHORIA_CLUSTER__SHARD_INDEX` / `__SHARD_BASE_URLS` per pod.

## Operator reconcile loop

```
watch the desired shard count (a CR field, e.g. EcphoriaCluster.spec.shards, or a ConfigMap value)
on change or periodically:
  actual  = count of `…-shard-*` StatefulSets
  tenants = list of active tenants (from any shard: `SELECT DISTINCT tenant_id`)
  plan    = reconcile_plan(desired, actual, tenants)     # the tested brain
  if plan.scale_to > actual:  create the new shard StatefulSets (+ services), wait until Ready
  for move in plan.moves:     migrate `move.key` from shard `move.from` to shard `move.to`
  if plan.scale_to < actual:  after migrations drain a shard, delete its StatefulSet
  update Helm `sharding.shards` / the CR status
```

The **migration step** is the one runtime piece still to wire for the deployed topology: the operator
(or an admin endpoint it calls) must drive `migrate_tenant_memories_to` *across pods* — e.g. export a
tenant's memories from the source shard's gateway and import them into the destination's, then remove
them from the source. The in-process `ShardedCluster::apply_moves` is the model; a cross-pod
export/import admin API is the remaining work (and a *full* tenant move should also relocate episodic
events + state, not just memories).

## Implementation options

- **kube-rs (Rust):** reuse `reconcile_plan` directly; controller-runtime via `kube`. Same language.
- **kubebuilder (Go):** call a Ecphoria admin endpoint for the plan/migrations.

## Honest status

The operator's **decision logic is built and unit-tested** (`reconcile_plan`), and the **routing +
data-movement primitives exist**. The controller binary (k8s API watches/patches) and the cross-pod
migration API are **not built here** — a controller can't be integration-tested without a live
cluster (kind/k3d/minikube), and shipping an unverified controller as "done" would be dishonest. This
doc + the tested brain are the foundation an operator is written against.
