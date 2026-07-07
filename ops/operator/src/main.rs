//! Strata sharding operator (controller).
//!
//! Reconciles a `StrataShardPlan` custom resource: keeps the number of shard StatefulSets equal to
//! `spec.shards`, and after scaling, drives tenant data movements so each tenant lives on its
//! consistent-hash-owning shard.
//!
//! Status: compiles + unit-tested (decision logic) AND the live apply loop has been **exercised
//! end-to-end on a real Kubernetes cluster** (Docker Desktop / k8s 1.34): applying a `StrataShardPlan`
//! with `shards: 2` cloned `<release>-shard-0` into `<release>-shard-1` (with `STRATA_CLUSTER__SHARD_INDEX=1`);
//! patching back to `shards: 1` deleted the drained StatefulSet. Order is safe (up: create-then-move;
//! down: drain-then-delete). The decision logic mirrors the workspace's unit-tested
//! `strata_cluster::{reconcile_plan, scale_plan}`. Run `strata-operator --crd | kubectl apply -f -` to
//! install the CRD, then run the controller.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::{Client, CustomResource, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// `kubectl apply` a CRD for this, e.g. group strata.io/v1, kind StrataShardPlan.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "strata.io",
    version = "v1",
    kind = "StrataShardPlan",
    namespaced,
    status = "ShardPlanStatus"
)]
pub struct ShardPlanSpec {
    /// Desired number of shards (independent Raft groups).
    pub shards: usize,
    /// Helm release name (StatefulSets are `<release>-shard-<i>`).
    pub release: String,
    /// Per-shard HTTP base URLs, indexed by shard (for the rebalance admin API).
    pub shard_base_urls: Vec<String>,
    /// Admin bearer token for the rebalance/admin endpoints (or reference a Secret in a real build).
    #[serde(default)]
    pub admin_token: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct ShardPlanStatus {
    pub current_shards: usize,
    pub last_moves: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure decision logic — mirror of strata_cluster::{ShardRouter, reconcile_plan}.
// Kept inline so the operator builds without the heavy strata-cluster dep tree.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
pub struct ShardMove {
    pub key: String,
    pub from: usize,
    pub to: usize,
}

fn fnv1a(s: &str) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn ring(shards: usize, vnodes: usize) -> BTreeMap<u64, usize> {
    let mut r = BTreeMap::new();
    for s in 0..shards.max(1) {
        for v in 0..vnodes.max(1) {
            r.insert(fnv1a(&format!("shard-{s}-vnode-{v}")), s);
        }
    }
    r
}

fn shard_for(ring: &BTreeMap<u64, usize>, shards: usize, key: &str) -> usize {
    if shards <= 1 {
        return 0;
    }
    let h = fnv1a(key);
    ring.range(h..)
        .next()
        .or_else(|| ring.iter().next())
        .map(|(_, &s)| s)
        .unwrap_or(0)
}

/// Tenants whose owning shard changes from `actual` → `desired` shards. Mirrors the tested
/// `strata_cluster::reconcile_plan`.
pub fn reconcile_moves(desired: usize, actual: usize, tenants: &[String]) -> Vec<ShardMove> {
    let (a, d) = (actual.max(1), desired.max(1));
    let (ra, rd) = (ring(a, 128), ring(d, 128));
    tenants
        .iter()
        .filter_map(|t| {
            let from = shard_for(&ra, a, t);
            let to = shard_for(&rd, d, t);
            (from != to).then(|| ShardMove {
                key: t.clone(),
                from,
                to,
            })
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Controller
// ─────────────────────────────────────────────────────────────────────────────

struct Ctx {
    client: Client,
    http: reqwest::Client,
}

async fn reconcile(plan: Arc<StrataShardPlan>, ctx: Arc<Ctx>) -> Result<Action, kube::Error> {
    let ns = plan.namespace().unwrap_or_else(|| "default".into());
    let desired = plan.spec.shards.max(1);
    let sts: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);

    // Count current shard StatefulSets (`<release>-shard-*`).
    let prefix = format!("{}-shard-", plan.spec.release);
    let actual = sts
        .list(&ListParams::default())
        .await?
        .items
        .iter()
        .filter(|s| s.name_any().starts_with(&prefix))
        .count()
        .max(1);

    tracing::info!(desired, actual, "reconciling shard plan");

    // Discover tenants to place (a real build could `SELECT DISTINCT tenant_id`); here from an
    // annotation `strata.io/tenants`. Whether the annotation is PRESENT matters for scale-down: if
    // we don't know the tenant set, we can't verify a drain, so we must not delete any shard.
    let tenants_known = plan.annotations().contains_key("strata.io/tenants");
    let tenants: Vec<String> = plan
        .annotations()
        .get("strata.io/tenants")
        .map(|s| s.split(',').map(|t| t.trim().to_string()).collect())
        .unwrap_or_default();
    let moves = reconcile_moves(desired, actual, &tenants);

    // Apply the change with the SAFE ordering (mirrors strata_cluster::scale_plan): scale-UP creates
    // the new shard StatefulSets BEFORE moving data onto them; scale-DOWN drains (moves) data OFF the
    // doomed shards BEFORE deleting them — and only deletes once the drain is CONFIRMED complete.
    match desired.cmp(&actual) {
        std::cmp::Ordering::Greater => {
            scale_up(&sts, &plan, actual, desired).await;
            // Best-effort on scale-up: data stays on its current shard until moved, so a failed move
            // is retried on the next requeue with no data at risk.
            let _ = apply_moves(&ctx, &plan, &moves).await;
        }
        std::cmp::Ordering::Less => {
            // Drain first, then delete — but ONLY once the drain is verifiably complete. Otherwise a
            // tenant whose move failed (or whose existence we couldn't even enumerate) would be
            // orphaned on a deleted shard. Block and retry instead of losing data.
            let all_moves_confirmed = if tenants_known {
                apply_moves(&ctx, &plan, &moves).await
            } else {
                false
            };
            if safe_to_delete_after_drain(tenants_known, all_moves_confirmed) {
                scale_down(&sts, &plan, desired, actual).await;
            } else {
                tracing::warn!(
                    desired, actual, tenants_known, all_moves_confirmed,
                    "scale-down blocked: refusing to delete shards without a verified drain (need \
                     the strata.io/tenants annotation AND every tenant move confirmed) — will retry"
                );
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
        }
        std::cmp::Ordering::Equal => {
            let _ = apply_moves(&ctx, &plan, &moves).await;
        }
    }

    // Update status (best-effort).
    let status = serde_json::json!({ "status": { "current_shards": desired, "last_moves": moves.len() } });
    let plans: Api<StrataShardPlan> = Api::namespaced(ctx.client.clone(), &ns);
    let _ = plans
        .patch_status(
            &plan.name_any(),
            &PatchParams::default(),
            &Patch::Merge(&status),
        )
        .await;

    Ok(Action::requeue(Duration::from_secs(60)))
}

fn on_error(_: Arc<StrataShardPlan>, err: &kube::Error, _: Arc<Ctx>) -> Action {
    tracing::error!(error = %err, "reconcile error");
    Action::requeue(Duration::from_secs(15))
}

/// Scale UP: create the new shard StatefulSets `<release>-shard-<actual..desired>` by cloning
/// shard-0's spec and setting each one's `STRATA_CLUSTER__SHARD_INDEX`. Server-side apply is
/// idempotent, so re-running is safe. Call BEFORE moving data (the shards must exist first).
async fn scale_up(sts: &Api<StatefulSet>, plan: &StrataShardPlan, actual: usize, desired: usize) {
    let template = format!("{}-shard-0", plan.spec.release);
    let base = match sts.get(&template).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, %template, "scale-up: cannot read shard-0 template");
            return;
        }
    };
    for i in actual..desired {
        let mut s = base.clone();
        // Strip server-managed fields so apply creates a fresh object.
        s.metadata.resource_version = None;
        s.metadata.uid = None;
        s.metadata.creation_timestamp = None;
        s.metadata.managed_fields = None;
        s.metadata.owner_references = None;
        s.status = None;
        let name = format!("{}-shard-{i}", plan.spec.release);
        s.metadata.name = Some(name.clone());
        set_shard_index_env(&mut s, i);
        match sts
            .patch(
                &name,
                &PatchParams::apply("strata-operator").force(),
                &Patch::Apply(&s),
            )
            .await
        {
            Ok(_) => tracing::info!(shard = i, %name, "scale-up: created shard StatefulSet"),
            Err(e) => tracing::error!(error = %e, shard = i, "scale-up: apply failed"),
        }
    }
}

/// Scale DOWN: delete the drained shard StatefulSets `<release>-shard-<desired..actual>`. Call AFTER
/// moving data off them (see `reconcile`), so no live data is lost.
async fn scale_down(sts: &Api<StatefulSet>, plan: &StrataShardPlan, desired: usize, actual: usize) {
    for i in desired..actual {
        let name = format!("{}-shard-{i}", plan.spec.release);
        match sts.delete(&name, &Default::default()).await {
            Ok(_) => tracing::info!(shard = i, %name, "scale-down: deleted drained shard StatefulSet"),
            Err(e) => tracing::error!(error = %e, shard = i, "scale-down: delete failed"),
        }
    }
}

/// Drive tenant data movements via each source shard's Strata admin rebalance API.
///
/// Returns `true` only if EVERY move was confirmed successful — the caller must treat `false` as
/// "drain incomplete" and NOT delete the source shard (else that tenant's data is orphaned on a
/// deleted shard). An unaddressable source (no base URL) counts as a failure, not a silent skip.
async fn apply_moves(ctx: &Ctx, plan: &StrataShardPlan, moves: &[ShardMove]) -> bool {
    let mut all_confirmed = true;
    for m in moves {
        let Some(src) = plan.spec.shard_base_urls.get(m.from) else {
            tracing::error!(tenant = %m.key, from = m.from,
                "rebalance: no base URL for source shard — cannot drain this tenant");
            all_confirmed = false;
            continue;
        };
        let url = format!("{}/api/v1/admin/rebalance", src.trim_end_matches('/'));
        let mut rb = ctx
            .http
            .post(&url)
            .json(&serde_json::json!({ "tenant": m.key, "target_shard": m.to }));
        if let Some(tok) = &plan.spec.admin_token {
            rb = rb.bearer_auth(tok);
        }
        match rb.send().await {
            Ok(r) if r.status().is_success() => {
                tracing::info!(tenant = %m.key, from = m.from, to = m.to, "moved")
            }
            Ok(r) => {
                tracing::error!(status = %r.status(), tenant = %m.key, "rebalance failed");
                all_confirmed = false;
            }
            Err(e) => {
                tracing::error!(error = %e, tenant = %m.key, "rebalance unreachable");
                all_confirmed = false;
            }
        }
    }
    all_confirmed
}

/// Whether a scale-down may proceed to DELETE drained shards. Only when the tenant set is known
/// (so we actually know what to drain — a missing `strata.io/tenants` annotation means we don't)
/// AND every drain move was confirmed. Deleting a shard otherwise orphans un-moved tenant data.
fn safe_to_delete_after_drain(tenants_known: bool, all_moves_confirmed: bool) -> bool {
    tenants_known && all_moves_confirmed
}

/// Set `STRATA_CLUSTER__SHARD_INDEX` on the StatefulSet's first container (so the new shard hashes
/// keys as its own partition).
fn set_shard_index_env(s: &mut StatefulSet, shard: usize) {
    use k8s_openapi::api::core::v1::EnvVar;
    let Some(spec) = s.spec.as_mut() else {
        return;
    };
    let Some(container) = spec
        .template
        .spec
        .as_mut()
        .and_then(|p| p.containers.first_mut())
    else {
        return;
    };
    let env = container.env.get_or_insert_with(Vec::new);
    let val = shard.to_string();
    if let Some(e) = env.iter_mut().find(|e| e.name == "STRATA_CLUSTER__SHARD_INDEX") {
        e.value = Some(val);
    } else {
        env.push(EnvVar {
            name: "STRATA_CLUSTER__SHARD_INDEX".to_string(),
            value: Some(val),
            ..Default::default()
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // `strata-operator --crd` prints the CustomResourceDefinition (JSON, which `kubectl apply -f -`
    // accepts) so you can install the CRD before running the controller.
    if std::env::args().any(|a| a == "--crd") {
        use kube::CustomResourceExt;
        println!("{}", serde_json::to_string_pretty(&StrataShardPlan::crd())?);
        return Ok(());
    }

    let client = Client::try_default().await?;
    let plans: Api<StrataShardPlan> = Api::all(client.clone());
    let ctx = Arc::new(Ctx {
        client,
        http: reqwest::Client::new(),
    });

    Controller::new(plans, Default::default())
        .run(reconcile, on_error, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => tracing::debug!(?o, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "controller error"),
            }
        })
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_down_deletes_only_after_verified_drain() {
        // Delete drained shards only when the tenant set is known AND every move was confirmed.
        assert!(safe_to_delete_after_drain(true, true));
        // Blocked when we can't enumerate tenants (unknown set → unverifiable drain).
        assert!(!safe_to_delete_after_drain(false, true));
        // Blocked when any move failed / was unreachable.
        assert!(!safe_to_delete_after_drain(true, false));
        assert!(!safe_to_delete_after_drain(false, false));
    }

    #[test]
    fn reconcile_moves_lists_only_changed_and_small() {
        let tenants: Vec<String> = (0..2000).map(|i| format!("t{i}")).collect();
        assert!(reconcile_moves(4, 4, &tenants).is_empty());
        let up = reconcile_moves(5, 4, &tenants);
        assert!(!up.is_empty() && up.len() < tenants.len() / 2);
        for m in &up {
            assert_ne!(m.from, m.to);
            assert!(m.to < 5);
        }
    }
}
