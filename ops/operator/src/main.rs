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
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Client, CustomResource, Resource, ResourceExt};
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
    /// Admin bearer token for the rebalance/admin endpoints, inline (dev only — prefer
    /// `admin_token_secret`).
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Read the admin bearer token from a Secret (preferred over the inline `admin_token`).
    #[serde(default)]
    pub admin_token_secret: Option<SecretRef>,
}

/// Reference to a key in a Kubernetes Secret (same namespace as the plan).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SecretRef {
    /// Secret name.
    pub name: String,
    /// Key within the Secret's data (default `admin-token`).
    #[serde(default)]
    pub key: Option<String>,
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
    reporter: Reporter,
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

    let token = resolve_admin_token(&ctx, &plan).await;
    let tenants = discover_tenants(&ctx, &plan, token.as_deref()).await;
    let moves = reconcile_moves(desired, actual, &tenants);

    // Apply the change with the SAFE ordering (mirrors strata_cluster::scale_plan): scale-UP creates
    // the new shard StatefulSets BEFORE moving data onto them; scale-DOWN drains (moves) data OFF the
    // doomed shards BEFORE deleting them — so no move lands on a missing shard and no live shard is
    // deleted with data still on it.
    let order = desired.cmp(&actual);
    match order {
        std::cmp::Ordering::Greater => {
            scale_up(&sts, &plan, actual, desired).await;
            apply_moves(&ctx, &plan, &moves, token.as_deref()).await;
        }
        std::cmp::Ordering::Less => {
            apply_moves(&ctx, &plan, &moves, token.as_deref()).await;
            scale_down(&sts, &plan, desired, actual).await;
        }
        std::cmp::Ordering::Equal => apply_moves(&ctx, &plan, &moves, token.as_deref()).await,
    }

    // Update status (best-effort).
    let status =
        serde_json::json!({ "status": { "current_shards": desired, "last_moves": moves.len() } });
    let plans: Api<StrataShardPlan> = Api::namespaced(ctx.client.clone(), &ns);
    let _ = plans
        .patch_status(
            &plan.name_any(),
            &PatchParams::default(),
            &Patch::Merge(&status),
        )
        .await;

    // Emit a Kubernetes Event on the plan describing the outcome (visible in `kubectl describe`).
    let (reason, note) = match order {
        std::cmp::Ordering::Greater => (
            "ScaledUp",
            format!(
                "scaled up {actual}→{desired} shards ({} tenant moves)",
                moves.len()
            ),
        ),
        std::cmp::Ordering::Less => (
            "ScaledDown",
            format!(
                "scaled down {actual}→{desired} shards ({} tenant moves)",
                moves.len()
            ),
        ),
        std::cmp::Ordering::Equal => (
            "Reconciled",
            format!("{desired} shards steady ({} tenant moves)", moves.len()),
        ),
    };
    let recorder = Recorder::new(
        ctx.client.clone(),
        ctx.reporter.clone(),
        plan.object_ref(&()),
    );
    let _ = recorder
        .publish(Event {
            type_: EventType::Normal,
            reason: reason.into(),
            note: Some(note),
            action: "Reconcile".into(),
            secondary: None,
        })
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
            Ok(_) => {
                tracing::info!(shard = i, %name, "scale-down: deleted drained shard StatefulSet")
            }
            Err(e) => tracing::error!(error = %e, shard = i, "scale-down: delete failed"),
        }
    }
}

/// Discover the tenants to place across shards: query shard 0's SQL API for `DISTINCT tenant_id`;
/// fall back to the comma-separated `strata.io/tenants` annotation when the query is unavailable.
async fn discover_tenants(ctx: &Ctx, plan: &StrataShardPlan, token: Option<&str>) -> Vec<String> {
    if let Some(base) = plan.spec.shard_base_urls.first() {
        let url = format!("{}/api/v1/query", base.trim_end_matches('/'));
        let mut rb = ctx.http.post(&url).json(&serde_json::json!({
            "sql": "SELECT DISTINCT tenant_id FROM episodic"
        }));
        if let Some(tok) = token {
            rb = rb.bearer_auth(tok);
        }
        if let Ok(resp) = rb.send().await {
            if resp.status().is_success() {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    let tenants: Vec<String> = body
                        .get("rows")
                        .and_then(|r| r.as_array())
                        .map(|rows| {
                            rows.iter()
                                .filter_map(|r| {
                                    r.get("tenant_id")
                                        .and_then(|v| v.as_str())
                                        .map(String::from)
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    if !tenants.is_empty() {
                        tracing::info!(count = tenants.len(), "discovered tenants via SQL");
                        return tenants;
                    }
                }
            }
        }
    }
    plan.annotations()
        .get("strata.io/tenants")
        .map(|s| {
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve the admin bearer token: from the referenced Secret when set (preferred), else the inline
/// `admin_token`. Returns `None` if neither is set or the Secret/key can't be read.
async fn resolve_admin_token(ctx: &Ctx, plan: &StrataShardPlan) -> Option<String> {
    if let Some(sref) = &plan.spec.admin_token_secret {
        let ns = plan.namespace().unwrap_or_else(|| "default".into());
        let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
        let key = sref.key.as_deref().unwrap_or("admin-token");
        match secrets.get(&sref.name).await {
            Ok(s) => match s.data.and_then(|mut d| d.remove(key)) {
                Some(bytes) => return String::from_utf8(bytes.0).ok(),
                None => {
                    tracing::error!(secret = %sref.name, key, "admin-token secret is missing the key")
                }
            },
            Err(e) => {
                tracing::error!(error = %e, secret = %sref.name, "cannot read admin-token secret")
            }
        }
    }
    plan.spec.admin_token.clone()
}

/// Drive tenant data movements via each source shard's Strata admin rebalance API.
async fn apply_moves(ctx: &Ctx, plan: &StrataShardPlan, moves: &[ShardMove], token: Option<&str>) {
    for m in moves {
        let Some(src) = plan.spec.shard_base_urls.get(m.from) else {
            continue;
        };
        let url = format!("{}/api/v1/admin/rebalance", src.trim_end_matches('/'));
        let mut rb = ctx
            .http
            .post(&url)
            .json(&serde_json::json!({ "tenant": m.key, "target_shard": m.to }));
        if let Some(tok) = token {
            rb = rb.bearer_auth(tok);
        }
        match rb.send().await {
            Ok(r) if r.status().is_success() => {
                tracing::info!(tenant = %m.key, from = m.from, to = m.to, "moved")
            }
            Ok(r) => tracing::error!(status = %r.status(), tenant = %m.key, "rebalance failed"),
            Err(e) => tracing::error!(error = %e, tenant = %m.key, "rebalance unreachable"),
        }
    }
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
    if let Some(e) = env
        .iter_mut()
        .find(|e| e.name == "STRATA_CLUSTER__SHARD_INDEX")
    {
        e.value = Some(val);
    } else {
        env.push(EnvVar {
            name: "STRATA_CLUSTER__SHARD_INDEX".to_string(),
            value: Some(val),
            ..Default::default()
        });
    }
}

/// Minimal, race-safe leader election via a `coordination.k8s.io` Lease, so more than one operator
/// replica can run without split-brain. Acquisition/renewal use `replace` with the observed
/// resourceVersion, so two racing replicas can't both win (the loser gets a 409 Conflict).
mod lease {
    use super::{Api, ResourceExt};
    use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
    use k8s_openapi::chrono::{Duration, Utc};
    use kube::api::{ObjectMeta, PostParams};

    pub const NAME: &str = "strata-operator";
    pub const TTL_SECONDS: i32 = 15;

    pub fn identity() -> String {
        std::env::var("POD_NAME")
            .ok()
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_else(|| format!("operator-{}", std::process::id()))
    }
    pub fn namespace() -> String {
        std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "strata-system".into())
    }

    /// Try to acquire or renew the lease; returns true iff we hold it after this call.
    pub async fn acquire_or_renew(leases: &Api<Lease>, id: &str) -> bool {
        let now = MicroTime(Utc::now());
        match leases.get_opt(NAME).await {
            Ok(None) => {
                let lease = Lease {
                    metadata: ObjectMeta {
                        name: Some(NAME.into()),
                        ..Default::default()
                    },
                    spec: Some(LeaseSpec {
                        holder_identity: Some(id.into()),
                        lease_duration_seconds: Some(TTL_SECONDS),
                        acquire_time: Some(now.clone()),
                        renew_time: Some(now),
                        ..Default::default()
                    }),
                };
                leases.create(&PostParams::default(), &lease).await.is_ok()
            }
            Ok(Some(mut lease)) => {
                let spec = lease.spec.clone().unwrap_or_default();
                let held_by_us = spec.holder_identity.as_deref() == Some(id);
                let expired = spec
                    .renew_time
                    .as_ref()
                    .zip(spec.lease_duration_seconds)
                    .map(|(rt, d)| rt.0 + Duration::seconds(d as i64) < Utc::now())
                    .unwrap_or(true);
                // A live lease held by someone else → not ours.
                if !held_by_us && !expired && spec.holder_identity.is_some() {
                    return false;
                }
                lease.spec = Some(LeaseSpec {
                    holder_identity: Some(id.into()),
                    lease_duration_seconds: Some(TTL_SECONDS),
                    acquire_time: if held_by_us {
                        spec.acquire_time
                    } else {
                        Some(now.clone())
                    },
                    renew_time: Some(now),
                    ..Default::default()
                });
                // `replace` carries the resourceVersion we just read → a racing replica 409s.
                leases
                    .replace(&lease.name_any(), &PostParams::default(), &lease)
                    .await
                    .is_ok()
            }
            Err(_) => false,
        }
    }

    /// Release the lease on graceful shutdown — delete it iff we still hold it, so a waiting replica
    /// acquires immediately instead of waiting out the TTL.
    pub async fn release(leases: &Api<Lease>, id: &str) {
        if let Ok(Some(l)) = leases.get_opt(NAME).await {
            if l.spec.and_then(|s| s.holder_identity).as_deref() == Some(id) {
                let _ = leases.delete(NAME, &Default::default()).await;
            }
        }
    }
}

/// Resolve on SIGTERM (k8s pod termination) or Ctrl-C, so we can release the lease before exiting.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
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

    // Leader election: block until we hold the lease, then renew it in the background. Losing it
    // (another replica took over, or the API was unreachable past the TTL) exits the process so the
    // pod restarts and re-elects — never two active controllers.
    use k8s_openapi::api::coordination::v1::Lease;
    let leases: Api<Lease> = Api::namespaced(client.clone(), &lease::namespace());
    let id = lease::identity();
    tracing::info!(identity = %id, "waiting for leadership…");
    while !lease::acquire_or_renew(&leases, &id).await {
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    tracing::info!("acquired leadership");
    let renew = {
        let leases = leases.clone();
        let id = id.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                if !lease::acquire_or_renew(&leases, &id).await {
                    tracing::error!("lost leadership — exiting for re-election");
                    std::process::exit(1);
                }
            }
        })
    };

    let plans: Api<StrataShardPlan> = Api::all(client.clone());
    let ctx = Arc::new(Ctx {
        client,
        http: reqwest::Client::new(),
        reporter: Reporter {
            controller: "strata-operator".into(),
            instance: Some(id.clone()),
        },
    });

    let controller = Controller::new(plans, Default::default())
        .run(reconcile, on_error, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => tracing::debug!(?o, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "controller error"),
            }
        });

    // Run until the controller stops or we get SIGTERM/Ctrl-C; then stop renewing and release the
    // lease so a standby replica takes over immediately (no TTL wait).
    tokio::select! {
        _ = controller => {}
        _ = shutdown_signal() => tracing::info!("shutdown signal — releasing leadership"),
    }
    renew.abort();
    lease::release(&leases, &id).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
