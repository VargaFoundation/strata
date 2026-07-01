//! Admin commands — wrap the server's `/api/v1/admin/*` endpoints. These are admin-only, so pass an
//! admin token via `--token` / `STRATA_TOKEN` when the server has auth enabled.

use crate::client::StrataClient;
use crate::commands::{MemoryCmd, RetentionCmd, TenantCmd};

fn show(v: &serde_json::Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

pub async fn retention(url: &str, action: RetentionCmd) -> anyhow::Result<()> {
    let c = StrataClient::new(url);
    let res = match action {
        RetentionCmd::Enforce => {
            c.post_json("/api/v1/admin/retention", serde_json::json!({}))
                .await?
        }
        RetentionCmd::List => c.get_json("/api/v1/admin/retention/policies").await?,
        RetentionCmd::Set { source, days } => {
            c.put_json(
                "/api/v1/admin/retention/policies",
                serde_json::json!({ "source": source, "retention_days": days }),
            )
            .await?
        }
    };
    show(&res)
}

pub async fn audit(url: &str, since: Option<&str>, tenant: Option<&str>) -> anyhow::Result<()> {
    let c = StrataClient::new(url);
    let mut qs = Vec::new();
    if let Some(s) = since {
        qs.push(format!("since={s}"));
    }
    if let Some(t) = tenant {
        qs.push(format!("tenant={t}"));
    }
    let path = if qs.is_empty() {
        "/api/v1/admin/audit".to_string()
    } else {
        format!("/api/v1/admin/audit?{}", qs.join("&"))
    };
    show(&c.get_json(&path).await?)
}

pub async fn tenant(url: &str, action: TenantCmd) -> anyhow::Result<()> {
    let c = StrataClient::new(url);
    let res = match action {
        TenantCmd::Delete { tenant } => {
            c.delete_json(&format!("/api/v1/admin/tenants/{tenant}"))
                .await?
        }
        TenantCmd::Export { tenant } => {
            c.get_json(&format!("/api/v1/admin/tenants/{tenant}/export"))
                .await?
        }
        TenantCmd::Import { tenant, file } => {
            let body: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&file)?)?;
            c.post_json(&format!("/api/v1/admin/tenants/{tenant}/import"), body)
                .await?
        }
    };
    show(&res)
}

pub async fn memory(url: &str, action: MemoryCmd) -> anyhow::Result<()> {
    let c = StrataClient::new(url);
    let res = match action {
        MemoryCmd::Decay => {
            c.post_json("/api/v1/admin/memory/decay", serde_json::json!({}))
                .await?
        }
        MemoryCmd::Consolidate => {
            c.post_json("/api/v1/admin/memory/consolidate", serde_json::json!({}))
                .await?
        }
    };
    show(&res)
}

pub async fn reindex(url: &str) -> anyhow::Result<()> {
    let c = StrataClient::new(url);
    show(
        &c.post_json("/api/v1/admin/reindex", serde_json::json!({}))
            .await?,
    )
}

pub async fn rebalance(url: &str, tenant: &str, target_shard: usize) -> anyhow::Result<()> {
    let c = StrataClient::new(url);
    show(
        &c.post_json(
            "/api/v1/admin/rebalance",
            serde_json::json!({ "tenant": tenant, "target_shard": target_shard }),
        )
        .await?,
    )
}
