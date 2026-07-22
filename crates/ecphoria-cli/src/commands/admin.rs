//! Admin commands — wrap the server's `/api/v1/admin/*` endpoints. These are admin-only, so pass an
//! admin token via `--token` / `ECPHORIA_TOKEN` when the server has auth enabled.

use crate::client::EcphoriaClient;
use crate::commands::{GraphCmd, MemoryCmd, RetentionCmd, TenantCmd};

fn show(v: &serde_json::Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

pub async fn retention(url: &str, action: RetentionCmd) -> anyhow::Result<()> {
    let c = EcphoriaClient::new(url);
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
    let c = EcphoriaClient::new(url);
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
    let c = EcphoriaClient::new(url);
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
    let c = EcphoriaClient::new(url);
    let res = match action {
        MemoryCmd::Add {
            content,
            subject,
            user,
            importance,
        } => {
            let mut body = serde_json::json!({ "content": content });
            if let Some(s) = subject {
                body["subject"] = serde_json::json!(s);
            }
            if let Some(u) = user {
                body["user_id"] = serde_json::json!(u);
            }
            if let Some(i) = importance {
                body["importance"] = serde_json::json!(i);
            }
            c.post_json("/api/v1/memories", body).await?
        }
        MemoryCmd::Search { query, user, k } => {
            let mut body = serde_json::json!({ "query": query, "k": k });
            if let Some(u) = user {
                body["user_id"] = serde_json::json!(u);
            }
            c.post_json("/api/v1/memories/search", body).await?
        }
        MemoryCmd::List {
            user,
            limit,
            offset,
            mem_type,
            min_importance,
            updated_after,
            updated_before,
            metadata_key,
            metadata_value,
        } => {
            let mut path = format!("/api/v1/memories?limit={limit}&offset={offset}");
            if let Some(u) = user {
                path.push_str(&format!("&user_id={}", urlencode(&u)));
            }
            if let Some(mt) = mem_type {
                path.push_str(&format!("&mem_type={}", urlencode(&mt)));
            }
            if let Some(mi) = min_importance {
                path.push_str(&format!("&min_importance={mi}"));
            }
            if let Some(a) = updated_after {
                path.push_str(&format!("&updated_after={}", urlencode(&a)));
            }
            if let Some(b) = updated_before {
                path.push_str(&format!("&updated_before={}", urlencode(&b)));
            }
            if let (Some(k), Some(v)) = (metadata_key, metadata_value) {
                path.push_str(&format!(
                    "&metadata_key={}&metadata_value={}",
                    urlencode(&k),
                    urlencode(&v)
                ));
            }
            c.get_json(&path).await?
        }
        MemoryCmd::Get { id } => c.get_json(&format!("/api/v1/memories/{id}")).await?,
        MemoryCmd::Update {
            id,
            content,
            importance,
            mem_type,
            metadata,
        } => {
            let mut body = serde_json::json!({});
            if let Some(c) = content {
                body["content"] = serde_json::json!(c);
            }
            if let Some(i) = importance {
                body["importance"] = serde_json::json!(i);
            }
            if let Some(mt) = mem_type {
                body["mem_type"] = serde_json::json!(mt);
            }
            if let Some(m) = metadata {
                let parsed: serde_json::Value = serde_json::from_str(&m)
                    .map_err(|e| anyhow::anyhow!("--metadata must be valid JSON: {e}"))?;
                body["metadata"] = parsed;
            }
            c.patch_json(&format!("/api/v1/memories/{id}"), body)
                .await?
        }
        MemoryCmd::History { id } => {
            c.get_json(&format!("/api/v1/memories/{id}/history"))
                .await?
        }
        MemoryCmd::Decay => {
            c.post_json("/api/v1/admin/memory/decay", serde_json::json!({}))
                .await?
        }
        MemoryCmd::Consolidate => {
            c.post_json("/api/v1/admin/memory/consolidate", serde_json::json!({}))
                .await?
        }
        MemoryCmd::Reembed => {
            c.post_json("/api/v1/admin/memory/reembed", serde_json::json!({}))
                .await?
        }
    };
    show(&res)
}

pub async fn graph(url: &str, action: GraphCmd) -> anyhow::Result<()> {
    let c = EcphoriaClient::new(url);
    let enc = |s: &str| urlencode(s);
    let asof = |a: &Option<String>| {
        a.as_ref()
            .map(|s| format!("?as_of={}", enc(s)))
            .unwrap_or_default()
    };
    let res = match action {
        GraphCmd::Centrality { as_of, limit } => {
            let mut path = format!("/api/v1/memories/graph/centrality{}", asof(&as_of));
            if let Some(l) = limit {
                path.push_str(if path.contains('?') { "&" } else { "?" });
                path.push_str(&format!("limit={l}"));
            }
            c.get_json(&path).await?
        }
        GraphCmd::Path { src, dst, as_of } => {
            let mut path = format!(
                "/api/v1/memories/graph/path?src={}&dst={}",
                enc(&src),
                enc(&dst)
            );
            if let Some(a) = as_of {
                path.push_str(&format!("&as_of={}", enc(&a)));
            }
            c.get_json(&path).await?
        }
        GraphCmd::Communities { as_of } => {
            c.get_json(&format!(
                "/api/v1/memories/graph/communities{}",
                asof(&as_of)
            ))
            .await?
        }
        GraphCmd::Neighbors {
            entity,
            depth,
            limit,
        } => {
            c.get_json(&format!(
                "/api/v1/memories/graph?entity={}&depth={depth}&limit={limit}",
                enc(&entity)
            ))
            .await?
        }
    };
    show(&res)
}

/// Minimal percent-encoding for query-string values (space and the reserved chars we might hit).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub async fn reindex(url: &str) -> anyhow::Result<()> {
    let c = EcphoriaClient::new(url);
    show(
        &c.post_json("/api/v1/admin/reindex", serde_json::json!({}))
            .await?,
    )
}

pub async fn rebalance(url: &str, tenant: &str, target_shard: usize) -> anyhow::Result<()> {
    let c = EcphoriaClient::new(url);
    show(
        &c.post_json(
            "/api/v1/admin/rebalance",
            serde_json::json!({ "tenant": tenant, "target_shard": target_shard }),
        )
        .await?,
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn urlencode_escapes_query_values() {
        assert_eq!(super::urlencode("Alice Smith"), "Alice%20Smith");
        assert_eq!(super::urlencode("a/b?c&d"), "a%2Fb%3Fc%26d");
        assert_eq!(super::urlencode("safe-.~_0"), "safe-.~_0");
    }
}
