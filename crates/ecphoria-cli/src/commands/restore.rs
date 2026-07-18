use crate::client::EcphoriaClient;

/// Restore all stores from a backup directory (POST /api/v1/admin/restore). DESTRUCTIVE.
pub async fn run(url: &str, path: &str) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);
    let res = client
        .post_json("/api/v1/admin/restore", serde_json::json!({ "path": path }))
        .await?;
    println!("{}", serde_json::to_string_pretty(&res)?);
    Ok(())
}
