use crate::client::EcphoriaClient;

/// Trigger a server-side backup of all stores (POST /api/v1/admin/backup).
pub async fn run(url: &str) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);
    let res = client
        .post_json("/api/v1/admin/backup", serde_json::json!({}))
        .await?;
    println!("{}", serde_json::to_string_pretty(&res)?);
    Ok(())
}
