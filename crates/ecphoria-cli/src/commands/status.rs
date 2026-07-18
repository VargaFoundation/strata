use crate::client::EcphoriaClient;

pub async fn run(url: &str) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);
    let health = client.health().await?;

    let status = health["status"].as_str().unwrap_or("unknown");
    let version = health["version"].as_str().unwrap_or("unknown");

    println!("Status:  {status}");
    println!("Version: {version}");
    println!("URL:     {url}");

    Ok(())
}
