use crate::client::EcphoriaClient;
use crate::output;

pub async fn run(url: &str, sql: &str) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);
    let result = client.query(sql).await?;
    output::print_json(&result, true);
    Ok(())
}
