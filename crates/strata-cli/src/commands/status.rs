use crate::client::StrataClient;
use crate::output;

pub async fn run(url: &str) -> anyhow::Result<()> {
    let client = StrataClient::new(url);
    let health = client.health().await?;
    output::print_json(&health, false);
    Ok(())
}
