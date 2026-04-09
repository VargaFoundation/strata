use crate::client::StrataClient;
use crate::output;

pub async fn run(url: &str, sql: &str) -> anyhow::Result<()> {
    let client = StrataClient::new(url);
    let result = client.query(sql).await?;
    output::print_json(&result, true);
    Ok(())
}
