use crate::client::StrataClient;
use crate::output;

pub async fn run(url: &str, source: &str, file: &str) -> anyhow::Result<()> {
    let client = StrataClient::new(url);
    let result = client.ingest(source, file).await?;
    output::print_json(&result, false);
    Ok(())
}
