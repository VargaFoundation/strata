use crate::client::EcphoriaClient;

pub async fn run(url: &str, entity: &str) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);

    // Query all episodic events for this entity
    let sql =
        format!("SELECT * FROM episodic WHERE payload::VARCHAR LIKE '%{entity}%' ORDER BY ts");
    let result = client.query(&sql).await?;

    let rows = result["rows"].as_array();
    let count = rows.map(|r| r.len()).unwrap_or(0);

    if count == 0 {
        println!("No data found for entity: {entity}");
    } else {
        // Output as NDJSON (one JSON object per line) for GDPR export
        if let Some(rows) = rows {
            for row in rows {
                println!("{}", serde_json::to_string(row)?);
            }
        }
        eprintln!("Exported {count} records for entity: {entity}");
    }

    Ok(())
}
