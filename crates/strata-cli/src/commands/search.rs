//! Semantic search from the CLI.

use crate::client::StrataClient;

pub async fn run(url: &str, text: &str, k: usize) -> anyhow::Result<()> {
    let client = StrataClient::new(url);
    let result = client.search(text, k).await?;

    let results = result.get("results").and_then(|r| r.as_array());

    match results {
        Some(items) if !items.is_empty() => {
            for (i, item) in items.iter().enumerate() {
                let score = item
                    .get("score")
                    .and_then(|s| s.as_f64())
                    .map_or_else(|| "?".to_string(), |s| format!("{s:.4}"));

                let source = item
                    .get("source")
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown");

                let event_type = item
                    .get("event_type")
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown");

                let ts = item.get("ts").and_then(|s| s.as_str()).unwrap_or("");

                let summary = item.get("summary").and_then(|s| s.as_str()).unwrap_or("");

                println!("{}. [score: {}] {} / {}", i + 1, score, source, event_type,);
                if !ts.is_empty() {
                    println!("   ts: {ts}");
                }
                if !summary.is_empty() {
                    println!("   {summary}");
                }
                println!();
            }
            println!("{} result(s)", items.len());
        }
        _ => {
            println!("No results found.");
        }
    }

    Ok(())
}
