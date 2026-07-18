//! Show available tables, event sources, and agent IDs.

use crate::client::EcphoriaClient;

pub async fn run(url: &str) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);

    // Fetch sources and agents in parallel
    let (sources_res, agents_res) =
        tokio::try_join!(client.schema_sources(), client.schema_agents())?;

    // Display sources
    println!("Event Sources:");
    if let Some(sources) = sources_res.get("sources").and_then(|s| s.as_array()) {
        if sources.is_empty() {
            println!("  (none)");
        } else {
            for src in sources {
                let name = src.as_str().unwrap_or("?");
                println!("  - {name}");
            }
        }
    } else {
        println!("  (none)");
    }

    println!();

    // Display agents
    println!("Agent IDs:");
    if let Some(agents) = agents_res.get("agents").and_then(|a| a.as_array()) {
        if agents.is_empty() {
            println!("  (none)");
        } else {
            for agent in agents {
                let id = agent.as_str().unwrap_or("?");
                println!("  - {id}");
            }
        }
    } else {
        println!("  (none)");
    }

    Ok(())
}
