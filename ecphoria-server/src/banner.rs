//! Startup banner with version and connection info.

use ecphoria_core::CoreConfig;
use ecphoria_gateway::server::GatewayConfig;

pub fn print() {
    let version = env!("CARGO_PKG_VERSION");
    eprintln!(
        r#"
  ____  _             _
 / ___|| |_ _ __ __ _| |_ __ _
 \___ \| __| '__/ _` | __/ _` |
  ___) | |_| | | (_| | || (_| |
 |____/ \__|_|  \__,_|\__\__,_|

  The open-source context lake for AI agents
  Version: {version}
"#
    );
}

/// Print connection info after the server starts.
pub fn print_ready(gateway: &GatewayConfig, core: &CoreConfig) {
    let http = &gateway.listen;
    let pg = &gateway.pg_listen;
    let grpc = &gateway.grpc_listen;
    let provider = &core.embedding.provider;
    let episodic = &core.memory.episodic.db_path;

    eprintln!("  Ready!");
    eprintln!("  ├─ REST API:   http://{http}");
    eprintln!("  ├─ PostgreSQL: psql -h {} -p {}", host(pg), port(pg));
    eprintln!("  ├─ gRPC:       {grpc}");
    eprintln!("  ├─ MCP:        http://{http}/mcp");
    eprintln!("  ├─ Admin UI:   http://{http}/ui");
    eprintln!("  ├─ Health:     http://{http}/health");
    eprintln!("  ├─ Metrics:    http://{http}/metrics");

    if episodic == ":memory:" {
        eprintln!("  ├─ Storage:    in-memory (data lost on restart!)");
    } else {
        eprintln!("  ├─ Storage:    {episodic}");
    }

    match provider.as_str() {
        "none" | "" => {
            eprintln!("  └─ Embedding:  disabled");
            eprintln!();
            eprintln!("  Semantic search is disabled. To enable:");
            eprintln!("    ECPHORIA_EMBEDDING__PROVIDER=ollama  (local, needs Ollama running)");
            eprintln!("    ECPHORIA_EMBEDDING__PROVIDER=openai  (cloud, needs OPENAI_API_KEY)");
        }
        "ollama" => {
            eprintln!(
                "  └─ Embedding:  ollama ({}, {})",
                core.embedding.model, core.embedding.ollama_url
            );
        }
        "openai" => {
            eprintln!("  └─ Embedding:  openai ({})", core.embedding.model);
        }
        other => {
            eprintln!("  └─ Embedding:  {other}");
        }
    }
    eprintln!();
}

fn host(addr: &str) -> &str {
    addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr)
}

fn port(addr: &str) -> &str {
    addr.rsplit_once(':').map(|(_, p)| p).unwrap_or("5432")
}
