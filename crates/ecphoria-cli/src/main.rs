use clap::Parser;

mod client;
mod commands;
mod output;

#[derive(Parser)]
#[command(name = "ecphoria", about = "Ecphoria context lake CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: commands::Command,

    /// Server URL
    #[arg(long, env = "ECPHORIA_URL", default_value = "http://localhost:8432")]
    url: String,

    /// Bearer token / API key for authenticated servers (admin commands require it).
    #[arg(long, env = "ECPHORIA_TOKEN")]
    token: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();
    // Propagate a `--token` flag to the env the client reads (so both flag and env work).
    if let Some(t) = &cli.token {
        std::env::set_var("ECPHORIA_TOKEN", t);
    }
    commands::execute(cli.command, &cli.url).await
}
