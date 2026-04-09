use clap::Parser;

mod client;
mod commands;
mod output;

#[derive(Parser)]
#[command(name = "strata", about = "Strata context lake CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: commands::Command,

    /// Server URL
    #[arg(long, env = "STRATA_URL", default_value = "http://localhost:8432")]
    url: String,
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
    commands::execute(cli.command, &cli.url).await
}
