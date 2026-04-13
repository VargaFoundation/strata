pub mod backup;
pub mod export;
pub mod ingest;
pub mod query;
pub mod restore;
pub mod schema;
pub mod search;
pub mod shell;
pub mod status;

use clap::Subcommand;

#[derive(Subcommand)]
pub enum Command {
    /// Show server status
    Status,
    /// Execute a SQL query
    Query {
        /// SQL query string
        sql: String,
    },
    /// Ingest data from a file
    Ingest {
        /// Data source name
        #[arg(long)]
        source: String,
        /// File path to ingest
        #[arg(long)]
        file: String,
    },
    /// Export data for GDPR compliance
    Export {
        /// Entity ID to export
        #[arg(long)]
        entity: String,
    },
    /// Create a backup
    Backup {
        /// Backup target (e.g., s3://bucket/path)
        #[arg(long)]
        target: String,
    },
    /// Restore from a backup
    Restore {
        /// Backup source to restore from
        #[arg(long)]
        from: String,
    },
    /// Interactive SQL shell (REPL)
    Shell,
    /// Show available tables, event sources, and agent IDs
    Schema,
    /// Semantic search across stored events
    Search {
        /// Search text
        text: String,
        /// Number of results to return
        #[arg(short, long, default_value = "5")]
        k: usize,
    },
}

pub async fn execute(cmd: Command, url: &str) -> anyhow::Result<()> {
    match cmd {
        Command::Status => status::run(url).await,
        Command::Query { sql } => query::run(url, &sql).await,
        Command::Ingest { source, file } => ingest::run(url, &source, &file).await,
        Command::Export { entity } => export::run(url, &entity).await,
        Command::Backup { target } => backup::run(url, &target).await,
        Command::Restore { from } => restore::run(url, &from).await,
        Command::Shell => shell::run(url).await,
        Command::Schema => schema::run(url).await,
        Command::Search { text, k } => search::run(url, &text, k).await,
    }
}
