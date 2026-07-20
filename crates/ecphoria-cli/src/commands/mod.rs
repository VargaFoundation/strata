pub mod admin;
pub mod backup;
pub mod doctor;
pub mod export;
pub mod import;
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
    /// Export data: a GDPR entity dump (--entity), or memories to an Obsidian markdown vault
    /// (--to obsidian --path <dir>)
    Export {
        /// Entity ID to export (GDPR NDJSON dump)
        #[arg(long)]
        entity: Option<String>,
        /// Export target format: `obsidian` (write a markdown vault)
        #[arg(long)]
        to: Option<String>,
        /// Destination directory (with --to obsidian)
        #[arg(long)]
        path: Option<String>,
        /// Scope the export to this user id
        #[arg(long)]
        user: Option<String>,
    },
    /// Import an external knowledge store into memory (Obsidian vault, or a Mem0/Zep JSON export)
    Import {
        /// Source format: obsidian | mem0 | zep
        #[arg(long, default_value = "obsidian")]
        from: String,
        /// Path to the source: an Obsidian vault directory, or a Mem0/Zep JSON export file
        #[arg(long)]
        path: String,
        /// Scope imported memories to this user id
        #[arg(long)]
        user: Option<String>,
        /// Keep running and live-import notes as the vault changes (obsidian only)
        #[arg(long)]
        watch: bool,
    },
    /// Lint a ecphoria.toml (+ ECPHORIA_* env) for misconfigurations — no server needed
    Doctor {
        /// Path to the config file (default: ecphoria.toml, or ECPHORIA_CONFIG)
        #[arg(long)]
        config: Option<String>,
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

    // ── Admin (require an admin token; pass --token / ECPHORIA_TOKEN) ──────────
    /// Trigger a server-side backup of all stores (to the data dir)
    Backup,
    /// Restore all stores from a backup directory (DESTRUCTIVE)
    Restore {
        /// Server-local path of a backup directory
        #[arg(long)]
        path: String,
    },
    /// Retention policy management
    Retention {
        #[command(subcommand)]
        action: RetentionCmd,
    },
    /// Query the audit log
    Audit {
        /// Only entries since this ISO-8601 timestamp
        #[arg(long)]
        since: Option<String>,
        /// Only entries for this tenant
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Tenant administration
    Tenant {
        #[command(subcommand)]
        action: TenantCmd,
    },
    /// Memory maintenance
    Memory {
        #[command(subcommand)]
        action: MemoryCmd,
    },
    /// Reindex unembedded events into the semantic store
    Reindex,
    /// Rebalance a tenant to a target shard (cluster mode)
    Rebalance {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        target_shard: usize,
    },
}

#[derive(Subcommand)]
pub enum RetentionCmd {
    /// Enforce retention policies now (delete old events)
    Enforce,
    /// List per-source retention policies
    List,
    /// Set a per-source retention policy
    Set {
        #[arg(long)]
        source: String,
        #[arg(long)]
        days: u32,
    },
}

#[derive(Subcommand)]
pub enum TenantCmd {
    /// GDPR erasure — delete ALL data for a tenant
    Delete {
        #[arg(long)]
        tenant: String,
    },
    /// Export a tenant snapshot (JSON to stdout)
    Export {
        #[arg(long)]
        tenant: String,
    },
    /// Import a tenant snapshot from a JSON file
    Import {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        file: String,
    },
}

#[derive(Subcommand)]
pub enum MemoryCmd {
    /// Forget low-importance memories (time-decayed)
    Decay,
    /// Consolidate low-importance memories into summaries
    Consolidate,
}

pub async fn execute(cmd: Command, url: &str) -> anyhow::Result<()> {
    match cmd {
        Command::Status => status::run(url).await,
        Command::Doctor { config } => doctor::run(config.as_deref()).await,
        Command::Query { sql } => query::run(url, &sql).await,
        Command::Ingest { source, file } => ingest::run(url, &source, &file).await,
        Command::Export {
            entity,
            to,
            path,
            user,
        } => match (to.as_deref(), path.as_deref()) {
            (Some("obsidian"), Some(p)) => export::run_obsidian(url, p, user.as_deref()).await,
            (Some("obsidian"), None) => {
                anyhow::bail!("--to obsidian requires --path <vault-dir>")
            }
            (Some(other), _) => {
                anyhow::bail!("unknown export target '{other}' (supported: obsidian)")
            }
            (None, _) => match entity {
                Some(e) => export::run(url, &e).await,
                None => anyhow::bail!("provide --entity <id> or --to obsidian --path <dir>"),
            },
        },
        Command::Import {
            from,
            path,
            user,
            watch,
        } => import::run(url, &from, &path, user.as_deref(), watch).await,
        Command::Shell => shell::run(url).await,
        Command::Schema => schema::run(url).await,
        Command::Search { text, k } => search::run(url, &text, k).await,
        Command::Backup => backup::run(url).await,
        Command::Restore { path } => restore::run(url, &path).await,
        Command::Retention { action } => admin::retention(url, action).await,
        Command::Audit { since, tenant } => {
            admin::audit(url, since.as_deref(), tenant.as_deref()).await
        }
        Command::Tenant { action } => admin::tenant(url, action).await,
        Command::Memory { action } => admin::memory(url, action).await,
        Command::Reindex => admin::reindex(url).await,
        Command::Rebalance {
            tenant,
            target_shard,
        } => admin::rebalance(url, &tenant, target_shard).await,
    }
}
