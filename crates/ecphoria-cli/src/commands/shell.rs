//! Interactive SQL shell (REPL) for Ecphoria.

use crate::client::EcphoriaClient;
use crate::output;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

pub async fn run(url: &str) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);

    // Verify connectivity before entering the loop
    match client.health().await {
        Ok(h) => {
            let version = h["version"].as_str().unwrap_or("unknown");
            println!("Connected to Ecphoria ({url}), version {version}");
        }
        Err(e) => {
            anyhow::bail!("Cannot connect to Ecphoria at {url}: {e}");
        }
    }

    let mut rl = DefaultEditor::new()?;

    // Try to load history (ignore errors — file may not exist yet)
    let history_path = dirs_history_path();
    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    loop {
        let readline = rl.readline("ecphoria> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Meta-commands
                if trimmed == r"\q" || trimmed.eq_ignore_ascii_case("exit") {
                    println!("Goodbye.");
                    break;
                }
                if trimmed == r"\?" || trimmed.eq_ignore_ascii_case("help") {
                    print_help();
                    continue;
                }

                let _ = rl.add_history_entry(&line);

                // Execute the SQL query
                match client.query(trimmed).await {
                    Ok(result) => {
                        if result.get("columns").is_some() {
                            output::print_table(&result);
                        } else if let Some(err) = result.get("error") {
                            eprintln!("Error: {}", err);
                        } else {
                            output::print_json(&result, true);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {e}");
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl+C: clear the line, continue
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl+D: exit
                println!("Goodbye.");
                break;
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    // Save history
    if let Some(ref path) = history_path {
        let _ = rl.save_history(path);
    }

    Ok(())
}

fn print_help() {
    println!("Type SQL queries to execute against the Ecphoria server.");
    println!();
    println!("Meta-commands:");
    println!("  \\q      Exit the shell");
    println!("  \\?      Show this help");
    println!("  help    Show this help");
    println!("  exit    Exit the shell");
}

fn dirs_history_path() -> Option<std::path::PathBuf> {
    // Store history alongside other config in ~/.ecphoria/
    let home = std::env::var("HOME").ok()?;
    let dir = std::path::PathBuf::from(home).join(".ecphoria");
    let _ = std::fs::create_dir_all(&dir);
    Some(dir.join("shell_history"))
}
