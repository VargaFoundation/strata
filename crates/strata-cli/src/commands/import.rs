//! `strata import` — bring external note/knowledge stores into Strata's memory.
//!
//! Obsidian: each Markdown note becomes a memory (subject = note title), YAML frontmatter is
//! attached as metadata, and every `[[wikilink]]` becomes a knowledge-graph edge
//! (`note --links_to--> target`). Runs over the REST API, so it works against any Strata server.

use crate::client::StrataClient;

/// A parsed Obsidian note.
struct Note {
    title: String,
    body: String,
    frontmatter: serde_json::Value,
    links: Vec<String>,
}

pub async fn run(url: &str, from: &str, path: &str, user: Option<&str>) -> anyhow::Result<()> {
    match from {
        "obsidian" => import_obsidian(url, path, user).await,
        other => anyhow::bail!("unknown import source '{other}' (supported: obsidian)"),
    }
}

async fn import_obsidian(url: &str, vault: &str, user: Option<&str>) -> anyhow::Result<()> {
    let root = std::path::Path::new(vault);
    if !root.is_dir() {
        anyhow::bail!("vault path '{vault}' is not a directory");
    }
    let mut files = Vec::new();
    collect_markdown(root, &mut files)?;
    if files.is_empty() {
        println!("No .md files found under {vault}.");
        return Ok(());
    }

    let client = StrataClient::new(url);
    let (mut memories, mut edges, mut errors) = (0u32, 0u32, 0u32);

    for file in &files {
        let content = match std::fs::read_to_string(file) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("  skip {}: {e}", file.display());
                errors += 1;
                continue;
            }
        };
        let note = parse_note(file, &content);

        // The note itself → a memory (subject = title, frontmatter + path as metadata).
        let mut metadata = note.frontmatter.clone();
        if let serde_json::Value::Object(ref mut m) = metadata {
            m.insert("source".into(), serde_json::json!("obsidian"));
            m.insert("path".into(), serde_json::json!(file.display().to_string()));
        }
        let mut body = serde_json::json!({
            "content": if note.body.trim().is_empty() { note.title.clone() } else { note.body.clone() },
            "subject": note.title,
            "metadata": metadata,
            "mem_type": "semantic",
        });
        if let Some(u) = user {
            body["user_id"] = serde_json::json!(u);
        }
        match client.post_json("/api/v1/memories", body).await {
            Ok(_) => memories += 1,
            Err(e) => {
                eprintln!("  memory failed for '{}': {e}", note.title);
                errors += 1;
                continue;
            }
        }

        // Each [[wikilink]] → a graph edge note --links_to--> target.
        for target in &note.links {
            let edge = serde_json::json!({
                "src": note.title,
                "relation": "links_to",
                "dst": target,
            });
            match client.post_json("/api/v1/memories/link", edge).await {
                Ok(_) => edges += 1,
                Err(e) => {
                    eprintln!("  edge {} -> {} failed: {e}", note.title, target);
                    errors += 1;
                }
            }
        }
    }

    println!(
        "Imported {} note(s): {memories} memories, {edges} graph edges, {errors} error(s).",
        files.len()
    );
    Ok(())
}

/// Recursively collect `.md` files, skipping the Obsidian `.obsidian` config dir.
fn collect_markdown(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|n| n.to_str()) == Some(".obsidian") {
                continue;
            }
            collect_markdown(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("md") {
            out.push(path);
        }
    }
    Ok(())
}

/// Parse a note into (title, body, frontmatter, wikilinks). Title = frontmatter `title` or filename.
fn parse_note(file: &std::path::Path, content: &str) -> Note {
    let (frontmatter, body) = split_frontmatter(content);
    let title = frontmatter
        .get("title")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            file.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("untitled")
                .to_string()
        });
    let links = extract_wikilinks(body);
    Note {
        title,
        body: body.to_string(),
        frontmatter,
        links,
    }
}

/// Split a leading `---\n…\n---` YAML-ish frontmatter block from the body. The block is parsed
/// line-by-line into a JSON object (`key: value`) — no YAML dependency; nested/complex YAML is kept
/// as raw strings. Returns `({}, whole)` when there is no frontmatter.
fn split_frontmatter(content: &str) -> (serde_json::Value, &str) {
    let rest = match content.strip_prefix("---\n") {
        Some(r) => r,
        None => return (serde_json::json!({}), content),
    };
    let Some(end) = rest
        .find("\n---\n")
        .or_else(|| rest.strip_suffix("\n---").map(|_| rest.len() - 4))
    else {
        return (serde_json::json!({}), content);
    };
    let (fm, after) = rest.split_at(end);
    let body = after.strip_prefix("\n---\n").unwrap_or("");
    let mut map = serde_json::Map::new();
    for line in fm.lines() {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim();
            let v = v.trim().trim_matches('"');
            if !k.is_empty() {
                map.insert(k.to_string(), serde_json::json!(v));
            }
        }
    }
    (serde_json::Value::Object(map), body)
}

/// Extract `[[wikilink]]` targets, dropping any `|alias` and `#heading` fragments and deduplicating.
fn extract_wikilinks(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'[' && bytes[i + 1] == b'[' {
            if let Some(close) = text[i + 2..].find("]]") {
                let inner = &text[i + 2..i + 2 + close];
                // Strip Obsidian alias (`Target|Alias`) and heading (`Target#Section`) parts.
                let target = inner
                    .split('|')
                    .next()
                    .unwrap_or(inner)
                    .split('#')
                    .next()
                    .unwrap_or(inner)
                    .trim();
                if !target.is_empty() && !out.contains(&target.to_string()) {
                    out.push(target.to_string());
                }
                i += 2 + close + 2;
                continue;
            }
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parses_frontmatter_body_and_links() {
        let content = "---\ntitle: My Note\ntags: a, b\n---\nHello [[Other Note]] and [[Third|alias]] and [[Fourth#Heading]].";
        let note = parse_note(Path::new("/vault/my-note.md"), content);
        assert_eq!(note.title, "My Note");
        assert_eq!(note.frontmatter["tags"], "a, b");
        assert!(note.body.starts_with("Hello"));
        assert_eq!(note.links, vec!["Other Note", "Third", "Fourth"]);
    }

    #[test]
    fn title_falls_back_to_filename() {
        let note = parse_note(Path::new("/vault/Some Idea.md"), "no frontmatter here");
        assert_eq!(note.title, "Some Idea");
        assert!(note.frontmatter.as_object().unwrap().is_empty());
        assert!(note.links.is_empty());
    }

    #[test]
    fn wikilinks_dedupe_and_strip() {
        let links = extract_wikilinks("[[A]] [[A]] [[B|x]] plain [[ C ]]");
        assert_eq!(links, vec!["A", "B", "C"]);
    }
}
