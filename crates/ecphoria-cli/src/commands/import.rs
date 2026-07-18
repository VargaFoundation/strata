//! `ecphoria import` — bring external note/knowledge stores into Ecphoria's memory.
//!
//! - **Obsidian**: each Markdown note becomes a memory (subject = note title), YAML frontmatter is
//!   attached as metadata, and every `[[wikilink]]` becomes a knowledge-graph edge
//!   (`note --links_to--> target`).
//! - **Mem0**: a `mem0` export (JSON) — a `get_all()` dump or a plain array of memory objects; each
//!   `memory` text becomes a memory, carrying its `user_id`/`metadata`/`created_at`.
//! - **Zep**: a `zep` export (JSON) — graph `facts` and/or session `messages`; each fact/message
//!   becomes a memory.
//!
//! All variants run over the REST API, so they work against any Ecphoria server.

use crate::client::EcphoriaClient;

/// A parsed Obsidian note.
struct Note {
    title: String,
    body: String,
    frontmatter: serde_json::Value,
    links: Vec<String>,
}

/// A memory record extracted from an external store, ready to POST to `/api/v1/memories`.
struct MemRecord {
    content: String,
    subject: Option<String>,
    user_id: Option<String>,
    metadata: serde_json::Value,
}

pub async fn run(url: &str, from: &str, path: &str, user: Option<&str>) -> anyhow::Result<()> {
    match from {
        "obsidian" => import_obsidian(url, path, user).await,
        "mem0" => import_records(url, path, user, "mem0", parse_mem0).await,
        "zep" => import_records(url, path, user, "zep", parse_zep).await,
        other => {
            anyhow::bail!("unknown import source '{other}' (supported: obsidian, mem0, zep)")
        }
    }
}

/// Generic JSON-file importer: read `path`, run `parse` to extract records, POST each as a memory.
/// The record's own `user_id` wins; otherwise the CLI `--user` fallback applies. `source` is stamped
/// into each memory's metadata.
async fn import_records(
    url: &str,
    path: &str,
    user: Option<&str>,
    source: &str,
    parse: fn(&serde_json::Value) -> anyhow::Result<Vec<MemRecord>>,
) -> anyhow::Result<()> {
    let raw =
        std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("cannot read '{path}': {e}"))?;
    let doc: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("'{path}' is not valid JSON: {e}"))?;
    let records = parse(&doc)?;
    if records.is_empty() {
        println!("No memories found in {path}.");
        return Ok(());
    }

    let client = EcphoriaClient::new(url);
    let (mut imported, mut errors) = (0u32, 0u32);
    for rec in &records {
        let mut metadata = rec.metadata.clone();
        if !metadata.is_object() {
            metadata = serde_json::json!({});
        }
        if let serde_json::Value::Object(ref mut m) = metadata {
            m.entry("source").or_insert(serde_json::json!(source));
        }
        let mut body = serde_json::json!({
            "content": rec.content,
            "metadata": metadata,
        });
        if let Some(s) = &rec.subject {
            body["subject"] = serde_json::json!(s);
        }
        // Record's own user_id wins; else the CLI fallback.
        if let Some(u) = rec.user_id.as_deref().or(user) {
            body["user_id"] = serde_json::json!(u);
        }
        match client.post_json("/api/v1/memories", body).await {
            Ok(_) => imported += 1,
            Err(e) => {
                eprintln!("  memory failed: {e}");
                errors += 1;
            }
        }
    }
    println!("Imported {imported} memory(ies) from {source}, {errors} error(s).");
    Ok(())
}

/// Parse a Mem0 export. Accepts a top-level array, or an object wrapping the list under `results`,
/// `memories`, or `data`. Each item's text comes from `memory` (Mem0's field) or `text`/`content`.
fn parse_mem0(doc: &serde_json::Value) -> anyhow::Result<Vec<MemRecord>> {
    let items = as_list(doc, &["results", "memories", "data"]).ok_or_else(|| {
        anyhow::anyhow!("mem0 export: expected an array or a {{results:[…]}} object")
    })?;
    let mut out = Vec::new();
    for item in items {
        let Some(content) = str_field(item, &["memory", "text", "content"]) else {
            continue; // skip entries with no text
        };
        if content.trim().is_empty() {
            continue;
        }
        let mut metadata = item
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        // Preserve Mem0's timestamps in metadata (Ecphoria assigns its own valid_from).
        if let serde_json::Value::Object(ref mut m) = metadata {
            for key in ["created_at", "updated_at", "hash", "categories"] {
                if let Some(v) = item.get(key) {
                    m.entry(key).or_insert(v.clone());
                }
            }
        }
        out.push(MemRecord {
            content,
            subject: None,
            user_id: str_field(item, &["user_id"]),
            metadata,
        });
    }
    Ok(out)
}

/// Parse a Zep export. Accepts graph `facts` (each a string or `{fact|content}` object) and/or
/// session `messages` (each `{role, content}`), at the top level or nested. Facts and messages both
/// become memories; a message's `role` is kept in metadata.
fn parse_zep(doc: &serde_json::Value) -> anyhow::Result<Vec<MemRecord>> {
    let mut out = Vec::new();
    let user = str_field(doc, &["user_id", "session_id"]);
    if let Some(facts) = as_list(doc, &["facts"]) {
        for f in facts {
            let content = match f {
                serde_json::Value::String(s) => Some(s.clone()),
                _ => str_field(f, &["fact", "content"]),
            };
            if let Some(c) = content.filter(|c| !c.trim().is_empty()) {
                out.push(MemRecord {
                    content: c,
                    subject: None,
                    user_id: user.clone(),
                    metadata: serde_json::json!({ "kind": "fact" }),
                });
            }
        }
    }
    if let Some(messages) = as_list(doc, &["messages"]) {
        for m in messages {
            let Some(content) = str_field(m, &["content", "message"]) else {
                continue;
            };
            if content.trim().is_empty() {
                continue;
            }
            let role = str_field(m, &["role", "role_type"]).unwrap_or_else(|| "user".into());
            out.push(MemRecord {
                content,
                subject: None,
                user_id: user.clone(),
                metadata: serde_json::json!({ "kind": "message", "role": role }),
            });
        }
    }
    if out.is_empty() {
        anyhow::bail!("zep export: found neither `facts` nor `messages`");
    }
    Ok(out)
}

/// Return a JSON array from `doc` itself (if it is an array) or from the first present key in `keys`.
fn as_list<'a>(doc: &'a serde_json::Value, keys: &[&str]) -> Option<&'a Vec<serde_json::Value>> {
    if let Some(arr) = doc.as_array() {
        return Some(arr);
    }
    keys.iter()
        .find_map(|k| doc.get(*k).and_then(|v| v.as_array()))
}

/// First present, non-null string among `keys`.
fn str_field(v: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|k| v.get(*k).and_then(|x| x.as_str()))
        .map(|s| s.to_string())
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

    let client = EcphoriaClient::new(url);
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

    #[test]
    fn mem0_parses_results_wrapper_and_bare_array() {
        // get_all()-style {results:[…]}
        let doc = serde_json::json!({
            "results": [
                {"memory": "likes tea", "user_id": "alice", "metadata": {"topic": "drink"},
                 "created_at": "2026-01-01T00:00:00Z"},
                {"memory": "  ", "user_id": "bob"},        // blank text → skipped
                {"user_id": "carol"}                        // no text → skipped
            ]
        });
        let recs = parse_mem0(&doc).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].content, "likes tea");
        assert_eq!(recs[0].user_id.as_deref(), Some("alice"));
        assert_eq!(recs[0].metadata["topic"], "drink");
        assert_eq!(recs[0].metadata["created_at"], "2026-01-01T00:00:00Z");

        // Bare array with alternate text key.
        let bare = serde_json::json!([{"text": "prefers window seats", "user_id": "dave"}]);
        let recs = parse_mem0(&bare).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].content, "prefers window seats");
    }

    #[test]
    fn zep_parses_facts_and_messages() {
        let doc = serde_json::json!({
            "user_id": "u1",
            "facts": [
                "the sky is blue",
                {"fact": "grass is green"},
                {"fact": "   "}                              // blank → skipped
            ],
            "messages": [
                {"role": "assistant", "content": "hello"},
                {"role_type": "user", "message": "hi there"},
                {"role": "user", "content": ""}              // blank → skipped
            ]
        });
        let recs = parse_zep(&doc).unwrap();
        // 2 facts + 2 messages
        assert_eq!(recs.len(), 4);
        assert!(recs.iter().all(|r| r.user_id.as_deref() == Some("u1")));
        assert_eq!(recs[0].metadata["kind"], "fact");
        assert_eq!(recs[2].metadata["kind"], "message");
        assert_eq!(recs[2].metadata["role"], "assistant");
    }

    #[test]
    fn zep_errors_when_empty() {
        assert!(parse_zep(&serde_json::json!({"other": 1})).is_err());
    }
}
