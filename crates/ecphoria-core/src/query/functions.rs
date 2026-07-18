//! Custom SQL function registration and hybrid query support for DuckDB.
//!
//! Ecphoria extends DuckDB SQL with virtual functions:
//! - `ecphoria_search('text', k)` — semantic similarity search
//! - `ecphoria_state('agent_id', 'key')` — state key lookup
//!
//! Since DuckDB UDF registration via the vtab API requires unsafe C FFI,
//! we use a query-rewriting approach: the executor detects these functions
//! in SQL, evaluates them separately, and injects the results as CTEs
//! before sending the rewritten query to DuckDB.

use std::sync::Arc;

use crate::embedding::EmbeddingProvider;
use crate::memory::semantic::SemanticStore;
use crate::memory::state::StateStore;

/// Rewrite a SQL query containing ecphoria_search() or ecphoria_state() calls
/// into a pure SQL query with CTE-injected results.
///
/// Example input:
/// ```sql
/// SELECT e.source, s.content, s.score
/// FROM episodic e
/// JOIN ecphoria_search('billing issue', 3) s ON true
/// ```
///
/// Becomes:
/// ```sql
/// WITH _ecphoria_search AS (
///   SELECT '...' AS id, '...' AS content, '{}' AS metadata, 0.95 AS score
///   UNION ALL SELECT ...
/// )
/// SELECT e.source, s.content, s.score
/// FROM episodic e
/// JOIN _ecphoria_search s ON true
/// ```
pub async fn rewrite_hybrid_query(
    sql: &str,
    semantic: &Arc<SemanticStore>,
    state: &Arc<StateStore>,
    embedding: &Option<Arc<dyn EmbeddingProvider>>,
) -> crate::Result<Option<String>> {
    let upper = sql.to_uppercase();

    // Check if this query contains ecphoria functions that need rewriting
    let has_search = upper.contains("ECPHORIA_SEARCH(") || upper.contains("ECPHORIA_SEARCH (");
    let has_state = upper.contains("ECPHORIA_STATE(") || upper.contains("ECPHORIA_STATE (");

    if !has_search && !has_state {
        return Ok(None); // No rewriting needed
    }

    let mut ctes: Vec<String> = Vec::new();
    let mut rewritten = sql.to_string();

    // Handle ecphoria_search() calls
    if has_search {
        if let Some(args) = extract_function_args(sql, "ecphoria_search") {
            let parts: Vec<&str> = args.splitn(2, ',').collect();
            let query_text = parts
                .first()
                .unwrap_or(&"")
                .trim()
                .trim_matches('\'')
                .trim_matches('"');
            let k = parts
                .get(1)
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(5);

            // Embed and search
            let results = if let Some(provider) = embedding {
                let vectors = provider.embed_query(&[query_text.to_string()]).await?;
                if let Some(vector) = vectors.first() {
                    semantic.search(vector, k).await?
                } else {
                    Vec::new()
                }
            } else {
                return Err(crate::Error::Embedding(
                    "no embedding provider configured for ecphoria_search()".into(),
                ));
            };

            // Build CTE with search results
            let rows: Vec<String> = results
                .iter()
                .map(|r| {
                    let meta_json = serde_json::to_string(&r.entry.metadata)
                        .unwrap_or_else(|_| "{}".to_string())
                        .replace('\'', "''");
                    let content_escaped = r.entry.content.replace('\'', "''");
                    format!(
                        "SELECT '{}' AS id, '{}' AS content, '{}' AS metadata, {} AS score",
                        r.entry.id, content_escaped, meta_json, r.score
                    )
                })
                .collect();

            let cte_body = if rows.is_empty() {
                "SELECT '' AS id, '' AS content, '{}' AS metadata, 0.0 AS score WHERE false"
                    .to_string()
            } else {
                rows.join(" UNION ALL ")
            };

            ctes.push(format!("_ecphoria_search AS ({})", cte_body));

            // Replace the function call in the SQL with the CTE reference
            // Match patterns like: ecphoria_search('text', k) or ecphoria_search('text')
            let func_pattern = find_function_call(sql, "ecphoria_search");
            if let Some((start, end)) = func_pattern {
                rewritten = format!("{}_ecphoria_search{}", &sql[..start], &sql[end..]);
            }
        }
    }

    // Handle ecphoria_state() calls
    if has_state {
        if let Some(args) = extract_function_args(&rewritten, "ecphoria_state") {
            let parts: Vec<&str> = args.splitn(2, ',').collect();
            let agent_id = parts
                .first()
                .unwrap_or(&"")
                .trim()
                .trim_matches('\'')
                .trim_matches('"');
            let key = parts
                .get(1)
                .map(|s| s.trim().trim_matches('\'').trim_matches('"'))
                .unwrap_or_default();

            let entry = state.get(agent_id, key).await?;
            let (value_str, version) = match entry {
                Some(e) => (
                    serde_json::to_string(&e.value)
                        .unwrap_or_else(|_| "null".to_string())
                        .replace('\'', "''"),
                    e.version,
                ),
                None => ("null".to_string(), 0),
            };

            let cte_body = format!(
                "SELECT '{}' AS agent_id, '{}' AS key, '{}' AS value, {} AS version",
                agent_id.replace('\'', "''"),
                key.replace('\'', "''"),
                value_str,
                version
            );

            ctes.push(format!("_ecphoria_state AS ({})", cte_body));

            let func_pattern = find_function_call(&rewritten, "ecphoria_state");
            if let Some((start, end)) = func_pattern {
                rewritten = format!(
                    "{}_ecphoria_state{}",
                    &rewritten[..start],
                    &rewritten[end..]
                );
            }
        }
    }

    if ctes.is_empty() {
        return Ok(None);
    }

    // Prepend CTEs to the rewritten query
    let cte_prefix = format!("WITH {}", ctes.join(", "));

    // If the query already has a WITH clause, we need to merge
    let final_sql = if rewritten.trim().to_uppercase().starts_with("WITH") {
        // Replace "WITH" with "WITH our_ctes,"
        let with_pos = rewritten.to_uppercase().find("WITH").unwrap_or(0);
        format!("WITH {}, {}", ctes.join(", "), &rewritten[with_pos + 4..])
    } else {
        format!("{} {}", cte_prefix, rewritten)
    };

    Ok(Some(final_sql))
}

/// Find the byte range of a function call like `func_name(...)` in SQL.
fn find_function_call(sql: &str, func_name: &str) -> Option<(usize, usize)> {
    let lower = sql.to_lowercase();
    let func_lower = func_name.to_lowercase();
    let pos = lower.find(&func_lower)?;
    let after_name = &sql[pos + func_name.len()..];
    let open = after_name.find('(')?;
    let rest = &after_name[open + 1..];
    let close = rest.find(')')?;
    let start = pos;
    let end = pos + func_name.len() + open + 1 + close + 1;
    Some((start, end))
}

/// Extract function arguments from SQL like `func_name('arg1', arg2)`.
fn extract_function_args<'a>(sql: &'a str, func_name: &str) -> Option<&'a str> {
    let lower = sql.to_lowercase();
    let func_lower = func_name.to_lowercase();
    let pos = lower.find(&func_lower)?;
    let after_name = &sql[pos + func_name.len()..];
    let open = after_name.find('(')?;
    let rest = &after_name[open + 1..];
    let close = rest.find(')')?;
    Some(&rest[..close])
}

/// Register all custom SQL functions with the DuckDB connection.
///
/// Currently a no-op — functions are handled via query rewriting in the executor.
/// See `rewrite_hybrid_query()` for the implementation.
pub fn register_all(_conn: &duckdb::Connection) -> crate::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_function_call_basic() {
        let sql = "SELECT * FROM ecphoria_search('hello', 3)";
        let (start, end) = find_function_call(sql, "ecphoria_search").unwrap();
        assert_eq!(&sql[start..end], "ecphoria_search('hello', 3)");
    }

    #[test]
    fn find_function_call_in_join() {
        let sql = "SELECT e.* FROM episodic e JOIN ecphoria_search('test', 5) s ON true";
        let (start, end) = find_function_call(sql, "ecphoria_search").unwrap();
        assert_eq!(&sql[start..end], "ecphoria_search('test', 5)");
    }

    #[test]
    fn find_function_call_missing() {
        let sql = "SELECT * FROM episodic";
        assert!(find_function_call(sql, "ecphoria_search").is_none());
    }

    #[test]
    fn extract_args_basic() {
        let args = extract_function_args("ecphoria_search('hello', 3)", "ecphoria_search");
        assert_eq!(args, Some("'hello', 3"));
    }

    #[test]
    fn extract_args_in_context() {
        let args = extract_function_args(
            "SELECT * FROM episodic JOIN ecphoria_state('bot', 'mood') ON true",
            "ecphoria_state",
        );
        assert_eq!(args, Some("'bot', 'mood'"));
    }
}
