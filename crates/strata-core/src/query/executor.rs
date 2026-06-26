//! Query execution — routes query plans to the appropriate memory stores.

use std::sync::Arc;

use crate::embedding::EmbeddingProvider;
use crate::memory::episodic::EpisodicStore;
use crate::memory::semantic::SemanticStore;
use crate::memory::state::StateStore;

use super::QueryPlan;

/// Executes query plans against the engine subsystems.
pub struct QueryExecutor {
    episodic: Arc<EpisodicStore>,
    semantic: Arc<SemanticStore>,
    state: Arc<StateStore>,
    embedding: Option<Arc<dyn EmbeddingProvider>>,
    /// When set, SQL queries are scoped to this tenant (row-level isolation).
    tenant: Option<String>,
}

impl QueryExecutor {
    pub fn new(
        episodic: Arc<EpisodicStore>,
        semantic: Arc<SemanticStore>,
        state: Arc<StateStore>,
        embedding: Option<Arc<dyn EmbeddingProvider>>,
    ) -> Self {
        Self {
            episodic,
            semantic,
            state,
            embedding,
            tenant: None,
        }
    }

    /// Scope all SQL execution to a single tenant (row-level isolation).
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Execute a query plan and return results as JSON rows.
    ///
    /// For SQL queries containing `strata_search()` or `strata_state()` in
    /// non-top-level positions (JOINs, subqueries, CTEs), the executor
    /// automatically rewrites the query using CTE injection.
    pub async fn execute(
        &self,
        plan: QueryPlan,
        max_rows: usize,
    ) -> crate::Result<Vec<serde_json::Value>> {
        match plan {
            QueryPlan::Sql(sql) => {
                // Try hybrid query rewriting for SQL containing strata functions
                let effective = match super::functions::rewrite_hybrid_query(
                    &sql,
                    &self.semantic,
                    &self.state,
                    &self.embedding,
                )
                .await
                {
                    Ok(Some(rewritten)) => rewritten,
                    _ => sql,
                };
                // Scope to the tenant if one is set (row-level isolation), else run as-is.
                match &self.tenant {
                    Some(t) => self.episodic.query_sql_for_tenant(&effective, t, max_rows),
                    None => self.episodic.query_sql_limited(&effective, max_rows),
                }
            }

            QueryPlan::Dml(_sql) => Err(crate::Error::Query(
                "DML statements are not allowed via query_sql (use ingest/state API)".into(),
            )),

            QueryPlan::VectorSearch { query_text, k } => {
                self.execute_vector_search(&query_text, k).await
            }

            QueryPlan::StateGet { agent_id, key } => self.execute_state_get(&agent_id, &key).await,
        }
    }

    /// Execute a vector search by embedding query text and searching semantic memory.
    async fn execute_vector_search(
        &self,
        query_text: &str,
        k: usize,
    ) -> crate::Result<Vec<serde_json::Value>> {
        let provider = self.embedding.as_ref().ok_or_else(|| {
            crate::Error::Embedding(
                "no embedding provider configured — strata_search() requires an embedding provider"
                    .into(),
            )
        })?;

        let vectors = provider.embed(&[query_text.to_string()]).await?;
        let vector = vectors
            .into_iter()
            .next()
            .ok_or_else(|| crate::Error::Embedding("embedding returned empty result".into()))?;

        // Tenant-scoped queries only ever see their own event embeddings.
        let results = match &self.tenant {
            Some(t) => {
                let t = t.clone();
                self.semantic
                    .search_filtered(&vector, k, move |e| {
                        e.metadata
                            .get("tenant_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("default")
                            == t
                    })
                    .await?
            }
            None => self.semantic.search(&vector, k).await?,
        };

        Ok(results
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.entry.id.to_string(),
                    "content": r.entry.content,
                    "metadata": r.entry.metadata,
                    "score": r.score,
                })
            })
            .collect())
    }

    /// Execute a state key lookup and return the result as a JSON row.
    async fn execute_state_get(
        &self,
        agent_id: &str,
        key: &str,
    ) -> crate::Result<Vec<serde_json::Value>> {
        // Tenant-scoped queries namespace the agent so they can't read another tenant's state.
        let scoped = match &self.tenant {
            Some(t) => crate::engine::scoped_agent(t, agent_id),
            None => agent_id.to_string(),
        };
        match self.state.get(&scoped, key).await? {
            Some(entry) => Ok(vec![serde_json::json!({
                // Return the caller's un-prefixed agent_id, not the internal namespaced one.
                "agent_id": agent_id,
                "key": entry.key,
                "value": entry.value,
                "version": entry.version,
            })]),
            None => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_executor() -> QueryExecutor {
        QueryExecutor::new(
            Arc::new(EpisodicStore::new()),
            Arc::new(SemanticStore::new()),
            Arc::new(StateStore::new()),
            None,
        )
    }

    #[tokio::test]
    async fn execute_sql_query() {
        let executor = make_executor();

        let results = executor
            .execute(QueryPlan::Sql("SELECT 1::VARCHAR as v".into()), 100)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn execute_dml_rejected() {
        let executor = make_executor();

        let result = executor
            .execute(QueryPlan::Dml("INSERT INTO foo VALUES (1)".into()), 100)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_vector_search_no_provider() {
        let executor = make_executor();

        let result = executor
            .execute(
                QueryPlan::VectorSearch {
                    query_text: "test".into(),
                    k: 5,
                },
                100,
            )
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no embedding provider"));
    }

    #[tokio::test]
    async fn execute_state_get_missing() {
        let executor = make_executor();

        let results = executor
            .execute(
                QueryPlan::StateGet {
                    agent_id: "bot".into(),
                    key: "mood".into(),
                },
                100,
            )
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn execute_state_get_found() {
        let state = Arc::new(StateStore::new());
        state
            .set("bot", "mood", serde_json::json!("happy"))
            .await
            .unwrap();

        let executor = QueryExecutor::new(
            Arc::new(EpisodicStore::new()),
            Arc::new(SemanticStore::new()),
            state,
            None,
        );

        let results = executor
            .execute(
                QueryPlan::StateGet {
                    agent_id: "bot".into(),
                    key: "mood".into(),
                },
                100,
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["agent_id"], "bot");
        assert_eq!(results[0]["key"], "mood");
        assert_eq!(results[0]["value"], "happy");
        assert_eq!(results[0]["version"], 1);
    }
}
