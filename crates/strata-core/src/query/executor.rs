//! Query execution against DuckDB and memory stores.

use super::QueryPlan;

/// Executes query plans against the engine subsystems.
pub struct QueryExecutor;

impl QueryExecutor {
    pub fn new() -> Self {
        Self
    }

    /// Execute a query plan and return results as JSON rows.
    pub async fn execute(&self, _plan: QueryPlan) -> crate::Result<Vec<serde_json::Value>> {
        // TODO: execute against DuckDB / memory stores
        Ok(vec![])
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_empty() {
        let executor = QueryExecutor::new();
        let results = executor
            .execute(QueryPlan::Sql("SELECT 1".into()))
            .await
            .unwrap();
        assert!(results.is_empty());
    }
}
