//! Query planning — routes SQL to the appropriate execution backend.

/// A planned query ready for execution.
#[derive(Debug)]
pub enum QueryPlan {
    /// Pure SQL query executed via DuckDB.
    Sql(String),
    /// Vector similarity search.
    VectorSearch { query_text: String, k: usize },
    /// Hybrid: SQL + vector search combined.
    Hybrid {
        sql: String,
        query_text: String,
        k: usize,
    },
}

/// Plans queries by analyzing SQL and routing appropriately.
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(sql: &str) -> crate::Result<QueryPlan> {
        // TODO: parse SQL, detect strata_search() calls, route accordingly
        Ok(QueryPlan::Sql(sql.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_simple_sql() {
        let plan = QueryPlanner::plan("SELECT 1").unwrap();
        assert!(matches!(plan, QueryPlan::Sql(_)));
    }
}
