//! Materialized views — incremental view computation over DuckDB.
//!
//! A materialized view stores the result of a SQL query as a concrete table.
//! `refresh()` re-runs the query and replaces the table contents.

use dashmap::DashMap;
use duckdb::Connection;
use parking_lot::Mutex;
use std::sync::Arc;

/// Definition of a materialized view.
#[derive(Debug, Clone)]
struct ViewDefinition {
    sql: String,
}

/// Manages materialized views for real-time feature computation.
#[derive(Debug)]
pub struct MaterializedViewManager {
    db: Arc<Mutex<Connection>>,
    views: DashMap<String, ViewDefinition>,
}

impl MaterializedViewManager {
    /// Create a new manager backed by the given DuckDB connection.
    pub fn with_connection(db: Arc<Mutex<Connection>>) -> Self {
        Self {
            db,
            views: DashMap::new(),
        }
    }

    /// Create an in-memory manager (for testing).
    pub fn new() -> Self {
        let conn = Connection::open_in_memory().expect("failed to create in-memory duckdb");
        Self::with_connection(Arc::new(Mutex::new(conn)))
    }

    /// Validate that a view name contains only safe characters (alphanumeric + underscore).
    fn validate_name(name: &str) -> crate::Result<()> {
        if name.is_empty() {
            return Err(crate::Error::Query("view name cannot be empty".into()));
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(crate::Error::Query(format!(
                "view name must be alphanumeric/underscore, got: {name}"
            )));
        }
        Ok(())
    }

    /// Validate that SQL contains only a SELECT query.
    fn validate_select(sql: &str) -> crate::Result<()> {
        use sqlparser::dialect::DuckDbDialect;
        use sqlparser::parser::Parser;

        let statements = Parser::parse_sql(&DuckDbDialect {}, sql)
            .map_err(|e| crate::Error::Query(format!("SQL parse error: {e}")))?;

        if statements.len() != 1 {
            return Err(crate::Error::Query(
                "exactly one SELECT statement is required".into(),
            ));
        }

        match &statements[0] {
            sqlparser::ast::Statement::Query(_) => Ok(()),
            _ => Err(crate::Error::Query(
                "only SELECT queries are allowed for materialized views".into(),
            )),
        }
    }

    /// Create a new materialized view from a SQL definition.
    ///
    /// Creates a table `mv_{name}` populated by running the SQL query.
    /// The name must be alphanumeric/underscore and the SQL must be a SELECT query.
    pub async fn create_view(&self, name: &str, sql: &str) -> crate::Result<()> {
        Self::validate_name(name)?;
        Self::validate_select(sql)?;

        let table_name = format!("mv_{name}");

        let db = self.db.lock();
        db.execute_batch(&format!("DROP TABLE IF EXISTS \"{table_name}\""))
            .map_err(|e| crate::Error::Query(format!("drop existing view: {e}")))?;

        db.execute_batch(&format!("CREATE TABLE \"{table_name}\" AS {sql}"))
            .map_err(|e| crate::Error::Query(format!("create materialized view: {e}")))?;

        self.views.insert(
            name.to_string(),
            ViewDefinition {
                sql: sql.to_string(),
            },
        );

        tracing::info!(name, "materialized view created");
        Ok(())
    }

    /// Refresh a materialized view by re-running its query.
    pub async fn refresh(&self, name: &str) -> crate::Result<()> {
        let view = self
            .views
            .get(name)
            .ok_or_else(|| crate::Error::Query(format!("view not found: {name}")))?;

        let table_name = format!("mv_{name}");
        let sql = view.sql.clone();
        drop(view);

        let db = self.db.lock();
        db.execute_batch(&format!("DROP TABLE IF EXISTS \"{table_name}\""))
            .map_err(|e| crate::Error::Query(format!("drop for refresh: {e}")))?;
        db.execute_batch(&format!("CREATE TABLE \"{table_name}\" AS {sql}"))
            .map_err(|e| crate::Error::Query(format!("refresh view: {e}")))?;

        tracing::debug!(name, "materialized view refreshed");
        Ok(())
    }

    /// Drop a materialized view.
    pub async fn drop_view(&self, name: &str) -> crate::Result<()> {
        let table_name = format!("mv_{name}");

        let db = self.db.lock();
        db.execute_batch(&format!("DROP TABLE IF EXISTS \"{table_name}\""))
            .map_err(|e| crate::Error::Query(format!("drop view: {e}")))?;

        self.views.remove(name);
        tracing::info!(name, "materialized view dropped");
        Ok(())
    }

    /// List all registered views.
    pub fn list_views(&self) -> Vec<String> {
        self.views.iter().map(|entry| entry.key().clone()).collect()
    }
}

impl Default for MaterializedViewManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_and_drop_view() {
        let mgr = MaterializedViewManager::new();
        mgr.create_view("test_view", "SELECT 1::VARCHAR as v")
            .await
            .unwrap();
        assert_eq!(mgr.list_views().len(), 1);

        mgr.drop_view("test_view").await.unwrap();
        assert_eq!(mgr.list_views().len(), 0);
    }

    #[tokio::test]
    async fn refresh_view() {
        let mgr = MaterializedViewManager::new();

        {
            let db = mgr.db.lock();
            db.execute_batch(
                "CREATE TABLE data (v INT);
                 INSERT INTO data VALUES (1);",
            )
            .unwrap();
        }

        mgr.create_view("count_v", "SELECT count(*)::VARCHAR as cnt FROM data")
            .await
            .unwrap();

        // Add more data and refresh
        {
            let db = mgr.db.lock();
            db.execute_batch("INSERT INTO data VALUES (2); INSERT INTO data VALUES (3);")
                .unwrap();
        }

        mgr.refresh("count_v").await.unwrap();
    }

    #[tokio::test]
    async fn drop_nonexistent_view() {
        let mgr = MaterializedViewManager::new();
        mgr.drop_view("does_not_exist").await.unwrap();
    }

    #[tokio::test]
    async fn list_views() {
        let mgr = MaterializedViewManager::new();
        mgr.create_view("v1", "SELECT 1::VARCHAR as a")
            .await
            .unwrap();
        mgr.create_view("v2", "SELECT 2::VARCHAR as b")
            .await
            .unwrap();

        let mut views = mgr.list_views();
        views.sort();
        assert_eq!(views, vec!["v1", "v2"]);
    }

    #[tokio::test]
    async fn refresh_nonexistent_view_errors() {
        let mgr = MaterializedViewManager::new();
        let result = mgr.refresh("nope").await;
        assert!(result.is_err());
    }
}
