//! Custom SQL function registration for DuckDB.
//!
//! Registers UDFs like `embed()`, `cosine_similarity()`, `strata_search()`.

/// Register all custom SQL functions with the DuckDB connection.
pub fn register_all(_conn: &duckdb::Connection) -> crate::Result<()> {
    // TODO: register embed(), cosine_similarity(), strata_search(), strata_state()
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn placeholder() {
        // Function registration tested via integration tests
    }
}
