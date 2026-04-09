//! Type mapping between DuckDB types and PostgreSQL wire types.

/// Convert a DuckDB column type to a PostgreSQL OID.
pub fn duckdb_type_to_pg_oid(_duckdb_type: &str) -> u32 {
    // TODO: implement type mapping
    // TEXT = 25, INT4 = 23, FLOAT8 = 701, BOOL = 16, etc.
    25 // default to TEXT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_type_is_text() {
        assert_eq!(duckdb_type_to_pg_oid("VARCHAR"), 25);
    }
}
