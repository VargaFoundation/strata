//! Type mapping between DuckDB types and PostgreSQL wire types.

/// Convert a DuckDB column type name to a PostgreSQL OID.
///
/// Maps DuckDB's type system to PostgreSQL OIDs so that clients (psql, Grafana,
/// DBeaver, Metabase) correctly interpret column types instead of treating
/// everything as TEXT.
pub fn duckdb_type_to_pg_oid(duckdb_type: &str) -> u32 {
    // Normalize: uppercase, strip whitespace
    let t = duckdb_type.trim().to_uppercase();
    let t = t.as_str();

    match t {
        // Boolean
        "BOOLEAN" | "BOOL" => 16,

        // Integer types
        "TINYINT" | "INT1" => 21,  // INT2 (PG has no int1)
        "SMALLINT" | "INT2" => 21, // INT2
        "INTEGER" | "INT" | "INT4" | "SIGNED" => 23, // INT4
        "BIGINT" | "INT8" | "LONG" | "HUGEINT" => 20, // INT8

        // Unsigned integers → promote to next signed size
        "UTINYINT" | "USMALLINT" => 23, // INT4
        "UINTEGER" | "UBIGINT" => 20,   // INT8

        // Floating point
        "FLOAT" | "FLOAT4" | "REAL" => 700, // FLOAT4
        "DOUBLE" | "FLOAT8" => 701,         // FLOAT8
        "DECIMAL" | "NUMERIC" => 1700,      // NUMERIC

        // Text
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" => 25, // TEXT

        // Binary
        "BLOB" | "BYTEA" | "VARBINARY" => 17, // BYTEA

        // Date / Time
        "DATE" => 1082,                                     // DATE
        "TIME" | "TIME WITHOUT TIME ZONE" => 1083,          // TIME
        "TIMESTAMP" | "DATETIME" => 1114,                   // TIMESTAMP
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => 1184, // TIMESTAMPTZ
        "INTERVAL" => 1186,                                 // INTERVAL

        // JSON
        "JSON" => 114,   // JSON
        "JSONB" => 3802, // JSONB (though DuckDB JSON is closer to PG's json)

        // UUID
        "UUID" => 2950,

        // Arrays (DuckDB LIST type) — report as TEXT, clients parse the string
        _ if t.ends_with("[]") || t.starts_with("LIST") => 25,

        // Structs, maps, unions — report as TEXT
        _ if t.starts_with("STRUCT") || t.starts_with("MAP") || t.starts_with("UNION") => 25,

        // Default: TEXT (safe fallback)
        _ => 25,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varchar_maps_to_text() {
        assert_eq!(duckdb_type_to_pg_oid("VARCHAR"), 25);
    }

    #[test]
    fn integer_types() {
        assert_eq!(duckdb_type_to_pg_oid("INTEGER"), 23);
        assert_eq!(duckdb_type_to_pg_oid("INT"), 23);
        assert_eq!(duckdb_type_to_pg_oid("INT4"), 23);
        assert_eq!(duckdb_type_to_pg_oid("BIGINT"), 20);
        assert_eq!(duckdb_type_to_pg_oid("SMALLINT"), 21);
    }

    #[test]
    fn float_types() {
        assert_eq!(duckdb_type_to_pg_oid("FLOAT"), 700);
        assert_eq!(duckdb_type_to_pg_oid("DOUBLE"), 701);
        assert_eq!(duckdb_type_to_pg_oid("FLOAT8"), 701);
    }

    #[test]
    fn boolean_type() {
        assert_eq!(duckdb_type_to_pg_oid("BOOLEAN"), 16);
        assert_eq!(duckdb_type_to_pg_oid("BOOL"), 16);
    }

    #[test]
    fn timestamp_types() {
        assert_eq!(duckdb_type_to_pg_oid("TIMESTAMP"), 1114);
        assert_eq!(duckdb_type_to_pg_oid("TIMESTAMPTZ"), 1184);
        assert_eq!(duckdb_type_to_pg_oid("DATE"), 1082);
    }

    #[test]
    fn json_types() {
        assert_eq!(duckdb_type_to_pg_oid("JSON"), 114);
    }

    #[test]
    fn uuid_type() {
        assert_eq!(duckdb_type_to_pg_oid("UUID"), 2950);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(duckdb_type_to_pg_oid("integer"), 23);
        assert_eq!(duckdb_type_to_pg_oid("Boolean"), 16);
        assert_eq!(duckdb_type_to_pg_oid("  varchar  "), 25);
    }

    #[test]
    fn unknown_defaults_to_text() {
        assert_eq!(duckdb_type_to_pg_oid("CUSTOM_TYPE"), 25);
    }
}
