//! Output formatting for CLI results.

/// Print a JSON value as a formatted table or raw JSON.
pub fn print_json(value: &serde_json::Value, json_output: bool) {
    if json_output {
        println!("{}", serde_json::to_string_pretty(value).unwrap());
    } else {
        // Simple key-value display for objects
        if let Some(obj) = value.as_object() {
            for (k, v) in obj {
                println!("{}: {}", k, v);
            }
        } else {
            println!("{}", serde_json::to_string_pretty(value).unwrap());
        }
    }
}

/// Print query results as a formatted ASCII table.
///
/// Expects a JSON object with `"columns"` (array of strings) and `"rows"`
/// (array of arrays). Falls back to raw JSON when the shape does not match.
pub fn print_table(value: &serde_json::Value) {
    let columns = match value.get("columns").and_then(|c| c.as_array()) {
        Some(cols) => cols
            .iter()
            .map(|c| c.as_str().unwrap_or("?").to_string())
            .collect::<Vec<_>>(),
        None => {
            // Fallback: try to display as pretty JSON
            println!("{}", serde_json::to_string_pretty(value).unwrap());
            return;
        }
    };

    let rows: Vec<Vec<String>> = value
        .get("rows")
        .and_then(|r| r.as_array())
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    if let Some(arr) = row.as_array() {
                        arr.iter().map(format_cell).collect()
                    } else {
                        vec![format_cell(row)]
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    // Print header
    print_row(&columns, &widths);
    print_separator(&widths);

    // Print data rows
    for row in &rows {
        print_row(row, &widths);
    }

    // Print row count
    let count = rows.len();
    if count == 1 {
        println!("(1 row)");
    } else {
        println!("({count} rows)");
    }
}

fn format_cell(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}

fn print_row(cells: &[String], widths: &[usize]) {
    let formatted: Vec<String> = cells
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            let w = widths.get(i).copied().unwrap_or(cell.len());
            format!(" {:<width$} ", cell, width = w)
        })
        .collect();
    println!("|{}|", formatted.join("|"));
}

fn print_separator(widths: &[usize]) {
    let segments: Vec<String> = widths.iter().map(|w| "-".repeat(w + 2)).collect();
    println!("|{}|", segments.join("|"));
}

/// Format a JSON value as a pretty-printed string.
#[cfg(test)]
pub fn format_json(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

/// Format a JSON value as a compact key-value string.
#[cfg(test)]
pub fn format_kv(value: &serde_json::Value) -> String {
    if let Some(obj) = value.as_object() {
        obj.iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        format_json(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_json_object() {
        let value = serde_json::json!({"status": "ok", "version": "0.1.0"});
        let formatted = format_json(&value);
        assert!(formatted.contains("status"));
        assert!(formatted.contains("ok"));
    }

    #[test]
    fn format_json_array() {
        let value = serde_json::json!([1, 2, 3]);
        let formatted = format_json(&value);
        assert!(formatted.contains("1"));
    }

    #[test]
    fn format_json_scalar() {
        let value = serde_json::json!(42);
        let formatted = format_json(&value);
        assert_eq!(formatted, "42");
    }

    #[test]
    fn format_kv_object() {
        let value = serde_json::json!({"a": 1, "b": 2});
        let formatted = format_kv(&value);
        assert!(formatted.contains("a: 1"));
        assert!(formatted.contains("b: 2"));
    }

    #[test]
    fn format_kv_non_object_falls_back() {
        let value = serde_json::json!("hello");
        let formatted = format_kv(&value);
        assert!(formatted.contains("hello"));
    }
}
