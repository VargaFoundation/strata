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
