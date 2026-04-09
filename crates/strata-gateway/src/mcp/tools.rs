//! MCP tool handlers — executable tools exposed via MCP.

/// List available MCP tools.
pub fn list_tools() -> Vec<McpTool> {
    vec![
        McpTool {
            name: "query".into(),
            description: "Execute a SQL query against Strata".into(),
        },
        McpTool {
            name: "ingest".into(),
            description: "Ingest events into episodic memory".into(),
        },
        McpTool {
            name: "search".into(),
            description: "Semantic search across stored knowledge".into(),
        },
        McpTool {
            name: "get_state".into(),
            description: "Get agent state by key".into(),
        },
        McpTool {
            name: "set_state".into(),
            description: "Set agent state key-value".into(),
        },
        McpTool {
            name: "embed".into(),
            description: "Compute embeddings for text".into(),
        },
    ]
}

/// An MCP tool descriptor.
#[derive(Debug, Clone, serde::Serialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_tools_returns_expected_count() {
        let tools = list_tools();
        assert_eq!(tools.len(), 6);
    }

    #[test]
    fn list_tools_contains_query() {
        let tools = list_tools();
        assert!(tools.iter().any(|t| t.name == "query"));
    }

    #[test]
    fn list_tools_contains_ingest() {
        let tools = list_tools();
        assert!(tools.iter().any(|t| t.name == "ingest"));
    }

    #[test]
    fn list_tools_contains_search() {
        let tools = list_tools();
        assert!(tools.iter().any(|t| t.name == "search"));
    }

    #[test]
    fn list_tools_contains_state_operations() {
        let tools = list_tools();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"get_state"));
        assert!(names.contains(&"set_state"));
    }

    #[test]
    fn list_tools_contains_embed() {
        let tools = list_tools();
        assert!(tools.iter().any(|t| t.name == "embed"));
    }

    #[test]
    fn tools_have_descriptions() {
        let tools = list_tools();
        for tool in &tools {
            assert!(
                !tool.description.is_empty(),
                "tool {} has empty description",
                tool.name
            );
        }
    }

    #[test]
    fn tool_serializes_to_json() {
        let tool = McpTool {
            name: "test".into(),
            description: "A test tool".into(),
        };
        let json = serde_json::to_value(&tool).unwrap();
        assert_eq!(json["name"], "test");
        assert_eq!(json["description"], "A test tool");
    }
}
