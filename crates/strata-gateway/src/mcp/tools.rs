//! MCP tool handlers — executable tools exposed via MCP.

/// List available MCP tools with their JSON Schema input definitions.
pub fn list_tools() -> Vec<McpTool> {
    vec![
        McpTool {
            name: "query".into(),
            description: "Execute a read-only SQL query against Strata's episodic memory (DuckDB). Returns rows as JSON.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT query to execute against the episodic table. Only read-only queries are allowed."
                    }
                },
                "required": ["sql"]
            }),
        },
        McpTool {
            name: "ingest".into(),
            description: "Ingest events into episodic memory. Events are automatically embedded if an embedding provider is configured.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "Source identifier for the events (e.g. 'my-app', 'support-bot')"
                    },
                    "events": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "event_type": {
                                    "type": "string",
                                    "description": "Type of event (e.g. 'user.signup', 'order.placed')"
                                }
                            }
                        },
                        "description": "Array of event objects to ingest. Each event should have at least an event_type field."
                    }
                },
                "required": ["source", "events"]
            }),
        },
        McpTool {
            name: "search".into(),
            description: "Semantic similarity search across stored knowledge. Provide text to find the most relevant entries by meaning.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Natural language text to search for semantically similar entries"
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of results to return (default: 5)",
                        "default": 5
                    }
                },
                "required": ["text"]
            }),
        },
        McpTool {
            name: "get_state".into(),
            description: "Get the current value of an agent's state key. Returns the value, version, and metadata.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": {
                        "type": "string",
                        "description": "The agent identifier"
                    },
                    "key": {
                        "type": "string",
                        "description": "The state key to retrieve"
                    }
                },
                "required": ["agent_id", "key"]
            }),
        },
        McpTool {
            name: "set_state".into(),
            description: "Set an agent's state key to a new value. Returns the new version number.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": {
                        "type": "string",
                        "description": "The agent identifier"
                    },
                    "key": {
                        "type": "string",
                        "description": "The state key to set"
                    },
                    "value": {
                        "description": "The value to store (any JSON value)"
                    }
                },
                "required": ["agent_id", "key", "value"]
            }),
        },
        McpTool {
            name: "embed".into(),
            description: "Compute vector embeddings for the given text using the configured embedding provider (Ollama or OpenAI).".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Text to compute embeddings for"
                    }
                },
                "required": ["text"]
            }),
        },
        McpTool {
            name: "start_session".into(),
            description: "Start a new conversation session for an agent. Events can be associated with the session via session_id.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Unique session identifier"
                    },
                    "agent_id": {
                        "type": "string",
                        "description": "The agent this session belongs to"
                    },
                    "parent_session_id": {
                        "type": "string",
                        "description": "Optional parent session ID for nested conversations"
                    }
                },
                "required": ["session_id", "agent_id"]
            }),
        },
        McpTool {
            name: "end_session".into(),
            description: "End an active conversation session, optionally providing a summary.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session to end"
                    },
                    "summary": {
                        "type": "string",
                        "description": "Optional summary of the conversation session"
                    }
                },
                "required": ["session_id"]
            }),
        },
        McpTool {
            name: "recall_session".into(),
            description: "Recall all events from a conversation session, ordered chronologically.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session to recall"
                    }
                },
                "required": ["session_id"]
            }),
        },
        McpTool {
            name: "add_memory".into(),
            description: "Remember a fact about a user/agent. Deduplicates similar memories and, when a 'subject' is given, supersedes any older conflicting memory (bi-temporal). Scope it with user_id/agent_id/session_id.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "content": {"type": "string", "description": "The fact/statement to remember"},
                    "subject": {"type": "string", "description": "Optional stable key the memory is about (e.g. 'favorite_color') — enables contradiction resolution"},
                    "user_id": {"type": "string", "description": "Scope the memory to a user"},
                    "agent_id": {"type": "string", "description": "Scope the memory to an agent"},
                    "session_id": {"type": "string", "description": "Scope the memory to a session"},
                    "importance": {"type": "number", "description": "Optional importance 0.0–1.0"}
                },
                "required": ["content"]
            }),
        },
        McpTool {
            name: "search_memory".into(),
            description: "Search a user's/agent's memories by meaning (semantic when embeddings are configured, otherwise by importance/recency).".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "What to recall"},
                    "user_id": {"type": "string"},
                    "agent_id": {"type": "string"},
                    "session_id": {"type": "string"},
                    "k": {"type": "integer", "description": "Number of results (default 5)"}
                },
                "required": ["query"]
            }),
        },
        McpTool {
            name: "get_memories".into(),
            description: "List the active memories in a scope (user/agent/session).".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "agent_id": {"type": "string"},
                    "session_id": {"type": "string"},
                    "limit": {"type": "integer", "description": "Max memories to return (default 50)"}
                }
            }),
        },
        McpTool {
            name: "memory_history".into(),
            description: "Get the full temporal history of a memory by id — every superseded version, oldest first.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "The memory id"}
                },
                "required": ["id"]
            }),
        },
        McpTool {
            name: "delete_memory".into(),
            description: "Delete a memory by id.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "The memory id"}
                },
                "required": ["id"]
            }),
        },
        McpTool {
            name: "remember".into(),
            description: "Remember free-form text about a user/agent. With LLM extraction enabled it distills atomic facts (deduplicated and contradiction-resolved); otherwise it stores the text as a single memory.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Free-form text / conversation snippet to remember"},
                    "user_id": {"type": "string"},
                    "agent_id": {"type": "string"},
                    "session_id": {"type": "string"}
                },
                "required": ["text"]
            }),
        },
    ]
}

/// An MCP tool descriptor with JSON Schema input definition.
#[derive(Debug, Clone, serde::Serialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_tools_returns_expected_count() {
        let tools = list_tools();
        assert_eq!(tools.len(), 15); // 6 core + 3 session + 6 memory tools
    }

    #[test]
    fn list_tools_contains_memory_operations() {
        let tools = list_tools();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"add_memory"));
        assert!(names.contains(&"search_memory"));
        assert!(names.contains(&"get_memories"));
        assert!(names.contains(&"memory_history"));
        assert!(names.contains(&"delete_memory"));
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
            input_schema: serde_json::json!({"type": "object"}),
        };
        let json = serde_json::to_value(&tool).unwrap();
        assert_eq!(json["name"], "test");
        assert_eq!(json["description"], "A test tool");
        assert!(json["inputSchema"].is_object());
    }

    #[test]
    fn tools_have_input_schemas() {
        let tools = list_tools();
        for tool in &tools {
            assert!(
                tool.input_schema.is_object(),
                "tool {} has no inputSchema",
                tool.name
            );
            assert_eq!(
                tool.input_schema["type"], "object",
                "tool {} inputSchema must be type=object",
                tool.name
            );
        }
    }
}
