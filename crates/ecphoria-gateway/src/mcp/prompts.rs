//! MCP prompt templates — reusable prompts for AI agents.

/// List available MCP prompts.
pub fn list_prompts() -> Vec<McpPrompt> {
    vec![
        McpPrompt {
            name: "analyze_events".into(),
            description: "Analyze recent events for a given source".into(),
        },
        McpPrompt {
            name: "summarize_state".into(),
            description: "Summarize current agent state".into(),
        },
    ]
}

/// An MCP prompt descriptor.
#[derive(Debug, Clone, serde::Serialize)]
pub struct McpPrompt {
    pub name: String,
    pub description: String,
}
