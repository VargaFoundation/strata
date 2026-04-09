//! MCP resource handlers — expose Strata data as MCP resources.

/// List available MCP resources.
pub fn list_resources() -> Vec<McpResource> {
    vec![
        McpResource {
            uri: "strata://episodic".into(),
            name: "Episodic Memory".into(),
            description: "Append-only event store".into(),
        },
        McpResource {
            uri: "strata://semantic".into(),
            name: "Semantic Memory".into(),
            description: "Vector embedding store".into(),
        },
        McpResource {
            uri: "strata://state".into(),
            name: "State Memory".into(),
            description: "Agent key-value state".into(),
        },
    ]
}

/// An MCP resource descriptor.
#[derive(Debug, Clone, serde::Serialize)]
pub struct McpResource {
    pub uri: String,
    pub name: String,
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_resources_returns_three_memory_types() {
        let resources = list_resources();
        assert_eq!(resources.len(), 3);
    }

    #[test]
    fn list_resources_contains_episodic() {
        let resources = list_resources();
        assert!(resources.iter().any(|r| r.uri == "strata://episodic"));
    }

    #[test]
    fn list_resources_contains_semantic() {
        let resources = list_resources();
        assert!(resources.iter().any(|r| r.uri == "strata://semantic"));
    }

    #[test]
    fn list_resources_contains_state() {
        let resources = list_resources();
        assert!(resources.iter().any(|r| r.uri == "strata://state"));
    }

    #[test]
    fn resources_have_names_and_descriptions() {
        let resources = list_resources();
        for r in &resources {
            assert!(!r.name.is_empty());
            assert!(!r.description.is_empty());
        }
    }

    #[test]
    fn resource_serializes_to_json() {
        let resource = McpResource {
            uri: "strata://test".into(),
            name: "Test".into(),
            description: "A test resource".into(),
        };
        let json = serde_json::to_value(&resource).unwrap();
        assert_eq!(json["uri"], "strata://test");
    }
}
