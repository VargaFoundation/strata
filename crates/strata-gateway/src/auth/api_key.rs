//! API key authentication.

/// Validate an API key against the configured keys.
pub fn validate_api_key(_key: &str) -> bool {
    // TODO: check against stored API keys
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_empty_key() {
        assert!(!validate_api_key(""));
    }
}
