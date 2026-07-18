//! API key authentication.
//!
//! Validation is now handled by `ApiKeyStore` in the middleware module.
//! This module is kept for backwards compatibility.

use super::middleware::ApiKeyStore;

/// Validate an API key against the given store.
pub fn validate_api_key(store: &ApiKeyStore, key: &str) -> bool {
    store.validate(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_empty_key() {
        let store = ApiKeyStore::new(vec!["valid-key".into()]);
        assert!(!validate_api_key(&store, ""));
    }

    #[test]
    fn accept_valid_key() {
        let store = ApiKeyStore::new(vec!["my-secret".into()]);
        assert!(validate_api_key(&store, "my-secret"));
    }

    #[test]
    fn reject_invalid_key() {
        let store = ApiKeyStore::new(vec!["my-secret".into()]);
        assert!(!validate_api_key(&store, "wrong"));
    }
}
