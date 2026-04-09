//! Webhook schema normalization — transforms vendor-specific webhook
//! payloads into standard Strata events.

use crate::memory::episodic::Event;

/// Normalize a raw webhook payload into a Strata event.
pub fn normalize_webhook(_source: &str, _payload: &serde_json::Value) -> crate::Result<Vec<Event>> {
    // TODO: match source (github, sentry, pagerduty, etc.) and normalize
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_empty_payload() {
        let events = normalize_webhook("github", &serde_json::json!({})).unwrap();
        assert!(events.is_empty());
    }
}
