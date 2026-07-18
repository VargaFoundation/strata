//! Webhook schema normalization — transforms vendor-specific webhook
//! payloads into standard Ecphoria events.

use chrono::Utc;
use uuid::Uuid;

use crate::memory::episodic::Event;

/// Normalize a raw webhook payload into Ecphoria events.
pub fn normalize_webhook(source: &str, payload: &serde_json::Value) -> crate::Result<Vec<Event>> {
    match source {
        "github" => normalize_github(payload),
        "sentry" => normalize_sentry(payload),
        "slack" => normalize_slack(payload),
        "pagerduty" => normalize_pagerduty(payload),
        _ => normalize_generic(source, payload),
    }
}

fn normalize_github(payload: &serde_json::Value) -> crate::Result<Vec<Event>> {
    let action = payload
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let repo = payload
        .pointer("/repository/full_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let sender = payload
        .pointer("/sender/login")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Detect event type from payload shape
    let event_type = if payload.get("pull_request").is_some() {
        format!("pull_request.{action}")
    } else if payload.get("issue").is_some() {
        format!("issue.{action}")
    } else if payload.get("commits").is_some() {
        "push".to_string()
    } else if payload.get("release").is_some() {
        format!("release.{action}")
    } else {
        format!("github.{action}")
    };

    Ok(vec![Event {
        id: Uuid::new_v4(),
        source: format!("github/{repo}"),
        event_type,
        payload: serde_json::json!({
            "action": action,
            "repository": repo,
            "sender": sender,
            "raw": payload,
        }),
        timestamp: Utc::now(),
        parent_id: None,
        trace_id: None,
        tags: vec![],
        idempotency_key: None,
    }])
}

fn normalize_sentry(payload: &serde_json::Value) -> crate::Result<Vec<Event>> {
    let action = payload
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("triggered");
    let project = payload
        .pointer("/data/issue/project/slug")
        .or_else(|| payload.pointer("/project_slug"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let title = payload
        .pointer("/data/issue/title")
        .or_else(|| payload.pointer("/message"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let level = payload
        .pointer("/data/issue/level")
        .and_then(|v| v.as_str())
        .unwrap_or("error");

    Ok(vec![Event {
        id: Uuid::new_v4(),
        source: format!("sentry/{project}"),
        event_type: format!("issue.{action}"),
        payload: serde_json::json!({
            "project": project,
            "title": title,
            "level": level,
            "action": action,
        }),
        timestamp: Utc::now(),
        parent_id: None,
        trace_id: None,
        tags: vec![],
        idempotency_key: None,
    }])
}

fn normalize_slack(payload: &serde_json::Value) -> crate::Result<Vec<Event>> {
    let event_type = payload
        .pointer("/event/type")
        .and_then(|v| v.as_str())
        .unwrap_or("message");
    let channel = payload
        .pointer("/event/channel")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let user = payload
        .pointer("/event/user")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let text = payload
        .pointer("/event/text")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    Ok(vec![Event {
        id: Uuid::new_v4(),
        source: format!("slack/{channel}"),
        event_type: event_type.to_string(),
        payload: serde_json::json!({
            "channel": channel,
            "user": user,
            "text": text,
        }),
        timestamp: Utc::now(),
        parent_id: None,
        trace_id: None,
        tags: vec![],
        idempotency_key: None,
    }])
}

fn normalize_pagerduty(payload: &serde_json::Value) -> crate::Result<Vec<Event>> {
    let event_type = payload
        .pointer("/event/event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("incident.trigger");
    let service = payload
        .pointer("/event/data/service/name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let title = payload
        .pointer("/event/data/title")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    Ok(vec![Event {
        id: Uuid::new_v4(),
        source: format!("pagerduty/{service}"),
        event_type: event_type.to_string(),
        payload: serde_json::json!({
            "service": service,
            "title": title,
            "event_type": event_type,
        }),
        timestamp: Utc::now(),
        parent_id: None,
        trace_id: None,
        tags: vec![],
        idempotency_key: None,
    }])
}

fn normalize_generic(source: &str, payload: &serde_json::Value) -> crate::Result<Vec<Event>> {
    let event_type = payload
        .get("event_type")
        .or_else(|| payload.get("type"))
        .or_else(|| payload.get("action"))
        .and_then(|v| v.as_str())
        .unwrap_or("webhook")
        .to_string();

    Ok(vec![Event {
        id: Uuid::new_v4(),
        source: source.to_string(),
        event_type,
        payload: payload.clone(),
        timestamp: Utc::now(),
        parent_id: None,
        trace_id: None,
        tags: vec![],
        idempotency_key: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A bounded arbitrary `serde_json::Value` strategy (depth-limited) for fuzzing the webhook
    /// normalizers — which parse attacker-controlled vendor payloads.
    fn arb_json() -> impl Strategy<Value = serde_json::Value> {
        let leaf = prop_oneof![
            Just(serde_json::Value::Null),
            any::<bool>().prop_map(serde_json::Value::from),
            any::<i64>().prop_map(serde_json::Value::from),
            ".*".prop_map(serde_json::Value::from),
        ];
        leaf.prop_recursive(4, 32, 8, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..8).prop_map(serde_json::Value::Array),
                prop::collection::hash_map(".*", inner, 0..8)
                    .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(400))]

        /// The webhook normalizers must never panic on arbitrary vendor JSON — a webhook endpoint is
        /// internet-facing, so a panic on crafted input would be a DoS. Every source is exercised.
        #[test]
        fn normalizers_never_panic(payload in arb_json()) {
            for source in ["github", "sentry", "slack", "pagerduty", "unknown-vendor"] {
                let _ = normalize_webhook(source, &payload);
            }
        }
    }

    #[test]
    fn normalize_github_push() {
        let payload = serde_json::json!({
            "action": "completed",
            "commits": [{"id": "abc123"}],
            "repository": {"full_name": "org/repo"},
            "sender": {"login": "user1"}
        });
        let events = normalize_webhook("github", &payload).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source, "github/org/repo");
        assert_eq!(events[0].event_type, "push");
    }

    #[test]
    fn normalize_github_pr() {
        let payload = serde_json::json!({
            "action": "opened",
            "pull_request": {"number": 42},
            "repository": {"full_name": "org/repo"},
            "sender": {"login": "user1"}
        });
        let events = normalize_webhook("github", &payload).unwrap();
        assert_eq!(events[0].event_type, "pull_request.opened");
    }

    #[test]
    fn normalize_sentry_issue() {
        let payload = serde_json::json!({
            "action": "created",
            "data": {
                "issue": {
                    "title": "TypeError: null is not an object",
                    "level": "error",
                    "project": {"slug": "frontend"}
                }
            }
        });
        let events = normalize_webhook("sentry", &payload).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source, "sentry/frontend");
        assert_eq!(events[0].event_type, "issue.created");
    }

    #[test]
    fn normalize_slack_message() {
        let payload = serde_json::json!({
            "event": {
                "type": "message",
                "channel": "C123",
                "user": "U456",
                "text": "Hello world"
            }
        });
        let events = normalize_webhook("slack", &payload).unwrap();
        assert_eq!(events[0].source, "slack/C123");
        assert_eq!(events[0].event_type, "message");
    }

    #[test]
    fn normalize_pagerduty_incident() {
        let payload = serde_json::json!({
            "event": {
                "event_type": "incident.triggered",
                "data": {
                    "title": "High CPU",
                    "service": {"name": "api-prod"}
                }
            }
        });
        let events = normalize_webhook("pagerduty", &payload).unwrap();
        assert_eq!(events[0].source, "pagerduty/api-prod");
        assert_eq!(events[0].event_type, "incident.triggered");
    }

    #[test]
    fn normalize_generic_webhook() {
        let payload = serde_json::json!({"event_type": "deploy", "env": "prod"});
        let events = normalize_webhook("custom-ci", &payload).unwrap();
        assert_eq!(events[0].source, "custom-ci");
        assert_eq!(events[0].event_type, "deploy");
    }

    #[test]
    fn normalize_unknown_generic() {
        let payload = serde_json::json!({"data": "something"});
        let events = normalize_webhook("unknown", &payload).unwrap();
        assert_eq!(events[0].event_type, "webhook");
    }
}
