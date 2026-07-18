//! JWT token validation and claims extraction.

use jsonwebtoken::{Algorithm, DecodingKey, Validation};

/// JWT claims payload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Claims {
    /// Subject — the agent or user identity.
    pub sub: String,
    /// Role: admin | writer | reader | agent.
    pub role: String,
    /// Expiration timestamp (Unix epoch seconds).
    pub exp: u64,
    /// Optional: the specific agent_id this token is scoped to (for Agent role).
    #[serde(default)]
    pub agent_id: Option<String>,
    /// Optional: tenant ID for multi-tenancy row-level security.
    #[serde(default)]
    pub tenant_id: Option<String>,
}

/// Validate a JWT token and extract claims.
///
/// `secret` is the HMAC-SHA256 shared secret configured via `gateway.jwt_secret`.
pub fn validate_token(token: &str, secret: &str) -> Result<Claims, crate::Error> {
    let key = DecodingKey::from_secret(secret.as_bytes());
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_required_spec_claims(&["sub", "role", "exp"]);
    // jsonwebtoken checks `exp` automatically when present.

    let token_data = jsonwebtoken::decode::<Claims>(token, &key, &validation)
        .map_err(|e| crate::Error::Auth(format!("JWT validation failed: {e}")))?;

    Ok(token_data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_token(claims: &Claims, secret: &str) -> String {
        let key = jsonwebtoken::EncodingKey::from_secret(secret.as_bytes());
        let header = jsonwebtoken::Header::new(Algorithm::HS256);
        jsonwebtoken::encode(&header, claims, &key).unwrap()
    }

    #[test]
    fn validate_valid_token() {
        let secret = "test-secret-key-256-bits-long!!!";
        let claims = Claims {
            sub: "agent-42".into(),
            role: "writer".into(),
            exp: (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as u64,
            agent_id: None,
            tenant_id: None,
        };
        let token = make_token(&claims, secret);
        let result = validate_token(&token, secret).unwrap();
        assert_eq!(result.sub, "agent-42");
        assert_eq!(result.role, "writer");
    }

    #[test]
    fn reject_expired_token() {
        let secret = "test-secret-key-256-bits-long!!!";
        let claims = Claims {
            sub: "user-1".into(),
            role: "admin".into(),
            exp: 1000, // long expired
            agent_id: None,
            tenant_id: None,
        };
        let token = make_token(&claims, secret);
        let err = validate_token(&token, secret).unwrap_err();
        assert!(err.to_string().contains("ExpiredSignature"), "got: {err}");
    }

    #[test]
    fn reject_wrong_secret() {
        let claims = Claims {
            sub: "user-1".into(),
            role: "reader".into(),
            exp: (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as u64,
            agent_id: None,
            tenant_id: None,
        };
        let token = make_token(&claims, "correct-secret-key-very-long!!");
        let err = validate_token(&token, "wrong-secret-key-very-long!!!").unwrap_err();
        assert!(err.to_string().contains("JWT validation failed"));
    }

    #[test]
    fn claims_serialization_roundtrip() {
        let claims = Claims {
            sub: "user-123".into(),
            role: "admin".into(),
            exp: 1700000000,
            agent_id: Some("agent-x".into()),
            tenant_id: None,
        };
        let json = serde_json::to_string(&claims).unwrap();
        let deserialized: Claims = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sub, "user-123");
        assert_eq!(deserialized.role, "admin");
        assert_eq!(deserialized.exp, 1700000000);
        assert_eq!(deserialized.agent_id.as_deref(), Some("agent-x"));
    }

    #[test]
    fn claims_without_agent_id() {
        let json = r#"{"sub":"u1","role":"reader","exp":9999999999}"#;
        let claims: Claims = serde_json::from_str(json).unwrap();
        assert!(claims.agent_id.is_none());
    }

    #[test]
    fn validate_agent_role_with_agent_id() {
        let secret = "test-secret-key-256-bits-long!!!";
        let claims = Claims {
            sub: "bot-7".into(),
            role: "agent".into(),
            exp: (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as u64,
            agent_id: Some("bot-7".into()),
            tenant_id: None,
        };
        let token = make_token(&claims, secret);
        let result = validate_token(&token, secret).unwrap();
        assert_eq!(result.role, "agent");
        assert_eq!(result.agent_id.as_deref(), Some("bot-7"));
    }
}
