//! JWT token validation and generation.

/// Validate a JWT token and extract claims.
pub fn validate_token(_token: &str) -> Result<Claims, crate::Error> {
    // TODO: validate with jsonwebtoken crate
    Err(crate::Error::Auth("JWT validation not implemented".into()))
}

/// JWT claims payload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Claims {
    pub sub: String,
    pub role: String,
    pub exp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_token_returns_error() {
        // JWT not implemented yet — should return Auth error
        let result = validate_token("some.jwt.token");
        assert!(result.is_err());
    }

    #[test]
    fn claims_serialization_roundtrip() {
        let claims = Claims {
            sub: "user-123".into(),
            role: "admin".into(),
            exp: 1700000000,
        };
        let json = serde_json::to_string(&claims).unwrap();
        let deserialized: Claims = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sub, "user-123");
        assert_eq!(deserialized.role, "admin");
        assert_eq!(deserialized.exp, 1700000000);
    }
}
