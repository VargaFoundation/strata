//! OIDC/OAuth2 integration — fetches and caches JWKS for RS256 token validation.
//!
//! Supports federated identity providers (Okta, Auth0, Azure AD, Google)
//! by validating JWTs against the provider's public keys (JWKS endpoint).

use dashmap::DashMap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use std::time::Instant;

use super::jwt::Claims;

/// OIDC configuration.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct OidcConfig {
    /// Enable OIDC authentication.
    pub enabled: bool,
    /// The OIDC issuer URL (e.g., "https://accounts.google.com").
    /// Used for `iss` claim validation.
    pub issuer_url: String,
    /// JWKS URI — endpoint serving the JSON Web Key Set.
    /// If empty, derived from issuer_url + "/.well-known/jwks.json".
    pub jwks_uri: String,
    /// Expected audience (`aud` claim).
    pub audience: String,
    /// Claim to use as the Ecphoria role (default: "role").
    pub role_claim: String,
    /// JWKS cache TTL in seconds (default: 3600 = 1 hour).
    pub jwks_cache_ttl_secs: u64,
}

impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            issuer_url: String::new(),
            jwks_uri: String::new(),
            audience: String::new(),
            role_claim: "role".into(),
            jwks_cache_ttl_secs: 3600,
        }
    }
}

/// Cached JWKS entry.
struct CachedJwk {
    key: DecodingKey,
    fetched_at: Instant,
}

/// OIDC validator that fetches and caches JWKS for RS256 token validation.
pub struct OidcValidator {
    config: OidcConfig,
    /// Cached decoding keys indexed by key ID (kid).
    key_cache: DashMap<String, CachedJwk>,
    http: reqwest::Client,
}

impl OidcValidator {
    /// Create a new OIDC validator with the given configuration.
    pub fn new(config: OidcConfig) -> Self {
        Self {
            config,
            key_cache: DashMap::new(),
            http: reqwest::Client::new(),
        }
    }

    /// Get the effective JWKS URI.
    fn jwks_uri(&self) -> String {
        if !self.config.jwks_uri.is_empty() {
            self.config.jwks_uri.clone()
        } else {
            format!(
                "{}/.well-known/jwks.json",
                self.config.issuer_url.trim_end_matches('/')
            )
        }
    }

    /// Validate an RS256 JWT token against the JWKS.
    ///
    /// Fetches the JWKS if not cached or cache expired. Extracts claims
    /// and maps them to Ecphoria's Claims structure.
    pub async fn validate_token(&self, token: &str) -> Result<Claims, crate::Error> {
        // Decode header to get the key ID (kid)
        let header = jsonwebtoken::decode_header(token)
            .map_err(|e| crate::Error::Auth(format!("invalid JWT header: {e}")))?;

        let kid = header
            .kid
            .ok_or_else(|| crate::Error::Auth("JWT missing kid header".into()))?;

        // Get or fetch the decoding key
        let decoding_key = self.get_key(&kid).await?;

        // Validate the token
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.config.issuer_url]);
        if !self.config.audience.is_empty() {
            validation.set_audience(&[&self.config.audience]);
        }
        validation.set_required_spec_claims(&["sub", "exp"]);

        // Decode into a generic claims map first
        let token_data =
            jsonwebtoken::decode::<serde_json::Value>(token, &decoding_key, &validation)
                .map_err(|e| crate::Error::Auth(format!("OIDC token validation failed: {e}")))?;

        let raw = token_data.claims;

        // Map to Ecphoria Claims
        let sub = raw
            .get("sub")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let role = raw
            .get(&self.config.role_claim)
            .and_then(|v| v.as_str())
            .unwrap_or("reader")
            .to_string();

        let exp = raw.get("exp").and_then(|v| v.as_u64()).unwrap_or(0);

        let agent_id = raw
            .get("agent_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let tenant_id = raw
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(Claims {
            sub,
            role,
            exp,
            agent_id,
            tenant_id,
        })
    }

    /// Get a cached decoding key, or fetch from JWKS endpoint.
    async fn get_key(&self, kid: &str) -> Result<DecodingKey, crate::Error> {
        let cache_ttl = std::time::Duration::from_secs(self.config.jwks_cache_ttl_secs);

        // Check cache
        if let Some(entry) = self.key_cache.get(kid) {
            if entry.fetched_at.elapsed() < cache_ttl {
                return Ok(entry.key.clone());
            }
        }

        // Fetch JWKS
        self.refresh_jwks().await?;

        // Try cache again after refresh
        self.key_cache
            .get(kid)
            .map(|entry| entry.key.clone())
            .ok_or_else(|| crate::Error::Auth(format!("key ID '{kid}' not found in JWKS")))
    }

    /// Fetch the JWKS from the provider and update the cache.
    async fn refresh_jwks(&self) -> Result<(), crate::Error> {
        let uri = self.jwks_uri();

        let resp = self
            .http
            .get(&uri)
            .send()
            .await
            .map_err(|e| crate::Error::Auth(format!("JWKS fetch failed: {e}")))?;

        let jwks: JwksResponse = resp
            .json()
            .await
            .map_err(|e| crate::Error::Auth(format!("JWKS parse failed: {e}")))?;

        let now = Instant::now();
        for key in &jwks.keys {
            if let (Some(kid), Some(n), Some(e)) = (&key.kid, &key.n, &key.e) {
                if let Ok(decoding_key) = DecodingKey::from_rsa_components(n, e) {
                    self.key_cache.insert(
                        kid.clone(),
                        CachedJwk {
                            key: decoding_key,
                            fetched_at: now,
                        },
                    );
                }
            }
        }

        tracing::info!(
            uri = %uri,
            keys = jwks.keys.len(),
            "JWKS refreshed"
        );

        Ok(())
    }

    /// Whether OIDC is configured and enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled && !self.config.issuer_url.is_empty()
    }
}

/// JWKS response format.
#[derive(Debug, serde::Deserialize)]
struct JwksResponse {
    keys: Vec<JwkKey>,
}

/// A single JWK key entry.
#[derive(Debug, serde::Deserialize)]
struct JwkKey {
    kid: Option<String>,
    #[allow(dead_code)]
    kty: Option<String>,
    n: Option<String>,
    e: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_oidc_config() {
        let config = OidcConfig::default();
        assert!(!config.enabled);
        assert!(config.issuer_url.is_empty());
        assert_eq!(config.role_claim, "role");
        assert_eq!(config.jwks_cache_ttl_secs, 3600);
    }

    #[test]
    fn jwks_uri_from_issuer() {
        let config = OidcConfig {
            issuer_url: "https://accounts.google.com".into(),
            ..Default::default()
        };
        let validator = OidcValidator::new(config);
        assert_eq!(
            validator.jwks_uri(),
            "https://accounts.google.com/.well-known/jwks.json"
        );
    }

    #[test]
    fn jwks_uri_explicit_override() {
        let config = OidcConfig {
            issuer_url: "https://issuer.example.com".into(),
            jwks_uri: "https://custom.example.com/keys".into(),
            ..Default::default()
        };
        let validator = OidcValidator::new(config);
        assert_eq!(validator.jwks_uri(), "https://custom.example.com/keys");
    }

    #[test]
    fn not_enabled_when_no_issuer() {
        let config = OidcConfig {
            enabled: true,
            ..Default::default()
        };
        let validator = OidcValidator::new(config);
        assert!(!validator.is_enabled());
    }

    #[test]
    fn enabled_with_issuer() {
        let config = OidcConfig {
            enabled: true,
            issuer_url: "https://auth.example.com".into(),
            ..Default::default()
        };
        let validator = OidcValidator::new(config);
        assert!(validator.is_enabled());
    }

    #[test]
    fn oidc_config_deserialize() {
        let toml_str = r#"
            enabled = true
            issuer_url = "https://auth.example.com"
            audience = "ecphoria-api"
            role_claim = "custom_role"
        "#;
        let config: OidcConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(config.issuer_url, "https://auth.example.com");
        assert_eq!(config.audience, "ecphoria-api");
        assert_eq!(config.role_claim, "custom_role");
    }
}
