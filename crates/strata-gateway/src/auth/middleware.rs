//! Authentication middleware — Tower layer for request authentication.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};

use super::audit::AuditLog;
use super::jwt;

/// Authentication context injected into request extensions.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub identity: String,
    pub role: Role,
    /// For Agent role: the specific agent_id this identity is scoped to.
    pub agent_id: Option<String>,
    /// Tenant ID for multi-tenancy row-level security.
    pub tenant_id: Option<String>,
}

/// User roles for RBAC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Admin,
    Writer,
    Reader,
    Agent,
}

impl Role {
    /// Parse a role string (case-insensitive).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(Self::Admin),
            "writer" => Some(Self::Writer),
            "reader" => Some(Self::Reader),
            "agent" => Some(Self::Agent),
            _ => None,
        }
    }

    /// Check whether this role is allowed to perform the given HTTP method.
    pub fn allows_method(&self, method: &Method) -> bool {
        match self {
            Role::Admin => true,
            Role::Writer => matches!(
                *method,
                Method::GET | Method::HEAD | Method::OPTIONS | Method::POST | Method::PUT
            ),
            Role::Reader => matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS),
            // Agent has the same method access as Writer — scoping is per agent_id,
            // enforced in handlers.
            Role::Agent => matches!(
                *method,
                Method::GET | Method::HEAD | Method::OPTIONS | Method::POST | Method::PUT
            ),
        }
    }

    /// Check whether this role may access admin-only paths.
    pub fn allows_admin_path(&self) -> bool {
        matches!(self, Role::Admin)
    }
}

/// What an API key grants — its tenant scope (or none) and role.
#[derive(Debug, Clone)]
struct ApiKeyInfo {
    tenant: Option<String>,
    role: Role,
}

/// SHA-256 digest of an API key. Keys are hashed at config load and never held in plaintext
/// afterwards; presented tokens are hashed and compared digest-to-digest in constant time.
type KeyDigest = [u8; 32];

fn digest_key(key: &str) -> KeyDigest {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.finalize().into()
}

/// Decode a 64-char hex string into a 32-byte digest.
fn hex_to_digest(s: &str) -> Option<KeyDigest> {
    if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Parse an API-key config entry, backward-compatibly:
/// - `"<key>"`              → Writer, no tenant (legacy behavior, unchanged)
/// - `"<key>@<tenant>"`     → Writer, scoped to `<tenant>`
/// - `"<key>@<tenant>:<role>"` → `<role>`, scoped to `<tenant>`
///
/// `<key>` is either the plaintext secret (hashed here, at load) or `sha256:<64-hex>` — the
/// SHA-256 of the secret — so config files never store a credential at rest. Returns `None`
/// for a `sha256:` entry with malformed hex: it must be skipped (fail-closed), never silently
/// downgraded to a plaintext key.
fn parse_api_key(entry: &str) -> Option<(KeyDigest, ApiKeyInfo)> {
    let (secret, info) = match entry.split_once('@') {
        None => (
            entry,
            ApiKeyInfo {
                tenant: None,
                role: Role::Writer,
            },
        ),
        Some((key, rest)) => {
            let (tenant, role) = match rest.split_once(':') {
                Some((t, r)) => (t, Role::from_str_loose(r).unwrap_or(Role::Writer)),
                None => (rest, Role::Writer),
            };
            (
                key,
                ApiKeyInfo {
                    tenant: (!tenant.is_empty()).then(|| tenant.to_string()),
                    role,
                },
            )
        }
    };
    let digest = match secret.strip_prefix("sha256:") {
        Some(hex) => hex_to_digest(hex)?,
        None => digest_key(secret),
    };
    Some((digest, info))
}

/// Shared authentication state for the middleware.
#[derive(Clone)]
pub struct AuthState {
    /// `(digest, grant)` pairs — a Vec (not a map) so lookups can scan every entry with a
    /// constant-time comparison instead of a timing-dependent hash lookup on the secret.
    keys: Arc<Vec<(KeyDigest, ApiKeyInfo)>>,
    jwt_secret: Option<Arc<String>>,
    oidc: Option<Arc<super::oidc::OidcValidator>>,
    rate_limiter: Option<Arc<RateLimiter>>,
    audit_log: Option<AuditLog>,
    /// When true, reject any non-Admin credential that carries no tenant (a "bare" API key, or a
    /// JWT/OIDC token with no `tenant_id`) so multi-tenant deployments can forbid unscoped access.
    require_tenant: bool,
    /// Shared cluster secret used to authenticate the `x-strata-shard-forwarded` marker, so a client
    /// can't forge it to bypass rate-limiting. `None` → no forwarded request is trusted to skip.
    shard_forward_secret: Option<Arc<String>>,
}

impl std::fmt::Debug for AuthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthState")
            .field("keys_count", &self.keys.len())
            .field("jwt_configured", &self.jwt_secret.is_some())
            .field("rate_limiter", &self.rate_limiter.is_some())
            .field("audit_log", &self.audit_log.is_some())
            .finish()
    }
}

impl AuthState {
    /// Create with OIDC support.
    pub fn with_oidc(
        api_keys: Vec<String>,
        jwt_secret: Option<String>,
        oidc_config: super::oidc::OidcConfig,
        rate_limit_per_key: u32,
    ) -> Self {
        let mut state = Self::new(api_keys, jwt_secret, rate_limit_per_key);
        if oidc_config.enabled && !oidc_config.issuer_url.is_empty() {
            state.oidc = Some(Arc::new(super::oidc::OidcValidator::new(oidc_config)));
            tracing::info!("OIDC authentication enabled");
        }
        state
    }

    pub fn new(api_keys: Vec<String>, jwt_secret: Option<String>, rate_limit_per_key: u32) -> Self {
        let rate_limiter = if rate_limit_per_key > 0 {
            Some(Arc::new(RateLimiter::new(rate_limit_per_key)))
        } else {
            None
        };
        let audit_log = match AuditLog::new() {
            Ok(log) => Some(log),
            Err(e) => {
                tracing::warn!(error = %e, "failed to initialize audit log — auditing disabled");
                None
            }
        };
        let mut keys: Vec<(KeyDigest, ApiKeyInfo)> = Vec::with_capacity(api_keys.len());
        for entry in &api_keys {
            match parse_api_key(entry) {
                Some(parsed) => keys.push(parsed),
                // Fail closed: a malformed sha256: digest must not become a usable plaintext key.
                None => tracing::warn!(
                    "ignoring malformed `sha256:` API-key entry (bad hex digest) — it will NOT authenticate"
                ),
            }
        }
        Self {
            keys: Arc::new(keys),
            jwt_secret: jwt_secret.map(Arc::new),
            oidc: None,
            rate_limiter,
            audit_log,
            require_tenant: false,
            shard_forward_secret: None,
        }
    }

    /// Opt-in: reject non-Admin credentials that carry no tenant (for multi-tenant deployments).
    pub fn with_require_tenant(mut self, require: bool) -> Self {
        self.require_tenant = require;
        self
    }

    /// Set the shared cluster secret used to authenticate the internal shard-forward marker.
    pub fn with_shard_forward_secret(mut self, secret: Option<String>) -> Self {
        self.shard_forward_secret = secret.map(Arc::new);
        self
    }

    /// Apply the `require_tenant` policy to a resolved context: a non-Admin identity with no tenant
    /// is a bare API key / tenant-less token (an unscoped superuser), rejected when the policy is on.
    fn enforce_tenant(&self, ctx: AuthContext) -> Option<AuthContext> {
        if self.require_tenant && ctx.tenant_id.is_none() && ctx.role != Role::Admin {
            tracing::warn!(
                identity = %ctx.identity,
                "rejected: require_tenant is enabled and the credential carries no tenant"
            );
            return None;
        }
        Some(ctx)
    }

    /// Constant-time lookup: hash the presented token, then compare it against every stored
    /// digest without early exit, so response timing reveals nothing about partial matches.
    fn find_key(&self, token: &str) -> Option<&ApiKeyInfo> {
        let digest = digest_key(token);
        let mut found: Option<&ApiKeyInfo> = None;
        for (stored, info) in self.keys.iter() {
            if ct_eq(stored, &digest) && found.is_none() {
                found = Some(info);
            }
        }
        found
    }

    fn validate_api_key(&self, key: &str) -> bool {
        self.find_key(key).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.jwt_secret.is_none()
    }

    /// Get a reference to the audit log (for the audit query handler).
    pub fn audit_log(&self) -> Option<&AuditLog> {
        self.audit_log.as_ref()
    }

    /// Validate a bearer token through the full chain (OIDC → JWT → API key).
    /// Returns the resolved [`AuthContext`], or `None` if the token is invalid.
    /// Shared by the REST middleware and the gRPC interceptor.
    pub async fn authenticate(&self, token: &str) -> Option<AuthContext> {
        // 1. OIDC (RS256 with JWKS)
        if let Some(ref oidc) = self.oidc {
            if let Ok(claims) = oidc.validate_token(token).await {
                let role = Role::from_str_loose(&claims.role).unwrap_or(Role::Reader);
                let agent_id = claims.agent_id.clone().or(if claims.role == "agent" {
                    Some(claims.sub.clone())
                } else {
                    None
                });
                return self.enforce_tenant(AuthContext {
                    identity: claims.sub,
                    role,
                    agent_id,
                    tenant_id: claims.tenant_id,
                });
            }
        }
        // 2. JWT (HS256 shared secret)
        if let Some(ref secret) = self.jwt_secret {
            if let Ok(claims) = jwt::validate_token(token, secret) {
                let role = Role::from_str_loose(&claims.role).unwrap_or(Role::Reader);
                let agent_id = claims.agent_id.clone().or(if claims.role == "agent" {
                    Some(claims.sub.clone())
                } else {
                    None
                });
                return self.enforce_tenant(AuthContext {
                    identity: claims.sub,
                    role,
                    agent_id,
                    tenant_id: claims.tenant_id,
                });
            }
        }
        // 3. API key — may carry a tenant + role (parsed from the key config; a bare key = Writer/none).
        if let Some(info) = self.find_key(token) {
            return self.enforce_tenant(AuthContext {
                identity: "api-key-user".into(),
                role: info.role.clone(),
                agent_id: None,
                tenant_id: info.tenant.clone(),
            });
        }
        None
    }

    /// Make the audit log durable (file-backed) at `path`. Empty or `:memory:` keeps it
    /// in-memory. Enterprise/compliance deployments should set a real path.
    pub fn with_audit_path(mut self, path: &str) -> Self {
        if !path.is_empty() && path != ":memory:" {
            match AuditLog::open(std::path::Path::new(path)) {
                Ok(log) => self.audit_log = Some(log),
                Err(e) => {
                    tracing::warn!(error = %e, "failed to open durable audit log — keeping in-memory")
                }
            }
        }
        self
    }
}

// ── Backwards-compatible ApiKeyStore alias ────────────────────────────

/// Shared set of valid API keys (backwards-compatible wrapper).
#[derive(Debug, Clone)]
pub struct ApiKeyStore {
    inner: AuthState,
}

impl ApiKeyStore {
    pub fn new(keys: Vec<String>) -> Self {
        Self {
            inner: AuthState::new(keys, None, 0),
        }
    }

    pub fn validate(&self, key: &str) -> bool {
        self.inner.validate_api_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.keys.is_empty()
    }
}

// ── Rate Limiter (token bucket) ──────────────────────────────────────

use dashmap::DashMap;

/// Per-key token bucket rate limiter.
#[derive(Debug)]
pub struct RateLimiter {
    /// tokens_per_sec is the refill rate AND the bucket capacity.
    tokens_per_sec: u32,
    buckets: DashMap<String, TokenBucket>,
}

#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(tokens_per_sec: u32) -> Self {
        Self {
            tokens_per_sec,
            buckets: DashMap::new(),
        }
    }

    /// Try to consume one token. Returns (allowed, remaining).
    pub fn try_acquire(&self, key: &str) -> (bool, u32) {
        let now = Instant::now();
        let cap = self.tokens_per_sec as f64;

        let mut entry = self
            .buckets
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket {
                tokens: cap,
                last_refill: now,
            });

        let bucket = entry.value_mut();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * cap).min(cap);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            (true, bucket.tokens as u32)
        } else {
            (false, 0)
        }
    }
}

// ── Middleware ────────────────────────────────────────────────────────

/// Axum middleware that validates Bearer tokens (JWT or API key).
///
/// Tries JWT first (if `jwt_secret` is configured), falls back to API key lookup.
/// On success, injects `AuthContext` into request extensions.
/// Enforces RBAC: rejects requests the role is not allowed to make.
/// Enforces per-key rate limits if configured.
pub async fn require_auth(
    axum::extract::State(state): axum::extract::State<AuthState>,
    mut req: Request,
    next: Next,
) -> Result<Response, Response> {
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        _ => return Err(StatusCode::UNAUTHORIZED.into_response()),
    };

    // Auth chain: OIDC (RS256) → JWT (HS256) → API key (shared with the gRPC interceptor).
    let auth_ctx = match state.authenticate(token).await {
        Some(ctx) => ctx,
        None => return Err(StatusCode::UNAUTHORIZED.into_response()),
    };

    // ── RBAC: check method permission ────────────────────────────
    if !auth_ctx.role.allows_method(req.method()) {
        return Err(StatusCode::FORBIDDEN.into_response());
    }

    // ── RBAC: admin-only paths ───────────────────────────────────
    // `/admin/*` and Raft cluster membership control (`/cluster/*`) require the Admin role.
    let path = req.uri().path().to_string();
    if (path.contains("/admin/") || path.contains("/cluster/"))
        && !auth_ctx.role.allows_admin_path()
    {
        return Err(StatusCode::FORBIDDEN.into_response());
    }

    // ── Agent scope: reject access to other agents' state ────────
    if auth_ctx.role == Role::Agent {
        if let Some(ref scoped_agent) = auth_ctx.agent_id {
            // Paths like /api/v1/state/{agent_id}/{key}
            if path.starts_with("/state/") || path.contains("/state/") {
                let segments: Vec<&str> = path.split('/').collect();
                // Pattern: .../state/{agent_id}/{key}
                if let Some(pos) = segments.iter().position(|&s| s == "state") {
                    if let Some(path_agent) = segments.get(pos + 1) {
                        if *path_agent != scoped_agent.as_str() {
                            return Err(StatusCode::FORBIDDEN.into_response());
                        }
                    }
                }
            }
        }
    }

    // ── Rate limiting ────────────────────────────────────────────
    // A request reverse-proxied from another shard was already rate-limited on the origin pod; don't
    // double-count it. But only trust the `x-strata-shard-forwarded` marker when it carries the
    // shared cluster secret (constant-time checked) — otherwise a client could set the header to
    // bypass rate-limiting. With no secret configured, the marker is never trusted (re-count).
    let is_shard_forwarded = state.shard_forward_secret.as_ref().is_some_and(|secret| {
        req.headers()
            .get("x-strata-shard-forwarded")
            .map(|v| ct_eq(v.as_bytes(), secret.as_bytes()))
            .unwrap_or(false)
    });
    // Bucket per (identity, tenant) so a noisy tenant on a shared key can't exhaust others' budget.
    let rl_key = match &auth_ctx.tenant_id {
        Some(t) => format!("{}\u{1f}{}", auth_ctx.identity, t),
        None => auth_ctx.identity.clone(),
    };
    if let Some(ref limiter) = state.rate_limiter {
        if is_shard_forwarded {
            // Skip acquisition; still authenticated above.
        } else {
            let (allowed, remaining) = limiter.try_acquire(&rl_key);
            if !allowed {
                let mut resp = StatusCode::TOO_MANY_REQUESTS.into_response();
                resp.headers_mut()
                    .insert("X-RateLimit-Remaining", "0".parse().unwrap());
                return Err(resp);
            }
            // Store remaining for the response header (injected after handler runs)
            req.extensions_mut().insert(RateLimitInfo { remaining });
        }
    }

    // Capture for audit logging
    let audit_identity = auth_ctx.identity.clone();
    let audit_tenant = auth_ctx.tenant_id.clone();
    let audit_ip = client_ip(&req);
    let audit_method = req.method().to_string();
    let audit_path = path.clone();
    let audit_log = state.audit_log.clone();
    let audit_start = Instant::now();

    req.extensions_mut().insert(auth_ctx);

    let mut response = next.run(req).await;

    // Inject rate limit header into the response
    if state.rate_limiter.is_some() {
        let remaining = response
            .extensions()
            .get::<RateLimitInfo>()
            .map(|info| info.remaining);
        if let Some(remaining) = remaining {
            response.headers_mut().insert(
                "X-RateLimit-Remaining",
                remaining.to_string().parse().unwrap(),
            );
        }
    }

    // Record audit entry
    if let Some(ref log) = audit_log {
        let status = response.status().as_u16();
        let duration = audit_start.elapsed();
        log.record(
            &audit_identity,
            &audit_method,
            &audit_path,
            status,
            duration,
            audit_tenant.as_deref(),
            audit_ip.as_deref(),
        );
    }

    Ok(response)
}

/// Constant-time byte-slice equality (the length is not secret). Prevents a timing side-channel when
/// comparing the shard-forward marker against the cluster secret.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Best-effort client IP from proxy headers (`X-Forwarded-For` first hop, else `X-Real-IP`).
fn client_ip(req: &Request) -> Option<String> {
    let h = req.headers();
    h.get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            h.get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        })
}

/// Carried through request extensions to inject the rate-limit header into the response.
#[derive(Debug, Clone)]
struct RateLimitInfo {
    remaining: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn role_equality() {
        assert_eq!(Role::Admin, Role::Admin);
        assert_ne!(Role::Admin, Role::Reader);
        assert_ne!(Role::Writer, Role::Agent);
    }

    #[test]
    fn auth_context_clone() {
        let ctx = AuthContext {
            identity: "user-1".into(),
            role: Role::Admin,
            agent_id: None,
            tenant_id: None,
        };
        let cloned = ctx.clone();
        assert_eq!(cloned.identity, "user-1");
        assert_eq!(cloned.role, Role::Admin);
    }

    #[test]
    fn auth_context_debug() {
        let ctx = AuthContext {
            identity: "agent-bot".into(),
            role: Role::Agent,
            agent_id: Some("agent-bot".into()),
            tenant_id: Some("tenant-1".into()),
        };
        let debug = format!("{:?}", ctx);
        assert!(debug.contains("agent-bot"));
        assert!(debug.contains("Agent"));
    }

    #[test]
    fn all_roles_are_distinct() {
        let roles = [Role::Admin, Role::Writer, Role::Reader, Role::Agent];
        for (i, a) in roles.iter().enumerate() {
            for (j, b) in roles.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn api_key_store_validates() {
        let store = ApiKeyStore::new(vec!["secret-123".into(), "key-456".into()]);
        assert!(store.validate("secret-123"));
        assert!(store.validate("key-456"));
        assert!(!store.validate("invalid"));
        assert!(!store.validate(""));
    }

    #[test]
    fn api_key_store_empty() {
        let store = ApiKeyStore::new(vec![]);
        assert!(store.is_empty());
        assert!(!store.validate("anything"));
    }

    #[test]
    fn role_from_str_loose() {
        assert_eq!(Role::from_str_loose("admin"), Some(Role::Admin));
        assert_eq!(Role::from_str_loose("ADMIN"), Some(Role::Admin));
        assert_eq!(Role::from_str_loose("Writer"), Some(Role::Writer));
        assert_eq!(Role::from_str_loose("reader"), Some(Role::Reader));
        assert_eq!(Role::from_str_loose("agent"), Some(Role::Agent));
        assert_eq!(Role::from_str_loose("unknown"), None);
    }

    #[test]
    fn reader_allows_get_only() {
        assert!(Role::Reader.allows_method(&Method::GET));
        assert!(Role::Reader.allows_method(&Method::HEAD));
        assert!(!Role::Reader.allows_method(&Method::POST));
        assert!(!Role::Reader.allows_method(&Method::PUT));
        assert!(!Role::Reader.allows_method(&Method::DELETE));
    }

    #[test]
    fn writer_allows_get_and_post() {
        assert!(Role::Writer.allows_method(&Method::GET));
        assert!(Role::Writer.allows_method(&Method::POST));
        assert!(Role::Writer.allows_method(&Method::PUT));
        assert!(!Role::Writer.allows_method(&Method::DELETE));
    }

    #[test]
    fn admin_allows_everything() {
        assert!(Role::Admin.allows_method(&Method::GET));
        assert!(Role::Admin.allows_method(&Method::POST));
        assert!(Role::Admin.allows_method(&Method::PUT));
        assert!(Role::Admin.allows_method(&Method::DELETE));
    }

    #[test]
    fn only_admin_can_access_admin_paths() {
        assert!(Role::Admin.allows_admin_path());
        assert!(!Role::Writer.allows_admin_path());
        assert!(!Role::Reader.allows_admin_path());
        assert!(!Role::Agent.allows_admin_path());
    }

    #[tokio::test]
    async fn api_keys_can_be_tenant_and_role_scoped() {
        // The client sends the SECRET part (before '@'); tenant + role are server-side config.
        let state = AuthState::new(
            vec![
                "bare".into(),
                "sk_acme@acme:reader".into(),
                "sk_beta@beta".into(),
            ],
            None,
            0,
        );
        // Bare key = Writer, no tenant (legacy behavior, unchanged).
        let c = state.authenticate("bare").await.unwrap();
        assert_eq!(c.role, Role::Writer);
        assert!(c.tenant_id.is_none());
        // Scoped key → the configured role + tenant.
        let c = state.authenticate("sk_acme").await.unwrap();
        assert_eq!(c.role, Role::Reader);
        assert_eq!(c.tenant_id.as_deref(), Some("acme"));
        // key@tenant without a role defaults to Writer.
        let c = state.authenticate("sk_beta").await.unwrap();
        assert_eq!(c.role, Role::Writer);
        assert_eq!(c.tenant_id.as_deref(), Some("beta"));
        // A wrong secret is rejected.
        assert!(state.authenticate("nope").await.is_none());
    }

    #[tokio::test]
    async fn require_tenant_rejects_bare_credentials() {
        let state = AuthState::new(vec!["bare".into(), "sk_acme@acme".into()], None, 0)
            .with_require_tenant(true);
        // A bare key (Writer, no tenant) is rejected under require_tenant.
        assert!(state.authenticate("bare").await.is_none());
        // A tenant-scoped key still authenticates.
        assert!(state.authenticate("sk_acme").await.is_some());
    }

    #[tokio::test]
    async fn require_tenant_allows_admin_without_tenant() {
        let state = AuthState::new(vec!["adm@:admin".into()], None, 0).with_require_tenant(true);
        // An Admin with no tenant is intentionally cross-tenant → still allowed.
        let ctx = state.authenticate("adm").await;
        assert_eq!(ctx.map(|c| c.role), Some(Role::Admin));
    }

    #[tokio::test]
    async fn bare_key_allowed_when_require_tenant_off() {
        // Default (off) preserves the legacy behaviour: a bare key authenticates.
        let state = AuthState::new(vec!["bare".into()], None, 0);
        assert!(state.authenticate("bare").await.is_some());
    }

    #[test]
    fn ct_eq_matches_only_equal_slices() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secreT"));
        assert!(!ct_eq(b"secret", b"secre"));
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn rate_limiter_basic() {
        let limiter = RateLimiter::new(5);
        // First 5 should succeed
        for _ in 0..5 {
            let (allowed, _) = limiter.try_acquire("key-1");
            assert!(allowed);
        }
        // 6th should fail (bucket empty)
        let (allowed, remaining) = limiter.try_acquire("key-1");
        assert!(!allowed);
        assert_eq!(remaining, 0);
    }

    #[test]
    fn rate_limiter_independent_keys() {
        let limiter = RateLimiter::new(2);
        let (allowed, _) = limiter.try_acquire("a");
        assert!(allowed);
        let (allowed, _) = limiter.try_acquire("a");
        assert!(allowed);
        // "a" exhausted, but "b" should still work
        let (allowed, _) = limiter.try_acquire("b");
        assert!(allowed);
    }

    #[test]
    fn auth_state_with_jwt() {
        let state = AuthState::new(vec!["key1".into()], Some("my-secret".into()), 0);
        assert!(!state.is_empty());
        assert!(state.validate_api_key("key1"));
        assert!(!state.validate_api_key("bad"));
    }

    #[test]
    fn auth_state_empty() {
        let state = AuthState::new(vec![], None, 0);
        assert!(state.is_empty());
    }

    fn digest_hex(secret: &str) -> String {
        digest_key(secret)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    #[tokio::test]
    async fn hashed_key_entry_authenticates_the_plaintext_secret() {
        // Config stores only the SHA-256 digest; the client still sends the secret.
        let hex = digest_hex("topsecret");
        let state = AuthState::new(vec![format!("sha256:{hex}@acme:reader")], None, 0);
        let ctx = state.authenticate("topsecret").await.unwrap();
        assert_eq!(ctx.role, Role::Reader);
        assert_eq!(ctx.tenant_id.as_deref(), Some("acme"));
        // The digest itself is not the credential.
        assert!(state.authenticate(&hex).await.is_none());
        assert!(state.authenticate(&format!("sha256:{hex}")).await.is_none());
    }

    #[test]
    fn malformed_sha256_entry_fails_closed() {
        // A bad digest must be dropped — not silently treated as a plaintext key.
        let state = AuthState::new(vec!["sha256:not-valid-hex@acme".into()], None, 0);
        assert!(!state.validate_api_key("sha256:not-valid-hex"));
        assert!(!state.validate_api_key("not-valid-hex"));
        assert!(state.is_empty(), "malformed entry must be skipped entirely");
    }

    #[test]
    fn hex_to_digest_roundtrip_and_rejects() {
        let d = digest_key("abc");
        let hex: String = d.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex_to_digest(&hex), Some(d));
        assert_eq!(hex_to_digest(&hex.to_uppercase()), Some(d));
        assert!(hex_to_digest("zz").is_none());
        assert!(hex_to_digest(&hex[..62]).is_none());
    }
}
