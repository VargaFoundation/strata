//! `ecphoria doctor` — a static configuration linter.
//!
//! Loads a `ecphoria.toml` (plus the `ECPHORIA_*` env overrides for the fields it checks) and reports
//! actionable misconfigurations *before* you start the server: an open (unauthenticated) database on
//! a public bind, a fail-closed auth setup, a weak JWT secret, an embedding/index dimension
//! mismatch, an unauthenticated Raft cluster, and plaintext API keys. Purely local — it makes no
//! network calls.

use std::fmt::Write as _;

/// Severity of a finding. `Error` means the server will refuse to start (or a serious risk);
/// `Warn` is a likely-misconfiguration; `Info` is advisory.
#[derive(PartialEq, Eq)]
enum Level {
    Error,
    Warn,
    Info,
}

struct Finding {
    level: Level,
    message: String,
}

pub async fn run(config_path: Option<&str>) -> anyhow::Result<()> {
    let path = config_path
        .map(|s| s.to_string())
        .or_else(|| std::env::var("ECPHORIA_CONFIG").ok())
        .unwrap_or_else(|| "ecphoria.toml".to_string());

    let toml_val: toml::Value = match std::fs::read_to_string(&path) {
        Ok(s) => toml::from_str(&s).map_err(|e| anyhow::anyhow!("failed to parse {path}: {e}"))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // No file: lint pure defaults + env (still useful in container/env-only deployments).
            eprintln!("note: {path} not found — linting defaults + ECPHORIA_* env only\n");
            toml::Value::Table(Default::default())
        }
        Err(e) => return Err(anyhow::anyhow!("failed to read {path}: {e}")),
    };

    let findings = lint(&toml_val, &EnvLookup::process());
    print_findings(&findings);

    if findings.iter().any(|f| f.level == Level::Error) {
        std::process::exit(1);
    }
    Ok(())
}

/// Env-var source (indirected so lint() is unit-testable with a fake env).
type EnvGetter = Box<dyn Fn(&str) -> Option<String>>;
struct EnvLookup {
    get: EnvGetter,
}
impl EnvLookup {
    fn process() -> Self {
        Self {
            get: Box::new(|k| std::env::var(k).ok()),
        }
    }
    #[cfg(test)]
    fn fake(vars: std::collections::HashMap<String, String>) -> Self {
        Self {
            get: Box::new(move |k| vars.get(k).cloned()),
        }
    }
    fn get(&self, key: &str) -> Option<String> {
        (self.get)(key)
    }
}

/// Read a dotted config path (e.g. `gateway.auth_enabled`) as a string, with the corresponding
/// `ECPHORIA_GATEWAY__AUTH_ENABLED` env var taking precedence (mirrors the server's layering).
fn cfg_str(toml_val: &toml::Value, env: &EnvLookup, dotted: &str) -> Option<String> {
    let env_key = format!("ECPHORIA_{}", dotted.to_uppercase().replace('.', "__"));
    if let Some(v) = env.get(&env_key) {
        return Some(v);
    }
    let mut cur = toml_val;
    for seg in dotted.split('.') {
        cur = cur.get(seg)?;
    }
    match cur {
        toml::Value::String(s) => Some(s.clone()),
        toml::Value::Integer(i) => Some(i.to_string()),
        toml::Value::Boolean(b) => Some(b.to_string()),
        _ => None,
    }
}

fn cfg_bool(toml_val: &toml::Value, env: &EnvLookup, dotted: &str, default: bool) -> bool {
    cfg_str(toml_val, env, dotted)
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(default)
}

/// Is a `host:port` bind loopback-only? Anything that doesn't parse as loopback is treated as
/// exposed (matches the server's own guard).
fn is_loopback_bind(addr: &str) -> bool {
    if let Ok(sa) = addr.parse::<std::net::SocketAddr>() {
        return sa.ip().is_loopback();
    }
    match addr.rsplit_once(':') {
        Some((host, _)) => host == "localhost",
        None => false,
    }
}

/// Does an `api_keys` array contain at least one entry? (TOML array or comma-string / env.)
fn has_api_keys(toml_val: &toml::Value, env: &EnvLookup) -> bool {
    if let Some(s) = env.get("ECPHORIA_GATEWAY__API_KEYS") {
        return s.split(',').any(|k| !k.trim().is_empty());
    }
    toml_val
        .get("gateway")
        .and_then(|g| g.get("api_keys"))
        .and_then(|v| v.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false)
}

fn api_keys_list(toml_val: &toml::Value, env: &EnvLookup) -> Vec<String> {
    if let Some(s) = env.get("ECPHORIA_GATEWAY__API_KEYS") {
        return s
            .split(',')
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect();
    }
    toml_val
        .get("gateway")
        .and_then(|g| g.get("api_keys"))
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// The lint rules (pure — takes the parsed config + an env lookup, returns findings).
fn lint(toml_val: &toml::Value, env: &EnvLookup) -> Vec<Finding> {
    let mut out = Vec::new();

    let auth_enabled = cfg_bool(toml_val, env, "gateway.auth_enabled", false);
    let allow_insecure = cfg_bool(toml_val, env, "gateway.allow_insecure", false);
    let listen = cfg_str(toml_val, env, "gateway.listen").unwrap_or_else(|| "0.0.0.0:8432".into());
    let pg = cfg_str(toml_val, env, "gateway.pg_listen").unwrap_or_else(|| "0.0.0.0:5432".into());
    let grpc =
        cfg_str(toml_val, env, "gateway.grpc_listen").unwrap_or_else(|| "0.0.0.0:9432".into());
    let exposed: Vec<&str> = [&listen, &pg, &grpc]
        .into_iter()
        .map(String::as_str)
        .filter(|a| !is_loopback_bind(a))
        .collect();

    let jwt = cfg_str(toml_val, env, "gateway.jwt_secret");
    let oidc_enabled = cfg_bool(toml_val, env, "gateway.oidc.enabled", false);
    let has_keys = has_api_keys(toml_val, env);
    let has_any_credential = has_keys || jwt.is_some() || oidc_enabled;

    // 1. Unauthenticated public bind (the server refuses to start unless allow_insecure).
    if !auth_enabled && !exposed.is_empty() {
        if allow_insecure {
            out.push(Finding {
                level: Level::Warn,
                message: format!(
                    "serving WITHOUT authentication on non-loopback interfaces ({}) — allow_insecure=true. \
                     Anyone who can reach these ports can read/write all data.",
                    exposed.join(", ")
                ),
            });
        } else {
            out.push(Finding {
                level: Level::Error,
                message: format!(
                    "auth_enabled=false while binding non-loopback interfaces ({}). The server will \
                     REFUSE to start. Enable auth, bind loopback, or set allow_insecure=true.",
                    exposed.join(", ")
                ),
            });
        }
    }

    // 2. Auth enabled but no credential configured → fail-closed at startup.
    if auth_enabled && !has_any_credential {
        out.push(Finding {
            level: Level::Error,
            message:
                "auth_enabled=true but no api_keys, jwt_secret, or OIDC configured — the server \
                      will REFUSE to start (fail-closed)."
                    .into(),
        });
    }

    // 3. Weak JWT secret (HS256 needs ≥32 bytes).
    if let Some(secret) = &jwt {
        if secret.len() < 32 {
            out.push(Finding {
                level: Level::Error,
                message: format!(
                    "jwt_secret is too short ({} bytes); HS256 requires ≥32 bytes — the server will \
                     reject it at startup.",
                    secret.len()
                ),
            });
        }
    }

    // 4. Plaintext API keys (suggest hashing).
    let plaintext_keys = api_keys_list(toml_val, env)
        .iter()
        .filter(|k| {
            let secret = k.split('@').next().unwrap_or(k);
            !secret.starts_with("sha256:")
        })
        .count();
    if plaintext_keys > 0 {
        out.push(Finding {
            level: Level::Info,
            message: format!(
                "{plaintext_keys} API key(s) are stored in plaintext. Prefer hashed entries \
                 'sha256:<64-hex>@tenant:role' so no credential sits at rest \
                 (KEY=$(openssl rand -hex 32); echo -n \"$KEY\" | sha256sum)."
            ),
        });
    }

    // 5. Embedding dimension vs semantic index dimension mismatch.
    let emb_dim = cfg_str(toml_val, env, "embedding.dimension").and_then(|s| s.parse::<i64>().ok());
    let idx_dim = cfg_str(toml_val, env, "memory.semantic.default_dimension")
        .and_then(|s| s.parse::<i64>().ok());
    if let (Some(e), Some(i)) = (emb_dim, idx_dim) {
        if e != i {
            out.push(Finding {
                level: Level::Warn,
                message: format!(
                    "embedding.dimension ({e}) != memory.semantic.default_dimension ({i}). Vectors \
                     won't fit the index — search will fail. Make them equal."
                ),
            });
        }
    }

    // 6. Cluster enabled without a shared secret → unauthenticated Raft.
    if cfg_bool(toml_val, env, "cluster.enabled", false) {
        let secret = cfg_str(toml_val, env, "cluster.secret")
            .or_else(|| env.get("ECPHORIA_CLUSTER__SECRET"))
            .or_else(|| env.get("ECPHORIA_CLUSTER__SECRET_FILE"));
        if secret.is_none() {
            out.push(Finding {
                level: Level::Warn,
                message: "cluster.enabled=true but no cluster secret is set — Raft RPCs are \
                          unauthenticated. Set ECPHORIA_CLUSTER__SECRET on every node."
                    .into(),
            });
        }
    }

    out
}

fn print_findings(findings: &[Finding]) {
    if findings.is_empty() {
        println!("✓ ecphoria doctor: no configuration issues found.");
        return;
    }
    let (mut errs, mut warns, mut infos) = (0, 0, 0);
    let mut buf = String::new();
    for f in findings {
        let tag = match f.level {
            Level::Error => {
                errs += 1;
                "ERROR"
            }
            Level::Warn => {
                warns += 1;
                "WARN "
            }
            Level::Info => {
                infos += 1;
                "INFO "
            }
        };
        let _ = writeln!(buf, "  [{tag}] {}", f.message);
    }
    print!("{buf}");
    println!("\necphoria doctor: {errs} error(s), {warns} warning(s), {infos} info.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> EnvLookup {
        EnvLookup::fake(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn cfg(s: &str) -> toml::Value {
        toml::from_str(s).unwrap()
    }

    #[test]
    fn flags_unauthenticated_public_bind() {
        let c = cfg("[gateway]\nlisten = \"0.0.0.0:8432\"\nauth_enabled = false\n");
        let f = lint(&c, &env(&[]));
        assert!(f
            .iter()
            .any(|x| x.level == Level::Error && x.message.contains("REFUSE to start")));
    }

    #[test]
    fn loopback_bind_is_clean() {
        let c = cfg(
            "[gateway]\nlisten = \"127.0.0.1:8432\"\npg_listen = \"127.0.0.1:5432\"\n\
             grpc_listen = \"127.0.0.1:9432\"\nauth_enabled = false\n",
        );
        let f = lint(&c, &env(&[]));
        assert!(
            f.iter().all(|x| x.level != Level::Error),
            "loopback dev config should have no errors"
        );
    }

    #[test]
    fn allow_insecure_downgrades_to_warn() {
        let c = cfg(
            "[gateway]\nlisten = \"0.0.0.0:8432\"\nauth_enabled = false\nallow_insecure = true\n",
        );
        let f = lint(&c, &env(&[]));
        assert!(f
            .iter()
            .any(|x| x.level == Level::Warn && x.message.contains("WITHOUT authentication")));
        assert!(f.iter().all(|x| x.level != Level::Error));
    }

    #[test]
    fn flags_weak_jwt_and_missing_credentials() {
        let weak = cfg("[gateway]\nauth_enabled = true\njwt_secret = \"short\"\n");
        let f = lint(&weak, &env(&[]));
        assert!(f
            .iter()
            .any(|x| x.message.contains("jwt_secret is too short")));

        let none = cfg("[gateway]\nlisten = \"127.0.0.1:8432\"\npg_listen=\"127.0.0.1:5432\"\ngrpc_listen=\"127.0.0.1:9432\"\nauth_enabled = true\n");
        let f = lint(&none, &env(&[]));
        assert!(f
            .iter()
            .any(|x| x.level == Level::Error && x.message.contains("fail-closed")));
    }

    #[test]
    fn flags_dimension_mismatch() {
        let c = cfg("[embedding]\ndimension = 1536\n[memory.semantic]\ndefault_dimension = 768\n");
        let f = lint(&c, &env(&[]));
        assert!(f
            .iter()
            .any(|x| x.level == Level::Warn && x.message.contains("won't fit the index")));
    }

    #[test]
    fn env_overrides_toml() {
        // TOML says auth off + public bind (would be an error), but env enables auth with a key.
        let c = cfg("[gateway]\nlisten = \"0.0.0.0:8432\"\nauth_enabled = false\n");
        let f = lint(
            &c,
            &env(&[
                ("ECPHORIA_GATEWAY__AUTH_ENABLED", "true"),
                ("ECPHORIA_GATEWAY__API_KEYS", "sk_live@acme:writer"),
            ]),
        );
        assert!(
            f.iter().all(|x| x.level != Level::Error),
            "env should clear the auth error"
        );
    }

    #[test]
    fn suggests_hashing_plaintext_keys() {
        let c = cfg("[gateway]\nlisten=\"127.0.0.1:8432\"\npg_listen=\"127.0.0.1:5432\"\ngrpc_listen=\"127.0.0.1:9432\"\nauth_enabled = true\napi_keys = [\"plain@acme\", \"sha256:abcd@beta\"]\n");
        let f = lint(&c, &env(&[]));
        assert!(f
            .iter()
            .any(|x| x.level == Level::Info && x.message.contains("plaintext")));
    }
}
