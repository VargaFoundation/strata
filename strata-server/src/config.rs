//! Configuration loading — layers defaults, strata.toml, and environment variables.

use serde::Deserialize;
use strata_cluster::ClusterConfig;
use strata_core::CoreConfig;
use strata_gateway::server::GatewayConfig;

#[derive(Debug, Default)]
pub struct ServerConfig {
    /// Core sub-configs addressable at the top level as documented, e.g.
    /// `STRATA_STORAGE__DATA_DIR` / `STRATA_EMBEDDING__PROVIDER` (not `STRATA_CORE__STORAGE__…`).
    pub core: CoreConfig,
    pub gateway: GatewayConfig,
    pub cluster: ClusterConfig,
}

/// Gateway + cluster sections, deserialized separately from `core` (see [`load`]).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GatewayAndCluster {
    gateway: GatewayConfig,
    cluster: ClusterConfig,
}

/// Load configuration from strata.toml + STRATA_ env vars.
pub fn load() -> anyhow::Result<ServerConfig> {
    let config = config::Config::builder()
        .add_source(config::File::with_name("strata").required(false))
        // `prefix_separator("_")` so the documented single-underscore form works
        // (`STRATA_GATEWAY__LISTEN`), while `separator("__")` splits nested keys. Without the
        // explicit prefix separator, the `config` crate reuses `__` after the prefix too and
        // silently ignores every `STRATA_*` var — booting nodes on default ports.
        .add_source(
            config::Environment::with_prefix("STRATA")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    // Propagate deserialization errors instead of silently falling back to ALL defaults — a single
    // bad env var (e.g. a malformed STRATA_CLUSTER__PEERS) must fail loudly, not boot a misconfigured
    // node on default ports.
    //
    // Deserialize `core` and `gateway`/`cluster` SEPARATELY rather than via one `#[serde(flatten)]`
    // struct. serde's `flatten` buffers every value as an opaque `Content` and re-deserializes it,
    // which bypasses the `config` crate's lenient string→number coercion — so a numeric env var like
    // `STRATA_QUERY__MAX_ROWS=5000` or `STRATA_EMBEDDING__DIMENSION=1024` under a flattened section
    // would fail with "invalid type: string, expected u64". Reading each sub-config directly (no
    // flatten) keeps that coercion, so numeric env overrides work for every section. Each
    // deserialization ignores the other's top-level keys (serde ignores unknown fields).
    let core: CoreConfig = config.clone().try_deserialize().map_err(|e| {
        anyhow::anyhow!("invalid configuration (check STRATA_* env vars / strata.toml): {e}")
    })?;
    let rest: GatewayAndCluster = config.try_deserialize().map_err(|e| {
        anyhow::anyhow!("invalid configuration (check STRATA_* env vars / strata.toml): {e}")
    })?;
    Ok(ServerConfig {
        core,
        gateway: rest.gateway,
        cluster: rest.cluster,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_defaults() {
        let config = load().unwrap();
        assert_eq!(config.gateway.listen, "0.0.0.0:8432");
    }
}
