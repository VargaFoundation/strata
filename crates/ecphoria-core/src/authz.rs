//! Authorization backend seam — the pluggable point for cross-scope read access.
//!
//! Today the only decision Ecphoria externalizes is *whose memories may a user additionally read*
//! (the cross-scope grant). [`memory_search_shared`](crate::EcphoriaEngine::memory_search_shared)
//! resolves that through an [`AuthzBackend`]. The default [`LocalGrants`] answers from the
//! tenant-strict `memory_grants` table. Keeping it behind a trait means a richer policy engine
//! (teams/roles, or an external ReBAC backend like SpiceDB) can be dropped in later **without
//! changing the read path** — the engine just gets a different `Arc<dyn AuthzBackend>` via
//! [`EcphoriaEngine::set_authz_backend`](crate::EcphoriaEngine::set_authz_backend).
//!
//! Invariant every backend MUST preserve: results are **tenant-strict** — a backend may only widen
//! read access to grantors *within the same tenant*, never across tenants.

use std::sync::Arc;

use async_trait::async_trait;

use crate::memory::cognition::MemoryStore;

/// Resolves cross-scope read authorization. See the module docs.
#[async_trait]
pub trait AuthzBackend: Send + Sync {
    /// The user ids within `tenant` whose memories `user` is allowed to additionally read (the
    /// grantors). MUST be tenant-strict: never return a principal from another tenant.
    async fn granted_read_scopes(&self, tenant: &str, user: &str) -> crate::Result<Vec<String>>;
}

/// Default backend: reads the tenant-strict `memory_grants` table (a grant lets `user` read a
/// grantor's memories within the same tenant).
pub struct LocalGrants {
    store: Arc<MemoryStore>,
}

impl LocalGrants {
    pub fn new(store: Arc<MemoryStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl AuthzBackend for LocalGrants {
    async fn granted_read_scopes(&self, tenant: &str, user: &str) -> crate::Result<Vec<String>> {
        Ok(self
            .store
            .list_grants(tenant, user)
            .await?
            .into_iter()
            .map(|g| g.grantor_user_id)
            .collect())
    }
}
