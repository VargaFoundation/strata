//! Consistent-hash shard routing — the foundation for multi-Raft-group write sharding.
//!
//! A single Raft group serializes every write through one leader, which caps write throughput. The
//! path to scaling writes is to partition keys (by tenant/agent) across N **independent** Raft
//! groups ("shards"), each with its own leader. This module is the routing primitive for that: a
//! consistent-hash ring that maps a key → shard id with minimal remapping when N changes.
//!
//! Today Ecphoria runs a single group (`shards = 1` → everything routes to shard 0). Wiring each shard
//! to its own `ClusterCoordinator` + cross-shard write forwarding is the next increment; this layer
//! lets call sites compute the target shard now without churn later.

use std::collections::BTreeMap;

/// A key whose owning shard changes on a reshard — one data movement a rebalance must perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMove {
    pub key: String,
    pub from: usize,
    pub to: usize,
}

/// What a rebalancing operator should do to converge from `actual` to `desired` shards: the target
/// shard count to scale the StatefulSets to, and the tenant data movements (consistent hashing keeps
/// the move set small). The pure brain of a Kubernetes operator — fully unit-testable.
#[derive(Debug, PartialEq, Eq)]
pub struct ReconcilePlan {
    pub scale_to: usize,
    pub moves: Vec<ShardMove>,
}

/// Compute the reconcile plan for `tenants` when resharding `actual` → `desired` shards.
pub fn reconcile_plan(desired: usize, actual: usize, tenants: &[String]) -> ReconcilePlan {
    let desired = desired.max(1);
    let actual = actual.max(1);
    let old = ShardRouter::new(actual, 128);
    let new = ShardRouter::new(desired, 128);
    ReconcilePlan {
        scale_to: desired,
        moves: old.reshard_moves(&new, tenants),
    }
}

/// Direction of a shard-count change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleDirection {
    Up,
    Down,
    None,
}

/// An **ordered** scale plan for a Kubernetes operator: which shards to add or remove plus the tenant
/// data movements, sequenced so no data is lost. Pure + unit-testable; the operator just applies it.
///
/// - **Up**: create `add_shards` (new StatefulSets) FIRST, then apply `moves` (migrate data onto them).
/// - **Down**: apply `moves` FIRST (drain data OFF `remove_shards`), THEN delete `remove_shards`.
///
/// Applying in the wrong order loses data (moving onto a shard that doesn't exist yet, or deleting a
/// shard before its tenants have been drained), which is why the plan makes the sequence explicit.
#[derive(Debug, PartialEq, Eq)]
pub struct ScalePlan {
    pub direction: ScaleDirection,
    pub scale_to: usize,
    /// Shard indices to create (scale-up). Empty on scale-down / no-op.
    pub add_shards: Vec<usize>,
    /// Shard indices to drain then delete (scale-down). Empty on scale-up / no-op.
    pub remove_shards: Vec<usize>,
    /// Tenant data movements (consistent hashing keeps this set small).
    pub moves: Vec<ShardMove>,
}

/// Plan a scale from `actual` → `desired` shards for `tenants`, with the safe ordering made explicit.
/// The highest-indexed shards are the ones added (up) or removed (down), matching a StatefulSet's
/// ordinal-based scaling. Every tenant currently on a to-be-removed shard appears in `moves` (so a
/// scale-down never deletes a shard with live data).
pub fn scale_plan(desired: usize, actual: usize, tenants: &[String]) -> ScalePlan {
    let desired = desired.max(1);
    let actual = actual.max(1);
    let moves = reconcile_plan(desired, actual, tenants).moves;
    let (direction, add_shards, remove_shards) = match desired.cmp(&actual) {
        std::cmp::Ordering::Greater => {
            (ScaleDirection::Up, (actual..desired).collect(), Vec::new())
        }
        std::cmp::Ordering::Less => (
            ScaleDirection::Down,
            Vec::new(),
            (desired..actual).collect(),
        ),
        std::cmp::Ordering::Equal => (ScaleDirection::None, Vec::new(), Vec::new()),
    };
    ScalePlan {
        direction,
        scale_to: desired,
        add_shards,
        remove_shards,
        moves,
    }
}

/// Maps keys to shard ids on a consistent-hash ring (virtual nodes for balance).
#[derive(Debug, Clone)]
pub struct ShardRouter {
    ring: BTreeMap<u64, usize>,
    shards: usize,
}

impl ShardRouter {
    /// Build a ring for `shards` shards with `vnodes` virtual nodes each (more vnodes = smoother
    /// balance). Both are clamped to ≥1.
    pub fn new(shards: usize, vnodes: usize) -> Self {
        let shards = shards.max(1);
        let vnodes = vnodes.max(1);
        let mut ring = BTreeMap::new();
        for s in 0..shards {
            for v in 0..vnodes {
                ring.insert(hash(&format!("shard-{s}-vnode-{v}")), s);
            }
        }
        Self { ring, shards }
    }

    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.shards
    }

    /// Given the current keys and a NEW router (different shard count), list the keys whose owning
    /// shard changes — i.e. the data movements a rebalance must perform. Consistent hashing keeps
    /// this set small (only a fraction of keys move when shard count changes).
    pub fn reshard_moves(&self, new_router: &ShardRouter, keys: &[String]) -> Vec<ShardMove> {
        keys.iter()
            .filter_map(|k| {
                let from = self.shard_for(k);
                let to = new_router.shard_for(k);
                (from != to).then(|| ShardMove {
                    key: k.clone(),
                    from,
                    to,
                })
            })
            .collect()
    }

    /// The shard a key routes to (first ring point clockwise of the key's hash, wrapping around).
    pub fn shard_for(&self, key: &str) -> usize {
        if self.shards == 1 {
            return 0;
        }
        let h = hash(key);
        self.ring
            .range(h..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, &s)| s)
            .unwrap_or(0)
    }
}

/// A set of independent Raft groups (shards), each its own [`crate::ClusterCoordinator`], with
/// writes routed by key via consistent hashing. This is the multi-group composition for horizontal
/// write scaling: each shard has its own leader, so write throughput scales with shard count.
///
/// Callers start N single-group coordinators (one per partition) and wrap them here. `client_write`
/// hashes the key to a shard and proposes the write through that shard's Raft group. Reads remain
/// per-shard (a future cross-shard read-aggregation layer can fan out via `coordinator(i)`).
/// One shard: an independent Raft group + the engine it commits to.
struct ShardHandle {
    coordinator: crate::ClusterCoordinator,
    engine: std::sync::Arc<ecphoria_core::EcphoriaEngine>,
}

pub struct ShardedCluster {
    shards: Vec<ShardHandle>,
    router: ShardRouter,
}

impl ShardedCluster {
    /// Wrap N already-started single-group coordinators (with their engines) as shards `0..N`.
    pub fn new(
        shards: Vec<(
            crate::ClusterCoordinator,
            std::sync::Arc<ecphoria_core::EcphoriaEngine>,
        )>,
    ) -> Self {
        let router = ShardRouter::new(shards.len(), 128);
        let shards = shards
            .into_iter()
            .map(|(coordinator, engine)| ShardHandle {
                coordinator,
                engine,
            })
            .collect();
        Self { shards, router }
    }

    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.shards.len()
    }

    /// The shard owning `key`.
    pub fn shard_for(&self, key: &str) -> usize {
        self.router.shard_for(key)
    }

    /// Access a shard's coordinator (e.g. for per-shard status).
    pub fn coordinator(&self, shard: usize) -> Option<&crate::ClusterCoordinator> {
        self.shards.get(shard).map(|s| &s.coordinator)
    }

    /// The engine of the shard owning `key` (for single-key, shard-local reads).
    pub fn engine_for(&self, key: &str) -> &std::sync::Arc<ecphoria_core::EcphoriaEngine> {
        &self.shards[self.router.shard_for(key)].engine
    }

    /// Route a write to the shard owning `key` and propose it through that shard's Raft group.
    pub async fn client_write(
        &self,
        key: &str,
        request: crate::raft::types::AppRequest,
    ) -> crate::Result<crate::raft::types::AppResponse> {
        let s = self.router.shard_for(key);
        self.shards[s].coordinator.client_write(request).await
    }

    /// Cross-shard read: run a SQL query on **every** shard and concatenate the rows. A scatter-
    /// gather for analytics that span partitions (each shard scans its own slice in parallel).
    pub async fn query_all(&self, sql: &str) -> crate::Result<Vec<serde_json::Value>> {
        let mut out = Vec::new();
        for s in &self.shards {
            out.extend(s.engine.query_sql(sql).await?);
        }
        Ok(out)
    }

    /// Cross-shard memory search: fan out to every shard, merge by score, return the global top-k.
    pub async fn memory_search_all(
        &self,
        query: &str,
        scope: &ecphoria_core::memory::cognition::MemoryScope,
        k: usize,
    ) -> crate::Result<Vec<ecphoria_core::memory::cognition::MemoryHit>> {
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(s.engine.memory_search(query, scope, k).await?);
        }
        all.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(k);
        Ok(all)
    }

    /// Execute a set of rebalance moves: for each, migrate the tenant's memories from the source
    /// shard's engine to the destination's, then drop them from the source. Returns memories moved.
    /// (Compute the moves with [`ShardRouter::reshard_moves`] after the operator adds/removes shards.)
    pub async fn apply_moves(&self, moves: &[ShardMove]) -> crate::Result<usize> {
        let mut migrated = 0;
        for m in moves {
            if m.from == m.to || m.from >= self.shards.len() || m.to >= self.shards.len() {
                continue;
            }
            let from = self.shards[m.from].engine.clone();
            let to = self.shards[m.to].engine.clone();
            migrated += from.migrate_tenant_memories_to(&to, &m.key).await?;
        }
        Ok(migrated)
    }

    /// Gracefully shut down every shard.
    pub async fn shutdown(self) {
        for s in self.shards {
            let mut c = s.coordinator;
            let _ = c.shutdown().await;
        }
    }
}

/// FNV-1a 64-bit — deterministic, dependency-free, good enough spread for routing (not security).
fn hash(s: &str) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_shard_routes_everything_to_zero() {
        let r = ShardRouter::new(1, 64);
        for k in ["a", "tenant-x", "anything"] {
            assert_eq!(r.shard_for(k), 0);
        }
    }

    #[test]
    fn routing_is_deterministic_and_in_range() {
        let r = ShardRouter::new(4, 64);
        for k in ["alice", "bob", "tenant-7", "agent-42"] {
            let s = r.shard_for(k);
            assert_eq!(s, r.shard_for(k), "stable for the same key");
            assert!(s < 4, "shard id in range");
        }
    }

    #[test]
    fn distribution_is_roughly_balanced() {
        let r = ShardRouter::new(4, 256);
        let mut counts = [0usize; 4];
        for i in 0..8000 {
            counts[r.shard_for(&format!("key-{i}"))] += 1;
        }
        // Fair share is 2000/shard; assert none is starved or dominant (well within FNV+vnode noise).
        for c in counts {
            assert!(c > 1000 && c < 3000, "imbalanced shard count: {c}");
        }
    }

    #[test]
    fn reconcile_plan_scales_and_lists_moves() {
        let tenants: Vec<String> = (0..2000).map(|i| format!("tenant-{i}")).collect();
        // No change → scale stays, no moves.
        let same = reconcile_plan(4, 4, &tenants);
        assert_eq!(same.scale_to, 4);
        assert!(same.moves.is_empty());
        // Scale up 4 → 5 → a fraction of tenants move, each to a valid shard, none staying put.
        let up = reconcile_plan(5, 4, &tenants);
        assert_eq!(up.scale_to, 5);
        assert!(
            !up.moves.is_empty() && up.moves.len() < tenants.len() / 2,
            "expected a small non-empty move set, got {}",
            up.moves.len()
        );
        for m in &up.moves {
            assert_ne!(m.from, m.to);
            assert!(m.to < 5);
        }
    }

    #[test]
    fn scale_plan_up_adds_high_shards_and_moves() {
        let tenants: Vec<String> = (0..2000).map(|i| format!("tenant-{i}")).collect();
        let up = scale_plan(6, 4, &tenants);
        assert_eq!(up.direction, ScaleDirection::Up);
        assert_eq!(up.scale_to, 6);
        assert_eq!(up.add_shards, vec![4, 5]);
        assert!(up.remove_shards.is_empty());
        assert!(!up.moves.is_empty());
    }

    #[test]
    fn scale_plan_down_drains_every_removed_shard_before_delete() {
        let tenants: Vec<String> = (0..3000).map(|i| format!("tenant-{i}")).collect();
        let down = scale_plan(3, 5, &tenants);
        assert_eq!(down.direction, ScaleDirection::Down);
        assert_eq!(down.remove_shards, vec![3, 4]);
        assert!(down.add_shards.is_empty());

        // Safety invariant: EVERY tenant currently on a shard being removed has a move (so deleting
        // the shard afterwards never drops live data).
        let old = ShardRouter::new(5, 128);
        let moved: std::collections::HashSet<&str> =
            down.moves.iter().map(|m| m.key.as_str()).collect();
        for t in &tenants {
            let s = old.shard_for(t);
            if s == 3 || s == 4 {
                assert!(
                    moved.contains(t.as_str()),
                    "tenant {t} on removed shard {s} not drained"
                );
            }
        }
    }

    #[test]
    fn scale_plan_noop_when_equal() {
        let plan = scale_plan(4, 4, &[]);
        assert_eq!(plan.direction, ScaleDirection::None);
        assert!(
            plan.add_shards.is_empty() && plan.remove_shards.is_empty() && plan.moves.is_empty()
        );
    }

    #[test]
    fn reshard_moves_lists_only_changed_keys() {
        let keys: Vec<String> = (0..2000).map(|i| format!("tenant-{i}")).collect();
        let r4 = ShardRouter::new(4, 128);
        let r5 = ShardRouter::new(5, 128);
        let moves = r4.reshard_moves(&r5, &keys);
        // Every listed key genuinely changed shard, and consistent hashing keeps the set small.
        for m in &moves {
            assert_ne!(m.from, m.to);
            assert_eq!(r4.shard_for(&m.key), m.from);
            assert_eq!(r5.shard_for(&m.key), m.to);
        }
        assert!(!moves.is_empty() && moves.len() < keys.len() / 2);
    }

    #[test]
    fn adding_a_shard_remaps_only_a_fraction() {
        // Consistent hashing: growing 4 → 5 shards must move far fewer than "all" keys.
        let r4 = ShardRouter::new(4, 128);
        let r5 = ShardRouter::new(5, 128);
        let n = 4000;
        let moved = (0..n)
            .filter(|i| {
                let k = format!("key-{i}");
                r4.shard_for(&k) != r5.shard_for(&k)
            })
            .count();
        // A naive `hash % N` would remap ~80%; consistent hashing should move well under half.
        assert!(moved < n / 2, "too many keys remapped: {moved}/{n}");
    }
}
