//! Consistent-hash shard routing — the foundation for multi-Raft-group write sharding.
//!
//! A single Raft group serializes every write through one leader, which caps write throughput. The
//! path to scaling writes is to partition keys (by tenant/agent) across N **independent** Raft
//! groups ("shards"), each with its own leader. This module is the routing primitive for that: a
//! consistent-hash ring that maps a key → shard id with minimal remapping when N changes.
//!
//! Today Strata runs a single group (`shards = 1` → everything routes to shard 0). Wiring each shard
//! to its own `ClusterCoordinator` + cross-shard write forwarding is the next increment; this layer
//! lets call sites compute the target shard now without churn later.

use std::collections::BTreeMap;

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
pub struct ShardedCluster {
    coordinators: Vec<crate::ClusterCoordinator>,
    router: ShardRouter,
}

impl ShardedCluster {
    /// Wrap N already-started single-group coordinators as shards `0..N`.
    pub fn new(coordinators: Vec<crate::ClusterCoordinator>) -> Self {
        let router = ShardRouter::new(coordinators.len(), 128);
        Self {
            coordinators,
            router,
        }
    }

    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.coordinators.len()
    }

    /// The shard owning `key`.
    pub fn shard_for(&self, key: &str) -> usize {
        self.router.shard_for(key)
    }

    /// Access a shard's coordinator (e.g. for per-shard reads / status).
    pub fn coordinator(&self, shard: usize) -> Option<&crate::ClusterCoordinator> {
        self.coordinators.get(shard)
    }

    /// Route a write to the shard owning `key` and propose it through that shard's Raft group.
    pub async fn client_write(
        &self,
        key: &str,
        request: crate::raft::types::AppRequest,
    ) -> crate::Result<crate::raft::types::AppResponse> {
        let s = self.router.shard_for(key);
        self.coordinators[s].client_write(request).await
    }

    /// Gracefully shut down every shard.
    pub async fn shutdown(self) {
        for mut c in self.coordinators {
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
