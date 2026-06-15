//! Mapping cluster node ids to consensus (raft) node ids
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md), workstream E
//! step 4 — wiring).
//!
//! The cluster identifies a node by its string [`NodeId`] (a certificate CN, ADR
//! 0004). openraft requires a `Copy` numeric node id ([`RaftNodeId`]). The lease
//! consensus group is replicated state shared by every node, so the
//! `NodeId → RaftNodeId` mapping must be **identical on every node** — a node that
//! derives a different id for a peer would disagree on cluster membership and break
//! consensus.
//!
//! So the raft id is a **deterministic, version-stable hash** of the node id
//! ([`raft_id`], via [`hrw::stable_id`](crate::hrw::stable_id)) — the same hash
//! discipline placement already relies on, not `std::hash` (which is per-process
//! seeded). [`NodeRegistry`] keeps the reverse map (raft id → node id) so an
//! inbound consensus RPC tagged with a `RaftNodeId` can be answered over the right
//! peer link, and flags the astronomically-unlikely 64-bit collision rather than
//! letting two nodes silently share an id.

use crate::hrw;
use crate::lease_raft::RaftNodeId;
use crate::NodeId;
use std::collections::BTreeMap;

/// The consensus node id for a cluster `node`: a deterministic, version-stable hash
/// of its id, so every node computes the same value for the same peer.
#[must_use]
pub fn raft_id(node: &NodeId) -> RaftNodeId {
    hrw::stable_id(node.0.as_bytes())
}

/// The reverse mapping `RaftNodeId → NodeId`, learned as cluster nodes are observed.
///
/// Forward (`NodeId → RaftNodeId`) needs no state — it is the pure [`raft_id`] hash.
/// The reverse direction is needed to route a consensus RPC (addressed by raft id)
/// back to a peer's link, so it is accumulated here.
#[derive(Debug, Default, Clone)]
pub struct NodeRegistry {
    by_raft: BTreeMap<RaftNodeId, NodeId>,
}

impl NodeRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Learn `node`'s raft id, returning it.
    ///
    /// # Errors
    /// Returns `Err(raft_id)` if a *different* node already holds that raft id — a
    /// 64-bit hash collision (astronomically unlikely for a real cluster). The
    /// caller must refuse to admit the colliding node rather than corrupt consensus.
    pub fn observe(&mut self, node: &NodeId) -> Result<RaftNodeId, RaftNodeId> {
        let id = raft_id(node);
        match self.by_raft.get(&id) {
            Some(existing) if existing != node => Err(id),
            _ => {
                self.by_raft.insert(id, node.clone());
                Ok(id)
            }
        }
    }

    /// The cluster node id for a raft id, if it has been observed.
    #[must_use]
    pub fn node(&self, id: RaftNodeId) -> Option<&NodeId> {
        self.by_raft.get(&id)
    }

    /// Forget a node (e.g. it left the cluster).
    pub fn forget(&mut self, node: &NodeId) {
        self.by_raft.remove(&raft_id(node));
    }

    /// The number of nodes currently mapped.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_raft.len()
    }

    /// Whether the registry has no nodes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_raft.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{raft_id, NodeRegistry};
    use crate::NodeId;

    fn n(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// The raft id is a pure function of the node id — the same everywhere, every
    /// run (the cross-node-agreement requirement).
    #[test]
    fn raft_id_is_deterministic() {
        assert_eq!(raft_id(&n("node-a")), raft_id(&n("node-a")));
        assert_ne!(raft_id(&n("node-a")), raft_id(&n("node-b")));
    }

    /// A handful of distinct node ids get distinct raft ids (no collisions in
    /// practice).
    #[test]
    fn distinct_nodes_get_distinct_ids() {
        let ids: std::collections::BTreeSet<u64> = ["a", "b", "c", "node-1", "node-2", "broker-x"]
            .iter()
            .map(|s| raft_id(&n(s)))
            .collect();
        assert_eq!(ids.len(), 6, "no collisions among distinct node ids");
    }

    #[test]
    fn registry_round_trips_and_is_idempotent() {
        let mut reg = NodeRegistry::new();
        let a = n("node-a");
        let id_a = reg.observe(&a).unwrap();
        // Observing again is idempotent.
        assert_eq!(reg.observe(&a).unwrap(), id_a);
        assert_eq!(reg.node(id_a), Some(&a));
        assert_eq!(reg.len(), 1);

        let b = n("node-b");
        let id_b = reg.observe(&b).unwrap();
        assert_eq!(reg.node(id_b), Some(&b));
        assert_eq!(reg.len(), 2);

        // Forgetting drops the reverse mapping.
        reg.forget(&a);
        assert_eq!(reg.node(id_a), None);
        assert!(!reg.is_empty());
    }
}
