//! Session placement over live membership (ADR 0001 §1).
//!
//! Wraps the deterministic [`crate::hrw`] primitives with the *current* eligible
//! member set — this node plus every peer not believed `Dead` — and recomputes
//! as SWIM membership changes. It is pure and sans-I/O: feed it
//! [`observe`](Placement::observe) for each membership change and query
//! [`owner`](Placement::owner) / [`replica_set`](Placement::replica_set) /
//! [`owns`](Placement::owns). The replica set is bounded at `R` (default 3) —
//! the small group ADR 0001 scopes durability/consensus to, not the whole
//! cluster.
//!
//! `Suspect` members stay in the ring: a transiently-slow node should not
//! trigger ownership churn (and the reassignment it would reverse on
//! refutation). Only a confirmed `Dead` removes a node, which is exactly the
//! ADR 0001 takeover trigger.

use crate::swim::MemberState;
use crate::{hrw, NodeId};
use std::collections::{BTreeMap, BTreeSet};

/// Default replication factor: each session's replica set spans R nodes
/// (ADR 0001 §1).
pub const DEFAULT_REPLICAS: usize = 3;

/// The placement ring for one node: maps client ids to their owner and replica
/// set over the current eligible membership.
#[derive(Debug, Clone)]
pub struct Placement {
    local: NodeId,
    replicas: usize,
    /// Nodes eligible to own sessions: this node plus non-`Dead` peers. A
    /// `BTreeSet` keeps the derived node list deterministic across calls.
    eligible: BTreeSet<NodeId>,
    /// Each peer's inter-node (peer-link) address, so the owner of a session can
    /// be reached for session relocation (ADR 0005).
    addrs: BTreeMap<NodeId, String>,
}

impl Placement {
    /// Create a ring containing only this node. `replicas` is clamped to at
    /// least 1.
    #[must_use]
    pub fn new(local: NodeId, replicas: usize) -> Self {
        let mut eligible = BTreeSet::new();
        eligible.insert(local.clone());
        Self {
            local,
            replicas: replicas.max(1),
            eligible,
            addrs: BTreeMap::new(),
        }
    }

    /// Apply an observed membership state. A non-`Dead` peer becomes eligible
    /// for placement (recording its peer-link `addr` for relocation); a `Dead`
    /// peer is removed. This node is always eligible and is never removed — it
    /// cannot hand off its own participation.
    pub fn observe(&mut self, id: &NodeId, state: MemberState, addr: &str) {
        if id == &self.local {
            return;
        }
        match state {
            MemberState::Dead => {
                self.eligible.remove(id);
                self.addrs.remove(id);
            }
            MemberState::Alive | MemberState::Suspect => {
                self.eligible.insert(id.clone());
                if !addr.is_empty() {
                    self.addrs.insert(id.clone(), addr.to_string());
                }
            }
        }
    }

    fn nodes(&self) -> Vec<NodeId> {
        self.eligible.iter().cloned().collect()
    }

    /// The owner node for `client`. There is always an owner (this node is
    /// always eligible).
    #[must_use]
    pub fn owner(&self, client: &str) -> NodeId {
        hrw::owner(client.as_bytes(), &self.nodes())
            .cloned()
            .unwrap_or_else(|| self.local.clone())
    }

    /// The ordered replica set for `client` (owner first), capped at `R` and at
    /// the current member count.
    #[must_use]
    pub fn replica_set(&self, client: &str) -> Vec<NodeId> {
        hrw::replica_set(client.as_bytes(), &self.nodes(), self.replicas)
    }

    /// Whether this node owns `client`.
    #[must_use]
    pub fn owns(&self, client: &str) -> bool {
        self.owner(client) == self.local
    }

    /// Where to relocate `client`'s session: `Some((owner, peer_addr))` when the
    /// owner is another node whose address is known, `None` when this node is the
    /// owner (no relocation) or the owner's address is not yet learned (serve
    /// locally — ADR 0005 degrade-don't-refuse).
    #[must_use]
    pub fn owner_route(&self, client: &str) -> Option<(NodeId, String)> {
        let owner = self.owner(client);
        if owner == self.local {
            return None;
        }
        self.addrs.get(&owner).map(|addr| (owner, addr.clone()))
    }

    /// Whether this node is in `client`'s replica set (owner or a failover
    /// replica).
    #[must_use]
    pub fn is_replica(&self, client: &str) -> bool {
        self.replica_set(client).iter().any(|n| n == &self.local)
    }

    /// The number of nodes currently eligible for placement (always ≥ 1).
    #[must_use]
    pub fn member_count(&self) -> usize {
        self.eligible.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{Placement, DEFAULT_REPLICAS};
    use crate::swim::MemberState;
    use crate::NodeId;

    fn node(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// Build a ring for `local` that has observed each of `peers` as Alive
    /// (each with a synthetic `<peer>:7000` peer-link address).
    fn ring(local: &str, peers: &[&str]) -> Placement {
        let mut p = Placement::new(node(local), DEFAULT_REPLICAS);
        for peer in peers {
            p.observe(&node(peer), MemberState::Alive, &format!("{peer}:7000"));
        }
        p
    }

    #[test]
    fn alone_this_node_owns_everything() {
        let p = Placement::new(node("a"), DEFAULT_REPLICAS);
        assert_eq!(p.member_count(), 1);
        for c in ["x", "y", "session-42"] {
            assert_eq!(p.owner(c), node("a"));
            assert!(p.owns(c));
            assert_eq!(p.replica_set(c), vec![node("a")]);
        }
    }

    #[test]
    fn alive_and_suspect_are_eligible_dead_is_removed() {
        let mut p = ring("a", &["b", "c"]);
        assert_eq!(p.member_count(), 3);

        // Suspect keeps the node in the ring (no churn on a transient blip).
        p.observe(&node("b"), MemberState::Suspect, "b:7000");
        assert_eq!(p.member_count(), 3);

        // Dead removes it.
        p.observe(&node("c"), MemberState::Dead, "");
        assert_eq!(p.member_count(), 2);

        // A node first seen as Suspect is still a member.
        p.observe(&node("d"), MemberState::Suspect, "d:7000");
        assert_eq!(p.member_count(), 3);
    }

    #[test]
    fn this_node_is_never_removed() {
        let mut p = ring("a", &["b"]);
        // Even a (spurious) Dead about ourselves must not drop us.
        p.observe(&node("a"), MemberState::Dead, "");
        assert_eq!(p.member_count(), 2);
        // We can still own keys.
        assert!(["x", "y", "z", "w"].iter().any(|c| p.owns(c)));
    }

    #[test]
    fn owner_route_points_at_a_remote_owner_and_is_none_when_local() {
        let p = ring("a", &["b", "c", "d", "e"]);
        let mut remote = 0;
        for i in 0..200 {
            let c = format!("client-{i}");
            match p.owner_route(&c) {
                None => {
                    // No route iff this node is the owner.
                    assert!(p.owns(&c), "no route for {c} but it is not local-owned");
                }
                Some((owner, addr)) => {
                    assert_ne!(owner, node("a"));
                    assert_eq!(owner, p.owner(&c));
                    assert_eq!(addr, format!("{}:7000", owner.0));
                    remote += 1;
                }
            }
        }
        assert!(remote > 0, "some sessions should route to a remote owner");
    }

    #[test]
    fn owner_route_is_none_until_the_owner_address_is_known() {
        // A peer eligible for placement but with no address yet cannot be a relay
        // target — serve locally rather than guess.
        let mut p = Placement::new(node("a"), DEFAULT_REPLICAS);
        p.observe(&node("b"), MemberState::Alive, ""); // eligible, address unknown
        for i in 0..200 {
            let c = format!("client-{i}");
            if p.owner(&c) == node("b") {
                assert_eq!(
                    p.owner_route(&c),
                    None,
                    "no address → no route → serve local"
                );
            }
        }
    }

    #[test]
    fn replica_set_shrinks_gracefully_below_r() {
        let p = ring("a", &["b"]); // 2 members, R = 3
        let rs = p.replica_set("session-x");
        assert_eq!(rs.len(), 2, "replica set capped at the member count");
        assert_eq!(rs[0], p.owner("session-x"), "owner leads the replica set");
        // R is honored once enough members exist.
        let p = ring("a", &["b", "c", "d", "e"]); // 5 members
        assert_eq!(p.replica_set("session-x").len(), 3);
    }

    #[test]
    fn owns_and_is_replica_agree_with_the_ring() {
        let p = ring("a", &["b", "c", "d", "e"]);
        for i in 0..200 {
            let c = format!("client-{i}");
            let rs = p.replica_set(&c);
            assert_eq!(p.owns(&c), rs.first() == Some(&node("a")));
            assert_eq!(p.is_replica(&c), rs.contains(&node("a")));
            // The owner is always the head of the replica set.
            assert_eq!(p.owner(&c), rs[0]);
        }
    }

    #[test]
    fn a_dead_node_only_moves_the_keys_it_owned() {
        let before = ring("a", &["b", "c", "d"]); // 4 members
        let mut after = before.clone();
        after.observe(&node("d"), MemberState::Dead, ""); // 3 members

        let mut moved = 0;
        let mut moved_were_ds = 0;
        let total = 2_000;
        for i in 0..total {
            let c = format!("client-{i}");
            let o0 = before.owner(&c);
            let o1 = after.owner(&c);
            if o0 != o1 {
                moved += 1;
                // The only keys that may move are those d owned.
                assert_eq!(o0, node("d"), "a non-owned key was reassigned");
                if o0 == node("d") {
                    moved_were_ds += 1;
                }
            }
        }
        assert_eq!(moved, moved_were_ds);
        assert!(moved > 0, "removing a node should move its keys");
        // d held ~1/4 of keys; nothing else should have moved.
        assert!(
            moved < total / 2,
            "far too many keys moved: {moved}/{total}"
        );
    }

    #[test]
    fn a_joining_node_moves_only_a_minority() {
        let before = ring("a", &["b", "c", "d"]);
        let mut after = before.clone();
        after.observe(&node("e"), MemberState::Alive, "e:7000");

        let total = 2_000;
        let moved = (0..total)
            .filter(|i| {
                let c = format!("client-{i}");
                before.owner(&c) != after.owner(&c)
            })
            .count();
        // Ideal is ~1/5 (the new node's share); assert well under half — the
        // rendezvous property the durability design relies on.
        assert!(
            moved < total / 3,
            "too many keys moved on join: {moved}/{total}"
        );
    }
}
