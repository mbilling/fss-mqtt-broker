//! The lease consensus state machine and its openraft binding
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md), workstream E
//! step 3b-ii).
//!
//! ADR 0006 scopes consensus to the **ownership lease**: per placement group, which
//! node holds the lease and at what epoch. That little, low-traffic decision is what
//! we run through openraft (the ratified engine); the high-traffic session-log
//! replication does *not* go through it (that is [`cluster_log`](crate::cluster_log)).
//!
//! This module is the part we design — what the consensus group *agrees on*:
//! [`LeaseMap`], the replicated table of `group -> (holder, epoch)`, with its pure
//! [`apply`](LeaseMap::apply). Each assignment takes a **strictly increasing epoch**
//! (a monotonic counter in the replicated state), so a newly-assigned holder always
//! supersedes the previous one — the fence token [`cluster_log`](crate::cluster_log)
//! and [`repl_net`](crate::repl_net) already carry to reject a stale holder.
//!
//! ## Node ids
//!
//! openraft requires a `Copy` node id, so the consensus group uses numeric
//! [`RaftNodeId`]s. The cluster's string [`NodeId`](crate::NodeId) (a certificate
//! CN) is mapped to a stable `RaftNodeId` by the wiring layer (the next sub-step),
//! not here — this module stays a pure, deterministic state machine.
//!
//! [`LeaseConfig`] binds these types to openraft via `declare_raft_types!`; a
//! compile-time assertion in the tests pins that it is a valid `RaftTypeConfig`.
//! The storage and network trait impls that drive a live group are the next
//! sub-steps (3b-ii storage; 3b-ii network over the peer mesh).

use crate::lease::Epoch;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
// Required in scope by `declare_raft_types!` (its default `SnapshotData`).
use std::io::Cursor;

/// Identifier of a placement group whose ownership lease is under consensus.
pub type GroupId = u64;

/// A consensus-group node id (openraft requires `Copy`); mapped from the cluster's
/// string [`NodeId`](crate::NodeId) by the wiring layer.
pub type RaftNodeId = u64;

/// A command the lease consensus group agrees on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseRequest {
    /// (Re)assign `group`'s ownership lease to `node`, minting a fresh epoch.
    Assign {
        /// The placement group whose lease is being assigned.
        group: GroupId,
        /// The node to grant the lease to.
        node: RaftNodeId,
    },
}

/// The result of applying a [`LeaseRequest`]: the group's now-current lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseResponse {
    /// The group the lease is for.
    pub group: GroupId,
    /// The node now holding the lease.
    pub holder: RaftNodeId,
    /// The epoch minted for this assignment (strictly increasing).
    pub epoch: Epoch,
}

/// One group's current lease: who holds it and at what epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// The node holding the lease.
    pub holder: RaftNodeId,
    /// The epoch the lease was minted at (the fence token).
    pub epoch: Epoch,
}

/// The replicated lease table — the state machine openraft drives.
///
/// Pure and deterministic: replaying the same committed [`LeaseRequest`]s on any
/// replica yields the same table (the requirement for a Raft state machine).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LeaseMap {
    leases: BTreeMap<GroupId, LeaseRecord>,
    next_epoch: Epoch,
}

impl LeaseMap {
    /// An empty lease table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a committed request, returning the resulting lease.
    ///
    /// Each assignment mints a **strictly increasing** epoch from a monotonic
    /// counter, so a new holder always supersedes the previous one and a stale
    /// holder is fenced at the replication layer (ADR 0006 §1).
    pub fn apply(&mut self, req: &LeaseRequest) -> LeaseResponse {
        match req {
            LeaseRequest::Assign { group, node } => {
                self.next_epoch += 1;
                let epoch = self.next_epoch;
                self.leases.insert(
                    *group,
                    LeaseRecord {
                        holder: *node,
                        epoch,
                    },
                );
                LeaseResponse {
                    group: *group,
                    holder: *node,
                    epoch,
                }
            }
        }
    }

    /// The current lease for `group`, if one has been assigned.
    #[must_use]
    pub fn get(&self, group: GroupId) -> Option<LeaseRecord> {
        self.leases.get(&group).copied()
    }

    /// The highest epoch minted so far (0 if none) — the monotonic fence source.
    #[must_use]
    pub fn high_epoch(&self) -> Epoch {
        self.next_epoch
    }
}

openraft::declare_raft_types!(
    /// openraft type binding for the lease consensus group: our request/response
    /// over numeric [`RaftNodeId`]s. Storage, network, and the remaining defaults
    /// are supplied by openraft.
    pub LeaseConfig:
        D = LeaseRequest,
        R = LeaseResponse,
        NodeId = RaftNodeId,
        Node = openraft::BasicNode,
);

#[cfg(test)]
mod tests {
    use super::{LeaseConfig, LeaseMap, LeaseRequest, RaftNodeId};

    fn assign(group: u64, node: RaftNodeId) -> LeaseRequest {
        LeaseRequest::Assign { group, node }
    }

    /// `LeaseConfig` must be a valid openraft `RaftTypeConfig` — this fails to
    /// compile if any associated type (D/R/NodeId/Node/...) violates a bound.
    #[test]
    fn lease_config_is_a_valid_raft_type_config() {
        fn assert_cfg<C: openraft::RaftTypeConfig>() {}
        assert_cfg::<LeaseConfig>();
    }

    #[test]
    fn assign_mints_a_lease_at_a_fresh_epoch() {
        let mut m = LeaseMap::new();
        let r = m.apply(&assign(1, 10));
        assert_eq!(r.epoch, 1);
        assert_eq!(r.holder, 10);
        let lease = m.get(1).unwrap();
        assert_eq!(lease.holder, 10);
        assert_eq!(lease.epoch, 1);
    }

    /// Reassigning a group bumps the epoch, so the new holder supersedes the old.
    #[test]
    fn reassign_bumps_the_epoch_monotonically() {
        let mut m = LeaseMap::new();
        assert_eq!(m.apply(&assign(1, 10)).epoch, 1);
        let r = m.apply(&assign(1, 20));
        assert_eq!(r.epoch, 2);
        assert_eq!(m.get(1).unwrap().holder, 20);
        assert_eq!(m.get(1).unwrap().epoch, 2);
    }

    /// Epochs are globally monotonic across groups (one shared counter), so no two
    /// assignments ever share an epoch.
    #[test]
    fn epochs_are_globally_monotonic_across_groups() {
        let mut m = LeaseMap::new();
        assert_eq!(m.apply(&assign(1, 10)).epoch, 1);
        assert_eq!(m.apply(&assign(2, 10)).epoch, 2);
        assert_eq!(m.apply(&assign(1, 20)).epoch, 3);
        assert_eq!(m.high_epoch(), 3);
        assert_eq!(m.get(1).unwrap().epoch, 3);
        assert_eq!(m.get(2).unwrap().epoch, 2);
    }

    #[test]
    fn unknown_group_has_no_lease() {
        let m = LeaseMap::new();
        assert!(m.get(99).is_none());
    }

    /// Deterministic replay: applying the same committed sequence on a second
    /// table yields the same leases (the Raft state-machine requirement).
    #[test]
    fn replay_is_deterministic() {
        let ops = [assign(1, 10), assign(2, 20), assign(1, 30)];
        let mut m1 = LeaseMap::new();
        let mut m2 = LeaseMap::new();
        for op in &ops {
            m1.apply(op);
        }
        for op in &ops {
            m2.apply(op);
        }
        assert_eq!(m1.get(1), m2.get(1));
        assert_eq!(m1.get(2), m2.get(2));
        assert_eq!(m1.high_epoch(), m2.high_epoch());
    }

    /// The lease table round-trips through serde (it is the replicated snapshot).
    #[test]
    fn lease_map_serde_roundtrips() {
        let mut m = LeaseMap::new();
        m.apply(&assign(1, 10));
        m.apply(&assign(2, 20));
        let bytes = bincode::serialize(&m).unwrap();
        let back: LeaseMap = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back.get(1), m.get(1));
        assert_eq!(back.get(2), m.get(2));
        assert_eq!(back.high_epoch(), m.high_epoch());
    }
}
