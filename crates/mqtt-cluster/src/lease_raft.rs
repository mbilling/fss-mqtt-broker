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
    /// (Re)assign many groups' leases in one committed entry, each minting its own
    /// fresh epoch (in order). The leader's reconcile batches all pending
    /// assignments into a single consensus write rather than one per group, so a
    /// fresh leader (or a rebalance) does not burst hundreds of tiny entries through
    /// the log.
    AssignMany {
        /// The `(group, node)` assignments to apply, in order.
        assignments: Vec<(GroupId, RaftNodeId)>,
    },
    // ── The replication factor (ADR 0080). Appended variants: a build that
    // predates them cannot decode them, so they are proposed only once every
    // member speaks `PROTO_REPLICATION_FACTOR`.
    /// Found the cluster's replication factor. Applied only while none is
    /// committed; a cluster that already has one changes it through
    /// [`BeginReplicaChange`](Self::BeginReplicaChange).
    SetReplicas {
        /// The replication factor, in [`REPLICAS_MIN`]..=[`REPLICAS_MAX`].
        r: u8,
    },
    /// Open the joint phase of a live replication-factor change (ADR 0080 §4):
    /// until it is committed, a group's appends need a quorum of its `from` set and
    /// of its `to` set. Applied only when `from` is the committed factor (3 when
    /// none is) and no change is open.
    BeginReplicaChange {
        /// The factor in force.
        from: u8,
        /// The factor being moved to.
        to: u8,
    },
    /// Close the open change: `to` becomes the committed factor. Applied only
    /// when it names the open change's target.
    CommitReplicaChange {
        /// The factor the open change moves to.
        to: u8,
    },
}

/// The replication factor a cluster with no committed one runs at: every cluster
/// that predates ADR 0080.
pub const REPLICAS_LEGACY: u8 = 3;
/// The smallest replication factor a cluster may run at.
pub const REPLICAS_MIN: u8 = 2;
/// The largest: seven copies already tolerate three failures.
pub const REPLICAS_MAX: u8 = 7;

/// The cluster's replication factor as the lease group agreed it (ADR 0080 §1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationRecord {
    /// The committed factor; `None` for a cluster that never set one.
    pub replicas: Option<u8>,
    /// A live change in progress, `(from, to)`: the joint phase.
    pub change: Option<(u8, u8)>,
}

impl ReplicationRecord {
    /// The factor in force: the committed one, else [`REPLICAS_LEGACY`].
    #[must_use]
    pub fn effective(&self) -> u8 {
        self.replicas.unwrap_or(REPLICAS_LEGACY)
    }

    /// Whether nothing has ever been recorded — the state every pre-ADR-0080
    /// cluster is in, which is persisted in the legacy shape.
    #[must_use]
    pub fn is_unset(&self) -> bool {
        *self == Self::default()
    }

    fn valid(r: u8) -> bool {
        (REPLICAS_MIN..=REPLICAS_MAX).contains(&r)
    }

    /// Apply one replication command. Deterministic, as every state-machine
    /// transition must be: a command whose precondition does not hold is a no-op
    /// on every replica alike, never an error.
    fn apply(&mut self, req: &LeaseRequest) {
        match *req {
            LeaseRequest::SetReplicas { r } => {
                if self.replicas.is_none() && self.change.is_none() && Self::valid(r) {
                    self.replicas = Some(r);
                }
            }
            LeaseRequest::BeginReplicaChange { from, to } => {
                if self.change.is_none()
                    && from == self.effective()
                    && Self::valid(to)
                    && to != from
                {
                    self.change = Some((from, to));
                }
            }
            LeaseRequest::CommitReplicaChange { to } => {
                if self.change.is_some_and(|(_, target)| target == to) {
                    self.replicas = Some(to);
                    self.change = None;
                }
            }
            LeaseRequest::Assign { .. } | LeaseRequest::AssignMany { .. } => {}
        }
    }
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
///
/// Its serde shape is the pre-ADR-0080 one (`leases`, `next_epoch`); the
/// replication record rides beside it only through [`encode_state`] /
/// [`decode_state`], so a cluster that never sets a factor persists and ships
/// byte-identical state to what an older build reads.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LeaseMap {
    leases: BTreeMap<GroupId, LeaseRecord>,
    next_epoch: Epoch,
    #[serde(skip)]
    replication: ReplicationRecord,
}

/// Encode the whole state machine for persistence and snapshots (ADR 0080 §1).
///
/// While no replication factor was ever recorded this is exactly the legacy
/// encoding of the lease table, so an older build in a rolling upgrade still
/// decodes it. Once one is recorded — which the capability gate allows only when
/// every member is new enough — it is the table followed by the record.
///
/// # Errors
/// A postcard encoding failure.
pub fn encode_state(map: &LeaseMap) -> Result<Vec<u8>, postcard::Error> {
    if map.replication.is_unset() {
        postcard::to_allocvec(map)
    } else {
        postcard::to_allocvec(&(map, &map.replication))
    }
}

/// Decode what [`encode_state`] wrote, from either shape. Strict: each shape
/// must consume the bytes exactly, and neither is a prefix-complete reading of
/// the other (the extended one has bytes left over as legacy; the legacy one
/// runs out as extended), so the two cannot be confused.
///
/// # Errors
/// Bytes that are neither shape.
pub fn decode_state(bytes: &[u8]) -> Result<LeaseMap, postcard::Error> {
    if let Ok(((mut map, replication), rest)) =
        postcard::take_from_bytes::<(LeaseMap, ReplicationRecord)>(bytes)
    {
        if rest.is_empty() {
            map.replication = replication;
            return Ok(map);
        }
    }
    match postcard::take_from_bytes::<LeaseMap>(bytes)? {
        (map, []) => Ok(map),
        _ => Err(postcard::Error::DeserializeBadEncoding),
    }
}

impl LeaseMap {
    /// An empty lease table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a committed request. Returns the resulting lease for a single
    /// [`Assign`](LeaseRequest::Assign), the **last** lease for an
    /// [`AssignMany`](LeaseRequest::AssignMany) (each is minted in order), or `None`
    /// for an empty batch.
    ///
    /// Each assignment mints a **strictly increasing** epoch from a monotonic
    /// counter, so a new holder always supersedes the previous one and a stale
    /// holder is fenced at the replication layer (ADR 0006 §1).
    pub fn apply(&mut self, req: &LeaseRequest) -> Option<LeaseResponse> {
        match req {
            LeaseRequest::Assign { group, node } => Some(self.assign_one(*group, *node)),
            LeaseRequest::AssignMany { assignments } => assignments
                .iter()
                .map(|(group, node)| self.assign_one(*group, *node))
                .last(),
            LeaseRequest::SetReplicas { .. }
            | LeaseRequest::BeginReplicaChange { .. }
            | LeaseRequest::CommitReplicaChange { .. } => {
                self.replication.apply(req);
                None
            }
        }
    }

    /// The cluster's replication factor as committed (ADR 0080).
    #[must_use]
    pub fn replication(&self) -> ReplicationRecord {
        self.replication
    }

    /// Assign one group to `node` at a fresh epoch, returning the minted lease.
    fn assign_one(&mut self, group: GroupId, node: RaftNodeId) -> LeaseResponse {
        self.next_epoch += 1;
        let epoch = self.next_epoch;
        self.leases.insert(
            group,
            LeaseRecord {
                holder: node,
                epoch,
            },
        );
        LeaseResponse {
            group,
            holder: node,
            epoch,
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
    ///
    /// The response is `Option<LeaseResponse>`: a committed `Normal` entry (an
    /// `Assign` or `AssignMany`) yields `Some(lease)` (the last, for a batch), while
    /// the `Blank`/`Membership` entries Raft commits internally — and an empty batch
    /// — yield `None`.
    pub LeaseConfig:
        D = LeaseRequest,
        R = Option<LeaseResponse>,
        NodeId = RaftNodeId,
        Node = openraft::BasicNode,
);

#[cfg(test)]
mod tests {
    use super::{
        decode_state, encode_state, LeaseConfig, LeaseMap, LeaseRequest, RaftNodeId,
        ReplicationRecord, REPLICAS_LEGACY,
    };

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
        let r = m.apply(&assign(1, 10)).unwrap();
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
        assert_eq!(m.apply(&assign(1, 10)).unwrap().epoch, 1);
        let r = m.apply(&assign(1, 20)).unwrap();
        assert_eq!(r.epoch, 2);
        assert_eq!(m.get(1).unwrap().holder, 20);
        assert_eq!(m.get(1).unwrap().epoch, 2);
    }

    /// Epochs are globally monotonic across groups (one shared counter), so no two
    /// assignments ever share an epoch.
    #[test]
    fn epochs_are_globally_monotonic_across_groups() {
        let mut m = LeaseMap::new();
        assert_eq!(m.apply(&assign(1, 10)).unwrap().epoch, 1);
        assert_eq!(m.apply(&assign(2, 10)).unwrap().epoch, 2);
        assert_eq!(m.apply(&assign(1, 20)).unwrap().epoch, 3);
        assert_eq!(m.high_epoch(), 3);
        assert_eq!(m.get(1).unwrap().epoch, 3);
        assert_eq!(m.get(2).unwrap().epoch, 2);
    }

    /// A batched `AssignMany` mints a fresh, increasing epoch per assignment (in
    /// order) and applies them all, returning the last — equivalent to the same
    /// sequence of single `Assign`s but in one committed entry.
    #[test]
    fn assign_many_applies_each_at_a_fresh_epoch() {
        let mut m = LeaseMap::new();
        let last = m
            .apply(&LeaseRequest::AssignMany {
                assignments: vec![(1, 10), (2, 20), (3, 30)],
            })
            .unwrap();
        assert_eq!(last.group, 3);
        assert_eq!(last.epoch, 3);
        assert_eq!(
            m.get(1).unwrap(),
            super::LeaseRecord {
                holder: 10,
                epoch: 1
            }
        );
        assert_eq!(
            m.get(2).unwrap(),
            super::LeaseRecord {
                holder: 20,
                epoch: 2
            }
        );
        assert_eq!(
            m.get(3).unwrap(),
            super::LeaseRecord {
                holder: 30,
                epoch: 3
            }
        );
        assert_eq!(m.high_epoch(), 3);

        // An empty batch is a no-op with no response.
        assert!(m
            .apply(&LeaseRequest::AssignMany {
                assignments: vec![],
            })
            .is_none());
        assert_eq!(m.high_epoch(), 3);
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
        let bytes = postcard::to_allocvec(&m).unwrap();
        let back: LeaseMap = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back.get(1), m.get(1));
        assert_eq!(back.get(2), m.get(2));
        assert_eq!(back.high_epoch(), m.high_epoch());
    }

    /// ADR 0080: the founding command applies once, with a valid factor, and a
    /// cluster that never ran it reads as the legacy 3.
    #[test]
    fn set_replicas_founds_the_factor_once() {
        let mut m = LeaseMap::new();
        assert_eq!(m.replication().effective(), REPLICAS_LEGACY);
        assert!(m.replication().is_unset());
        for bad in [0u8, 1, 8, 255] {
            assert!(m.apply(&LeaseRequest::SetReplicas { r: bad }).is_none());
            assert!(m.replication().is_unset(), "factor {bad} is outside 2..=7");
        }
        m.apply(&LeaseRequest::SetReplicas { r: 2 });
        assert_eq!(m.replication().replicas, Some(2));
        m.apply(&LeaseRequest::SetReplicas { r: 5 });
        assert_eq!(
            m.replication().effective(),
            2,
            "a founded factor changes only through the joint phase"
        );
    }

    /// ADR 0080 §4: the live change opens only from the factor in force, cannot
    /// stack, and closes only on its own target.
    #[test]
    fn a_replica_change_opens_from_the_factor_in_force_and_closes_on_its_target() {
        let mut m = LeaseMap::new();
        // Unset reads as 3, so a change must start from 3.
        m.apply(&LeaseRequest::BeginReplicaChange { from: 2, to: 5 });
        assert_eq!(
            m.replication().change,
            None,
            "from must be the factor in force"
        );
        m.apply(&LeaseRequest::BeginReplicaChange { from: 3, to: 3 });
        assert_eq!(
            m.replication().change,
            None,
            "a change to itself is no change"
        );
        m.apply(&LeaseRequest::BeginReplicaChange { from: 3, to: 2 });
        assert_eq!(m.replication().change, Some((3, 2)));
        m.apply(&LeaseRequest::BeginReplicaChange { from: 3, to: 5 });
        assert_eq!(m.replication().change, Some((3, 2)), "changes do not stack");
        m.apply(&LeaseRequest::SetReplicas { r: 5 });
        assert_eq!(
            m.replication().replicas,
            None,
            "no founding during a change"
        );
        m.apply(&LeaseRequest::CommitReplicaChange { to: 5 });
        assert_eq!(
            m.replication().change,
            Some((3, 2)),
            "commit names the open target"
        );
        m.apply(&LeaseRequest::CommitReplicaChange { to: 2 });
        assert_eq!(
            m.replication(),
            ReplicationRecord {
                replicas: Some(2),
                change: None
            }
        );
    }

    /// Replication commands mint no epoch and touch no lease.
    #[test]
    fn replication_commands_leave_the_lease_table_alone() {
        let mut m = LeaseMap::new();
        m.apply(&assign(1, 10));
        m.apply(&LeaseRequest::SetReplicas { r: 2 });
        m.apply(&LeaseRequest::BeginReplicaChange { from: 2, to: 3 });
        m.apply(&LeaseRequest::CommitReplicaChange { to: 3 });
        assert_eq!(m.high_epoch(), 1);
        assert_eq!(m.get(1).unwrap().holder, 10);
    }

    /// The rolling-upgrade contract (ADR 0080 §1): while no factor is recorded the
    /// state encodes byte for byte as the pre-ADR-0080 lease table, so an older
    /// build reads what a newer one persisted or shipped; legacy bytes decode as
    /// an unset factor.
    #[test]
    fn unset_state_encodes_exactly_as_the_legacy_table() {
        let mut m = LeaseMap::new();
        m.apply(&assign(1, 10));
        m.apply(&assign(2, 20));
        let legacy = postcard::to_allocvec(&m).unwrap();
        assert_eq!(encode_state(&m).unwrap(), legacy);
        let back = decode_state(&legacy).unwrap();
        assert!(back.replication().is_unset());
        assert_eq!(back.get(2), m.get(2));
        assert_eq!(back.high_epoch(), 2);
    }

    /// Once a factor is recorded the extended encoding round-trips it — and an
    /// older build's strict decode of the bare table refuses those bytes instead
    /// of silently dropping the factor.
    #[test]
    fn a_recorded_factor_round_trips_and_is_not_misread_as_legacy() {
        let mut m = LeaseMap::new();
        m.apply(&assign(1, 10));
        m.apply(&LeaseRequest::SetReplicas { r: 2 });
        m.apply(&LeaseRequest::BeginReplicaChange { from: 2, to: 3 });
        let bytes = encode_state(&m).unwrap();
        let back = decode_state(&bytes).unwrap();
        assert_eq!(back.replication(), m.replication());
        assert_eq!(back.get(1), m.get(1));
        let (_, rest) = postcard::take_from_bytes::<LeaseMap>(&bytes).unwrap();
        assert!(
            !rest.is_empty(),
            "a legacy strict decoder sees trailing bytes and fails closed"
        );
        assert!(decode_state(&bytes[..bytes.len() - 1]).is_err());
    }

    /// The openraft types the store persists and the mesh ships round-trip
    /// under postcard (ADR 0052) — pinned here so a codec or openraft upgrade
    /// that changes a serde shape fails at `cargo test`, not in a cluster.
    #[test]
    fn openraft_wire_types_roundtrip_under_postcard() {
        use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, StoredMembership, Vote};

        let log_id = LogId::new(CommittedLeaderId::new(3, 1), 7);
        let entry = Entry::<LeaseConfig> {
            log_id,
            payload: EntryPayload::Normal(assign(1, 10)),
        };
        let bytes = postcard::to_allocvec(&entry).unwrap();
        let back: Entry<LeaseConfig> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back.log_id, entry.log_id);

        let vote = Vote::<u64>::new(3, 1);
        let bytes = postcard::to_allocvec(&vote).unwrap();
        assert_eq!(postcard::from_bytes::<Vote<u64>>(&bytes).unwrap(), vote);

        let membership = StoredMembership::<u64, openraft::BasicNode>::default();
        let bytes = postcard::to_allocvec(&membership).unwrap();
        assert_eq!(
            postcard::from_bytes::<StoredMembership<u64, openraft::BasicNode>>(&bytes).unwrap(),
            membership
        );
    }
}
