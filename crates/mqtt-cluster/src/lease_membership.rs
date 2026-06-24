//! Reconciling SWIM membership into the lease group's openraft voter set
//! ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md) §2, workstream
//! E step 4d).
//!
//! The lease consensus group needs an explicit voter set; SWIM gives a
//! weakly-consistent, churning membership. This module is the policy that drives one
//! from the other:
//!
//! - **Pure decision** ([`MembershipReconciler::decide`]): given the current raft
//!   view and the desired stable voter set (the `Alive` members, mapped to
//!   [`RaftNodeId`](crate::lease_raft::RaftNodeId)), return the [`MembershipAction`]
//!   to take. **The founder bootstraps** (with itself); the elected leader then grows
//!   membership. Only the **leader** reconciles voters afterwards, and only once a
//!   prior change has settled (no overlapping joint-consensus changes). A non-leader /
//!   not-yet-bootstrapped node does nothing — the leader pulls it in as a learner.
//! - **Executor** ([`apply_action`]): perform the action against the raft handle
//!   (`initialize` / `add_learner` + `change_membership`).
//! - **View** ([`raft_view`]): read the current state from the raft's metrics.
//!
//! The caller (the live driver, step 4f) computes the desired set from SWIM, reads
//! [`raft_view`], calls [`decide`](MembershipReconciler::decide), and applies the
//! result — **debounced** so a flapping member does not churn the voter set. Keeping
//! `decide` pure makes the policy exhaustively unit-testable without a cluster.

use crate::lease_group::LeaseRaft;
use crate::lease_raft::RaftNodeId;
use openraft::{BasicNode, ServerState};
use std::collections::{BTreeMap, BTreeSet};

/// What to do to bring the lease group's voter set toward the desired membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipAction {
    /// Nothing to do.
    None,
    /// This node should bootstrap the group with `voters` (just itself).
    Initialize(BTreeSet<RaftNodeId>),
    /// This node (the leader) should set the voter set to `desired`, first adding
    /// `add_as_learner` (the new members) as learners so they can be promoted.
    SetVoters {
        /// The target voter set.
        desired: BTreeSet<RaftNodeId>,
        /// Members in `desired` not yet voters — to add as learners before promoting.
        add_as_learner: BTreeSet<RaftNodeId>,
    },
}

/// A snapshot of the local raft node's membership-relevant state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftView {
    /// Whether the group has been initialized (has a voter set).
    pub initialized: bool,
    /// Whether this node is the current leader (only the leader changes membership).
    pub is_leader: bool,
    /// Whether a membership change is still in flight (the config is in joint
    /// consensus). The leader must not propose another change until it settles, or
    /// openraft rejects it ("already undergoing a configuration change") — see
    /// [ADR 0026](../../../docs/adr/0026-lease-timing-durable-storage.md) §2.
    pub changing: bool,
    /// The current voter set.
    pub voters: BTreeSet<RaftNodeId>,
}

/// Errors from applying a [`MembershipAction`].
#[derive(Debug, thiserror::Error)]
pub enum MembershipError {
    /// A raft membership operation failed.
    #[error("membership change failed: {0}")]
    Raft(String),
}

/// The membership-reconciliation policy for one node.
#[derive(Debug, Clone, Copy)]
pub struct MembershipReconciler {
    local: RaftNodeId,
    can_bootstrap: bool,
}

impl MembershipReconciler {
    /// A reconciler for the node with raft id `local`.
    ///
    /// `can_bootstrap` gates whether this node may *create* the lease group (a
    /// **founder** — a node started with no SWIM seeds), and is the **sole** guard
    /// against a split-brain bootstrap: exactly one founder per cluster, so exactly one
    /// node ever initializes. A non-founder never initializes; it waits to be added by
    /// the founder's elected leader. (An earlier min-id tiebreak was removed — it both
    /// failed to prevent the multi-founder race and broke a legitimate non-min founder;
    /// ADR 0026 T7.)
    #[must_use]
    pub fn new(local: RaftNodeId, can_bootstrap: bool) -> Self {
        Self {
            local,
            can_bootstrap,
        }
    }

    /// Decide the action to take given the current `view` and the `desired` stable
    /// voter set. Pure — see the module docs for the policy.
    #[must_use]
    pub fn decide(&self, view: &RaftView, desired: &BTreeSet<RaftNodeId>) -> MembershipAction {
        if desired.is_empty() {
            return MembershipAction::None;
        }
        if !view.initialized {
            // A founder bootstraps the group with itself; the elected leader grows it
            // from there. `can_bootstrap` is the *sole* guard — exactly one founder
            // per cluster (a node started with no SWIM seeds), so exactly one node
            // ever initializes. There is deliberately no min-id tiebreak: it failed to
            // prevent the multi-founder race it was meant to (each founder first sees
            // only itself, so each is trivially its own min and would still bootstrap)
            // while wrongly blocking a legitimate founder that was not the global min
            // id — so the durable group never formed at all (ADR 0026 T7). Non-founders
            // wait to be added by the leader.
            if self.can_bootstrap {
                let mut just_me = BTreeSet::new();
                just_me.insert(self.local);
                return MembershipAction::Initialize(just_me);
            }
            return MembershipAction::None;
        }
        // Initialized: only the leader reconciles the voter set.
        if !view.is_leader {
            return MembershipAction::None;
        }
        // A membership change is still settling (joint consensus): wait for it rather
        // than re-proposing, which openraft rejects as already in progress and which —
        // re-fired every driver tick under churn — amplifies the churn (ADR 0026 §2).
        if view.changing {
            return MembershipAction::None;
        }
        if &view.voters != desired {
            let add_as_learner = desired.difference(&view.voters).copied().collect();
            return MembershipAction::SetVoters {
                desired: desired.clone(),
                add_as_learner,
            };
        }
        MembershipAction::None
    }
}

/// The current [`RaftView`] of `raft`, read from its metrics.
#[must_use]
pub fn raft_view(raft: &LeaseRaft) -> RaftView {
    let metrics = raft.metrics().borrow().clone();
    let membership = metrics.membership_config.membership();
    let voters: BTreeSet<RaftNodeId> = membership.voter_ids().collect();
    RaftView {
        initialized: !voters.is_empty(),
        is_leader: metrics.state == ServerState::Leader,
        // A joint (transitional) config carries >1 config set; a settled uniform one carries
        // exactly one. More than one means a membership change is still in flight.
        changing: membership.get_joint_config().len() > 1,
        voters,
    }
}

/// Apply a [`MembershipAction`] to `raft`.
///
/// `Initialize` bootstraps the group; `SetVoters` adds the new members as learners
/// (blocking until they catch up) and then replaces the voter set. Removed voters
/// are dropped (not retained as learners).
///
/// # Errors
/// [`MembershipError::Raft`] if a raft membership operation fails.
pub async fn apply_action(
    raft: &LeaseRaft,
    action: &MembershipAction,
) -> Result<(), MembershipError> {
    match action {
        MembershipAction::None => Ok(()),
        MembershipAction::Initialize(voters) => {
            let members: BTreeMap<RaftNodeId, BasicNode> = voters
                .iter()
                .map(|id| (*id, BasicNode::default()))
                .collect();
            raft.initialize(members)
                .await
                .map_err(|e| MembershipError::Raft(e.to_string()))
        }
        MembershipAction::SetVoters {
            desired,
            add_as_learner,
        } => {
            for id in add_as_learner {
                raft.add_learner(*id, BasicNode::default(), true)
                    .await
                    .map_err(|e| MembershipError::Raft(e.to_string()))?;
            }
            // A BTreeSet<NodeId> converts to ChangeMembers::ReplaceAllVoters.
            raft.change_membership(desired.clone(), false)
                .await
                .map_err(|e| MembershipError::Raft(e.to_string()))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_action, raft_view, MembershipAction, MembershipReconciler, RaftView};
    use crate::cluster_log::ReplicaState;
    use crate::durable_plane::DurablePlane;
    use crate::lease_group::{config, LeaseRaft};
    use crate::lease_raft::{LeaseRecord, LeaseRequest, RaftNodeId};
    use crate::lease_store::LeaseStore;
    use crate::node_registry::raft_id;
    use crate::peer::{self, PeerMessage};
    use crate::raft_mesh::MeshRaftNetwork;
    use crate::repl_net::PeerReplicaTransport;
    use crate::NodeId;
    use bytes::BytesMut;
    use openraft::storage::Adaptor;
    use openraft::{Raft, ServerState};
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::mpsc;

    fn n(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    fn set(ids: &[RaftNodeId]) -> BTreeSet<RaftNodeId> {
        ids.iter().copied().collect()
    }

    // ---- pure policy ----

    #[test]
    fn empty_desired_is_a_noop() {
        let r = MembershipReconciler::new(1, true);
        let view = RaftView {
            initialized: false,
            is_leader: false,
            changing: false,
            voters: set(&[]),
        };
        assert_eq!(r.decide(&view, &set(&[])), MembershipAction::None);
    }

    #[test]
    fn any_founder_bootstraps_with_itself_regardless_of_id_rank() {
        let view = RaftView {
            initialized: false,
            is_leader: false,
            changing: false,
            voters: set(&[]),
        };
        // The founder bootstraps with itself whether or not it is the smallest id —
        // `can_bootstrap` is the sole guard (ADR 0026 T7). Node 1 (the min):
        assert_eq!(
            MembershipReconciler::new(1, true).decide(&view, &set(&[1, 2, 3])),
            MembershipAction::Initialize(set(&[1])),
        );
        // ...and node 2 (NOT the min) — the case the old min-id tiebreak wrongly blocked,
        // leaving the durable group unformed.
        assert_eq!(
            MembershipReconciler::new(2, true).decide(&view, &set(&[1, 2, 3])),
            MembershipAction::Initialize(set(&[2])),
        );
    }

    #[test]
    fn a_non_founder_never_bootstraps() {
        let view = RaftView {
            initialized: false,
            is_leader: false,
            changing: false,
            voters: set(&[]),
        };
        // Not a founder (started with seeds) — it waits to be added rather than starting
        // a rival group, even though it is the smallest id.
        assert_eq!(
            MembershipReconciler::new(1, false).decide(&view, &set(&[1, 2, 3])),
            MembershipAction::None,
        );
    }

    #[test]
    fn only_the_leader_reconciles_voters() {
        let desired = set(&[1, 2, 3]);
        let follower = RaftView {
            initialized: true,
            is_leader: false,
            changing: false,
            voters: set(&[1]),
        };
        assert_eq!(
            MembershipReconciler::new(2, true).decide(&follower, &desired),
            MembershipAction::None,
        );
    }

    #[test]
    fn a_leader_does_not_re_propose_while_a_change_is_in_flight() {
        // Leader, voters {1} but desired {1,2,3} — yet a change is already settling
        // (joint consensus). It must wait, not fire a second change (ADR 0026 §2).
        let changing = RaftView {
            initialized: true,
            is_leader: true,
            changing: true,
            voters: set(&[1]),
        };
        assert_eq!(
            MembershipReconciler::new(1, true).decide(&changing, &set(&[1, 2, 3])),
            MembershipAction::None,
        );
        // Once it settles (changing = false), the same gap is reconciled.
        let settled = RaftView {
            changing: false,
            ..changing
        };
        assert_eq!(
            MembershipReconciler::new(1, true).decide(&settled, &set(&[1, 2, 3])),
            MembershipAction::SetVoters {
                desired: set(&[1, 2, 3]),
                add_as_learner: set(&[2, 3]),
            },
        );
    }

    #[test]
    fn leader_grows_and_shrinks_the_voter_set() {
        let r = MembershipReconciler::new(1, true);
        // Grow {1} -> {1,2,3}: add 2 and 3 as learners.
        let view = RaftView {
            initialized: true,
            is_leader: true,
            changing: false,
            voters: set(&[1]),
        };
        assert_eq!(
            r.decide(&view, &set(&[1, 2, 3])),
            MembershipAction::SetVoters {
                desired: set(&[1, 2, 3]),
                add_as_learner: set(&[2, 3]),
            },
        );
        // Shrink {1,2,3} -> {1,2}: no learners to add, just replace.
        let view = RaftView {
            initialized: true,
            is_leader: true,
            changing: false,
            voters: set(&[1, 2, 3]),
        };
        assert_eq!(
            r.decide(&view, &set(&[1, 2])),
            MembershipAction::SetVoters {
                desired: set(&[1, 2]),
                add_as_learner: set(&[]),
            },
        );
        // Already at target: nothing to do.
        let view = RaftView {
            initialized: true,
            is_leader: true,
            changing: false,
            voters: set(&[1, 2]),
        };
        assert_eq!(r.decide(&view, &set(&[1, 2])), MembershipAction::None);
    }

    // ---- live bring-up + grow ----

    async fn node(id: &str) -> (DurablePlane, LeaseStore) {
        let net = MeshRaftNetwork::new();
        let store = LeaseStore::new();
        let (ls, sm) = Adaptor::new(store.clone());
        let raft: LeaseRaft = Raft::new(raft_id(&n(id)), config(), net.clone(), ls, sm)
            .await
            .unwrap();
        let plane = DurablePlane::new(
            raft,
            net,
            Arc::new(PeerReplicaTransport::new()),
            Arc::new(Mutex::new(ReplicaState::new())),
        );
        (plane, store)
    }

    async fn read_frame(
        rh: &mut (impl tokio::io::AsyncRead + Unpin),
        buf: &mut BytesMut,
    ) -> Option<PeerMessage> {
        loop {
            if let Ok(Some(msg)) = peer::decode(buf) {
                return Some(msg);
            }
            if rh.read_buf(buf).await.ok()? == 0 {
                return None;
            }
        }
    }

    fn spawn_link(
        plane: DurablePlane,
        io: DuplexStream,
        out_tx: mpsc::UnboundedSender<PeerMessage>,
        mut out_rx: mpsc::UnboundedReceiver<PeerMessage>,
    ) {
        let (mut rh, mut wh) = tokio::io::split(io);
        tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                let mut bytes = Vec::new();
                peer::encode(&msg, &mut bytes).unwrap();
                if wh.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });
        tokio::spawn(async move {
            let mut buf = BytesMut::new();
            while let Some(frame) = read_frame(&mut rh, &mut buf).await {
                if let Some(reply) = plane.handle(frame).await {
                    let _ = out_tx.send(reply);
                }
            }
        });
    }

    /// The reconciler bootstraps a node and then grows the group to a second node —
    /// over the wire — after which a committed lease replicates to the new voter.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reconciler_bootstraps_then_grows_the_group() {
        let (p1, s1) = node("m-node-1").await;
        let (p2, s2) = node("m-node-2").await;
        let r1 = raft_id(&n("m-node-1"));
        let r2 = raft_id(&n("m-node-2"));
        let desired = set(&[r1, r2]);

        // Wire the two planes together.
        let (io1, io2) = tokio::io::duplex(256 * 1024);
        let (out1_tx, out1_rx) = mpsc::unbounded_channel();
        let (out2_tx, out2_rx) = mpsc::unbounded_channel();
        p1.register(&n("m-node-2"), out1_tx.clone());
        p2.register(&n("m-node-1"), out2_tx.clone());
        spawn_link(p1.clone(), io1, out1_tx, out1_rx);
        spawn_link(p2.clone(), io2, out2_tx, out2_rx);

        // The bootstrap node is min(desired); run the reconciler there.
        let boot_is_1 = *desired.iter().min().unwrap() == r1;
        let (boot, boot_id, other_store) = if boot_is_1 {
            (&p1, r1, &s2)
        } else {
            (&p2, r2, &s1)
        };
        let recon = MembershipReconciler::new(boot_id, true);

        // Step 1: bootstrap (Initialize with self), then it wins the election.
        let action = recon.decide(&raft_view(boot.raft()), &desired);
        assert!(matches!(action, MembershipAction::Initialize(_)));
        apply_action(boot.raft(), &action).await.unwrap();
        boot.raft()
            .wait(Some(Duration::from_secs(15)))
            .state(ServerState::Leader, "bootstrap node leads")
            .await
            .unwrap();

        // Step 2: as leader, grow membership to the full desired set.
        let action = recon.decide(&raft_view(boot.raft()), &desired);
        assert!(matches!(action, MembershipAction::SetVoters { .. }));
        apply_action(boot.raft(), &action).await.unwrap();

        // Both nodes are now voters.
        boot.raft()
            .wait(Some(Duration::from_secs(15)))
            .metrics(
                |m| m.membership_config.voter_ids().count() == 2,
                "both nodes are voters",
            )
            .await
            .unwrap();

        // A committed lease replicates to the grown member.
        let resp = boot
            .raft()
            .client_write(LeaseRequest::Assign {
                group: 1,
                node: boot_id,
            })
            .await
            .unwrap();
        let epoch = resp.data.unwrap().epoch;
        // The non-bootstrap node applied it.
        let target = if boot_is_1 { &p2 } else { &p1 };
        target
            .raft()
            .wait(Some(Duration::from_secs(15)))
            .applied_index_at_least(Some(resp.log_id.index), "follower applied the lease")
            .await
            .unwrap();
        assert_eq!(
            other_store.current_lease(1),
            Some(LeaseRecord {
                holder: boot_id,
                epoch
            })
        );

        p1.raft().shutdown().await.unwrap();
        p2.raft().shutdown().await.unwrap();
    }
}
