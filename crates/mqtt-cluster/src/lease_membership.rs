//! Reconciling SWIM membership into the lease group's openraft voter set
//! ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md) §2, workstream
//! E step 4d).
//!
//! The lease consensus group needs an explicit voter set; SWIM gives a
//! weakly-consistent, churning membership. This module is the policy that drives one
//! from the other:
//!
//! - **Pure decision** ([`MembershipReconciler::decide`]): given the current raft
//!   view and the `eligible` member set (the `Alive`, admitted members, mapped to
//!   [`RaftNodeId`](crate::lease_raft::RaftNodeId)), return the [`MembershipAction`]
//!   to take. **The founder bootstraps** (with itself); the elected leader then grows
//!   membership. Only the **leader** reconciles afterwards, and only once a prior change
//!   has settled (no overlapping joint-consensus changes). A non-leader /
//!   not-yet-bootstrapped node does nothing — the leader pulls it in as a learner.
//! - **Executor** ([`apply_action`]): perform the action against the raft handle
//!   (`initialize` / `add_learner` + `change_membership`).
//! - **View** ([`raft_view`]): read the current state from the raft's metrics.
//!
//! **Bounded voters** ([ADR 0021](../../../docs/adr/0021-bounded-lease-voters.md)): the
//! voter set is capped at a small `N` ([`target_voters`], sticky vacancy-fill); every
//! other eligible member joins as a non-voting *learner* that still receives the committed
//! lease log and can own/serve placement groups. So consensus cost (quorum, election size)
//! stays fixed as the cluster grows, and cluster membership is decoupled from voting.
//!
//! The caller (the live driver, step 4f) computes the eligible set from SWIM, reads
//! [`raft_view`], calls [`decide`](MembershipReconciler::decide), and applies the
//! result — **debounced** so a flapping member does not churn the voter set. Keeping
//! `decide` pure makes the policy exhaustively unit-testable without a cluster.

use crate::lease_group::LeaseRaft;
use crate::lease_raft::RaftNodeId;
use openraft::{BasicNode, ChangeMembers, ServerState};
use std::collections::{BTreeMap, BTreeSet};

/// What to do to bring the lease group toward the desired membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipAction {
    /// Nothing to do.
    None,
    /// This node should bootstrap the group with `voters` (just itself).
    Initialize(BTreeSet<RaftNodeId>),
    /// This node (the leader) should drive the group toward `target_voters` (the
    /// bounded, sticky voter set, ADR 0021), first adding `add_as_learner` (eligible
    /// members not yet known to the group) as learners so the committed lease log reaches
    /// them and any filling a voter vacancy can be promoted. A voter dropped from the set
    /// is **retained as a learner** (it keeps the log), not removed.
    Reconcile {
        /// The bounded target voter set (≤ `N`).
        target_voters: BTreeSet<RaftNodeId>,
        /// Eligible members not yet in the group — added as learners first.
        add_as_learner: BTreeSet<RaftNodeId>,
    },
    /// This node (the leader) should remove `departed` members — nodes still in the group
    /// as learners that have left the cluster — from membership entirely.
    Drop(BTreeSet<RaftNodeId>),
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
    /// All nodes currently in the group — voters **and** learners. Lets the policy tell a
    /// new eligible member (add as learner) from a departed one (drop), since under ADR
    /// 0021 most members are non-voting learners.
    pub nodes: BTreeSet<RaftNodeId>,
}

/// A node's **failure domain** — an operator-supplied rack / zone / availability-zone label
/// (ADR 0016 T4). Voter selection spreads the bounded set across distinct domains so losing a
/// whole domain cannot, on its own, take quorum. A node with no label is treated as its **own
/// singleton domain**, so an entirely unlabelled cluster behaves exactly as the id-ordered
/// vacancy-fill did before domains existed.
pub type FailureDomain = String;

/// How many members of `set` already share `id`'s failure domain. An unlabelled node is its own
/// singleton domain (load 0), which is what makes the unlabelled case fall straight back to the
/// lowest-id ordering.
fn domain_load(
    set: &BTreeSet<RaftNodeId>,
    id: RaftNodeId,
    domains: &BTreeMap<RaftNodeId, FailureDomain>,
) -> usize {
    match domains.get(&id) {
        None => 0,
        Some(d) => set.iter().filter(|m| domains.get(m) == Some(d)).count(),
    }
}

/// Greedily grow `seed` up to `cap` by repeatedly taking the pool candidate whose failure
/// domain is **least represented** in the set so far, breaking ties by lowest id. With no
/// domain labels every load is 0, so this reduces exactly to "take the lowest-id candidates".
fn pick_balanced(
    cap: usize,
    pool: &BTreeSet<RaftNodeId>,
    seed: &BTreeSet<RaftNodeId>,
    domains: &BTreeMap<RaftNodeId, FailureDomain>,
) -> BTreeSet<RaftNodeId> {
    let mut result: BTreeSet<RaftNodeId> = seed.clone();
    let mut remaining: BTreeSet<RaftNodeId> = pool.difference(&result).copied().collect();
    while result.len() < cap {
        let Some(next) = remaining
            .iter()
            .copied()
            .min_by_key(|id| (domain_load(&result, *id, domains), *id))
        else {
            break; // pool exhausted before the cap was reached
        };
        result.insert(next);
        remaining.remove(&next);
    }
    result
}

/// The bounded, sticky target voter set (ADR 0021 §1), spread across failure domains (ADR 0016
/// T4): keep every still-eligible current voter, then fill vacancies up to `cap` preferring the
/// **least-represented failure domain** (lowest-id tie-break) so one domain's loss never costs
/// quorum; on the upgrade path from an all-voters cluster, shrink an over-large live voter set
/// to `cap` the same domain-balanced way. A deterministic function of *(cap, eligible, current
/// voters, domains)* — every node and every successive leader computes the same target, so
/// reconcilers do not disagree (they must therefore see the same `domains`). With no domain
/// labels it is identical to the prior lowest-id behaviour. Effective voters =
/// `min(cap, eligible.len())`.
#[must_use]
fn target_voters(
    cap: usize,
    eligible: &BTreeSet<RaftNodeId>,
    current: &BTreeSet<RaftNodeId>,
    domains: &BTreeMap<RaftNodeId, FailureDomain>,
) -> BTreeSet<RaftNodeId> {
    let cap = cap.max(1);
    // Still-eligible current voters are sticky — they never lose a seat to a join.
    let live: BTreeSet<RaftNodeId> = current.intersection(eligible).copied().collect();
    if live.len() >= cap {
        // Shrink (e.g. a pre-0021 all-voters cluster adopting the cap): keep a domain-balanced
        // `cap` of the live voters; the rest become learners via `change_membership(retain)`.
        return pick_balanced(cap, &live, &BTreeSet::new(), domains);
    }
    // Grow / vacancy-fill: keep all sticky live voters, then fill from the eligible non-voters
    // choosing domains under-represented among the voters already chosen.
    let candidates: BTreeSet<RaftNodeId> = eligible.difference(&live).copied().collect();
    pick_balanced(cap, &candidates, &live, domains)
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
    voter_cap: usize,
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
    ///
    /// `voter_cap` is the bounded voter-set size `N` (ADR 0021): at most `N` members vote;
    /// every other eligible member joins as a non-voting learner. Clamped to `≥ 1`.
    #[must_use]
    pub fn new(local: RaftNodeId, can_bootstrap: bool, voter_cap: usize) -> Self {
        Self {
            local,
            can_bootstrap,
            voter_cap: voter_cap.max(1),
        }
    }

    /// Decide the action to take given the current `view` and the `eligible` member set
    /// (alive, admitted members mapped to [`RaftNodeId`]). Pure — see the module docs and
    /// [ADR 0021](../../../docs/adr/0021-bounded-lease-voters.md) for the policy.
    ///
    /// Voter selection is failure-domain-unaware here (every node its own singleton domain);
    /// use [`decide_with_domains`](Self::decide_with_domains) to spread the voter set across
    /// failure domains (ADR 0016 T4).
    #[must_use]
    pub fn decide(&self, view: &RaftView, eligible: &BTreeSet<RaftNodeId>) -> MembershipAction {
        self.decide_with_domains(view, eligible, &BTreeMap::new())
    }

    /// Like [`decide`](Self::decide), but spreads the bounded voter set across failure domains
    /// (ADR 0016 T4): `domains` maps each eligible member to its rack/zone label, so vacancy
    /// fill (and the upgrade-path shrink) prefer under-represented domains. `domains` must be
    /// consistent across nodes — it is derived from the same replicated membership view — so
    /// every successive leader computes the same target and reconcilers do not disagree.
    #[must_use]
    pub fn decide_with_domains(
        &self,
        view: &RaftView,
        eligible: &BTreeSet<RaftNodeId>,
        domains: &BTreeMap<RaftNodeId, FailureDomain>,
    ) -> MembershipAction {
        if eligible.is_empty() {
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
        // Initialized: only the leader reconciles membership.
        if !view.is_leader {
            return MembershipAction::None;
        }
        // A membership change is still settling (joint consensus): wait for it rather
        // than re-proposing, which openraft rejects as already in progress and which —
        // re-fired every driver tick under churn — amplifies the churn (ADR 0026 §2).
        if view.changing {
            return MembershipAction::None;
        }
        // Reshape the voter set first: bring every eligible member into the group (as a
        // learner) and drive the voters toward the bounded sticky target.
        let target_voters = target_voters(self.voter_cap, eligible, &view.voters, domains);
        let add_as_learner: BTreeSet<RaftNodeId> =
            eligible.difference(&view.nodes).copied().collect();
        if target_voters != view.voters || !add_as_learner.is_empty() {
            return MembershipAction::Reconcile {
                target_voters,
                add_as_learner,
            };
        }
        // Voters are at target and every eligible member is in the group. Drop any member
        // that has left the cluster (now lingering as a learner) from membership entirely.
        let departed: BTreeSet<RaftNodeId> = view.nodes.difference(eligible).copied().collect();
        if !departed.is_empty() {
            return MembershipAction::Drop(departed);
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
    let nodes: BTreeSet<RaftNodeId> = membership.nodes().map(|(id, _)| *id).collect();
    RaftView {
        initialized: !voters.is_empty(),
        is_leader: metrics.state == ServerState::Leader,
        // A joint (transitional) config carries >1 config set; a settled uniform one carries
        // exactly one. More than one means a membership change is still in flight.
        changing: membership.get_joint_config().len() > 1,
        voters,
        nodes,
    }
}

/// Apply a [`MembershipAction`] to `raft`.
///
/// - `Initialize` bootstraps the group.
/// - `Reconcile` adds each new eligible member as a learner (blocking until it catches up
///   so the committed lease log reaches it) and then, **only if the voter set actually
///   differs**, replaces the voters with `target_voters` using `retain = true` — so any
///   voter dropped from the set becomes a *learner* (it keeps the log, ADR 0021 §3), not
///   removed. A pure learner addition (target unchanged) thus issues no voter change.
/// - `Drop` removes departed members (now learners) from the group entirely.
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
        MembershipAction::Reconcile {
            target_voters,
            add_as_learner,
        } => {
            for id in add_as_learner {
                raft.add_learner(*id, BasicNode::default(), true)
                    .await
                    .map_err(|e| MembershipError::Raft(e.to_string()))?;
            }
            // Skip a no-op voter change (a pure learner join leaves the voter set at
            // target): openraft would otherwise propose a redundant joint config.
            let current: BTreeSet<RaftNodeId> = raft
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .voter_ids()
                .collect();
            if &current != target_voters {
                // retain = true: a voter dropped from the set is kept as a learner.
                raft.change_membership(
                    ChangeMembers::ReplaceAllVoters(target_voters.clone()),
                    true,
                )
                .await
                .map_err(|e| MembershipError::Raft(e.to_string()))?;
            }
            Ok(())
        }
        MembershipAction::Drop(departed) => {
            // The departed members are learners here (a still-voting member would have
            // been reshaped out first), so RemoveNodes is safe.
            raft.change_membership(ChangeMembers::RemoveNodes(departed.clone()), false)
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

    /// A fresh, uninitialized raft view (no group yet).
    fn uninit() -> RaftView {
        RaftView {
            initialized: false,
            is_leader: false,
            changing: false,
            voters: set(&[]),
            nodes: set(&[]),
        }
    }

    /// A leader view with the given voter set and full node set (voters + learners).
    fn leader(voters: &[RaftNodeId], nodes: &[RaftNodeId]) -> RaftView {
        RaftView {
            initialized: true,
            is_leader: true,
            changing: false,
            voters: set(voters),
            nodes: set(nodes),
        }
    }

    // ---- pure policy (ADR 0021) ----

    #[test]
    fn empty_eligible_is_a_noop() {
        assert_eq!(
            MembershipReconciler::new(1, true, 5).decide(&uninit(), &set(&[])),
            MembershipAction::None,
        );
    }

    #[test]
    fn any_founder_bootstraps_with_itself_regardless_of_id_rank() {
        // The founder bootstraps with itself whether or not it is the smallest id —
        // `can_bootstrap` is the sole guard (ADR 0026 T7). Node 1 (the min):
        assert_eq!(
            MembershipReconciler::new(1, true, 5).decide(&uninit(), &set(&[1, 2, 3])),
            MembershipAction::Initialize(set(&[1])),
        );
        // ...and node 2 (NOT the min) — the case the old min-id tiebreak wrongly blocked,
        // leaving the durable group unformed.
        assert_eq!(
            MembershipReconciler::new(2, true, 5).decide(&uninit(), &set(&[1, 2, 3])),
            MembershipAction::Initialize(set(&[2])),
        );
    }

    #[test]
    fn a_non_founder_never_bootstraps() {
        // Not a founder (started with seeds) — it waits to be added rather than starting
        // a rival group, even though it is the smallest id.
        assert_eq!(
            MembershipReconciler::new(1, false, 5).decide(&uninit(), &set(&[1, 2, 3])),
            MembershipAction::None,
        );
    }

    #[test]
    fn only_the_leader_reconciles_membership() {
        let follower = RaftView {
            is_leader: false,
            ..leader(&[1], &[1])
        };
        assert_eq!(
            MembershipReconciler::new(2, true, 5).decide(&follower, &set(&[1, 2, 3])),
            MembershipAction::None,
        );
    }

    #[test]
    fn a_leader_does_not_re_propose_while_a_change_is_in_flight() {
        // Leader, voters {1} but eligible {1,2,3} — yet a change is already settling
        // (joint consensus). It must wait, not fire a second change (ADR 0026 §2).
        let changing = RaftView {
            changing: true,
            ..leader(&[1], &[1])
        };
        assert_eq!(
            MembershipReconciler::new(1, true, 5).decide(&changing, &set(&[1, 2, 3])),
            MembershipAction::None,
        );
        // Once it settles, the same gap is reconciled (grow toward the cap).
        assert_eq!(
            MembershipReconciler::new(1, true, 5).decide(&leader(&[1], &[1]), &set(&[1, 2, 3])),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2, 3]),
                add_as_learner: set(&[2, 3]),
            },
        );
    }

    #[test]
    fn more_than_n_members_yield_exactly_n_voters() {
        // cap 3, all five already learners; the founder is the sole voter. Fill the two
        // vacancies with the lowest-id learners → exactly 3 voters, no learners to add.
        let r = MembershipReconciler::new(1, true, 3);
        assert_eq!(
            r.decide(&leader(&[1], &[1, 2, 3, 4, 5]), &set(&[1, 2, 3, 4, 5])),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2, 3]),
                add_as_learner: set(&[]),
            },
        );
    }

    #[test]
    fn a_high_id_join_becomes_a_learner_without_changing_voters() {
        // cap 3, voters already full at {1,2,3}; nodes 4,5 join. They join as learners and
        // the voter set is unchanged (target == voters), so apply issues no voter change.
        let r = MembershipReconciler::new(1, true, 3);
        assert_eq!(
            r.decide(&leader(&[1, 2, 3], &[1, 2, 3]), &set(&[1, 2, 3, 4, 5])),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2, 3]),
                add_as_learner: set(&[4, 5]),
            },
        );
    }

    #[test]
    fn a_live_voter_is_never_demoted_just_because_a_node_joins() {
        // Sticky (ADR 0021 §1): voters {1,2,3} at the cap, node 4 joins. 4 becomes a
        // learner; no live voter is displaced.
        let r = MembershipReconciler::new(1, true, 3);
        assert_eq!(
            r.decide(&leader(&[1, 2, 3], &[1, 2, 3]), &set(&[1, 2, 3, 4])),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2, 3]),
                add_as_learner: set(&[4]),
            },
        );
    }

    #[test]
    fn a_dead_voter_is_replaced_by_the_lowest_id_learner() {
        // cap 3, voters {1,2,3}, learners {4,5}. Voter 2 dies (drops out of eligible).
        let r = MembershipReconciler::new(1, true, 3);
        // Step 1: 2 is reshaped out of the voter set and the lowest-id learner (4) fills
        // the vacancy — voter count restored to 3. (retain keeps 2 as a learner for now.)
        assert_eq!(
            r.decide(
                &leader(&[1, 2, 3], &[1, 2, 3, 4, 5]),
                &set(&[1, 3, 4, 5]), // 2 gone
            ),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 3, 4]),
                add_as_learner: set(&[]),
            },
        );
        // Step 2: voters now {1,3,4}; the dead node 2 lingers as a learner and is dropped.
        assert_eq!(
            r.decide(&leader(&[1, 3, 4], &[1, 2, 3, 4, 5]), &set(&[1, 3, 4, 5]),),
            MembershipAction::Drop(set(&[2])),
        );
    }

    #[test]
    fn an_all_voters_cluster_shrinks_to_the_cap() {
        // Upgrade path (ADR 0021 §3): a pre-0021 cluster has every member voting. With
        // cap 3, keep the lowest-id 3 as voters; the rest are demoted to learners
        // (apply uses retain = true).
        let r = MembershipReconciler::new(1, true, 3);
        assert_eq!(
            r.decide(
                &leader(&[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5]),
                &set(&[1, 2, 3, 4, 5]),
            ),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2, 3]),
                add_as_learner: set(&[]),
            },
        );
    }

    #[test]
    fn n_larger_than_the_cluster_makes_every_member_a_voter() {
        // cap 5 but only 3 members: all three vote (effective voters = min(N, cluster)).
        let r = MembershipReconciler::new(1, true, 5);
        assert_eq!(
            r.decide(&leader(&[1], &[1, 2, 3]), &set(&[1, 2, 3])),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2, 3]),
                add_as_learner: set(&[]),
            },
        );
    }

    #[test]
    fn n_equals_one_keeps_a_single_voter() {
        // cap 1: exactly one voter; every other member is a learner.
        let r = MembershipReconciler::new(1, true, 1);
        // Growth: members 2,3 join as learners, the voter set stays {1}.
        assert_eq!(
            r.decide(&leader(&[1], &[1]), &set(&[1, 2, 3])),
            MembershipAction::Reconcile {
                target_voters: set(&[1]),
                add_as_learner: set(&[2, 3]),
            },
        );
        // Steady state: one voter, the rest learners, nothing to do.
        assert_eq!(
            r.decide(&leader(&[1], &[1, 2, 3]), &set(&[1, 2, 3])),
            MembershipAction::None,
        );
    }

    #[test]
    fn a_zero_cap_is_clamped_to_a_single_voter() {
        // A degenerate `N = 0` must not yield a zero-voter (un-electable) group: it is
        // clamped to a single voter, the rest joining as learners.
        let r = MembershipReconciler::new(1, true, 0);
        assert_eq!(
            r.decide(&leader(&[1], &[1]), &set(&[1, 2, 3])),
            MembershipAction::Reconcile {
                target_voters: set(&[1]),
                add_as_learner: set(&[2, 3]),
            },
        );
    }

    #[test]
    fn a_departed_learner_is_dropped() {
        // cap 3, voters {1,2,3}, learner {4}. Node 4 leaves the cluster → dropped entirely.
        let r = MembershipReconciler::new(1, true, 3);
        assert_eq!(
            r.decide(&leader(&[1, 2, 3], &[1, 2, 3, 4]), &set(&[1, 2, 3])),
            MembershipAction::Drop(set(&[4])),
        );
    }

    #[test]
    fn a_steady_bounded_group_is_a_noop() {
        // cap 3: voters at target, every eligible member present, none departed → None.
        let r = MembershipReconciler::new(1, true, 3);
        assert_eq!(
            r.decide(
                &leader(&[1, 2, 3], &[1, 2, 3, 4, 5]),
                &set(&[1, 2, 3, 4, 5])
            ),
            MembershipAction::None,
        );
    }

    // ---- failure-domain-aware voter selection (ADR 0016 T4) ----

    /// Build a `RaftNodeId -> failure domain` map from `(id, domain)` pairs.
    fn dom(pairs: &[(RaftNodeId, &str)]) -> std::collections::BTreeMap<RaftNodeId, String> {
        pairs
            .iter()
            .map(|(id, d)| (*id, (*d).to_string()))
            .collect()
    }

    #[test]
    fn no_domains_matches_the_lowest_id_fill() {
        // An empty domain map must reproduce the prior id-ordered behaviour exactly — the
        // domain-aware path is a strict superset, so existing clusters are unaffected.
        let r = MembershipReconciler::new(1, true, 3);
        let eligible = set(&[1, 2, 3, 4, 5]);
        let view = leader(&[1], &[1, 2, 3, 4, 5]);
        assert_eq!(
            r.decide_with_domains(&view, &eligible, &dom(&[])),
            r.decide(&view, &eligible),
        );
    }

    #[test]
    fn vacancy_fill_spreads_across_failure_domains() {
        // cap 3, founder voter {1} in zone a; learners 2 (a), 3 (b), 4 (c). The id-ordered
        // fill would pick 2,3 → zones {a,a,b}; the domain-aware fill picks 3,4 → {a,b,c}, so no
        // single zone holds a quorum.
        let r = MembershipReconciler::new(1, true, 3);
        let domains = dom(&[(1, "a"), (2, "a"), (3, "b"), (4, "c")]);
        assert_eq!(
            r.decide_with_domains(&leader(&[1], &[1, 2, 3, 4]), &set(&[1, 2, 3, 4]), &domains,),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 3, 4]),
                add_as_learner: set(&[]),
            },
        );
        // Contrast: without the labels the same view fills with the lowest ids (zones ignored).
        assert_eq!(
            r.decide(&leader(&[1], &[1, 2, 3, 4]), &set(&[1, 2, 3, 4])),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2, 3]),
                add_as_learner: set(&[]),
            },
        );
    }

    #[test]
    fn within_a_domain_ties_break_by_lowest_id() {
        // cap 2, voter {1} in zone a; learners 2 and 3 both in zone b. The vacancy prefers
        // zone b (under-represented), and ties within it go to the lowest id (2).
        let r = MembershipReconciler::new(1, true, 2);
        assert_eq!(
            r.decide_with_domains(
                &leader(&[1], &[1, 2, 3]),
                &set(&[1, 2, 3]),
                &dom(&[(1, "a"), (2, "b"), (3, "b")]),
            ),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2]),
                add_as_learner: set(&[]),
            },
        );
    }

    #[test]
    fn the_upgrade_shrink_is_also_domain_balanced() {
        // Upgrade path: an all-voters {1(a),2(a),3(b),4(c)} cluster adopts cap 3. The id-ordered
        // shrink keeps {1,2,3} (zones a,a,b); the domain-aware shrink keeps {1,3,4} (a,b,c).
        let r = MembershipReconciler::new(1, true, 3);
        assert_eq!(
            r.decide_with_domains(
                &leader(&[1, 2, 3, 4], &[1, 2, 3, 4]),
                &set(&[1, 2, 3, 4]),
                &dom(&[(1, "a"), (2, "a"), (3, "b"), (4, "c")]),
            ),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 3, 4]),
                add_as_learner: set(&[]),
            },
        );
    }

    #[test]
    fn a_live_voter_stays_even_if_its_domain_is_over_represented() {
        // Stickiness (ADR 0021 §1) wins over spread: voters {1,2} are both zone a and at cap 2;
        // a new zone-b learner 3 does NOT displace a live voter — it joins as a learner.
        let r = MembershipReconciler::new(1, true, 2);
        assert_eq!(
            r.decide_with_domains(
                &leader(&[1, 2], &[1, 2]),
                &set(&[1, 2, 3]),
                &dom(&[(1, "a"), (2, "a"), (3, "b")]),
            ),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 2]),
                add_as_learner: set(&[3]),
            },
        );
    }

    #[test]
    fn a_dead_voter_is_replaced_preferring_a_fresh_domain() {
        // cap 3, voters {1(a),2(a),3(b)}, learners {4(c),5(a)}. Voter 2 dies. The vacancy is
        // filled by zone c (4) over the lower-id zone-a learner 5 — restoring domain spread
        // rather than clustering two voters in zone a again.
        let r = MembershipReconciler::new(1, true, 3);
        assert_eq!(
            r.decide_with_domains(
                &leader(&[1, 2, 3], &[1, 2, 3, 4, 5]),
                &set(&[1, 3, 4, 5]), // 2 gone
                &dom(&[(1, "a"), (2, "a"), (3, "b"), (4, "c"), (5, "a")]),
            ),
            MembershipAction::Reconcile {
                target_voters: set(&[1, 3, 4]),
                add_as_learner: set(&[]),
            },
        );
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
            Arc::new(std::sync::RwLock::new(crate::placement::Placement::new(
                n(id),
                crate::placement::DEFAULT_REPLICAS,
            ))),
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
        let recon = MembershipReconciler::new(boot_id, true, 5);

        // Step 1: bootstrap (Initialize with self), then it wins the election.
        let action = recon.decide(&raft_view(boot.raft()), &desired);
        assert!(matches!(action, MembershipAction::Initialize(_)));
        apply_action(boot.raft(), &action).await.unwrap();
        boot.raft()
            .wait(Some(Duration::from_secs(15)))
            .state(ServerState::Leader, "bootstrap node leads")
            .await
            .unwrap();

        // Step 2: as leader, grow membership to the full desired set (≤ cap, so both vote).
        let action = recon.decide(&raft_view(boot.raft()), &desired);
        assert!(matches!(action, MembershipAction::Reconcile { .. }));
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
