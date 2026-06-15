//! Ownership leases and epoch fencing — the split-brain-safe core of
//! [ADR 0006](../../../docs/adr/0006-consensus-and-replication.md).
//!
//! ADR 0006 scopes consensus to *ownership leases*: a consensus engine (openraft —
//! ratified by the workstream-E spike) establishes, per placement group, **which
//! node holds the lease and at what epoch**, and the lease-holder then does
//! epoch-fenced quorum replication of the per-session append-log. This module is
//! the thin, **engine-agnostic** layer the broker writes *on top of* that engine:
//! the fencing rule that makes a superseded lease-holder unable to diverge the log.
//! It is pure and sans-I/O — the same testability discipline as the SWIM state
//! machine and the placement ring — so the safety property is pinned by tests, not
//! entangled with the engine or the network.
//!
//! `Epoch` maps onto the engine's monotonic leadership *term*; we do not invent our
//! own election. What we own is the fence: an append carries the lease-holder's
//! epoch, and a replica accepts it only if it is not stale.
//!
//! ## The safety property
//!
//! **Two distinct epochs can never both achieve quorum-durable replication.** Each
//! replica's acknowledged epoch is *monotonic* (it never moves backward), and any
//! two quorums of a replica set intersect in at least one replica. So once a quorum
//! has acknowledged epoch `E'`, the intersecting replica in every other quorum
//! rejects any older epoch `E < E'` — a stale lease-holder (e.g. one that lost its
//! lease during a partition and has not noticed) cannot reach quorum and therefore
//! cannot append. That is exactly the [`ReplError::NoQuorum`] /
//! [`ReplError::NotOwner`] fencing the cluster `ReplicatedLog` backend (workstream
//! E step 3) relies on.
//!
//! [`ReplError::NoQuorum`]: ../../mqtt_storage/repl/enum.ReplError.html
//! [`ReplError::NotOwner`]: ../../mqtt_storage/repl/enum.ReplError.html

use crate::NodeId;
use std::collections::BTreeMap;

/// A monotonic fence token identifying a leadership term, supplied by the
/// consensus engine. Higher epochs supersede lower ones.
pub type Epoch = u64;

/// The ownership lease for one placement group, as published by consensus: the
/// node that may write the group's session logs, and the epoch that authorizes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipLease {
    /// The node currently authorized to write (the lease-holder).
    pub holder: NodeId,
    /// The leadership epoch the lease was granted at; carried on every append so
    /// replicas can fence a superseded holder.
    pub epoch: Epoch,
}

/// Why a lease grant or an append could not be made durable.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LeaseError {
    /// Fewer than a quorum of replicas accepted at this epoch — either an election
    /// that did not win, or a **fenced** (stale-epoch) append. The caller must not
    /// treat the write as durable, and must not release a QoS≥1 PUBACK.
    #[error("no quorum at epoch {epoch}: {accepted} of {needed} replicas accepted")]
    NoQuorum {
        /// The epoch the operation was attempted at.
        epoch: Epoch,
        /// How many replicas accepted.
        accepted: usize,
        /// How many were needed (the quorum).
        needed: usize,
    },
}

/// A placement group's replica set and their fence state.
///
/// Tracks, per replica, the highest epoch it has acknowledged. The two operations
/// model the only epoch-sensitive steps the broker performs on top of the engine:
/// granting a lease (election) and replicating an append (the durability write).
#[derive(Debug, Clone)]
pub struct LeaseGroup {
    /// node -> highest epoch acknowledged (monotonic).
    fences: BTreeMap<NodeId, Epoch>,
    quorum: usize,
}

impl LeaseGroup {
    /// A group over `replicas` with a majority quorum (`R/2 + 1`).
    ///
    /// # Panics
    /// Panics if `replicas` is empty — a placement group always has at least its
    /// owner.
    #[must_use]
    pub fn new(replicas: impl IntoIterator<Item = NodeId>) -> Self {
        let fences: BTreeMap<NodeId, Epoch> = replicas.into_iter().map(|n| (n, 0)).collect();
        assert!(
            !fences.is_empty(),
            "a lease group needs at least one replica"
        );
        let quorum = fences.len() / 2 + 1;
        Self { fences, quorum }
    }

    /// The number of replicas in the group.
    #[must_use]
    pub fn replica_count(&self) -> usize {
        self.fences.len()
    }

    /// The quorum size (majority of the replica set).
    #[must_use]
    pub fn quorum(&self) -> usize {
        self.quorum
    }

    /// The highest epoch `node` has acknowledged (0 if not a member).
    #[must_use]
    pub fn fence_of(&self, node: &NodeId) -> Epoch {
        self.fences.get(node).copied().unwrap_or(0)
    }

    /// Attempt to grant `candidate` the lease at `epoch` (an election).
    ///
    /// A voter grants iff `epoch` is **strictly greater** than its acknowledged
    /// epoch — so each epoch has at most one leader (you cannot re-win an epoch a
    /// quorum already moved to). On a grant the voter advances its fence to
    /// `epoch`. If a quorum of `voters` grant, the lease is established; otherwise
    /// the grant is fenced ([`LeaseError::NoQuorum`]) and no lease exists at
    /// `epoch`. Callers retry at a strictly higher epoch (the engine's term bump).
    ///
    /// # Errors
    /// [`LeaseError::NoQuorum`] if fewer than a quorum of voters grant.
    pub fn grant(
        &mut self,
        candidate: &NodeId,
        epoch: Epoch,
        voters: &[NodeId],
    ) -> Result<OwnershipLease, LeaseError> {
        let mut accepted = 0;
        for v in voters {
            if let Some(f) = self.fences.get_mut(v) {
                // Strictly greater: a genuinely new term, not a re-grant.
                if epoch > *f {
                    *f = epoch;
                    accepted += 1;
                }
            }
        }
        if accepted >= self.quorum {
            Ok(OwnershipLease {
                holder: candidate.clone(),
                epoch,
            })
        } else {
            Err(LeaseError::NoQuorum {
                epoch,
                accepted,
                needed: self.quorum,
            })
        }
    }

    /// Replicate a lease-holder's append at `lease.epoch` across the group.
    ///
    /// Each replica accepts iff the epoch is **not stale** (`>= ` its acknowledged
    /// epoch), advancing its fence; a replica that has moved to a newer epoch
    /// rejects. The append is durable iff a quorum accept. A superseded
    /// lease-holder is thereby fenced: the quorum that elected its successor rejects
    /// it, so it cannot reach quorum and cannot diverge the log.
    ///
    /// Returns the assigned... nothing here — this models only the *fencing*
    /// decision; the byte log and offsets live in `mqtt-storage::repl`. `Ok`
    /// means "quorum-durable at this epoch", which gates the QoS≥1 PUBACK.
    ///
    /// # Errors
    /// [`LeaseError::NoQuorum`] if fewer than a quorum accept (the holder is fenced).
    pub fn replicate(&mut self, lease: &OwnershipLease) -> Result<(), LeaseError> {
        let epoch = lease.epoch;
        // Snapshot whether each replica *would* accept before mutating, so a
        // minority of stale-accepts does not advance fences when the append is not
        // durable. (Quorum-or-nothing: an append that cannot commit leaves no trace.)
        let accepted = self.fences.values().filter(|f| epoch >= **f).count();
        if accepted >= self.quorum {
            for f in self.fences.values_mut() {
                if epoch >= *f {
                    *f = epoch;
                }
            }
            Ok(())
        } else {
            Err(LeaseError::NoQuorum {
                epoch,
                accepted,
                needed: self.quorum,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Epoch, LeaseError, LeaseGroup, OwnershipLease};
    use crate::NodeId;

    fn n(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    fn group3() -> (LeaseGroup, Vec<NodeId>) {
        let nodes = vec![n("a"), n("b"), n("c")];
        (LeaseGroup::new(nodes.clone()), nodes)
    }

    #[test]
    fn quorum_is_a_majority() {
        assert_eq!(LeaseGroup::new([n("a")]).quorum(), 1);
        assert_eq!(LeaseGroup::new([n("a"), n("b"), n("c")]).quorum(), 2);
        assert_eq!(
            LeaseGroup::new([n("a"), n("b"), n("c"), n("d"), n("e")]).quorum(),
            3
        );
    }

    #[test]
    fn holder_replicates_repeatedly_at_its_epoch() {
        let (mut g, all) = group3();
        let lease = g.grant(&n("a"), 1, &all).unwrap();
        assert_eq!(
            lease,
            OwnershipLease {
                holder: n("a"),
                epoch: 1
            }
        );
        // The established holder appends many times at the same epoch.
        for _ in 0..5 {
            assert!(g.replicate(&lease).is_ok());
        }
    }

    /// The headline property: after a new lease is granted at a higher epoch, the
    /// previous holder is fenced and can no longer reach quorum.
    #[test]
    fn superseded_holder_is_fenced() {
        let (mut g, all) = group3();
        let old = g.grant(&n("a"), 1, &all).unwrap();
        assert!(g.replicate(&old).is_ok(), "a leads at epoch 1");

        // b wins epoch 2 with a quorum {b, c} (a is partitioned away).
        let new = g.grant(&n("b"), 2, &[n("b"), n("c")]).unwrap();
        assert_eq!(new.epoch, 2);

        // The stale holder a (still at epoch 1) cannot commit: {b, c} reject it.
        assert_eq!(
            g.replicate(&old),
            Err(LeaseError::NoQuorum {
                epoch: 1,
                accepted: 1,
                needed: 2
            }),
        );
        // The new holder commits.
        assert!(g.replicate(&new).is_ok());
    }

    /// You cannot win an epoch a quorum has already moved to (one leader per epoch).
    #[test]
    fn cannot_regrant_a_known_epoch() {
        let (mut g, all) = group3();
        g.grant(&n("a"), 5, &all).unwrap();
        // Every replica is now at epoch 5; a grant at 5 finds no voter to advance.
        assert_eq!(
            g.grant(&n("b"), 5, &all),
            Err(LeaseError::NoQuorum {
                epoch: 5,
                accepted: 0,
                needed: 2
            }),
        );
        // A strictly higher epoch wins.
        assert!(g.grant(&n("b"), 6, &all).is_ok());
    }

    /// A minority partition cannot grant a lease, so no split-brain second leader.
    #[test]
    fn minority_cannot_grant() {
        let (mut g, _all) = group3();
        assert!(matches!(
            g.grant(&n("a"), 1, &[n("a")]),
            Err(LeaseError::NoQuorum {
                needed: 2,
                accepted: 1,
                ..
            })
        ));
    }

    /// A failed (minority) append leaves no fence advanced — quorum-or-nothing, so a
    /// fenced write never partially mutates group state.
    #[test]
    fn fenced_append_does_not_advance_fences() {
        let (mut g, all) = group3();
        let new = g.grant(&n("b"), 2, &all).unwrap(); // everyone at epoch 2
        let stale = OwnershipLease {
            holder: n("a"),
            epoch: 1,
        };
        assert!(g.replicate(&stale).is_err());
        // No replica regressed to epoch 1.
        for node in &all {
            assert_eq!(g.fence_of(node), 2);
        }
        assert!(g.replicate(&new).is_ok());
    }

    /// Fences are monotonic across an interleaving of grants and replicates: the
    /// highest acknowledged epoch never decreases for any replica.
    #[test]
    fn fences_are_monotonic() {
        let (mut g, all) = group3();
        let mut prev: Vec<Epoch> = all.iter().map(|x| g.fence_of(x)).collect();
        let ops: &[Epoch] = &[1, 3, 2, 3, 5, 4];
        for &e in ops {
            // Attempt a grant at e then an append; ignore fencing errors.
            let _ = g.grant(&n("a"), e, &all);
            let _ = g.replicate(&OwnershipLease {
                holder: n("a"),
                epoch: e,
            });
            let now: Vec<Epoch> = all.iter().map(|x| g.fence_of(x)).collect();
            for (before, after) in prev.iter().zip(&now) {
                assert!(after >= before, "fence regressed: {before} -> {after}");
            }
            prev = now;
        }
    }

    /// Two overlapping quorums cannot both commit different epochs — the
    /// intersection replica fences the older one.
    #[test]
    fn overlapping_quorums_cannot_both_commit() {
        let (mut g, _all) = group3();
        // Quorum {a,b} elects a@1.
        let a1 = g.grant(&n("a"), 1, &[n("a"), n("b")]).unwrap();
        assert!(g.replicate(&a1).is_ok());
        // Quorum {b,c} elects c@2 — b is the intersection and moves to epoch 2.
        let c2 = g.grant(&n("c"), 2, &[n("b"), n("c")]).unwrap();
        // a@1 now reaches only a (c and b are at epoch 2): below quorum.
        assert!(g.replicate(&a1).is_err());
        assert!(g.replicate(&c2).is_ok());
    }
}
