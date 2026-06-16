//! Leader-driven lease assignment
//! ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md) §3, workstream
//! E step 4f).
//!
//! A group's owner is chosen by HRW [`Placement`], which is generally **not** the
//! lease-group raft leader — and openraft's `client_write` does not forward from a
//! follower. So leases are assigned by the **leader**: a reconcile keeps every
//! group's committed lease pointed at the group's current placement owner, issuing
//! `Assign { group, owner }` when they differ (the lease state machine mints a fresh
//! monotonic epoch). The assignment replicates to every node, and each owner reads
//! its epoch from its own [`LeaseStore`](crate::lease_store::LeaseStore)
//! ([`LocalLeaseSource`](crate::cluster_store::LocalLeaseSource)) — no write to
//! forward.
//!
//! [`pending`](LeaseAssigner::pending) is the pure decision (which groups differ);
//! [`reconcile`](LeaseAssigner::reconcile) applies them, but only when this node is
//! the leader. The live driver (the node assembly, next) calls `reconcile` on a tick
//! and on membership/leadership change.

use crate::lease_group::LeaseRaft;
use crate::lease_membership::raft_view;
use crate::lease_raft::{GroupId, LeaseRequest, RaftNodeId};
use crate::lease_store::LeaseStore;
use crate::node_registry::raft_id;
use crate::placement::{Placement, NUM_GROUPS};
use std::sync::{Arc, RwLock};

/// Errors from applying lease assignments.
#[derive(Debug, thiserror::Error)]
pub enum AssignError {
    /// A `client_write(Assign)` to the lease group failed.
    #[error("lease assignment failed: {0}")]
    Raft(String),
}

/// Keeps each group's lease assigned to its current placement owner (leader-driven).
#[derive(Debug, Clone)]
pub struct LeaseAssigner {
    placement: Arc<RwLock<Placement>>,
}

impl LeaseAssigner {
    /// An assigner resolving group owners from `placement`.
    #[must_use]
    pub fn new(placement: Arc<RwLock<Placement>>) -> Self {
        Self { placement }
    }

    /// The `(group, desired-holder)` pairs whose committed lease holder differs from
    /// the group's current placement owner — the assignments the leader should make.
    ///
    /// Pure given the placement ring and the lease map: in steady state (every lease
    /// already on its owner) this is empty, so `reconcile` is idempotent.
    #[must_use]
    pub fn pending(&self, store: &LeaseStore) -> Vec<(GroupId, RaftNodeId)> {
        let placement = self
            .placement
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (0..NUM_GROUPS)
            .filter_map(|group| {
                let desired = raft_id(&placement.group_owner(group));
                let current = store.current_lease(group).map(|rec| rec.holder);
                (current != Some(desired)).then_some((group, desired))
            })
            .collect()
    }

    /// As the lease-group leader, assign every pending group to its placement owner.
    /// Returns how many were assigned. A **no-op on a follower** (only the leader can
    /// `client_write`).
    ///
    /// # Errors
    /// [`AssignError::Raft`] if a lease assignment fails.
    pub async fn reconcile(
        &self,
        raft: &LeaseRaft,
        store: &LeaseStore,
    ) -> Result<usize, AssignError> {
        if !raft_view(raft).is_leader {
            return Ok(0);
        }
        let pending = self.pending(store);
        let count = pending.len();
        for (group, node) in pending {
            raft.client_write(LeaseRequest::Assign { group, node })
                .await
                .map_err(|e| AssignError::Raft(e.to_string()))?;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::LeaseAssigner;
    use crate::lease_group::{config, LeaseRaft};
    use crate::lease_store::LeaseStore;
    use crate::node_registry::raft_id;
    use crate::placement::{Placement, DEFAULT_REPLICAS, NUM_GROUPS};
    use crate::raft_mesh::MeshRaftNetwork;
    use crate::NodeId;
    use openraft::storage::Adaptor;
    use openraft::{BasicNode, Raft, ServerState};
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    fn nid(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// On a single-node cluster the leader assigns every group's lease to itself
    /// (the sole owner); reconcile is then idempotent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leader_assigns_every_group_to_its_owner() {
        let local_node = nid("assign-node");
        let local = raft_id(&local_node);
        let placement = Arc::new(RwLock::new(Placement::new(
            local_node.clone(),
            DEFAULT_REPLICAS,
        )));
        let store = LeaseStore::new();
        let (ls, sm) = Adaptor::new(store.clone());
        let raft: LeaseRaft = Raft::new(local, config(), MeshRaftNetwork::new(), ls, sm)
            .await
            .unwrap();

        let assigner = LeaseAssigner::new(placement.clone());

        // Before initialization this node is not the leader → reconcile is a no-op.
        assert_eq!(assigner.reconcile(&raft, &store).await.unwrap(), 0);

        raft.initialize(BTreeMap::from([(local, BasicNode::default())]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "leader")
            .await
            .unwrap();

        let total = usize::try_from(NUM_GROUPS).unwrap();
        // Every group is unassigned → all pending.
        assert_eq!(assigner.pending(&store).len(), total);

        // The leader assigns them all to itself (the sole owner).
        let made = assigner.reconcile(&raft, &store).await.unwrap();
        assert_eq!(made, total);

        // Idempotent: nothing left to assign.
        assert!(assigner.pending(&store).is_empty());
        assert_eq!(assigner.reconcile(&raft, &store).await.unwrap(), 0);

        // A sampling of groups is now held by this node.
        for group in [0, 1, NUM_GROUPS / 2, NUM_GROUPS - 1] {
            assert_eq!(store.current_lease(group).unwrap().holder, local);
        }

        raft.shutdown().await.unwrap();
    }
}
