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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// How long a fresh cluster's leader holds its first lease assignment waiting for
/// the replication-factor capability (ADR 0080 §2) before founding without one.
/// The capability is recomputed on the hub's one-second sweep, so on a founder it
/// arrives within a tick or two; the bound only matters when a member really
/// cannot apply the command.
pub const FOUNDING_WAIT: Duration = Duration::from_secs(10);

/// Founding a cluster's replication factor (ADR 0080 §2): the configured value,
/// and the flag saying every member can apply the command that records it.
#[derive(Debug, Clone)]
pub struct Founding {
    replicas: u8,
    capable: Arc<AtomicBool>,
    wait: Duration,
    held_since: Arc<Mutex<Option<Instant>>>,
}

impl Founding {
    /// Found new clusters at `replicas`, once `capable` is set.
    #[must_use]
    pub fn new(replicas: u8, capable: Arc<AtomicBool>) -> Self {
        Self::with_wait(replicas, capable, FOUNDING_WAIT)
    }

    /// [`new`](Self::new) with an explicit hold bound (tests).
    #[must_use]
    pub fn with_wait(replicas: u8, capable: Arc<AtomicBool>, wait: Duration) -> Self {
        Self {
            replicas,
            capable,
            wait,
            held_since: Arc::new(Mutex::new(None)),
        }
    }

    /// Whether the first assignment has been held for the whole bound.
    fn waited_out(&self) -> bool {
        let mut since = self
            .held_since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        since.get_or_insert_with(Instant::now).elapsed() >= self.wait
    }
}

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
    founding: Option<Founding>,
}

impl LeaseAssigner {
    /// An assigner resolving group owners from `placement`.
    #[must_use]
    pub fn new(placement: Arc<RwLock<Placement>>) -> Self {
        Self {
            placement,
            founding: None,
        }
    }

    /// Found a fresh cluster's replication factor before its first assignment
    /// (ADR 0080 §2). Without it a fresh cluster runs at the legacy 3.
    #[must_use]
    pub fn with_founding(mut self, founding: Founding) -> Self {
        self.founding = Some(founding);
        self
    }

    /// The replication factor this node would found a cluster at, if founding.
    #[must_use]
    pub fn founding_replicas(&self) -> Option<u8> {
        self.founding.as_ref().map(|f| f.replicas)
    }

    /// On a FRESH cluster — no lease ever minted, no factor recorded — commit the
    /// configured factor before anything is assigned, so no durable write is ever
    /// made under another one. Returns `false` while the first assignment must
    /// still wait for the capability.
    async fn found(&self, raft: &LeaseRaft, store: &LeaseStore) -> Result<bool, AssignError> {
        let Some(founding) = &self.founding else {
            return Ok(true);
        };
        if !store.replication().is_unset() || store.high_epoch() > 0 {
            return Ok(true); // founded, or a pre-0080 cluster that keeps 3
        }
        if founding.capable.load(Ordering::Relaxed) {
            raft.client_write(LeaseRequest::SetReplicas {
                r: founding.replicas,
            })
            .await
            .map_err(|e| AssignError::Raft(e.to_string()))?;
            tracing::info!(
                replicas = founding.replicas,
                "founded the cluster's replication factor (ADR 0080)"
            );
            return Ok(true);
        }
        if founding.waited_out() {
            tracing::warn!(
                wanted = founding.replicas,
                "founding WITHOUT a replication factor: a member cannot apply the command \
                 (an older build?). This cluster runs at the legacy 3 (ADR 0080)"
            );
            return Ok(true);
        }
        Ok(false)
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
                // Drive the lease toward the DESIRED HRW owner — never the committed
                // lease the data path now reads back via `group_owner`, or reconcile
                // would see desired == current forever and freeze (2026-07-20 post-mortem).
                let desired = raft_id(&placement.hrw_owner(group));
                let current = store.current_lease(group).map(|rec| rec.holder);
                (current != Some(desired)).then_some((group, desired))
            })
            .collect()
    }

    /// As the lease-group leader, assign every pending group to its placement owner
    /// in a **single** batched consensus write. Returns how many were assigned. A
    /// **no-op on a follower** (only the leader can `client_write`) and when nothing
    /// is pending (so reconcile stays idempotent — no empty entries appended).
    ///
    /// Batching matters at scale: a fresh leader sees every group unassigned, and a
    /// membership change moves many groups at once. One `AssignMany` entry replaces
    /// hundreds of single `Assign`s, so the lease log does not burst.
    ///
    /// # Errors
    /// [`AssignError::Raft`] if the assignment write fails.
    pub async fn reconcile(
        &self,
        raft: &LeaseRaft,
        store: &LeaseStore,
    ) -> Result<usize, AssignError> {
        if !raft_view(raft).is_leader {
            return Ok(0);
        }
        if !self.found(raft, store).await? {
            return Ok(0);
        }
        let assignments = self.pending(store);
        if assignments.is_empty() {
            return Ok(0);
        }
        let count = assignments.len();
        raft.client_write(LeaseRequest::AssignMany { assignments })
            .await
            .map_err(|e| AssignError::Raft(e.to_string()))?;
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

    /// `pending` targets the DESIRED HRW owner, never the committed lease the data path
    /// now reads back via `group_owner` — otherwise, once the data path follows the
    /// committed lease, the assigner would see desired == current for every group and
    /// freeze, never reconciling a membership change (2026-07-20 post-mortem).
    #[test]
    fn pending_targets_the_hrw_owner_not_the_committed_lease() {
        use crate::swim::MemberState;
        let local = nid("assign-node");
        let placement = Arc::new(RwLock::new(Placement::new(local.clone(), DEFAULT_REPLICAS)));
        {
            let mut p = placement.write().unwrap();
            p.observe(&nid("peer"), MemberState::Alive, "peer:7000", None);
            // Force EVERY group's committed owner to "peer" via the data-path overlay.
            let mut leases = BTreeMap::new();
            for g in 0..NUM_GROUPS {
                leases.insert(g, nid("peer"));
            }
            p.set_lease_owners(leases);
        }
        // An empty store → every group is pending; the desired holder must be the HRW
        // owner, not the overlaid "peer".
        let store = LeaseStore::new();
        let assigner = LeaseAssigner::new(placement.clone());
        let pending = assigner.pending(&store);
        assert_eq!(pending.len(), usize::try_from(NUM_GROUPS).unwrap());
        let p = placement.read().unwrap();
        for (g, desired) in &pending {
            assert_eq!(*desired, raft_id(&p.hrw_owner(*g)));
        }
        // Some groups HRW-hash to us, which the "everything is peer" overlay could never
        // produce — proof `pending` ignored the committed lease.
        assert!(
            pending.iter().any(|(_, d)| *d == raft_id(&local)),
            "the assigner must still target our HRW-owned groups, not the committed lease"
        );
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

    /// A single-node lease group, initialised and leading.
    async fn leading(name: &str) -> (LeaseRaft, LeaseStore, Arc<RwLock<Placement>>) {
        let node = nid(name);
        let local = raft_id(&node);
        let placement = Arc::new(RwLock::new(Placement::new(node, DEFAULT_REPLICAS)));
        let store = LeaseStore::new();
        let (ls, sm) = Adaptor::new(store.clone());
        let raft: LeaseRaft = Raft::new(local, config(), MeshRaftNetwork::new(), ls, sm)
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(local, BasicNode::default())]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "leader")
            .await
            .unwrap();
        (raft, store, placement)
    }

    /// ADR 0080 §2: a fresh cluster's leader records the configured factor BEFORE
    /// its first assignment, so no lease — and so no durable write — exists under
    /// another one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fresh_cluster_is_founded_at_the_configured_factor_before_any_lease() {
        use super::Founding;
        use std::sync::atomic::AtomicBool;
        let (raft, store, placement) = leading("found-node").await;
        let capable = Arc::new(AtomicBool::new(true));
        let assigner = LeaseAssigner::new(placement).with_founding(Founding::new(2, capable));
        assert!(store.replication().is_unset());
        let made = assigner.reconcile(&raft, &store).await.unwrap();
        assert_eq!(made, usize::try_from(NUM_GROUPS).unwrap());
        assert_eq!(store.replication().replicas, Some(2));
        // The factor's entry precedes every lease: the first minted epoch is 1.
        assert_eq!(store.high_epoch(), NUM_GROUPS);
        raft.shutdown().await.unwrap();
    }

    /// Without the capability the first assignment is HELD for the bound, then the
    /// cluster is founded without a factor (the legacy 3) rather than never serving.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_incapable_fresh_cluster_holds_then_founds_at_the_legacy_factor() {
        use super::Founding;
        use std::sync::atomic::AtomicBool;
        let (raft, store, placement) = leading("hold-node").await;
        let capable = Arc::new(AtomicBool::new(false));
        let assigner = LeaseAssigner::new(placement).with_founding(Founding::with_wait(
            2,
            capable,
            Duration::from_millis(300),
        ));
        assert_eq!(assigner.reconcile(&raft, &store).await.unwrap(), 0, "held");
        assert_eq!(store.high_epoch(), 0);
        tokio::time::sleep(Duration::from_millis(400)).await;
        let made = assigner.reconcile(&raft, &store).await.unwrap();
        assert_eq!(made, usize::try_from(NUM_GROUPS).unwrap(), "no longer held");
        assert!(store.replication().is_unset(), "runs at the legacy 3");
        assert_eq!(store.replication().effective(), 3);
        raft.shutdown().await.unwrap();
    }

    /// A cluster that already minted leases before ADR 0080 is never founded
    /// behind its back: it keeps 3 until an operator changes it live.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_existing_cluster_is_not_founded() {
        use super::Founding;
        use std::sync::atomic::AtomicBool;
        let (raft, store, placement) = leading("old-node").await;
        // An upgraded cluster: its leases predate founding.
        LeaseAssigner::new(placement.clone())
            .reconcile(&raft, &store)
            .await
            .unwrap();
        assert!(store.high_epoch() > 0);
        let capable = Arc::new(AtomicBool::new(true));
        let assigner = LeaseAssigner::new(placement).with_founding(Founding::new(2, capable));
        assigner.reconcile(&raft, &store).await.unwrap();
        assert!(
            store.replication().is_unset(),
            "no founding on an existing cluster"
        );
        raft.shutdown().await.unwrap();
    }
}
