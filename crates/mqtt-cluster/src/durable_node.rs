//! Assembling a node's durable cluster session store
//! ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md), workstream E
//! step 4f).
//!
//! [`build_durable_node`] ties the workstream-E components into one node's durable
//! stack and a background driver:
//!
//! - the **lease consensus group** — a [`MeshRaftNetwork`] + [`LeaseStore`] +
//!   openraft [`Raft`], bundled with a replication transport + follower state into a
//!   [`DurablePlane`] (the endpoint the broker routes peer frames to);
//! - the **durable store** — a [`ReplicatedSessionStore`] over a [`GroupRoutedLog`]
//!   that routes each session to its group's `ClusterLog`, the epoch read from the
//!   local lease map ([`LocalLeaseSource`]);
//! - the **driver** — a task that, on a tick over the live [`Placement`] membership,
//!   reconciles the lease group's voters ([`MembershipReconciler`]) and assigns each
//!   group's lease to its placement owner ([`LeaseAssigner`]) when this node leads.
//!
//! The replication transport is **shared** between the plane (which the broker
//! registers peers on) and the store (which replicates over it), so a session-log
//! append fans out to the same peer links the consensus RPCs use. Returns the store
//! (to hand the hub) and the plane (to attach to the hub).

use crate::cluster_log::ReplicaState;
use crate::cluster_store::{GroupRoutedLog, LocalLeaseSource};
use crate::durable_plane::DurablePlane;
use crate::lease_assign::LeaseAssigner;
use crate::lease_group::{config as lease_config, LeaseRaft};
use crate::lease_membership::{apply_action, raft_view, MembershipReconciler};
use crate::lease_raft::RaftNodeId;
use crate::lease_store::LeaseStore;
use crate::node_registry::raft_id;
use crate::placement::Placement;
use crate::raft_mesh::MeshRaftNetwork;
use crate::repl_net::PeerReplicaTransport;
use crate::NodeId;
use mqtt_storage::logged::ReplicatedSessionStore;
use mqtt_storage::SessionStore;
use openraft::storage::Adaptor;
use openraft::Raft;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tracing::{debug, warn};

/// How often the driver reconciles membership + lease assignment against the live
/// placement ring. A short tick keeps lease/voter changes responsive after a
/// membership change; the work is cheap (a membership read plus, in steady state, a
/// no-op).
const DRIVER_TICK: Duration = Duration::from_millis(200);

/// Build a node's durable session store, lease-group endpoint, and background
/// driver. Returns the store (for the hub) and the [`DurablePlane`] (to attach to
/// the hub so it routes peer consensus/replication frames).
///
/// `can_bootstrap` marks this node a **founder** (started with no SWIM seeds): only
/// a founder creates the lease group; joiners wait to be added by the founder's
/// leader. Exactly one founder per cluster (see [`MembershipReconciler::new`]).
///
/// # Panics
/// Panics if the lease `Raft` fails to start (a programming/config error at boot).
pub async fn build_durable_node(
    node_id: NodeId,
    placement: Arc<RwLock<Placement>>,
    can_bootstrap: bool,
) -> (Arc<dyn SessionStore>, DurablePlane) {
    let local = raft_id(&node_id);

    // --- lease consensus group + durable-plane endpoint ---
    let network = MeshRaftNetwork::new();
    let lease_store = LeaseStore::new();
    let (log_store, state_machine) = Adaptor::new(lease_store.clone());
    let raft: LeaseRaft = Raft::new(
        local,
        lease_config(),
        network.clone(),
        log_store,
        state_machine,
    )
    .await
    .expect("lease raft starts");
    let transport = Arc::new(PeerReplicaTransport::new());
    let plane = DurablePlane::new(
        raft.clone(),
        network,
        transport.clone(),
        Arc::new(Mutex::new(ReplicaState::new())),
    );

    // --- durable store over the shared transport ---
    let lease_source = LocalLeaseSource::new(lease_store.clone(), local);
    let group_log = GroupRoutedLog::new(node_id, placement.clone(), transport, lease_source);
    let store: Arc<dyn SessionStore> = Arc::new(ReplicatedSessionStore::new(group_log));

    // --- driver: membership + lease assignment over the live placement ---
    tokio::spawn(run_driver(
        raft,
        lease_store,
        placement.clone(),
        MembershipReconciler::new(local, can_bootstrap),
        LeaseAssigner::new(placement),
    ));

    (store, plane)
}

/// The lease-group control loop: on each tick, reconcile the voter set toward the
/// live membership and (as leader) keep each group's lease on its placement owner.
async fn run_driver(
    raft: LeaseRaft,
    lease_store: LeaseStore,
    placement: Arc<RwLock<Placement>>,
    reconciler: MembershipReconciler,
    assigner: LeaseAssigner,
) {
    // A one-tick debounce: only act once the desired set is stable across a tick, so
    // a flapping member does not churn the voter set.
    let mut prev_desired: BTreeSet<RaftNodeId> = BTreeSet::new();
    loop {
        tokio::time::sleep(DRIVER_TICK).await;

        let desired: BTreeSet<RaftNodeId> = {
            let p = placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            p.members().iter().map(raft_id).collect()
        };

        if desired == prev_desired {
            let action = reconciler.decide(&raft_view(&raft), &desired);
            if let Err(e) = apply_action(&raft, &action).await {
                warn!(error = %e, "lease-group membership reconcile failed");
            }
            match assigner.reconcile(&raft, &lease_store).await {
                Ok(n) if n > 0 => debug!(assigned = n, "lease assignments reconciled"),
                Ok(_) => {}
                Err(e) => warn!(error = %e, "lease assignment reconcile failed"),
            }
        }
        prev_desired = desired;
    }
}

#[cfg(test)]
mod tests {
    use super::build_durable_node;
    use crate::placement::{Placement, DEFAULT_REPLICAS};
    use crate::NodeId;
    use mqtt_core::{ClientId, Message, QoS};
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant};

    /// A single node's durable stack bootstraps itself (the driver elects the lease
    /// group and assigns leases), after which an enqueue commits and replays — the
    /// whole assembly wired together end to end on one node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_node_durable_store_bootstraps_and_serves() {
        let node = NodeId("durable-solo".to_string());
        let placement = Arc::new(RwLock::new(Placement::new(node.clone(), DEFAULT_REPLICAS)));
        let (store, _plane) = build_durable_node(node, placement, true).await;

        let client = ClientId("c".to_string());
        let msg = Message {
            topic: "t".to_string(),
            payload: bytes::Bytes::from_static(b"durable"),
            qos: QoS::AtLeastOnce,
            retain: false,
        };

        // Poll until the driver has bootstrapped the lease group and assigned this
        // node its groups' leases, at which point the enqueue commits.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if store.enqueue(&client, &msg).await.is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "durable store never became writable (lease not assigned)"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // The committed message replays.
        let pending = store.pending(&client, 0, 100).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0].message.payload[..], b"durable");
    }
}
