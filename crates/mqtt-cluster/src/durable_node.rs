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
    data_dir: Option<&std::path::Path>,
) -> (
    Arc<dyn SessionStore>,
    DurablePlane,
    tokio::task::JoinHandle<()>,
) {
    let local = raft_id(&node_id);

    // --- lease consensus group + durable-plane endpoint ---
    let network = MeshRaftNetwork::new();
    // On-disk persistence when a data dir is given (ADR 0018): the lease store (phase 2 —
    // restoring Raft safety via the persisted vote) and the follower replica copy (phase
    // 3 — clustered sessions survive a full-cluster restart); otherwise both in-memory.
    let lease_store = match data_dir {
        Some(dir) => LeaseStore::open(dir.join("lease.redb")).expect("open the lease store"),
        None => LeaseStore::new(),
    };
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
    // One follower-copy `ReplicaState`, shared: the plane applies inbound Replicates
    // into it, and the store reads it back for takeover recovery (workstream F).
    // Persistent (ADR 0018 phase 3) when a data dir is given, so the committed copy
    // survives a restart.
    let replicas = Arc::new(Mutex::new(match data_dir {
        Some(dir) => ReplicaState::open(dir.join("replicas.redb")).expect("open the replica store"),
        None => ReplicaState::new(),
    }));
    let plane = DurablePlane::new(raft.clone(), network, transport.clone(), replicas.clone());

    // --- durable store over the shared transport ---
    let lease_source = LocalLeaseSource::new(lease_store.clone(), local);
    let group_log = GroupRoutedLog::new(
        node_id,
        placement.clone(),
        transport,
        lease_source,
        replicas,
    );
    let store: Arc<dyn SessionStore> = Arc::new(ReplicatedSessionStore::new(group_log));

    // --- driver: membership + lease assignment over the live placement ---
    // The handle is returned so the caller can stop the driver on shutdown (otherwise
    // the loop outlives `raft`, spinning against a shut-down consensus handle) and so a
    // restart can release the on-disk lease/replica locks (ADR 0018 phase 5).
    let driver = tokio::spawn(run_driver(
        raft,
        lease_store,
        placement.clone(),
        MembershipReconciler::new(local, can_bootstrap),
        LeaseAssigner::new(placement),
    ));

    (store, plane, driver)
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
    use mqtt_storage::SessionStore;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant};

    /// A single node's durable stack bootstraps itself (the driver elects the lease
    /// group and assigns leases), after which an enqueue commits and replays — the
    /// whole assembly wired together end to end on one node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_node_durable_store_bootstraps_and_serves() {
        let node = NodeId("durable-solo".to_string());
        let placement = Arc::new(RwLock::new(Placement::new(node.clone(), DEFAULT_REPLICAS)));
        let (store, _plane, _driver) = build_durable_node(node, placement, true, None).await;

        let client = ClientId("c".to_string());
        let msg = Message {
            topic: "t".to_string(),
            payload: bytes::Bytes::from_static(b"durable"),
            qos: QoS::AtLeastOnce,
            retain: false,
        };

        // Poll until the driver has bootstrapped the lease group and assigned this
        // node its groups' leases, at which point the enqueue commits.
        wait_writable(&store, &client, &msg).await;

        // The committed message replays.
        let pending = store.pending(&client, 0, 100).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0].message.payload[..], b"durable");
    }

    /// Poll an enqueue until the durable store is writable (lease bootstrapped and
    /// assigned to this node), or fail after a generous deadline.
    async fn wait_writable(store: &Arc<dyn SessionStore>, client: &ClientId, msg: &Message) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if store.enqueue(client, msg).await.is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "durable store never became writable (lease not assigned)"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// ADR 0018 phase 5 (cluster-path node-level restart). A persistent durable node
    /// bootstraps its lease group on disk, is **fully torn down** (driver aborted, raft
    /// shut down, store + plane dropped — which releases the `lease.redb`/`replicas.redb`
    /// file locks), and is then **rebuilt from the same data directory**: it reopens its
    /// persisted lease state without a double-init, re-leads, and becomes writable again.
    ///
    /// This is the assembly-level proof that graceful shutdown (ADR 0019) makes the
    /// durable plane cleanly restartable. It deliberately does **not** assert the
    /// pre-restart message survives: a *single* durable node holds committed session
    /// entries in the leader's in-memory log until a follower has them, so session
    /// restart-durability needs R≥2 (proven at the store level in `cluster_log`); here
    /// the persistent state that must survive is the **lease vote/membership**.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_persistent_durable_node_restarts_from_its_data_dir() {
        let dir = tempfile::tempdir().unwrap();
        let node = NodeId("durable-restart".to_string());
        let client = ClientId("c".to_string());
        let msg = Message {
            topic: "t".to_string(),
            payload: bytes::Bytes::from_static(b"durable"),
            qos: QoS::AtLeastOnce,
            retain: false,
        };

        // --- lifetime #1: bootstrap on disk, become writable ---
        let placement = Arc::new(RwLock::new(Placement::new(node.clone(), DEFAULT_REPLICAS)));
        let (store, plane, driver) =
            build_durable_node(node.clone(), placement, true, Some(dir.path())).await;
        wait_writable(&store, &client, &msg).await;

        // --- teardown: release the on-disk locks (the part ADR 0019 unblocks) ---
        driver.abort();
        let _ = driver.await;
        plane.raft().shutdown().await.unwrap();
        drop(store);
        drop(plane);
        // The last `Database` handle drops synchronously above; give any in-flight
        // blocking apply a moment to release before reopening the same files.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // --- lifetime #2: a fresh node over the SAME directory recovers and re-leads ---
        let placement = Arc::new(RwLock::new(Placement::new(node.clone(), DEFAULT_REPLICAS)));
        let (store, plane, driver) =
            build_durable_node(node, placement, true, Some(dir.path())).await;
        // Becoming writable again proves the persisted lease store reopened (no
        // "Database already open" lock, no double-init panic) and the node re-led.
        wait_writable(&store, &client, &msg).await;

        driver.abort();
        let _ = driver.await;
        plane.raft().shutdown().await.unwrap();
    }
}
