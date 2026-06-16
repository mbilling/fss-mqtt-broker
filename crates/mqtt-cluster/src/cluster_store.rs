//! The durable cluster session store: assembling lease → epoch → per-group
//! `ClusterLog` → `ReplicatedSessionStore`
//! ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md) §1/§3,
//! workstream E step 4e).
//!
//! A [`ReplicatedSessionStore`](mqtt_storage::logged::ReplicatedSessionStore) runs
//! over a single [`ReplicatedLog`], but every *group* has its own
//! [`ClusterLog`](crate::cluster_log::ClusterLog) — a different epoch and replica
//! set. [`GroupRoutedLog`] bridges the two: it implements `ReplicatedLog` and routes
//! each key to its **group's** `ClusterLog`, building that log lazily the first time
//! the group is touched (acquiring the group's ownership lease then). Wrap it in a
//! `ReplicatedSessionStore` and the result is a durable, consensus-backed
//! `SessionStore` — the headline ADR 0001 guarantee, assembled.
//!
//! - **group routing**: a key `q/<client>` / `m/<client>` → `group_of(client)` →
//!   that group's `ClusterLog`. All of one client's keys (and all clients in a
//!   group) share one log, one lease, one replica set.
//! - **lazy lease acquisition**: on first touch of an owned group, the
//!   [`LeaseSource`] yields the group's epoch; the `ClusterLog` is built at that
//!   epoch over the group's replica set and cached. A key whose group this node does
//!   **not** own returns [`ReplError::NotOwner`] — the session should have been
//!   relocated to the owner ([ADR 0005](0005-session-affinity.md)).
//! - **the lease is abstracted** ([`LeaseSource`]) so this layer is exercised
//!   without standing up the consensus group; the openraft-backed source is wired in
//!   at step 4f.

use crate::cluster_log::{ClusterLog, ReplicaTransport};
use crate::lease::{Epoch, OwnershipLease};
use crate::lease_raft::{GroupId, RaftNodeId};
use crate::lease_store::LeaseStore;
use crate::placement::{group_of, Placement};
use crate::NodeId;
use async_trait::async_trait;
use mqtt_storage::repl::{LogEntry, ReplError, ReplicatedLog};
use mqtt_storage::Offset;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

/// Yields the current ownership-lease epoch for a group this node owns.
///
/// The caller has already confirmed ownership; the source establishes/refreshes the
/// lease (via the consensus group, step 4f) and returns its epoch. Abstracted so the
/// store layer is testable without the lease group.
#[async_trait]
pub trait LeaseSource: Send + Sync {
    /// The current lease epoch for `group` (this node is the owner).
    ///
    /// # Errors
    /// [`ReplError`] if the lease cannot be acquired (no quorum / not owner).
    async fn epoch_for(&self, group: GroupId) -> Result<Epoch, ReplError>;
}

/// The production [`LeaseSource`]: reads the group's lease epoch from this node's
/// own replicated [`LeaseStore`].
///
/// Per ADR 0007, leases are **leader-driven**: the lease-group leader assigns each
/// group's lease to the group's placement owner (the lease state machine mints the
/// epoch). That assignment replicates to every node, so the owner reads its epoch
/// straight from its applied `LeaseStore` — no app-level forwarding of a write to
/// the leader. A group whose lease has not (yet) been assigned to this node returns
/// [`ReplError::NotOwner`] (the caller serves it ephemerally until the leader
/// assigns it — ADR 0005 degrade-don't-refuse).
#[derive(Debug, Clone)]
pub struct LocalLeaseSource {
    store: LeaseStore,
    local: RaftNodeId,
}

impl LocalLeaseSource {
    /// A source reading `store` (this node's lease map) for leases held by `local`.
    #[must_use]
    pub fn new(store: LeaseStore, local: RaftNodeId) -> Self {
        Self { store, local }
    }
}

#[async_trait]
impl LeaseSource for LocalLeaseSource {
    async fn epoch_for(&self, group: GroupId) -> Result<Epoch, ReplError> {
        match self.store.current_lease(group) {
            // The lease is assigned to us — return the epoch we hold it at.
            Some(rec) if rec.holder == self.local => Ok(rec.epoch),
            // Assigned to another node, or not yet assigned: we cannot write durably.
            _ => Err(ReplError::NotOwner),
        }
    }
}

/// A [`ReplicatedLog`] that routes each key to its placement group's
/// [`ClusterLog`], building (and caching) that log lazily on first touch.
pub struct GroupRoutedLog<S: LeaseSource, T: ReplicaTransport + Clone> {
    local: NodeId,
    placement: Arc<RwLock<Placement>>,
    transport: T,
    leases: S,
    /// Per-group `ClusterLog`, built lazily. Cached so a group's offset state is
    /// stable across calls.
    logs: Mutex<BTreeMap<GroupId, Arc<ClusterLog<T>>>>,
}

impl<S: LeaseSource, T: ReplicaTransport + Clone> std::fmt::Debug for GroupRoutedLog<S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupRoutedLog")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

impl<S: LeaseSource, T: ReplicaTransport + Clone> GroupRoutedLog<S, T> {
    /// Build a group-routed log for `local`, resolving ownership/replica-sets from
    /// `placement`, replicating over `transport`, and acquiring leases from `leases`.
    #[must_use]
    pub fn new(local: NodeId, placement: Arc<RwLock<Placement>>, transport: T, leases: S) -> Self {
        Self {
            local,
            placement,
            transport,
            leases,
            logs: Mutex::new(BTreeMap::new()),
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, BTreeMap<GroupId, Arc<ClusterLog<T>>>> {
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The `ClusterLog` for `key`'s group, built lazily. Errors with
    /// [`ReplError::NotOwner`] if this node does not own the group.
    async fn log_for_key(&self, key: &str) -> Result<Arc<ClusterLog<T>>, ReplError> {
        // Keys are `q/<client>` / `m/<client>`; the client follows the 2-byte prefix.
        let client = key.get(2..).unwrap_or(key);
        let group = group_of(client);

        if let Some(log) = self.cache().get(&group).cloned() {
            return Ok(log);
        }

        // Resolve ownership + replica set without holding the lock across the await.
        let replica_set = {
            let placement = self
                .placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !placement.owns_group(group) {
                return Err(ReplError::NotOwner);
            }
            placement.group_replica_set(group)
        };

        let epoch = self.leases.epoch_for(group).await?;
        let lease = OwnershipLease {
            holder: self.local.clone(),
            epoch,
        };
        let log = Arc::new(ClusterLog::new(
            self.local.clone(),
            lease,
            &replica_set,
            self.transport.clone(),
        ));
        // or_insert so concurrent builders converge on one canonical log (no
        // divergent offset state).
        Ok(self.cache().entry(group).or_insert(log).clone())
    }
}

#[async_trait]
impl<S: LeaseSource, T: ReplicaTransport + Clone> ReplicatedLog for GroupRoutedLog<S, T> {
    type Key = String;

    async fn append(&self, key: &String, record: Vec<u8>) -> Result<Offset, ReplError> {
        self.log_for_key(key).await?.append(key, record).await
    }

    async fn read(
        &self,
        key: &String,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<LogEntry>, ReplError> {
        self.log_for_key(key).await?.read(key, after, limit).await
    }

    async fn truncate(&self, key: &String, up_to: Offset) -> Result<(), ReplError> {
        self.log_for_key(key).await?.truncate(key, up_to).await
    }

    async fn remove(&self, key: &String) -> Result<(), ReplError> {
        self.log_for_key(key).await?.remove(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::{GroupRoutedLog, LeaseSource};
    use crate::cluster_log::ReplicaState;
    use crate::lease::Epoch;
    use crate::lease_raft::GroupId;
    use crate::peer::PeerMessage;
    use crate::placement::{group_of, Placement, DEFAULT_REPLICAS, NUM_GROUPS};
    use crate::repl_net::PeerReplicaTransport;
    use crate::swim::MemberState;
    use crate::NodeId;
    use async_trait::async_trait;
    use mqtt_core::{ClientId, Message, QoS};
    use mqtt_storage::logged::ReplicatedSessionStore;
    use mqtt_storage::SessionStore;
    use std::sync::{Arc, Mutex, RwLock};
    use tokio::sync::mpsc;

    fn nid(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// A fixed-epoch lease source (the real one talks to the consensus group, 4f).
    #[derive(Debug)]
    struct FixedLease(Epoch);

    #[async_trait]
    impl LeaseSource for FixedLease {
        async fn epoch_for(&self, _group: GroupId) -> Result<Epoch, mqtt_storage::repl::ReplError> {
            Ok(self.0)
        }
    }

    /// A follower: read replication frames off its link channel, apply to its
    /// `ReplicaState`, and ack on the owner's transport (in-process stand-in for the
    /// peer link, which the wire tests already cover).
    fn spawn_follower(
        transport: Arc<PeerReplicaTransport>,
        state: Arc<Mutex<ReplicaState>>,
        mut rx: mpsc::UnboundedReceiver<PeerMessage>,
    ) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let PeerMessage::Replicate { req_id, epoch, op } = msg {
                    let accepted = state.lock().unwrap().apply(epoch, &op);
                    transport.complete_ack(req_id, accepted);
                }
            }
        });
    }

    /// Find a placement group `owner` owns, and a client id that hashes to it.
    fn owned_group_and_client(p: &Placement) -> (GroupId, ClientId) {
        let group = (0..NUM_GROUPS)
            .find(|g| p.owns_group(*g))
            .expect("owns some group");
        for i in 0..100_000 {
            let c = format!("client-{i}");
            if group_of(&c) == group {
                return (group, ClientId(c));
            }
        }
        panic!("no client hashes to the owned group");
    }

    /// A client whose group `owner` does NOT own (for the not-owner path).
    fn foreign_client(p: &Placement) -> ClientId {
        for i in 0..100_000 {
            let c = format!("client-{i}");
            if !p.owns_group(group_of(&c)) {
                return ClientId(c);
            }
        }
        panic!("this node owns every group");
    }

    /// An enqueue through the durable store quorum-replicates to a follower — so the
    /// message survives the owner's loss (a replica has it). The headline durability
    /// claim, assembled end to end (store → group routing → `ClusterLog` → replication).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn enqueue_replicates_to_a_follower() {
        let owner = nid("owner");
        // A 3-node ring (R=3, quorum=2): owner + two followers.
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000");
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000");
        let placement = Arc::new(RwLock::new(p));

        // Owner's transport, wired to two in-process followers.
        let transport = Arc::new(PeerReplicaTransport::new());
        let f1_state = Arc::new(Mutex::new(ReplicaState::new()));
        let f2_state = Arc::new(Mutex::new(ReplicaState::new()));
        for (node, state) in [(nid("f1"), &f1_state), (nid("f2"), &f2_state)] {
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node, tx);
            spawn_follower(transport.clone(), state.clone(), rx);
        }

        let store = ReplicatedSessionStore::new(GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport.clone(),
            FixedLease(1),
        ));

        let (group, client) = owned_group_and_client(&placement.read().unwrap());
        let msg = Message {
            topic: "t".to_string(),
            payload: bytes::Bytes::from_static(b"durable"),
            qos: QoS::AtLeastOnce,
            retain: false,
        };
        store.ensure_session(&client).await.unwrap();
        store.enqueue(&client, &msg).await.unwrap();

        // The message is durable: a follower's replicated copy holds the queue entry.
        let qkey = format!("q/{}", client.0);
        let on_f1 = f1_state.lock().unwrap().entries(&qkey).len();
        let on_f2 = f2_state.lock().unwrap().entries(&qkey).len();
        assert!(
            on_f1 + on_f2 >= 1,
            "the enqueue should have replicated to at least one follower (group {group})"
        );
        // The owner can read its own committed queue back.
        assert_eq!(store.pending(&client, 0, 100).await.unwrap().len(), 1);
    }

    /// `LocalLeaseSource` reads a lease the (leader-driven) consensus group assigned
    /// to this node, and reports `NotOwner` for a group not assigned to it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_lease_source_reads_an_assigned_lease() {
        use crate::cluster_store::LocalLeaseSource;
        use crate::lease_group::config;
        use crate::lease_raft::LeaseRequest;
        use crate::lease_store::LeaseStore;
        use crate::node_registry::raft_id;
        use crate::raft_mesh::MeshRaftNetwork;
        use openraft::storage::Adaptor;
        use openraft::{BasicNode, Raft, ServerState};
        use std::collections::BTreeMap;
        use std::time::Duration;

        let local = raft_id(&nid("lease-node"));
        let store = LeaseStore::new();
        let source = LocalLeaseSource::new(store.clone(), local);
        let (ls, sm) = Adaptor::new(store);
        let raft = Raft::new(local, config(), MeshRaftNetwork::new(), ls, sm)
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(local, BasicNode::default())]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "leader")
            .await
            .unwrap();

        // No lease for group 7 yet → NotOwner.
        assert!(source.epoch_for(7).await.is_err());

        // Assign group 7 to this node; the owner reads its epoch locally.
        let resp = raft
            .client_write(LeaseRequest::Assign {
                group: 7,
                node: local,
            })
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(resp.log_id.index), "applied")
            .await
            .unwrap();
        assert_eq!(source.epoch_for(7).await.unwrap(), resp.data.unwrap().epoch);

        // A group assigned to another node is NotOwner to us.
        let resp = raft
            .client_write(LeaseRequest::Assign {
                group: 8,
                node: local.wrapping_add(1),
            })
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(resp.log_id.index), "applied")
            .await
            .unwrap();
        assert!(source.epoch_for(8).await.is_err());

        raft.shutdown().await.unwrap();
    }

    /// A session whose group this node does not own is refused (it belongs on the
    /// group owner — relocation, ADR 0005), surfaced as a storage backend error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn foreign_group_is_not_owned() {
        let owner = nid("owner");
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000");
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000");
        let placement = Arc::new(RwLock::new(p));

        let store = ReplicatedSessionStore::new(GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(1),
        ));

        let foreign = foreign_client(&placement.read().unwrap());
        let msg = Message {
            topic: "t".to_string(),
            payload: bytes::Bytes::from_static(b"x"),
            qos: QoS::AtLeastOnce,
            retain: false,
        };
        assert!(
            store.enqueue(&foreign, &msg).await.is_err(),
            "a non-owned group must be refused"
        );
    }
}
