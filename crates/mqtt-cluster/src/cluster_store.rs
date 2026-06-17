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

use crate::cluster_log::{merge_replica_logs, ClusterLog, ReplicaState, ReplicaTransport};
use crate::lease::{Epoch, OwnershipLease};
use crate::lease_raft::{GroupId, RaftNodeId};
use crate::lease_store::LeaseStore;
use crate::placement::{group_of, Placement};
use crate::NodeId;
use async_trait::async_trait;
use mqtt_storage::repl::{LogEntry, ReplError, ReplicatedLog};
use mqtt_storage::Offset;
use std::collections::{BTreeMap, BTreeSet};
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

/// A group's cached `ClusterLog` together with the keys already recovered against
/// it. The recovery markers live **with** the log so that rebuilding the log on an
/// epoch change (below) atomically resets recovery for the whole group — every key
/// must re-recover against the new lease's replica set, not trust a marker set under
/// the old epoch.
struct GroupEntry<T: ReplicaTransport> {
    log: Arc<ClusterLog<T>>,
    recovered: Mutex<BTreeSet<String>>,
}

/// A [`ReplicatedLog`] that routes each key to its placement group's
/// [`ClusterLog`], building (and caching) that log lazily on first touch,
/// **recovering** each key from a quorum of replicas the first time it is served
/// after a takeover (workstream F), and **rebuilding** the cached log when the
/// group's lease epoch advances (ownership lost and regained — the stale log would
/// self-fence forever).
pub struct GroupRoutedLog<S: LeaseSource, T: ReplicaTransport + Clone + 'static> {
    local: NodeId,
    placement: Arc<RwLock<Placement>>,
    transport: T,
    leases: S,
    /// This node's own follower copy of the session logs (shared with the
    /// `DurablePlane`). On takeover this node was a replica, so its committed copy of
    /// a key lives here — it is one of the quorum reads recovery merges.
    local_replicas: Arc<Mutex<ReplicaState>>,
    /// Per-group cached log + recovery markers, built lazily and rebuilt when the
    /// lease epoch advances. Cached so a group's offset state is stable across calls.
    groups: Mutex<BTreeMap<GroupId, Arc<GroupEntry<T>>>>,
}

impl<S: LeaseSource, T: ReplicaTransport + Clone + 'static> std::fmt::Debug
    for GroupRoutedLog<S, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupRoutedLog")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

impl<S: LeaseSource, T: ReplicaTransport + Clone + 'static> GroupRoutedLog<S, T> {
    /// Build a group-routed log for `local`, resolving ownership/replica-sets from
    /// `placement`, replicating over `transport`, acquiring leases from `leases`, and
    /// recovering from `local_replicas` + peer reads on takeover.
    #[must_use]
    pub fn new(
        local: NodeId,
        placement: Arc<RwLock<Placement>>,
        transport: T,
        leases: S,
        local_replicas: Arc<Mutex<ReplicaState>>,
    ) -> Self {
        Self {
            local,
            placement,
            transport,
            leases,
            local_replicas,
            groups: Mutex::new(BTreeMap::new()),
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, BTreeMap<GroupId, Arc<GroupEntry<T>>>> {
        self.groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The `ClusterLog` for `key`'s group, built and recovered lazily. Errors with
    /// [`ReplError::NotOwner`] if this node does not own the group, or
    /// [`ReplError::NoQuorum`] if recovery cannot reach a quorum of replicas.
    async fn log_for_key(&self, key: &str) -> Result<Arc<ClusterLog<T>>, ReplError> {
        // Keys are `q/<client>` / `m/<client>`; the client follows the 2-byte prefix.
        let client = key.get(2..).unwrap_or(key);
        let group = group_of(client);

        // Resolve ownership + replica set without holding a lock across an await.
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

        // Read the group's current lease epoch on every call. A higher epoch than the
        // cached log's means ownership was lost and regained: the cached log writes at
        // the old epoch and would be fenced by followers forever, so it must be rebuilt
        // (and the group's keys re-recovered) against the new lease.
        let epoch = self.leases.epoch_for(group).await?;

        // Get-or-(re)build the group entry at the current epoch. Resolve to an owned
        // `Arc` and drop the guard before any await (the guard is not `Send`).
        let entry = {
            let mut cache = self.cache();
            match cache.get(&group) {
                Some(entry) if entry.log.epoch() == epoch => entry.clone(),
                _ => {
                    let lease = OwnershipLease {
                        holder: self.local.clone(),
                        epoch,
                    };
                    let entry = Arc::new(GroupEntry {
                        log: Arc::new(ClusterLog::new(
                            self.local.clone(),
                            lease,
                            &replica_set,
                            self.transport.clone(),
                        )),
                        recovered: Mutex::new(BTreeSet::new()),
                    });
                    cache.insert(group, entry.clone());
                    entry
                }
            }
        };

        // Recover this key once per epoch: a new owner was a replica, so the committed
        // log lives in the replica set (its own copy + peers). Seeding it lets the
        // recovered queue replay (a fresh session simply recovers to empty). The marker
        // lives in the entry, so an epoch rebuild above resets it for the group.
        let recover = !entry
            .recovered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(key);
        if recover {
            let recovered = self.recover_key(key, &replica_set).await?;
            entry.log.seed_key(key, recovered).await;
            entry
                .recovered
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key.to_string());
        }
        Ok(entry.log.clone())
    }

    /// Recover `key`'s committed log by reading a quorum of the replica set: this
    /// node's own follower copy plus the reachable peers'. Errors with
    /// [`ReplError::NoQuorum`] if fewer than a quorum can be read (recovery is unsafe
    /// — a committed entry might live only on an unread replica).
    async fn recover_key(
        &self,
        key: &str,
        replica_set: &[NodeId],
    ) -> Result<Vec<LogEntry>, ReplError> {
        let quorum = replica_set.len() / 2 + 1;
        // Local copy first (sync; the guard is dropped before any await).
        let mut reads = vec![self
            .local_replicas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries(key)];
        let mut have = 1;
        for replica in replica_set.iter().filter(|n| **n != self.local) {
            if let Some(entries) = self.transport.read_replica(replica, key).await {
                reads.push(entries);
                have += 1;
            }
        }
        if have < quorum {
            return Err(ReplError::NoQuorum);
        }
        Ok(merge_replica_logs(&reads))
    }
}

#[async_trait]
impl<S: LeaseSource, T: ReplicaTransport + Clone + 'static> ReplicatedLog for GroupRoutedLog<S, T> {
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
    use crate::cluster_log::{ReplOp, ReplicaState};
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
    use mqtt_storage::repl::ReplicatedLog;
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
    /// peer link, which the wire tests already cover). Also answers recovery-reads
    /// (`ReplicaRead`) from its stored copy, as the real plane does.
    fn spawn_follower(
        transport: Arc<PeerReplicaTransport>,
        state: Arc<Mutex<ReplicaState>>,
        mut rx: mpsc::UnboundedReceiver<PeerMessage>,
    ) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    PeerMessage::Replicate { req_id, epoch, op } => {
                        let accepted = state.lock().unwrap().apply(epoch, &op);
                        transport.complete_ack(req_id, accepted);
                    }
                    PeerMessage::ReplicaRead { req_id, key } => {
                        let entries = state
                            .lock()
                            .unwrap()
                            .entries(&key)
                            .into_iter()
                            .map(|e| (e.offset, e.record))
                            .collect();
                        transport.complete_read(req_id, entries);
                    }
                    _ => {}
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
            Arc::new(Mutex::new(ReplicaState::new())),
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
            Arc::new(Mutex::new(ReplicaState::new())),
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

    /// Takeover recovery: a new owner was a replica, so its committed copy of a key
    /// lives in the shared follower `ReplicaState`. On the key's first touch the
    /// store recovers it from a quorum of the replica set (here just this node's own
    /// copy), replays the recovered entries, and continues appending after the
    /// recovered watermark — the heart of workstream F.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn takeover_recovers_a_keys_log_from_the_shared_replica_state() {
        let owner = nid("owner");
        // A single-node group (quorum 1): recovery reads only this node's own copy.
        let placement = Arc::new(RwLock::new(Placement::new(owner.clone(), DEFAULT_REPLICAS)));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        // This node was a replica: its follower copy already holds the committed
        // queue (offsets 1, 2), applied at some prior epoch.
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        {
            let mut r = replicas.lock().unwrap();
            for (offset, record) in [(1u64, b"m1".to_vec()), (2u64, b"m2".to_vec())] {
                r.apply(
                    3,
                    &ReplOp::Append {
                        key: qkey.clone(),
                        offset,
                        record,
                    },
                );
            }
        }

        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(7),
            replicas,
        );

        // First touch recovers the committed copy; the recovered entries replay.
        let got = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(got.iter().map(|e| e.offset).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(&got[1].record, b"m2");

        // Appends continue after the recovered watermark (no offset reuse).
        assert_eq!(log.append(&qkey, b"m3".to_vec()).await.unwrap(), 3);
        assert_eq!(log.read(&qkey, 0, 100).await.unwrap().len(), 3);
    }

    /// When a group's lease epoch advances (ownership lost then regained), the cached
    /// `ClusterLog` — which writes at the stale epoch and would be fenced by followers
    /// forever — is rebuilt at the new epoch and the group's keys are re-recovered
    /// from the replica set. Without the rebuild the second read would return the
    /// stale recovered range, never seeing the entry committed under the new owner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_higher_lease_epoch_rebuilds_and_re_recovers_the_log() {
        use std::sync::atomic::{AtomicU64, Ordering};

        /// A lease source whose epoch can be bumped to simulate a regain.
        #[derive(Clone)]
        struct BumpableLease(Arc<AtomicU64>);
        #[async_trait]
        impl LeaseSource for BumpableLease {
            async fn epoch_for(
                &self,
                _group: GroupId,
            ) -> Result<Epoch, mqtt_storage::repl::ReplError> {
                Ok(self.0.load(Ordering::Relaxed))
            }
        }

        let owner = nid("owner");
        // Single-node group (quorum 1): recovery reads only this node's own copy.
        let placement = Arc::new(RwLock::new(Placement::new(owner.clone(), DEFAULT_REPLICAS)));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        // This node's follower copy holds the committed queue (offsets 1, 2).
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        {
            let mut r = replicas.lock().unwrap();
            for (offset, record) in [(1u64, b"m1".to_vec()), (2u64, b"m2".to_vec())] {
                r.apply(
                    3,
                    &ReplOp::Append {
                        key: qkey.clone(),
                        offset,
                        record,
                    },
                );
            }
        }

        let epoch = Arc::new(AtomicU64::new(3));
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            BumpableLease(epoch.clone()),
            replicas.clone(),
        );

        // At epoch 3 the log recovers [1, 2].
        let got = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(got.iter().map(|e| e.offset).collect::<Vec<_>>(), vec![1, 2]);

        // Ownership was lost then regained at a higher epoch; meanwhile a committed
        // entry (offset 3) landed in this node's replica copy. Bumping the epoch must
        // rebuild the log and re-recover, picking up offset 3 the stale cached log
        // would never see.
        replicas.lock().unwrap().apply(
            7,
            &ReplOp::Append {
                key: qkey.clone(),
                offset: 3,
                record: b"m3".to_vec(),
            },
        );
        epoch.store(7, Ordering::Relaxed);

        let got = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(
            got.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(&got[2].record, b"m3");
    }
}
