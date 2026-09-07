//! Production `GroupRoutedLog` -> `ClusterLog` -> redb replicas, with only the peer
//! transport and lease clock simulated. No sockets, cloud resources or sleeps.
use super::*;
use mqtt_cluster::cluster_log::{ReplOp, ReplicaRead, ReplicaState, ReplicaTransport};
use mqtt_cluster::cluster_store::{GroupRoutedLog, LeaseSource};
use mqtt_cluster::lease::Epoch;
use mqtt_cluster::lease_raft::GroupId;
use mqtt_cluster::placement::{group_of, Placement};
use mqtt_cluster::swim::MemberState;
use mqtt_storage::logged::ReplicatedSessionStore;
use mqtt_storage::repl::ReplError;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

struct Lease(Arc<AtomicU64>);
#[async_trait::async_trait]
impl LeaseSource for Lease {
    async fn epoch_for(&self, _group: GroupId) -> Result<Epoch, ReplError> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

#[derive(Debug)]
struct Replicas {
    states: BTreeMap<NodeId, Arc<Mutex<ReplicaState>>>,
    reachable: Mutex<BTreeSet<NodeId>>,
    drop_truncates: AtomicBool,
    rejected_truncates: AtomicUsize,
}

#[async_trait::async_trait]
impl ReplicaTransport for Replicas {
    async fn deliver(&self, replica: &NodeId, epoch: Epoch, op: &ReplOp) -> bool {
        if !self.reachable.lock().unwrap().contains(replica) {
            return false;
        }
        if matches!(op, ReplOp::Truncate { .. }) && self.drop_truncates.load(Ordering::SeqCst) {
            self.rejected_truncates.fetch_add(1, Ordering::SeqCst);
            return false;
        }
        self.states[replica].lock().unwrap().apply(epoch, op)
    }

    async fn read_replica(&self, replica: &NodeId, key: &str) -> Option<ReplicaRead> {
        if !self.reachable.lock().unwrap().contains(replica) {
            return None;
        }
        let state = self.states[replica].lock().unwrap();
        Some(ReplicaRead {
            watermark: state.watermark(key),
            complete: state.complete(key),
            entries: state.epoch_entries(key),
        })
    }

    async fn list_remote_keys(&self) -> Vec<String> {
        self.reachable
            .lock()
            .unwrap()
            .iter()
            .flat_map(|node| self.states[node].lock().unwrap().keys())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

struct Fleet {
    transport: Arc<Replicas>,
    epoch: Arc<AtomicU64>,
    client: ClientId,
    // Declared last so the durable replicas are dropped before the directory.
    _dir: tempfile::TempDir,
}

impl Fleet {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let client = ClientId("quorum-retirement".into());
        let nodes: Vec<_> = ["a", "b", "c"]
            .into_iter()
            .map(|n| NodeId(n.into()))
            .collect();
        let states = nodes
            .iter()
            .map(|node| {
                let mut state =
                    ReplicaState::open(dir.path().join(format!("{}.redb", node.0))).unwrap();
                // Explicit genesis: all three empty replicas are current for this group.
                state.mark_groups_current(&[(group_of(client.as_str()), nodes.clone())]);
                (node.clone(), Arc::new(Mutex::new(state)))
            })
            .collect();
        Self {
            transport: Arc::new(Replicas {
                states,
                reachable: Mutex::new(nodes.into_iter().collect()),
                drop_truncates: AtomicBool::new(false),
                rejected_truncates: AtomicUsize::new(0),
            }),
            epoch: Arc::new(AtomicU64::new(1)),
            client,
            _dir: dir,
        }
    }

    fn owner(&self, node: &str) -> (Arc<ParkingStore>, Arc<RwLock<Placement>>) {
        let local = NodeId(node.into());
        let mut placement = Placement::new(local.clone(), 3);
        for other in self.transport.states.keys().filter(|n| **n != local) {
            placement.observe(other, MemberState::Alive, "127.0.0.1:7000", None);
        }
        placement.set_lease_owners(BTreeMap::from([(
            group_of(self.client.as_str()),
            local.clone(),
        )]));
        let placement = Arc::new(RwLock::new(placement));
        let log = GroupRoutedLog::new(
            local.clone(),
            placement.clone(),
            self.transport.clone(),
            Lease(self.epoch.clone()),
            self.transport.states[&local].clone(),
        );
        (
            ParkingStore::with_store(Arc::new(ReplicatedSessionStore::new(log))),
            placement,
        )
    }

    fn lose_owner(&self) {
        self.transport
            .reachable
            .lock()
            .unwrap()
            .remove(&NodeId("a".into()));
        self.epoch.store(2, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_quorum_truncate_failure_keeps_the_id_through_owner_loss() {
    let fleet = Fleet::new();
    let (store, _) = fleet.owner("a");
    let (mut hub, _) = Hub::with_config(NodeId("a".into()), store.clone());
    let offset = delivery(&mut hub, &fleet.client, 7, QoS::ExactlyOnce).await;
    fleet.transport.drop_truncates.store(true, Ordering::SeqCst);
    hub.pub_comp(&fleet.client, 7).await;
    assert!(
        fleet.transport.rejected_truncates.load(Ordering::SeqCst) >= 2,
        "the production truncate must actually encounter both unavailable followers"
    );
    assert_eq!(
        store.outbound(&fleet.client).await.unwrap().len(),
        1,
        "a local-only truncate is not sufficient to retire a QoS2 ID"
    );
    drop(hub);
    drop(store);
    fleet.lose_owner();
    fleet
        .transport
        .drop_truncates
        .store(false, Ordering::SeqCst);
    let (successor, _) = fleet.owner("b");
    assert_eq!(
        successor.pending(&fleet.client, 0, 10).await.unwrap()[0].offset,
        offset,
        "the surviving quorum did not receive the truncate"
    );
    assert_released_restore(successor, &fleet.client, 7).await;
}

#[tokio::test]
async fn a_not_owner_truncate_keeps_the_id_for_the_successor() {
    let fleet = Fleet::new();
    let (store, placement) = fleet.owner("a");
    let (mut hub, _) = Hub::with_config(NodeId("a".into()), store.clone());
    delivery(&mut hub, &fleet.client, 7, QoS::ExactlyOnce).await;
    placement
        .write()
        .unwrap()
        .set_lease_owners(BTreeMap::from([(
            group_of(fleet.client.as_str()),
            NodeId("b".into()),
        )]));
    assert!(matches!(
        store.ack_durable(&fleet.client, 1).await,
        Err(StorageError::NotOwner)
    ));
    hub.pub_comp(&fleet.client, 7).await;
    assert!(
        !store.ops().iter().any(|(op, _)| op == "clear"),
        "a refused truncate must not attempt ID clearance"
    );
    drop(hub);
    drop(store);
    fleet.lose_owner();
    let (successor, _) = fleet.owner("b");
    assert_eq!(
        successor.outbound(&fleet.client).await.unwrap()[0].packet_id,
        7
    );
    assert_released_restore(successor, &fleet.client, 7).await;
}

#[tokio::test]
async fn a_failed_clear_after_quorum_truncation_recovers_as_an_orphan() {
    let fleet = Fleet::new();
    let (store, _) = fleet.owner("a");
    let (mut hub, _) = Hub::with_config(NodeId("a".into()), store.clone());
    delivery(&mut hub, &fleet.client, 7, QoS::ExactlyOnce).await;
    store
        .clear_failures
        .lock()
        .unwrap()
        .push_back(StorageError::NoQuorum);
    hub.pub_comp(&fleet.client, 7).await;
    assert!(store
        .pending(&fleet.client, 0, 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.outbound(&fleet.client).await.unwrap().len(), 1);
    drop(hub);
    drop(store);
    fleet.lose_owner();
    let (successor, _) = fleet.owner("b");
    assert!(
        successor
            .pending(&fleet.client, 0, 10)
            .await
            .unwrap()
            .is_empty(),
        "the durable truncate must survive losing its owner"
    );
    assert_released_restore(successor, &fleet.client, 7).await;
}
