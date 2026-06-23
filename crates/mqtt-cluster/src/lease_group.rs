//! Bringing up the openraft lease consensus group
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md), workstream E
//! step 3b-ii — network + bring-up).
//!
//! This wires [`LeaseStore`](crate::lease_store::LeaseStore) and the
//! [`LeaseConfig`](crate::lease_raft::LeaseConfig) binding into a runnable openraft
//! group: a tuned [`config`] and the [`LeaseRaft`] handle type. The
//! [`RaftNetwork`](openraft::RaftNetwork) that carries openraft's RPCs between nodes
//! is implemented in two forms:
//!
//! - an **in-memory router** (in the tests) that forwards RPCs to in-process node
//!   handles — enough to bring up a real, multi-node group and prove leader election
//!   and lease replication end to end through openraft;
//! - the **peer-mesh network** (the next sub-step) that carries the same RPCs over
//!   the mTLS bus, mapping the cluster's string [`NodeId`](crate::NodeId) to the
//!   numeric `RaftNodeId`.
//!
//! The tests here are the milestone the spike pointed at: openraft electing a leader
//! and committing an [`Assign`](crate::lease_raft::LeaseRequest::Assign) into our
//! [`LeaseMap`](crate::lease_raft::LeaseMap), replicated to every replica.

use crate::lease_raft::LeaseConfig;
use openraft::Config;
use std::sync::Arc;

/// The openraft handle type for the lease group.
pub type LeaseRaft = openraft::Raft<LeaseConfig>;

/// A tuned openraft [`Config`] for the lease group: a small, low-traffic group, so
/// a brisk heartbeat and short election timeouts give fast failover without much
/// chatter.
///
/// # Panics
/// Panics if the (constant) configuration is invalid — a programming error.
#[must_use]
pub fn config() -> Arc<Config> {
    Arc::new(
        // Timing is budgeted for a fsync-on-commit persistent store (ADR 0026): every
        // raft write (`save_vote`/`append_to_log`) durably commits before it returns, which
        // costs tens of milliseconds on real disk. The heartbeat and leader-lease window
        // must comfortably exceed that, or the leader cannot sustain its lease across slow
        // commits and the group re-elects continuously. These values keep a stable leader
        // on disk; the cost is failover detection in ~1.5–3s (fine for durable takeover,
        // which already rebuilds a committed log over seconds). Safety is independent of
        // these timeouts.
        Config {
            cluster_name: "mqttd-lease".to_string(),
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        }
        .validate()
        .expect("lease raft config is valid"),
    )
}

#[cfg(test)]
mod tests {
    use super::{config, LeaseRaft};
    use crate::lease_raft::{LeaseConfig, LeaseRecord, LeaseRequest, LeaseResponse};
    use crate::lease_store::LeaseStore;
    use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
    use openraft::network::RPCOption;
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::storage::Adaptor;
    use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory, ServerState};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    type NodeId = u64;
    type NetErr = RPCError<NodeId, BasicNode, RaftError<NodeId>>;

    /// An in-memory network: forwards openraft RPCs to in-process node handles.
    #[derive(Clone, Default)]
    struct Router {
        nodes: Arc<Mutex<BTreeMap<NodeId, LeaseRaft>>>,
    }

    impl Router {
        fn register(&self, id: NodeId, raft: LeaseRaft) {
            self.nodes.lock().unwrap().insert(id, raft);
        }

        fn get(&self, id: NodeId) -> Option<LeaseRaft> {
            self.nodes.lock().unwrap().get(&id).cloned()
        }
    }

    /// Build an `Unreachable` RPC error for a missing target (generic over the RPC's
    /// error type, so it serves all three RPCs).
    fn unreachable<E: std::error::Error>(id: NodeId) -> RPCError<NodeId, BasicNode, E> {
        let e = std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("node {id} not registered"),
        );
        RPCError::Unreachable(Unreachable::new(&e))
    }

    /// A connection to one target node (a handle into the router).
    struct Conn {
        target: NodeId,
        router: Router,
    }

    impl RaftNetworkFactory<LeaseConfig> for Router {
        type Network = Conn;
        async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Conn {
            Conn {
                target,
                router: self.clone(),
            }
        }
    }

    impl RaftNetwork<LeaseConfig> for Conn {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<LeaseConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<NodeId>, NetErr> {
            let raft = self
                .router
                .get(self.target)
                .ok_or_else(|| unreachable(self.target))?;
            raft.append_entries(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        }

        async fn vote(
            &mut self,
            rpc: VoteRequest<NodeId>,
            _option: RPCOption,
        ) -> Result<VoteResponse<NodeId>, NetErr> {
            let raft = self
                .router
                .get(self.target)
                .ok_or_else(|| unreachable(self.target))?;
            raft.vote(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        }

        async fn install_snapshot(
            &mut self,
            rpc: InstallSnapshotRequest<LeaseConfig>,
            _option: RPCOption,
        ) -> Result<
            InstallSnapshotResponse<NodeId>,
            RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
        > {
            let raft = self
                .router
                .get(self.target)
                .ok_or_else(|| unreachable(self.target))?;
            raft.install_snapshot(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        }
    }

    /// Bring up node `id` against `router`, returning its handle and its store.
    async fn start(router: &Router, id: NodeId) -> (LeaseRaft, LeaseStore) {
        let store = LeaseStore::new();
        let (log_store, state_machine) = Adaptor::new(store.clone());
        let raft = Raft::new(id, config(), router.clone(), log_store, state_machine)
            .await
            .expect("raft node starts");
        router.register(id, raft.clone());
        (raft, store)
    }

    fn members(ids: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
        ids.iter().map(|id| (*id, BasicNode::default())).collect()
    }

    const TIMEOUT: Option<Duration> = Some(Duration::from_secs(10));

    /// A single-node group elects itself and commits a lease into the state
    /// machine — the full openraft stack (storage + network + state machine) end to
    /// end, deterministically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_group_elects_and_commits_a_lease() {
        let router = Router::default();
        let (raft, store) = start(&router, 1).await;
        raft.initialize(members(&[1])).await.unwrap();
        raft.wait(TIMEOUT)
            .state(ServerState::Leader, "sole node becomes leader")
            .await
            .unwrap();

        let resp = raft
            .client_write(LeaseRequest::Assign { group: 7, node: 1 })
            .await
            .unwrap();
        assert_eq!(
            resp.data,
            Some(LeaseResponse {
                group: 7,
                holder: 1,
                epoch: 1
            })
        );
        assert_eq!(
            store.current_lease(7),
            Some(LeaseRecord {
                holder: 1,
                epoch: 1
            })
        );
        raft.shutdown().await.unwrap();
    }

    /// A three-node group elects a leader and replicates a committed lease to every
    /// replica — split-brain-safe ownership, through real consensus.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_group_replicates_a_committed_lease() {
        let router = Router::default();
        let mut stores = BTreeMap::new();
        let mut rafts = BTreeMap::new();
        for id in [1, 2, 3] {
            let (raft, store) = start(&router, id).await;
            rafts.insert(id, raft);
            stores.insert(id, store);
        }

        // Initialize the cluster from node 1; a leader emerges via vote RPCs.
        rafts[&1].initialize(members(&[1, 2, 3])).await.unwrap();
        rafts[&1]
            .wait(TIMEOUT)
            .state(ServerState::Leader, "node 1 leads")
            .await
            .unwrap();

        // Commit a lease via the leader.
        let resp = rafts[&1]
            .client_write(LeaseRequest::Assign { group: 5, node: 2 })
            .await
            .unwrap();
        let epoch = resp.data.unwrap().epoch;

        // Every replica applies and converges on the same lease.
        for id in [1, 2, 3] {
            rafts[&id]
                .wait(TIMEOUT)
                .applied_index_at_least(Some(resp.log_id.index), "replica applied the lease")
                .await
                .unwrap();
            assert_eq!(
                stores[&id].current_lease(5),
                Some(LeaseRecord { holder: 2, epoch }),
                "node {id} converged on the committed lease",
            );
        }

        for id in [1, 2, 3] {
            rafts[&id].shutdown().await.unwrap();
        }
    }
}
