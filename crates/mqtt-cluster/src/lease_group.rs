//! Bringing up the openraft lease consensus group
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md), workstream E
//! step 3b-ii — network + bring-up).
//!
//! This wires [`LeaseStore`](crate::lease_store::LeaseStore) and the
//! [`LeaseConfig`](crate::lease_raft::LeaseConfig) binding into a runnable openraft
//! group: a tuned [`config`] and the [`LeaseRaft`] handle type. The
//! [`RaftNetwork`](openraft_legacy::network_v1::RaftNetwork) that carries openraft's RPCs between nodes
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

use crate::lease_raft::{LeaseConfig, LeaseRequest};
use crate::lease_store::LeaseStore;
use openraft::errors::{ChangeMembershipError, ClientWriteError, ForwardReason, RaftError};
use openraft::raft::ClientWriteResponse;
use openraft::Config;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// The openraft handle type for the lease group: its state machine is the
/// [`LeaseStore`], which is also its log store.
pub type LeaseRaft = openraft::Raft<LeaseConfig, LeaseStore>;

/// What a lease-group write (`client_write`, `add_learner`, `change_membership`)
/// fails with.
pub type WriteError = RaftError<LeaseConfig, ClientWriteError<LeaseConfig>>;

const HEARTBEAT_MS: u64 = 500;
const ELECTION_TIMEOUT_MIN_MS: u64 = 1500;
const ELECTION_TIMEOUT_MAX_MS: u64 = 3000;

/// How long a write waits for this leader's lease (see [`leased`]): the longest
/// election timeout, the time a quorum has to answer a leader before a follower
/// campaigns against it.
const LEASE_WAIT: Duration = Duration::from_millis(ELECTION_TIMEOUT_MAX_MS);
const LEASE_POLL: Duration = Duration::from_millis(20);

/// Run a lease-group write, waiting out a leader that is not ready to take it yet.
///
/// openraft 0.10 refuses a write on a leader that no quorum has acknowledged within
/// the lease period (`ForwardReason::LeaseExpired`): every leader from its election
/// until its first append is answered, and a leader whose acks stopped. It also
/// refuses a membership change until the leader's first entry of its term has
/// committed (`UncommittedLeaderLog`). openraft 0.9 accepted both writes and
/// committed them once the quorum answered. This retries those refusals on this
/// node, as openraft advises, for up to [`LEASE_WAIT`]. Any other result returns
/// at once, including the refusal of a node that is no longer the leader.
///
/// # Errors
/// What the write returns; a leader still not ready after [`LEASE_WAIT`] as that
/// refusal.
pub async fn leased<T, F, Fut>(mut write: F) -> Result<T, WriteError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, WriteError>>,
{
    let deadline = tokio::time::Instant::now() + LEASE_WAIT;
    loop {
        match write().await {
            Err(e) if leader_not_ready(&e) && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(LEASE_POLL).await;
            }
            result => return result,
        }
    }
}

/// Whether `e` is a leader's refusal that clears once a quorum has answered it.
fn leader_not_ready(e: &WriteError) -> bool {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
            f.reason == ForwardReason::LeaseExpired
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::UncommittedLeaderLog(_),
        )) => true,
        _ => false,
    }
}

/// Propose `req` to the lease group, through [`leased`].
///
/// # Errors
/// As [`leased`]: this node is not the leader, its lease stayed unconfirmed, or
/// the group failed.
pub async fn propose(
    raft: &LeaseRaft,
    req: LeaseRequest,
) -> Result<ClientWriteResponse<LeaseConfig>, WriteError> {
    leased(|| raft.client_write(req.clone())).await
}

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
            heartbeat_interval: HEARTBEAT_MS,
            election_timeout_min: ELECTION_TIMEOUT_MIN_MS,
            election_timeout_max: ELECTION_TIMEOUT_MAX_MS,
            ..Default::default()
        }
        .validate()
        .expect("lease raft config is valid"),
    )
}

#[cfg(test)]
mod tests {
    use super::{config, propose, LeaseRaft};
    use crate::lease_raft::testing::log_id;
    use crate::lease_raft::{LeaseConfig, LeaseRecord, LeaseRequest, LeaseResponse};
    use crate::lease_store::{LeaseStore, SnapshotData};
    use openraft::errors::{
        ChangeMembershipError, ClientWriteError, ForwardReason, ForwardToLeader, RPCError,
        RaftError, RemoteError, UncommittedLeaderLog, Unreachable,
    };
    use openraft::network::RPCOption;
    use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
    use openraft::{BasicNode, Raft, RaftNetworkFactory, ServerState};
    use openraft_legacy::network_v1::{
        Adapter, ChunkedSnapshotReceiver, InstallSnapshotError, InstallSnapshotRequest,
        InstallSnapshotResponse, RaftNetwork,
    };
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    type NodeId = u64;
    type NetErr = RPCError<LeaseConfig, RaftError<LeaseConfig>>;

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
    fn unreachable<E: std::error::Error>(id: NodeId) -> RPCError<LeaseConfig, E> {
        RPCError::Unreachable(Unreachable::from_string(format!(
            "node {id} not registered"
        )))
    }

    /// A connection to one target node (a handle into the router).
    struct Conn {
        target: NodeId,
        router: Router,
    }

    impl RaftNetworkFactory<LeaseConfig> for Router {
        type Network = Adapter<LeaseConfig, Conn, SnapshotData>;
        async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
            Conn {
                target,
                router: self.clone(),
            }
            .into_v2()
        }
    }

    impl RaftNetwork<LeaseConfig> for Conn {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<LeaseConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<LeaseConfig>, NetErr> {
            let raft = self
                .router
                .get(self.target)
                .ok_or_else(|| unreachable(self.target))?;
            raft.append_entries(rpc)
                .await
                .map(AppendEntriesResponse::from)
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        }

        async fn vote(
            &mut self,
            rpc: VoteRequest<LeaseConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<LeaseConfig>, NetErr> {
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
            InstallSnapshotResponse<LeaseConfig>,
            RPCError<LeaseConfig, RaftError<LeaseConfig, InstallSnapshotError>>,
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
        let raft = Raft::new(id, config(), router.clone(), store.clone(), store.clone())
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

        let resp = propose(&raft, LeaseRequest::Assign { group: 7, node: 1 })
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
        let resp = propose(&rafts[&1], LeaseRequest::Assign { group: 5, node: 2 })
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

    fn not_leased() -> super::WriteError {
        RaftError::APIError(ClientWriteError::ForwardToLeader(
            ForwardToLeader::empty().with_reason(ForwardReason::LeaseExpired),
        ))
    }

    /// openraft 0.10 refuses writes on a leader that no quorum has acknowledged yet,
    /// and membership changes until its first entry commits; a lease-group write
    /// waits both out, as openraft 0.9 queued such writes.
    #[tokio::test(start_paused = true)]
    async fn a_write_waits_until_the_leader_is_ready() {
        let uncommitted = RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::UncommittedLeaderLog(UncommittedLeaderLog {
                committed: None,
                leader_log_id: log_id(2, 1, 5),
            }),
        ));
        let mut refusals = vec![not_leased(), uncommitted, not_leased()];
        let attempts = Cell::new(0);
        let written = super::leased(|| {
            attempts.set(attempts.get() + 1);
            let refusal = refusals.pop();
            async move { refusal.map_or(Ok(7), Err) }
        })
        .await;
        assert_eq!(written.unwrap(), 7);
        assert_eq!(attempts.get(), 4);
    }

    /// Any other refusal returns at once: a write on a follower does not wait.
    #[tokio::test(start_paused = true)]
    async fn a_write_on_a_follower_fails_at_once() {
        let attempts = Cell::new(0);
        let written: Result<(), _> = super::leased(|| {
            attempts.set(attempts.get() + 1);
            async {
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(
                    ForwardToLeader::new(2, BasicNode::default()),
                )))
            }
        })
        .await;
        assert!(written.is_err());
        assert_eq!(attempts.get(), 1);
    }

    /// A leader that no quorum answers fails the write after one election timeout.
    #[tokio::test(start_paused = true)]
    async fn a_lease_that_never_comes_fails_the_write_after_the_wait() {
        let started = tokio::time::Instant::now();
        let written: Result<(), _> = super::leased(|| async { Err(not_leased()) }).await;
        assert!(matches!(
            written,
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))
        ));
        let waited = started.elapsed();
        assert!(
            waited >= super::LEASE_WAIT && waited < super::LEASE_WAIT + Duration::from_secs(1),
            "{waited:?}"
        );
    }
}
