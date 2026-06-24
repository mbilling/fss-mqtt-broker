//! The lease-group `RaftNetwork` over the peer mesh
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md), workstream E
//! step 3b-ii — mesh network).
//!
//! Step 3b-ii's bring-up proved openraft elects and replicates a lease over an
//! in-memory router. This module carries the same RPCs (append-entries / vote /
//! install-snapshot) over the **mTLS peer bus**, so a real cluster runs the lease
//! group. It mirrors [`repl_net`](crate::repl_net): the sender ships a
//! [`PeerMessage::RaftRpc`] and awaits the [`PeerMessage::RaftRpcReply`], correlated
//! by `req_id`; a dropped link fails in-flight RPCs rather than hanging.
//!
//! [`MeshRaftNetwork`] is keyed by the numeric `RaftNodeId`; mapping the cluster's
//! string [`NodeId`](crate::NodeId) to it (and registering each peer's link channel)
//! is the live-hub wiring (workstream E step 4). Here the three handles — register a
//! peer, route a reply ([`complete_reply`](MeshRaftNetwork::complete_reply)), drop a
//! peer ([`fail_node`](MeshRaftNetwork::fail_node)) — and the receive-side
//! [`dispatch`] are driven directly so the over-the-wire consensus is pinned by a
//! test that elects a leader and commits a lease across two nodes through serialized
//! frames.

use crate::lease_group::LeaseRaft;
use crate::lease_raft::LeaseConfig;
use crate::peer::PeerMessage;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

type RaftNodeId = u64;

/// A Raft RPC request, serialized into a [`PeerMessage::RaftRpc`] payload.
#[derive(Serialize, Deserialize)]
enum RpcRequest {
    AppendEntries(AppendEntriesRequest<LeaseConfig>),
    Vote(VoteRequest<RaftNodeId>),
    InstallSnapshot(InstallSnapshotRequest<LeaseConfig>),
}

/// A Raft RPC reply, serialized into a [`PeerMessage::RaftRpcReply`] payload.
#[derive(Serialize, Deserialize)]
enum RpcReply {
    AppendEntries(AppendEntriesResponse<RaftNodeId>),
    Vote(VoteResponse<RaftNodeId>),
    InstallSnapshot(InstallSnapshotResponse<RaftNodeId>),
    /// The remote raft returned an error (rendered); mapped to a network error so
    /// openraft retries.
    Err(String),
}

/// Decode an inbound [`PeerMessage::RaftRpc`] payload, run it against the local
/// `raft`, and encode the reply payload for a [`PeerMessage::RaftRpcReply`].
///
/// This is the receive side — what the link handler calls on an inbound RPC. It is
/// the mirror of [`MeshRaftNetwork`]'s send side.
#[must_use]
pub async fn dispatch(raft: &LeaseRaft, payload: &[u8]) -> Vec<u8> {
    let reply = match bincode::deserialize::<RpcRequest>(payload) {
        Ok(RpcRequest::AppendEntries(rpc)) => match raft.append_entries(rpc).await {
            Ok(r) => RpcReply::AppendEntries(r),
            Err(e) => RpcReply::Err(e.to_string()),
        },
        Ok(RpcRequest::Vote(rpc)) => match raft.vote(rpc).await {
            Ok(r) => RpcReply::Vote(r),
            Err(e) => RpcReply::Err(e.to_string()),
        },
        Ok(RpcRequest::InstallSnapshot(rpc)) => match raft.install_snapshot(rpc).await {
            Ok(r) => RpcReply::InstallSnapshot(r),
            Err(e) => RpcReply::Err(e.to_string()),
        },
        Err(e) => RpcReply::Err(format!("undecodable raft rpc: {e}")),
    };
    bincode::serialize(&reply).unwrap_or_default()
}

struct Pending {
    node: RaftNodeId,
    reply: oneshot::Sender<Vec<u8>>,
}

#[derive(Default)]
struct Inner {
    peers: HashMap<RaftNodeId, mpsc::UnboundedSender<PeerMessage>>,
    pending: HashMap<u64, Pending>,
}

#[derive(Default)]
struct Shared {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
}

/// A leader-side [`RaftNetworkFactory`]/[`RaftNetwork`] that carries lease-group RPCs
/// over the peer mesh. Cheaply cloneable: openraft owns one clone (the factory), the
/// wiring layer holds another to register peers and route replies. See module docs.
#[derive(Clone, Default)]
pub struct MeshRaftNetwork {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for MeshRaftNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshRaftNetwork").finish_non_exhaustive()
    }
}

impl MeshRaftNetwork {
    /// An empty network with no peers registered.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.shared
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Register (or replace) the outbound link channel for a peer raft node.
    pub fn register(&self, node: RaftNodeId, tx: mpsc::UnboundedSender<PeerMessage>) {
        self.lock().peers.insert(node, tx);
    }

    /// Whether a peer's raft link is currently registered — i.e. an RPC to it can be
    /// delivered. Used to gate lease-group voter admission on link readiness (ADR 0028):
    /// adding a voter the leader cannot yet reach would lose quorum until the mesh
    /// converges, churning the group through repeated elections during bring-up.
    #[must_use]
    pub fn is_connected(&self, node: RaftNodeId) -> bool {
        self.lock().peers.contains_key(&node)
    }

    /// Route an inbound [`PeerMessage::RaftRpcReply`] to its waiting RPC.
    pub fn complete_reply(&self, req_id: u64, payload: Vec<u8>) {
        if let Some(p) = self.lock().pending.remove(&req_id) {
            let _ = p.reply.send(payload);
        }
    }

    /// Drop a peer and fail its in-flight RPCs (a dropped link cannot reply).
    pub fn fail_node(&self, node: RaftNodeId) {
        let mut inner = self.lock();
        inner.peers.remove(&node);
        let failed: Vec<u64> = inner
            .pending
            .iter()
            .filter(|(_, p)| p.node == node)
            .map(|(id, _)| *id)
            .collect();
        for id in failed {
            inner.pending.remove(&id);
        }
    }

    /// Send an RPC to `target` and await its reply payload.
    async fn send(&self, target: RaftNodeId, req: &RpcRequest) -> Result<RpcReply, SendFail> {
        let payload = bincode::serialize(req).map_err(|e| SendFail::Codec(e.to_string()))?;
        let req_id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut inner = self.lock();
            let Some(sender) = inner.peers.get(&target).cloned() else {
                return Err(SendFail::Unreachable);
            };
            inner.pending.insert(
                req_id,
                Pending {
                    node: target,
                    reply: reply_tx,
                },
            );
            if sender
                .send(PeerMessage::RaftRpc { req_id, payload })
                .is_err()
            {
                inner.pending.remove(&req_id);
                return Err(SendFail::Unreachable);
            }
        }
        let bytes = reply_rx.await.map_err(|_| SendFail::Unreachable)?;
        bincode::deserialize(&bytes).map_err(|e| SendFail::Codec(e.to_string()))
    }
}

/// Why a `send` could not produce a reply (pre-`RPCError`, so it is generic over the
/// per-RPC error type).
enum SendFail {
    Unreachable,
    Codec(String),
}

fn unreachable<E: std::error::Error>(target: RaftNodeId) -> RPCError<RaftNodeId, BasicNode, E> {
    let e = std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("raft node {target} unreachable"),
    );
    RPCError::Unreachable(Unreachable::new(&e))
}

fn net_err<E: std::error::Error>(msg: &str) -> RPCError<RaftNodeId, BasicNode, E> {
    let e = std::io::Error::other(msg.to_string());
    RPCError::Network(NetworkError::new(&e))
}

fn map_fail<E: std::error::Error>(
    target: RaftNodeId,
    fail: SendFail,
) -> RPCError<RaftNodeId, BasicNode, E> {
    match fail {
        SendFail::Unreachable => unreachable(target),
        SendFail::Codec(m) => net_err(&m),
    }
}

/// A connection to one target node.
pub struct MeshConn {
    target: RaftNodeId,
    net: MeshRaftNetwork,
}

impl std::fmt::Debug for MeshConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshConn")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl RaftNetworkFactory<LeaseConfig> for MeshRaftNetwork {
    type Network = MeshConn;
    async fn new_client(&mut self, target: RaftNodeId, _node: &BasicNode) -> MeshConn {
        MeshConn {
            target,
            net: self.clone(),
        }
    }
}

impl RaftNetwork<LeaseConfig> for MeshConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<LeaseConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
        match self
            .net
            .send(self.target, &RpcRequest::AppendEntries(rpc))
            .await
        {
            Ok(RpcReply::AppendEntries(r)) => Ok(r),
            Ok(RpcReply::Err(m)) => Err(net_err(&m)),
            Ok(_) => Err(net_err("unexpected reply kind for append_entries")),
            Err(f) => Err(map_fail(self.target, f)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        match self.net.send(self.target, &RpcRequest::Vote(rpc)).await {
            Ok(RpcReply::Vote(r)) => Ok(r),
            Ok(RpcReply::Err(m)) => Err(net_err(&m)),
            Ok(_) => Err(net_err("unexpected reply kind for vote")),
            Err(f) => Err(map_fail(self.target, f)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<LeaseConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
    > {
        match self
            .net
            .send(self.target, &RpcRequest::InstallSnapshot(rpc))
            .await
        {
            Ok(RpcReply::InstallSnapshot(r)) => Ok(r),
            Ok(RpcReply::Err(m)) => Err(net_err(&m)),
            Ok(_) => Err(net_err("unexpected reply kind for install_snapshot")),
            Err(f) => Err(map_fail(self.target, f)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{dispatch, MeshRaftNetwork};
    use crate::lease_group::{config, LeaseRaft};
    use crate::lease_raft::{LeaseRecord, LeaseRequest};
    use crate::lease_store::LeaseStore;
    use crate::peer::{self, PeerMessage};
    use bytes::BytesMut;
    use openraft::storage::Adaptor;
    use openraft::{BasicNode, Raft, ServerState};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::mpsc;

    type NodeId = u64;

    async fn read_frame(
        rh: &mut (impl tokio::io::AsyncRead + Unpin),
        buf: &mut BytesMut,
    ) -> Option<PeerMessage> {
        loop {
            if let Ok(Some(msg)) = peer::decode(buf) {
                return Some(msg);
            }
            if rh.read_buf(buf).await.ok()? == 0 {
                return None;
            }
        }
    }

    /// Wire a node's link to one peer: drain its outbound channel to the wire, and
    /// on the inbound side dispatch `RaftRpc` requests to the local raft (replying
    /// via the same outbound) and route `RaftRpcReply` back into the network.
    fn spawn_link(
        raft: LeaseRaft,
        net: MeshRaftNetwork,
        io: DuplexStream,
        out_tx: mpsc::UnboundedSender<PeerMessage>,
        mut out_rx: mpsc::UnboundedReceiver<PeerMessage>,
    ) {
        let (mut rh, mut wh) = tokio::io::split(io);
        tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                let mut bytes = Vec::new();
                peer::encode(&msg, &mut bytes).unwrap();
                if wh.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });
        tokio::spawn(async move {
            let mut buf = BytesMut::new();
            while let Some(msg) = read_frame(&mut rh, &mut buf).await {
                match msg {
                    PeerMessage::RaftRpc { req_id, payload } => {
                        let reply = dispatch(&raft, &payload).await;
                        let _ = out_tx.send(PeerMessage::RaftRpcReply {
                            req_id,
                            payload: reply,
                        });
                    }
                    PeerMessage::RaftRpcReply { req_id, payload } => {
                        net.complete_reply(req_id, payload);
                    }
                    _ => {}
                }
            }
        });
    }

    async fn start(net: MeshRaftNetwork, id: NodeId) -> (LeaseRaft, LeaseStore) {
        let store = LeaseStore::new();
        let (log_store, sm) = Adaptor::new(store.clone());
        let raft = Raft::new(id, config(), net, log_store, sm).await.unwrap();
        (raft, store)
    }

    fn members(ids: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
        ids.iter().map(|id| (*id, BasicNode::default())).collect()
    }

    /// The mesh milestone: two openraft nodes, each driving the other only through
    /// **serialized `RaftRpc` frames over a duplex link**, elect a leader and commit a
    /// lease that replicates to both — real consensus over the wire.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_nodes_elect_and_replicate_over_the_wire() {
        let net1 = MeshRaftNetwork::new();
        let net2 = MeshRaftNetwork::new();
        let (raft1, store1) = start(net1.clone(), 1).await;
        let (raft2, store2) = start(net2.clone(), 2).await;

        // One bidirectional link between the two nodes.
        let (io1, io2) = tokio::io::duplex(256 * 1024);
        let (out1_tx, out1_rx) = mpsc::unbounded_channel();
        let (out2_tx, out2_rx) = mpsc::unbounded_channel();
        // node 1 reaches node 2 via out1; node 2 reaches node 1 via out2.
        net1.register(2, out1_tx.clone());
        net2.register(1, out2_tx.clone());
        spawn_link(raft1.clone(), net1.clone(), io1, out1_tx, out1_rx);
        spawn_link(raft2.clone(), net2.clone(), io2, out2_tx, out2_rx);

        // Bring up the cluster from node 1.
        raft1.initialize(members(&[1, 2])).await.unwrap();
        raft1
            .wait(Some(Duration::from_secs(15)))
            .state(ServerState::Leader, "node 1 leads via wire votes")
            .await
            .unwrap();

        // Commit a lease through the leader; it replicates to node 2 over the wire.
        let resp = raft1
            .client_write(LeaseRequest::Assign { group: 9, node: 1 })
            .await
            .unwrap();
        let epoch = resp.data.unwrap().epoch;

        for (raft, store) in [(&raft1, &store1), (&raft2, &store2)] {
            raft.wait(Some(Duration::from_secs(15)))
                .applied_index_at_least(Some(resp.log_id.index), "applied over the wire")
                .await
                .unwrap();
            assert_eq!(
                store.current_lease(9),
                Some(LeaseRecord { holder: 1, epoch })
            );
        }

        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
    }
}
