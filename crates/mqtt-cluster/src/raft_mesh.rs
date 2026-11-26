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
//!
//! The payloads are the ones mqttd 1.3 (openraft 0.9) sends, so nodes of both
//! versions run one lease group during a rolling upgrade (ADR 0058). openraft 0.10
//! kept their layouts with two exceptions this module absorbs: a vote request
//! travels as [`VoteWire`] (0.9's two fields), and snapshots still go in chunks
//! through openraft-legacy's v1 network. Golden-bytes tests pin every request and
//! reply.

use crate::lease_group::LeaseRaft;
use crate::lease_raft::LeaseConfig;
use crate::lease_store::SnapshotData;
use crate::peer::PeerMessage;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::{NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use openraft::type_config::alias::{LogIdOf, VoteOf};
use openraft::{BasicNode, RaftNetworkFactory};
use openraft_legacy::network_v1::{
    Adapter, ChunkedSnapshotReceiver, InstallSnapshotError, InstallSnapshotRequest,
    InstallSnapshotResponse, RaftNetwork,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

type RaftNodeId = u64;

/// Maximum serialized size of one Raft RPC payload (request or reply), enforced
/// on **both** sides (ADR 0052): a receiver rejects an oversized inbound payload
/// before decoding, and a sender fails the send rather than shipping a payload
/// the receiver would reject. The enclosing peer frame allows 16 MiB; this
/// tighter bound reflects the actual content — `LeaseMap` snapshots and
/// append-entries batches are KiB-scale, so 4 MiB is generous headroom, not a
/// ceiling anyone should meet.
pub(crate) const MAX_RAFT_RPC: usize = 4 * 1024 * 1024;

/// How long a sender waits before it retries an unreachable node: openraft 0.9's
/// constant 500 ms, kept across the 0.10 upgrade (whose v1 default is 200 ms).
const RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Strict decode: the payload must be exactly one `T` (ADR 0052) and within
/// [`MAX_RAFT_RPC`].
fn strict_decode<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, String> {
    if payload.len() > MAX_RAFT_RPC {
        return Err(format!("raft rpc payload exceeds {MAX_RAFT_RPC} bytes"));
    }
    match postcard::take_from_bytes(payload) {
        Ok((msg, [])) => Ok(msg),
        Ok(_) => Err("trailing bytes after raft rpc payload".into()),
        Err(e) => Err(e.to_string()),
    }
}

/// A Raft RPC request, serialized into a [`PeerMessage::RaftRpc`] payload.
#[derive(Serialize, Deserialize)]
enum RpcRequest {
    AppendEntries(AppendEntriesRequest<LeaseConfig>),
    Vote(VoteWire),
    InstallSnapshot(InstallSnapshotRequest<LeaseConfig>),
}

/// A Raft RPC reply, serialized into a [`PeerMessage::RaftRpcReply`] payload.
#[derive(Serialize, Deserialize)]
enum RpcReply {
    AppendEntries(AppendEntriesResponse<LeaseConfig>),
    Vote(VoteResponse<LeaseConfig>),
    InstallSnapshot(InstallSnapshotResponse<LeaseConfig>),
    /// The remote raft returned an error (rendered); mapped to a network error so
    /// openraft retries.
    Err(String),
}

/// A vote request as mqttd 1.3 (openraft 0.9) encodes it: the vote and the
/// candidate's last log id.
///
/// openraft 0.10's [`VoteRequest`] adds `leadership_transfer_from`, a third field
/// that postcard, a positional format, cannot leave out: either version would
/// reject the other's request. Only a leadership transfer sets it, and this network
/// carries no transfer (the v1 `transfer_leader` reports the target unreachable,
/// as openraft 0.9 had none), so dropping it on send and reading `None` on receive
/// loses nothing.
#[derive(Serialize, Deserialize)]
struct VoteWire {
    vote: VoteOf<LeaseConfig>,
    last_log_id: Option<LogIdOf<LeaseConfig>>,
}

impl From<VoteRequest<LeaseConfig>> for VoteWire {
    fn from(rpc: VoteRequest<LeaseConfig>) -> Self {
        Self {
            vote: rpc.vote,
            last_log_id: rpc.last_log_id,
        }
    }
}

impl From<VoteWire> for VoteRequest<LeaseConfig> {
    fn from(wire: VoteWire) -> Self {
        VoteRequest::new(wire.vote, wire.last_log_id)
    }
}

/// Decode an inbound [`PeerMessage::RaftRpc`] payload, run it against the local
/// `raft`, and encode the reply payload for a [`PeerMessage::RaftRpcReply`].
///
/// This is the receive side — what the link handler calls on an inbound RPC. It is
/// the mirror of [`MeshRaftNetwork`]'s send side.
#[must_use]
pub async fn dispatch(raft: &LeaseRaft, payload: &[u8]) -> Vec<u8> {
    let reply = match strict_decode::<RpcRequest>(payload) {
        Ok(RpcRequest::AppendEntries(rpc)) => {
            report_contradicting_append(raft, &rpc);
            match raft.append_entries(rpc).await {
                // A follower accepts a request whole or not at all, which is what
                // 0.9's `Success` told the leader.
                Ok(r) => RpcReply::AppendEntries(r.into()),
                Err(e) => RpcReply::Err(e.to_string()),
            }
        }
        Ok(RpcRequest::Vote(rpc)) => match raft.vote(rpc.into()).await {
            Ok(r) => RpcReply::Vote(r),
            Err(e) => RpcReply::Err(e.to_string()),
        },
        Ok(RpcRequest::InstallSnapshot(rpc)) => match raft.install_snapshot(rpc).await {
            Ok(r) => RpcReply::InstallSnapshot(r),
            Err(e) => RpcReply::Err(e.to_string()),
        },
        Err(e) => RpcReply::Err(format!("undecodable raft rpc: {e}")),
    };
    postcard::to_allocvec(&reply).unwrap_or_default()
}

/// Whether a leader's `prev_log_id` contradicts this node's applied log (#754).
///
/// Every entry at or below `last_applied` is committed, and committed entries never
/// change, so a `prev_log_id` at such an index can name a leader no newer than the one
/// that wrote the applied entry. A newer one means some node's lease-Raft state went
/// backwards. openraft checks the same thing only as a `debug_assert` (against
/// `committed`, which is never behind `last_applied`), so a release build would accept
/// it silently and a debug build panics without saying who sent what.
fn prev_contradicts_applied(
    prev: Option<&LogIdOf<LeaseConfig>>,
    applied: Option<&LogIdOf<LeaseConfig>>,
) -> bool {
    match (prev, applied) {
        (Some(prev), Some(applied)) => {
            prev.index <= applied.index && prev.leader_id > applied.leader_id
        }
        _ => false,
    }
}

/// Log the evidence when an inbound `AppendEntries` contradicts the applied log (#754),
/// before openraft sees it. The condition is impossible in a correct cluster, so this
/// costs one metrics read per request and logs nothing.
fn report_contradicting_append(raft: &LeaseRaft, rpc: &AppendEntriesRequest<LeaseConfig>) {
    let metrics = raft.metrics();
    let m = metrics.borrow_watched();
    if prev_contradicts_applied(rpc.prev_log_id.as_ref(), m.last_applied.as_ref()) {
        tracing::error!(
            local = m.id,
            local_vote = %m.vote,
            request_vote = %rpc.vote,
            prev_log_id = ?rpc.prev_log_id,
            last_applied = ?m.last_applied,
            last_log_index = ?m.last_log_index,
            leader_commit = ?rpc.leader_commit,
            entries = rpc.entries.len(),
            membership = ?m.membership_config,
            "lease raft: a leader named a log id newer than this node's applied entry at \
             that index; some node's lease-Raft state went backwards (#754)"
        );
    }
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
        let payload = postcard::to_allocvec(req).map_err(|e| SendFail::Codec(e.to_string()))?;
        // Sender-side bound (mirrors peer::encode's rationale): an oversized
        // payload fails here instead of being rejected by the receiver.
        if payload.len() > MAX_RAFT_RPC {
            return Err(SendFail::Codec(format!(
                "raft rpc payload exceeds {MAX_RAFT_RPC} bytes"
            )));
        }
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
        strict_decode(&bytes).map_err(SendFail::Codec)
    }
}

/// Why a `send` could not produce a reply (pre-`RPCError`, so it is generic over the
/// per-RPC error type).
enum SendFail {
    Unreachable,
    Codec(String),
}

fn unreachable<E: std::error::Error>(target: RaftNodeId) -> RPCError<LeaseConfig, E> {
    RPCError::Unreachable(Unreachable::from_string(format!(
        "raft node {target} unreachable"
    )))
}

fn net_err<E: std::error::Error>(msg: &str) -> RPCError<LeaseConfig, E> {
    RPCError::Network(NetworkError::from_string(msg))
}

fn map_fail<E: std::error::Error>(target: RaftNodeId, fail: SendFail) -> RPCError<LeaseConfig, E> {
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
    /// The v1 connection behind openraft-legacy's adapter, which sends a snapshot in
    /// `InstallSnapshot` chunks, as openraft 0.9 did.
    type Network = Adapter<LeaseConfig, MeshConn, SnapshotData>;

    async fn new_client(&mut self, target: RaftNodeId, _node: &BasicNode) -> Self::Network {
        MeshConn {
            target,
            net: self.clone(),
        }
        .into_v2()
    }
}

impl RaftNetwork<LeaseConfig> for MeshConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<LeaseConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<LeaseConfig>, RPCError<LeaseConfig, RaftError<LeaseConfig>>>
    {
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

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<LeaseConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<LeaseConfig>,
        RPCError<LeaseConfig, RaftError<LeaseConfig, InstallSnapshotError>>,
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

    async fn vote(
        &mut self,
        rpc: VoteRequest<LeaseConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<LeaseConfig>, RPCError<LeaseConfig, RaftError<LeaseConfig>>> {
        match self
            .net
            .send(self.target, &RpcRequest::Vote(rpc.into()))
            .await
        {
            Ok(RpcReply::Vote(r)) => Ok(r),
            Ok(RpcReply::Err(m)) => Err(net_err(&m)),
            Ok(_) => Err(net_err("unexpected reply kind for vote")),
            Err(f) => Err(map_fail(self.target, f)),
        }
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(RETRY_BACKOFF))
    }
}

#[cfg(test)]
mod tests {
    use crate::lease_raft::testing::{committed_vote, hex, log_id, membership, unhex, vote};

    /// #754: a `prev_log_id` at or below the applied index is contradictory exactly
    /// when its leader is newer than the applied entry's.
    #[test]
    fn a_prev_log_id_newer_than_the_applied_entry_at_or_below_its_index_contradicts() {
        let applied = log_id(3, 1, 10);
        let check = |prev| super::prev_contradicts_applied(Some(&prev), Some(&applied));
        assert!(check(log_id(4, 2, 10)), "same index, newer leader");
        assert!(
            check(log_id(3, 2, 7)),
            "lower index, same term, newer leader node"
        );
        assert!(!check(log_id(3, 1, 10)), "the applied entry itself");
        assert!(
            !check(log_id(2, 9, 7)),
            "an older leader below the applied index"
        );
        assert!(
            !check(log_id(5, 1, 11)),
            "past the applied index, the log decides"
        );
        assert!(!super::prev_contradicts_applied(None, Some(&applied)));
        assert!(!super::prev_contradicts_applied(Some(&applied), None));
    }

    use super::{dispatch, strict_decode, MeshRaftNetwork, RpcReply, RpcRequest};
    use crate::lease_group::{config, LeaseRaft};
    use crate::lease_raft::{LeaseConfig, LeaseRecord, LeaseRequest};
    use crate::lease_store::LeaseStore;
    use crate::peer::{self, PeerMessage};
    use bytes::BytesMut;
    use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
    use openraft::type_config::alias::EntryOf;
    use openraft::{BasicNode, EntryPayload, Raft, ServerState, StoredMembership};
    use openraft_legacy::network_v1::{
        InstallSnapshotRequest, InstallSnapshotResponse, SnapshotMeta,
    };
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::mpsc;

    type NodeId = u64;

    // The payloads mqttd 1.3 (openraft 0.9.25, postcard 1.1.3) sent for these RPCs,
    // from a program built on those versions with 1.3's RpcRequest and RpcReply.
    const V13_APPEND: &str =
        "0003010101030107030301080100090103010902010201020201026e3102026e3203010a0001030106";
    const V13_VOTE: &str = "010402000103010a";
    const V13_VOTE_EMPTY_LOG: &str = "0101020000";
    const V13_SNAPSHOT: &str =
        "020402010103010a01030109010201020201026e3102026e320431302d31000301020301";
    const V13_APPEND_SUCCESS: &str = "0000";
    const V13_APPEND_PARTIAL: &str = "000101030109";
    const V13_APPEND_CONFLICT: &str = "0002";
    const V13_APPEND_HIGHER: &str = "0003050301";
    const V13_VOTE_REPLY: &str = "01040200010103010a";
    const V13_SNAPSHOT_REPLY: &str = "02040201";
    const V13_ERR: &str = "0304626f6f6d";

    fn requests() -> Vec<(&'static str, RpcRequest)> {
        let entries: Vec<EntryOf<LeaseConfig>> = vec![
            EntryOf::<LeaseConfig> {
                log_id: log_id(3, 1, 8),
                payload: EntryPayload::Normal(LeaseRequest::Assign { group: 9, node: 1 }),
            },
            EntryOf::<LeaseConfig> {
                log_id: log_id(3, 1, 9),
                payload: EntryPayload::Membership(membership()),
            },
            EntryOf::<LeaseConfig> {
                log_id: log_id(3, 1, 10),
                payload: EntryPayload::Blank,
            },
        ];
        vec![
            (
                V13_APPEND,
                RpcRequest::AppendEntries(AppendEntriesRequest {
                    vote: committed_vote(3, 1),
                    prev_log_id: Some(log_id(3, 1, 7)),
                    entries,
                    leader_commit: Some(log_id(3, 1, 6)),
                }),
            ),
            (
                V13_VOTE,
                RpcRequest::Vote(VoteRequest::new(vote(4, 2), Some(log_id(3, 1, 10))).into()),
            ),
            (
                V13_VOTE_EMPTY_LOG,
                RpcRequest::Vote(VoteRequest::new(vote(1, 2), None).into()),
            ),
            (
                V13_SNAPSHOT,
                RpcRequest::InstallSnapshot(InstallSnapshotRequest {
                    vote: committed_vote(4, 2),
                    meta: SnapshotMeta {
                        last_log_id: Some(log_id(3, 1, 10)),
                        last_membership: StoredMembership::new(Some(log_id(3, 1, 9)), membership()),
                        snapshot_id: "10-1".to_string(),
                    },
                    offset: 0,
                    data: vec![1, 2, 3],
                    done: true,
                }),
            ),
        ]
    }

    fn replies() -> Vec<(&'static str, RpcReply)> {
        vec![
            (
                V13_APPEND_SUCCESS,
                RpcReply::AppendEntries(AppendEntriesResponse::Success),
            ),
            (
                V13_APPEND_PARTIAL,
                RpcReply::AppendEntries(AppendEntriesResponse::PartialSuccess(Some(log_id(
                    3, 1, 9,
                )))),
            ),
            (
                V13_APPEND_CONFLICT,
                RpcReply::AppendEntries(AppendEntriesResponse::Conflict),
            ),
            (
                V13_APPEND_HIGHER,
                RpcReply::AppendEntries(AppendEntriesResponse::HigherVote(committed_vote(5, 3))),
            ),
            (
                V13_VOTE_REPLY,
                RpcReply::Vote(VoteResponse::new(vote(4, 2), Some(log_id(3, 1, 10)), true)),
            ),
            (
                V13_SNAPSHOT_REPLY,
                RpcReply::InstallSnapshot(InstallSnapshotResponse {
                    vote: committed_vote(4, 2),
                }),
            ),
            (V13_ERR, RpcReply::Err("boom".to_string())),
        ]
    }

    /// Every lease-group request and reply encodes to the bytes mqttd 1.3 sent, and
    /// decodes from them, so the two versions run one lease group during a rolling
    /// upgrade (ADR 0058).
    #[test]
    fn the_raft_rpcs_keep_the_1_3_wire() {
        for (golden, req) in requests() {
            assert_eq!(hex(&postcard::to_allocvec(&req).unwrap()), golden);
            let back: RpcRequest = strict_decode(&unhex(golden)).unwrap();
            assert_eq!(hex(&postcard::to_allocvec(&back).unwrap()), golden);
        }
        for (golden, reply) in replies() {
            assert_eq!(hex(&postcard::to_allocvec(&reply).unwrap()), golden);
            let back: RpcReply = strict_decode(&unhex(golden)).unwrap();
            assert_eq!(hex(&postcard::to_allocvec(&back).unwrap()), golden);
        }
    }

    /// A vote request from a 1.3 candidate is a plain vote here: it never authorizes
    /// a leadership transfer.
    #[test]
    fn a_1_3_vote_request_carries_no_leadership_transfer() {
        let Ok(RpcRequest::Vote(wire)) = strict_decode::<RpcRequest>(&unhex(V13_VOTE)) else {
            panic!("a vote request");
        };
        let rpc: VoteRequest<LeaseConfig> = wire.into();
        assert_eq!(rpc, VoteRequest::new(vote(4, 2), Some(log_id(3, 1, 10))));
        assert_eq!(rpc.leadership_transfer_from, None);
    }

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
        let raft = Raft::new(id, config(), net, store.clone(), store.clone())
            .await
            .unwrap();
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
        let resp = crate::lease_group::propose(&raft1, LeaseRequest::Assign { group: 9, node: 1 })
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

    /// A member that joins after the leader purged its log catches up from a
    /// snapshot, sent in `InstallSnapshot` chunks over the wire (openraft-legacy's v1
    /// transfer, the one mqttd 1.3 speaks) and assembled by the lease store.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_new_member_catches_up_from_a_snapshot_over_the_wire() {
        const WAIT: Option<Duration> = Some(Duration::from_secs(15));
        let net1 = MeshRaftNetwork::new();
        let net2 = MeshRaftNetwork::new();
        let (raft1, _store1) = start(net1.clone(), 1).await;
        raft1.initialize(members(&[1])).await.unwrap();
        raft1
            .wait(WAIT)
            .state(ServerState::Leader, "node 1 leads alone")
            .await
            .unwrap();

        let mut last = None;
        for group in 0..3 {
            let resp = crate::lease_group::propose(&raft1, LeaseRequest::Assign { group, node: 1 })
                .await
                .unwrap();
            last = Some(resp.log_id);
        }
        let last = last.unwrap();
        raft1.trigger().snapshot().await.unwrap();
        raft1
            .wait(WAIT)
            .snapshot(last, "a snapshot at the last lease")
            .await
            .unwrap();
        raft1.trigger().purge_log(last.index).await.unwrap();
        raft1
            .wait(WAIT)
            .purged(Some(last), "the log purged through the snapshot")
            .await
            .unwrap();

        // Node 2 starts only now: the log it would replay is gone.
        let (raft2, store2) = start(net2.clone(), 2).await;
        let (io1, io2) = tokio::io::duplex(256 * 1024);
        let (out1_tx, out1_rx) = mpsc::unbounded_channel();
        let (out2_tx, out2_rx) = mpsc::unbounded_channel();
        net1.register(2, out1_tx.clone());
        net2.register(1, out2_tx.clone());
        spawn_link(raft1.clone(), net1.clone(), io1, out1_tx, out1_rx);
        spawn_link(raft2.clone(), net2.clone(), io2, out2_tx, out2_rx);

        raft1
            .add_learner(2, BasicNode::default(), true)
            .await
            .unwrap();
        raft2
            .wait(WAIT)
            .applied_index_at_least(Some(last.index), "the learner installed the snapshot")
            .await
            .unwrap();
        for group in 0..3 {
            assert_eq!(store2.current_lease(group).map(|l| l.holder), Some(1));
        }

        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
    }
}
