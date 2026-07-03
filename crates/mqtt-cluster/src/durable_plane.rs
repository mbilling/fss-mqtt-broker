//! The node's durable-plane endpoint: consensus + replication over the peer mesh
//! ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md) §4, workstream
//! E step 4c).
//!
//! A node participates in the durable plane in three roles, all riding the single
//! peer link per pair:
//!
//! - **consensus member** — its lease-group [`Raft`](openraft::Raft) handle, reached
//!   over the [`MeshRaftNetwork`];
//! - **replication follower** — a [`ReplicaState`] holding its copy of the session
//!   logs it replicates for other groups' owners;
//! - **replication leader** — a [`PeerReplicaTransport`], when it owns a group and
//!   quorum-appends that group's session logs.
//!
//! [`DurablePlane`] bundles those handles and exposes the three things a peer link
//! needs: [`register`](DurablePlane::register) a peer's outbound channel on connect,
//! [`fail`](DurablePlane::fail) it on disconnect, and [`handle`](DurablePlane::handle)
//! an inbound cluster frame (returning the reply to send back). The link does the
//! I/O; the plane does the routing — so the consensus/replication plane stays off
//! the hub actor's serial command loop. Wiring it into `mqttd`'s peer pump is the
//! final integration step (4f).

use crate::cluster_log::{ReplOp, ReplicaState};
use crate::lease::Epoch;
use crate::lease_group::LeaseRaft;
use crate::node_registry::raft_id;
use crate::peer::PeerMessage;
use crate::raft_mesh::{dispatch, MeshRaftNetwork};
use crate::repl_net::PeerReplicaTransport;
use crate::NodeId;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

/// One queued replica write: the holder's `op` at `epoch`, plus a one-shot to return
/// whether it was accepted (durably applied / not fenced) to the waiting frame.
type ReplicaWrite = (Epoch, ReplOp, oneshot::Sender<bool>);

/// A node's endpoint on the durable plane: its lease-group consensus handle, its
/// replication-follower state, and its replication-leader transport, plus the routing
/// for inbound peer frames. Cheap to clone (all handles are shared).
#[derive(Clone)]
pub struct DurablePlane {
    raft: LeaseRaft,
    network: MeshRaftNetwork,
    transport: Arc<PeerReplicaTransport>,
    replicas: Arc<Mutex<ReplicaState>>,
    /// Sender into the single **replica-writer** task (ADR 0027): inbound `Replicate`
    /// frames hand their op here instead of each fsyncing on its own, so the writer can
    /// group-commit a burst into one transaction. The recovery-read path still locks
    /// `replicas` directly (between batches).
    replica_tx: mpsc::UnboundedSender<ReplicaWrite>,
    /// Aborts the replica-writer when the **last** plane clone drops. The writer holds a
    /// clone of `replicas` (hence of the persistent `replicas.redb` handle); without this,
    /// the writer outlives the plane and the file lock is never released, so a restart
    /// over the same data dir cannot reopen it (ADR 0018 phase 5 / ADR 0019 shutdown).
    _writer: Arc<AbortOnDrop>,
}

/// Aborts a spawned task when dropped — used to bound the replica-writer's lifetime to the
/// plane's (so its `replicas` clone, and the redb handle inside it, are released on drop).
struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl std::fmt::Debug for DurablePlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurablePlane").finish_non_exhaustive()
    }
}

impl DurablePlane {
    /// Assemble the plane from its handles. `network` must be the same
    /// [`MeshRaftNetwork`] `raft` was built with (so replies route back to it).
    #[must_use]
    pub fn new(
        raft: LeaseRaft,
        network: MeshRaftNetwork,
        transport: Arc<PeerReplicaTransport>,
        replicas: Arc<Mutex<ReplicaState>>,
    ) -> Self {
        let (replica_tx, writer) = spawn_replica_writer(replicas.clone());
        Self {
            raft,
            network,
            transport,
            replicas,
            replica_tx,
            _writer: Arc::new(AbortOnDrop(writer)),
        }
    }

    /// The lease-group consensus handle (to initialize, propose lease assignments,
    /// read membership, ...).
    #[must_use]
    pub fn raft(&self) -> &LeaseRaft {
        &self.raft
    }

    /// The replication-leader transport (to quorum-append a group's session logs).
    #[must_use]
    pub fn transport(&self) -> &Arc<PeerReplicaTransport> {
        &self.transport
    }

    /// This node's lease-group role and consensus epoch, for the observability gauges
    /// (ADR 0020): `(is_leader, epoch)` where `epoch` is the current consensus term.
    /// Read from the raft metrics — a read-only snapshot, no metrics dependency here.
    #[must_use]
    pub fn lease_role(&self) -> (bool, u64) {
        let m = self.raft.metrics().borrow().clone();
        let is_leader = m.current_leader == Some(m.id);
        (is_leader, m.current_term)
    }

    /// The number of voters currently configured in the lease group, read from the
    /// raft metrics. A readiness signal: a failover is only safe once the group has
    /// grown to enough voters that losing one still leaves a quorum.
    #[must_use]
    pub fn voter_count(&self) -> usize {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .count()
    }

    /// Whether the lease group can serve this node's durable sessions: it has a
    /// current leader (so consensus is making progress and leases can be assigned)
    /// **and** this node is a voter (so the leader can assign it ownership, and a
    /// takeover would keep quorum). A node still catching up as a learner — or one in
    /// a group with no leader — reports not-ready, which is the signal an orchestrator
    /// should gate client traffic on (the node should not take sessions it cannot yet
    /// durably own). See [`voter_count`](Self::voter_count).
    #[must_use]
    pub fn lease_group_ready(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader.is_some()
            && metrics
                .membership_config
                .voter_ids()
                .any(|id| id == metrics.id)
    }

    /// Register a peer's outbound link channel for *both* planes — consensus
    /// (keyed by the peer's [`raft_id`]) and replication (keyed by its [`NodeId`]).
    /// Called when a peer link is established.
    pub fn register(&self, node: &NodeId, tx: mpsc::UnboundedSender<PeerMessage>) {
        self.network.register(raft_id(node), tx.clone());
        self.transport.register(node.clone(), tx);
    }

    /// Fail a peer on both planes when its link drops, so in-flight consensus RPCs
    /// and replication appends resolve rather than hang.
    pub fn fail(&self, node: &NodeId) {
        self.network.fail_node(raft_id(node));
        self.transport.fail_node(node);
    }

    /// Route an inbound durable-plane frame, returning the reply to send back over
    /// the same link (or `None` for replies, which terminate here, and for frames
    /// that are not part of this plane).
    pub async fn handle(&self, frame: PeerMessage) -> Option<PeerMessage> {
        match frame {
            // Consensus request → run it against our lease raft, reply with the result.
            PeerMessage::RaftRpc { req_id, payload } => Some(PeerMessage::RaftRpcReply {
                req_id,
                payload: dispatch(&self.raft, &payload).await,
            }),
            // Consensus reply → wake the waiting RPC.
            PeerMessage::RaftRpcReply { req_id, payload } => {
                self.network.complete_reply(req_id, payload);
                None
            }
            // Replication append → hand to the replica-writer, which group-commits a burst
            // of ops into one fsync'd transaction and answers with accept/fence (ADR 0027).
            // A `true` ack still means the op is durably on disk (the batch committed).
            PeerMessage::Replicate { req_id, epoch, op } => {
                let (reply_tx, reply_rx) = oneshot::channel();
                // If the writer is gone (shutdown) or never answers, the op is not durable
                // → do not ack acceptance.
                let accepted = if self.replica_tx.send((epoch, op, reply_tx)).is_ok() {
                    reply_rx.await.unwrap_or(false)
                } else {
                    false
                };
                Some(PeerMessage::ReplicateAck { req_id, accepted })
            }
            // Replication ack → wake the waiting append.
            PeerMessage::ReplicateAck { req_id, accepted } => {
                self.transport.complete_ack(req_id, accepted);
                None
            }
            // Recovery-read (workstream F): answer from our follower copy of the key,
            // with its truncation low-water so the recovering owner cannot resurrect an
            // already-acked prefix from a stale replica (ADR 0018 §3b).
            PeerMessage::ReplicaRead { req_id, key } => {
                let (watermark, entries) = {
                    let r = self.lock_replicas();
                    (
                        r.watermark(&key),
                        r.entries(&key)
                            .into_iter()
                            .map(|e| (e.offset, e.record))
                            .collect(),
                    )
                };
                Some(PeerMessage::ReplicaReadReply {
                    req_id,
                    watermark,
                    entries,
                })
            }
            // Recovery-read reply → wake the waiting read.
            PeerMessage::ReplicaReadReply {
                req_id,
                watermark,
                entries,
            } => {
                self.transport.complete_read(req_id, watermark, entries);
                None
            }
            // Not a durable-plane frame (Hello / Interest / Publish / ProxyHello):
            // handled elsewhere.
            _ => None,
        }
    }

    fn lock_replicas(&self) -> std::sync::MutexGuard<'_, ReplicaState> {
        self.replicas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Spawn the single **replica-writer** task for a node's follower copy (ADR 0027).
///
/// Inbound `Replicate` frames send their op here rather than each fsyncing on its own.
/// The writer takes the next queued op, **drains everything else already queued**, and
/// applies the whole burst with one [`ReplicaState::apply_batch`] — a single fsync for
/// the batch instead of one per message. Each waiting frame is then answered with its
/// op's accept/fence result. Under load this collapses N per-message fsyncs into one per
/// batch (the contention ADR 0026 T4 found); at rest (one op in flight) it is exactly the
/// previous one-op-per-commit behaviour. Returns the sender the plane keeps and the task's
/// abort handle (the plane aborts it when its last clone drops, releasing the `replicas`
/// redb handle for a clean restart); the loop also exits on its own when every sender drops.
fn spawn_replica_writer(
    replicas: Arc<Mutex<ReplicaState>>,
) -> (
    mpsc::UnboundedSender<ReplicaWrite>,
    tokio::task::AbortHandle,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ReplicaWrite>();
    let join = tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            // Coalesce the current backlog: this op plus everything already queued.
            let mut batch = vec![first];
            while let Ok(next) = rx.try_recv() {
                batch.push(next);
            }
            let mut ops = Vec::with_capacity(batch.len());
            let mut replies = Vec::with_capacity(batch.len());
            for (epoch, op, reply) in batch {
                ops.push((epoch, op));
                replies.push(reply);
            }
            // Run the (fsyncing) batch apply off the async worker; one fsync for the burst.
            let n = replies.len();
            let replicas = replicas.clone();
            let results = tokio::task::spawn_blocking(move || {
                replicas
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .apply_batch(&ops)
            })
            .await
            .unwrap_or_else(|_| vec![false; n]);
            for (reply, accepted) in replies.into_iter().zip(results) {
                let _ = reply.send(accepted);
            }
        }
    });
    let abort = join.abort_handle();
    (tx, abort)
}

#[cfg(test)]
mod tests {
    use super::DurablePlane;
    use crate::cluster_log::{ReplOp, ReplicaState, ReplicaTransport};
    use crate::lease_group::{config, LeaseRaft};
    use crate::lease_raft::LeaseRequest;
    use crate::lease_store::LeaseStore;
    use crate::node_registry::raft_id;
    use crate::peer::{self, PeerMessage};
    use crate::raft_mesh::MeshRaftNetwork;
    use crate::repl_net::PeerReplicaTransport;
    use crate::NodeId;
    use bytes::BytesMut;
    use openraft::storage::Adaptor;
    use openraft::{BasicNode, Raft, ServerState};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::mpsc;

    fn n(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// Build a node: its raft (over a fresh `MeshRaftNetwork` + `LeaseStore`) and a
    /// `DurablePlane` bundling it with a replication transport + follower state.
    async fn node(id: &str) -> DurablePlane {
        let net = MeshRaftNetwork::new();
        let store = LeaseStore::new();
        let (ls, sm) = Adaptor::new(store);
        let raft: LeaseRaft = Raft::new(raft_id(&n(id)), config(), net.clone(), ls, sm)
            .await
            .unwrap();
        DurablePlane::new(
            raft,
            net,
            Arc::new(PeerReplicaTransport::new()),
            Arc::new(Mutex::new(ReplicaState::new())),
        )
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

    /// Drive one peer link for `plane`: drain its outbound channel to the wire, and
    /// route every inbound frame through `plane.handle`, sending replies back out the
    /// same channel. Exactly what mqttd's peer pump will do.
    fn spawn_link(
        plane: DurablePlane,
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
            while let Some(frame) = read_frame(&mut rh, &mut buf).await {
                if let Some(reply) = plane.handle(frame).await {
                    let _ = out_tx.send(reply);
                }
            }
        });
    }

    fn members(ids: &[&str]) -> BTreeMap<u64, BasicNode> {
        ids.iter()
            .map(|id| (raft_id(&n(id)), BasicNode::default()))
            .collect()
    }

    /// Two nodes, wired only through `DurablePlane` over a duplex link, run BOTH
    /// planes end to end: the lease group elects a leader and commits a lease
    /// (consensus), and a session-log append quorum-replicates (replication) — all
    /// through `register` / `handle` / the shared handles.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn plane_carries_consensus_and_replication_over_the_wire() {
        let p1 = node("node-1").await;
        let p2 = node("node-2").await;

        let (io1, io2) = tokio::io::duplex(256 * 1024);
        let (out1_tx, out1_rx) = mpsc::unbounded_channel();
        let (out2_tx, out2_rx) = mpsc::unbounded_channel();
        // Each plane reaches the other peer via the other's outbound channel.
        p1.register(&n("node-2"), out1_tx.clone());
        p2.register(&n("node-1"), out2_tx.clone());
        spawn_link(p1.clone(), io1, out1_tx, out1_rx);
        spawn_link(p2.clone(), io2, out2_tx, out2_rx);

        // --- consensus: elect + commit a lease ---
        p1.raft()
            .initialize(members(&["node-1", "node-2"]))
            .await
            .unwrap();
        p1.raft()
            .wait(Some(Duration::from_secs(15)))
            .state(ServerState::Leader, "node-1 leads over the plane")
            .await
            .unwrap();
        let resp = p1
            .raft()
            .client_write(LeaseRequest::Assign {
                group: 3,
                node: raft_id(&n("node-1")),
            })
            .await
            .unwrap();
        assert_eq!(resp.data.unwrap().holder, raft_id(&n("node-1")));

        // --- replication: node-1 quorum-appends to node-2's follower copy ---
        let op = ReplOp::Append {
            key: "client-x".to_string(),
            offset: 1,
            record: b"hello".to_vec(),
        };
        // A 1-of-1 "quorum" to node-2 is enough to prove the wire path: deliver
        // returns true once node-2's plane applied and acked over the link.
        assert!(
            p1.transport()
                .deliver(&n("node-2"), resp.data.unwrap().epoch, &op)
                .await
        );

        p1.raft().shutdown().await.unwrap();
        p2.raft().shutdown().await.unwrap();
    }

    /// `lease_group_ready` is the health endpoint's durable-readiness signal: false
    /// before the lease group is initialized (no leader, not a voter), true once a
    /// node has bootstrapped it (a leader exists and this node is a voter).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_group_ready_tracks_leadership_and_voter_membership() {
        let p = node("ready-node").await;
        // Un-initialized: no leader and not yet a voter → not ready.
        assert!(!p.lease_group_ready());

        p.raft().initialize(members(&["ready-node"])).await.unwrap();
        p.raft()
            .wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "node leads its own group")
            .await
            .unwrap();
        // Leader of a group it is a voter in → ready.
        assert!(p.lease_group_ready());

        p.raft().shutdown().await.unwrap();
    }

    /// A delivery to an unregistered peer fails fast (no hang) — the plane reports
    /// the peer unreachable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deliver_to_unregistered_peer_is_unreachable() {
        let p = node("solo").await;
        let op = ReplOp::Append {
            key: "k".to_string(),
            offset: 1,
            record: vec![1],
        };
        assert!(!p.transport().deliver(&n("ghost"), 1, &op).await);
        p.raft().shutdown().await.unwrap();
    }

    /// Recovery-read (workstream F): after replicating to a follower, a new owner
    /// reads that replica's log back over the wire — the entries a takeover rebuilds
    /// from. An unreachable peer reads `None`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_read_returns_a_replicas_log_over_the_wire() {
        let p1 = node("node-1").await;
        let p2 = node("node-2").await;

        let (io1, io2) = tokio::io::duplex(256 * 1024);
        let (out1_tx, out1_rx) = mpsc::unbounded_channel();
        let (out2_tx, out2_rx) = mpsc::unbounded_channel();
        p1.register(&n("node-2"), out1_tx.clone());
        p2.register(&n("node-1"), out2_tx.clone());
        spawn_link(p1.clone(), io1, out1_tx, out1_rx);
        spawn_link(p2.clone(), io2, out2_tx, out2_rx);

        // node-1 replicates two entries into node-2's follower copy.
        for (offset, rec) in [(1u64, b"m1".as_slice()), (2, b"m2".as_slice())] {
            let op = ReplOp::Append {
                key: "q/c".to_string(),
                offset,
                record: rec.to_vec(),
            };
            assert!(p1.transport().deliver(&n("node-2"), 1, &op).await);
        }

        // node-1 recovery-reads node-2's log for the key.
        let read = p1
            .transport()
            .read_replica(&n("node-2"), "q/c")
            .await
            .expect("reachable replica");
        assert_eq!(
            read.entries.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(&read.entries[1].record, b"m2");

        // An unreachable peer reads None.
        assert!(p1
            .transport()
            .read_replica(&n("ghost"), "q/c")
            .await
            .is_none());

        p1.raft().shutdown().await.unwrap();
        p2.raft().shutdown().await.unwrap();
    }

    /// ADR 0027: a burst of `Replicate` frames handled concurrently is all accepted and
    /// durably applied by the single replica-writer — the ops can coalesce into one
    /// fsync'd batch, and none is dropped or corrupted under the concurrency. (The
    /// fsync-count reduction itself is validated live; here we prove correctness.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn replica_writer_group_commits_a_concurrent_burst() {
        let p = node("burst-node").await;

        // Fire 50 appends to distinct keys concurrently through the plane — they race into
        // the writer's queue, where it batches whatever is pending into one apply_batch.
        let mut tasks = Vec::new();
        for i in 0..50u8 {
            let plane = p.clone();
            tasks.push(tokio::spawn(async move {
                let frame = PeerMessage::Replicate {
                    req_id: u64::from(i),
                    epoch: 1,
                    op: ReplOp::Append {
                        key: format!("q/{i}"),
                        offset: 1,
                        record: vec![i],
                    },
                };
                match plane.handle(frame).await {
                    Some(PeerMessage::ReplicateAck { accepted, .. }) => accepted,
                    _ => false,
                }
            }));
        }
        let mut accepted = 0;
        for t in tasks {
            if t.await.unwrap() {
                accepted += 1;
            }
        }
        assert_eq!(accepted, 50, "every op in the burst is accepted");

        // All 50 keys are durably present on the follower copy, each with its record.
        // (Scoped so the guard is dropped before the await below.)
        {
            let replicas = p.replicas.lock().unwrap();
            for i in 0..50u8 {
                let key = format!("q/{i}");
                assert_eq!(replicas.fence_for_key(&key), 1);
                let entries = replicas.entries(&key);
                assert_eq!(entries.len(), 1, "key q/{i} applied exactly once");
                assert_eq!(entries[0].record, vec![i]);
            }
        }

        p.raft().shutdown().await.unwrap();
    }
}
