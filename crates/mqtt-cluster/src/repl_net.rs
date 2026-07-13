//! Networked replication over the peer mesh — the real [`ReplicaTransport`]
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md), workstream E
//! step 3b).
//!
//! Step 3a built [`ClusterLog`](crate::cluster_log::ClusterLog) over the
//! [`ReplicaTransport`](crate::cluster_log::ReplicaTransport) seam and proved the
//! durability contract with an in-process sim. This module realizes that seam over
//! the wire: the lease-holder ships [`PeerMessage::Replicate`] to each replica and
//! awaits a [`PeerMessage::ReplicateAck`], counting accepts toward quorum.
//!
//! The peer link is a single multiplexed stream per node pair (publishes, interest,
//! session proxies — and now replication), so this transport does not own a
//! connection. It is driven by three handles that map onto the existing mesh:
//!
//! - **outbound** — per replica, an `mpsc::Sender<PeerMessage>` into that peer's
//!   link (the same `tx` the hub registers on `PeerConnected`). [`deliver`] pushes
//!   a `Replicate` onto it.
//! - **ack routing** — when a `ReplicateAck` arrives inbound on a link, the link
//!   handler calls [`PeerReplicaTransport::complete_ack`], which wakes the pending
//!   [`deliver`].
//! - **disconnect** — when a link drops, the handler calls
//!   [`PeerReplicaTransport::fail_node`], failing that replica's in-flight requests
//!   (no quorum from a dead replica) instead of hanging on an ack that will never
//!   come.
//!
//! The follower side is just [`ReplicaState::apply`](crate::cluster_log::ReplicaState::apply)
//! — the link handler applies the op and replies with the ack. Wiring all three
//! handles into the live hub is the integration step (workstream E step 4); here
//! they are driven directly so the over-the-wire protocol, ack correlation, and
//! fencing are pinned by tests over real framed streams.

use crate::cluster_log::{ReplOp, ReplicaTransport};
use crate::lease::Epoch;
use crate::peer::{PeerMessage, ReplicaEntryWire};
use crate::NodeId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Default timeout for a replication RPC (an append ack or a recovery read).
///
/// Bounds a **half-open** peer link — TCP still up but the peer wedged — so an
/// append cannot hang quorum, and a takeover recovery-read cannot hang serving a
/// session, waiting on a reply that will never come. On timeout the request
/// resolves exactly as a dropped link would (an append counts no ack; a read reads
/// unreachable) and its in-flight entry is reaped. `fail_node` still handles the
/// common case (a link that actually drops) faster; this is the backstop for a link
/// that stays up but stops answering.
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// A leader-side [`ReplicaTransport`] that replicates over the peer mesh.
///
/// Holds, per replica, the outbound channel into that peer's link, and a table of
/// in-flight requests keyed by `req_id`. See the module docs for how the three
/// handles map onto the mesh.
#[derive(Debug)]
pub struct PeerReplicaTransport {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
    /// How long to wait for a peer's reply before treating it as unreachable.
    rpc_timeout: Duration,
}

impl Default for PeerReplicaTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
struct Inner {
    followers: HashMap<NodeId, (mpsc::UnboundedSender<PeerMessage>, u32)>,
    pending: HashMap<u64, Pending>,
    /// In-flight recovery-reads (workstream F), keyed by `req_id`.
    pending_reads: HashMap<u64, PendingRead>,
    /// In-flight key-discovery requests (ADR 0042 T9, exhibit ⑥), keyed by `req_id`.
    pending_keys: HashMap<u64, PendingKeys>,
}

#[derive(Debug)]
struct PendingKeys {
    node: NodeId,
    reply: oneshot::Sender<Vec<String>>,
}

#[derive(Debug)]
struct Pending {
    node: NodeId,
    ack: oneshot::Sender<bool>,
}

/// A recovery-read reply: the replica's truncation low-water, its completeness
/// verdict (ADR 0043 P1 — `false` for replies from pre-proto-4 peers), and its
/// stored entries.
type ReadReply = (u64, bool, Vec<ReplicaEntryWire>);

#[derive(Debug)]
struct PendingRead {
    node: NodeId,
    /// Resolves with the replica's `(watermark, entries)`.
    reply: oneshot::Sender<ReadReply>,
}

impl PeerReplicaTransport {
    /// An empty transport with no replicas registered, using the default RPC
    /// timeout ([`DEFAULT_RPC_TIMEOUT`]).
    #[must_use]
    pub fn new() -> Self {
        Self::with_timeout(DEFAULT_RPC_TIMEOUT)
    }

    /// An empty transport whose RPCs (append acks, recovery reads) resolve to
    /// unreachable after `rpc_timeout` with no reply. Mainly for tests; production
    /// uses [`new`](Self::new).
    #[must_use]
    pub fn with_timeout(rpc_timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            next_id: AtomicU64::new(0),
            rpc_timeout,
        }
    }

    /// Register (or replace) the outbound link channel for a replica.
    ///
    /// `tx` is the sender into that peer's link — the same channel the hub holds
    /// for the node. Called when a peer link is (re)established.
    pub fn register(&self, node: NodeId, tx: mpsc::UnboundedSender<PeerMessage>, proto: u32) {
        self.lock().followers.insert(node, (tx, proto));
    }

    /// Drop a replica and fail every request in flight to it.
    ///
    /// Called when the peer link drops: a dead replica cannot ack, so its pending
    /// appends resolve to "not accepted" rather than hanging.
    pub fn fail_node(&self, node: &NodeId) {
        let mut inner = self.lock();
        inner.followers.remove(node);
        let failed: Vec<u64> = inner
            .pending
            .iter()
            .filter(|(_, p)| p.node == *node)
            .map(|(id, _)| *id)
            .collect();
        for id in failed {
            if let Some(p) = inner.pending.remove(&id) {
                let _ = p.ack.send(false);
            }
        }
        // Fail in-flight recovery-reads to this replica too (dropping the sender
        // resolves the awaiting `read_replica` to `None`).
        let failed_reads: Vec<u64> = inner
            .pending_reads
            .iter()
            .filter(|(_, p)| p.node == *node)
            .map(|(id, _)| *id)
            .collect();
        for id in failed_reads {
            inner.pending_reads.remove(&id);
        }
        let failed_keys: Vec<u64> = inner
            .pending_keys
            .iter()
            .filter(|(_, p)| p.node == *node)
            .map(|(id, _)| *id)
            .collect();
        for id in failed_keys {
            inner.pending_keys.remove(&id);
        }
    }

    /// Resolve a pending request with the replica's verdict.
    ///
    /// Called by the link handler when a [`PeerMessage::ReplicateAck`] arrives. An
    /// unknown `req_id` (already failed/timed out) is ignored.
    pub fn complete_ack(&self, req_id: u64, accepted: bool) {
        if let Some(p) = self.lock().pending.remove(&req_id) {
            let _ = p.ack.send(accepted);
        }
    }

    /// Resolve a pending recovery-read with the replica's watermark and entries.
    ///
    /// Called by the link handler when a [`PeerMessage::ReplicaReadReply`] arrives.
    pub fn complete_read(&self, req_id: u64, watermark: u64, entries: Vec<ReplicaEntryWire>) {
        if let Some(p) = self.lock().pending_reads.remove(&req_id) {
            // A legacy reply carries no completeness verdict: conservative false.
            let _ = p.reply.send((watermark, false, entries));
        }
    }

    /// Resolve a pending recovery-read from a proto-4 reply, completeness included
    /// (ADR 0043 P1).
    pub fn complete_read2(
        &self,
        req_id: u64,
        watermark: u64,
        complete: bool,
        entries: Vec<ReplicaEntryWire>,
    ) {
        if let Some(p) = self.lock().pending_reads.remove(&req_id) {
            let _ = p.reply.send((watermark, complete, entries));
        }
    }

    /// Ask `owner` to re-commit `key`'s committed log (ADR 0043 P1) — the
    /// catch-up request a hollow replica sends. Fire-and-forget; a no-op toward
    /// a peer that cannot speak it (pre-proto-4) or has no live link — the
    /// sweep retries.
    pub fn request_catch_up(&self, owner: &NodeId, key: &str) {
        let inner = self.lock();
        if let Some((tx, proto)) = inner.followers.get(owner) {
            if *proto >= 4 {
                let _ = tx.send(PeerMessage::ReplicaCatchUp {
                    key: key.to_string(),
                });
            }
        }
    }

    /// Resolve a pending key-discovery request with the replica's local key set.
    ///
    /// Called by the link handler when a [`PeerMessage::ReplicaKeysReply`] arrives.
    pub fn complete_keys(&self, req_id: u64, keys: Vec<String>) {
        if let Some(p) = self.lock().pending_keys.remove(&req_id) {
            let _ = p.reply.send(keys);
        }
    }

    /// The peers currently registered on this transport (a live link each) — the
    /// catch-up sweep's discovery targets (ADR 0043 P1).
    #[must_use]
    pub fn connected(&self) -> Vec<NodeId> {
        self.lock().followers.keys().cloned().collect()
    }

    /// Ask one connected replica for its local key set (ADR 0042 T9, exhibit ⑥;
    /// public for the ADR 0043 P1 catch-up sweep, which must know **which** member
    /// answered — a group is stamped caught-up only once every other member of its
    /// replica set has been heard).
    pub async fn keys_of(&self, replica: &NodeId) -> Option<Vec<String>> {
        let req_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut inner = self.lock();
            let Some((tx, _)) = inner.followers.get(replica).cloned() else {
                return None; // replica not connected
            };
            inner.pending_keys.insert(
                req_id,
                PendingKeys {
                    node: replica.clone(),
                    reply: reply_tx,
                },
            );
            if tx.send(PeerMessage::ReplicaKeys { req_id }).is_err() {
                inner.pending_keys.remove(&req_id);
                return None;
            }
        }
        let Ok(res) = tokio::time::timeout(self.rpc_timeout, reply_rx).await else {
            self.lock().pending_keys.remove(&req_id);
            return None;
        };
        res.ok()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl ReplicaTransport for PeerReplicaTransport {
    async fn deliver(&self, replica: &NodeId, epoch: Epoch, op: &ReplOp) -> bool {
        let req_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (ack_tx, ack_rx) = oneshot::channel();
        let frame = PeerMessage::Replicate {
            req_id,
            epoch,
            op: op.clone(),
        };

        {
            let mut inner = self.lock();
            // Clone the sender so we hold no borrow of `inner` across the insert.
            let Some((tx, _)) = inner.followers.get(replica).cloned() else {
                tracing::debug!(replica = %replica.0, "replicate: follower not registered");
                return false; // replica not connected → no ack toward quorum
            };
            // Register the pending request before sending, so a concurrent
            // fail_node/complete_ack can never race ahead of it.
            inner.pending.insert(
                req_id,
                Pending {
                    node: replica.clone(),
                    ack: ack_tx,
                },
            );
            if tx.send(frame).is_err() {
                // Link gone between register and send: drop the pending entry.
                inner.pending.remove(&req_id);
                return false;
            }
        }

        // Resolved by complete_ack (the replica replied) or fail_node (link
        // dropped); a closed channel also reads as "not accepted". A wedged but
        // still-connected replica is bounded by the RPC timeout, after which the
        // pending entry is reaped and the append counts no ack toward quorum.
        let Ok(res) = tokio::time::timeout(self.rpc_timeout, ack_rx).await else {
            tracing::debug!(replica = %replica.0, req_id, "replicate: ack timed out");
            self.lock().pending.remove(&req_id);
            return false;
        };
        res.unwrap_or(false)
    }

    async fn read_replica(
        &self,
        replica: &NodeId,
        key: &str,
    ) -> Option<crate::cluster_log::ReplicaRead> {
        let req_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut inner = self.lock();
            let Some((tx, proto)) = inner.followers.get(replica).cloned() else {
                return None; // replica not connected
            };
            inner.pending_reads.insert(
                req_id,
                PendingRead {
                    node: replica.clone(),
                    reply: reply_tx,
                },
            );
            // Proto 4 peers answer with their completeness verdict (ADR 0043 P1);
            // the request's version tells the server which reply we can decode.
            let frame = if proto >= 4 {
                PeerMessage::ReplicaRead2 {
                    req_id,
                    key: key.to_string(),
                }
            } else {
                PeerMessage::ReplicaRead {
                    req_id,
                    key: key.to_string(),
                }
            };
            if tx.send(frame).is_err() {
                inner.pending_reads.remove(&req_id);
                return None;
            }
        }
        // Bounded like deliver: a wedged replica must not hang a takeover recovery.
        let Ok(res) = tokio::time::timeout(self.rpc_timeout, reply_rx).await else {
            self.lock().pending_reads.remove(&req_id);
            return None;
        };
        let (watermark, complete, entries) = res.ok()?;
        Some(crate::cluster_log::ReplicaRead {
            watermark,
            complete,
            entries: entries
                .into_iter()
                .map(|e| crate::cluster_log::EpochEntry {
                    epoch: e.epoch,
                    seq: e.seq,
                    offset: e.offset,
                    record: e.record,
                })
                .collect(),
        })
    }

    async fn list_remote_keys(&self) -> Vec<String> {
        // Ask every connected replica for its key set and union the answers
        // (ADR 0042 T9, exhibit ⑥). Best-effort: an unreachable replica
        // contributes nothing. Sequential is fine — this runs on the off-loop
        // takeover scan, the mesh is small, and each ask is bounded by the RPC
        // timeout.
        let replicas: Vec<NodeId> = self.lock().followers.keys().cloned().collect();
        let mut keys: Vec<String> = Vec::new();
        for replica in replicas {
            if let Some(mut k) = self.keys_of(&replica).await {
                keys.append(&mut k);
            }
        }
        keys.sort_unstable();
        keys.dedup();
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::PeerReplicaTransport;
    use crate::cluster_log::{ReplOp, ReplicaState, ReplicaTransport};
    use crate::peer::{self, PeerMessage};
    use crate::NodeId;
    use bytes::BytesMut;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::mpsc;

    fn n(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    fn append(key: &str, offset: u64) -> ReplOp {
        ReplOp::Append {
            key: key.to_string(),
            offset,
            seq: offset,
            record: b"payload".to_vec(),
        }
    }

    /// Spawn the **leader side** pumps for one replica link over `leader_io`:
    /// drain `out_rx` (what `deliver` pushes) to the wire, and route inbound
    /// `ReplicateAck`s back into the transport. Mirrors what the hub does on a link.
    fn spawn_leader_link(
        transport: Arc<PeerReplicaTransport>,
        leader_io: DuplexStream,
        mut out_rx: mpsc::UnboundedReceiver<PeerMessage>,
    ) {
        let (mut rh, mut wh) = tokio::io::split(leader_io);
        // writer: out_rx -> wire
        tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                let mut bytes = Vec::new();
                peer::encode(&msg, &mut bytes).unwrap();
                if wh.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });
        // reader: wire -> complete_ack
        tokio::spawn(async move {
            let mut buf = BytesMut::new();
            loop {
                match read_frame(&mut rh, &mut buf).await {
                    Some(PeerMessage::ReplicateAck { req_id, accepted }) => {
                        transport.complete_ack(req_id, accepted);
                    }
                    Some(_) => {}
                    None => break,
                }
            }
        });
    }

    /// Spawn the **follower side** over `follower_io`: apply each `Replicate` to a
    /// shared `ReplicaState` and reply with a `ReplicateAck`. Mirrors the hub's
    /// inbound replication handler.
    fn spawn_follower_link(state: Arc<Mutex<ReplicaState>>, follower_io: DuplexStream) {
        let (mut rh, mut wh) = tokio::io::split(follower_io);
        tokio::spawn(async move {
            let mut buf = BytesMut::new();
            while let Some(msg) = read_frame(&mut rh, &mut buf).await {
                if let PeerMessage::Replicate { req_id, epoch, op } = msg {
                    let accepted = state.lock().unwrap().apply(epoch, &op);
                    let mut bytes = Vec::new();
                    peer::encode(&PeerMessage::ReplicateAck { req_id, accepted }, &mut bytes)
                        .unwrap();
                    if wh.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            }
        });
    }

    async fn read_frame(
        rh: &mut (impl tokio::io::AsyncRead + Unpin),
        buf: &mut BytesMut,
    ) -> Option<PeerMessage> {
        loop {
            if let Ok(Some(msg)) = peer::decode(buf) {
                return Some(msg);
            }
            let n = rh.read_buf(buf).await.ok()?;
            if n == 0 {
                return None;
            }
        }
    }

    /// Connect a leader transport to one follower replica over a duplex link and
    /// return the transport, the follower's shared state, and the follower id.
    fn wired() -> (Arc<PeerReplicaTransport>, Arc<Mutex<ReplicaState>>, NodeId) {
        let transport = Arc::new(PeerReplicaTransport::new());
        let follower = n("b");
        let state = Arc::new(Mutex::new(ReplicaState::new()));
        let (leader_io, follower_io) = tokio::io::duplex(64 * 1024);
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        transport.register(follower.clone(), out_tx, crate::peer::PROTO_MAX);
        spawn_leader_link(transport.clone(), leader_io, out_rx);
        spawn_follower_link(state.clone(), follower_io);
        (transport, state, follower)
    }

    #[tokio::test]
    async fn deliver_round_trips_and_applies_on_the_follower() {
        let (transport, state, b) = wired();
        assert!(transport.deliver(&b, 1, &append("c", 1)).await);
        assert!(transport.deliver(&b, 1, &append("c", 2)).await);
        // The follower stored both, over the wire.
        let offsets: Vec<u64> = state
            .lock()
            .unwrap()
            .entries("c")
            .into_iter()
            .map(|e| e.offset)
            .collect();
        assert_eq!(offsets, vec![1, 2]);
    }

    /// A stale-epoch op is rejected by the follower; the ack carries `accepted=false`
    /// and `deliver` reports it — fencing, over the wire.
    #[tokio::test]
    async fn stale_epoch_is_fenced_over_the_wire() {
        let (transport, state, b) = wired();
        // Follower advances to epoch 5 first.
        assert!(transport.deliver(&b, 5, &append("c", 1)).await);
        // A delivery at epoch 4 is fenced.
        assert!(!transport.deliver(&b, 4, &append("c", 2)).await);
        assert_eq!(state.lock().unwrap().fence_for_key("c"), 5);
    }

    /// Delivering to a replica that was never registered fails immediately (no ack
    /// to await) — an unreachable replica contributes nothing to quorum.
    #[tokio::test]
    async fn deliver_to_unknown_replica_is_false() {
        let transport = PeerReplicaTransport::new();
        assert!(!transport.deliver(&n("ghost"), 1, &append("c", 1)).await);
    }

    /// A replica whose link is up but **wedged** (it never answers) does not hang an
    /// append forever: the RPC timeout resolves the deliver to "not accepted" and
    /// reaps the in-flight entry, so the append can fall short of quorum and retry.
    #[tokio::test]
    async fn deliver_times_out_on_a_wedged_replica() {
        let transport = Arc::new(PeerReplicaTransport::with_timeout(Duration::from_millis(
            50,
        )));
        let b = n("b");
        // Registered (so the send succeeds) but never serviced — no ack ever comes.
        let (out_tx, _out_rx) = mpsc::unbounded_channel();
        transport.register(b.clone(), out_tx, crate::peer::PROTO_MAX);
        assert!(!transport.deliver(&b, 1, &append("c", 1)).await);
    }

    /// Likewise a recovery-read against a wedged replica times out to `None` rather
    /// than hanging a takeover.
    #[tokio::test]
    async fn read_replica_times_out_on_a_wedged_replica() {
        let transport = Arc::new(PeerReplicaTransport::with_timeout(Duration::from_millis(
            50,
        )));
        let b = n("b");
        let (out_tx, _out_rx) = mpsc::unbounded_channel();
        transport.register(b.clone(), out_tx, crate::peer::PROTO_MAX);
        assert!(transport.read_replica(&b, "k").await.is_none());
    }

    /// If a replica's link drops with a request in flight, `fail_node` resolves it
    /// to `false` rather than hanging forever.
    #[tokio::test]
    async fn fail_node_resolves_in_flight_requests() {
        let transport = Arc::new(PeerReplicaTransport::new());
        let b = n("b");
        // Register a follower whose receiver we keep alive (so the send succeeds)
        // but never service — no acks are ever produced.
        let (out_tx, _out_rx) = mpsc::unbounded_channel();
        transport.register(b.clone(), out_tx, crate::peer::PROTO_MAX);

        let t2 = transport.clone();
        let b2 = b.clone();
        let handle = tokio::spawn(async move { t2.deliver(&b2, 1, &append("c", 1)).await });
        // Yield so the spawned deliver runs up to its await point, registering the
        // pending request (deliver inserts pending before awaiting). Then the link
        // "drops": fail_node must resolve the in-flight request rather than hang.
        tokio::task::yield_now().await;
        transport.fail_node(&b);
        assert!(
            !handle.await.unwrap(),
            "in-flight request fails on disconnect"
        );
    }
}
