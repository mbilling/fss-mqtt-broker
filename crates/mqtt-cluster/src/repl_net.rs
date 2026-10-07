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

/// The record-byte budget of one page of a paged recovery read (#758): well under
/// [`crate::peer::MAX_FRAME`] with room for the entries' framing, and small enough
/// that a page crosses a busy link inside the RPC timeout.
const READ_PAGE_BYTES: u32 = 4 * 1024 * 1024;

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
    followers: HashMap<NodeId, mpsc::UnboundedSender<PeerMessage>>,
    /// The peer-bus proto each link negotiated, where the caller said (#758): a
    /// link at [`crate::peer::PROTO_REPLICA_READ_PAGED`] or above reads in pages.
    protos: HashMap<NodeId, u32>,
    /// In-flight pages of paged recovery-reads (#758), keyed by `req_id`.
    pending_chunks: HashMap<u64, PendingChunk>,
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
/// verdict (ADR 0043 P1), and its stored entries.
type ReadReply = (u64, bool, Vec<ReplicaEntryWire>);

/// One page of a paged recovery-read: as [`ReadReply`], plus whether more remain.
type ChunkReply = (u64, bool, Vec<ReplicaEntryWire>, bool);

#[derive(Debug)]
struct PendingChunk {
    node: NodeId,
    reply: oneshot::Sender<ChunkReply>,
}

#[derive(Debug)]
struct PendingRead {
    node: NodeId,
    /// Resolves with the replica's `(watermark, complete, entries)`.
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
    pub fn register(&self, node: NodeId, tx: mpsc::UnboundedSender<PeerMessage>) {
        let mut inner = self.lock();
        inner.protos.remove(&node);
        inner.followers.insert(node, tx);
    }

    /// [`register`](Self::register), recording the proto the link negotiated so
    /// recovery reads can be paged where the peer understands it (#758).
    pub fn register_with_proto(
        &self,
        node: NodeId,
        tx: mpsc::UnboundedSender<PeerMessage>,
        proto: u32,
    ) {
        let mut inner = self.lock();
        inner.protos.insert(node.clone(), proto);
        inner.followers.insert(node, tx);
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
        inner.protos.remove(node);
        let failed_chunks: Vec<u64> = inner
            .pending_chunks
            .iter()
            .filter(|(_, p)| p.node == *node)
            .map(|(id, _)| *id)
            .collect();
        for id in failed_chunks {
            inner.pending_chunks.remove(&id);
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

    /// Resolve a pending recovery-read with the replica's watermark, completeness
    /// verdict (ADR 0043 P1), and entries.
    ///
    /// Called by the link handler when a [`PeerMessage::ReplicaReadReply`] arrives.
    pub fn complete_read(
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

    /// Resolve one page of a paged recovery-read (#758).
    ///
    /// Called by the link handler when a [`PeerMessage::ReplicaReadChunk`] arrives.
    pub fn complete_read_chunk(
        &self,
        req_id: u64,
        watermark: u64,
        complete: bool,
        entries: Vec<ReplicaEntryWire>,
        more: bool,
    ) {
        if let Some(p) = self.lock().pending_chunks.remove(&req_id) {
            let _ = p.reply.send((watermark, complete, entries, more));
        }
    }

    /// Ask one page of `key` from `replica`, above offset `after`.
    async fn read_page(&self, replica: &NodeId, key: &str, after: u64) -> Option<ChunkReply> {
        let req_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut inner = self.lock();
            let tx = inner.followers.get(replica).cloned()?;
            inner.pending_chunks.insert(
                req_id,
                PendingChunk {
                    node: replica.clone(),
                    reply: reply_tx,
                },
            );
            let frame = PeerMessage::ReplicaReadFrom {
                req_id,
                key: key.to_string(),
                after,
                max_bytes: READ_PAGE_BYTES,
            };
            if tx.send(frame).is_err() {
                inner.pending_chunks.remove(&req_id);
                return None;
            }
        }
        let Ok(res) = tokio::time::timeout(self.rpc_timeout, reply_rx).await else {
            self.lock().pending_chunks.remove(&req_id);
            return None;
        };
        res.ok()
    }

    /// A recovery read assembled from pages (#758): each page bounded by the RPC
    /// timeout, the watermark the highest any page reported, complete only if every
    /// page was, and the entries every page returned above that watermark. All or
    /// nothing: a page that does not arrive (the replica crashed, the link dropped,
    /// the timeout fired), or one that claims more but brings nothing new, fails the
    /// whole read, exactly as an unreachable replica would. Recovery then counts it
    /// as unanswered and retries from the first page on its next attempt.
    async fn read_replica_paged(
        &self,
        replica: &NodeId,
        key: &str,
    ) -> Option<crate::cluster_log::ReplicaRead> {
        let mut after = 0;
        let mut watermark = 0;
        let mut complete = true;
        let mut entries: Vec<ReplicaEntryWire> = Vec::new();
        loop {
            let (wm, page_complete, page, more) = self.read_page(replica, key, after).await?;
            watermark = watermark.max(wm);
            complete &= page_complete;
            let progressed = page.last().is_some_and(|e| e.offset > after);
            for entry in page {
                if entry.offset > after {
                    after = entry.offset;
                    entries.push(entry);
                }
            }
            if !more {
                break;
            }
            // "More remain" but nothing new arrived: the replica is not answering
            // the read it was asked. Returning what came so far would hand the
            // merge a copy short of its tail as if it were whole, so the read
            // fails like an unreachable replica, and recovery retries it.
            if !progressed {
                return None;
            }
        }
        entries.retain(|e| e.offset > watermark);
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

    /// Ask `owner` to re-commit `key`'s committed log (ADR 0043 P1) — the
    /// catch-up request a hollow replica sends. Fire-and-forget; a no-op toward
    /// a peer with no live link — the sweep retries.
    pub fn request_catch_up(&self, owner: &NodeId, key: &str) {
        if let Some(tx) = self.lock().followers.get(owner) {
            let _ = tx.send(PeerMessage::ReplicaCatchUp {
                key: key.to_string(),
            });
        }
    }

    /// Ask `owner` to re-commit `key`'s committed log to `target` (ADR 0043 P3) —
    /// the decommission drain's hand-off request for a post-departure replica-set
    /// member the owner's fan-out does not reach yet. Fire-and-forget; a no-op
    /// toward a peer with no live link — the drain re-verifies and re-asks.
    pub fn request_catch_up_to(&self, owner: &NodeId, key: &str, target: &NodeId) {
        if let Some(tx) = self.lock().followers.get(owner) {
            let _ = tx.send(PeerMessage::ReplicaCatchUpTo {
                key: key.to_string(),
                target: target.0.clone(),
            });
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
            let Some(tx) = inner.followers.get(replica).cloned() else {
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
            queued: crate::peer::Queued::now(),
        };

        let sent = std::time::Instant::now();
        {
            let mut inner = self.lock();
            // Clone the sender so we hold no borrow of `inner` across the insert.
            let Some(tx) = inner.followers.get(replica).cloned() else {
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
            tracing::debug!(replica = %replica.0, req_id, "replicate: queued to link");
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
        let accepted = res.unwrap_or(false);
        if accepted {
            crate::stage_timing::record(crate::stage_timing::Stage::ReplicateRtt, sent.elapsed());
        }
        accepted
    }

    async fn read_replica(
        &self,
        replica: &NodeId,
        key: &str,
    ) -> Option<crate::cluster_log::ReplicaRead> {
        // A link that pages reads in pages: a log over MAX_FRAME cannot be read
        // in one frame at all (#758).
        let paged = self
            .lock()
            .protos
            .get(replica)
            .is_some_and(|p| *p >= crate::peer::PROTO_REPLICA_READ_PAGED);
        if paged {
            return self.read_replica_paged(replica, key).await;
        }
        let req_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut inner = self.lock();
            let Some(tx) = inner.followers.get(replica).cloned() else {
                return None; // replica not connected
            };
            inner.pending_reads.insert(
                req_id,
                PendingRead {
                    node: replica.clone(),
                    reply: reply_tx,
                },
            );
            let frame = PeerMessage::ReplicaRead {
                req_id,
                key: key.to_string(),
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
                    Some(PeerMessage::ReplicateAck {
                        req_id, accepted, ..
                    }) => {
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
                if let PeerMessage::Replicate {
                    req_id, epoch, op, ..
                } = msg
                {
                    let accepted = state.lock().unwrap().apply(epoch, &op);
                    let mut bytes = Vec::new();
                    peer::encode(
                        &PeerMessage::ReplicateAck {
                            req_id,
                            accepted,
                            queued: crate::peer::Queued::default(),
                        },
                        &mut bytes,
                    )
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
        transport.register(follower.clone(), out_tx);
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
        transport.register(b.clone(), out_tx);
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
        transport.register(b.clone(), out_tx);
        assert!(transport.read_replica(&b, "k").await.is_none());
    }

    /// #758: a replica whose log is larger than one peer frame can carry is read in
    /// pages over a proto-10 link, and the assembled read is exactly the replica's
    /// (every entry above its watermark, in order, with its tags). A proto-9 link
    /// still gets the one-frame read.
    #[tokio::test]
    async fn a_log_larger_than_a_frame_is_read_in_pages() {
        use crate::cluster_log::{ReplOp, ReplicaState};
        let transport = Arc::new(PeerReplicaTransport::new());
        let b = n("b");
        // 12 records of 1 MiB: three 4 MiB pages, and over `MAX_FRAME` in total.
        let state = Arc::new(Mutex::new(ReplicaState::new()));
        {
            let mut r = state.lock().unwrap();
            for off in 1..=12u64 {
                assert!(r.apply(
                    2,
                    &ReplOp::Append {
                        key: "q/big".into(),
                        offset: off,
                        seq: off,
                        record: vec![u8::try_from(off).unwrap(); 1 << 20],
                    }
                ));
            }
            assert!(r.apply(
                2,
                &ReplOp::Truncate {
                    key: "q/big".into(),
                    up_to: 2
                }
            ));
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        transport.register_with_proto(b.clone(), tx, crate::peer::PROTO_REPLICA_READ_PAGED);
        let server = {
            let transport = transport.clone();
            let state = state.clone();
            tokio::spawn(async move {
                let mut pages = 0;
                let mut legacy = 0;
                while let Some(msg) = rx.recv().await {
                    match msg {
                        PeerMessage::ReplicaReadFrom {
                            req_id,
                            key,
                            after,
                            max_bytes,
                        } => {
                            pages += 1;
                            let r = state.lock().unwrap();
                            let wm = r.watermark(&key);
                            let (page, more) =
                                r.epoch_entries_page(&key, after.max(wm), max_bytes as usize);
                            let entries = page
                                .into_iter()
                                .map(|e| crate::peer::ReplicaEntryWire {
                                    offset: e.offset,
                                    epoch: e.epoch,
                                    seq: e.seq,
                                    record: e.record,
                                })
                                .collect();
                            transport.complete_read_chunk(req_id, wm, true, entries, more);
                        }
                        PeerMessage::ReplicaRead { req_id, .. } => {
                            legacy += 1;
                            transport.complete_read(req_id, 0, true, Vec::new());
                        }
                        _ => {}
                    }
                }
                (pages, legacy)
            })
        };

        let read = transport.read_replica(&b, "q/big").await.expect("read");
        assert_eq!(read.watermark, 2);
        assert!(read.complete);
        assert_eq!(
            read.entries.iter().map(|e| e.offset).collect::<Vec<_>>(),
            (3..=12).collect::<Vec<_>>(),
            "every entry above the watermark, in order"
        );
        assert!(read
            .entries
            .iter()
            .all(|e| e.epoch == 2 && e.seq == e.offset));
        assert!(read
            .entries
            .iter()
            .all(|e| e.record == vec![u8::try_from(e.offset).unwrap(); 1 << 20]));

        // The same replica behind a proto-9 link: the one-frame read.
        let (tx9, rx9) = mpsc::unbounded_channel();
        drop(rx9);
        transport.register(b.clone(), tx9);
        assert!(
            transport.read_replica(&b, "q/big").await.is_none(),
            "legacy path, no server"
        );
        transport.fail_node(&b);
        let (pages, legacy) = server.await.unwrap();
        assert_eq!(
            (pages, legacy),
            (3, 0),
            "three pages over the proto-10 link"
        );
    }

    /// #758: a paged read is all or nothing. A replica that stops answering after
    /// the first page, or claims more but sends nothing new, fails the whole read:
    /// the pages that did arrive are never returned as if they were the replica's
    /// whole copy.
    #[tokio::test]
    async fn a_paged_read_that_stops_short_fails_whole() {
        for stall in [true, false] {
            let transport = Arc::new(PeerReplicaTransport::with_timeout(Duration::from_millis(
                100,
            )));
            let b = n("b");
            let (tx, mut rx) = mpsc::unbounded_channel();
            transport.register_with_proto(b.clone(), tx, crate::peer::PROTO_REPLICA_READ_PAGED);
            let server = {
                let transport = transport.clone();
                tokio::spawn(async move {
                    let mut pages = 0;
                    while let Some(msg) = rx.recv().await {
                        if let PeerMessage::ReplicaReadFrom { req_id, after, .. } = msg {
                            pages += 1;
                            if pages == 1 {
                                // One real entry, and "more remain".
                                let entry = crate::peer::ReplicaEntryWire {
                                    offset: after + 1,
                                    epoch: 1,
                                    seq: 1,
                                    record: b"m".to_vec(),
                                };
                                transport.complete_read_chunk(req_id, 0, true, vec![entry], true);
                            } else if !stall {
                                // "More remain" again, but nothing new.
                                transport.complete_read_chunk(req_id, 0, true, Vec::new(), true);
                            }
                            // stall: never answer the second page (a crashed replica).
                        }
                    }
                })
            };
            assert!(
                transport.read_replica(&b, "q/k").await.is_none(),
                "a read that stops short must fail whole (stall={stall})"
            );
            transport.fail_node(&b);
            server.abort();
        }
    }

    /// Catch-up requests (ADR 0043 P1/P3) reach a connected owner's link and
    /// silently no-op toward a disconnected one — the callers (sweep, drain)
    /// retry, so "no link yet" must be pending, never an error.
    #[tokio::test]
    async fn catch_up_requests_reach_the_owner_link() {
        let transport = PeerReplicaTransport::new();
        let (owner_tx, mut owner_rx) = mpsc::unbounded_channel();
        transport.register(n("owner"), owner_tx);

        transport.request_catch_up(&n("ghost"), "q/c"); // not connected: no-op
        transport.request_catch_up_to(&n("ghost"), "q/c", &n("x"));
        transport.request_catch_up(&n("owner"), "q/c");
        transport.request_catch_up_to(&n("owner"), "q/c", &n("x"));

        assert!(matches!(
            owner_rx.try_recv(),
            Ok(PeerMessage::ReplicaCatchUp { key }) if key == "q/c"
        ));
        assert!(
            matches!(owner_rx.try_recv(), Ok(PeerMessage::ReplicaCatchUpTo { target, .. }) if target == "x"),
            "the targeted request reaches the owner"
        );
        assert!(owner_rx.try_recv().is_err(), "nothing for the ghost leaked");
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
        transport.register(b.clone(), out_tx);

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
