//! ADR 0093: `K` hub loops ("data shards") on one node behind one [`HubTx`], each owning
//! the clients of disjoint placement groups.
//!
//! A publish is matched on its publisher's shard, the subscribers that shard owns are
//! delivered there, and the rest are handed to their shard as one
//! [`HubCommand::ShardDeliver`] per batch (the messages and their clients), whose durable
//! completions come back as one [`HubCommand::ShardDone`] (D3). What a shard matches the
//! other shards' clients against is their routing as each last published it (Amendment 4):
//! every shard keeps the filters, `$share` memberships and presence of the clients it owns,
//! and at the end of a dispatch batch in which they changed it publishes an immutable copy
//! of them to the node's [`RoutingBoard`]. A SUBACK, UNSUBACK or CONNACK waits for that
//! publication, so a publish that follows the answer, on any shard, is matched with the
//! change. Everything peer-facing, retained, relayed or administrative goes to the first
//! shard, so a node with peers runs one shard (D5).
//!
//! With one shard (the default, `node.hub_shards = 1`) nothing here runs: [`HubTx`] is
//! the loop's own sender, [`Hub::owns`] is always true, there is no board, nothing is
//! published and no answer waits.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use bytes::Bytes;
use mqtt_codec::QoS;
use mqtt_core::{AppProperties, ClientId, SharedSubscriptionTable, SubscriptionTable};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use super::lanes::AppendGate;
use super::{min_qos, AttachOutcome, DurableOutcome, Hub, HubCommand};

/// The most shards a node may run (`MQTTD_HUB_SHARDS`): the width of
/// [`super::forwarding::PendingPublish::shard_outstanding`]'s bits, a byte (four until
/// ADR 0093 Amendment 3).
pub const MAX_SHARDS: usize = 8;

/// ADR 0093 D3, the per-pair credit's default (`node.hub_handoff_inflight`): how many
/// gated hand-offs one shard may owe another an answer for before the origin stops
/// taking its publishers' commands. In ADR 0091 D6's form it is a time: at about 10 µs a
/// dispatch on the target, 16,384 hand-offs are some 0.16 s of its queue, a sixth of
/// the 1 s stall budget, so a hand-off's pending entry is answered long before the
/// bound would evict it; and by Little's law a round trip of 0.2 s lets 16,384 in flight
/// carry 80,000 crossing messages a second, above what one m6i.8xlarge pair deals across.
pub const DEFAULT_HANDOFF_INFLIGHT: u32 = 16_384;

/// ADR 0093 Amendment 4: how far apart a shard under constant churn spaces its routing
/// publications, in copies: after a copy that took `c`, the next waits until `9c` has
/// passed, so copying takes at most about a tenth of the loop's time. A copy is
/// proportional to the shard's own tables (`crates/mqtt-core/benches/routing_copy.rs`:
/// about 5 ms for 10,000 device subscriptions), so without the spacing a shard with a
/// large table that changes every batch would spend its time copying.
const PUBLISH_SPACING: u32 = 9;

/// ADR 0093 Amendment 4: the routing of the clients one shard owns, as that shard last
/// published it: their ordinary filters, and their `$share` memberships with each member's
/// presence. The other shards match their publishes against it and own nothing else about
/// those clients.
#[derive(Debug, Default)]
pub struct RoutingPartition {
    pub(super) table: SubscriptionTable,
    pub(super) shared: SharedSubscriptionTable,
}

/// ADR 0093 Amendment 4: where the shards of one node publish their routing partitions.
///
/// A shard writes its own cell at the end of a dispatch batch in which its routing changed,
/// then bumps the version. A shard about to match a publish compares the version with the
/// one it last read and reloads the other cells only when it moved, so the common case is
/// one atomic load. A cell is replaced, never edited: a shard reading an old partition
/// keeps it whole until it reloads. A partition replaced in a cell or in a shard's view is
/// handed to the board's reaper thread, so freeing it, which costs about half what copying
/// it did, falls on no shard's loop and on no lock. The reaper's queue holds
/// [`REAPER_QUEUE`] partitions a shard at most: when it is full, the reaper is behind and
/// the shard frees the partition itself, so memory never holds more than that many old
/// copies.
#[derive(Debug)]
pub struct RoutingBoard {
    cells: Box<[RwLock<Arc<RoutingPartition>>]>,
    version: AtomicU64,
    /// Each shard's session count as of its last publication, for the node-wide
    /// `max_sessions` quota (ADR 0041 T4).
    sessions: Box<[AtomicUsize]>,
    reaper: std::sync::mpsc::SyncSender<Arc<RoutingPartition>>,
}

/// How many replaced partitions per shard the reaper's queue holds before a shard frees one
/// itself (ADR 0093 Amendment 4).
const REAPER_QUEUE: usize = 2;

impl RoutingBoard {
    /// A board for a node of `shards` shards, every partition empty, with its reaper
    /// thread, which ends with the board.
    ///
    /// # Panics
    ///
    /// When the reaper thread cannot be started, at the node's start.
    #[must_use]
    pub fn new(shards: usize) -> Arc<Self> {
        let (reaper, freed) =
            std::sync::mpsc::sync_channel::<Arc<RoutingPartition>>(REAPER_QUEUE * shards.max(1));
        std::thread::Builder::new()
            .name("mqttd-routing-reaper".to_string())
            .spawn(move || while freed.recv().is_ok() {})
            .expect("the routing reaper thread starts");
        Arc::new(Self {
            cells: (0..shards).map(|_| RwLock::new(Arc::default())).collect(),
            version: AtomicU64::new(0),
            sessions: (0..shards).map(|_| AtomicUsize::new(0)).collect(),
            reaper,
        })
    }

    /// Hand `partition` to the reaper. With the reaper's queue full, or the reaper gone,
    /// it is freed here.
    fn reap(&self, partition: Arc<RoutingPartition>) {
        let _ = self.reaper.try_send(partition);
    }

    fn publish(&self, shard: usize, partition: RoutingPartition, sessions: usize) {
        if let Some(cell) = self.cells.get(shard) {
            let old = std::mem::replace(
                &mut *cell.write().unwrap_or_else(PoisonError::into_inner),
                Arc::new(partition),
            );
            // Outside the lock, and off this loop.
            self.reap(old);
        }
        if let Some(count) = self.sessions.get(shard) {
            count.store(sessions, Ordering::Relaxed);
        }
        // Release: a shard that reads the new version sees the cell it was bumped for.
        self.version.fetch_add(1, Ordering::Release);
    }

    fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    fn partition(&self, shard: usize) -> Arc<RoutingPartition> {
        self.cells.get(shard).map_or_else(Arc::default, |cell| {
            cell.read().unwrap_or_else(PoisonError::into_inner).clone()
        })
    }

    fn sessions_besides(&self, shard: usize) -> usize {
        self.sessions
            .iter()
            .enumerate()
            .filter(|(k, _)| *k != shard)
            .map(|(_, count)| count.load(Ordering::Relaxed))
            .sum()
    }
}

/// An answer that waits for its shard's next publication (ADR 0093 Amendment 4): the
/// connection writes the SUBACK, UNSUBACK or CONNACK it carries only once every shard
/// can match with the change it reports.
#[derive(Debug)]
pub(super) enum AfterPublish {
    Subscribe(oneshot::Sender<Vec<bool>>, Vec<bool>),
    Unsubscribe(oneshot::Sender<Vec<Option<QoS>>>, Vec<Option<QoS>>),
    Attach(oneshot::Sender<AttachOutcome>, AttachOutcome),
}

/// An [`AfterPublish`] waiting since `since`.
#[derive(Debug)]
pub(super) struct Waiting {
    since: std::time::Instant,
    answer: AfterPublish,
}

impl AfterPublish {
    /// A dropped receiver is a connection that has gone; the change stands either way.
    fn send(self) {
        match self {
            Self::Subscribe(tx, verdicts) => {
                let _ = tx.send(verdicts);
            }
            Self::Unsubscribe(tx, answer) => {
                let _ = tx.send(answer);
            }
            Self::Attach(tx, outcome) => {
                let _ = tx.send(outcome);
            }
        }
    }
}

/// The shard that owns `client`: its placement group (ADR 0007 §1), folded onto the
/// node's shards. Every shard and every connection computes the same answer from the
/// client id alone.
#[must_use]
pub fn shard_of(client: &str, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    let group = mqtt_cluster::placement::group_of(client);
    let shards = u64::try_from(shards).unwrap_or(u64::MAX);
    usize::try_from(group % shards).unwrap_or(0)
}

/// The sender connections and the rest of the node reach the hub by (ADR 0092 T5).
///
/// One shard: the loop's own channel, nothing in between. Several: one channel per
/// shard, and each command goes to the shard that owns the client it names — a
/// connection pins itself to its client's shard after CONNECT ([`HubTx::pin`]), so its
/// sends cost one branch more than the channel's. A command that names no client goes
/// to the first shard; one every shard must see is sent to each; one that carries an
/// answer every shard must give is joined.
#[derive(Clone, Debug)]
pub struct HubTx(Inner);

#[derive(Clone, Debug)]
enum Inner {
    One(mpsc::UnboundedSender<HubCommand>),
    Sharded(Arc<[mpsc::UnboundedSender<HubCommand>]>),
}

impl From<mpsc::UnboundedSender<HubCommand>> for HubTx {
    fn from(tx: mpsc::UnboundedSender<HubCommand>) -> Self {
        Self(Inner::One(tx))
    }
}

/// Where one command goes.
enum Target {
    Shard(usize),
    All,
    Join,
}

impl HubTx {
    /// The senders of every shard, in shard order. One is the plain sender.
    ///
    /// # Panics
    ///
    /// With no sender at all: a hub has at least one shard.
    #[must_use]
    pub fn sharded(mut shards: Vec<mpsc::UnboundedSender<HubCommand>>) -> Self {
        assert!(!shards.is_empty(), "a hub has at least one shard");
        if shards.len() == 1 {
            return Self(Inner::One(shards.remove(0)));
        }
        Self(Inner::Sharded(shards.into()))
    }

    /// How many shards this sender reaches.
    #[must_use]
    pub fn shards(&self) -> usize {
        match &self.0 {
            Inner::One(_) => 1,
            Inner::Sharded(s) => s.len(),
        }
    }

    /// The sender of `client`'s shard alone: what a connection uses once it knows who
    /// it is, so nothing on its message path routes.
    #[must_use]
    pub fn pin(&self, client: &ClientId) -> Self {
        match &self.0 {
            Inner::One(_) => self.clone(),
            Inner::Sharded(s) => Self(Inner::One(s[shard_of(&client.0, s.len())].clone())),
        }
    }

    /// The sender of shard `i` alone (the last shard for an `i` past the end).
    #[must_use]
    pub fn shard(&self, i: usize) -> Self {
        match &self.0 {
            Inner::One(_) => self.clone(),
            Inner::Sharded(s) => Self(Inner::One(s[i.min(s.len() - 1)].clone())),
        }
    }

    /// Whether the hub has gone: any shard's loop dropped its receiver.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        match &self.0 {
            Inner::One(tx) => tx.is_closed(),
            Inner::Sharded(s) => s.iter().any(mpsc::UnboundedSender::is_closed),
        }
    }

    /// Send one command to the shard that must dispatch it. Fails only when the hub is
    /// gone, as the channel's own `send` does.
    ///
    /// # Errors
    ///
    /// The command, when its shard's loop has gone.
    #[allow(clippy::result_large_err)] // the channel's own error: the command handed back
    pub fn send(&self, cmd: HubCommand) -> Result<(), mpsc::error::SendError<HubCommand>> {
        let shards = match &self.0 {
            Inner::One(tx) => return tx.send(cmd),
            Inner::Sharded(s) => s,
        };
        let target = match cmd.route() {
            Route::Client(client) => Target::Shard(shard_of(&client.0, shards.len())),
            Route::First => Target::Shard(0),
            Route::All => Target::All,
            Route::Join => Target::Join,
        };
        match target {
            Target::Shard(i) => shards[i].send(cmd),
            Target::All => {
                for tx in &shards[1..] {
                    match cmd.duplicate() {
                        Some(copy) => tx.send(copy)?,
                        None => break,
                    }
                }
                shards[0].send(cmd)
            }
            Target::Join => join(shards, cmd),
        }
    }
}

/// Send a command every shard must answer, and answer its sender once all have —
/// or never, when a shard's answer is lost, so a dead shard reads as a dead hub.
/// Without a runtime to wait on, the first shard answers alone.
#[allow(clippy::result_large_err)] // the channel's own error
fn join(
    shards: &[mpsc::UnboundedSender<HubCommand>],
    cmd: HubCommand,
) -> Result<(), mpsc::error::SendError<HubCommand>> {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return shards[0].send(cmd);
    };
    match cmd {
        HubCommand::Ping { reply } => fan(
            &handle,
            shards,
            reply,
            |reply| HubCommand::Ping { reply },
            |_: Vec<()>| (),
        ),
        HubCommand::Flush { reply } => fan(
            &handle,
            shards,
            reply,
            |reply| HubCommand::Flush { reply },
            |_: Vec<()>| (),
        ),
        HubCommand::Drained { reply } => fan(
            &handle,
            shards,
            reply,
            |reply| HubCommand::Drained { reply },
            |evicted: Vec<u64>| evicted.iter().sum(),
        ),
        other => shards[0].send(other),
    }
}

#[allow(clippy::result_large_err)] // the channel's own error
fn fan<T, R>(
    handle: &tokio::runtime::Handle,
    shards: &[mpsc::UnboundedSender<HubCommand>],
    reply: oneshot::Sender<R>,
    make: impl Fn(oneshot::Sender<T>) -> HubCommand,
    fold: impl FnOnce(Vec<T>) -> R + Send + 'static,
) -> Result<(), mpsc::error::SendError<HubCommand>>
where
    T: Send + 'static,
    R: Send + 'static,
{
    let mut waits = Vec::with_capacity(shards.len());
    for tx in shards {
        let (t, r) = oneshot::channel();
        tx.send(make(t))?;
        waits.push(r);
    }
    handle.spawn(async move {
        let mut answers = Vec::with_capacity(waits.len());
        for w in waits {
            let Ok(v) = w.await else {
                // A shard that never answers: the sender's wait ends as a dropped
                // reply, which is what /livez takes as "not live".
                return;
            };
            answers.push(v);
        }
        let _ = reply.send(fold(answers));
    });
    Ok(())
}

/// Which shard a command belongs to.
enum Route<'a> {
    /// The shard that owns this client.
    Client(&'a ClientId),
    /// The first shard: nothing about the command names a client.
    First,
    /// Every shard, each its own copy.
    All,
    /// Every shard, and one answer from all of them.
    Join,
}

impl HubCommand {
    fn route(&self) -> Route<'_> {
        match self {
            Self::Attach { client, .. }
            | Self::Subscribe { client, .. }
            | Self::Unsubscribe { client, .. }
            | Self::PubAck { client, .. }
            | Self::PubAcks { client, .. }
            | Self::PubRec { client, .. }
            | Self::PubComp { client, .. }
            | Self::Detach { client, .. }
            | Self::Evict { client, .. }
            | Self::Qos2OpDone { client, .. }
            | Self::ReplayPageDone { client, .. }
            | Self::PkidBlockReserved { client, .. }
            | Self::RemoteSharedDeliver { client, .. }
            | Self::RemoteSharedDeliverAcked { client, .. } => Route::Client(client),
            Self::SessionRecovered { pending, .. } => Route::Client(&pending.client),
            Self::AppendDone { job, .. } => Route::Client(&job.client),
            Self::Publish {
                publisher: Some(client),
                ..
            } => Route::Client(client),
            Self::PublishBatch(batch) => batch.original.route(),
            Self::RuleDerived(derived) => derived.publish.route(),
            Self::SetQuotas(_)
            | Self::SetBrownout { .. }
            | Self::AttachAuthorizer(_)
            | Self::AttachRules(_)
            | Self::SweepIdentities(_)
            | Self::Draining => Route::All,
            Self::Ping { .. } | Self::Flush { .. } | Self::Drained { .. } => Route::Join,
            #[cfg(test)]
            Self::Ordered(inner) => inner.route(),
            _ => Route::First,
        }
    }

    /// A copy of a command every shard must see; `None` for one that cannot be copied.
    fn duplicate(&self) -> Option<Self> {
        match self {
            Self::SetQuotas(q) => Some(Self::SetQuotas(*q)),
            Self::SetBrownout { axis, on } => Some(Self::SetBrownout {
                axis: *axis,
                on: *on,
            }),
            Self::AttachAuthorizer(w) => {
                Some(Self::AttachAuthorizer(super::AuthzWatch(w.0.clone())))
            }
            Self::AttachRules(r) => Some(Self::AttachRules(r.clone())),
            Self::SweepIdentities(p) => Some(Self::SweepIdentities(p.clone())),
            Self::Draining => Some(Self::Draining),
            _ => None,
        }
    }
}

/// One subscriber a publish was handed to its shard for.
#[derive(Debug)]
pub enum ShardTarget {
    /// An ordinary subscriber: its shard computes the delivery terms from the filters
    /// it holds for it, and skips it if it has unsubscribed since the match.
    Ordinary(ClientId),
    /// A `$share` member the origin's dealing chose, at the already-downgraded `QoS`.
    Shared {
        /// The chosen member.
        client: ClientId,
        /// `min(publish QoS, granted)`.
        qos: QoS,
    },
}

impl ShardTarget {
    fn client(&self) -> &ClientId {
        match self {
            Self::Ordinary(c) | Self::Shared { client: c, .. } => c,
        }
    }
}

/// A publish matched on one shard, handed to another for the subscribers it owns
/// ([`HubCommand::ShardDeliver`]). Boxed in the command: once per cross-shard message,
/// and it must not size every queued command (#835).
#[derive(Debug)]
pub struct ShardDeliver {
    /// The shard the publish entered on, where its pending entry lives.
    pub origin: u8,
    /// The origin's pending publish, when the publisher waits for an acknowledgement:
    /// answered with one [`HubCommand::ShardDone`] once every delivery here has settled.
    /// `None` for a `QoS` 0 publish, a Will or a peer's unanswered forward.
    pub gate: Option<u64>,
    /// Destination topic.
    pub topic: String,
    /// Application payload.
    pub payload: Bytes,
    /// The publish `QoS` (each ordinary subscriber receives `min(qos, granted)`).
    pub qos: QoS,
    /// Whether the publish carried RETAIN (Retain As Published, #198).
    pub retain: bool,
    /// The publisher's Message Expiry Interval, in seconds.
    pub message_expiry: Option<u32>,
    /// The publisher's forwardable MQTT 5 application properties (ADR 0030).
    pub app: AppProperties,
    /// The subscribers this shard owns.
    pub targets: Vec<ShardTarget>,
    /// ADR 0093 D3: an ungated publish's ingress credit (ADR 0082 T3), released once
    /// this shard has dispatched it; a gated publish's stays on its pending entry until
    /// the last shard answers.
    pub credit: Option<crate::ingress::IngressPermit>,
}

/// The hand-offs one shard owes another from one of its dispatch batches, sent as one
/// [`HubCommand::ShardDeliver`] at the batch's end (ADR 0093 D3): the target is woken
/// once per batch, not once per message.
#[derive(Debug)]
pub struct ShardBatch {
    /// The shard the publishes entered on.
    pub origin: u8,
    /// The publishes, in dispatch order.
    pub messages: Vec<ShardDeliver>,
}

/// One settled hand-off, answered to its origin in a [`HubCommand::ShardDone`].
#[derive(Debug, Clone, Copy)]
pub struct ShardAnswer {
    /// The pending publish on the origin shard.
    pub id: u64,
    /// What its deliveries on the answering shard came to.
    pub outcome: DurableOutcome,
    /// Whether a subscriber there took it (fan-out evidence, as a peer's `Reached`).
    pub reached: bool,
}

/// ADR 0093 D4, the second arm (`node.hub_handoff_wake = "remote"`): where a shard's
/// batches are left for it without waking it from the sender's worker — a wake issued
/// by a running task puts the woken task in that worker's LIFO slot, to run next on it
/// behind the sender, which is how the prototype's pair took turns on one worker — and
/// the mailman, a thread of the node's own, wakes it instead: a wake from outside the
/// runtime goes through its injection queue to an idle worker.
#[derive(Debug, Default)]
pub struct ShardMailbox {
    queue: std::sync::Mutex<Vec<HubCommand>>,
    notify: tokio::sync::Notify,
}

impl ShardMailbox {
    /// Leave a command for the shard, waking nobody.
    pub(super) fn post(&self, cmd: HubCommand) {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(cmd);
    }

    /// Everything posted since the last take, in order.
    pub(super) fn take(&self) -> Vec<HubCommand> {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if queue.is_empty() {
            Vec::new()
        } else {
            std::mem::take(&mut *queue)
        }
    }
}

/// Wait until the mailman wakes shard `shard`'s mailbox; forever when there is none.
pub(super) async fn mailbox_wake(boxes: Option<&[ShardMailbox]>, shard: u8) {
    match boxes.and_then(|b| b.get(usize::from(shard))) {
        Some(mailbox) => mailbox.notify.notified().await,
        None => std::future::pending().await,
    }
}

/// Start the mailman: a thread that wakes shard `k` of `mailboxes` for every `k` sent
/// to it, and ends when every sender is gone.
///
/// # Panics
///
/// When the thread cannot be started, at the node's start.
#[must_use]
pub fn spawn_mailman(mailboxes: Arc<[ShardMailbox]>) -> std::sync::mpsc::Sender<u8> {
    let (tx, rx) = std::sync::mpsc::channel::<u8>();
    std::thread::Builder::new()
        .name("mqttd-hub-mailman".to_string())
        .spawn(move || {
            while let Ok(k) = rx.recv() {
                if let Some(mailbox) = mailboxes.get(usize::from(k)) {
                    mailbox.notify.notify_one();
                }
            }
        })
        .expect("the hub mailman thread starts");
    tx
}

/// The receiving shard's aggregate for one [`ShardDeliver`] (what
/// [`super::lanes::RemoteAppendGate`] is for a peer's forward): the appends and held
/// deliveries still unsettled before the origin can be answered.
#[derive(Debug)]
pub(super) struct ShardGate {
    /// The shard to answer.
    origin: u8,
    /// Obligations still unresolved.
    awaiting: usize,
    /// The composed outcome so far ([`DurableOutcome::and`] precedence).
    worst: DurableOutcome,
    /// Whether a subscriber here took the message.
    reached: bool,
}

impl Hub {
    /// Make this loop shard `shard` of `txs.len()`, with every shard's sender (its own
    /// included) for the hand-offs and the node's routing board (Amendment 4). One sender
    /// leaves the hub as it was.
    ///
    /// # Panics
    ///
    /// When `shard` is not an index into `txs`, or there are more than [`MAX_SHARDS`].
    pub fn set_shards(
        &mut self,
        shard: u8,
        txs: Vec<mpsc::UnboundedSender<HubCommand>>,
        board: &Arc<RoutingBoard>,
    ) {
        assert!(
            usize::from(shard) < txs.len() && txs.len() <= MAX_SHARDS,
            "shard {shard} of {} (at most {MAX_SHARDS})",
            txs.len()
        );
        self.shard = shard;
        self.shard_count = u8::try_from(txs.len()).expect("at most MAX_SHARDS");
        // ADR 0093 Amendment 4: with more than one shard, the node's board is where this
        // shard publishes its clients' routing and reads the others'.
        if txs.len() > 1 {
            self.routing_board = Some(board.clone());
            self.foreign_routing = (0..txs.len()).map(|_| Arc::default()).collect();
        }
        // ADR 0093 D3: one batch of hand-offs and one of answers per shard, and the
        // per-pair count of hand-offs still owed an answer.
        self.handoff_batches = (0..txs.len()).map(|_| Vec::new()).collect();
        self.shard_done_batches = (0..txs.len()).map(|_| Vec::new()).collect();
        self.handoff_inflight = vec![0; txs.len()];
        self.shard_txs = txs;
        if self.shard_count > 1 {
            // A publish's identity on the node is `(node, id)` (#784), recorded with
            // every copy a session queues: each shard issues from its own range, the
            // shard in the top bits, so two shards never mint the same id.
            let low = (1u64 << 60) - 1;
            let tag = u64::from(shard) << 60;
            self.publish_ids = (self.publish_ids & low) | tag;
            self.forward_seq = (self.forward_seq & low) | tag;
        }
    }

    /// ADR 0093 D3: the per-pair credit's cap (`node.hub_handoff_inflight`), see
    /// [`DEFAULT_HANDOFF_INFLIGHT`]. `0` means no cap.
    pub fn set_handoff_inflight_cap(&mut self, cap: u32) {
        self.handoff_inflight_cap = cap;
    }

    /// ADR 0093 Amendment 3: the queue depth from which this loop yields every
    /// [`super::HUB_YIELD_EVERY_DEEP`] commands (`node.hub_yield_deep`). `0` turns the
    /// rule off.
    pub fn set_hub_yield_deep(&mut self, deep: u32) {
        self.yield_deep = usize::try_from(deep).unwrap_or(usize::MAX);
    }

    /// Whether a target shard is at its hand-off cap, so this loop's data lane waits
    /// (ADR 0093 D3). Never with one shard or no cap.
    pub(super) fn handoff_gated(&self) -> bool {
        self.shard_count > 1
            && self.handoff_inflight_cap > 0
            && self
                .handoff_inflight
                .iter()
                .any(|&owed| owed >= self.handoff_inflight_cap)
    }

    /// ADR 0093 D4's second arm: every shard's mailbox and the mailman that wakes a shard
    /// for what was posted to it. Without this, a batch goes on the shard's channel and
    /// the wake is the sender's worker's.
    pub fn set_shard_mailboxes(
        &mut self,
        mailboxes: Arc<[ShardMailbox]>,
        mailman: std::sync::mpsc::Sender<u8>,
    ) {
        self.shard_mailboxes = Some(mailboxes);
        self.mailman = Some(mailman);
    }

    /// Which shard this loop is, and how many the node runs.
    #[must_use]
    pub fn shard(&self) -> (u8, u8) {
        (self.shard, self.shard_count)
    }

    /// Whether this shard owns `client`: its sessions, its in-flight state and its
    /// deliveries are here. Always, with one shard.
    #[inline]
    pub(super) fn owns(&self, client: &ClientId) -> bool {
        self.shard_count <= 1
            || shard_of(&client.0, usize::from(self.shard_count)) == usize::from(self.shard)
    }

    /// ADR 0093 Amendment 4: the routing of a client this shard owns changed (a filter,
    /// a `$share` membership, its presence). The shard publishes at the end of the batch.
    /// Nothing with one shard.
    pub(super) fn routing_changed(&mut self) {
        if self.routing_board.is_some() {
            self.routing_dirty = true;
        }
    }

    /// Send `answer`, or keep it until this shard has published the routing change it
    /// reports (ADR 0093 Amendment 4). Call [`routing_changed`](Self::routing_changed)
    /// first. With one shard, or with nothing unpublished, it goes now.
    pub(super) fn answer_after_publish(&mut self, answer: AfterPublish) {
        if self.routing_dirty {
            self.after_publish.push(Waiting {
                since: std::time::Instant::now(),
                answer,
            });
        } else {
            answer.send();
        }
    }

    /// The end of a dispatch batch (ADR 0093 Amendment 4): publish this shard's routing if
    /// it changed, then send the answers that waited for it. One publication a batch at
    /// most, however many changes the batch made, and under constant churn one in every
    /// [`PUBLISH_SPACING`] copies' worth of time: the answers wait for it meanwhile.
    /// `idle` (the loop is about to park, or a flush asks) publishes whatever the spacing.
    /// Nothing with one shard.
    pub(super) fn publish_routing(&mut self, idle: bool) {
        let Some(board) = &self.routing_board else {
            return;
        };
        if self.routing_dirty {
            let spaced = self
                .routing_published_at
                .is_none_or(|at| at.elapsed() >= self.routing_copy_cost * PUBLISH_SPACING);
            if !idle && !spaced {
                return;
            }
            self.routing_dirty = false;
            let started = std::time::Instant::now();
            let partition = RoutingPartition {
                table: self.table.clone(),
                shared: self.shared.clone(),
            };
            board.publish(usize::from(self.shard), partition, self.session_count());
            let published = std::time::Instant::now();
            self.routing_copy_cost = published - started;
            self.routing_published_at = Some(published);
            if let Some(m) = &self.metrics {
                m.hub_routing_published(self.shard, self.routing_copy_cost.as_secs_f64());
            }
            #[cfg(test)]
            self.probe
                .routing_publications
                .fetch_add(1, Ordering::Relaxed);
        }
        for waiting in std::mem::take(&mut self.after_publish) {
            if let Some(m) = &self.metrics {
                m.hub_routing_answer_waited(self.shard, waiting.since.elapsed().as_secs_f64());
            }
            waiting.answer.send();
        }
    }

    /// Before a publish is matched (ADR 0093 Amendment 4): the other shards' routing as
    /// each last published it. One atomic load when nothing moved since the last call.
    pub(super) fn refresh_foreign_routing(&mut self) {
        let Some(board) = &self.routing_board else {
            return;
        };
        let version = board.version();
        if version == self.foreign_version {
            return;
        }
        self.foreign_version = version;
        let me = usize::from(self.shard);
        for (k, slot) in self.foreign_routing.iter_mut().enumerate() {
            if k != me {
                let fresh = board.partition(k);
                if !Arc::ptr_eq(slot, &fresh) {
                    // Freed by the reaper, not by this shard's publish path.
                    board.reap(std::mem::replace(slot, fresh));
                }
            }
        }
    }

    /// The other shards' routing partitions (ADR 0093 Amendment 4), with each one's shard.
    /// Empty with one shard.
    pub(super) fn foreign_partitions(&self) -> impl Iterator<Item = (usize, &RoutingPartition)> {
        let me = usize::from(self.shard);
        self.foreign_routing
            .iter()
            .enumerate()
            .filter(move |(k, _)| *k != me)
            .map(|(k, partition)| (k, &**partition))
    }

    /// The node's session count for the `max_sessions` quota (ADR 0041 T4): this shard's,
    /// plus every other shard's as of its last publication.
    pub(super) fn node_session_count(&self) -> usize {
        self.session_count()
            + self
                .routing_board
                .as_ref()
                .map_or(0, |board| board.sessions_besides(usize::from(self.shard)))
    }

    /// A subscriber of the publish being dispatched that another shard owns.
    pub(super) fn hand_off(&mut self, target: ShardTarget) {
        self.handoff.push(target);
    }

    /// Put the subscribers collected by [`hand_off`](Self::hand_off) into their shards'
    /// batches — sent at the dispatch batch's end by
    /// [`flush_shard_batches`](Self::flush_shard_batches), ADR 0093 D3 — and owe the
    /// publisher's acknowledgement their answers when the publish is gated. A
    /// settle-window re-delivery hands off nothing: the other shard was told the
    /// publish once.
    #[allow(clippy::too_many_arguments)] // the publish's fields
    pub(super) fn flush_handoffs(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
        gate: &AppendGate,
    ) {
        if self.handoff.is_empty() {
            return;
        }
        let mut targets = std::mem::take(&mut self.handoff);
        let gated = match gate {
            AppendGate::Pending(id) => Some(*id),
            AppendGate::Replay(_) => {
                targets.clear();
                self.handoff = targets;
                return;
            }
            AppendGate::Peer { .. }
            | AppendGate::Shard { .. }
            | AppendGate::None
            | AppendGate::LiveOnly => None,
        };
        let count = usize::from(self.shard_count);
        let me = usize::from(self.shard);
        for shard in (0..count).filter(|s| *s != me) {
            // Two shards: everything collected is the other shard's. More: its share.
            let batch = if count == 2 {
                std::mem::take(&mut targets)
            } else {
                let (mine, rest): (Vec<ShardTarget>, Vec<ShardTarget>) = targets
                    .into_iter()
                    .partition(|t| shard_of(&t.client().0, count) == shard);
                targets = rest;
                mine
            };
            if batch.is_empty() {
                continue;
            }
            // ADR 0093 D3: the publish's ingress credit travels with its hand-off — on
            // the pending entry until the last shard answers when it is gated, inside
            // the batch until the target has dispatched it when it is not — so the pool
            // is charged for the hand-off's lifetime, and a shard that answers late
            // parks the publishers that feed it as a loop that dispatches late parks
            // its own (ADR 0082).
            let credit = if let Some(id) = gated {
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    p.shard_outstanding |= 1 << shard;
                }
                if let Some(credit) = self.publish_credit.take() {
                    self.pending_publishes.hold_credit(id, credit);
                }
                // The per-pair credit: one more answer owed by `shard`.
                if let Some(owed) = self.handoff_inflight.get_mut(shard) {
                    *owed = owed.saturating_add(1);
                }
                None
            } else {
                self.publish_credit.take()
            };
            if let Some(m) = &self.metrics {
                m.publish_forwarded("shard");
            }
            if let Some(pending) = self.handoff_batches.get_mut(shard) {
                pending.push(ShardDeliver {
                    origin: self.shard,
                    gate: gated,
                    topic: topic.to_string(),
                    payload: payload.clone(),
                    qos,
                    retain,
                    message_expiry,
                    app: app.clone(),
                    targets: batch,
                    credit,
                });
            }
        }
        if targets.capacity() > 0 && targets.is_empty() {
            self.handoff = targets;
        }
    }

    /// Send what this dispatch batch collected (ADR 0093 D3): one
    /// [`HubCommand::ShardDeliver`] per shard owed a hand-off and one
    /// [`HubCommand::ShardDone`] per origin owed answers, at the batch's end — so a
    /// shard is woken once per batch, not once per message. Nothing with one shard.
    pub(super) fn flush_shard_batches(&mut self) {
        if self.shard_count <= 1 {
            return;
        }
        for target in 0..usize::from(self.shard_count) {
            let messages = match self.handoff_batches.get_mut(target) {
                Some(batch) if !batch.is_empty() => std::mem::take(batch),
                _ => Vec::new(),
            };
            if !messages.is_empty() {
                let cmd = HubCommand::ShardDeliver(Box::new(ShardBatch {
                    origin: self.shard,
                    messages,
                }));
                self.send_to_shard(target, cmd);
            }
            let answers = match self.shard_done_batches.get_mut(target) {
                Some(batch) if !batch.is_empty() => std::mem::take(batch),
                _ => Vec::new(),
            };
            if !answers.is_empty() {
                let cmd = HubCommand::ShardDone {
                    shard: self.shard,
                    answers,
                };
                self.send_to_shard(target, cmd);
            }
        }
    }

    /// One command to shard `target`: on its channel, which wakes it from this worker,
    /// or into its mailbox with the mailman asked to wake it (ADR 0093 D4's second arm).
    fn send_to_shard(&self, target: usize, cmd: HubCommand) {
        if let (Some(boxes), Some(mailman)) = (&self.shard_mailboxes, &self.mailman) {
            if let Some(mailbox) = boxes.get(target) {
                mailbox.post(cmd);
                let _ = mailman.send(u8::try_from(target).unwrap_or(u8::MAX));
                return;
            }
        }
        if let Some(tx) = self.shard_txs.get(target) {
            let _ = tx.send(cmd);
        }
    }

    /// One batch of hand-offs from another shard (ADR 0093 D3): each publish delivered
    /// to the subscribers this shard owns, each answered when its deliveries settle.
    pub(super) fn dispatch_shard_batch(&mut self, b: ShardBatch) {
        for d in b.messages {
            self.dispatch_shard_deliver(d);
        }
    }

    /// Deliver a publish another shard matched to the subscribers this one owns, and
    /// answer the origin once every delivery has settled (gated) or not at all.
    pub(super) fn dispatch_shard_deliver(&mut self, d: ShardDeliver) {
        let gate = match d.gate {
            Some(id) => AppendGate::Shard {
                origin: d.origin,
                id,
            },
            None => AppendGate::None,
        };
        let mut worst = DurableOutcome::Ok;
        let mut reached = false;
        for target in d.targets {
            match target {
                ShardTarget::Ordinary(client) => {
                    if !self.owns(&client) {
                        continue;
                    }
                    let terms = self.delivery_terms(&client, &d.topic);
                    if !terms.matched {
                        // Unsubscribed since the origin matched (its mirror in flight).
                        continue;
                    }
                    let out = self.deliver_to_client(
                        &client,
                        &d.topic,
                        &d.payload,
                        min_qos(d.qos, terms.granted),
                        d.message_expiry,
                        &d.app,
                        d.retain && terms.retain_as_published,
                        &gate,
                    );
                    worst = worst.and(out);
                    reached = true;
                }
                ShardTarget::Shared { client, qos } => {
                    if !self.owns(&client) {
                        continue;
                    }
                    let delivered = self.deliver_to_present_client(
                        &client,
                        &d.topic,
                        &d.payload,
                        qos,
                        d.message_expiry,
                        &d.app,
                        false, // shared delivery clears RETAIN (#198)
                        &gate,
                    );
                    if let Some(out) = delivered {
                        worst = worst.and(out);
                        reached = true;
                    } else {
                        // The member left between the origin's dealing and this
                        // delivery (the origin dealt from routing this shard had not yet
                        // republished, Amendment 4): nothing was stored, and the
                        // publisher is not told it was (#973). Withheld; the retry is
                        // dealt to a member that is here.
                        debug!(member = %client.0, topic = %d.topic,
                               "shard hand-off to a member that is not here");
                        if let Some(m) = &self.metrics {
                            m.publish_dropped("shard-member-gone");
                        }
                        worst = worst.and(DurableOutcome::Failed);
                    }
                }
            }
        }
        if let Some(id) = d.gate {
            self.finish_shard_verdict(d.origin, id, worst, reached);
        }
    }

    /// The hand-off's deliveries are all planned: answer the origin now when none owes
    /// an append or a held window, else when the last of those settles.
    fn finish_shard_verdict(&mut self, origin: u8, id: u64, sync: DurableOutcome, reached: bool) {
        if let Some(g) = self.shard_append_pending.get_mut(&id) {
            g.worst = g.worst.and(sync);
            g.reached |= reached;
            return;
        }
        self.answer_shard(origin, id, sync, reached);
    }

    /// One more obligation on the hand-off of the origin's publish `id`: a submitted
    /// append or a held delivery.
    pub(super) fn register_shard_gate(&mut self, origin: u8, id: u64) {
        let g = self.shard_append_pending.entry(id).or_insert(ShardGate {
            origin,
            awaiting: 0,
            worst: DurableOutcome::Ok,
            reached: false,
        });
        g.awaiting += 1;
    }

    /// One obligation on the hand-off of publish `id` ended with `out`; the origin is
    /// answered once the last one has.
    pub(super) fn settle_shard(&mut self, id: u64, out: DurableOutcome, reached: bool) {
        let Some(g) = self.shard_append_pending.get_mut(&id) else {
            return;
        };
        g.awaiting = g.awaiting.saturating_sub(1);
        g.worst = g.worst.and(out);
        g.reached |= reached;
        if g.awaiting == 0 {
            if let Some(g) = self.shard_append_pending.remove(&id) {
                self.answer_shard(g.origin, id, g.worst, g.reached);
            }
        }
    }

    /// Answer the origin for one settled hand-off: queued for the batch's end
    /// ([`flush_shard_batches`](Self::flush_shard_batches)), one command per origin per
    /// batch (ADR 0093 D3).
    fn answer_shard(&mut self, origin: u8, id: u64, outcome: DurableOutcome, reached: bool) {
        if let Some(batch) = self.shard_done_batches.get_mut(usize::from(origin)) {
            batch.push(ShardAnswer {
                id,
                outcome,
                reached,
            });
        }
    }

    /// Shard `shard` settled every delivery of pending publish `id` it was handed. A
    /// second answer for the same hand-off changes nothing.
    pub(super) fn shard_done(
        &mut self,
        shard: u8,
        id: u64,
        outcome: DurableOutcome,
        reached: bool,
    ) {
        let bit = 1u8 << shard.min(7);
        let Some(p) = self.pending_publishes.get_mut(id) else {
            return;
        };
        if p.shard_outstanding & bit == 0 {
            return;
        }
        p.shard_outstanding &= !bit;
        let last = p.shard_outstanding == 0;
        // The per-pair credit: one answer fewer owed by `shard`; the data lane may
        // move again (`next_command`).
        if let Some(owed) = self.handoff_inflight.get_mut(usize::from(shard)) {
            *owed = owed.saturating_sub(1);
        }
        if let DurableOutcome::Ok = outcome {
            p.stored = true;
            if reached {
                p.ack_awaits_settle = false;
            }
        }
        if last {
            // ADR 0093 D3: the last shard answered; the publish's credit returns to the
            // pool now, whatever else the entry still waits for.
            self.pending_publishes.release_credit(id);
        }
        match outcome {
            DurableOutcome::Ok => self.try_complete_pending(id),
            DurableOutcome::Failed => self.drop_pending(id),
            DurableOutcome::Refused(r) => self.refuse_pending(id, r),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_in(shard: usize, shards: usize) -> ClientId {
        (0..10_000)
            .map(|i| ClientId(format!("c{i}").into()))
            .find(|c| shard_of(&c.0, shards) == shard)
            .expect("the hash spreads ids over both shards")
    }

    #[test]
    fn one_shard_owns_everything() {
        for id in ["a", "b", "device-0001", ""] {
            assert_eq!(shard_of(id, 1), 0);
            assert_eq!(shard_of(id, 0), 0);
        }
    }

    #[test]
    fn a_client_keyed_command_goes_to_its_shard() {
        let (t0, mut r0) = mpsc::unbounded_channel();
        let (t1, mut r1) = mpsc::unbounded_channel();
        let tx = HubTx::sharded(vec![t0, t1]);
        assert_eq!(tx.shards(), 2);
        let on1 = client_in(1, 2);
        tx.send(HubCommand::PubAck {
            client: on1.clone(),
            pkid: 1,
            received: tokio::time::Instant::now(),
        })
        .unwrap();
        assert!(r0.try_recv().is_err());
        assert!(matches!(r1.try_recv(), Ok(HubCommand::PubAck { client, .. }) if client == on1));
        // Pinned: every send goes there, whatever the command names.
        let pinned = tx.pin(&on1);
        assert_eq!(pinned.shards(), 1);
        pinned
            .send(HubCommand::Publish {
                topic: "t".into(),
                payload: Bytes::new(),
                qos: QoS::AtMostOnce,
                retain: false,
                message_expiry: None,
                app: AppProperties::default(),
                done: None,
                v5: false,
                publisher: None,
                credit: None,
            })
            .unwrap();
        assert!(r0.try_recv().is_err());
        assert!(matches!(r1.try_recv(), Ok(HubCommand::Publish { .. })));
    }

    #[test]
    fn a_command_naming_no_client_goes_first_and_a_setting_goes_everywhere() {
        let (t0, mut r0) = mpsc::unbounded_channel();
        let (t1, mut r1) = mpsc::unbounded_channel();
        let tx = HubTx::sharded(vec![t0, t1]);
        tx.send(HubCommand::Draining).unwrap();
        assert!(matches!(r0.try_recv(), Ok(HubCommand::Draining)));
        assert!(matches!(r1.try_recv(), Ok(HubCommand::Draining)));
        tx.send(HubCommand::SetQuotas(super::super::Quotas::default()))
            .unwrap();
        assert!(matches!(r0.try_recv(), Ok(HubCommand::SetQuotas(_))));
        assert!(matches!(r1.try_recv(), Ok(HubCommand::SetQuotas(_))));
        tx.send(HubCommand::RelayClosed { id: 7 }).unwrap();
        assert!(matches!(
            r0.try_recv(),
            Ok(HubCommand::RelayClosed { id: 7 })
        ));
        assert!(r1.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_joined_command_is_answered_once_every_shard_has() {
        let (t0, mut r0) = mpsc::unbounded_channel();
        let (t1, mut r1) = mpsc::unbounded_channel();
        let tx = HubTx::sharded(vec![t0, t1]);
        let (reply, done) = oneshot::channel();
        tx.send(HubCommand::Drained { reply }).unwrap();
        let Ok(HubCommand::Drained { reply: a }) = r0.try_recv() else {
            panic!("shard 0 gets its barrier")
        };
        let Ok(HubCommand::Drained { reply: b }) = r1.try_recv() else {
            panic!("shard 1 gets its barrier")
        };
        a.send(2).unwrap();
        tokio::task::yield_now().await;
        b.send(3).unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), done)
                .await
                .expect("answered")
                .unwrap(),
            5
        );
        // A shard that never answers leaves the sender's wait unanswered.
        let (reply, done) = oneshot::channel();
        tx.send(HubCommand::Ping { reply }).unwrap();
        let Ok(HubCommand::Ping { reply: a }) = r0.try_recv() else {
            panic!("shard 0 gets its ping")
        };
        let Ok(HubCommand::Ping { reply: b }) = r1.try_recv() else {
            panic!("shard 1 gets its ping")
        };
        a.send(()).unwrap();
        drop(b);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), done)
                .await
                .expect("the dropped answer closes the wait")
                .is_err()
        );
    }
}
