//! ADR 0092 T5: the two-shard prototype — `K` hub loops ("data shards") on one node
//! behind one [`HubTx`], each owning the clients of disjoint placement groups.
//!
//! What it measures is the split's cost per message, before the ADR is written: a
//! publish is matched on its publisher's shard, the subscribers that shard owns are
//! delivered there, and the rest are handed to their shard as one
//! [`HubCommand::ShardDeliver`] (the message and its clients), whose durable
//! completions come back as one [`HubCommand::ShardDone`] — two hand-offs per
//! cross-shard message. Routing state is not snapshotted in the prototype: the shard
//! that owns a client mirrors its filters, and whether it is connected, to every other
//! shard ([`HubCommand::ShardRoute`]); the others match on them and own nothing else
//! about the client. Everything peer-facing, retained, relayed or administrative goes
//! to the first shard, so the prototype is for one node.
//!
//! With one shard (the default, `node.hub_shards = 1`) nothing here runs: [`HubTx`] is
//! the loop's own sender, [`Hub::owns`] is always true, the mirror sends nothing.

use std::sync::Arc;

use bytes::Bytes;
use mqtt_codec::QoS;
use mqtt_core::{AppProperties, ClientId};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use super::lanes::AppendGate;
use super::{min_qos, parse_shared, ClientSubs, DurableOutcome, Hub, HubCommand};

/// The most shards a node may run (`MQTTD_HUB_SHARDS`): the prototype's bound, and
/// the width of [`super::forwarding::PendingPublish::shard_outstanding`]'s bits.
pub const MAX_SHARDS: usize = 4;

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
    /// included) for the hand-offs. One sender leaves the hub as it was.
    ///
    /// # Panics
    ///
    /// When `shard` is not an index into `txs`, or there are more than [`MAX_SHARDS`].
    pub fn set_shards(&mut self, shard: u8, txs: Vec<mpsc::UnboundedSender<HubCommand>>) {
        assert!(
            usize::from(shard) < txs.len() && txs.len() <= MAX_SHARDS,
            "shard {shard} of {} (at most {MAX_SHARDS})",
            txs.len()
        );
        self.shard = shard;
        self.shard_count = u8::try_from(txs.len()).expect("at most MAX_SHARDS");
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

    /// Tell every other shard what `client` subscribes to and whether it is connected,
    /// as of now: called wherever its filters or its presence change. Nothing with one
    /// shard.
    pub(super) fn mirror_routing(&mut self, client: &ClientId) {
        if self.shard_count <= 1 {
            return;
        }
        let mut filters: Vec<(String, QoS)> = self
            .subs
            .get(client)
            .into_iter()
            .flat_map(ClientSubs::iter)
            .map(|e| (String::from(&*e.filter), e.qos))
            .collect();
        let online = self.online.contains_key(client);
        let me = usize::from(self.shard);
        let others: Vec<&mpsc::UnboundedSender<HubCommand>> = self
            .shard_txs
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != me)
            .map(|(_, tx)| tx)
            .collect();
        for (n, tx) in others.iter().enumerate() {
            let filters = if n + 1 == others.len() {
                std::mem::take(&mut filters)
            } else {
                filters.clone()
            };
            let _ = tx.send(HubCommand::ShardRoute {
                client: client.clone(),
                filters,
                online,
            });
        }
    }

    /// Another shard's client, as it subscribes now: replace what this shard matches
    /// on for it. Nothing else about the client lives here.
    pub(super) fn apply_shard_route(
        &mut self,
        client: &ClientId,
        filters: Vec<(String, QoS)>,
        online: bool,
    ) {
        if self.owns(client) {
            debug!(client = %client.0, "a routing mirror for a client this shard owns; ignored");
            return;
        }
        self.table.remove_client(client);
        self.shared.remove_client(client);
        for (f, qos) in filters {
            if let Some((group, filter)) = parse_shared(&f) {
                self.shared
                    .subscribe(client.clone(), group, filter, qos, online);
            } else {
                let key = self.table.intern(&f);
                self.table.subscribe(client.clone(), key);
            }
        }
    }

    /// A subscriber of the publish being dispatched that another shard owns.
    pub(super) fn hand_off(&mut self, target: ShardTarget) {
        self.handoff.push(target);
    }

    /// Send the subscribers collected by [`hand_off`](Self::hand_off) to their shards,
    /// one [`HubCommand::ShardDeliver`] each, and owe the publisher's acknowledgement
    /// their answers when the publish is gated. A settle-window re-delivery hands off
    /// nothing: the other shard was told the publish once.
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
            if let Some(id) = gated {
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    p.shard_outstanding |= 1 << shard;
                }
            }
            if let Some(m) = &self.metrics {
                m.publish_forwarded("shard");
            }
            let _ = self.shard_txs[shard].send(HubCommand::ShardDeliver(Box::new(ShardDeliver {
                origin: self.shard,
                gate: gated,
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain,
                message_expiry,
                app: app.clone(),
                targets: batch,
            })));
        }
        if targets.capacity() > 0 && targets.is_empty() {
            self.handoff = targets;
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
                        // delivery (its mirror in flight): nothing was stored, and the
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

    fn answer_shard(&self, origin: u8, id: u64, outcome: DurableOutcome, reached: bool) {
        let Some(tx) = self.shard_txs.get(usize::from(origin)) else {
            return;
        };
        let _ = tx.send(HubCommand::ShardDone {
            shard: self.shard,
            id,
            outcome,
            reached,
        });
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
        match outcome {
            DurableOutcome::Ok => {
                p.stored = true;
                if reached {
                    p.ack_awaits_settle = false;
                }
                self.try_complete_pending(id);
            }
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
