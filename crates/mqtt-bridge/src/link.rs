//! One link per upstream ([ADR 0060] Amendment 1).
//!
//! A link owns two connections, its own session on the local cluster and the connection to
//! its upstream, and moves messages both ways between them. Each direction is a [`Flow`]:
//! messages taken from the source, their forwards on the way to the destination, and the
//! source acknowledgements that wait for those forwards.
//!
//! **Out, local cluster → upstream: the local session is the buffer.** The bridge keeps no
//! copy of these messages on disk. It acknowledges a message at the local cluster only when
//! the upstream has acknowledged every forward the message produced, in the order the
//! messages arrived [MQTT-4.6.0-2]. While the upstream is unreachable the link is not
//! connected to the local cluster at all, so the messages wait in the session there:
//!
//! 1. [`Link::provision`] creates the local session (`clean_start = false` and a Session
//!    Expiry Interval) if it does not exist yet, subscribes without retained messages, and
//!    disconnects.
//! 2. The link connects to the upstream, retrying with backoff.
//! 3. Only then does it connect to the local cluster ([`Link::local_allowed`]). A session
//!    that is gone (`session_present = 0`) is counted and audited, and subscribed again.
//! 4. Each message is forwarded as it arrives.
//! 5. Acknowledgements are released in order ([`Flow::release`]).
//! 6. When the upstream link fails, the link disconnects from the local cluster without
//!    acknowledging what is pending; the local cluster resends it ([`Link::remote_down`]).
//! 7. When the local link fails, the pending acknowledgements are discarded, so none is ever
//!    sent on another connection ([`Flow::abandon_resending_source`]).
//!
//! **In, upstream → local cluster: the spool is the fallback.** An upstream's session is clean
//! by default, so leaving it would lose what it publishes meanwhile; the link stays connected
//! to the upstream. While the local link is up it forwards each message and acknowledges the
//! upstream after the local cluster's PUBACK. While it is down it spools each message (fsync)
//! and acknowledges after the commit, and what was on its way to the local cluster when the
//! link failed goes to the spool too. When the local link is back the spool replays oldest
//! first, each message deleted only once the local cluster acknowledges it, and newer
//! messages follow.
//!
//! A direction holds at most `max_inflight_bytes`: past it the link stops reading that
//! direction's source, and reads again below half (§3). Toward a destination it keeps to the
//! Receive Maximum the destination granted.
//!
//! [ADR 0060]: ../../../docs/adr/0060-bridge-durability-and-ack-contract.md

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_codec::packet::Publish;
use mqtt_codec::properties::{Properties, Property};
use mqtt_codec::QoS;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::client::{
    subscription_options, ClientError, ConnectOptions, Event, Incoming, MqttClient, Outgoing,
    Session, CONNECT_TIMEOUT,
};
use crate::config::{BridgeConfig, HaMode};
use crate::forward::{owns, plan_forwards, read_hop_count, set_hop_count, Side};
use crate::metrics::{side_index, BridgeMetrics, CrossDirection};
use crate::spool::{message_bytes, Spool, SpoolError, SpooledMessage};

/// Reconnect backoff bounds.
const BACKOFF_START: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// A temporarily refused forward is published again after this, doubling per attempt…
const RETRY_FIRST: Duration = Duration::from_secs(1);
/// …up to this.
const RETRY_MAX: Duration = Duration::from_secs(30);
/// How long to wait before reading a spool again after it failed.
const SPOOL_RETRY: Duration = Duration::from_secs(1);
/// Events one wake handles before the flows are driven, so acknowledgements that arrive
/// together are released, and spool removals committed, together.
const EVENT_BATCH: usize = 512;

/// Everything one link needs, built by the engine from the configuration.
pub(crate) struct LinkSpec {
    /// The upstream's index in the configuration.
    pub index: usize,
    /// The upstream's name.
    pub name: String,
    /// The metrics name of this link's local side.
    pub local_side: String,
    /// How to connect this link's local session.
    pub local: ConnectOptions,
    /// How to connect the upstream.
    pub remote: ConnectOptions,
    /// What the local session subscribes to: the upstream's `out`/`both` filters.
    pub local_subs: Vec<(String, QoS)>,
    /// What the upstream connection subscribes to: its `in`/`both` filters.
    pub remote_subs: Vec<(String, QoS)>,
    /// The byte bound per direction (§3).
    pub max_inflight_bytes: usize,
    /// Messages from the upstream that the bridge holds for the local cluster (§7).
    pub inbound_spool: Arc<Spool>,
    /// An older bridge's spool of messages for this upstream, sent before the local session
    /// connects (§8).
    pub legacy_outbound: Option<Arc<Spool>>,
    /// Read by the `fss_bridge_connected` gauge.
    pub local_connected: Arc<AtomicBool>,
    /// Read by the `fss_bridge_connected` gauge.
    pub remote_connected: Arc<AtomicBool>,
}

/// Run one link until the task is aborted.
pub(crate) async fn run(spec: LinkSpec, cfg: Arc<BridgeConfig>, metrics: Arc<BridgeMetrics>) {
    let mut link = Link::new(spec, cfg, metrics);
    link.provision().await;
    link.serve().await;
}

/// What finishing a forward means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// One forward of the taken message with this sequence number.
    Taken(u64),
    /// The spooled message under this key: remove it from the spool.
    Spooled(u64),
    /// Nothing waits on it any more (its source acknowledgement was abandoned); only its
    /// packet id is still in use until the destination answers.
    Orphan,
}

/// One forward on its way to a destination.
#[derive(Debug)]
struct Fwd {
    origin: Origin,
    publish: Publish,
    /// Bytes counted against the bound for this forward alone: a spooled message's size.
    /// A taken message's bytes are counted once, on the message.
    bytes: usize,
    attempts: u32,
    /// Position in the order the flow took or loaded its forwards.
    order: u64,
}

/// A message taken from the source, waiting for its forwards.
#[derive(Debug)]
struct Taken {
    seq: u64,
    /// The source packet id to acknowledge; `None` for `QoS` 0 or once the source connection
    /// that delivered it has ended.
    ack: Option<u16>,
    /// Forwards not yet done.
    remaining: usize,
    bytes: usize,
}

/// A forward the destination refused for good.
#[derive(Debug)]
struct Refusal {
    topic: String,
    reason: u8,
}

/// What a PUBACK reason code means for the forward (§5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Accepted (0x00, or 0x10 no matching subscribers, or any code below 0x80).
    Delivered,
    /// Refused in a way a retry cannot change: not authorized (0x87), topic name invalid
    /// (0x90), payload format invalid (0x99).
    Refused,
    /// Any other error code: publish it again later.
    Retry,
}

fn verdict(reason: u8) -> Verdict {
    match reason {
        0x00..=0x7f => Verdict::Delivered,
        0x87 | 0x90 | 0x99 => Verdict::Refused,
        _ => Verdict::Retry,
    }
}

/// The wait before publishing a refused forward again: 1 s, doubling, at most 30 s.
fn retry_delay(attempts: u32) -> Duration {
    let doublings = attempts.saturating_sub(1).min(5);
    (RETRY_FIRST * (1u32 << doublings)).min(RETRY_MAX)
}

/// One direction of a link.
#[derive(Debug)]
struct Flow {
    /// Messages taken from the source, oldest first, until their forwards are done.
    taken: VecDeque<Taken>,
    next_seq: u64,
    next_order: u64,
    /// Whether this flow replays a spool before its queue.
    has_spool: bool,
    /// Spooled messages loaded for the destination, oldest first.
    replay: VecDeque<Fwd>,
    /// The last spool key loaded into `replay`.
    replay_cursor: Option<u64>,
    /// The spool had nothing after `replay_cursor` the last time it was read. The queue is
    /// sent only then, so nothing overtakes a spooled message.
    replay_done: bool,
    /// Taken messages' forwards waiting for the destination, oldest first.
    queue: VecDeque<Fwd>,
    /// Forwards on the wire, by destination packet id.
    inflight: HashMap<u16, Fwd>,
    /// Temporarily refused forwards and when to publish them again.
    retry: Vec<(Instant, Fwd)>,
    /// Packet ids a SUBSCRIBE on the destination connection is using.
    reserved: HashSet<u16>,
    next_pkid: u16,
    /// The destination's Receive Maximum.
    window: usize,
    /// Bytes held: taken messages plus spooled messages in memory.
    held: usize,
    /// Bytes of spooled messages in memory.
    spooled_held: usize,
    /// Whether the source's reads are paused (§3).
    paused: bool,
    /// Spool keys acknowledged and not yet removed (committed in one flush per wake).
    removals: Vec<u64>,
    /// Refusals not yet reported.
    refusals: Vec<Refusal>,
}

impl Flow {
    fn new(has_spool: bool) -> Self {
        Self {
            taken: VecDeque::new(),
            next_seq: 0,
            next_order: 0,
            has_spool,
            replay: VecDeque::new(),
            replay_cursor: None,
            replay_done: !has_spool,
            queue: VecDeque::new(),
            inflight: HashMap::new(),
            retry: Vec::new(),
            reserved: HashSet::new(),
            next_pkid: 0,
            window: usize::from(u16::MAX),
            held: 0,
            spooled_held: 0,
            paused: false,
            removals: Vec::new(),
            refusals: Vec::new(),
        }
    }

    fn bump_order(&mut self) -> u64 {
        let order = self.next_order;
        self.next_order += 1;
        order
    }

    /// Take a message from the source: `ack` is its packet id to acknowledge (if any),
    /// `forwards` what it must become at the destination, `bytes` its size.
    fn take(&mut self, ack: Option<u16>, forwards: Vec<Publish>, bytes: usize) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.held += bytes;
        self.taken.push_back(Taken {
            seq,
            ack,
            remaining: forwards.len(),
            bytes,
        });
        for publish in forwards {
            let order = self.bump_order();
            self.queue.push_back(Fwd {
                origin: Origin::Taken(seq),
                publish,
                bytes: 0,
                attempts: 0,
                order,
            });
        }
    }

    /// Whether another `QoS` 1 publish fits: inside the destination's Receive Maximum, with a
    /// packet id free for it.
    fn has_slot(&self) -> bool {
        self.inflight.len() < self.window
            && self.inflight.len() + self.reserved.len() < usize::from(u16::MAX)
    }

    /// A packet id no publish or subscribe on the destination connection is using. Callers
    /// ensure one is free ([`Self::has_slot`], or an empty connection for a subscribe).
    fn alloc_pkid(&mut self) -> u16 {
        loop {
            self.next_pkid = self.next_pkid.wrapping_add(1).max(1);
            if !self.inflight.contains_key(&self.next_pkid)
                && !self.reserved.contains(&self.next_pkid)
            {
                return self.next_pkid;
            }
        }
    }

    /// A packet id for a SUBSCRIBE on the destination connection, kept until its SUBACK.
    fn reserve(&mut self) -> u16 {
        let pkid = self.alloc_pkid();
        self.reserved.insert(pkid);
        pkid
    }

    /// Send what the destination has room for: the replay first, then the queue once the
    /// replay is done.
    fn send_ready(&mut self, dest: &Session) {
        loop {
            let from_replay = !self.replay.is_empty();
            let next = if from_replay {
                self.replay.front()
            } else if self.replay_done {
                self.queue.front()
            } else {
                None
            };
            let Some(next) = next else { break };
            let at_least_once = next.publish.qos != QoS::AtMostOnce;
            if at_least_once && !self.has_slot() {
                break;
            }
            let popped = if from_replay {
                self.replay.pop_front()
            } else {
                self.queue.pop_front()
            };
            let Some(mut fwd) = popped else { break };
            if at_least_once {
                let pkid = self.alloc_pkid();
                fwd.publish.pkid = Some(pkid);
                // A send that fails means the connection is ending; its `Closed` event takes
                // this forward back with the rest of what was in flight.
                let _ = dest.tx.send(Outgoing::Publish(fwd.publish.clone()));
                self.inflight.insert(pkid, fwd);
            } else {
                let Fwd {
                    origin,
                    mut publish,
                    bytes,
                    ..
                } = fwd;
                publish.pkid = None;
                let _ = dest.tx.send(Outgoing::Publish(publish));
                self.finish(origin, bytes);
            }
        }
    }

    /// A forward is done: acknowledged, written at `QoS` 0, refused for good, or spooled.
    fn finish(&mut self, origin: Origin, bytes: usize) {
        match origin {
            Origin::Taken(seq) => {
                let index = self
                    .taken
                    .front()
                    .and_then(|front| usize::try_from(seq.checked_sub(front.seq)?).ok());
                if let Some(t) = index.and_then(|i| self.taken.get_mut(i)) {
                    if t.seq == seq {
                        t.remaining = t.remaining.saturating_sub(1);
                    }
                }
            }
            Origin::Spooled(key) => {
                self.removals.push(key);
                self.held = self.held.saturating_sub(bytes);
                self.spooled_held = self.spooled_held.saturating_sub(bytes);
            }
            Origin::Orphan => {}
        }
    }

    /// The destination acknowledged packet id `pkid` with `reason`.
    fn on_puback(&mut self, pkid: u16, reason: u8, now: Instant) {
        // An unknown id is a duplicate or stale PUBACK: nothing waits on it.
        let Some(mut fwd) = self.inflight.remove(&pkid) else {
            return;
        };
        match verdict(reason) {
            Verdict::Delivered => self.finish(fwd.origin, fwd.bytes),
            Verdict::Refused => {
                if fwd.origin != Origin::Orphan {
                    self.refusals.push(Refusal {
                        topic: fwd.publish.topic.clone(),
                        reason,
                    });
                }
                self.finish(fwd.origin, fwd.bytes);
            }
            Verdict::Retry => {
                if fwd.origin == Origin::Orphan {
                    return;
                }
                fwd.attempts += 1;
                fwd.publish.pkid = None;
                debug!(
                    topic = %fwd.publish.topic,
                    reason = format_args!("0x{reason:02x}"),
                    attempts = fwd.attempts,
                    "the destination refused a forward for now; publishing it again later"
                );
                self.retry.push((now + retry_delay(fwd.attempts), fwd));
            }
        }
    }

    /// Put the retries that are due back at the front of their queues, oldest first.
    fn due_retries(&mut self, now: Instant) {
        if self.retry.iter().all(|(at, _)| *at > now) {
            return;
        }
        let (mut due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.retry)
            .into_iter()
            .partition(|(at, _)| *at <= now);
        self.retry = rest;
        due.sort_by_key(|(_, f)| std::cmp::Reverse(f.order));
        for (_, f) in due {
            if matches!(f.origin, Origin::Spooled(_)) {
                self.replay.push_front(f);
            } else {
                self.queue.push_front(f);
            }
        }
    }

    fn next_retry(&self) -> Option<Instant> {
        self.retry.iter().map(|(at, _)| *at).min()
    }

    /// Release, oldest first, the taken messages whose forwards are all done, and
    /// acknowledge each at `source` when it is connected. Stops at the first message still
    /// waiting, so acknowledgements leave in the order the messages arrived [MQTT-4.6.0-2].
    fn release(&mut self, source: Option<&Session>) {
        while self.taken.front().is_some_and(|t| t.remaining == 0) {
            let Some(t) = self.taken.pop_front() else {
                break;
            };
            self.held = self.held.saturating_sub(t.bytes);
            if let (Some(pkid), Some(src)) = (t.ack, source) {
                let _ = src.tx.send(Outgoing::PubAck(pkid));
            }
        }
    }

    /// The source connection ended and its broker resends everything unacknowledged from the
    /// session: drop what waits on it. Forwards already on the wire become orphans, their
    /// packet ids in use until the destination answers.
    fn abandon_resending_source(&mut self) {
        self.taken.clear();
        self.queue.retain(|f| !matches!(f.origin, Origin::Taken(_)));
        self.retry
            .retain(|(_, f)| !matches!(f.origin, Origin::Taken(_)));
        for f in self.inflight.values_mut() {
            if matches!(f.origin, Origin::Taken(_)) {
                f.origin = Origin::Orphan;
            }
        }
        self.paused = false;
        self.recount();
    }

    /// The source connection ended and nothing will resend what it delivered: keep
    /// forwarding it, since the bridge holds the only copy, but no acknowledgement can be sent
    /// any more.
    fn abandon_acks(&mut self) {
        for t in &mut self.taken {
            t.ack = None;
        }
        self.paused = false;
    }

    /// The destination connection ended. Returns the taken messages' forwards it had not
    /// acknowledged, oldest first, followed by the queue. Spooled messages are only
    /// forgotten: they are still in the spool, and the replay starts again from its
    /// beginning. Orphans are dropped.
    fn lose_destination(&mut self) -> Vec<Fwd> {
        let mut back: Vec<Fwd> = self
            .inflight
            .drain()
            .map(|(_, f)| f)
            .chain(std::mem::take(&mut self.retry).into_iter().map(|(_, f)| f))
            .filter(|f| matches!(f.origin, Origin::Taken(_)))
            .collect();
        back.sort_by_key(|f| f.order);
        for f in &mut back {
            f.publish.pkid = None;
        }
        back.extend(self.queue.drain(..));
        self.replay.clear();
        self.replay_cursor = None;
        self.replay_done = !self.has_spool;
        self.reserved.clear();
        self.recount();
        back
    }

    /// Recompute the bytes held from what is actually held.
    fn recount(&mut self) {
        let spooled = |f: &Fwd| {
            if matches!(f.origin, Origin::Spooled(_)) {
                f.bytes
            } else {
                0
            }
        };
        self.spooled_held = self.replay.iter().map(spooled).sum::<usize>()
            + self.inflight.values().map(spooled).sum::<usize>()
            + self.retry.iter().map(|(_, f)| spooled(f)).sum::<usize>();
        self.held = self.taken.iter().map(|t| t.bytes).sum::<usize>() + self.spooled_held;
    }

    /// Keep about `budget` bytes of spooled messages loaded for the destination.
    fn load_replay(&mut self, spool: &Spool, budget: usize) -> Result<(), SpoolError> {
        while !self.replay_done && self.spooled_held < budget {
            let batch = spool.peek_after(self.replay_cursor, budget - self.spooled_held)?;
            if batch.is_empty() {
                self.replay_done = true;
                break;
            }
            for (key, m) in batch {
                self.replay_cursor = Some(key);
                let bytes = message_bytes(&m);
                self.held += bytes;
                self.spooled_held += bytes;
                let order = self.bump_order();
                self.replay.push_back(Fwd {
                    origin: Origin::Spooled(key),
                    publish: to_publish(m),
                    bytes,
                    attempts: 0,
                    order,
                });
            }
        }
        Ok(())
    }

    /// Whether nothing loaded from the spool is still waiting for the destination.
    fn replay_settled(&self) -> bool {
        self.replay_done && self.spooled_held == 0 && self.removals.is_empty()
    }

    /// Pause the source's reads past `max` bytes held; resume below half of it.
    fn flow_control(&mut self, source: Option<&Session>, max: usize) {
        let Some(src) = source else { return };
        if !self.paused && self.held > max {
            if src.tx.send(Outgoing::PauseReads(true)).is_ok() {
                self.paused = true;
            }
        } else if self.paused
            && self.held <= max / 2
            && src.tx.send(Outgoing::PauseReads(false)).is_ok()
        {
            self.paused = false;
        }
    }
}

/// A spooled message as a PUBLISH (its packet id is assigned when it is sent).
fn to_publish(m: SpooledMessage) -> Publish {
    Publish {
        dup: false,
        qos: QoS::from_u8(m.qos).unwrap_or(QoS::AtMostOnce),
        retain: m.retain,
        topic: m.topic,
        pkid: None,
        properties: Properties(
            m.user_properties
                .into_iter()
                .map(|(k, v)| Property::UserProperty(k, v))
                .collect(),
        ),
        payload: Bytes::from(m.payload),
    }
}

/// A forward as a spooled message: its topic, payload, `QoS`, retain flag and User Properties
/// (the hop count among them).
fn to_spooled(p: &Publish) -> SpooledMessage {
    SpooledMessage {
        topic: p.topic.clone(),
        payload: p.payload.to_vec(),
        qos: p.qos as u8,
        retain: p.retain,
        user_properties: p
            .properties
            .0
            .iter()
            .filter_map(|prop| match prop {
                Property::UserProperty(k, v) => Some((k.clone(), v.clone())),
                _ => None,
            })
            .collect(),
    }
}

/// The next event of the connection in `slot`; never resolves while it is empty.
async fn recv(slot: &mut Option<Session>) -> Incoming {
    match slot {
        Some(session) => session
            .rx
            .recv()
            .await
            .unwrap_or(Incoming::Closed(ClientError::Closed)),
        None => std::future::pending().await,
    }
}

/// Subscribe `client` to `subs` with `retain_handling`, waiting for each SUBACK.
async fn subscribe_and_wait(
    client: &mut MqttClient,
    subs: &[(String, QoS)],
    retain_handling: u8,
) -> Result<(), ClientError> {
    for (i, (filter, qos)) in subs.iter().enumerate() {
        let pkid = u16::try_from(i + 1).unwrap_or(u16::MAX);
        client
            .subscribe_with(
                pkid,
                filter,
                *qos,
                subscription_options(filter, retain_handling),
            )
            .await?;
        let answer = tokio::time::timeout(CONNECT_TIMEOUT, async {
            loop {
                if let Event::SubAck {
                    pkid: id,
                    return_codes,
                } = client.next_event().await?
                {
                    if id == pkid {
                        return Ok::<_, ClientError>(return_codes);
                    }
                }
            }
        })
        .await
        .map_err(|_| ClientError::Protocol(format!("no SUBACK for {filter:?}")))??;
        if answer.iter().any(|code| *code >= 0x80) {
            warn!(filter = %filter, codes = ?answer, "the local cluster refused a subscription");
        }
    }
    Ok(())
}

/// One upstream's link: its two connections and both directions between them.
struct Link {
    spec: LinkSpec,
    cfg: Arc<BridgeConfig>,
    metrics: Arc<BridgeMetrics>,
    local: Option<Session>,
    remote: Option<Session>,
    local_due: Instant,
    remote_due: Instant,
    local_backoff: mqtt_core::Backoff,
    remote_backoff: mqtt_core::Backoff,
    /// Local cluster → upstream.
    out: Flow,
    /// Upstream → local cluster.
    inn: Flow,
    /// [`Self::provision`] created the local session in this run, without its retained
    /// messages, so the first real subscribe asks for them.
    fresh: bool,
    /// The older bridge's spool for this upstream, until it is drained.
    legacy: Option<Arc<Spool>>,
    /// When to read a spool again after it failed.
    spool_retry: Option<Instant>,
    /// Whether the current run of spool refusals has been logged.
    spool_refusal_logged: bool,
}

impl Link {
    fn new(spec: LinkSpec, cfg: Arc<BridgeConfig>, metrics: Arc<BridgeMetrics>) -> Self {
        let now = Instant::now();
        let legacy = spec.legacy_outbound.clone();
        Self {
            out: Flow::new(legacy.is_some()),
            inn: Flow::new(true),
            spec,
            cfg,
            metrics,
            local: None,
            remote: None,
            local_due: now,
            remote_due: now,
            local_backoff: mqtt_core::Backoff::new(BACKOFF_START, BACKOFF_MAX),
            remote_backoff: mqtt_core::Backoff::new(BACKOFF_START, BACKOFF_MAX),
            fresh: false,
            legacy,
            spool_retry: None,
            spool_refusal_logged: false,
        }
    }

    /// Whether traffic flows from the local cluster to the upstream.
    fn has_out(&self) -> bool {
        !self.spec.local_subs.is_empty()
    }

    /// Whether traffic flows from the upstream to the local cluster.
    fn has_in(&self) -> bool {
        !self.spec.remote_subs.is_empty()
    }

    /// Whether the local session may connect now. With traffic out to the upstream it is the
    /// source of that traffic, so it connects only once the upstream is up and an older
    /// bridge's spool for it is drained: a connected source with nowhere to send would only
    /// pile messages up in the bridge and the broker's in-memory backlog (Amendment 1 §1).
    fn local_allowed(&self) -> bool {
        if !self.has_out() {
            return self.has_in();
        }
        self.remote.is_some() && self.legacy.is_none()
    }

    /// Step 1: make sure the local session exists before anything waits on the upstream, so
    /// the local cluster keeps what is published meanwhile. A new session is subscribed
    /// without retained messages (retain handling 2); the first real connect asks for them.
    async fn provision(&mut self) {
        if !self.has_out() {
            return;
        }
        let mut opts = self.spec.local.clone();
        // Only the session is checked here: let the broker send at most one message before
        // the disconnect, which leaves it unacknowledged in the session.
        opts.receive_maximum = Some(1);
        loop {
            match MqttClient::connect(&opts).await {
                Ok(mut client) => {
                    self.metrics.reconnect(side_index(self.spec.index, false));
                    self.local_backoff.reset();
                    if client.granted().session_present {
                        client.disconnect().await;
                        debug!(upstream = %self.spec.name, "the local session exists");
                        return;
                    }
                    match subscribe_and_wait(&mut client, &self.spec.local_subs, 2).await {
                        Ok(()) => {
                            client.disconnect().await;
                            self.fresh = true;
                            info!(
                                upstream = %self.spec.name,
                                client_id = %self.spec.local.client_id,
                                "created the local session; the local cluster keeps messages \
                                 for this upstream from now on"
                            );
                            return;
                        }
                        Err(e) => warn!(
                            upstream = %self.spec.name, error = %e,
                            "could not subscribe the new local session; retrying"
                        ),
                    }
                }
                Err(e) => warn!(
                    upstream = %self.spec.name, addr = %self.spec.local.addr, error = %e,
                    "could not connect to the local cluster to set up the session; retrying"
                ),
            }
            tokio::time::sleep(self.local_backoff.next_delay()).await;
        }
    }

    /// Steps 2–7: connect what may connect, move messages, and react to failures.
    async fn serve(&mut self) {
        loop {
            if self.remote.is_none() && Instant::now() >= self.remote_due {
                self.connect_remote().await;
            }
            if self.local.is_none() && self.local_allowed() && Instant::now() >= self.local_due {
                self.connect_local().await;
            }
            self.drive();

            let wake = self.next_wake();
            let first = tokio::select! {
                ev = recv(&mut self.remote) => Some((true, ev)),
                ev = recv(&mut self.local) => Some((false, ev)),
                () = sleep_until(wake) => None,
            };
            let Some((from_remote, ev)) = first else {
                continue;
            };
            self.handle(from_remote, ev);
            // Take what else is already waiting before driving the flows once for all of it.
            let mut handled = 1;
            while handled < EVENT_BATCH {
                let mut any = false;
                if let Some(ev) = self.remote.as_mut().and_then(|s| s.rx.try_recv().ok()) {
                    self.handle(true, ev);
                    any = true;
                    handled += 1;
                }
                if let Some(ev) = self.local.as_mut().and_then(|s| s.rx.try_recv().ok()) {
                    self.handle(false, ev);
                    any = true;
                    handled += 1;
                }
                if !any {
                    break;
                }
            }
        }
    }

    fn next_wake(&self) -> Option<Instant> {
        [
            self.remote.is_none().then_some(self.remote_due),
            (self.local.is_none() && self.local_allowed()).then_some(self.local_due),
            self.out.next_retry(),
            self.inn.next_retry(),
            self.spool_retry,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    async fn connect_remote(&mut self) {
        match MqttClient::connect(&self.spec.remote).await {
            Ok(client) => {
                self.remote_backoff.reset();
                let session = client.spawn();
                self.metrics.reconnect(side_index(self.spec.index, true));
                self.spec.remote_connected.store(true, Ordering::SeqCst);
                info!(
                    upstream = %self.spec.name, addr = %self.spec.remote.addr,
                    subs = self.spec.remote_subs.len(), "upstream connected"
                );
                self.out.window = usize::from(session.granted.receive_maximum);
                self.inn.paused = false;
                // A resumed session keeps its subscriptions; re-subscribing with retain
                // handling 1 sends no retained messages for them.
                let handling = u8::from(session.granted.session_present);
                for (filter, qos) in &self.spec.remote_subs {
                    let pkid = self.out.reserve();
                    let _ = session.tx.send(Outgoing::Subscribe {
                        pkid,
                        filter: filter.clone(),
                        qos: *qos,
                        options: subscription_options(filter, handling),
                    });
                }
                self.remote = Some(session);
            }
            Err(e) => {
                let delay = self.remote_backoff.next_delay();
                warn!(
                    upstream = %self.spec.name, addr = %self.spec.remote.addr, error = %e,
                    retry_in = ?delay, "upstream connect failed"
                );
                self.remote_due = Instant::now() + delay;
            }
        }
    }

    async fn connect_local(&mut self) {
        match MqttClient::connect(&self.spec.local).await {
            Ok(client) => {
                self.local_backoff.reset();
                let session = client.spawn();
                self.metrics.reconnect(side_index(self.spec.index, false));
                self.spec.local_connected.store(true, Ordering::SeqCst);
                let present = session.granted.session_present;
                info!(
                    upstream = %self.spec.name, addr = %self.spec.local.addr,
                    session_present = present, "local session connected"
                );
                if self.has_out() && !present && self.cfg.local_session_expiry() > 0 {
                    warn!(
                        upstream = %self.spec.name,
                        client_id = %self.spec.local.client_id,
                        "the local session was gone: messages queued in it, or published while \
                         it did not exist, will not reach this upstream"
                    );
                    self.metrics
                        .session_lost(side_index(self.spec.index, false), &self.spec.local_side);
                }
                // Retained messages are asked for when the session is new (or was set up
                // without them); a resumed session's subscriptions send none (§6).
                let handling = u8::from(present && !self.fresh);
                self.fresh = false;
                self.inn.window = usize::from(session.granted.receive_maximum);
                self.out.paused = false;
                for (filter, qos) in &self.spec.local_subs {
                    let pkid = self.inn.reserve();
                    let _ = session.tx.send(Outgoing::Subscribe {
                        pkid,
                        filter: filter.clone(),
                        qos: *qos,
                        options: subscription_options(filter, handling),
                    });
                }
                self.local = Some(session);
            }
            Err(e) => {
                let delay = self.local_backoff.next_delay();
                warn!(
                    upstream = %self.spec.name, addr = %self.spec.local.addr, error = %e,
                    retry_in = ?delay, "local connect failed"
                );
                self.local_due = Instant::now() + delay;
            }
        }
    }

    fn handle(&mut self, from_remote: bool, ev: Incoming) {
        let now = Instant::now();
        match (from_remote, ev) {
            (true, Incoming::Publish(p)) => self.take(Side::Upstream(self.spec.index), &p),
            (false, Incoming::Publish(p)) => self.take(Side::Local, &p),
            (true, Incoming::PubAck { pkid, reason }) => self.out.on_puback(pkid, reason, now),
            (false, Incoming::PubAck { pkid, reason }) => self.inn.on_puback(pkid, reason, now),
            (remote, Incoming::SubAck { pkid, return_codes }) => {
                let flow = if remote { &mut self.out } else { &mut self.inn };
                flow.reserved.remove(&pkid);
                if return_codes.iter().any(|code| *code >= 0x80) {
                    warn!(
                        upstream = %self.spec.name,
                        side = if remote { "upstream" } else { "local" },
                        codes = ?return_codes, "a subscription was refused"
                    );
                }
            }
            (true, Incoming::Closed(err)) => self.remote_down(&err),
            (false, Incoming::Closed(err)) => self.local_down(&err),
        }
    }

    /// Take a message that arrived from `source`: plan its forwards across the boundary and
    /// queue them on the flow toward the other side.
    fn take(&mut self, source: Side, p: &Publish) {
        let ack = p.pkid.filter(|_| p.qos != QoS::AtMostOnce);
        let forwards = self.plan(source, p);
        let bytes = p.topic.len() + p.payload.len();
        match source {
            Side::Local => self.out.take(ack, forwards, bytes),
            Side::Upstream(_) => self.inn.take(ack, forwards, bytes),
        }
    }

    /// The forwards a message from `source` becomes: the pure policy of [`plan_forwards`],
    /// restricted to this link's upstream, each counted and audited as it is planned.
    fn plan(&self, source: Side, p: &Publish) -> Vec<Publish> {
        // Partitioned HA (ADR 0059): every instance receives the full stream but forwards
        // only the topics it owns.
        if self.cfg.ha == HaMode::Partitioned && !owns(&p.topic, self.cfg.total, self.cfg.instance)
        {
            return Vec::new();
        }
        let hop = read_hop_count(&p.properties);
        let planned = plan_forwards(&self.cfg, source, &p.topic, hop);
        if planned.is_empty() && hop >= self.cfg.hop_count_limit {
            self.metrics.dropped_hop_limit(&p.topic, hop);
            debug!(topic = %p.topic, hop, "hop limit reached; dropped");
        }
        let (dir, mine) = match source {
            Side::Local => (CrossDirection::Out, Side::Upstream(self.spec.index)),
            Side::Upstream(_) => (CrossDirection::In, Side::Local),
        };
        planned
            .into_iter()
            .filter(|f| f.dest == mine)
            .map(|f| {
                self.metrics
                    .forwarded(&self.spec.name, dir, &p.topic, &f.topic, p.payload.len());
                Publish {
                    dup: false,
                    qos: QoS::from_u8(f.qos).unwrap_or(QoS::AtMostOnce),
                    // Preserve the source retain bit unless the rule opts out (issue #189).
                    retain: p.retain && f.retain_ok,
                    topic: f.topic,
                    pkid: None,
                    properties: set_hop_count(&p.properties, hop + 1),
                    payload: p.payload.clone(),
                }
            })
            .collect()
    }

    /// The upstream connection ended (step 6).
    fn remote_down(&mut self, err: &ClientError) {
        // Acknowledgements the upstream earned before it went (a PUBACK handled in the same
        // batch as the close) still go to the local cluster, ahead of the disconnect below,
        // rather than make it resend messages that were delivered.
        self.out.release(self.local.as_ref());
        self.remote = None;
        self.spec.remote_connected.store(false, Ordering::SeqCst);
        let delay = self.remote_backoff.next_delay();
        self.remote_due = Instant::now() + delay;
        warn!(upstream = %self.spec.name, error = %err, retry_in = ?delay, "upstream disconnected");
        // In: the source ended. What it delivered is still forwarded (with a clean session
        // nothing resends it), but it can no longer be acknowledged.
        self.inn.abandon_acks();
        // Out: the destination ended. Its packet ids went with it.
        let _ = self.out.lose_destination();
        if self.has_out() {
            if let Some(local) = self.local.take() {
                // What came in from the upstream and is still on its way to the local cluster
                // is the only copy: spool it before leaving.
                self.inbound_lost_destination();
                let _ = local.tx.send(Outgoing::Disconnect);
                self.spec.local_connected.store(false, Ordering::SeqCst);
                info!(
                    upstream = %self.spec.name,
                    "disconnected from the local cluster until the upstream is back; the local \
                     session keeps its messages meanwhile"
                );
            }
            // Nothing pending was acknowledged: the local cluster resends all of it.
            self.out.abandon_resending_source();
            self.local_due = Instant::now();
        }
    }

    /// The local connection ended (step 7).
    fn local_down(&mut self, err: &ClientError) {
        self.local = None;
        self.spec.local_connected.store(false, Ordering::SeqCst);
        let delay = self.local_backoff.next_delay();
        self.local_due = Instant::now() + delay;
        warn!(upstream = %self.spec.name, error = %err, retry_in = ?delay, "local session disconnected");
        // Out: the source ended. Its acknowledgements must never go out on another connection,
        // and the local cluster resends everything unacknowledged.
        self.out.abandon_resending_source();
        // In: the destination ended.
        self.inbound_lost_destination();
    }

    /// The local link is gone: what came in from the upstream and was not yet acknowledged by
    /// the local cluster goes back to the queue, oldest first, and from there to the spool.
    fn inbound_lost_destination(&mut self) {
        let back = self.inn.lose_destination();
        self.inn.queue = back.into();
        self.inn.recount();
        self.spool_queue();
    }

    /// While the local link is down: move the queued forwards of messages from the upstream
    /// into the spool, oldest first, each done once spooled (so the upstream is acknowledged).
    /// Stops at the first one the spool refuses (full, or a disk error): it and everything
    /// after it wait in memory unacknowledged, so the spool never holds a message newer than
    /// one it refused.
    fn spool_queue(&mut self) {
        while let Some(front) = self.inn.queue.front() {
            match self.spec.inbound_spool.push(&to_spooled(&front.publish)) {
                Ok(()) => {
                    self.spool_refusal_logged = false;
                    if let Some(f) = self.inn.queue.pop_front() {
                        self.inn.finish(f.origin, f.bytes);
                    }
                }
                Err(e) => {
                    if !self.spool_refusal_logged {
                        warn!(
                            upstream = %self.spec.name, error = %e,
                            "the spool refused a message for the local cluster; holding it, and \
                             what follows, in memory without acknowledging the upstream"
                        );
                        self.spool_refusal_logged = true;
                    }
                    break;
                }
            }
        }
    }

    /// Move messages: load replays, send what the destinations have room for, commit spool
    /// removals, report refusals, release acknowledgements in order, and bound what each
    /// direction holds.
    fn drive(&mut self) {
        let now = Instant::now();
        let budget = self.spec.max_inflight_bytes;
        self.out.due_retries(now);
        self.inn.due_retries(now);
        if self.spool_retry.is_some_and(|at| at <= now) {
            self.spool_retry = None;
        }

        // Out: an older bridge's spool first, then the local cluster's messages.
        if let Some(remote) = &self.remote {
            if let Some(legacy) = &self.legacy {
                if self.spool_retry.is_none() {
                    if let Err(e) = self.out.load_replay(legacy, budget / 2) {
                        warn!(upstream = %self.spec.name, error = %e, "could not read the old spool");
                        self.spool_retry = Some(now + SPOOL_RETRY);
                    }
                }
            }
            self.out.send_ready(remote);
        }

        // In: the spool first, then the upstream's newer messages; into the spool while the
        // local link is down.
        match &self.local {
            Some(local) => {
                if self.spool_retry.is_none() {
                    if let Err(e) = self.inn.load_replay(&self.spec.inbound_spool, budget / 2) {
                        warn!(upstream = %self.spec.name, error = %e, "could not read the spool");
                        self.spool_retry = Some(now + SPOOL_RETRY);
                    }
                }
                self.inn.send_ready(local);
            }
            None => self.spool_queue(),
        }

        // One flush for every spooled message acknowledged since the last wake. A failed
        // removal leaves them spooled, to be sent again: at-least-once.
        if !self.inn.removals.is_empty() {
            let keys = std::mem::take(&mut self.inn.removals);
            if let Err(e) = self.spec.inbound_spool.remove_many(&keys) {
                warn!(upstream = %self.spec.name, error = %e, "could not remove replayed messages from the spool");
            }
        }
        if !self.out.removals.is_empty() {
            let keys = std::mem::take(&mut self.out.removals);
            if let Some(legacy) = &self.legacy {
                if let Err(e) = legacy.remove_many(&keys) {
                    warn!(upstream = %self.spec.name, error = %e, "could not remove replayed messages from the old spool");
                }
            }
        }

        for r in std::mem::take(&mut self.out.refusals) {
            self.metrics.refused(
                side_index(self.spec.index, true),
                &self.spec.name,
                &r.topic,
                r.reason,
            );
        }
        for r in std::mem::take(&mut self.inn.refusals) {
            self.metrics.refused(
                side_index(self.spec.index, false),
                &self.spec.local_side,
                &r.topic,
                r.reason,
            );
        }

        self.out.release(self.local.as_ref());
        self.inn.release(self.remote.as_ref());
        self.out.flow_control(self.local.as_ref(), budget);
        self.inn.flow_control(self.remote.as_ref(), budget);

        // The older bridge's spool is done once everything in it was acknowledged.
        if self.legacy.is_some() && self.remote.is_some() && self.out.replay_settled() {
            if let Some(legacy) = &self.legacy {
                if legacy.is_empty().unwrap_or(false) {
                    info!(upstream = %self.spec.name, "drained the spool an older bridge left for this upstream");
                    self.legacy = None;
                    self.out.has_spool = false;
                    self.out.replay_done = true;
                }
            }
        }
    }
}

/// Sleep until `at`, or forever when there is nothing to wake for.
async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publish(topic: &str, qos: QoS) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            topic: topic.to_string(),
            pkid: None,
            properties: Properties::new(),
            payload: Bytes::from_static(b"x"),
        }
    }

    /// A destination session whose commands the test reads.
    fn dest(window: u16) -> (Session, tokio::sync::mpsc::UnboundedReceiver<Outgoing>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (_ev_tx, ev_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Session {
                tx,
                rx: ev_rx,
                granted: crate::client::Granted {
                    session_present: false,
                    receive_maximum: window,
                    keep_alive: 30,
                },
            },
            rx,
        )
    }

    fn sent(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outgoing>) -> Vec<Outgoing> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    fn pkids(out: &[Outgoing]) -> Vec<u16> {
        out.iter()
            .filter_map(|o| match o {
                Outgoing::Publish(p) => p.pkid,
                _ => None,
            })
            .collect()
    }

    #[test]
    fn acknowledgements_leave_in_arrival_order_whatever_order_the_destination_answers_in() {
        let (dst, mut dst_rx) = dest(10);
        let (src, mut src_rx) = dest(10);
        let mut flow = Flow::new(false);
        flow.take(Some(7), vec![publish("a", QoS::AtLeastOnce)], 1);
        flow.take(Some(8), vec![publish("b", QoS::AtLeastOnce)], 1);
        flow.send_ready(&dst);
        let ids = pkids(&sent(&mut dst_rx));
        assert_eq!(ids.len(), 2);
        // The destination answers the second first: nothing may be acknowledged yet.
        flow.on_puback(ids[1], 0, Instant::now());
        flow.release(Some(&src));
        assert!(
            sent(&mut src_rx).is_empty(),
            "8 must wait for 7 [MQTT-4.6.0-2]"
        );
        flow.on_puback(ids[0], 0, Instant::now());
        flow.release(Some(&src));
        let acks: Vec<u16> = sent(&mut src_rx)
            .into_iter()
            .filter_map(|o| match o {
                Outgoing::PubAck(id) => Some(id),
                _ => None,
            })
            .collect();
        assert_eq!(acks, vec![7, 8]);
        assert_eq!(flow.held, 0);
    }

    #[test]
    fn a_message_with_two_forwards_waits_for_both() {
        let (dst, mut dst_rx) = dest(10);
        let (src, mut src_rx) = dest(10);
        let mut flow = Flow::new(false);
        flow.take(
            Some(1),
            vec![
                publish("a", QoS::AtLeastOnce),
                publish("b", QoS::AtLeastOnce),
            ],
            1,
        );
        flow.send_ready(&dst);
        let ids = pkids(&sent(&mut dst_rx));
        flow.on_puback(ids[0], 0, Instant::now());
        flow.release(Some(&src));
        assert!(sent(&mut src_rx).is_empty());
        flow.on_puback(ids[1], 0, Instant::now());
        flow.release(Some(&src));
        assert_eq!(sent(&mut src_rx).len(), 1);
    }

    #[test]
    fn the_destination_window_bounds_what_is_on_the_wire() {
        let (dst, mut dst_rx) = dest(2);
        let mut flow = Flow::new(false);
        flow.window = 2; // the link sets it from the destination's CONNACK
        for _ in 0..5 {
            flow.take(None, vec![publish("a", QoS::AtLeastOnce)], 1);
        }
        flow.send_ready(&dst);
        let ids = pkids(&sent(&mut dst_rx));
        assert_eq!(ids.len(), 2, "Receive Maximum 2");
        flow.on_puback(ids[0], 0, Instant::now());
        flow.send_ready(&dst);
        assert_eq!(pkids(&sent(&mut dst_rx)).len(), 1);
        // QoS 0 needs no slot and is done when written.
        flow.take(None, vec![publish("q0", QoS::AtMostOnce)], 1);
        flow.queue.rotate_right(1); // put it at the front
        flow.send_ready(&dst);
        assert_eq!(sent(&mut dst_rx).len(), 1);
    }

    #[test]
    fn a_permanent_refusal_is_done_and_reported_and_a_temporary_one_waits_for_a_retry() {
        let (dst, mut dst_rx) = dest(10);
        let (src, mut src_rx) = dest(10);
        let mut flow = Flow::new(false);
        flow.take(Some(1), vec![publish("denied", QoS::AtLeastOnce)], 1);
        flow.take(Some(2), vec![publish("busy", QoS::AtLeastOnce)], 1);
        flow.send_ready(&dst);
        let ids = pkids(&sent(&mut dst_rx));
        let now = Instant::now();
        flow.on_puback(ids[0], 0x87, now); // not authorized: permanent
        flow.on_puback(ids[1], 0x97, now); // quota exceeded: temporary
        flow.release(Some(&src));
        assert_eq!(
            sent(&mut src_rx).len(),
            1,
            "only the refused one is acknowledged"
        );
        assert_eq!(flow.refusals.len(), 1);
        assert_eq!(flow.refusals[0].topic, "denied");
        assert_eq!(flow.next_retry(), Some(now + RETRY_FIRST));
        // Not due yet: nothing is sent.
        flow.due_retries(now);
        flow.send_ready(&dst);
        assert!(sent(&mut dst_rx).is_empty());
        // Due: published again as a new message, and acknowledged at the source once accepted.
        flow.due_retries(now + RETRY_FIRST);
        flow.send_ready(&dst);
        let again = pkids(&sent(&mut dst_rx));
        assert_eq!(again.len(), 1);
        flow.on_puback(again[0], 0, now);
        flow.release(Some(&src));
        assert_eq!(sent(&mut src_rx).len(), 1);
    }

    #[test]
    fn retry_delays_double_up_to_thirty_seconds() {
        let delays: Vec<u64> = (1..=8).map(|a| retry_delay(a).as_secs()).collect();
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30, 30]);
    }

    #[test]
    fn a_resending_source_that_ends_leaves_no_acknowledgement_to_cross_connections() {
        let (dst, mut dst_rx) = dest(10);
        let (src, mut src_rx) = dest(10);
        let mut flow = Flow::new(false);
        flow.take(Some(5), vec![publish("a", QoS::AtLeastOnce)], 1);
        flow.take(Some(6), vec![publish("b", QoS::AtLeastOnce)], 1);
        flow.send_ready(&dst);
        let ids = pkids(&sent(&mut dst_rx));
        // The source connection ends; then the destination acknowledges.
        flow.abandon_resending_source();
        flow.on_puback(ids[0], 0, Instant::now());
        flow.on_puback(ids[1], 0, Instant::now());
        // A new source connection must not receive the old connection's acknowledgements.
        flow.release(Some(&src));
        assert!(sent(&mut src_rx).is_empty(), "#1103 finding 2");
        assert!(flow.inflight.is_empty());
        assert_eq!(flow.held, 0);
    }

    #[test]
    fn orphaned_packet_ids_stay_in_use_until_the_destination_answers() {
        let (dst, mut dst_rx) = dest(10);
        let mut flow = Flow::new(false);
        flow.take(Some(1), vec![publish("a", QoS::AtLeastOnce)], 1);
        flow.send_ready(&dst);
        let orphan = pkids(&sent(&mut dst_rx))[0];
        flow.abandon_resending_source();
        for _ in 0..100 {
            flow.take(None, vec![publish("b", QoS::AtLeastOnce)], 1);
        }
        flow.send_ready(&dst);
        assert!(
            !pkids(&sent(&mut dst_rx)).contains(&orphan),
            "an id is in use until its PUBACK"
        );
    }

    #[test]
    fn losing_the_destination_hands_back_unacknowledged_forwards_oldest_first() {
        let (dst, mut dst_rx) = dest(2);
        let mut flow = Flow::new(false);
        flow.window = 2;
        for topic in ["a", "b", "c"] {
            flow.take(Some(1), vec![publish(topic, QoS::AtLeastOnce)], 1);
        }
        flow.send_ready(&dst); // a and b on the wire, c queued
        let _ = sent(&mut dst_rx);
        let back: Vec<String> = flow
            .lose_destination()
            .into_iter()
            .map(|f| f.publish.topic)
            .collect();
        assert_eq!(back, vec!["a", "b", "c"]);
        assert!(flow.inflight.is_empty());
    }

    #[test]
    fn a_replay_is_sent_before_the_queue_and_removed_only_when_acknowledged() {
        let spool = Spool::in_memory(100);
        for topic in ["s1", "s2"] {
            spool
                .push(&SpooledMessage {
                    topic: topic.into(),
                    payload: b"p".to_vec(),
                    qos: 1,
                    retain: false,
                    user_properties: vec![],
                })
                .unwrap();
        }
        let (dst, mut dst_rx) = dest(10);
        let mut flow = Flow::new(true);
        flow.take(Some(9), vec![publish("live", QoS::AtLeastOnce)], 1);
        // The queue waits for the replay.
        flow.send_ready(&dst);
        assert!(sent(&mut dst_rx).is_empty());
        flow.load_replay(&spool, 1 << 20).unwrap();
        flow.send_ready(&dst);
        let topics: Vec<String> = sent(&mut dst_rx)
            .into_iter()
            .filter_map(|o| match o {
                Outgoing::Publish(p) => Some(p.topic),
                _ => None,
            })
            .collect();
        assert_eq!(topics, vec!["s1", "s2", "live"]);
        // Nothing left the spool before its acknowledgement.
        assert_eq!(spool.len().unwrap(), 2);
        let s1 = flow
            .inflight
            .iter()
            .find(|(_, f)| f.publish.topic == "s1")
            .map(|(id, _)| *id)
            .unwrap();
        flow.on_puback(s1, 0, Instant::now());
        spool
            .remove_many(&std::mem::take(&mut flow.removals))
            .unwrap();
        assert_eq!(spool.len().unwrap(), 1);
        // Losing the destination forgets the loaded replay; it starts again from the spool.
        let _ = flow.lose_destination();
        assert!(!flow.replay_done);
        flow.load_replay(&spool, 1 << 20).unwrap();
        assert_eq!(flow.replay.len(), 1);
        assert_eq!(flow.replay[0].publish.topic, "s2");
    }

    #[test]
    fn verdicts_follow_the_reason_code() {
        assert_eq!(verdict(0x00), Verdict::Delivered);
        assert_eq!(verdict(0x10), Verdict::Delivered); // no matching subscribers
        for code in [0x87, 0x90, 0x99] {
            assert_eq!(verdict(code), Verdict::Refused, "0x{code:02x}");
        }
        for code in [0x80, 0x83, 0x91, 0x97] {
            assert_eq!(verdict(code), Verdict::Retry, "0x{code:02x}");
        }
    }
}
