//! Write elision on the durable `QoS` 1 path (ADR 0089).
//!
//! A `QoS` 1 delivery to a persistent session whose subscriber is online, with no write
//! for it still in flight, may be sent at once and HELD instead of written first: the publisher's obligation for that session is then discharged by whichever
//! comes first, the subscriber's PUBACK (no write, no replication) or the durable write
//! the window falls back to. A node that dies inside a window has acknowledged nobody
//! for that delivery, so the publisher resends it; nothing acknowledged is lost.
//!
//! **Order (D5, Amendment 1 A2).** A held delivery has no offset. A session's held
//! deliveries therefore fall back TOGETHER and in send order, and while one is held every
//! later delivery is written, so nothing written can overtake a delivery still held. A
//! session with a write still in flight is not eligible either: that delivery goes on the
//! wire after its write, and a held one would overtake it. One already written and sent,
//! still unacknowledged, does not block: it is ahead on the wire and in the store.
//!
//! `durable.write_elision_ms = 0` (the default) builds none of this state, and every
//! delivery takes the path it took before.

#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258).
use super::*;
use std::collections::VecDeque;

/// The most deliveries a node holds unwritten at once (D7). Past it a delivery is
/// written as before.
pub(super) const MAX_ELIDED: usize = 16_384;
/// The most payload bytes a node holds unwritten at once (D7).
pub(super) const MAX_ELIDED_BYTES: usize = 64 << 20;
/// Consecutive window timeouts, with no PUBACK discharging a held delivery between them,
/// after which a session stops being held (Amendment 1 A3).
pub(super) const BACKOFF_STRIKES: u8 = 3;
/// How long a session that keeps missing the window is written directly instead.
pub(super) const BACKOFF: Duration = Duration::from_secs(2);

/// The window a new hold gets: the configured one, halved once the node holds more than
/// three quarters of the D7 bound in entries or bytes (Amendment 1 A4). At the bound
/// itself `may_elide` stops new holds.
pub(super) fn window_in_force(window: Duration, entries: usize, bytes: usize) -> Duration {
    if entries > MAX_ELIDED / 4 * 3 || bytes > MAX_ELIDED_BYTES / 4 * 3 {
        window / 2
    } else {
        window
    }
}

/// A session's record of missing the window (Amendment 1 A3).
#[derive(Debug, Default)]
pub(super) struct Backoff {
    /// Window timeouts since the last discharge.
    strikes: u8,
    /// Not held again before this.
    until: Option<Instant>,
}

/// One delivery on the wire and held: what a fallback write needs.
#[derive(Debug)]
pub(super) struct Elided {
    pub(super) pkid: u16,
    pub(super) message: Message,
    pub(super) message_expiry: Option<u32>,
    pub(super) gate: AppendGate,
    pub(super) deadline: Instant,
    /// When it went on the wire: the hold time a discharge reports.
    pub(super) sent: Instant,
}

/// The node's held deliveries.
#[derive(Debug, Default)]
pub(super) struct ElisionState {
    /// `None`: elision off (`durable.write_elision_ms = 0`).
    pub(super) window: Option<Duration>,
    /// Per session, in send order.
    pub(super) by_client: HashMap<ClientId, VecDeque<Elided>>,
    /// `(deadline, session)`, oldest first: the window is fixed, so arrival order is
    /// deadline order. A session may appear more than once, or after it was flushed.
    pub(super) deadlines: VecDeque<(Instant, ClientId)>,
    pub(super) entries: usize,
    pub(super) bytes: usize,
    /// Deliveries discharged by the subscriber's PUBACK, and fallbacks (exported by
    /// reason as `mqttd_durable_elision_fallback_total`).
    pub(super) discharged: u64,
    pub(super) fallbacks: u64,
    /// Sessions that have missed the window lately (Amendment 1 A3). An entry goes when
    /// its session is discharged again or detaches.
    pub(super) backoff: HashMap<ClientId, Backoff>,
    /// Writes in flight whose live send waits for them, per session, connection and
    /// topic (Amendment 1 A5): a hold may not overtake one on its own topic.
    pub(super) writing: HashMap<(ClientId, u64, String), usize>,
}

impl Hub {
    /// Switch write elision on with this window (ADR 0089). `0` keeps it off.
    pub fn set_write_elision_ms(&mut self, ms: u32) {
        self.elision.window = (ms > 0).then(|| Duration::from_millis(u64::from(ms)));
    }

    /// Whether a delivery to online persistent session `client` may be held instead of
    /// written first (ADR 0089 D2).
    pub(super) fn may_elide(
        &self,
        client: &ClientId,
        message: &Message,
        gate: &AppendGate,
    ) -> bool {
        if self.elision.window.is_none()
            || message.qos != QoS::AtLeastOnce
            || self.brownout
            || self.draining
            || self.connecting.contains_key(client)
            || self.takeover_open()
        {
            return false;
        }
        // A subscriber that keeps missing the window is written directly for a while,
        // so its publishers pay the write, not the window on top (Amendment 1 A3).
        if self
            .elision
            .backoff
            .get(client)
            .and_then(|b| b.until)
            .is_some_and(|until| Instant::now() < until)
        {
            return false;
        }
        // Only a delivery somebody is waiting on: a local publish, or a peer's first
        // forward. A re-delivery or re-route (#784) may find the session already holds
        // the publish; a Will or a back-fill has nobody to answer.
        if !matches!(
            gate,
            AppendGate::Pending(_) | AppendGate::Peer { replay: false, .. }
        ) {
            return false;
        }
        // A write in flight on this topic: its delivery goes on the wire after the
        // write, and a held one would overtake it. Other topics' writes do not block:
        // MQTT orders a topic, not a session (Amendment 1 A5).
        let conn = self.online.get(client).map(|o| o.conn_id);
        if conn.is_some_and(|c| {
            self.elision
                .writing
                .contains_key(&(client.clone(), c, message.topic.clone()))
        }) {
            return false;
        }
        if let Some(inf) = self.inflight.get(client) {
            // A packet id and a send slot free now: a delivery that would wait in the
            // backlog or for a packet-id block is written as before. (A session's first
            // delivery reserves its first block, durably, on that path; elision starts
            // from the next one.) A delivery already written and on the wire, still
            // unacknowledged, does not block (Amendment 1 A2): it is ahead of this one
            // on the wire and in the store, so a replay keeps the order. Only a write
            // still in flight does (the lane check above): its delivery is sent after
            // the write, and a held one would overtake it.
            if inf.quota_full()
                || !inf.backlog.is_empty()
                || inf.records_pending > 0
                || (inf.block_remaining == 0 && inf.banked_base.is_none())
            {
                return false;
            }
        } else {
            return false;
        }
        self.elision.entries < MAX_ELIDED
            && self.elision.bytes + message.payload.len() <= MAX_ELIDED_BYTES
    }

    /// Send `message` to `client` now and hold its obligation for the window. The
    /// caller has checked [`may_elide`](Self::may_elide).
    pub(super) fn elide(
        &mut self,
        client: &ClientId,
        tx: &Outbound,
        message: &Message,
        message_expiry: Option<u32>,
        retain: bool,
        gate: &AppendGate,
    ) -> DurableOutcome {
        let window = self.elision.window.expect("may_elide checked the window");
        self.register_obligation(&gate.then());
        let before = self.inflight.get(client).map(|i| i.next_pkid);
        let _ = self.send_qos_publish(client, tx, message, retain, message_expiry, None);
        let pkid = self.inflight.get(client).and_then(|inf| {
            let id = inf.next_pkid;
            (Some(id) != before && inf.pending.contains_key(&id)).then_some(id)
        });
        let Some(pkid) = pkid else {
            // Not in flight after all (the connection had just closed): write it as an
            // offline delivery, and let that write carry the obligation.
            if let Some(m) = &self.metrics {
                m.durable_elision_fallback("disconnect");
            }
            let out = self.fallback_write(client, message, message_expiry, gate, None);
            self.release_registration(&gate.then());
            return out;
        };
        if let Some(m) = &self.metrics {
            m.publish_delivered(qos_num(message.qos));
        }
        let sent = Instant::now();
        let deadline = sent + window_in_force(window, self.elision.entries, self.elision.bytes);
        self.elision.entries += 1;
        self.elision.bytes += message.payload.len();
        self.elision
            .by_client
            .entry(client.clone())
            .or_default()
            .push_back(Elided {
                pkid,
                message: message.clone(),
                message_expiry,
                gate: gate.clone(),
                deadline,
                sent,
            });
        self.elision.deadlines.push_back((deadline, client.clone()));
        DurableOutcome::Ok
    }

    /// The subscriber acknowledged `pkid`: if it was held, its obligation is discharged
    /// with no write (D1). Returns whether it was held.
    pub(super) fn elided_acked(&mut self, client: &ClientId, pkid: u16) -> bool {
        let Some(queue) = self.elision.by_client.get_mut(client) else {
            return false;
        };
        let Some(at) = queue.iter().position(|e| e.pkid == pkid) else {
            return false;
        };
        let e = queue.remove(at).expect("position found it");
        if queue.is_empty() {
            self.elision.by_client.remove(client);
        }
        self.elision.entries -= 1;
        self.elision.bytes -= e.message.payload.len();
        self.elision.discharged += 1;
        self.elision.backoff.remove(client);
        if let Some(m) = &self.metrics {
            m.durable_elided(e.sent.elapsed().as_secs_f64());
        }
        self.settle_discharged(&e.gate.then());
        true
    }

    /// Fall back every delivery `client` holds, in send order (D3, D5). `reason` is
    /// the bounded class the fallback is counted under.
    pub(super) fn flush_elided(&mut self, client: &ClientId, reason: &'static str) {
        let Some(queue) = self.elision.by_client.remove(client) else {
            return;
        };
        for e in queue {
            self.elision.entries -= 1;
            self.elision.bytes -= e.message.payload.len();
            self.elision.fallbacks += 1;
            if let Some(m) = &self.metrics {
                m.durable_elision_fallback(reason);
            }
            let then = e.gate.then();
            let _ =
                self.fallback_write(client, &e.message, e.message_expiry, &e.gate, Some(e.pkid));
            self.release_registration(&then);
        }
    }

    /// A delivery on `topic` takes the durable path: what `client` holds on that topic is
    /// written first, in send order, so its appends sit ahead in the lane and the store
    /// keeps the wire's order (Amendment 1 A5; fallback reason `head`). Held deliveries
    /// on other topics stay held.
    pub(super) fn flush_elided_topic(&mut self, client: &ClientId, topic: &str) {
        let Some(queue) = self.elision.by_client.get_mut(client) else {
            return;
        };
        if !queue.iter().any(|e| e.message.topic == topic) {
            return;
        }
        let (same, rest): (VecDeque<Elided>, VecDeque<Elided>) = std::mem::take(queue)
            .into_iter()
            .partition(|e| e.message.topic == topic);
        if rest.is_empty() {
            self.elision.by_client.remove(client);
        } else {
            *self
                .elision
                .by_client
                .get_mut(client)
                .expect("present above") = rest;
        }
        for e in same {
            self.elision.entries -= 1;
            self.elision.bytes -= e.message.payload.len();
            self.elision.fallbacks += 1;
            if let Some(m) = &self.metrics {
                m.durable_elision_fallback("head");
            }
            let then = e.gate.then();
            let _ =
                self.fallback_write(client, &e.message, e.message_expiry, &e.gate, Some(e.pkid));
            self.release_registration(&then);
        }
    }

    /// A counted write landed (Amendment 1 A5).
    pub(super) fn write_landed(&mut self, job: &AppendJob) {
        let key = (
            job.client.clone(),
            job.planned_conn.unwrap_or_default(),
            job.message.topic.clone(),
        );
        if let Some(n) = self.elision.writing.get_mut(&key) {
            *n -= 1;
            if *n == 0 {
                self.elision.writing.remove(&key);
            }
        }
    }

    /// Connection `conn_id` of `client` detached: its writes in flight no longer gate
    /// a hold. A later connection counts its own.
    pub(super) fn forget_writing(&mut self, client: &ClientId, conn_id: u64) {
        self.elision
            .writing
            .retain(|(c, conn, _), _| !(c == client && *conn == conn_id));
    }

    /// The window ended for the oldest held deliveries; also, under brownout or a
    /// drain, nothing may stay held.
    pub(super) fn expire_elided(&mut self) {
        if self.elision.entries == 0 {
            self.elision.deadlines.clear();
            return;
        }
        if self.brownout || self.draining {
            let reason = if self.brownout { "brownout" } else { "drain" };
            let clients: Vec<ClientId> = self.elision.by_client.keys().cloned().collect();
            for c in clients {
                self.flush_elided(&c, reason);
            }
            self.elision.deadlines.clear();
            return;
        }
        let now = Instant::now();
        while let Some((deadline, _)) = self.elision.deadlines.front() {
            if *deadline > now {
                break;
            }
            let (_, client) = self.elision.deadlines.pop_front().expect("front exists");
            let expired = self
                .elision
                .by_client
                .get(&client)
                .and_then(VecDeque::front)
                .is_some_and(|e| e.deadline <= now);
            if expired {
                self.flush_elided(&client, "timeout");
                self.note_timeout(&client);
            }
        }
    }

    /// `client`'s held deliveries missed the window: one strike (Amendment 1 A3). At
    /// [`BACKOFF_STRIKES`] the session is written directly for [`BACKOFF`].
    fn note_timeout(&mut self, client: &ClientId) {
        let b = self.elision.backoff.entry(client.clone()).or_default();
        b.strikes = b.strikes.saturating_add(1);
        if b.strikes >= BACKOFF_STRIKES {
            b.strikes = 0;
            b.until = Some(Instant::now() + BACKOFF);
            if let Some(m) = &self.metrics {
                m.durable_elision_backoff();
            }
        }
    }

    /// The session detached: what it held is written (`flush_elided`), and its record
    /// of missing the window goes with it.
    pub(super) fn forget_backoff(&mut self, client: &ClientId) {
        self.elision.backoff.remove(client);
    }

    /// The write a held delivery falls back to. Its obligation is registered by the
    /// append itself; a refusal (brownout since) or a full lane withholds, as a failed
    /// write does today.
    fn fallback_write(
        &mut self,
        client: &ClientId,
        message: &Message,
        message_expiry: Option<u32>,
        gate: &AppendGate,
        delivered: Option<u16>,
    ) -> DurableOutcome {
        let submitted = if self.brownout {
            Submitted::Refused(PublishRefusal::Brownout)
        } else {
            self.submit_append_job(client, message, message_expiry, gate, None, delivered)
        };
        match submitted {
            Submitted::Queued => DurableOutcome::Ok,
            Submitted::Refused(_) | Submitted::Full => {
                // Register-and-fail, so the release that follows nets out and the
                // obligation ends Failed: the publisher retries.
                self.register_obligation(&gate.then());
                self.settle_failed(&gate.then());
                DurableOutcome::Failed
            }
        }
    }

    /// A fallback write landed at `offset` for the delivery in flight under `pkid`.
    pub(super) fn link_elided_offset(
        &mut self,
        client: &ClientId,
        pkid: u16,
        message: &Message,
        offset: Offset,
    ) {
        let Some(inf) = self.inflight.get_mut(client) else {
            return;
        };
        let same = inf.pending.get(&pkid).is_some_and(|p| {
            p.offset.is_none()
                && p.message.topic == message.topic
                && p.message.payload == message.payload
        });
        inf.track(offset);
        if same {
            if let Some(p) = inf.pending.get_mut(&pkid) {
                p.offset = Some(offset);
            }
        } else {
            // Acknowledged already (or the session moved on): nothing is owed for it.
            inf.release(offset);
            self.truncate_acked(client);
        }
    }

    /// Count one more obligation on `then`, as a submitted append does.
    fn register_obligation(&mut self, then: &AppendThen) {
        match then {
            AppendThen::Gate(id) => {
                if let Some(p) = self.pending_publishes.get_mut(*id) {
                    p.appends_outstanding += 1;
                }
            }
            AppendThen::Peer(node, seq) => {
                let g = self
                    .remote_append_pending
                    .entry((node.clone(), *seq))
                    .or_insert(RemoteAppendGate {
                        awaiting: 0,
                        worst: DurableOutcome::Ok,
                        congested: false,
                        answered: false,
                        reached: false,
                        held: false,
                    });
                g.awaiting += 1;
            }
            AppendThen::Ungated => {}
        }
    }

    /// Take back an obligation another one has replaced (the fallback write's).
    fn release_registration(&mut self, then: &AppendThen) {
        match then {
            AppendThen::Gate(id) => {
                if let Some(p) = self.pending_publishes.get_mut(*id) {
                    p.appends_outstanding = p.appends_outstanding.saturating_sub(1);
                }
                self.try_complete_pending(*id);
            }
            AppendThen::Peer(node, seq) => self.settle_peer(node, *seq, DurableOutcome::Ok, false),
            AppendThen::Ungated => {}
        }
    }

    /// The subscriber's PUBACK settled this obligation: delivered and acknowledged.
    fn settle_discharged(&mut self, then: &AppendThen) {
        match then {
            AppendThen::Gate(id) => {
                if let Some(p) = self.pending_publishes.get_mut(*id) {
                    p.appends_outstanding = p.appends_outstanding.saturating_sub(1);
                    p.stored = true;
                }
                self.try_complete_pending(*id);
            }
            AppendThen::Peer(node, seq) => self.settle_peer(node, *seq, DurableOutcome::Ok, true),
            AppendThen::Ungated => {}
        }
    }

    fn settle_failed(&mut self, then: &AppendThen) {
        match then {
            AppendThen::Gate(id) => {
                if let Some(p) = self.pending_publishes.get_mut(*id) {
                    p.appends_outstanding = p.appends_outstanding.saturating_sub(1);
                }
                self.drop_pending(*id);
            }
            AppendThen::Peer(node, seq) => {
                self.settle_peer(node, *seq, DurableOutcome::Failed, false);
            }
            AppendThen::Ungated => {}
        }
    }

    /// One obligation on peer forward `(node, seq)` ended with `out`; answered once
    /// the last one has, as the lane's completion does.
    fn settle_peer(&mut self, node: &NodeId, seq: u64, out: DurableOutcome, reached: bool) {
        let key = (node.clone(), seq);
        let Some(g) = self.remote_append_pending.get_mut(&key) else {
            return;
        };
        g.awaiting = g.awaiting.saturating_sub(1);
        g.worst = g.worst.and(out);
        g.reached |= reached;
        if g.awaiting == 0 && !g.held {
            if let Some(g) = self.remote_append_pending.remove(&key) {
                if !g.answered {
                    self.answer_forward(node, seq, g.worst.to_verdict_reached(g.reached));
                }
            }
        }
    }
}
