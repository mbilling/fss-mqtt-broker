//! The pending-publish ledger and cross-node forwarding — obligations,
//! verdicts, retransmission, and settle (issue #258 slice 5: moved verbatim
//! from `hub/mod.rs`, no logic edits).
//!
//! **The invariant this module owns:** an `Accepted` is released only against
//! recorded evidence. A gated publish is a [`PendingPublish`] whose ack waits on
//! its local appends (via the lanes' gate) and on every registered
//! [`ForwardObligation`]; a peer's verdict resolves exactly one obligation
//! (first-terminal-wins composition in `forward_answered` — an unknown or
//! missing answer WITHHOLDS, never fabricates), the sweep retransmits the same
//! frame under the same seq until answered, re-route grace re-checks remote
//! interest before any terminal answer, and `refuse_pending` refuses only a
//! publish stored NOWHERE. The mesh/settle honesty gates that decide when a
//! zero-match fan-out may be believed stay in `hub/mod.rs` with the sweep that
//! arms them (issues #294/#305 document the covered windows and the stated
//! residuals).

#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258): the five
// siblings share one type/state vocabulary by design, and enumerating it would
// re-couple every future hub change to six import lists. Scoped to these files.
use super::*;

/// The empty property block every propertyless publish borrows instead of
/// storing its own. `const`-constructible, so it costs one static, not one
/// allocation per in-flight message.
static NO_APP_PROPERTIES: AppProperties = AppProperties {
    payload_format: None,
    content_type: None,
    response_topic: None,
    correlation_data: None,
    user_properties: Vec::new(),
};

/// What one pending publish costs against [`PENDING_PUBLISH_MAX_BYTES`]: the
/// fixed entry plus the two buffers it keeps alive for retransmission.
///
/// `payload` is a refcounted [`Bytes`], usually shared with the delivery path, so
/// this can charge for memory the entry does not exclusively own. That is the
/// intended direction of the error: the bound exists to cap what a partition can
/// pin, and a pending entry is precisely what keeps the payload from being freed.
fn pending_cost(topic: &str, payload: &Bytes) -> usize {
    std::mem::size_of::<PendingPublish>() + topic.len() + payload.len()
}

/// The pending-publish table: an id-ordered map that knows its own byte total.
///
/// It is a type rather than a `BTreeMap` field plus a counter because the bound is
/// only a bound if the two agree, and they are touched from five places — one
/// insert, one eviction, three removals across two files. Here every mutation goes
/// through a method that moves both together, and nothing outside can reach the
/// map to insert or remove behind the counter's back.
#[derive(Debug, Default)]
pub(super) struct PendingTable {
    entries: BTreeMap<u64, PendingPublish>,
    bytes: usize,
}

impl PendingTable {
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Bytes charged for everything currently held (see [`pending_cost`]).
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn get(&self, id: u64) -> Option<&PendingPublish> {
        self.entries.get(&id)
    }

    pub(super) fn get_mut(&mut self, id: u64) -> Option<&mut PendingPublish> {
        self.entries.get_mut(&id)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&u64, &PendingPublish)> {
        self.entries.iter()
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&u64, &mut PendingPublish)> {
        self.entries.iter_mut()
    }

    /// Whether admitting an entry of `cost` bytes would breach either bound.
    ///
    /// An EMPTY table always admits: one publish larger than the whole byte bound
    /// must still be answerable, or a single oversized message would be refused
    /// forever with nothing to evict to make room for it.
    pub(super) fn is_full_for(&self, cost: usize) -> bool {
        !self.entries.is_empty()
            && (self.entries.len() >= PENDING_PUBLISH_CAP
                || self.bytes.saturating_add(cost) > PENDING_PUBLISH_MAX_BYTES)
    }

    pub(super) fn insert(&mut self, id: u64, mut entry: PendingPublish) {
        entry.cost = u32::try_from(pending_cost(&entry.topic, &entry.payload)).unwrap_or(u32::MAX);
        self.bytes += entry.cost as usize;
        if let Some(replaced) = self.entries.insert(id, entry) {
            // Ids are monotonic, so this is unreachable — but if it ever happened
            // the replaced entry's charge must leave with it.
            self.bytes -= replaced.cost as usize;
        }
    }

    pub(super) fn remove(&mut self, id: u64) -> Option<PendingPublish> {
        let entry = self.entries.remove(&id)?;
        self.bytes -= entry.cost as usize;
        Some(entry)
    }

    /// Evict the OLDEST entry (the lowest id): the overflow policy of both bounds.
    pub(super) fn pop_first(&mut self) -> Option<(u64, PendingPublish)> {
        let (id, entry) = self.entries.pop_first()?;
        self.bytes -= entry.cost as usize;
        Some((id, entry))
    }
}

#[allow(clippy::struct_excessive_bools)]
/// A `QoS` 1 publish whose acknowledgement is gated on **cluster-wide** durability
/// (ADR 0042 T9): the local fan-out's durable appends (synchronous), the retained
/// authority commit (exhibit ⑦), and one durability-gated ack per acked peer
/// forward (exhibit ⑤). The ack releases only when every obligation resolves;
/// a terminal failure drops the entry, withholding the ack (the publisher retries).
#[derive(Debug)]
pub(super) struct PendingPublish {
    /// What [`PendingTable::insert`] charged this entry against the byte bound.
    /// Written once, by the table, and handed back on removal — so the table's
    /// byte total is a sum of what it actually charged and cannot drift from the
    /// entries it holds, whatever later happens to `topic` or `payload`.
    ///
    /// `u32`, not `usize`: it fits the padding the entry already had, where a
    /// `usize` grew every entry by 8 bytes. An MQTT packet is at most 268,435,455
    /// bytes, so topic + payload + the struct cannot reach `u32::MAX`; the
    /// conversion saturates rather than trusting that.
    cost: u32,
    /// Releases the publisher's acknowledgement (dropped = withheld).
    pub(super) done: oneshot::Sender<PublishOutcome>,
    /// The forwarded frame, kept for retransmission and takeover re-routing.
    pub(super) topic: String,
    pub(super) payload: Bytes,
    pub(super) qos: QoS,
    pub(super) retain: bool,
    pub(super) message_expiry: Option<u32>,
    /// The publisher's MQTT 5 application properties — **boxed, and only when a
    /// publish actually carries any**. Inline this block is 112 bytes on an entry
    /// that is otherwise 208, and a telemetry PUBLISH sets none of it (the wire
    /// form's own doc calls empty "the common case"). `None` here is the same
    /// thing as an empty `AppProperties`; read it through [`PendingPublish::app`],
    /// which hands back a shared empty block so callers cannot tell the
    /// difference.
    pub(super) app: Option<Box<AppProperties>>,
    /// Outstanding forward answers: forward seq → what was forwarded, and where.
    pub(super) awaiting: HashMap<u64, ForwardObligation>,
    /// Whether this publish is known to be durably STORED somewhere — locally, or on
    /// a peer that answered [`ForwardVerdict::Stored`].
    ///
    /// [`refuse_pending`](Hub::refuse_pending) consults it: `Refused` makes the
    /// positive claim "nothing was stored, retry", so a publish that IS held for some
    /// of its subscribers may only be WITHHELD (which claims nothing). Without this a
    /// brownout entered during a takeover window would answer `0x97` for a message
    /// already durably owed to a subscriber, and the application's retry would
    /// duplicate it there (issue #238).
    pub(super) stored: bool,
    /// Peers whose durability ack already arrived — a takeover re-route never
    /// re-obligates them.
    pub(super) acked_nodes: HashSet<NodeId>,
    /// Whether the retained authority commit is still outstanding (exhibit ⑦).
    pub(super) awaiting_retained: bool,
    /// Set once the on-loop local fan-out pass completed OK — every owed durable
    /// append SUBMITTED to its lane (issue #242) and counted below.
    pub(super) local_done: bool,
    /// Lane appends submitted for this publish and not yet completed (issue #242 /
    /// ADR 0061). Incremented synchronously at submission, inside the same dispatch
    /// that created the gate; decremented only by the `AppendDone` handler after a
    /// real store outcome. The ack releases only at zero.
    pub(super) appends_outstanding: usize,
    /// When the publish first fanned out — the cutoff for re-delivery (only
    /// clients attached or materialized AFTER this can have missed it).
    pub(super) created_at: Instant,
    /// Engaged when a forward target died: counts down sweep ticks with no
    /// re-routable remote interest before the obligation is considered moot
    /// (see [`REROUTE_GRACE_TICKS`]).
    pub(super) reroute_grace: Option<u8>,
    /// Set when the publish arrived during a takeover window (an inherited-session
    /// scan pending or running): the ack waits until the scan lands, then the
    /// publish re-delivers locally against the just-materialized subscriptions
    /// (exhibit ⑥'s ack-into-the-void window; duplicates are legal at `QoS` 1).
    pub(super) awaiting_settle: bool,
    /// ADR 0072: the publisher selected the RELAXED tier (and the operator opted
    /// in), so the ack releases at `local_done` — every obligation SUBMITTED,
    /// nothing awaited. The appends, forwards and retained commit all still run;
    /// only the ack's meaning is weakened, at the publisher's explicit request.
    /// A refusal decided at the plan pass (brownout) still refuses.
    pub(super) relaxed: bool,
    /// The relaxed congestion valve (issue #399): set at submit time when any
    /// of this publish's append lanes was already past the soft depth
    /// threshold. A congested relaxed publish completes by the QUORUM rule
    /// (ack after its appends land), so the publisher's window throttles to
    /// the drain rate BEFORE the lanes overflow — without it, instant acks
    /// refill the window forever, the bounded lanes overflow, and every
    /// overflow fails the publish and closes the connection (the measured
    /// reconnect storm). Relaxed grants latency below congestion, not
    /// immunity from capacity.
    pub(super) congested: bool,
}

impl PendingPublish {
    /// The publish's application properties, empty block and all — so a caller
    /// never has to know whether this entry paid to store any.
    pub(super) fn app(&self) -> &AppProperties {
        self.app.as_deref().unwrap_or(&NO_APP_PROPERTIES)
    }
}

/// One outstanding cross-node obligation of a gated publish (ADR 0042 T9 exhibit ⑤;
/// 0041-T12 for the shared kind): where it went, and which frame answers it.
#[derive(Debug, Clone)]
pub(super) struct ForwardObligation {
    /// The node the forward went to.
    pub(super) node: NodeId,
    /// Which frame this obligation is, so a retransmit re-sends the SAME kind.
    pub(super) kind: ForwardKind,
}

/// The two things a gated publish can owe a peer an answer for.
#[derive(Debug, Clone)]
pub(super) enum ForwardKind {
    /// An interest-driven fan-out forward ([`PeerMessage::PublishAcked`]): the peer
    /// delivers to whichever of its own subscribers match.
    Ordinary,
    /// A shared-group delivery targeted at one named member
    /// ([`PeerMessage::SharedDeliverAcked`], proto 7). A refusal here does not refuse
    /// the publisher: a shared group exists so that one member's browned-out node
    /// becomes a RE-SELECTION, not a cluster-wide publish refusal.
    Shared {
        /// The group this delivery belongs to (for the re-selection).
        key: SharedKey,
        /// The chosen member.
        client: ClientId,
        /// The already-downgraded delivery `QoS`.
        qos: QoS,
        /// Every candidate already tried for this publish, so a re-selection pass is
        /// bounded: each candidate at most once, then the publisher is answered.
        tried: Vec<(Option<NodeId>, ClientId)>,
    },
}

impl Hub {
    /// A takeover-window re-delivery of pending publish `id` (ADR 0042 T9):
    /// deliver the frame ONLY to routing state that could have missed the
    /// original fan-out — offline persistent sessions (materialized since) and
    /// clients attached after the publish. Clients online since BEFORE the
    /// publish already received it live; re-sending would duplicate (dups are
    /// legal at `QoS` 1, but a boot-window re-send to a steady subscriber is a
    /// gratuitous one — observed as duplicate bridge forwards). Returns a non-`Ok`
    /// [`DurableOutcome`] on a terminal durable-append failure (the caller withholds)
    /// or a stated-policy refusal.
    ///
    /// A brownout entered DURING the takeover window can therefore refuse a publish
    /// registered before it began — the publisher waited out the window and is then
    /// told `0x97`. That is the same class as today's `Failed` → withhold, but with a
    /// reason it can act on (0041-T11, issue #238).
    pub(super) fn redeliver_pending(&mut self, id: u64) -> DurableOutcome {
        let Some(p) = self.pending_publishes.get(id) else {
            return DurableOutcome::Ok;
        };
        let (topic, payload, qos, expiry, app, since) = (
            p.topic.clone(),
            p.payload.clone(),
            p.qos,
            p.message_expiry,
            p.app().clone(),
            p.created_at,
        );
        let targets: Vec<(ClientId, QoS)> = self
            .table
            .matching_clients(&topic)
            .into_iter()
            .filter(|c| self.online.get(c).is_none_or(|o| o.attached_at > since))
            .map(|c| {
                let granted = self.granted_qos(&c, &topic);
                (c, granted)
            })
            .collect();
        let mut all_durable = DurableOutcome::Ok;
        for (c, granted) in targets {
            all_durable = all_durable.and(self.deliver_to_client(
                &c,
                &topic,
                &payload,
                min_qos(qos, granted),
                expiry,
                &app,
                false,
                // A gated publisher IS waiting on this id, so a refusal here is
                // answerable — but only as a WITHHOLD if the original fan-out
                // already stored the message (or still might, via an in-flight
                // lane append), which `refuse_pending` enforces.
                &AppendGate::Pending(id),
            ));
        }
        all_durable
    }

    /// The takeover window closed for this node (an inherited-session scan just
    /// landed): every held pending publish re-delivers **locally** against the
    /// just-materialized subscriptions (duplicates are legal at `QoS` 1 — the
    /// alternative was an ack into the void, exhibit ⑥), then re-checks remote
    /// interest via the sweep's re-route path before its ack can release.
    pub(super) fn settle_pending_publishes(&mut self) {
        let held: Vec<u64> = self
            .pending_publishes
            .iter()
            .filter(|(_, p)| p.awaiting_settle || p.reroute_grace.is_some())
            .map(|(id, _)| *id)
            .collect();
        // The hold clears only when the whole takeover WINDOW is over: one scan
        // is not enough — the group leases reassign for seconds after the death,
        // and a scan that ran before a lease landed saw nothing. Every scan in
        // the window re-delivers (duplicates are legal); the last one releases.
        // Never on a broken mesh (an unreachable-but-alive peer may hold
        // interest this node cannot see — T4 seed 4), and never on TIME alone
        // while the last scan still SKIPPED sessions (0043-P4 exhibit ②: a
        // restarted owner whose lease reassignment outlives the tick window
        // must keep holding, not ack into the void) — `routing_unsettled` is
        // the one observable-state predicate for all of it.
        let window_over = !self.routing_unsettled();
        for id in held {
            let out = self.redeliver_pending(id);
            match out {
                DurableOutcome::Ok => {}
                // The re-delivery's durable append failed terminally: withhold.
                DurableOutcome::Failed => {
                    self.drop_pending(id);
                    continue;
                }
                // A stated policy refused it (brownout entered during the window):
                // tell the publisher rather than closing on it.
                DurableOutcome::Refused(r) => {
                    self.refuse_pending(id, r);
                    continue;
                }
            }
            // The successor may have materialized the subscriber on ANOTHER node
            // and advertised its interest since this publish's original fan-out
            // (which found nothing): forward to it now — a publish that arrived
            // after the death dropped the dead node's interest has no obligation
            // to re-route, so this is where it re-targets.
            for node in self.reroute_candidates(id) {
                self.send_acked_forward(id, &node);
            }
            if window_over {
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    p.awaiting_settle = false;
                }
            }
            self.try_complete_pending(id);
        }
    }

    /// Forward a locally-originated publish to peers. A non-retained message goes
    /// only to peers whose announced interest matches (live delivery). A **retained**
    /// message goes to *every* peer regardless of current interest, so each node
    /// stores it for its future subscribers (ADR 0014). Receivers apply it locally
    /// only, so there is no relay/loop.
    ///
    /// Under durable retained (ADR 0037 §3) the retain flag no longer forces the
    /// broadcast: caches are warmed by the owner's post-commit fan-out instead, so a
    /// retained publish forwards like any other — to interested peers, for live
    /// delivery only.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn forward_to_peers(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
        gate: Option<u64>,
    ) {
        let retain_broadcasts = retain && self.durable_retained.is_none();
        // A gated QoS ≥ 1 forward is ACKED (ADR 0042 T9, exhibit ⑤): the
        // publisher's ack waits for each target's durability-gated answer, and
        // the sweep retransmits while unanswered. Targets come from the INTEREST
        // map, not the connected-peer map: a link-down (but not dead) peer's
        // subscribers are still owed the publish — the obligation is recorded
        // now, the frame flows when the link returns (sweep), or re-routes to
        // the successor when membership confirms death (`peer_dead`).
        let gated = gate.is_some() && qos_num(qos) >= 1;
        // ONE trie walk answers both questions below — who must be forwarded to,
        // and whether a given connected peer is interested. This used to be a
        // scan of every peer's whole filter set, per question, per message.
        let interested_nodes = self.interest.nodes_matching(topic);
        if gated {
            let id = gate.unwrap_or_default();
            for node in &interested_nodes {
                self.send_acked_forward(id, node);
            }
            if !retain_broadcasts {
                return;
            }
        }
        for (node, peer) in &self.peers {
            let interested = interested_nodes.contains(node);
            if gated && interested {
                continue; // already handled (acked or legacy) above
            }
            if !(retain_broadcasts || interested) {
                continue;
            }
            if let Some(m) = &self.metrics {
                m.publish_forwarded("subscriber-remote");
            }
            let _ = peer.tx.send(PeerMessage::Publish {
                topic: topic.to_string(),
                payload: payload.to_vec(),
                qos: qos as u8,
                retain,
                message_expiry,
                app: app_to_wire(app),
            });
        }
    }

    /// Peers that now advertise matching interest for pending publish `id` but
    /// have neither acked a forward nor have one outstanding — the re-route
    /// targets after a takeover (the dead owner's successor materializes the
    /// inherited sessions and re-advertises their filters).
    pub(super) fn reroute_candidates(&self, id: u64) -> Vec<NodeId> {
        let Some(p) = self.pending_publishes.get(id) else {
            return Vec::new();
        };
        // Off the message path (takeover re-route), but the same one-walk shape:
        // resolve the interested set once, then test membership per peer.
        let interested_nodes = self.interest.nodes_matching(&p.topic);
        self.peers
            .iter()
            .filter(|(n, _)| {
                !p.acked_nodes.contains(*n)
                    && !p.awaiting.values().any(|o| &o.node == *n)
                    && interested_nodes.contains(*n)
            })
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Send (or re-send, on re-route) one acked forward of pending publish `id` to
    /// `node`, recording the obligation (ADR 0042 T9, exhibit ⑤).
    pub(super) fn send_acked_forward(&mut self, id: u64, node: &NodeId) {
        self.register_forward(
            id,
            ForwardObligation {
                node: node.clone(),
                kind: ForwardKind::Ordinary,
            },
        );
    }

    /// Record `obligation` against pending publish `id` and send its frame (ADR 0042
    /// T9 exhibit ⑤; 0041-T12). One registration path for both forward kinds, so the
    /// seq space, the index, the cap and the sweep treat them identically.
    ///
    /// The frame is sent only when the link is up; a link-down (not dead) peer's
    /// obligation is still RECORDED and the sweep sends it when the link returns —
    /// which is also how the pre-0041-T12 `send_shared_to_peer` bug (a shared frame
    /// silently dropped on a downed link, with the publisher acked anyway) is closed.
    pub(super) fn register_forward(&mut self, id: u64, obligation: ForwardObligation) {
        self.forward_seq += 1;
        let seq = self.forward_seq;
        let node = obligation.node.clone();
        let Some(p) = self.pending_publishes.get_mut(id) else {
            return;
        };
        let frame = forward_frame(p, seq, &obligation);
        // Issue #480. Counted where the obligation is RECORDED rather than where
        // the frame is written, so a forward to a link that is momentarily down
        // still counts: the sweep will send it when the link returns, and it
        // crossed a node boundary either way. Counting at the write would
        // undercount exactly during the link flaps worth investigating.
        if let Some(m) = &self.metrics {
            m.publish_forwarded(match obligation.kind {
                ForwardKind::Shared { .. } => "shared-remote",
                ForwardKind::Ordinary => "subscriber-remote",
            });
        }
        p.awaiting.insert(seq, obligation);
        self.forward_index.insert(seq, id);
        debug!(publish = id, seq, target = %node.0, "forward obligation recorded");
        if let Some(peer) = self.peers.get(&node) {
            let _ = peer.tx.send(frame);
        }
    }

    /// Register a `QoS` 1 publish whose acknowledgement is gated on cluster-wide
    /// durability (ADR 0042 T9). At the cap the oldest entry is dropped loudly —
    /// its ack withheld, so its publisher retries.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn register_pending(
        &mut self,
        done: oneshot::Sender<PublishOutcome>,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) -> u64 {
        // Evict until the newcomer fits BOTH bounds. A loop, not an `if`: under the
        // byte bound one large publish can need several small ones gone, and
        // stopping after one eviction would admit it over the bound anyway.
        let cost = pending_cost(topic, payload);
        while self.pending_publishes.is_full_for(cost) {
            let Some((old_id, old)) = self.pending_publishes.pop_first() else {
                break;
            };
            warn!(
                topic = %old.topic,
                cap = PENDING_PUBLISH_CAP,
                max_bytes = PENDING_PUBLISH_MAX_BYTES,
                "pending-publish bound: dropped the OLDEST unacknowledged publish \
                 (ack withheld; its publisher retries — ADR 0042 T9)"
            );
            self.forward_index.retain(|_, pid| *pid != old_id);
            if let Some(m) = &self.metrics {
                m.publish_dropped("pending-cap");
            }
        }
        self.publish_ids += 1;
        let id = self.publish_ids;
        self.pending_publishes.insert(
            id,
            PendingPublish {
                cost: 0, // charged by `PendingTable::insert`
                done,
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain,
                message_expiry,
                app: (!app.is_empty()).then(|| Box::new(app.clone())),
                awaiting: HashMap::new(),
                stored: false,
                acked_nodes: HashSet::new(),
                awaiting_retained: false,
                local_done: false,
                appends_outstanding: 0,
                created_at: Instant::now(),
                reroute_grace: None,
                // During a takeover window the routing table may not yet hold the
                // sessions this node (or a successor) inherited — hold the ack
                // until the scan lands and the publish re-delivers (exhibit ⑥).
                // Only meaningful on a multi-node cluster: a standalone node has
                // no takeovers, and holding its boot-time acks would just delay
                // every early publish for nothing.
                awaiting_settle: self.routing_unsettled(),
                relaxed: false,
                congested: false,
            },
        );
        id
    }

    /// Mark a pending publish RELAXED (ADR 0072): its ack releases at
    /// `local_done` instead of waiting for the durability obligations.
    pub(super) fn pending_mark_relaxed(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(id) {
            p.relaxed = true;
        }
    }

    /// The local fan-out obligation resolved OK (durable appends included).
    pub(super) fn pending_local_done(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(id) {
            p.local_done = true;
        }
        self.try_complete_pending(id);
    }

    /// The peer-bus proto this link negotiated, or
    /// [`peer::PROTO_MIN`](mqtt_cluster::peer::PROTO_MIN) when there is no link — fail
    /// safe toward the OLD frame, which every peer can decode (0041-T12).
    pub(super) fn peer_proto(&self, node: &NodeId) -> u32 {
        self.peers
            .get(node)
            .map_or(mqtt_cluster::peer::PROTO_MIN, |p| p.proto)
    }

    /// Drop a pending publish, WITHHOLDING its acknowledgement (the sender side
    /// of fail-closed: the publisher's connection sees no ack and retries).
    pub(super) fn drop_pending(&mut self, id: u64) {
        if self.pending_publishes.remove(id).is_some() {
            self.forward_index.retain(|_, pid| *pid != id);
        }
    }

    /// REFUSE a pending publish (0041-T11, issue #238): unlike
    /// [`drop_pending`](Self::drop_pending), the publisher is told *why* — the
    /// connection turns `r` into `0x97` for v5, or a close for v3.1.1, which has no
    /// reason byte to carry it.
    ///
    /// A refusal is only sayable for a publish stored NOWHERE. `Refused` carries the
    /// positive claim "nothing was stored, so a retry is the right move"; for a publish
    /// already durably owed to some subscriber that claim is false, and the application's
    /// retry would duplicate it there. Such a publish is WITHHELD instead — which claims
    /// nothing at all and is strictly weaker (issue #238). A false refusal is as much a
    /// defect as a false ack.
    ///
    /// What remains is the asymmetry a withhold always had: for the local fan-out the
    /// PLAN pass makes a refusal effect-free, but a peer forward already sent may store
    /// the message on a peer that is not browned out and answer after this node has
    /// refused, so the publisher's retry can duplicate on that peer's subscriber (and,
    /// separately, a per-client store FAILURE mid-fan-out can leave earlier subscribers
    /// with a copy). Duplicates are legal at `QoS` 1; an ack for a message this node did
    /// not store would not be.
    ///
    /// One more named residual: `stored` tracks session-log appends and peer `Stored`
    /// verdicts, NOT the retained authority — a retained publish whose retained commit
    /// already landed cluster-wide can still be answered `Refused` when a later peer
    /// verdict refuses. The retained value was durably replaced while the publisher
    /// hears "nothing was stored". Tolerated because a retained retry is idempotent
    /// (it re-writes the same value) and the reason code still drives the right client
    /// action; folding retained commits into `stored` would withhold instead, trading
    /// an honest reason for a silent close on an idempotent surface.
    pub(super) fn refuse_pending(&mut self, id: u64, r: PublishRefusal) {
        let Some(p) = self.pending_publishes.remove(id) else {
            return;
        };
        self.forward_index.retain(|_, pid| *pid != id);
        // An append still IN FLIGHT in a lane (issue #242) may yet store a copy, so
        // "nothing was stored" cannot be claimed either — withhold, which claims
        // nothing and is always safe. The named trade: a v5 publisher racing a peer
        // refusal against its own in-flight local append loses the actionable 0x97
        // and sees a close instead; a false refusal would be a defect, a withhold
        // is not (ADR 0061).
        if p.stored || p.appends_outstanding > 0 {
            warn!(
                publish = id, topic = %p.topic, refusal = r.as_str(),
                "a later fan-out pass was refused for a publish already stored durably \
                 (or with an append still in flight); ack WITHHELD rather than claiming \
                 nothing was stored (issue #238)"
            );
            return; // dropping `p.done` withholds
        }
        warn!(
            publish = id, topic = %p.topic, refusal = r.as_str(),
            "publish refused; the publisher is told rather than acked (ADR 0041 T11)"
        );
        let _ = p.done.send(PublishOutcome::Refused(r));
    }

    /// Answer one forward we RECEIVED, choosing the frame by the LINK's negotiated
    /// proto (0041-T12, issue #238).
    ///
    /// Gated on the negotiated version, not on "do I support 7": sending
    /// `PublishVerdict` to a proto-6 origin would be an unknown variant index to its
    /// strict codec and would kill the link. At proto 6 the verdict collapses to
    /// `PublishAck { ok: verdict == Stored }` — which is today's behaviour exactly, so a
    /// refusal reaches that origin as a withheld ack (the rolling-upgrade skew residual
    /// the docs must name).
    pub(super) fn answer_forward(&self, node: &NodeId, seq: u64, verdict: ForwardVerdict) {
        let Some(peer) = self.peers.get(node) else {
            return; // link gone: the sender's sweep will retransmit
        };
        let frame = if peer.proto >= PROTO_FORWARD_VERDICT {
            PeerMessage::PublishVerdict { seq, verdict }
        } else {
            PeerMessage::PublishAck {
                seq,
                ok: verdict == ForwardVerdict::Stored,
            }
        };
        let _ = peer.tx.send(frame);
    }

    /// A peer's answer to one outstanding forward (ADR 0042 T9 exhibit ⑤; 0041-T12).
    ///
    /// The correlation is once-only (`forward_index.remove`), so a proto-6
    /// `PublishAck` and a proto-7 `PublishVerdict` for the same `seq` cannot both
    /// count. Composition is FIRST-TERMINAL-VERDICT-WINS rather than
    /// [`DurableOutcome::and`]'s precedence: the publisher gets exactly one answer,
    /// and both terminal answers (`Refused`, `Failed`) leave it unacked, so ordering
    /// cannot turn a refusal into an ack. The asymmetry it inherits — peer X may have
    /// stored a copy while peer Y refused, so the publisher hears `0x97` and its retry
    /// duplicates on X — is the one [`refuse_pending`](Self::refuse_pending) already
    /// documents; duplicates are legal at `QoS` 1, a false ack is not.
    pub(super) fn forward_answered(&mut self, node: &NodeId, seq: u64, verdict: ForwardVerdict) {
        let Some(id) = self.forward_index.remove(&seq) else {
            return; // stale answer (entry dropped or already resolved)
        };
        let Some(p) = self.pending_publishes.get_mut(id) else {
            return;
        };
        if p.awaiting.get(&seq).map(|o| &o.node) != Some(node) {
            return; // not the node this seq was sent to — ignore
        }
        let obligation = p.awaiting.remove(&seq).expect("checked just above");
        match DurableOutcome::from_verdict(verdict) {
            DurableOutcome::Ok => {
                debug!(publish = id, seq, from = %node.0, "forward stored");
                p.stored = true;
                p.acked_nodes.insert(node.clone());
                self.try_complete_pending(id);
            }
            DurableOutcome::Failed => {
                // Includes an unknown refusal code — a NEWER peer refusing for a
                // reason this build cannot name. Withhold: `Failed` claims nothing
                // about what the peer stored, which is the only honest reading of an
                // answer we cannot interpret. Never `Accepted` (the one irreversible
                // answer) and never a fabricated `Refused`.
                if let ForwardVerdict::Refused { code } = verdict {
                    warn!(
                        peer = %node.0, code,
                        "peer refused a forwarded publish with a refusal code this build \
                         does not know; ack WITHHELD rather than claiming nothing was stored"
                    );
                } else {
                    warn!(
                        peer = %node.0,
                        "peer reported a terminal durable failure for a forwarded publish; \
                         ack withheld (the publisher retries — ADR 0042 T9)"
                    );
                }
                self.drop_pending(id);
            }
            DurableOutcome::Refused(r) => match obligation.kind {
                // A shared group's whole point: one member's browned-out node is a
                // RE-BALANCE, not a cluster-wide publish refusal.
                ForwardKind::Shared { .. } => {
                    self.reselect_shared(id, obligation, DurableOutcome::Refused(r));
                }
                ForwardKind::Ordinary => self.refuse_pending(id, r),
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    /// The sweep-tick half of acked forwards (ADR 0042 T9, exhibit ⑤): retransmit
    /// unanswered forwards whose target link is up (same seq — duplicates are
    /// legal at `QoS` 1), and drive takeover re-routes: a forward whose target
    /// DIED re-forwards to whichever peers now advertise matching interest (the
    /// dead owner's successor, once it materializes inherited sessions —
    /// exhibit ⑥); with no such interest for [`REROUTE_GRACE_TICKS`] ticks the
    /// obligation is moot (the interest genuinely ended) and the ack releases.
    // Retransmit, downgrade, re-route, grace: one linear sweep pass per pending —
    // splitting it would scatter the obligation lifecycle.
    pub(super) fn sweep_pending_forwards(&mut self) {
        // One pass picks out the entries with work to do, so a deep table of
        // young, healthy publishes costs a walk and nothing else. Work is either
        // an OVERDUE forward — outstanding for at least one sweep interval; a
        // forward sent milliseconds before the tick is not late, and re-sending it
        // made the sweep's cost and its duplicate traffic proportional to the
        // table's DEPTH, offered to the peers exactly when they were already
        // behind (issue #633) — or a re-route grace that must count this tick.
        let now = Instant::now();
        let ids: Vec<(u64, bool)> = self
            .pending_publishes
            .iter()
            .filter_map(|(id, p)| {
                let overdue = !p.awaiting.is_empty()
                    && now.duration_since(p.created_at) >= super::SESSION_SWEEP_INTERVAL;
                (overdue || p.reroute_grace.is_some()).then_some((*id, overdue))
            })
            .collect();
        for (id, overdue) in ids {
            // Retransmit overdue forwards over live links.
            let outstanding: Vec<(u64, ForwardObligation)> = self
                .pending_publishes
                .get(id)
                .filter(|_| overdue)
                .map(|p| p.awaiting.iter().map(|(s, o)| (*s, o.clone())).collect())
                .unwrap_or_default();
            for (seq, obligation) in &outstanding {
                let Some(peer) = self.peers.get(&obligation.node) else {
                    continue; // link down (not dead): wait for it to return
                };
                let Some(p) = self.pending_publishes.get(id) else {
                    continue;
                };
                // The SAME frame the original forward sent (only the seq is the
                // outstanding one, so the receiver dedups): built by the shared
                // constructor so a retransmitted copy can never drift semantically
                // from the first send — and, since 0041-T12, so a SHARED obligation
                // retransmits `SharedDeliverAcked` rather than a fan-out
                // `PublishAcked` that would deliver to the wrong subscribers.
                let _ = peer.tx.send(forward_frame(p, *seq, obligation));
            }
            // Re-route after a target death (grace engaged by peer_dead).
            let Some(p) = self.pending_publishes.get(id) else {
                continue;
            };
            let Some(grace) = p.reroute_grace else {
                continue;
            };
            let candidates = self.reroute_candidates(id);
            if !candidates.is_empty() {
                debug!(
                    publish = id,
                    targets = candidates.len(),
                    "re-routing acked forward"
                );
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    p.reroute_grace = None;
                }
                for node in candidates {
                    self.send_acked_forward(id, &node);
                }
                continue;
            }
            let awaiting_empty = p.awaiting.is_empty();
            if awaiting_empty && grace <= 1 && !self.mesh_whole() {
                // An alive peer is unreachable: its interest is invisible, so
                // "no candidates" proves nothing. Hold at the last grace tick
                // until the mesh heals (seed 4) — the publisher waits, exactly
                // like a durable attach under partition.
                continue;
            }
            if awaiting_empty && grace <= 1 {
                // The grace ends with a FINAL local re-delivery: the subscriber
                // may have materialized HERE in the meantime — via this node's
                // takeover scan or its own re-attach — after this publish's
                // original local fan-out ran against a not-yet-materialized
                // table (exhibit ⑥'s race, both faces). Targeted: only routing
                // state that could have missed the original fan-out.
                debug!(
                    publish = id,
                    "re-route grace expired; final local re-delivery"
                );
                let out = self.redeliver_pending(id);
                match out {
                    DurableOutcome::Ok => {}
                    DurableOutcome::Failed => {
                        self.drop_pending(id);
                        continue;
                    }
                    DurableOutcome::Refused(r) => {
                        self.refuse_pending(id, r);
                        continue;
                    }
                }
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    p.reroute_grace = None;
                }
            } else if awaiting_empty {
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    p.reroute_grace = Some(grace - 1);
                }
            }
            self.try_complete_pending(id);
        }
    }
}

/// Inverted interest index: which peer nodes want a given topic.
///
/// Interest is announced node-keyed (a node sends the full list of filters its
/// sessions hold), and it used to be STORED that way too — `NodeId -> filters`.
/// Answering "who wants this topic?" then meant scanning every peer's entire
/// filter set on every forwarded message: O(all cluster-wide subscriptions) per
/// publish. That is the factor that turns forwarding into O(nodes²) work
/// cluster-wide, and it is why adding nodes bent the latency curve instead of
/// buying throughput.
///
/// This inverts it. [`SubscriptionTable`] is a filter->subscriber index with a
/// topic trie beneath it: ONE walk of the topic's levels yields the subscribers,
/// O(topic depth + matches), no matter how many filters the cluster holds. Its
/// subscriber is an opaque id, so here it is keyed by NODE id — that pun is the
/// whole trick, and containing it in this wrapper is why there is no second copy
/// of a trie whose wildcard rules and iterative teardown are already hardened and
/// equivalence-tested in `mqtt-core`.
#[derive(Debug, Default)]
pub(super) struct InterestIndex {
    /// Filter -> interested nodes, the node id wearing `ClientId`'s clothing.
    /// Also the only place the filter strings are stored.
    by_filter: SubscriptionTable,
    /// Nodes that have advertised interest at all — including a node that
    /// advertised an EMPTY filter list, which still counts as "has interest"
    /// exactly as the old `HashMap` entry did. The table cannot answer this
    /// (it only knows filters that have subscribers), so removal consults this.
    nodes: HashSet<NodeId>,
}

impl InterestIndex {
    /// A node id as the table's opaque subscriber key.
    fn key(node: &NodeId) -> ClientId {
        ClientId(std::sync::Arc::from(node.0.as_str()))
    }

    /// Replace `node`'s advertised interest wholesale — peers send full
    /// snapshots, so this is a remove-then-insert, not a merge.
    pub(super) fn replace(&mut self, node: NodeId, filters: Vec<String>) {
        let key = Self::key(&node);
        self.by_filter.remove_client(&key);
        for f in filters {
            let interned = self.by_filter.intern(&f);
            self.by_filter.subscribe(key.clone(), interned);
        }
        self.nodes.insert(node);
    }

    /// Drop `node`'s interest entirely; `true` if it had an entry, matching the
    /// old `HashMap::remove(..).is_some()`.
    pub(super) fn remove(&mut self, node: &NodeId) -> bool {
        self.by_filter.remove_client(&Self::key(node));
        self.nodes.remove(node)
    }

    /// Every node whose advertised interest matches `topic`, in one trie walk.
    pub(super) fn nodes_matching(&self, topic: &str) -> HashSet<NodeId> {
        self.by_filter
            .matching_clients(topic)
            .into_iter()
            .map(|c| NodeId(c.as_str().to_string()))
            .collect()
    }

    /// Whether `node` has advertised any interest (test/diagnostic helper).
    #[cfg(test)]
    pub(super) fn has_node(&self, node: &NodeId) -> bool {
        self.nodes.contains(node)
    }
}

#[cfg(test)]
mod interest_index_tests {
    use super::*;

    /// The linear scan the index replaced: every node whose advertised filters
    /// contain one matching `topic`. The reference the corpus is checked against.
    fn reference(topic: &str, per_node: &HashMap<NodeId, Vec<String>>) -> HashSet<NodeId> {
        per_node
            .iter()
            .filter(|(_, fs)| fs.iter().any(|f| topic_matches(f, topic)))
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// The index must answer EXACTLY what the old per-peer linear scan answered.
    /// Generated corpus: wildcards at every position, `$`-rooted topics, empty
    /// levels, multi-level topics — checked against the linear `topic_matches`
    /// reference the scan used, through replace/remove churn.
    #[test]
    fn interest_index_matches_the_linear_scan_it_replaced() {
        let alphabet = ["a", "b", "+", "#", ""];
        let mut filters: Vec<String> = Vec::new();
        let mut topics: Vec<String> = vec![
            "$SYS/broker".into(),
            "$SYS".into(),
            "a".into(),
            "a/b/c/d".into(),
        ];
        for d1 in alphabet {
            filters.push(d1.to_string());
            topics.push(d1.to_string());
            for d2 in alphabet {
                filters.push(format!("{d1}/{d2}"));
                topics.push(format!("{d1}/{d2}"));
                for d3 in alphabet {
                    filters.push(format!("{d1}/{d2}/{d3}"));
                    topics.push(format!("{d1}/{d2}/{d3}"));
                }
            }
        }
        filters.retain(|f| mqtt_core::valid_filter(f));
        filters.sort();
        filters.dedup();

        // Spread the filters over several nodes, as peers announce them.
        let mut per_node: HashMap<NodeId, Vec<String>> = HashMap::new();
        for (i, f) in filters.iter().enumerate() {
            per_node
                .entry(NodeId(format!("n{}", i % 4)))
                .or_default()
                .push(f.clone());
        }

        let mut idx = InterestIndex::default();
        for (node, fs) in &per_node {
            idx.replace(node.clone(), fs.clone());
        }

        for topic in &topics {
            assert_eq!(
                idx.nodes_matching(topic),
                reference(topic, &per_node),
                "index and linear scan disagree on topic {topic:?}"
            );
        }

        // Churn: drop one node entirely, re-announce another with a narrower list.
        let dropped = NodeId("n1".into());
        assert!(idx.remove(&dropped));
        per_node.remove(&dropped);

        let narrowed = NodeId("n2".into());
        let keep: Vec<String> = per_node[&narrowed].iter().take(3).cloned().collect();
        idx.replace(narrowed.clone(), keep.clone());
        per_node.insert(narrowed, keep);

        for topic in &topics {
            assert_eq!(
                idx.nodes_matching(topic),
                reference(topic, &per_node),
                "after churn, index and linear scan disagree on topic {topic:?}"
            );
        }
    }

    /// A node announcing an EMPTY filter list still "has interest": the old
    /// `HashMap` stored an empty set and `remove(..).is_some()` was true, and
    /// `peer_dead` branches on that answer.
    #[test]
    fn a_node_with_no_filters_still_has_an_interest_entry() {
        let mut idx = InterestIndex::default();
        let n = NodeId("quiet".into());
        idx.replace(n.clone(), Vec::new());
        assert!(idx.has_node(&n));
        assert!(idx.nodes_matching("anything").is_empty());
        assert!(idx.remove(&n), "an empty announcement is still an entry");
        assert!(!idx.remove(&n), "removing twice reports no entry");
    }

    /// Re-announcing REPLACES rather than merges: a filter dropped from the new
    /// snapshot must stop matching, or a node keeps receiving traffic for
    /// subscriptions it no longer has.
    #[test]
    fn re_announcing_replaces_the_previous_snapshot() {
        let mut idx = InterestIndex::default();
        let n = NodeId("n".into());
        idx.replace(n.clone(), vec!["a/#".into(), "b/c".into()]);
        assert_eq!(idx.nodes_matching("b/c"), HashSet::from([n.clone()]));
        idx.replace(n.clone(), vec!["a/#".into()]);
        assert!(
            idx.nodes_matching("b/c").is_empty(),
            "a filter absent from the new snapshot must stop matching"
        );
        assert_eq!(idx.nodes_matching("a/x"), HashSet::from([n]));
    }

    /// Two nodes sharing one filter both match, and removing one leaves the
    /// other routable — the shared-terminal case the trie prunes on last use.
    #[test]
    fn nodes_sharing_a_filter_are_independent() {
        let mut idx = InterestIndex::default();
        let (n1, n2) = (NodeId("n1".into()), NodeId("n2".into()));
        idx.replace(n1.clone(), vec!["s/+/t".into()]);
        idx.replace(n2.clone(), vec!["s/+/t".into()]);
        assert_eq!(
            idx.nodes_matching("s/x/t"),
            HashSet::from([n1.clone(), n2.clone()])
        );
        idx.remove(&n1);
        assert_eq!(idx.nodes_matching("s/x/t"), HashSet::from([n2]));
    }
}

#[cfg(test)]
mod footprint {
    use super::*;

    /// The in-flight table is bounded, so the size of one entry decides how many
    /// publishes a given memory budget buys. Measured 2026-08-27: 320 bytes, of
    /// which `AppProperties` is 112 — a block a fleet-telemetry PUBLISH almost
    /// never populates, paid on every entry.
    ///
    /// This guards against silent growth: an entry that doubles halves the cap
    /// affordable at the same budget, and nothing else would notice.
    #[test]
    fn a_pending_publish_stays_small() {
        const BUDGETED: usize = 216;
        let actual = std::mem::size_of::<PendingPublish>();
        assert!(
            actual <= BUDGETED,
            "PendingPublish grew to {actual} bytes (budget {BUDGETED}). The in-flight \
             bound is sized on this: at {actual} bytes, N entries now cost \
             {} KiB of fixed overhead alone. Either shrink the struct or re-derive \
             the bound deliberately — do not just raise this number.",
            PENDING_PUBLISH_CAP * actual / 1024
        );
    }

    /// One obligation per destination node, held for as long as the publish is
    /// unacknowledged, so a fan-out across a large cluster multiplies it.
    #[test]
    fn a_forward_obligation_stays_small() {
        const BUDGETED: usize = 120;
        let actual = std::mem::size_of::<ForwardObligation>();
        assert!(
            actual <= BUDGETED,
            "ForwardObligation grew to {actual} bytes (budget {BUDGETED})"
        );
    }
}

#[cfg(test)]
mod pending_bounds {
    use super::*;
    use crate::hub::Hub;
    use mqtt_storage::MemorySessionStore;
    use std::sync::Arc;

    fn hub() -> Hub {
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("pending"));
        let (mut hub, _tx) = Hub::with_config(
            NodeId("pending".into()),
            Arc::new(MemorySessionStore::new()),
        );
        hub.attach_metrics(metrics);
        hub
    }

    /// Register one `QoS` 1 publish; the receiver tells whether its ack was withheld.
    fn register(
        hub: &mut Hub,
        topic: &str,
        payload: &Bytes,
    ) -> (u64, oneshot::Receiver<PublishOutcome>) {
        let (done, wait) = oneshot::channel();
        let id = hub.register_pending(
            done,
            topic,
            payload,
            QoS::AtLeastOnce,
            false,
            None,
            &AppProperties::default(),
        );
        (id, wait)
    }

    fn withheld(wait: &mut oneshot::Receiver<PublishOutcome>) -> bool {
        matches!(wait.try_recv(), Err(oneshot::error::TryRecvError::Closed))
    }

    /// The byte total is the sum of what was charged, through every way out.
    #[test]
    fn the_byte_total_follows_every_mutation() {
        let mut hub = hub();
        let small = Bytes::from(vec![0u8; 100]);
        let large = Bytes::from(vec![0u8; 10_000]);
        let (a, _wa) = register(&mut hub, "t/a", &small);
        let (_b, _wb) = register(&mut hub, "t/bb", &large);
        let (_c, _wc) = register(&mut hub, "t/c", &small);
        let expect = pending_cost("t/a", &small)
            + pending_cost("t/bb", &large)
            + pending_cost("t/c", &small);
        assert_eq!(hub.pending_publishes.bytes(), expect);

        hub.pending_publishes.remove(a);
        assert_eq!(
            hub.pending_publishes.bytes(),
            expect - pending_cost("t/a", &small)
        );
        hub.pending_publishes.pop_first();
        assert_eq!(hub.pending_publishes.bytes(), pending_cost("t/c", &small));
        hub.pending_publishes.pop_first();
        assert_eq!(hub.pending_publishes.bytes(), 0);
        assert!(hub.pending_publishes.is_empty());
    }

    /// The entry bound: the publish past the cap evicts exactly the oldest, and
    /// everything below the cap is left alone — the 4096 default evicted
    /// publishers that were merely numerous (issue #633).
    #[test]
    fn the_entry_cap_evicts_only_the_oldest() {
        let mut hub = hub();
        let payload = Bytes::from_static(b"x");
        let (first, mut first_wait) = register(&mut hub, "t", &payload);
        let mut waits = Vec::with_capacity(PENDING_PUBLISH_CAP);
        for _ in 1..PENDING_PUBLISH_CAP {
            waits.push(register(&mut hub, "t", &payload).1);
        }
        assert_eq!(hub.pending_publishes.len(), PENDING_PUBLISH_CAP);
        assert!(!withheld(&mut first_wait), "nothing is evicted AT the cap");

        let (_new, _w) = register(&mut hub, "t", &payload);
        assert_eq!(hub.pending_publishes.len(), PENDING_PUBLISH_CAP);
        assert!(
            withheld(&mut first_wait),
            "the oldest publish's ack is withheld"
        );
        assert!(hub.pending_publishes.get(first).is_none());
        assert!(!waits.iter_mut().any(withheld));
    }

    /// The byte bound: large payloads reach it long before the entry cap, and one
    /// newcomer may need SEVERAL older entries gone.
    #[test]
    fn the_byte_bound_evicts_until_the_newcomer_fits() {
        let mut hub = hub();
        let mib = Bytes::from(vec![0u8; 1024 * 1024]);
        let mut waits = Vec::new();
        while !hub.pending_publishes.is_full_for(pending_cost("t", &mib)) {
            waits.push(register(&mut hub, "t", &mib).1);
        }
        assert!(hub.pending_publishes.len() < 64, "well under the entry cap");
        assert!(hub.pending_publishes.bytes() <= PENDING_PUBLISH_MAX_BYTES);

        // Four times the size of what it displaces: one eviction is not enough.
        let big = Bytes::from(vec![0u8; 4 * 1024 * 1024]);
        let before = hub.pending_publishes.len();
        let (id, mut wait) = register(&mut hub, "t", &big);
        assert!(hub.pending_publishes.bytes() <= PENDING_PUBLISH_MAX_BYTES);
        let evicted = before + 1 - hub.pending_publishes.len();
        assert!(evicted >= 4, "evicted {evicted}");
        let gone: Vec<bool> = waits.iter_mut().map(withheld).collect();
        assert_eq!(gone.iter().filter(|g| **g).count(), evicted);
        assert!(gone.iter().take(evicted).all(|g| *g), "oldest first");
        assert!(hub.pending_publishes.get(id).is_some() && !withheld(&mut wait));
    }

    /// A publish larger than the whole byte bound is still admitted into an empty
    /// table — refusing it would refuse it forever, with nothing left to evict.
    #[test]
    fn an_oversized_publish_is_admitted_alone() {
        let mut hub = hub();
        let small = Bytes::from_static(b"x");
        let (_s, mut small_wait) = register(&mut hub, "t", &small);
        let huge = Bytes::from(vec![0u8; PENDING_PUBLISH_MAX_BYTES + 1]);
        let (id, mut wait) = register(&mut hub, "t", &huge);
        assert!(withheld(&mut small_wait));
        assert_eq!(hub.pending_publishes.len(), 1);
        assert!(hub.pending_publishes.get(id).is_some() && !withheld(&mut wait));
    }

    /// The sweep retransmits what is OVERDUE, not what is merely outstanding: a
    /// forward younger than one sweep interval is left alone, and the same forward
    /// is re-sent once it has waited that long (issue #633).
    #[test]
    fn the_sweep_retransmits_only_overdue_forwards() {
        let mut hub = hub();
        let peer = NodeId("peer".into());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (ctl, _ctl_rx) = mpsc::unbounded_channel();
        hub.peer_connected(
            peer.clone(),
            1,
            tx,
            ctl,
            None,
            mqtt_cluster::peer::PROTO_MAX,
            Arc::default(),
        );
        let (id, _wait) = register(&mut hub, "t", &Bytes::from_static(b"x"));
        hub.send_acked_forward(id, &peer);
        while rx.try_recv().is_ok() {}

        hub.sweep_pending_forwards();
        assert!(
            rx.try_recv().is_err(),
            "a young forward is not retransmitted"
        );

        let aged = Instant::now()
            .checked_sub(crate::hub::SESSION_SWEEP_INTERVAL)
            .expect("the clock is past one sweep interval");
        hub.pending_publishes.get_mut(id).unwrap().created_at = aged;
        hub.sweep_pending_forwards();
        assert!(rx.try_recv().is_ok(), "an overdue forward is retransmitted");
        assert!(rx.try_recv().is_err(), "exactly once per sweep");
    }

    /// The once-a-second sweep walks the whole table on the hub thread, so its
    /// cost at a FULL table is what the higher cap buys at worst. A measurement,
    /// not an assertion — run it in release:
    /// `cargo test -p mqttd --release --lib -- --ignored sweep_cost --nocapture`
    #[test]
    #[ignore = "timing measurement; run in release with --nocapture"]
    fn sweep_cost_at_a_full_table() {
        for entries in [4096, PENDING_PUBLISH_CAP] {
            let mut hub = hub();
            let peer = NodeId("peer".into());
            let (tx, mut rx) = mpsc::unbounded_channel();
            let (ctl, _ctl_rx) = mpsc::unbounded_channel();
            hub.peer_connected(
                peer.clone(),
                1,
                tx,
                ctl,
                None,
                mqtt_cluster::peer::PROTO_MAX,
                Arc::default(),
            );
            let payload = Bytes::from(vec![0u8; 216]);
            let mut waits = Vec::with_capacity(entries);
            for _ in 0..entries {
                let (id, wait) = register(&mut hub, "fleet/site/1/telemetry", &payload);
                hub.send_acked_forward(id, &peer);
                waits.push(wait);
            }
            while rx.try_recv().is_ok() {}
            let mut time_sweep = |hub: &mut Hub, expect: usize| {
                let mut best = Duration::MAX;
                for _ in 0..10 {
                    let t = Instant::now();
                    hub.sweep_pending_forwards();
                    best = best.min(t.elapsed());
                    let mut frames = 0;
                    while rx.try_recv().is_ok() {
                        frames += 1;
                    }
                    assert_eq!(frames, expect);
                }
                best
            };
            // Young entries: the walk alone, nothing retransmitted.
            let young = time_sweep(&mut hub, 0);
            // Overdue entries: one retransmit per outstanding forward. Back-dated
            // rather than slept for — the measurement is of the sweep, not of the
            // clock, and a real wait would put a second per iteration into it.
            let aged = Instant::now()
                .checked_sub(crate::hub::SESSION_SWEEP_INTERVAL)
                .expect("the clock is past one sweep interval");
            for (_, p) in hub.pending_publishes.iter_mut() {
                p.created_at = aged;
            }
            let overdue = time_sweep(&mut hub, entries);
            println!("sweep over {entries} entries: {young:?} young, {overdue:?} all overdue");
        }
    }
}
