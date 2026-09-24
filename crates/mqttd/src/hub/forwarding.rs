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

#[cfg(test)]
thread_local! {
    /// Test-only: how many peers [`Hub::forward_to_peers`]'s fan-out loop has
    /// examined on THIS thread (issue #613 item 1.3).
    ///
    /// The early return that guard exists for is invisible to every behavioural
    /// observable — the same peers receive the same frames either way — so work
    /// NOT done is the only thing a regression test can assert on. Thread-local
    /// rather than global because `cargo test` runs the unit tests in parallel
    /// threads: the tests that read this call `forward_to_peers` directly on the
    /// test's own thread, so the count is exact and unpolluted. Compiled out of
    /// production builds entirely.
    pub(super) static PEER_FANOUT_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[allow(clippy::struct_excessive_bools)]
/// A `QoS` 1 publish whose acknowledgement is gated on **cluster-wide** durability
/// (ADR 0042 T9): the local fan-out's durable appends (synchronous), the retained
/// authority commit (exhibit ⑦), and one durability-gated ack per acked peer
/// forward (exhibit ⑤). The ack releases only when every obligation resolves;
/// a terminal failure drops the entry, withholding the ack (the publisher retries).
#[derive(Debug)]
pub(super) struct PendingPublish {
    /// Releases the publisher's acknowledgement. `Some` = the publisher is still
    /// waiting, and dropping the entry WITHHOLDS (the sender side of fail-closed).
    /// `None` = it has already been answered and this entry survives only for the
    /// settle window's re-delivery/re-route (issue #613 item 2.1) — dropping it
    /// then signals nothing at all, because `send` consumed the sender and there
    /// is no longer anything a receiver could observe closing.
    ///
    /// Always move through [`PendingPublish::answer`], never `take()` by hand: the
    /// answer must be at-most-once, and every terminal path has to be able to ask
    /// whether it still has anything to say.
    pub(super) done: Option<oneshot::Sender<PublishOutcome>>,
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
    ///
    /// A `Vec`, not a `HashSet`: it holds at most one entry per peer that answered
    /// THIS publish, it is read in exactly one place (`reroute_candidates`, off the
    /// message path) and written in one (`forward_answered`), and the 24 bytes it
    /// gives back are what pay for `done` becoming an `Option` without breaking the
    /// entry's size budget (issue #613 item 2.1 — measured: 216 -> 224 -> 200).
    pub(super) acked_nodes: Vec<NodeId>,
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
    /// scan pending or running). It is WORK-SET MEMBERSHIP, not an ack gate: it is
    /// what [`settle_pending_publishes`](Hub::settle_pending_publishes) filters on,
    /// so while it is set this publish still owes a local re-delivery against
    /// just-materialized subscriptions AND a re-route to peers that have since
    /// advertised interest (exhibit ⑥; duplicates are legal at `QoS` 1). Nothing
    /// but the window closing clears it, and NOTHING may narrow the set it selects
    /// — an entry dropped from it is a delivery silently lost, with no failing test
    /// to say so (issue #613, CORRECTION 1).
    pub(super) awaiting_settle: bool,
    /// Whether the settle window is holding this publish's ACKNOWLEDGEMENT, as
    /// opposed to merely owing it a replay (issue #613 item 2.1).
    ///
    /// Initialised to `routing_unsettled()`, exactly as `awaiting_settle` is — the
    /// fail-safe direction, since registration runs BEFORE the fan-out and cannot
    /// yet know whether the message reached anybody. `publish` clears it, one way
    /// only, once the fan-out produced evidence (a local match, or a shared group
    /// that placed the message). A zero-evidence publish on an unsettled view keeps
    /// it, and is held exactly as before.
    ///
    /// Invariant: `ack_awaits_settle` implies `awaiting_settle`. The ack hold may
    /// be released early; the replay obligation may not.
    pub(super) ack_awaits_settle: bool,
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

    /// Answer the publisher, at most once. Returns whether anything was said —
    /// `false` means the ack was already released (issue #613 item 2.1) and this
    /// entry is alive only for the settle window's replay, so the caller must not
    /// claim, withhold or refuse anything on its behalf.
    pub(super) fn answer(&mut self, outcome: PublishOutcome) -> bool {
        match self.done.take() {
            Some(tx) => {
                let _ = tx.send(outcome);
                true
            }
            None => false,
        }
    }

    /// Whether the publisher has already been answered. A `true` here is the one
    /// thing that makes dropping this entry harmless — and, for the cap
    /// (item 2.4), makes it the right entry to evict.
    pub(super) fn ack_released(&self) -> bool {
        self.done.is_none()
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
        let Some(p) = self.pending_publishes.get(&id) else {
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
                if let Some(p) = self.pending_publishes.get_mut(&id) {
                    // BOTH: the window closing ends the replay obligation and,
                    // for a publish whose fan-out never produced evidence, the ack
                    // hold it has been waiting out (issue #613 item 2.1). Clearing
                    // only one would either strand the entry or strand its
                    // publisher — a zero-evidence publish would never be acked at
                    // all.
                    p.awaiting_settle = false;
                    p.ack_awaits_settle = false;
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
        //
        // The answer lands in a Hub-owned scratch buffer holding the subscription
        // table's OWN interned ids (issue #613 item 1.4) — see
        // [`InterestIndex::matching_into`] for what that saves. TAKEN and restored
        // around the body rather than borrowed in place: the gated arm below calls
        // `&mut self`, and the take keeps the allocation while making every borrow
        // disjoint. EVERY exit below must restore it.
        let mut interested_nodes = std::mem::take(&mut self.interest_scratch);
        self.interest.matching_into(topic, &mut interested_nodes);
        // Issue #613 item 1.6: the O(peers) term, timed on its own rather than
        // folded into `hub_dispatch_seconds`. The clock is read only when metrics
        // are attached, so a bench or unit-test hub pays nothing, and EVERY exit
        // below observes — including the two early returns, because "a fan-out
        // walked zero links" and "no fan-outs happened" are different facts that
        // render identically if only the slow path reports.
        let fanout_started = self.metrics.as_ref().map(|_| Instant::now());
        let mut peer_visits = 0usize;
        if gated {
            let id = gate.unwrap_or_default();
            // One `NodeId` per ACKED forward — on the QoS >= 1 path only, where a
            // frame and a recorded obligation are being built for this target
            // anyway, so the string is noise against what it accompanies. The
            // QoS 0 fan-out below never mints one.
            for c in &interested_nodes {
                let node = NodeId(c.as_str().to_string());
                self.send_acked_forward(id, &node);
            }
            if !retain_broadcasts {
                self.interest_scratch = interested_nodes;
                self.observe_fanout(fanout_started, peer_visits);
                return;
            }
        }
        // Nothing to forward, so do not walk the peer map to discover that
        // (issue #613 item 1.3). The loop below has exactly two exits —
        // `retain_broadcasts` and `interested` — so with neither set every
        // iteration is a lookup that decides to do nothing: O(peers) per publish,
        // spent to reach `continue`. Behaviour-preserving by inspection, because
        // this IS the loop body's own guard, hoisted out of it unchanged. The
        // gated QoS >= 1 path already returned above, so what this saves is the
        // QoS 0 fan-out for a topic no remote node subscribes to — the common
        // shape in a cluster, and the one that scales with node count. It does
        // NOT help QoS 1.
        if interested_nodes.is_empty() && !retain_broadcasts {
            self.interest_scratch = interested_nodes;
            self.observe_fanout(fanout_started, peer_visits);
            return;
        }
        for (node, peer) in &self.peers {
            peer_visits += 1;
            #[cfg(test)]
            PEER_FANOUT_VISITS.with(|c| c.set(c.get() + 1));
            let interested = InterestIndex::contains(&interested_nodes, node);
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
        self.interest_scratch = interested_nodes;
        self.observe_fanout(fanout_started, peer_visits);
    }

    /// Record one peer fan-out's own time and link count (issue #613 item 1.6).
    ///
    /// `started` is `None` exactly when no metrics are attached, which is what
    /// keeps the clock read off a bench or unit-test hub. Split out of
    /// [`forward_to_peers`](Self::forward_to_peers) because that function has
    /// three exits and all three must report: a fan-out that early-returned
    /// having walked no links is a real observation, and dropping it would make
    /// `hub_fanout_peer_visits_total / hub_fanout_seconds_count` — the average
    /// links one publish walks, which is the quantity items 1.3 and 1.4 reduce —
    /// read as if the cheap path never ran.
    fn observe_fanout(&self, started: Option<Instant>, peer_visits: usize) {
        if let (Some(m), Some(at)) = (&self.metrics, started) {
            m.observe_hub_fanout(at.elapsed().as_secs_f64(), peer_visits);
        }
    }

    /// Peers that now advertise matching interest for pending publish `id` but
    /// have neither acked a forward nor have one outstanding — the re-route
    /// targets after a takeover (the dead owner's successor materializes the
    /// inherited sessions and re-advertises their filters).
    pub(super) fn reroute_candidates(&self, id: u64) -> Vec<NodeId> {
        let Some(p) = self.pending_publishes.get(&id) else {
            return Vec::new();
        };
        // Off the message path (takeover re-route), but the same one-walk shape:
        // resolve the interested set once, then test membership per peer.
        let interested_nodes = self.interest.nodes_matching(&p.topic);
        self.peers
            .iter()
            .filter(|(n, _)| {
                !p.acked_nodes.iter().any(|a| a == *n)
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
        let Some(p) = self.pending_publishes.get_mut(&id) else {
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

    /// The cap verdict for an ARRIVING gated publish (issue #613 item 2.4), plus
    /// the id of the victim when the verdict is
    /// [`EvictReplayOnly`](settle::Admission::EvictReplayOnly).
    ///
    /// **`&self`, deliberately, and this is the structural half of
    /// [`refuse_pending`](Self::refuse_pending)'s contract.** `Refused` makes the
    /// positive claim "nothing of this publish was stored anywhere, so retry".
    /// Here that is true BY CONSTRUCTION rather than by comment: this function
    /// cannot mutate the hub — no append, no forward, no retained commit, not even
    /// an id burned from `publish_ids` — and `register_pending` calls it as its
    /// first statement. A future change that stores before admitting would need
    /// `&mut self` and would not compile into this function, so the lie breaks the
    /// build (and `refusing_an_arrival_stores_nothing_anywhere`) rather than
    /// reaching a publisher.
    ///
    /// Both scans below are O(`PENDING_PUBLISH_CAP`) and are paid ONLY when the
    /// ledger is already full — under the cap this returns on the first line.
    fn pending_admission(&self) -> (settle::Admission, Option<u64>) {
        let len = self.pending_publishes.len();
        if len < PENDING_PUBLISH_CAP {
            return (settle::Admission::Admit, None);
        }
        // The LOWEST id whose publisher has already been answered: an entry alive
        // only for the settle window's replay (issue #613 item 2.1). Lowest, i.e.
        // oldest, so the replay records retire in arrival order like a ring.
        let victim = self
            .pending_publishes
            .iter()
            .find(|(_, p)| p.ack_released())
            .map(|(id, _)| *id);
        // Ids increase monotonically and `pending_publishes` is a `BTreeMap`, so
        // its first entry is the OLDEST — the one `pop_first` used to take
        // unconditionally. `first_key_value` is the non-destructive peek the age
        // check needs.
        let now = Instant::now();
        let oldest_age = self
            .pending_publishes
            .first_key_value()
            .map(|(_, p)| now.saturating_duration_since(p.created_at));
        let admission = settle::admit_pending(
            len,
            PENDING_PUBLISH_CAP,
            victim.is_some(),
            oldest_age,
            PENDING_PUBLISH_MAX_AGE,
        );
        (admission, victim)
    }

    /// Register a `QoS` 1 publish whose acknowledgement is gated on cluster-wide
    /// durability (ADR 0042 T9).
    ///
    /// `None` is a REFUSAL taken at the cap (issue #613 item 2.4): `done` has
    /// ALREADY been answered `Refused(PendingCap)`, and the caller must abandon
    /// the whole publish — no fan-out, no peer forward, no retained commit — or
    /// the refusal's claim "nothing was stored, retry" becomes false and the
    /// retry duplicates on every subscriber that received the first copy. That
    /// abandonment is what makes the refusal sayable, and it is the same
    /// plan-then-commit discipline the brownout refusal follows and the same rule
    /// [`refuse_pending`](Self::refuse_pending) enforces downstream (issue #238).
    ///
    /// **Why refuse the ARRIVING publish rather than evict the oldest.** The old
    /// eviction answered a cap overrun by withholding the ack of a publish that
    /// had already fanned out and may already be STORED — a publisher left hanging
    /// for a message the cluster kept, whose retry then duplicates it on every
    /// subscriber that got the first copy. It also punished the wrong publisher:
    /// the victim was whoever published FIRST, not whoever is overrunning the
    /// table. Refusing the arrival costs the overrunning publisher a `0x97` it can
    /// act on immediately (v3.1.1: a close, per [`Refusal311::CloseNoAck`]), and
    /// costs every already-registered publisher nothing at all.
    ///
    /// **The order of victims is the whole policy**, and it is decided by
    /// [`settle::admit_pending`]:
    /// 1. an entry whose publisher was ALREADY answered and which survives only
    ///    for the settle window's replay (issue #613 item 2.1) — evicting it
    ///    withholds nothing and refuses nobody, so it is always taken first;
    /// 2. otherwise the oldest entry, but only past [`PENDING_PUBLISH_MAX_AGE`],
    ///    where it is a leak rather than a publisher still waiting — the liveness
    ///    backstop, kept so one stuck obligation cannot wedge the node shut;
    /// 3. otherwise the arrival is refused.
    ///
    /// All three are counted apart — `publish_dropped{reason="pending-cap-replay"}`,
    /// `{reason="pending-cap"}` and `{reason="pending-cap-admission"}` — so a leak
    /// can never hide inside an overload number.
    ///
    /// Note the interaction with item 2.1, which is why (1) exists at all: an
    /// early-acked publish keeps its ledger slot until the settle window closes,
    /// so during a long window the ledger fills with records nobody is waiting on.
    /// Without (1), item 2.1's longer lifetimes would translate directly into
    /// refused live publishers — 2.1 would have made 2.4 strictly worse.
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
    ) -> Option<u64> {
        // FIRST statement of the function, before a single field is touched. That
        // placement is the contract, not a style choice — see
        // [`pending_admission`](Self::pending_admission).
        let (admission, replay_victim) = self.pending_admission();
        match admission {
            settle::Admission::Admit => {}
            settle::Admission::EvictReplayOnly => {
                // The preferred victim: already ANSWERED, alive only for the
                // settle window's replay (issue #613 items 2.1 x 2.4). No
                // publisher loses an answer and nothing is withheld. What IS lost
                // is the re-delivery to a session materialised during the window,
                // so it is said out loud and counted on its own rather than
                // disappearing into the withhold number.
                if let Some(victim) = replay_victim {
                    if let Some(old) = self.pending_publishes.remove(&victim) {
                        warn!(
                            publish = victim,
                            topic = %old.topic,
                            cap = PENDING_PUBLISH_CAP,
                            "pending-publish cap: evicted an already-ACKNOWLEDGED entry held \
                             only for the settle window's replay; no publisher lost an answer, \
                             but a session materialised during this window will not receive it \
                             (issue #613 item 2.1 x 2.4)"
                        );
                        self.forward_index.retain(|_, pid| *pid != victim);
                        if let Some(m) = &self.metrics {
                            m.publish_dropped("pending-cap-replay");
                        }
                    }
                }
            }
            settle::Admission::EvictOldest => {
                // The LIVENESS BACKSTOP, not the overload path: the oldest entry
                // is older than PENDING_PUBLISH_MAX_AGE, i.e. stuck rather than
                // merely queued. Its ack is WITHHELD — never refused, because it
                // may already be stored — exactly as the old unconditional
                // eviction did, and it keeps its own counter so a leak can never
                // hide inside an overload number.
                if let Some((old_id, old)) = self.pending_publishes.pop_first() {
                    warn!(
                        topic = %old.topic,
                        cap = PENDING_PUBLISH_CAP,
                        age_secs = Instant::now()
                            .saturating_duration_since(old.created_at)
                            .as_secs(),
                        "pending-publish cap: evicted an ABANDONED unacknowledged publish \
                         (older than PENDING_PUBLISH_MAX_AGE; ack withheld, its publisher \
                         retries — this is the liveness backstop, not the overload path, \
                         ADR 0042 T9)"
                    );
                    self.forward_index.retain(|_, pid| *pid != old_id);
                    if let Some(m) = &self.metrics {
                        m.publish_dropped("pending-cap");
                    }
                }
            }
            settle::Admission::Refuse => {
                // Every slot is held by a YOUNG entry whose publisher is still
                // waiting. Refuse the ARRIVAL: it is the one publish of which
                // nothing is stored anywhere, so `Refused`'s positive claim
                // "nothing was stored, retry" is exactly true — and nothing below
                // this point has run, which is what makes that structural.
                warn!(
                    topic = %topic,
                    cap = PENDING_PUBLISH_CAP,
                    "pending-publish cap: REFUSED the arriving publish. Nothing was stored \
                     for it, so the publisher is told (0x97 / close) and can retry; the \
                     publishes already registered keep their acks (issue #613 item 2.4)"
                );
                if let Some(m) = &self.metrics {
                    m.publish_dropped("pending-cap-admission");
                }
                let _ = done.send(PublishOutcome::Refused(PublishRefusal::PendingCap));
                return None;
            }
        }
        self.publish_ids += 1;
        let id = self.publish_ids;
        self.pending_publishes.insert(
            id,
            PendingPublish {
                done: Some(done),
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain,
                message_expiry,
                app: (!app.is_empty()).then(|| Box::new(app.clone())),
                awaiting: HashMap::new(),
                stored: false,
                acked_nodes: Vec::new(),
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
                // Same value, fail-safe: registration runs BEFORE the fan-out, so
                // the only thing knowable here is that the view is unsettled.
                // `publish` clears this one — and only this one — once the fan-out
                // has produced evidence (issue #613 item 2.1).
                ack_awaits_settle: self.routing_unsettled(),
                relaxed: false,
                congested: false,
            },
        );
        Some(id)
    }

    /// Mark a pending publish RELAXED (ADR 0072): its ack releases at
    /// `local_done` instead of waiting for the durability obligations.
    pub(super) fn pending_mark_relaxed(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(&id) {
            p.relaxed = true;
        }
    }

    /// Record that pending publish `id`'s fan-out REACHED somebody, so the settle
    /// window no longer holds its acknowledgement (issue #613 item 2.1).
    ///
    /// Called by [`publish`](Hub::publish) after the fan-out, and only when
    /// [`settle::awaits_settle`] says the evidence is there: a local match, a
    /// shared group that placed the message, or a routing view that is settled
    /// anyway. A fan-out that reached NOBODY while this node admits its view is
    /// incomplete keeps the hold, unchanged — that is the case the gate exists for.
    ///
    /// It touches the ACK and nothing else. `awaiting_settle` — the REPLAY
    /// obligation — is deliberately not reachable from here: the settle pass must
    /// still re-deliver this publish to sessions materialised after it and re-route
    /// it to peers that advertise interest after it, and an ack released early is
    /// no reason to stop owing either. Only the window closing clears that
    /// (issue #613, CORRECTION 1).
    pub(super) fn pending_fan_out_reached(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(&id) {
            p.ack_awaits_settle = false;
        }
    }

    /// The local fan-out obligation resolved OK (durable appends included).
    pub(super) fn pending_local_done(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(&id) {
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
        if let Some(p) = self.pending_publishes.remove(&id) {
            self.forward_index.retain(|_, pid| *pid != id);
            // Issue #613 item 2.1: this entry may have been answered already and
            // be alive only for the settle window's replay. Dropping it then
            // withholds NOTHING — `send` consumed the sender, so there is no
            // longer anything a receiver could observe closing, and the publisher
            // has its `Accepted` truthfully, against the fan-out that placed the
            // message. What IS lost is the replay to sessions materialised since,
            // so it is counted and said out loud rather than disappearing into the
            // withhold number.
            if p.ack_released() {
                warn!(
                    publish = id, topic = %p.topic,
                    "settle replay abandoned for an already-acknowledged publish; \
                     a session materialised during this window will not receive it \
                     (issue #613 item 2.1)"
                );
                if let Some(m) = &self.metrics {
                    m.publish_dropped("settle-replay");
                }
            }
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
    /// Since issue #613 item 2.1 the precondition is STRONGER: stored nowhere **and**
    /// not yet answered. An entry can now outlive its ack — released early against
    /// fan-out evidence, kept only for the settle window's replay — and for that entry
    /// there is no publisher left to refuse. Such a call logs, counts
    /// `publish_dropped{reason="settle-replay-refused"}` and claims nothing.
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
        // `mut` because the answer now moves through `PendingPublish::answer`,
        // which TAKES the sender so it can be at most once (issue #613 item 2.1).
        let Some(mut p) = self.pending_publishes.remove(&id) else {
            return;
        };
        self.forward_index.retain(|_, pid| *pid != id);
        // Issue #613 item 2.1. A publish whose ack already released cannot be
        // refused: `Refused` is a claim made TO a publisher, and this one has been
        // told `Accepted` — truthfully, against a fan-out that placed the message.
        // What was refused is the settle window's REPLAY, a broker-side obligation
        // with no publisher behind it. Say so and stop; never re-answer, and never
        // let this look like the withhold arm below. This strengthens the rule
        // this function enforces rather than weakening it: `Refused` now requires
        // stored-NOWHERE *and* not-yet-answered.
        if p.ack_released() {
            warn!(
                publish = id, topic = %p.topic, refusal = r.as_str(),
                "a settle-window replay was refused for a publish already acknowledged; \
                 nothing is claimed to the publisher (issue #613 item 2.1)"
            );
            if let Some(m) = &self.metrics {
                m.publish_dropped("settle-replay-refused");
            }
            return;
        }
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
        p.answer(PublishOutcome::Refused(r));
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
        let Some(p) = self.pending_publishes.get_mut(&id) else {
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
                if !p.acked_nodes.iter().any(|a| a == node) {
                    p.acked_nodes.push(node.clone());
                }
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
        let ids: Vec<u64> = self.pending_publishes.keys().copied().collect();
        for id in ids {
            // Retransmit outstanding forwards over live links.
            let outstanding: Vec<(u64, ForwardObligation)> = self
                .pending_publishes
                .get(&id)
                .map(|p| p.awaiting.iter().map(|(s, o)| (*s, o.clone())).collect())
                .unwrap_or_default();
            for (seq, obligation) in &outstanding {
                let Some(peer) = self.peers.get(&obligation.node) else {
                    continue; // link down (not dead): wait for it to return
                };
                let Some(p) = self.pending_publishes.get(&id) else {
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
            let Some(p) = self.pending_publishes.get(&id) else {
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
                if let Some(p) = self.pending_publishes.get_mut(&id) {
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
                if let Some(p) = self.pending_publishes.get_mut(&id) {
                    p.reroute_grace = None;
                }
            } else if awaiting_empty {
                if let Some(p) = self.pending_publishes.get_mut(&id) {
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
    ///
    /// Allocating: kept for the OFF-message paths (takeover re-route, tests),
    /// where a `HashSet<NodeId>` is the convenient shape and one publish's worth
    /// of allocation is invisible. The per-publish path uses
    /// [`matching_into`](Self::matching_into) instead.
    pub(super) fn nodes_matching(&self, topic: &str) -> HashSet<NodeId> {
        self.by_filter
            .matching_clients(topic)
            .into_iter()
            .map(|c| NodeId(c.as_str().to_string()))
            .collect()
    }

    /// Fill `out` with every node whose advertised interest matches `topic`, in
    /// one trie walk, as the subscription table's OWN interned ids (issue #613
    /// item 1.4).
    ///
    /// This is the per-publish form. [`nodes_matching`](Self::nodes_matching)
    /// builds a `HashSet<ClientId>`, mints a `NodeId(String)` per match, and
    /// collects those into a SECOND `HashSet<NodeId>` — two hash sets and one
    /// string allocation per interested node, on every message, to answer a
    /// question whose answer is usually "nobody". Here `out` is the caller's
    /// reused buffer and each entry is an `Arc<str>` refcount bump, so a steady
    /// publish stream allocates nothing at all.
    ///
    /// `out` is CLEARED first. Duplicates — a node interested via several
    /// overlapping filters, which `for_each_matching_client` does NOT collapse —
    /// are removed by the linear scan below, which is what the `HashSet` used to
    /// do; at cluster sizes (single-digit nodes matching one topic) a scan of a
    /// few `Arc` pointers beats hashing. Without it a node on two overlapping
    /// filters would receive two copies of a `QoS` 0 publish and two forward
    /// obligations at `QoS` 1.
    pub(super) fn matching_into(&self, topic: &str, out: &mut Vec<ClientId>) {
        out.clear();
        self.by_filter.for_each_matching_client(topic, |c| {
            if !out.iter().any(|k| k == c) {
                out.push(c.clone());
            }
        });
    }

    /// Whether `node` is in a set filled by [`matching_into`](Self::matching_into).
    ///
    /// Linear on purpose, and on the same reasoning: the set is "nodes interested
    /// in ONE topic", bounded by cluster size, so a scan of a handful of `Arc<str>`
    /// pointers is cheaper than the hash set that answering this used to require
    /// building. Compares by string content, which is exactly the pun
    /// [`key`](Self::key) establishes in the other direction.
    pub(super) fn contains(set: &[ClientId], node: &NodeId) -> bool {
        set.iter().any(|c| c.as_str() == node.0)
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

    /// The generated corpus the equivalence tests run over: wildcards at every
    /// position, `$`-rooted topics, empty levels, multi-level topics.
    fn corpus() -> (Vec<String>, Vec<String>) {
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
        (filters, topics)
    }

    /// Issue #613 item 1.4: `matching_into` is the PER-PUBLISH fast path, and it
    /// must answer exactly what [`InterestIndex::nodes_matching`] answers — which
    /// is itself already pinned against the linear scan it replaced, above. The
    /// fast path routing differently from the reference is the one way this
    /// optimization could change WHO gets a message, and it would be silent.
    #[test]
    fn matching_into_agrees_with_nodes_matching() {
        let (filters, topics) = corpus();
        let mut idx = InterestIndex::default();
        let mut per_node: HashMap<NodeId, Vec<String>> = HashMap::new();
        for (i, f) in filters.iter().enumerate() {
            per_node
                .entry(NodeId(format!("n{}", i % 4)))
                .or_default()
                .push(f.clone());
        }
        for (node, fs) in &per_node {
            idx.replace(node.clone(), fs.clone());
        }

        let mut scratch = Vec::new();
        for topic in &topics {
            idx.matching_into(topic, &mut scratch);
            let fast: HashSet<NodeId> = scratch
                .iter()
                .map(|c| NodeId(c.as_str().to_string()))
                .collect();
            assert_eq!(
                fast,
                idx.nodes_matching(topic),
                "matching_into and nodes_matching disagree on topic {topic:?}"
            );
            // The membership test the fan-out loop uses must agree with the set
            // it was filled from, in both directions — a `==` on the wrong field
            // in `InterestIndex::contains` would silently stop forwarding to
            // every peer, and nothing else would notice.
            for node in &fast {
                assert!(
                    InterestIndex::contains(&scratch, node),
                    "contains() missed {node:?} on topic {topic:?}"
                );
            }
            assert!(
                !InterestIndex::contains(&scratch, &NodeId("not-a-node".into())),
                "contains() matched a node that never advertised anything"
            );
        }
    }

    /// The dedup `matching_into` has to do itself. `for_each_matching_client`
    /// visits a client once per matching FILTER, and `by_filter` is
    /// filter -> clients, so a node interested via two overlapping filters is
    /// visited twice. Without the collapse it would receive two copies of a
    /// `QoS` 0 publish and two forward obligations at `QoS` 1 — the one thing the
    /// `HashSet` this replaced was doing for free.
    #[test]
    fn matching_into_collapses_a_node_on_overlapping_filters() {
        let mut idx = InterestIndex::default();
        let n = NodeId("n".into());
        idx.replace(n.clone(), vec!["a/#".into(), "a/x".into(), "+/x".into()]);
        let mut out = Vec::new();
        idx.matching_into("a/x", &mut out);
        assert_eq!(out.len(), 1, "three matching filters, one node: {out:?}");
        assert_eq!(out[0].as_str(), "n");
    }

    /// Issue #613 item 1.4: the per-publish path must hand back the table's OWN
    /// interned id, not a fresh allocation. This is what the change is FOR — the
    /// old path minted a `NodeId(String)` per interested node per message — and a
    /// straw implementation that round-trips through `nodes_matching` passes
    /// every behavioural assertion above while failing this one.
    #[test]
    fn resolving_the_same_interest_twice_mints_no_new_ids() {
        let mut idx = InterestIndex::default();
        idx.replace(NodeId("n0".into()), vec!["a/#".into()]);
        let (mut a, mut b) = (Vec::new(), Vec::new());
        idx.matching_into("a/x", &mut a);
        idx.matching_into("a/x", &mut b);
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        assert!(
            std::ptr::eq(a[0].0.as_ptr(), b[0].0.as_ptr()),
            "each resolution allocated its own id; the whole point of \
             matching_into is that it does not"
        );
    }

    /// `out` is CLEARED first, so a buffer reused across topics never leaks a
    /// stale node into the next publish's fan-out.
    #[test]
    fn matching_into_clears_the_caller_buffer() {
        let mut idx = InterestIndex::default();
        idx.replace(NodeId("n0".into()), vec!["a/#".into()]);
        let mut out = Vec::new();
        idx.matching_into("a/x", &mut out);
        assert_eq!(out.len(), 1);
        idx.matching_into("b/x", &mut out);
        assert!(out.is_empty(), "a stale node survived into the next topic");
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
            4096 * actual / 1024
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

/// The proofs for issue #613's forwarding-zone changes (items 1.3, 1.4, 2.1's
/// `register_pending`/terminal-path half, and 2.4).
///
/// Every assertion is on a DETERMINISTIC observable — a thread-local counter, a
/// pointer identity, a `try_recv` on a oneshot, or paused tokio time. None reads
/// a wall clock, sleeps, or spawns a task, so none can flake.
///
/// Rig facts, verified against the tree: `forwarding.rs` is a CHILD of `hub`, so
/// `hub.peers`, `hub.pending_publishes`, `hub.interest`, `hub.interest_scratch`,
/// `hub.publish_ids` and `hub.cluster_configured` are all reachable despite being
/// hub-private, and every function under test is synchronous and callable
/// directly on the test's own thread — which is what makes `PEER_FANOUT_VISITS`
/// exact under `cargo test`'s parallel threads.
#[cfg(test)]
mod zone_fwd_proofs {
    use super::*;

    /// A bare single-node hub. `cluster_configured` is false and
    /// `interest_authoritative` false at construction, so `routing_unsettled()`
    /// is FALSE here (it is gated on `clustered()`); the settle tests turn the
    /// cluster flag on explicitly and assert the predicate before relying on it.
    fn hub() -> Hub {
        Hub::with_config(
            NodeId("fwd-proofs".into()),
            Arc::new(MemorySessionStore::new()),
        )
        .0
    }

    /// The outbound halves of one peer link. Held by the caller: dropping them
    /// closes the channels, and a closed channel makes `peer.tx.send` fail
    /// silently, which would hide exactly the assertions these tests make.
    struct PeerLink {
        node: NodeId,
        rx: mpsc::UnboundedReceiver<PeerMessage>,
        _ctl_rx: mpsc::UnboundedReceiver<PeerMessage>,
    }

    /// Attach `n` peers, each advertising interest in `filter` only.
    fn attach_peers(hub: &mut Hub, n: usize, filter: &str) -> Vec<PeerLink> {
        (0..n)
            .map(|i| {
                let node = NodeId(format!("p{i}"));
                let (tx, rx) = mpsc::unbounded_channel();
                let (ctl, ctl_rx) = mpsc::unbounded_channel();
                hub.peer_connected(
                    node.clone(),
                    i as u64,
                    tx,
                    ctl,
                    None,
                    PROTO_FORWARD_VERDICT,
                    std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                );
                hub.interest.replace(node.clone(), vec![filter.to_string()]);
                PeerLink {
                    node,
                    rx,
                    _ctl_rx: ctl_rx,
                }
            })
            .collect()
    }

    fn fan_out(hub: &mut Hub, topic: &str, qos: QoS, retain: bool, gate: Option<u64>) {
        hub.forward_to_peers(
            topic,
            &Bytes::from_static(b"payload"),
            qos,
            retain,
            None,
            &AppProperties::default(),
            gate,
        );
    }

    fn visits() -> usize {
        PEER_FANOUT_VISITS.with(std::cell::Cell::get)
    }

    fn reset_visits() {
        PEER_FANOUT_VISITS.with(|c| c.set(0));
    }

    fn register(hub: &mut Hub, topic: &str) -> (Option<u64>, oneshot::Receiver<PublishOutcome>) {
        let (tx, rx) = oneshot::channel();
        let id = hub.register_pending(
            tx,
            topic,
            &Bytes::from_static(b"payload"),
            QoS::AtLeastOnce,
            false,
            None,
            &AppProperties::default(),
        );
        (id, rx)
    }

    // ---- item 1.3: the peer-map walk that produced nothing -----------------

    /// ITEM 1.3. A `QoS` 0 publish on a topic no remote node subscribes to used
    /// to walk the entire peer map to discover that, once per message: O(peers)
    /// of work whose every iteration reached `continue`. The saving has no
    /// behavioural observable — the same peers receive the same frames either
    /// way — so work NOT done is the only thing that can be asserted.
    ///
    /// NOTE for anyone reading this as a `QoS` 1 win: it is not one. The gated
    /// `QoS` >= 1 path returns before this loop already, so item 1.3 fixes the
    /// UNGATED path only.
    #[test]
    fn a_qos0_publish_nobody_wants_does_not_walk_the_peer_map() {
        let mut h = hub();
        let _peers = attach_peers(&mut h, 64, "other/#");
        reset_visits();
        fan_out(&mut h, "t/1", QoS::AtMostOnce, false, None);
        assert_eq!(
            visits(),
            0,
            "the fan-out loop ran for a topic with no interested node and no \
             retained broadcast; that is one lookup per peer, per message, to \
             decide nothing (issue #613 item 1.3)"
        );
    }

    /// The companion that pins the guard did not change WHO gets the message:
    /// one interested peer among 64 uninterested ones still receives exactly one
    /// frame, and the loop still visits every peer to find it.
    #[test]
    fn the_early_return_does_not_change_who_receives_a_publish() {
        let mut h = hub();
        let mut peers = attach_peers(&mut h, 64, "other/#");
        let wanted = NodeId("p7".into());
        h.interest.replace(wanted.clone(), vec!["t/#".into()]);
        reset_visits();
        fan_out(&mut h, "t/1", QoS::AtMostOnce, false, None);
        assert_eq!(visits(), 64, "one interested peer must not skip the loop");
        for p in &mut peers {
            let got = p.rx.try_recv();
            if p.node == wanted {
                assert!(
                    matches!(got, Ok(PeerMessage::Publish { .. })),
                    "the interested peer got {got:?}"
                );
            } else {
                assert!(got.is_err(), "an uninterested peer got {got:?}");
            }
        }
    }

    /// A RETAINED broadcast (no durable-retained authority configured) goes to
    /// EVERY peer regardless of interest — ADR 0014 — so the emptiness guard
    /// must not swallow it. This is the one way a mis-transcribed hoist would
    /// lose messages rather than merely fail to save work.
    #[test]
    fn the_early_return_never_swallows_a_retained_broadcast() {
        let mut h = hub();
        assert!(
            h.durable_retained.is_none(),
            "this test is about the retain-broadcasts path, which durable \
             retained turns off"
        );
        let mut peers = attach_peers(&mut h, 8, "other/#");
        reset_visits();
        fan_out(&mut h, "t/1", QoS::AtMostOnce, true, None);
        assert_eq!(visits(), 8, "a retained broadcast must walk every peer");
        for p in &mut peers {
            assert!(
                matches!(p.rx.try_recv(), Ok(PeerMessage::Publish { .. })),
                "peer {} missed a retained broadcast",
                p.node.0
            );
        }
    }

    // ---- item 1.4: the string minted per interested node, per publish ------

    /// ITEM 1.4. The interested-node set lives in a Hub-owned buffer that is
    /// TAKEN and restored around `forward_to_peers`, so a steady publish stream
    /// re-allocates nothing. Pointer identity is the assertion, because a
    /// correct-but-pointless implementation (a fresh `Vec` per call) is
    /// behaviourally identical.
    #[test]
    fn the_interest_buffer_is_reused_across_publishes() {
        let mut h = hub();
        let _peers = attach_peers(&mut h, 8, "t/#");
        fan_out(&mut h, "t/1", QoS::AtMostOnce, false, None);
        let (ptr, cap) = (h.interest_scratch.as_ptr(), h.interest_scratch.capacity());
        assert_eq!(h.interest_scratch.len(), 8, "the buffer was not restored");
        assert!(cap >= 8);
        fan_out(&mut h, "t/2", QoS::AtMostOnce, false, None);
        assert!(
            std::ptr::eq(h.interest_scratch.as_ptr(), ptr),
            "the second publish re-allocated the interested-node buffer"
        );
        assert_eq!(h.interest_scratch.capacity(), cap);
    }

    /// Every `return` inside `forward_to_peers` must restore the buffer, or it
    /// is silently lost and the optimization degrades to a fresh allocation per
    /// publish — correct, just pointless, and nothing else would notice. This
    /// drives all three exits: the emptiness guard (item 1.3), the gated
    /// non-broadcast return, and the normal fall-through.
    #[test]
    fn every_exit_from_the_fan_out_restores_the_buffer() {
        let mut h = hub();
        let _peers = attach_peers(&mut h, 8, "t/#");

        // Normal fall-through: grows the buffer.
        fan_out(&mut h, "t/1", QoS::AtMostOnce, false, None);
        let ptr = h.interest_scratch.as_ptr();
        assert!(h.interest_scratch.capacity() >= 8);

        // The emptiness guard (item 1.3's early return).
        fan_out(&mut h, "nobody/wants/this", QoS::AtMostOnce, false, None);
        assert!(
            std::ptr::eq(h.interest_scratch.as_ptr(), ptr),
            "the emptiness guard lost the buffer"
        );
        assert!(h.interest_scratch.is_empty());

        // The gated QoS >= 1 non-broadcast return.
        let (id, _rx) = register(&mut h, "t/3");
        let id = id.expect("an empty ledger admits");
        fan_out(&mut h, "t/3", QoS::AtLeastOnce, false, Some(id));
        assert!(
            std::ptr::eq(h.interest_scratch.as_ptr(), ptr),
            "the gated return lost the buffer"
        );
        assert_eq!(
            h.interest_scratch.len(),
            8,
            "the gated return restored an emptied buffer"
        );
    }

    // ---- item 2.1: the settle gate, as REDESIGNED (CORRECTION 1) -----------

    /// ITEM 2.1, CORRECTION 1's regression guard at the FWD level.
    ///
    /// `pending_fan_out_reached` releases the ACK hold and MUST NOT touch
    /// `awaiting_settle`, which is work-set membership: while it is set the
    /// publish still owes a local re-delivery to sessions the takeover scan
    /// materialises later, and a re-route to peers that advertise interest
    /// later. Narrowing that set drops deliveries with no compile error and no
    /// other failing test — which is exactly what the first design of item 2.1
    /// would have done.
    #[test]
    fn releasing_the_ack_hold_does_not_shrink_the_settle_work_set() {
        let mut h = hub();
        h.set_cluster_configured();
        assert!(
            h.routing_unsettled(),
            "the rig must actually be unsettled, or this test passes vacuously"
        );
        let (id, rx) = register(&mut h, "t/x");
        let id = id.expect("an empty ledger admits");
        {
            let p = &h.pending_publishes[&id];
            assert!(
                p.awaiting_settle,
                "registration holds the replay obligation"
            );
            assert!(p.ack_awaits_settle, "registration holds the ack, fail-safe");
        }

        h.pending_fan_out_reached(id);

        let p = &h.pending_publishes[&id];
        assert!(
            !p.ack_awaits_settle,
            "the evidence of a fan-out that reached somebody must release the ack hold"
        );
        assert!(
            p.awaiting_settle,
            "CORRECTION 1: the settle window still owes this publish a re-delivery \
             and a re-route. Clearing this here silently loses both."
        );
        assert!(
            h.pending_publishes
                .iter()
                .filter(|(_, p)| p.awaiting_settle || p.reroute_grace.is_some())
                .count()
                == 1,
            "the entry must still be SELECTED by settle_pending_publishes's filter"
        );
        drop(rx);
    }

    /// Nothing else may write the ack hold, and the one-way clear is one way:
    /// calling it twice, or on an id that does not exist, changes nothing.
    #[test]
    fn the_ack_hold_clear_is_one_way_and_narrow() {
        let mut h = hub();
        h.set_cluster_configured();
        let (id, _rx) = register(&mut h, "t/x");
        let id = id.expect("an empty ledger admits");
        h.pending_fan_out_reached(id);
        h.pending_fan_out_reached(id);
        h.pending_fan_out_reached(id + 9_999);
        let p = &h.pending_publishes[&id];
        assert!(!p.ack_awaits_settle);
        assert!(p.awaiting_settle);
    }

    /// The window closing is the ONE place both holds end. Clearing only the
    /// replay obligation would strand the publisher of a zero-evidence publish
    /// forever: nothing else ever clears the ack hold.
    #[test]
    fn closing_the_window_clears_both_holds_and_acks() {
        let mut h = hub();
        h.set_cluster_configured();
        assert!(h.routing_unsettled());
        let (id, mut rx) = register(&mut h, "t/x");
        let id = id.expect("an empty ledger admits");
        h.pending_local_done(id);
        assert!(
            h.pending_publishes.contains_key(&id),
            "a held publish is not completed while the window is open"
        );
        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "a zero-evidence publish on an unsettled view must NOT be acked"
        );

        // Close the window the one observable way this rig can: the predicate is
        // gated on `clustered()`, so dropping the cluster flag settles it.
        h.cluster_configured = false;
        assert!(!h.routing_unsettled());
        h.settle_pending_publishes();

        assert!(
            !h.pending_publishes.contains_key(&id),
            "the entry must retire once the window has no further claim on it"
        );
        assert!(
            matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
            "the ack held for the window must be released when it closes"
        );
    }

    /// An entry that has already been answered and survives only for the
    /// replay: `refuse_pending` must not claim `Refused` to a publisher that has
    /// been told `Accepted`, and `drop_pending` must not be read as a withhold —
    /// `send` consumed the sender, so there is nothing left to drop.
    ///
    /// This is what makes `refuse_pending`'s contract STRONGER after item 2.1:
    /// `Refused` now requires stored-nowhere AND not-yet-answered.
    #[test]
    fn an_already_acknowledged_entry_is_never_refused_or_withheld() {
        for withhold in [false, true] {
            let mut h = hub();
            let (id, mut rx) = register(&mut h, "t/x");
            let id = id.expect("an empty ledger admits");
            assert!(h
                .pending_publishes
                .get_mut(&id)
                .expect("just registered")
                .answer(PublishOutcome::Accepted));
            assert!(h.pending_publishes[&id].ack_released());
            assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));

            if withhold {
                h.drop_pending(id);
            } else {
                h.refuse_pending(id, PublishRefusal::Brownout);
            }

            assert!(!h.pending_publishes.contains_key(&id));
            assert!(
                matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed)),
                "the publisher was answered a SECOND time (withhold={withhold})"
            );
        }
    }

    /// `answer` is at most once, and reports whether it had anything to say.
    /// That report is what every terminal path branches on, so it is pinned.
    #[test]
    fn the_answer_is_at_most_once() {
        let mut h = hub();
        let (id, mut rx) = register(&mut h, "t/x");
        let id = id.expect("an empty ledger admits");
        let p = h.pending_publishes.get_mut(&id).expect("just registered");
        assert!(!p.ack_released());
        assert!(p.answer(PublishOutcome::Accepted), "the first answer lands");
        assert!(p.ack_released());
        assert!(
            !p.answer(PublishOutcome::Refused(PublishRefusal::Brownout)),
            "a second answer must say nothing at all"
        );
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    // ---- item 2.4: the cap punishing the wrong publisher -------------------

    /// Fill the ledger to the cap with young, unanswered entries, keeping every
    /// receiver alive.
    fn fill_to_cap(hub: &mut Hub) -> Vec<oneshot::Receiver<PublishOutcome>> {
        let mut rxs = Vec::with_capacity(PENDING_PUBLISH_CAP);
        for i in 0..PENDING_PUBLISH_CAP {
            let (id, rx) = register(hub, &format!("fill/{i}"));
            assert!(id.is_some(), "the fill loop must be admitted");
            rxs.push(rx);
        }
        assert_eq!(hub.pending_publishes.len(), PENDING_PUBLISH_CAP);
        rxs
    }

    /// ITEM 2.4. At the cap with every entry young, the ARRIVING publish is
    /// refused — the oldest, which may already be durably stored, keeps its ack.
    ///
    /// The old policy evicted the oldest and withheld its ack: a publisher left
    /// hanging for a message the cluster kept, whose retry then duplicates it on
    /// every subscriber that got the first copy, and it punished whoever
    /// published FIRST rather than whoever is overrunning the table.
    #[test]
    fn at_the_cap_the_arriving_publish_is_refused_not_the_oldest_evicted() {
        let mut h = hub();
        let mut rxs = fill_to_cap(&mut h);
        let first_id = *h
            .pending_publishes
            .keys()
            .next()
            .expect("the ledger is full");

        let (id, mut rx) = register(&mut h, "arriving");
        assert!(id.is_none(), "the arrival must be refused, not admitted");
        assert!(
            matches!(
                rx.try_recv(),
                Ok(PublishOutcome::Refused(PublishRefusal::PendingCap))
            ),
            "the refused publisher must be TOLD, with a reason it can act on"
        );
        assert!(
            h.pending_publishes.contains_key(&first_id),
            "the oldest entry was evicted anyway"
        );
        assert!(
            matches!(rxs[0].try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "the oldest publisher's ack was withheld by an arrival it had \
             nothing to do with"
        );
        assert_eq!(
            h.pending_publishes.len(),
            PENDING_PUBLISH_CAP,
            "the ledger must stay bounded"
        );
    }

    /// The STRUCTURAL half of `refuse_pending`'s contract: `Refused` claims
    /// "nothing of this publish was stored anywhere, so retry". The refusal is
    /// taken in `pending_admission`, which is `&self` and runs as
    /// `register_pending`'s first statement, so no id is burned, no entry is
    /// inserted, and no index is touched.
    ///
    /// If someone later stores something before admitting, THIS test fails
    /// rather than a publisher being lied to.
    #[test]
    fn refusing_an_arrival_stores_nothing_anywhere() {
        let mut h = hub();
        let _rxs = fill_to_cap(&mut h);
        let before_ids: Vec<u64> = h.pending_publishes.keys().copied().collect();
        let before_publish_ids = h.publish_ids;
        let before_forward_index = h.forward_index.len();

        let (id, _rx) = register(&mut h, "arriving");
        assert!(id.is_none());

        assert_eq!(
            h.publish_ids, before_publish_ids,
            "a refused publish burned an id, so something about it EXISTED"
        );
        assert_eq!(
            h.pending_publishes.keys().copied().collect::<Vec<_>>(),
            before_ids,
            "a refused publish changed the ledger"
        );
        assert_eq!(h.forward_index.len(), before_forward_index);
    }

    /// ITEMS 2.1 x 2.4. Item 2.1 makes entries outlive their acks, so during a
    /// long takeover window the ledger fills with records nobody is waiting on.
    /// Without this ordering, 2.1's longer lifetimes would translate directly
    /// into refused LIVE publishers — 2.1 would have made 2.4 strictly worse.
    ///
    /// An already-answered entry is the victim: evicting it withholds nothing
    /// and refuses nobody.
    #[test]
    fn at_the_cap_an_already_acked_replay_entry_is_evicted_before_refusing() {
        let mut h = hub();
        let mut rxs = fill_to_cap(&mut h);
        // Answer one entry in the middle, leaving it alive for the replay.
        let ids: Vec<u64> = h.pending_publishes.keys().copied().collect();
        let victim = ids[17];
        assert!(h
            .pending_publishes
            .get_mut(&victim)
            .expect("in the ledger")
            .answer(PublishOutcome::Accepted));

        let (id, mut rx) = register(&mut h, "arriving");
        assert!(
            id.is_some(),
            "a slot was held by an entry nobody is waiting on; the arrival must \
             not pay for it"
        );
        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "the arriving publisher must not be refused"
        );
        assert!(
            !h.pending_publishes.contains_key(&victim),
            "the already-answered entry must be the one evicted"
        );
        assert_eq!(h.pending_publishes.len(), PENDING_PUBLISH_CAP);
        for (i, rx) in rxs.iter_mut().enumerate() {
            if i == 17 {
                continue;
            }
            assert!(
                matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
                "an unanswered publisher at index {i} lost its ack"
            );
        }
    }

    /// The liveness backstop must survive the policy change: one stuck
    /// obligation cannot be allowed to wedge the ledger shut forever, so past
    /// `PENDING_PUBLISH_MAX_AGE` the oldest entry is evicted (ack WITHHELD,
    /// never refused — it may already be stored) and the arrival admitted.
    ///
    /// Paused tokio time IS the clock `created_at` is read against
    /// (`hub::Instant` is `tokio::time::Instant`), so this is deterministic.
    #[tokio::test(start_paused = true)]
    async fn the_cap_still_evicts_an_abandoned_entry() {
        let mut h = hub();
        let mut rxs = fill_to_cap(&mut h);
        let oldest = *h
            .pending_publishes
            .keys()
            .next()
            .expect("the ledger is full");

        tokio::time::advance(PENDING_PUBLISH_MAX_AGE + Duration::from_secs(1)).await;

        let (id, mut rx) = register(&mut h, "arriving");
        assert!(id.is_some(), "the backstop must admit the arrival");
        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "the arrival must not be refused once the backstop fires"
        );
        assert!(
            !h.pending_publishes.contains_key(&oldest),
            "the abandoned oldest entry must be evicted"
        );
        assert!(
            matches!(rxs[0].try_recv(), Err(oneshot::error::TryRecvError::Closed)),
            "the evicted entry's ack is WITHHELD (sender dropped), never refused \
             — it may already be stored"
        );
        assert_eq!(h.pending_publishes.len(), PENDING_PUBLISH_CAP);
    }

    /// Below the cap nothing is scanned and nothing is evicted — the common
    /// path stays a plain insert.
    #[test]
    fn below_the_cap_every_publish_is_admitted() {
        let mut h = hub();
        let mut rxs = Vec::new();
        for i in 0..64 {
            let (id, rx) = register(&mut h, &format!("t/{i}"));
            assert!(id.is_some());
            rxs.push(rx);
        }
        assert_eq!(h.pending_publishes.len(), 64);
        for rx in &mut rxs {
            assert!(matches!(
                rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
    }
}
