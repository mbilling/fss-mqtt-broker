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

use super::*;

#[allow(clippy::struct_excessive_bools)]
/// A `QoS` 1 publish whose acknowledgement is gated on **cluster-wide** durability
/// (ADR 0042 T9): the local fan-out's durable appends (synchronous), the retained
/// authority commit (exhibit ⑦), and one durability-gated ack per acked peer
/// forward (exhibit ⑤). The ack releases only when every obligation resolves;
/// a terminal failure drops the entry, withholding the ack (the publisher retries).
#[derive(Debug)]
pub(super) struct PendingPublish {
    /// Releases the publisher's acknowledgement (dropped = withheld).
    pub(super) done: oneshot::Sender<PublishOutcome>,
    /// The forwarded frame, kept for retransmission and takeover re-routing.
    pub(super) topic: String,
    pub(super) payload: Bytes,
    pub(super) qos: QoS,
    pub(super) retain: bool,
    pub(super) message_expiry: Option<u32>,
    pub(super) app: AppProperties,
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
    pub(super) async fn redeliver_pending(&mut self, id: u64) -> DurableOutcome {
        let Some(p) = self.pending_publishes.get(&id) else {
            return DurableOutcome::Ok;
        };
        let (topic, payload, qos, expiry, app, since) = (
            p.topic.clone(),
            p.payload.clone(),
            p.qos,
            p.message_expiry,
            p.app.clone(),
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
            all_durable = all_durable.and(
                self.deliver_to_client(
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
                )
                .await,
            );
        }
        all_durable
    }

    /// The takeover window closed for this node (an inherited-session scan just
    /// landed): every held pending publish re-delivers **locally** against the
    /// just-materialized subscriptions (duplicates are legal at `QoS` 1 — the
    /// alternative was an ack into the void, exhibit ⑥), then re-checks remote
    /// interest via the sweep's re-route path before its ack can release.
    pub(super) async fn settle_pending_publishes(&mut self) {
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
            let out = self.redeliver_pending(id).await;
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
        if gated {
            let targets: Vec<NodeId> = self
                .remote_interest
                .iter()
                .filter(|(_, filters)| filters.iter().any(|f| topic_matches(f, topic)))
                .map(|(node, _)| node.clone())
                .collect();
            let id = gate.unwrap_or_default();
            for node in targets {
                self.send_acked_forward(id, &node);
            }
            if !retain_broadcasts {
                return;
            }
        }
        for (node, peer) in &self.peers {
            let interested = self
                .remote_interest
                .get(node)
                .is_some_and(|filters| filters.iter().any(|f| topic_matches(f, topic)));
            if gated && interested {
                continue; // already handled (acked or legacy) above
            }
            if !(retain_broadcasts || interested) {
                continue;
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
        let Some(p) = self.pending_publishes.get(&id) else {
            return Vec::new();
        };
        self.peers
            .iter()
            .filter(|(n, _)| {
                !p.acked_nodes.contains(*n)
                    && !p.awaiting.values().any(|o| &o.node == *n)
                    && self
                        .remote_interest
                        .get(*n)
                        .is_some_and(|fs| fs.iter().any(|f| topic_matches(f, &p.topic)))
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
        if self.pending_publishes.len() >= PENDING_PUBLISH_CAP {
            if let Some((old_id, old)) = self.pending_publishes.pop_first() {
                warn!(
                    topic = %old.topic,
                    cap = PENDING_PUBLISH_CAP,
                    "pending-publish cap: dropped the OLDEST unacknowledged publish \
                     (ack withheld; its publisher retries — ADR 0042 T9)"
                );
                self.forward_index.retain(|_, pid| *pid != old_id);
                if let Some(m) = &self.metrics {
                    m.publish_dropped("pending-cap");
                }
            }
        }
        self.publish_ids += 1;
        let id = self.publish_ids;
        self.pending_publishes.insert(
            id,
            PendingPublish {
                done,
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain,
                message_expiry,
                app: app.clone(),
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
            },
        );
        id
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
        if self.pending_publishes.remove(&id).is_some() {
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
        let Some(p) = self.pending_publishes.remove(&id) else {
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
    pub(super) async fn forward_answered(
        &mut self,
        node: &NodeId,
        seq: u64,
        verdict: ForwardVerdict,
    ) {
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
                    self.reselect_shared(id, obligation, DurableOutcome::Refused(r))
                        .await;
                }
                ForwardKind::Ordinary => self.refuse_pending(id, r),
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(super) async fn sweep_pending_forwards(&mut self) {
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
                let out = self.redeliver_pending(id).await;
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
