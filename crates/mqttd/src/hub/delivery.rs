//! The delivery plane — fan-out planning, per-subscriber delivery, shared-group
//! selection, and the ordered send chain (issue #258 slice 4: moved verbatim from
//! `hub/mod.rs`, no logic edits).
//!
//! **The invariant this module owns:** every answerable publish resolves through
//! ONE plan (`deliver`): policy first (`hub/policy.rs`, effect-free refusals),
//! then targets, then per-subscriber delivery whose durable half is frozen into
//! lane jobs (`hub/lanes.rs`) — ack-after-durable, live sends only after the
//! store answered. The send chain (`send_to_client` → `send_qos_publish` →
//! `drain_backlog` → `stage_outbound_record`) is deliberately synchronous
//! (`fn`, not `async fn`): the compiler is the guard that no store await creeps
//! back onto the loop (ADR 0061). Shared groups select exactly one member per
//! message with in-group re-selection on refusal; backlog bounds are enforced
//! here against `crate::backpressure`'s limits, shedding QoS 0 rather than
//! queueing without bound.

use super::*;

impl Hub {
    /// Apply a message on this node: store/clear retained state and deliver to local
    /// ordinary subscribers. Does **not** forward or run shared selection — used both
    /// for local publishes (via
    /// [`publish`](Self::publish)) and for publishes received from a peer, which must
    /// never be re-forwarded.
    /// Returns `(durable, matched)`: `durable` is non-`Ok` when a durable enqueue
    /// failed (ADR 0041 T5) or a stated policy refused one (0041-T11), and `matched`
    /// is how many local ordinary subscribers the topic found — a gated forward
    /// answered from a zero-match fan-out while the routing view is unsettled must
    /// refuse, not OK (0043-P4 exhibit ②).
    #[allow(clippy::too_many_arguments)] // the delivery-path fields, plus the No Local publisher
    pub(super) async fn deliver(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
        publisher: Option<&ClientId>,
        gate: &AppendGate,
    ) -> (DurableOutcome, usize) {
        // PLAN pass (issue #238): the recipients and their delivery terms are computed
        // with no I/O, so the refusal can be decided BEFORE the retained mutation below
        // — a publish answered "not accepted" must not already have overwritten a
        // durable retained value that every future subscriber will now see. Also before
        // any live send: an online clean-session or `QoS` 0 subscriber of a wholesale-
        // refused publish receives nothing, because the publisher still owns the message
        // and is expected to retry it (there is no way to say "delivered to two of your
        // three subscribers" on one PUBACK).
        let targets = self.ordinary_targets(topic, publisher, retain);
        let matched = targets.len();
        if gate.answerable() {
            let owes = targets
                .iter()
                .any(|(c, granted, _)| self.owes_durable(c, min_qos(qos, *granted)));
            if let Some(r) = self.plan_refusal(owes) {
                self.count_refusal(r);
                return (DurableOutcome::Refused(r), matched);
            }
        }
        // COMMIT pass. Under durable retained (ADR 0037 §3) the cache is warmed
        // exclusively by the owner's post-commit, token-carrying fan-out — applying the
        // raw (uncommitted, untokened) flag here is exactly the everyday-race divergence
        // the ADR removes.
        let mut retained_ok = true;
        if retain && self.durable_retained.is_none() {
            // A zero-length retained payload clears the retained message
            // [MQTT-3.3.1-10]; `RetainedStore::set` implements both cases.
            let message = Message {
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain: true,
                app: app.clone(),
                // The absolute deadline persists with the copy (issue #227).
                expires_at: message_expiry.map(|s| self.clock.now_epoch_secs() + u64::from(s)),
            };
            self.retained_may_expire |= message.expires_at.is_some();
            if let Err(e) = self.retained.set(&message).await {
                // Fail closed (audit #203): a failed retained write must NOT be acked as
                // durable. The store is write-through fsync'd, so a QoS≥1 publisher told its
                // retained value was stored, when it was not, would lose it on the next
                // subscriber's replay. This matches the sibling paths that already fail closed
                // (the offline queue-enqueue and the QoS-2 dedup write); the publisher retries
                // and the value is re-stored (at-least-once for the retained store).
                warn!(topic = %topic, error = %e, "failed to update retained message");
                if let Some(m) = &self.metrics {
                    m.retained_apply_failed();
                }
                retained_ok = false;
            }
        }
        // Live deliveries carry retain=0 [MQTT-3.3.1-9]. The retained-write outcome gates the
        // publisher's ack alongside the live-delivery durability. A FAILED retained write
        // stays a withhold rather than a reason code — the store errored, and no reason
        // code honestly says "I do not know what happened" (`DurableOutcome::Failed`
        // therefore dominates any refusal).
        let (durable, _matched) = self
            .deliver_local(topic, payload, qos, message_expiry, app, targets, gate)
            .await;
        let durable = if retained_ok {
            durable
        } else {
            durable.and(DurableOutcome::Failed)
        };
        (durable, matched)
    }

    /// The highest `QoS` granted to `client` across its filters matching `topic`.
    pub(super) fn granted_qos(&self, client: &ClientId, topic: &str) -> QoS {
        self.subs_by_client
            .get(client)
            .into_iter()
            .flatten()
            .filter(|(f, _)| topic_matches(f, topic))
            .map(|(_, q)| *q)
            .max_by_key(|q| *q as u8)
            .unwrap_or(QoS::AtMostOnce)
    }

    /// Deliver a message to this node's **ordinary** local subscribers at
    /// `min(qos, granted)` each. Shared subscriptions are routed separately by
    /// [`deliver_shared`](Self::deliver_shared) (ADR 0015).
    /// Returns `false` when any recipient's durable enqueue failed (ADR 0041 T5).
    pub(super) fn suppressed_by_no_local(
        &self,
        client: &ClientId,
        topic: &str,
        publisher: Option<&ClientId>,
    ) -> bool {
        publisher == Some(client)
            && self
                .no_local
                .get(client)
                .is_some_and(|fs| fs.iter().any(|f| mqtt_core::topic_matches(f, topic)))
    }

    /// Retain As Published (#198, MQTT 5 §3.8.3.1): whether `client` holds a RAP subscription
    /// matching `topic`, so a message forwarded to it keeps the RETAIN flag it was published
    /// with instead of the flag being cleared [MQTT-3.3.1-9]. This is what lets a re-forwarder
    /// (the boundary bridge) carry *live* retained state across a boundary (#189).
    pub(super) fn keeps_retain_flag(&self, client: &ClientId, topic: &str) -> bool {
        self.retain_as_published
            .get(client)
            .is_some_and(|fs| fs.iter().any(|f| mqtt_core::topic_matches(f, topic)))
    }

    /// The ordinary (non-shared) local recipients of `topic` and each one's delivery
    /// terms: `(client, granted QoS, wire retain flag)`.
    ///
    /// Pure and I/O-free, so it can be computed in the PLAN pass — before any side
    /// effect — and handed to the COMMIT pass unchanged (issue #238).
    pub(super) fn ordinary_targets(
        &self,
        topic: &str,
        publisher: Option<&ClientId>,
        source_retain: bool,
    ) -> Vec<(ClientId, QoS, bool)> {
        self.table
            .matching_clients(topic)
            .into_iter()
            .filter(|c| !self.suppressed_by_no_local(c, topic, publisher))
            .map(|c| {
                let granted = self.granted_qos(&c, topic);
                let retain = source_retain && self.keeps_retain_flag(&c, topic);
                (c, granted, retain)
            })
            .collect()
    }

    /// Whether delivering to `client` at `delivered_qos` OWES a durable append: a
    /// persistent session is promised redelivery of anything unacknowledged (#124),
    /// and at-most-once is promised nothing. Both checks are in-memory.
    pub(super) fn owes_durable(&self, client: &ClientId, delivered_qos: QoS) -> bool {
        self.is_persistent(client) && delivered_qos != QoS::AtMostOnce
    }

    #[allow(clippy::too_many_arguments)] // the delivery fields, plus the two subscription options
    pub(super) async fn deliver_local(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
        targets: Vec<(ClientId, QoS, bool)>,
        gate: &AppendGate,
    ) -> (DurableOutcome, usize) {
        debug!(topic = %topic, ordinary = targets.len(), "local delivery");
        let matched = targets.len();
        // #219: a delivery made here is one the apply path must not repeat — record
        // the value identity into any open retained-delivery window. Recording only;
        // the live path is never suppressed (a wrongly swallowed publish would break
        // QoS 1, a rare extra copy does not). Computed only while windows exist.
        let window_id = (!self.retained_windows.is_empty()).then(|| {
            retained_value_id(
                topic,
                payload.as_ref(),
                qos as u8,
                &AppProps::from(app).encode(),
            )
        });
        let mut all_durable = DurableOutcome::Ok;
        for (c, granted, retain) in targets {
            if let Some(id) = window_id {
                if let Some(w) = self.retained_windows.get_mut(&c) {
                    w.seen.insert(topic.to_string(), id);
                }
            }
            all_durable = all_durable.and(
                self.deliver_to_client(
                    &c,
                    topic,
                    payload,
                    min_qos(qos, granted),
                    message_expiry,
                    app,
                    retain,
                    gate,
                )
                .await,
            );
        }
        (all_durable, matched)
    }

    /// Deliver one message to a single named recipient: live if online (tracking
    /// `QoS` > 0 in flight), else queued if the session is persistent, else dropped.
    /// The unit of both ordinary and shared (ADR 0015) delivery; `qos` is the
    /// already-downgraded delivery `QoS`.
    /// Returns [`DurableOutcome::Failed`] when a durable enqueue failed terminally —
    /// the caller withholds the publisher's ack so it retries (ADR 0041 T5) — and
    /// [`DurableOutcome::Refused`] when a stated policy refused it, which the caller
    /// turns into an answer the publisher can act on, per protocol version
    /// (0041-T11, issue #238).
    /// `retain` is the flag to put on the wire — set only for a Retain As Published
    /// subscriber on the ordinary path (#198); every other path clears it [MQTT-3.3.1-9].
    ///
    /// `answerable` says whether SOMEBODY IS BEING TOLD about a refusal — a gated
    /// publisher, or a peer awaiting a verdict. It is the whole justification for
    /// suppressing the live send when the durable copy is refused: the message is not
    /// lost, because its owner still holds it and will retry. With `answerable = false`
    /// there is no such owner (a Will, a retained-window back-fill), so suppressing the
    /// live delivery would destroy the message outright — and a Will suppressed during
    /// an incident is the opposite of what [MQTT-3.14.4-3] is for. An UNANSWERABLE
    /// refusal therefore delivers live anyway (recorded nowhere, so nothing is owed) and
    /// is counted as the genuine drop it is (issue #238).
    #[allow(clippy::too_many_arguments)] // the delivery fields, plus the RAP retain flag
    pub(super) async fn deliver_to_client(
        &mut self,
        client: &ClientId,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
        retain: bool,
        gate: &AppendGate,
    ) -> DurableOutcome {
        let answerable = gate.answerable();
        // DECIDE BEFORE COMMITTING, for the single-target callers too (issue #238).
        // `deliver`'s plan pass already decided for a local fan-out and returned before
        // reaching here, so this fires only for the callers that arrive with ONE target
        // and no plan pass of their own: a peer's answerable shared delivery
        // (`RemoteSharedDeliverAcked`) and the settle-window re-delivery
        // (`redeliver_pending`). Deciding here keeps a refusal effect-free on those paths
        // as well — nothing appended, nothing sent — which is what lets the origin
        // re-select within a shared group, and what keeps `submit_append`'s invariant
        // assert a statement about the WHOLE hub rather than just the local publish path.
        if answerable && self.owes_durable(client, qos) {
            if let Some(r) = self.plan_refusal(true) {
                self.count_refusal(r);
                return DurableOutcome::Refused(r);
            }
        }
        let message = Message {
            topic: topic.to_string(),
            payload: payload.clone(),
            qos,
            retain,
            app: app.clone(),
            expires_at: message_expiry.map(|s| self.clock.now_epoch_secs() + u64::from(s)),
        };
        let persistent = self.is_persistent(client);
        if let Some(online) = self.online.get(client) {
            let (conn_id, tx) = (online.conn_id, online.tx.clone());
            // Durability follows the SESSION, not the connection (#124). A persistent
            // subscriber is owed redelivery of anything unacknowledged, so the record
            // has to exist before the packet reaches the wire — otherwise a crash in
            // between loses a message the publisher was already acked for, and there is
            // no trace of it anywhere. A clean session is skipped because it has nothing
            // to resume into, and `QoS` 0 because at-most-once owes no redelivery.
            //
            // Since issue #242 the append runs OFF-loop, in this session's lane; the
            // live send moves with it into the `AppendDone` handler, which is what
            // keeps the durable-before-wire ordering structural: the send site
            // literally receives the offset from the completion.
            if persistent && qos != QoS::AtMostOnce {
                return match self.submit_append(
                    client,
                    &message,
                    message_expiry,
                    gate,
                    Some(conn_id),
                ) {
                    Submitted::Queued => DurableOutcome::Ok,
                    // Returns BEFORE any live send: delivering live with no durable
                    // record promises a redelivery the store cannot honour, and the
                    // publisher — which is about to be refused — would retry into a
                    // duplicate carrying no DUP flag and no offset to truncate.
                    Submitted::Refused(r) if answerable => DurableOutcome::Refused(r),
                    // UNANSWERABLE refusal (a Will, a retained-window back-fill):
                    // nobody will be told and nobody will retry, so the live send is
                    // the only way the message reaches its subscriber at all.
                    // `submit_append` already counted the lost durable copy as the
                    // genuine drop it is. No offset: nothing promises a redelivery.
                    // If earlier appends for this SAME client are still in flight, a
                    // direct send would OVERTAKE their post-durable live sends — the
                    // exact rule the `QoS` 0 branch below states — so it rides the
                    // lane as a passthrough instead; a saturated lane sheds it
                    // (already counted at submit): an unanswerable refusal accepts
                    // loss by definition, and reordering is what nothing permits.
                    Submitted::Refused(_) => {
                        if self
                            .append_lanes
                            .get(client)
                            .is_some_and(|l| l.outstanding > 0)
                        {
                            let _ = self.submit_passthrough(
                                client,
                                &message,
                                message_expiry,
                                Some(conn_id),
                            );
                            return DurableOutcome::Ok;
                        }
                        self.send_to_client(client, &tx, &message, retain, message_expiry, None)
                            .await;
                        if let Some(m) = &self.metrics {
                            m.publish_delivered(qos_num(qos));
                        }
                        DurableOutcome::Ok
                    }
                    // The lane is full: fail closed exactly like a failed store write
                    // (the caller withholds the publisher's ack so it retries).
                    Submitted::Full => DurableOutcome::Failed,
                };
            }
            // No append owed (`QoS` 0, or a clean session). If earlier appends for
            // this SAME client are still in flight, a direct send would overtake
            // their post-durable live sends — route it through the lane as a
            // passthrough job so per-client wire order survives the motion
            // (issue #242). The common case (no lane, or an idle one) stays a
            // direct on-loop send with zero added latency.
            if self
                .append_lanes
                .get(client)
                .is_some_and(|l| l.outstanding > 0)
            {
                // `Queued` delivers in order at completion. A saturated lane sheds
                // instead: at-most-once permits dropping (already counted), and
                // sending directly would REORDER, which nothing permits.
                let _ = self.submit_passthrough(client, &message, message_expiry, Some(conn_id));
                return DurableOutcome::Ok;
            }
            self.send_to_client(client, &tx, &message, retain, message_expiry, None)
                .await;
            if let Some(m) = &self.metrics {
                m.publish_delivered(qos_num(qos));
            }
            return DurableOutcome::Ok;
        }
        if persistent {
            // Offline but persistent: queue for replay on reconnect.
            return match self.submit_append(client, &message, message_expiry, gate, None) {
                Submitted::Queued => DurableOutcome::Ok,
                // At-most-once owes no redelivery, so a refused enqueue for it is a
                // genuine drop with nothing to refuse the publisher for — the same
                // rule the online branch states by not appending at `QoS` 0 at all.
                // (Reachable when a `QoS` ≥ 1 publish is downgraded to a `QoS` 0
                // subscription, which is exactly the case that owes nothing.)
                Submitted::Refused(_) if qos == QoS::AtMostOnce => DurableOutcome::Ok,
                // Unanswerable and offline: there is no live send to fall back on, so
                // the message is genuinely gone. `submit_append` counted it; reporting
                // a refusal to a caller that has nobody to refuse would only turn a
                // drop into a spurious withhold of an unrelated publisher's ack.
                Submitted::Refused(_) if !answerable => DurableOutcome::Ok,
                Submitted::Refused(r) => DurableOutcome::Refused(r),
                Submitted::Full => DurableOutcome::Failed,
            };
        }
        DurableOutcome::Ok
    }

    /// Route a message to the shared subscriptions matching `topic`: for each group,
    /// select exactly one member across the **whole cluster** (round-robin) and
    /// deliver to it — locally, or via a targeted `SharedDeliver` to the member's
    /// node (ADR 0015). The originating node is the sole selector, so there is no
    /// double delivery.
    /// Returns a non-`Ok` [`DurableOutcome`] when a chosen LOCAL member's durable enqueue
    /// failed or was refused — the same contract as [`deliver_local`](Self::deliver_local),
    /// and for the same reason: a shared subscriber is a
    /// persistent subscriber owed redelivery, so an acked-but-unrecorded message is the
    /// #124/#164 loss whether it arrived via an ordinary or a shared subscription.
    ///
    /// A member chosen on a PEER is ANSWERABLE since 0041-T12 (issue #238): a gated
    /// `QoS` ≥ 1 delivery to a proto-7 peer goes out as
    /// [`PeerMessage::SharedDeliverAcked`] and becomes an obligation on the pending
    /// publish — same seq space, same sweep retransmission, same cap — so the publisher's
    /// ack waits for the owning node to actually take the message. It replaces a
    /// four-state outcome with a three-state one: stored on the chosen member's node,
    /// stored on a RE-SELECTED member, or nobody took it and the publisher still owns it.
    /// The fourth state — nobody took it AND the publisher was acked — is the #238 defect
    /// and no longer reachable. `QoS` 0 deliveries and proto-6 peers keep today's
    /// fire-and-forget `SharedDeliver`: nothing is owed, or the link cannot carry the
    /// answer (a documented rolling-upgrade skew residual).
    pub(super) async fn deliver_shared(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
        gate: Option<u64>,
    ) -> DurableOutcome {
        let answerable = gate.is_some();
        let append_gate = gate.map_or(AppendGate::None, AppendGate::Pending);
        let mut all_durable = DurableOutcome::Ok;
        for (key, candidates) in self.shared_candidates(topic) {
            let Some(chosen) = self.select_shared(&key, &candidates) else {
                debug!(topic = %topic, "shared group has no reachable member");
                continue;
            };
            let delivered_qos = min_qos(qos, chosen.qos);
            match chosen.node {
                None => {
                    all_durable = all_durable.and(
                        self.deliver_to_client(
                            &chosen.client,
                            topic,
                            payload,
                            delivered_qos,
                            message_expiry,
                            app,
                            false, // shared delivery clears RETAIN (#198)
                            &append_gate,
                        )
                        .await,
                    );
                }
                Some(node) => {
                    let answerable_remote = answerable
                        && qos_num(delivered_qos) >= 1
                        && self.peer_proto(&node) >= PROTO_FORWARD_VERDICT;
                    match (answerable_remote, gate) {
                        (true, Some(id)) => self.register_forward(
                            id,
                            ForwardObligation {
                                node: node.clone(),
                                kind: ForwardKind::Shared {
                                    key: key.clone(),
                                    client: chosen.client.clone(),
                                    qos: delivered_qos,
                                    tried: vec![(Some(node), chosen.client.clone())],
                                },
                            },
                        ),
                        _ => self.send_shared_to_peer(
                            &node,
                            &chosen.client,
                            topic,
                            payload,
                            delivered_qos,
                            message_expiry,
                            app,
                        ),
                    }
                }
            }
        }
        all_durable
    }

    /// Re-select within a shared group after the chosen member's node refused
    /// (0041-T12, issue #238).
    ///
    /// A shared group exists precisely so that one member being unable to take a message
    /// is a re-balance, not a cluster-wide publish refusal. Each candidate is tried at
    /// most once per publish (`tried`), so the pass is bounded; only an EXHAUSTED pass
    /// answers the publisher, with the last refusal it saw — or a withhold when the last
    /// answer claimed nothing.
    pub(super) async fn reselect_shared(
        &mut self,
        id: u64,
        obligation: ForwardObligation,
        last: DurableOutcome,
    ) {
        let ForwardKind::Shared {
            key,
            qos: _,
            mut tried,
            ..
        } = obligation.kind
        else {
            return;
        };
        let mut last = last;
        loop {
            let Some(p) = self.pending_publishes.get(&id) else {
                return; // already resolved (cap eviction, an earlier terminal answer)
            };
            let (topic, payload, qos, message_expiry, app) = (
                p.topic.clone(),
                p.payload.clone(),
                p.qos,
                p.message_expiry,
                p.app.clone(),
            );
            let candidates: Vec<SharedCandidate> = self
                .shared_candidates(&topic)
                .into_iter()
                .find(|(k, _)| *k == key)
                .map(|(_, cs)| cs)
                .unwrap_or_default()
                .into_iter()
                .filter(|c| !tried.iter().any(|(n, cl)| *n == c.node && *cl == c.client))
                .collect();
            let Some(chosen) = self.select_shared(&key, &candidates) else {
                debug!(
                    publish = id, group = %key.0,
                    "shared re-selection exhausted; answering the publisher"
                );
                match last {
                    DurableOutcome::Refused(r) => self.refuse_pending(id, r),
                    _ => self.drop_pending(id),
                }
                return;
            };
            let delivered_qos = min_qos(qos, chosen.qos);
            tried.push((chosen.node.clone(), chosen.client.clone()));
            let Some(node) = chosen.node.clone() else {
                let out = self
                    .deliver_to_client(
                        &chosen.client,
                        &topic,
                        &payload,
                        delivered_qos,
                        message_expiry,
                        &app,
                        false,
                        &AppendGate::Pending(id),
                    )
                    .await;
                match out {
                    // `Ok` = the append is SUBMITTED (issue #242): the obligation is
                    // in `appends_outstanding`, so this releases nothing early.
                    DurableOutcome::Ok => self.try_complete_pending(id),
                    DurableOutcome::Failed => self.drop_pending(id),
                    // This member cannot take it either: keep re-balancing.
                    DurableOutcome::Refused(r) => {
                        last = DurableOutcome::Refused(r);
                        continue;
                    }
                }
                return;
            };
            if qos_num(delivered_qos) >= 1 && self.peer_proto(&node) >= PROTO_FORWARD_VERDICT {
                self.register_forward(
                    id,
                    ForwardObligation {
                        node,
                        kind: ForwardKind::Shared {
                            key,
                            client: chosen.client.clone(),
                            qos: delivered_qos,
                            tried,
                        },
                    },
                );
            } else {
                // Nothing is owed (`QoS` 0), or the link cannot carry an answer: today's
                // fire-and-forget delivery, and the obligation ends here.
                self.send_shared_to_peer(
                    &node,
                    &chosen.client,
                    &topic,
                    &payload,
                    delivered_qos,
                    message_expiry,
                    &app,
                );
                self.try_complete_pending(id);
            }
            return;
        }
    }

    /// The shared groups matching `topic`, each with its global candidate list:
    /// local members (`node` = None) first, then each peer's members in node-id
    /// order, so the round-robin cursor is stable (ADR 0015 §2).
    pub(super) fn shared_candidates(&self, topic: &str) -> Vec<SharedMatch> {
        let mut by_key: BTreeMap<SharedKey, Vec<SharedCandidate>> = BTreeMap::new();
        // Borrow each matching group's members (ADR 0010 T8): clone only what we keep — the
        // key and each candidate — not the whole member list per publish.
        self.shared
            .for_each_matching(topic, |group, filter, members| {
                let entry = by_key
                    .entry((group.to_string(), filter.to_string()))
                    .or_default();
                for (client, qos) in members {
                    let online = self.online.contains_key(client);
                    entry.push(SharedCandidate {
                        node: None,
                        client: client.clone(),
                        qos: *qos,
                        online,
                    });
                }
            });
        for (node, groups) in self.remote_shared.iter().collect::<BTreeMap<_, _>>() {
            for g in groups {
                if !topic_matches(&g.filter, topic) {
                    continue;
                }
                let entry = by_key
                    .entry((g.group.clone(), g.filter.clone()))
                    .or_default();
                for (client, qos, online) in &g.members {
                    entry.push(SharedCandidate {
                        node: Some((*node).clone()),
                        client: client.clone(),
                        qos: *qos,
                        online: *online,
                    });
                }
            }
        }
        by_key.into_iter().collect()
    }

    /// Round-robin one member for a shared group, advancing the per-group cursor.
    /// Prefers a member that can receive now — a **local online** or **any remote**
    /// member — and falls back to a **local persistent** (queued) member (ADR 0015 §4).
    pub(super) fn select_shared(
        &mut self,
        key: &SharedKey,
        candidates: &[SharedCandidate],
    ) -> Option<SharedCandidate> {
        let n = candidates.len();
        if n == 0 {
            return None;
        }
        let start = self.shared_cursor.get(key).copied().unwrap_or(0) % n;
        self.shared_cursor.insert(key.clone(), (start + 1) % n);
        self.choose_shared(candidates, start)
    }

    /// [`select_shared`](Self::select_shared) without advancing the cursor — the PLAN
    /// pass's view of who this publish would land on (issue #238). A publish that is
    /// about to be refused must not consume a group member's turn.
    pub(super) fn peek_shared(
        &self,
        key: &SharedKey,
        candidates: &[SharedCandidate],
    ) -> Option<SharedCandidate> {
        let n = candidates.len();
        if n == 0 {
            return None;
        }
        let start = self.shared_cursor.get(key).copied().unwrap_or(0) % n;
        self.choose_shared(candidates, start)
    }

    /// The selection rule itself, shared by [`select_shared`](Self::select_shared) and
    /// [`peek_shared`](Self::peek_shared) so a peek can never disagree with the
    /// selection it is predicting.
    pub(super) fn choose_shared(
        &self,
        candidates: &[SharedCandidate],
        start: usize,
    ) -> Option<SharedCandidate> {
        let n = candidates.len();
        let rotated = || candidates.iter().cycle().skip(start).take(n);
        // Immediately deliverable: any member online on its home node — local (our
        // `online`) or remote (its home node's gossiped liveness, ADR 0015 T8). Targeting a
        // member offline at home would only queue there while a live member could deliver now.
        let immediate = rotated().find(|c| c.online);
        immediate
            // No one online: a local persistent member queues for replay (ADR 0015 §4)...
            .or_else(|| rotated().find(|c| c.node.is_none() && self.is_persistent(&c.client)))
            // ...else a remote member (it queues at its home) so the message is not dropped.
            .or_else(|| rotated().find(|c| c.node.is_some()))
            .cloned()
    }

    /// Send one message to an online client at its (already downgraded) `QoS`,
    /// registering `QoS` > 0 deliveries in the in-flight table. `message_expiry` is
    /// the MQTT 5.0 Message Expiry Interval to forward (the remaining seconds), if any.
    ///
    /// `offset` is the message's place in the session's durable log when it has one
    /// (#124) — the caller appends *before* calling, so the record already exists when
    /// the packet reaches the wire. It is tracked as owed here, whether the message goes
    /// out now or waits in the flow-control backlog, and released when the subscriber
    /// acknowledges it.
    pub(super) async fn send_to_client(
        &mut self,
        client: &ClientId,
        tx: &Outbound,
        message: &Message,
        retain: bool,
        message_expiry: Option<u32>,
        offset: Option<Offset>,
    ) {
        let limits = self.subscriber_limits;
        // `QoS` 0 owes no acknowledgement, so a replayed one is settled the moment it is
        // handed to the channel — only `QoS` > 0 becomes owed.
        if let Some(offset) = offset.filter(|_| message.qos != QoS::AtMostOnce) {
            self.inflight
                .entry(client.clone())
                .or_default()
                .track(offset);
        }
        if message.qos == QoS::AtMostOnce {
            // QoS 0 is the only path with no other bound: the QoS 1/2 backlog is
            // capped in messages and bytes below, and Receive Maximum does not apply
            // to QoS 0 — so without this a subscriber that stopped reading a busy
            // topic grew its outbound channel without limit (#123).
            //
            // At-most-once is exactly the delivery contract that permits dropping,
            // so shedding here is legal where dropping a QoS 1/2 message or an ack
            // would not be. Counted and logged: a silent drop would be the same
            // defect in a different place.
            //
            // Two dimensions since issue #241: the fixed packet count, and the
            // operator's byte bound — because 10 000 packets at the 1 MiB default
            // packet ceiling is ~10 GiB, i.e. a count is not a memory budget. The
            // gate covers ONLY this shed-legal class; control packets and QoS 1/2
            // still flow past a full channel, exactly as before.
            let over_bytes = limits
                .max_outbound_bytes
                .is_some_and(|c| tx.bytes() + message_bytes(message) > c);
            if tx.depth() >= MAX_OUTBOUND_QUEUE || over_bytes {
                if let Some(m) = &self.metrics {
                    m.publish_dropped("outbound-full");
                }
                warn!(
                    client = %client.0,
                    bound = if over_bytes { "bytes" } else { "packets" },
                    cap_packets = MAX_OUTBOUND_QUEUE,
                    cap_bytes = ?limits.max_outbound_bytes,
                    queued_bytes = tx.bytes(),
                    topic = %message.topic,
                    "outbound queue full: shedding QoS 0 for a subscriber that is not reading"
                );
                return;
            }
            // Ignore send errors: a closed channel means the client is gone and a
            // Detach is already in flight.
            let _ = tx.send(publish_packet(
                &message.topic,
                message.payload.clone(),
                QoS::AtMostOnce,
                None,
                false,
                retain,
                message_expiry,
                &message.app,
            ));
            return;
        }

        // QoS > 0: respect the client's Receive Maximum (ADR 0012). If the quota is
        // full, hold the message until a PUBACK/PUBCOMP drains it; otherwise send now.
        //
        // A non-empty backlog ALSO diverts the message, even with quota free. Before
        // ADR 0057 that case was unreachable (acks drain the backlog before the loop
        // processes another publish, so backlog non-empty implied quota full); a
        // deferred `QoS` 2 delivery (outbound-id write failed, requeued at the front)
        // broke that invariant, and sending fresh traffic directly would let it OVERTAKE
        // the deferred message — per-client ordering is part of the contract.
        // `records_pending > 0` ALSO diverts (issue #242 finding A): a delivery is
        // staged behind its off-loop outbound-id record, and sending fresh traffic
        // directly would overtake it — the backlog is the per-client ordering
        // buffer, and the record's completion drains it.
        let inf = self.inflight.entry(client.clone()).or_default();
        let must_queue = inf.quota_full() || !inf.backlog.is_empty() || inf.records_pending > 0;
        if must_queue {
            // The backlog is bounded in messages AND bytes (ADR 0012, issue #241);
            // drop-oldest at either bound so a stalled consumer cannot force unbounded
            // memory. The byte bound may evict several entries for one arrival.
            let evicted = inf.push_backlog(
                BacklogEntry::new(message.clone(), retain, message_expiry, offset),
                &limits,
            );
            if !evicted.is_empty() {
                let bytes = inf.backlog.bytes();
                for (e, _) in &evicted {
                    if let Some(offset) = e.offset {
                        inf.release(offset);
                    }
                    if let Some(m) = &self.metrics {
                        m.publish_dropped("backlog-overflow");
                    }
                }
                warn_backlog_eviction(client, &evicted, bytes, &limits);
                // Nothing will deliver the evicted messages, so their offsets no longer
                // hold the truncation point back.
                self.truncate_acked(client).await;
            }
            // Quota free but the backlog holds a deferred delivery: retry the drain now,
            // in order — traffic is the retry clock, so a store that recovered gets the
            // deferred message out on the very next publish rather than at reconnect.
            let quota_free = self
                .inflight
                .get(client)
                .is_some_and(|inf| !inf.quota_full());
            if quota_free {
                self.drain_backlog(client);
            }
        } else {
            let _ = self.send_qos_publish(client, tx, message, retain, message_expiry, offset);
        }
    }

    /// Put one `QoS` > 0 message on the wire: allocate a packet id, register it in the
    /// in-flight table, and send. The caller has already confirmed quota is available
    /// (ADR 0012).
    /// Allocate an outbound packet id for `client` (1..=65535, never 0, skipping ids still
    /// in flight). Ids come from a durably-reserved block (ADR 0007 T9): when the block is
    /// spent, the next block's reservation — one store write advancing the persisted
    /// high-water, so a takeover resumes past it — runs OFF-loop (issue #242 finding A,
    /// reserve-at-spent): `None` means "spent, nothing banked yet"; the caller defers the
    /// delivery to the backlog front and submits the single-flight reserve via
    /// [`defer_for_pkid_block`](Self::defer_for_pkid_block). An id therefore reaches the
    /// wire only under a durably reserved high-water. A reservation failure (or a
    /// non-durable / clean session, base 0) banks a bare refill: the free-running
    /// in-memory counter, exactly as before.
    pub(super) fn alloc_pkid(&mut self, client: &ClientId) -> Option<u16> {
        loop {
            let inf = self.inflight.entry(client.clone()).or_default();
            if inf.block_remaining == 0 {
                // Spent: adopt the banked reservation, or tell the caller to defer.
                let base = inf.banked_base.take()?;
                if base != 0 {
                    inf.next_pkid = base;
                }
                inf.block_remaining = PKID_BLOCK;
            }
            inf.next_pkid = inf.next_pkid.wrapping_add(1);
            if inf.next_pkid == 0 {
                inf.next_pkid = 1; // packet id 0 is invalid
            }
            inf.block_remaining = inf.block_remaining.saturating_sub(1);
            let id = inf.next_pkid;
            if !inf.pending.contains_key(&id) {
                return Some(id);
            }
        }
    }

    /// See [`QosSend`] for what each return means to the caller's drain loop. This is
    /// the single choke point every `QoS` > 0 wire send passes through — the
    /// completion-handler live send, the ack-triggered drain, and the attach replay —
    /// so the staging below covers all three.
    pub(super) fn send_qos_publish(
        &mut self,
        client: &ClientId,
        tx: &Outbound,
        message: &Message,
        retain: bool,
        message_expiry: Option<u32>,
        offset: Option<Offset>,
    ) -> QosSend {
        // A `QoS` 0 parked in the backlog purely for wire order (issue #242
        // finding A — reachable only from the drain): no packet id, no pending
        // entry, no quota — it goes straight out in its FIFO slot.
        if message.qos == QoS::AtMostOnce {
            let _ = tx.send(publish_packet(
                &message.topic,
                message.payload.clone(),
                QoS::AtMostOnce,
                None,
                false,
                retain,
                message_expiry,
                &message.app,
            ));
            if let Some(m) = &self.metrics {
                m.publish_delivered(0);
            }
            return QosSend::Sent;
        }
        let Some(pkid) = self.alloc_pkid(client) else {
            // The durable packet-id block is spent (ADR 0007 T9): defer to the
            // backlog front and reserve the next block OFF-loop (issue #242
            // finding A) — the loop must not park for the reservation's quorum
            // write, and the message waits in the backlog exactly as long as it
            // used to wait in the inline await.
            self.defer_for_pkid_block(
                client,
                BacklogEntry::new(message.clone(), retain, message_expiry, offset),
            );
            return QosSend::Deferred;
        };
        // ADR 0057: a `QoS` 2 delivery backed by a durable offset records its packet id
        // BEFORE the packet reaches the wire — the same ordering as #124, because an id
        // recorded after the send is an id a crash can orphan, and an orphaned id is how
        // exactly-once quietly becomes at-least-once across a restart. Since issue #242
        // the record itself is a SECOND LANE STAGE: staged here, written off-loop,
        // sent only by its completion. `QoS` 1 is not recorded (a fresh-id DUP
        // redelivery is what at-least-once means); a delivery with no offset has no
        // durable message to resume, so an id would be pointless.
        if message.qos == QoS::ExactlyOnce {
            if let Some(off) = offset {
                return self.stage_outbound_record(
                    client,
                    message,
                    retain,
                    message_expiry,
                    pkid,
                    off,
                );
            }
        }
        let inf = self.inflight.entry(client.clone()).or_default();
        let state = if message.qos == QoS::AtLeastOnce {
            OutState::AwaitingPubAck
        } else {
            OutState::AwaitingPubRec
        };
        inf.pending.insert(
            pkid,
            PendingOut {
                message: message.clone(),
                state,
                offset,
            },
        );
        let _ = tx.send(publish_packet(
            &message.topic,
            message.payload.clone(),
            message.qos,
            Some(pkid),
            false,
            retain,
            message_expiry,
            &message.app,
        ));
        QosSend::Sent
    }

    /// Stage ADR 0057's outbound-id record in the session's lane (issue #242
    /// finding A): the pending entry parks in [`OutState::AwaitingIdRecord`] —
    /// reserving Receive-Maximum quota and pinning the pkid against reuse —
    /// `records_pending` diverts every later `QoS` > 0 send into the backlog, and
    /// the lane worker performs the store write off-loop. This function has NO
    /// send: only [`outbound_record_done`](Self::outbound_record_done), holding
    /// the store's own Ok behind the conn fence, puts the packet on the wire
    /// (ack-after-durable, #124/ADR 0057, made structural).
    pub(super) fn stage_outbound_record(
        &mut self,
        client: &ClientId,
        message: &Message,
        retain: bool,
        message_expiry: Option<u32>,
        pkid: u16,
        off: Offset,
    ) -> QosSend {
        let planned_conn = self.online.get(client).map(|o| o.conn_id);
        let inf = self.inflight.entry(client.clone()).or_default();
        inf.pending.insert(
            pkid,
            PendingOut {
                message: message.clone(),
                state: OutState::AwaitingIdRecord,
                offset: Some(off),
            },
        );
        inf.records_pending += 1;
        let job = AppendJob {
            client: client.clone(),
            message: message.clone(),
            work: LaneWork::RecordOutbound { pkid, offset: off },
            then: AppendThen::Ungated,
            planned_conn,
            retain,
            message_expiry,
        };
        match self.submit_lane_job(job) {
            Submitted::Queued => QosSend::Staged,
            // The lane is at cap: fail closed exactly like a failed id write —
            // the message is already durable at `off`, nothing is lost; back to
            // the FRONT (ordering holds) and the next drain retries.
            Submitted::Refused(_) | Submitted::Full => {
                let inf = self.inflight.entry(client.clone()).or_default();
                inf.pending.remove(&pkid);
                inf.records_pending = inf.records_pending.saturating_sub(1);
                inf.backlog.push_front_admitted(BacklogEntry::new(
                    message.clone(),
                    retain,
                    message_expiry,
                    Some(off),
                ));
                QosSend::Deferred
            }
        }
    }

    /// Drain backlogged `QoS` > 0 messages onto the wire while the client is online and
    /// quota is available (ADR 0012). Called after a PUBACK/PUBCOMP frees a slot, and
    /// by the two off-loop completions that re-open their gates (issue #242 finding A:
    /// an outbound-id record landing, a packet-id block arriving).
    pub(super) fn drain_backlog(&mut self, client: &ClientId) {
        let Some(tx) = self.online.get(client).map(|s| s.tx.clone()) else {
            return;
        };
        loop {
            let inf = self.inflight.entry(client.clone()).or_default();
            // A staged outbound-id record halts the drain (issue #242 finding A):
            // its delivery owns the wire next; the record's completion sends it
            // and re-enters here.
            if inf.quota_full() || inf.records_pending > 0 {
                break;
            }
            let Some(entry) = inf.backlog.pop_front() else {
                break;
            };
            match self.send_qos_publish(
                client,
                &tx,
                &entry.message,
                entry.retain,
                entry.message_expiry,
                entry.offset,
            ) {
                QosSend::Sent => {}
                // Staged: the entry is consumed, but nothing further may pass the
                // staged record. Deferred: the entry went back to the front;
                // retrying in this same loop would spin against a store that just
                // refused.
                QosSend::Staged | QosSend::Deferred => break,
            }
        }
    }

    /// Spill a persistent session's never-sent backlog (`QoS` > 0 messages held for
    /// quota, ADR 0012) into the durable offline queue so they replay on reconnect
    /// rather than being lost when the connection ends. Already-sent in-flight
    /// entries keep their DUP-redelivery behaviour and are left untouched.
    ///
    /// An entry that already carries a log offset (#124) is dropped from memory instead
    /// of spilled: it is in the log already and stays *owed* there, so the truncation
    /// point cannot pass it and the reconnect replays it. Re-enqueuing it would put a
    /// second copy in the log. What still spills is the case the offset does not cover —
    /// a message the store deliberately did not record (the queue cap under
    /// `reject-newest`).
    ///
    /// The spill rides the session's append LANE as one [`LaneWork::Spill`] job
    /// (issue #242 finding C): the store writes run OFF-loop — a direct enqueue
    /// here parked the loop for up to `MAX_BACKLOG` quorum writes — and the lane
    /// FIFO puts them strictly BEHIND every in-flight append, which is exactly the
    /// pre-motion order (the inline spill followed all previously admitted
    /// appends; racing them inverts replay order).
    pub(super) fn flush_backlog_to_store(&mut self, client: &ClientId) {
        let entries: Vec<(Message, Option<u64>)> = match self.inflight.get_mut(client) {
            Some(inf) if !inf.backlog.is_empty() => {
                let now = self.clock.now_epoch_secs();
                inf.backlog
                    .drain_all()
                    .into_iter()
                    // At-most-once owes no redelivery: a `QoS` 0 parked here for
                    // wire order (issue #242 finding A) dies with the connection,
                    // exactly like one sitting in the closed conn channel.
                    .filter(|e| e.offset.is_none() && e.message.qos != QoS::AtMostOnce)
                    .map(|e| {
                        let expiry_at = e.message_expiry.map(|s| now + u64::from(s));
                        (e.message, expiry_at)
                    })
                    .collect()
            }
            _ => return,
        };
        if entries.is_empty() {
            return;
        }
        let count = entries.len();
        let job = AppendJob::control(client.clone(), LaneWork::Spill { entries });
        // A control job: admitted into the LANE_CONTROL_HEADROOM slots even at the
        // delivery cap, so a saturated lane still serializes the spill behind its
        // own backlog.
        let lane = self.lane_for(client);
        if lane.tx.try_send(LaneJob::Deliver(Box::new(job))).is_ok() {
            lane.outstanding += 1;
            return;
        }
        // Beyond cap + headroom: shed, loudly. These entries are queue-cap
        // reject-newest survivors — a shed-accepting policy — and parking
        // the loop to save them is the defect this path used to be.
        warn!(client = %client.0, count,
              "append lane full at detach spill; shedding the spilled backlog");
        if let Some(m) = &self.metrics {
            for _ in 0..count {
                m.publish_dropped("append-backlog-full");
            }
        }
    }
}
