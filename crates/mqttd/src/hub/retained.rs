//! Retained-message authority, convergence, and hand-off — the ADR 0037 machinery
//! (issue #258 slice 1: moved verbatim from `hub/mod.rs`, no logic edits).
//!
//! **The invariant this module owns:** a retained value is a single cluster-wide
//! fact per topic. Every mutation is routed to the topic's durable AUTHORITY and
//! committed with a monotone `(epoch, offset)` token; every replica applies a
//! value only above the token it holds, tombstones discharge deletions across
//! restarts, and the digest/snapshot exchange converges any replica that missed
//! frames — so two nodes can disagree about a retained value only transiently,
//! and never silently. The delivery-side `RetainedWindow` keeps subscribe-time
//! replay exact while convergence is in flight.
//!
//! `Hub`'s retained-related FIELDS stay in `hub/mod.rs` (the struct is one item);
//! this module holds the types, helpers, and the `impl Hub` methods that are the
//! only writers of those fields.

#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258): the five
// siblings share one type/state vocabulary by design, and enumerating it would
// re-couple every future hub change to six import lists. Scoped to these files.
use super::*;

/// One client's fresh-subscription retained-delivery window (issue #219).
#[derive(Debug)]
pub(super) struct RetainedWindow {
    /// When the window closes (pruned by the sweep). The interest-forward path is
    /// authoritative from then on.
    pub(super) until: Instant,
    /// Per topic, the [`retained_value_id`] of the value this client last saw while
    /// the window was open — seeded by the subscribe-time replay, updated by live
    /// deliveries and by the apply path itself. Consulted ONLY by the apply path:
    /// the live path records here but is never suppressed, so a racy double stays
    /// within `QoS` 1's at-least-once while a wrongly swallowed live publish cannot
    /// happen.
    pub(super) seen: HashMap<String, u64>,
}

/// A retained mutation awaiting its authority commit (ADR 0037 §5/T8).
#[derive(Debug, Clone)]
pub(super) struct RetainedMutation {
    /// Destination topic.
    pub(super) topic: String,
    /// The retained payload; empty = clear (versioned tombstone).
    pub(super) payload: Bytes,
    /// The publish `QoS` as its 2-bit wire value.
    pub(super) qos: u8,
    /// The publisher's forwardable application properties (ADR 0038 T3), committed
    /// into the durable record with the value.
    pub(super) app: AppProperties,
    /// Set when a peer routed this mutation here (T8): the `(node, seq)` its
    /// commit-gated ack goes back to.
    pub(super) reply: Option<(NodeId, u64)>,
    /// The pending publish whose acknowledgement is gated on this mutation's
    /// authority commit (ADR 0042 T9, exhibit ⑦). Survives re-queues and rides
    /// the handoff hold, so the gate holds however long the commit takes.
    pub(super) publish: Option<u64>,
    /// Absolute expiry deadline (Unix epoch seconds; issue #227), committed with
    /// the value. `None` = never.
    pub(super) expires_at: Option<u64>,
    /// Set when this mutation came from a RESTORE (ADR 0062), not from a publish.
    ///
    /// It suppresses the one delivery an ordinary commit still makes — the
    /// window-scoped back-fill to a subscription younger than the interest horizon
    /// (issue #219). During a restore that delivery has no legitimate recipient: no
    /// client listener is bound, and a session's own export already accounts for every
    /// message it was owed, so anything delivered here would be a message the backup
    /// did not contain. Retained state is written; nothing is delivered.
    pub(super) restore: bool,
}

/// The order-independent digest of a retained set (0014-T6 + ADR 0037 P1): the topic
/// count, the XOR of each topic's stable 64-bit hash, and the XOR of each
/// `(topic, payload, qos)` **value** hash. Independent of iteration order and cheap to
/// compare (a collision merely skips a best-effort back-fill / detection). Equal topic
/// hashes with **differing value hashes** mean divergence: same topics, different values.
pub(super) fn retained_digest<'a>(
    entries: impl Iterator<Item = (&'a str, &'a [u8], u8, Vec<u8>)>,
) -> (u64, u64, u64) {
    let mut count = 0u64;
    let mut hash = 0u64;
    let mut value_hash = 0u64;
    for (topic, payload, qos, props) in entries {
        count += 1;
        hash ^= mqtt_cluster::hrw::stable_id(topic.as_bytes());
        value_hash ^= retained_value_id(topic, payload, qos, &props);
    }
    (count, hash, value_hash)
}

/// A stable 64-bit hash of one retained `(topic, payload, qos, props)` value
/// (ADR 0037 P1). The topic is length-prefixed so `("a", "bc")` and `("ab", "c")`
/// cannot collide; the canonical props encoding (ADR 0038 T3) is folded in so two
/// caches holding the same payload with different application properties still read
/// as divergent and reconcile by token.
pub(super) fn retained_value_id(topic: &str, payload: &[u8], qos: u8, props: &[u8]) -> u64 {
    let mut bytes = Vec::with_capacity(8 + topic.len() + payload.len() + 1 + props.len());
    bytes.extend_from_slice(&(topic.len() as u64).to_be_bytes());
    bytes.extend_from_slice(topic.as_bytes());
    bytes.extend_from_slice(payload);
    bytes.push(qos);
    bytes.extend_from_slice(props);
    mqtt_cluster::hrw::stable_id(&bytes)
}

/// Split retained entries into chunks whose summed (topic + payload) size stays under
/// [`RETAINED_CHUNK_BYTES`] (0014-T8). A single entry larger than the whole budget is
/// skipped with a warning — it could never fit a frame, and sending it would sever the
/// link instead of just missing one back-fill.
pub(super) fn chunk_retained(
    entries: impl Iterator<Item = RetainedWireEntry>,
) -> Vec<Vec<RetainedWireEntry>> {
    // Fixed per-entry overhead estimate for codec framing, the QoS byte, and the
    // two u64 token halves (ADR 0037 P5); the variable-length application
    // properties (ADR 0038 T3) are sized per entry. Calibrated to the old
    // fixed-width codec; postcard's varints are never wider (ADR 0052), so the
    // estimate is conservative — the safe direction for a chunk budget.
    const ENTRY_OVERHEAD: usize = 48;
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for entry in entries {
        let size =
            entry.topic.len() + entry.payload.len() + entry.props.size_hint() + ENTRY_OVERHEAD;
        if size > RETAINED_CHUNK_BYTES {
            warn!(
                topic = %entry.topic,
                bytes = size,
                "retained message exceeds the snapshot chunk budget; skipping back-fill for it"
            );
            continue;
        }
        if current_bytes + size > RETAINED_CHUNK_BYTES && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += size;
        current.push(entry);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

impl Hub {
    /// Whether storing a retained value for `topic` would GROW the retained set
    /// beyond the quota (ADR 0041 T4). Overwrites (topic already retained) never
    /// count. Enforced against this node's local retained view.
    pub(super) async fn retained_quota_exceeded(&self, topic: &str) -> bool {
        let over = if self.brownout {
            true // brownout (ADR 0041 T5): any retained GROWTH is refused
        } else if let Some(cap) = self.quotas.max_retained_messages {
            self.retained.count().await.unwrap_or(0) >= cap
        } else {
            false
        };
        if !over {
            return false;
        }
        // At the bound: only an overwrite of an existing topic may proceed.
        !self
            .retained
            .matching(topic)
            .await
            .is_ok_and(|m| m.iter().any(|r| r.topic == topic))
    }

    /// The retained authority commit obligation resolved (ADR 0042 T9, exhibit ⑦).
    pub(super) fn pending_retained_done(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(&id) {
            p.awaiting_retained = false;
        }
        self.try_complete_pending(id);
    }

    /// Offer `node` our retained topic-set digest (ADR 0014 §3, 0014-T6): the peer
    /// pulls the snapshot only if its own digest differs, so a link-up (or flap)
    /// between already-synced nodes transfers one small frame instead of the whole
    /// set. A no-op when we have no retained messages or the peer link is gone.
    pub(super) async fn send_retained_digest(&self, node: &NodeId) {
        let Some(peer) = self.peers.get(node) else {
            return;
        };
        let Some((count, hash, value_hash)) = self.local_retained_digest().await else {
            return;
        };
        let _ = peer.tx.send(PeerMessage::RetainedDigest {
            count,
            hash,
            value_hash,
        });
    }

    /// This node's retained digest, or `None` when there is nothing a peer could
    /// learn from us.
    ///
    /// With no values AND no tombstone tokens we stay silent. A tombstone-only state
    /// still offers its digest: a peer holding a value for a topic we committed a
    /// clear for must see a difference and pull the tombstone (ADR 0037 P5) — going
    /// silent would strand its stale value. That held only within one process life
    /// until issue #183: the tokens are in-memory, so a restarted tombstone-only
    /// node went silent again — `warm_retained_tokens_from_authority` re-arms them
    /// from the keyspace before the digest is offered.
    pub(super) async fn local_retained_digest(&self) -> Option<(u64, u64, u64)> {
        let retained = self.retained.all().await.ok()?;
        if retained.is_empty() && self.retained_tokens.is_empty() {
            return None;
        }
        Some(retained_digest(retained.iter().map(|m| {
            (
                m.topic.as_str(),
                m.payload.as_ref(),
                m.qos as u8,
                AppProps::from(&m.app).encode(),
            )
        })))
    }

    /// Offer EVERY peer our retained digest (issue #87): the periodic anti-entropy
    /// that turns a missed fan-out frame into a self-healing gap rather than a
    /// permanent divergence.
    ///
    /// The digest is computed once and fanned out, so the cost is one retained scan
    /// per period regardless of peer count — the same scan a link-up already pays.
    /// Peers already in sync compare equal and transfer nothing back.
    pub(super) async fn broadcast_retained_digest(&self) {
        if self.peers.is_empty() {
            return;
        }
        let Some((count, hash, value_hash)) = self.local_retained_digest().await else {
            return;
        };
        for peer in self.peers.values() {
            let _ = peer.tx.send(PeerMessage::RetainedDigest {
                count,
                hash,
                value_hash,
            });
        }
    }

    /// Compare a peer's retained digest against our own (0014-T6 + ADR 0037 P1). Equal
    /// topic *and* value hashes mean the sets are identical — nothing to back-fill,
    /// nothing diverging, nothing transferred. Any difference: pull the peer's (chunked)
    /// snapshot — to gap-fill missing topics and to detect (count, warn) divergent
    /// values on topics both sides hold.
    pub(super) async fn handle_retained_digest(
        &mut self,
        node: &NodeId,
        count: u64,
        hash: u64,
        value_hash: u64,
    ) {
        let Some(peer) = self.peers.get(node) else {
            return;
        };
        let Ok(retained) = self.retained.all().await else {
            return;
        };
        let ours = retained_digest(retained.iter().map(|m| {
            (
                m.topic.as_str(),
                m.payload.as_ref(),
                m.qos as u8,
                AppProps::from(&m.app).encode(),
            )
        }));
        if ours == (count, hash, value_hash) {
            // The pair has observably converged: the instant the tombstone reap
            // gates on (issue #229).
            self.retained_digest_matched_at
                .insert(node.clone(), self.clock.now_epoch_secs());
            debug!(node = %node.0, topics = count, "retained sets already match; skipping back-fill");
            return;
        }
        let _ = peer.tx.send(PeerMessage::RetainedRequest);
    }

    /// Send our full retained set to `node` so it can back-fill any retained
    /// messages published before it joined (ADR 0014 §3), split into bounded
    /// chunks (0014-T8) so no frame can approach the peer frame limit — one
    /// oversized frame would kill the link on the receiving side, and the link-up
    /// back-fill would then re-kill it on every reconnect. Chunks are independent
    /// under the receiver's gap-fill rule, so no ordering or completion marker is
    /// needed. A no-op when we have no retained messages or the peer link is gone.
    pub(super) async fn send_retained_snapshot(&mut self, node: &NodeId) {
        if !self.peers.contains_key(node) {
            return;
        }
        // Snapshot entries for TOMBSTONED topics are built from `retained_tokens`
        // alone — a cleared topic has no cache entry to rediscover the fence from —
        // so re-learn committed state a restart forgot before building the export
        // (issue #183). Values are additionally covered per-topic below.
        self.warm_retained_tokens_from_authority().await;
        let Some(peer) = self.peers.get(node) else {
            return;
        };
        let peer_tx = peer.tx.clone(); // the token repair below needs `&mut self`
        let Ok(retained) = self.retained.all().await else {
            return;
        };
        // Cached values carry their commit token (ADR 0037 P5); `(0, 0)` marks an
        // uncommitted (durable-off / pre-migration) value, which the receiver only
        // ever gap-fills with.
        //
        // `retained_tokens` is in-memory and empty after a restart, but the cache is
        // persistent — so a restarted node knows committed VALUES without their
        // tokens. It must not export them untokened: a peer that applied an older
        // fan-out still holds that older token and fences an untokened repair out as
        // stale — permanently, re-detected and re-refused on every anti-entropy round
        // (issue #214: the acked-facts proc tier caught exactly this, twice). For a
        // cache topic missing its token, re-read the AUTHORITY — the durable keyspace,
        // readable on the topic's group owner — and export the committed record under
        // its committed token, re-adopting it locally first: the record may also be
        // NEWER than the reopened cache (a crash between the commit and the owner's
        // own cache apply), and re-adopting keeps cache, fence and export agreeing.
        let mut entries: Vec<RetainedWireEntry> = Vec::new();
        for m in retained {
            let held = self.retained_tokens.get(&m.topic).copied();
            let entry = match held {
                Some((epoch, offset)) => RetainedWireEntry {
                    props: AppProps::from(&m.app),
                    topic: m.topic,
                    payload: m.payload.to_vec(),
                    qos: m.qos as u8,
                    epoch,
                    offset,
                    expires_at: m.expires_at,
                },
                None => match self.durable_retained_authority(&m.topic).await {
                    Some(authority) => {
                        // The same idempotent apply as a fan-out: store first,
                        // token after (an empty payload is the committed clear).
                        let payload = Bytes::from(authority.payload.clone());
                        let app = AppProperties::from(authority.props.clone());
                        self.apply_retained_update(
                            &m.topic,
                            &payload,
                            authority.qos,
                            &app,
                            authority.token(),
                            authority.expires_at,
                        )
                        .await;
                        RetainedWireEntry {
                            props: authority.props,
                            topic: m.topic,
                            payload: authority.payload,
                            qos: authority.qos,
                            epoch: authority.epoch,
                            offset: authority.offset,
                            expires_at: authority.expires_at,
                        }
                    }
                    // Never durably committed, or the authority is not readable
                    // from this node: the uncommitted-value contract, as before.
                    None => RetainedWireEntry {
                        props: AppProps::from(&m.app),
                        topic: m.topic,
                        payload: m.payload.to_vec(),
                        qos: m.qos as u8,
                        epoch: 0,
                        offset: 0,
                        expires_at: m.expires_at,
                    },
                },
            };
            entries.push(entry);
        }
        // Committed clears back-fill too: a token held for a topic no longer cached
        // is a tombstone, sent as an empty-payload entry so a peer that missed the
        // clear drops the topic instead of keeping it forever (ADR 0037 P5).
        let cached: HashSet<&str> = entries.iter().map(|e| e.topic.as_str()).collect();
        let tombstones: Vec<RetainedWireEntry> = self
            .retained_tokens
            .iter()
            .filter(|(topic, _)| !cached.contains(topic.as_str()))
            .map(|(topic, (epoch, offset))| RetainedWireEntry {
                topic: topic.clone(),
                epoch: *epoch,
                offset: *offset,
                ..Default::default()
            })
            .collect();
        entries.extend(tombstones);
        if entries.is_empty() {
            return;
        }
        for messages in chunk_retained(entries.into_iter()) {
            let _ = peer_tx.send(PeerMessage::RetainedSnapshot { messages });
        }
    }

    /// Apply a peer's retained snapshot.
    ///
    /// Under **durable retained** (ADR 0037 P5) each entry applies only when its
    /// `(epoch, offset)` token beats what we hold for the topic — the same monotonic
    /// rule as the commit fan-out, so divergent caches converge deterministically to
    /// the committed value on link-up. An empty payload is a committed clear
    /// (tombstone): it drops the topic and its token fences staler values. An
    /// **untokened** entry (`(0, 0)`, from an uncommitted cache) only gap-fills an
    /// absent topic — it never overwrites anything. That rule is ENFORCED here, not
    /// just stated: applying an untokened entry over a held value let two nodes with
    /// different uncommitted values swap them on every anti-entropy round, forever.
    ///
    /// When an entry is refused as stale against the durable AUTHORITY (readable only
    /// where the topic's group is owned), the refusal doubles as a repair trigger: a
    /// fresh process whose fence is ahead of its reopened cache re-adopts the
    /// authority's record, so the next digest exports the committed value instead of
    /// re-detecting the same divergence (issue #214).
    ///
    /// **Durable off** keeps the ADR 0014 §3 gap-fill rule verbatim: set a topic only
    /// if we do not already retain it, never clobbering our own value.
    ///
    /// Divergence detection (ADR 0037 P1) runs in both modes: a topic both sides hold
    /// differently is counted (`retained_divergence_total`) and surfaced with one
    /// `warn!` per snapshot chunk — under durable the same pass also resolves it in
    /// whichever direction the tokens order, and the warn reports what actually
    /// happened (applied vs kept), never a blanket "converged": claiming convergence
    /// while every repair was being refused is how issue #214 stayed invisible.
    pub(super) async fn apply_retained_snapshot(
        &mut self,
        node: &NodeId,
        messages: Vec<RetainedWireEntry>,
    ) {
        let have: HashMap<String, u64> = match self.retained.all().await {
            Ok(all) => all
                .into_iter()
                .map(|m| {
                    let id = retained_value_id(
                        &m.topic,
                        m.payload.as_ref(),
                        m.qos as u8,
                        &AppProps::from(&m.app).encode(),
                    );
                    (m.topic, id)
                })
                .collect(),
            Err(_) => return,
        };
        let durable = self.durable_retained.is_some();
        let (mut filled, mut diverged, mut applied_diverged, mut kept) = (0u64, 0u64, 0u64, 0u64);
        for entry in messages {
            let RetainedWireEntry {
                topic,
                payload,
                qos,
                epoch,
                offset,
                props,
                expires_at,
            } = entry;
            let payload = Bytes::from(payload);
            let held_value = have.get(&topic).copied();
            // Detection (P1): both sides hold the topic, with different values (an
            // incoming committed clear against our value counts too; differing
            // application properties on an equal payload count as well — ADR 0038 T3).
            let value_differs = held_value.is_some_and(|ours| {
                ours != retained_value_id(&topic, payload.as_ref(), qos, &props.encode())
            });
            if value_differs {
                diverged += 1;
                debug!(node = %node.0, %topic, ?epoch, ?offset, "retained value diverges from peer");
                if let Some(m) = &self.metrics {
                    m.retained_divergence();
                }
            }
            // Set only when the durable token rule says this value applies; consumed
            // after the store accepts it.
            let mut pending_token: Option<(u64, u64)> = None;
            if durable {
                let token = (epoch, offset);
                if !self
                    .snapshot_entry_applies(&topic, token, held_value.is_some(), payload.is_empty())
                    .await
                {
                    if value_differs {
                        kept += 1;
                    }
                    continue;
                }
                // Deliberately NOT recorded here — see below. The token is written only
                // after the store accepts the value, for the reason in
                // `apply_retained_update`: a fence with no value behind it is
                // unrepairable, including by this very path.
                pending_token = Some(token);
            } else if held_value.is_some() || payload.is_empty() {
                // Gap-fill only (ADR 0014 §3); a tombstone entry has nothing to fill.
                continue;
            }
            // An empty payload clears the topic [MQTT-3.3.1-10]. Application
            // properties back-fill with the value (ADR 0038 T3), so a replay from a
            // back-filled cache matches one from the origin node.
            let message = Message {
                topic,
                payload,
                qos: QoS::from_u8(qos).unwrap_or(QoS::AtMostOnce),
                retain: true,
                app: props.into(),
                expires_at,
            };
            let topic_key = message.topic.clone();
            self.retained_may_expire |= message.expires_at.is_some();
            let tombstone = message.payload.is_empty();
            if self.retained.set(&message).await.is_ok() {
                if let Some(token) = pending_token {
                    self.retained_tokens.insert(topic_key.clone(), token);
                    // Issue #229: same bookkeeping as the fan-out apply.
                    if tombstone {
                        self.retained_tombstone_observed_at
                            .entry(topic_key)
                            .or_insert_with(|| self.clock.now_epoch_secs());
                    } else {
                        self.retained_tombstone_observed_at.remove(&topic_key);
                    }
                }
                filled += 1;
                if value_differs {
                    applied_diverged += 1;
                }
            } else if let Some(m) = &self.metrics {
                m.retained_apply_failed();
            }
        }
        if filled > 0 {
            debug!(filled, "back-filled retained messages from a peer snapshot");
        }
        Self::warn_on_divergence(node, durable, diverged, applied_diverged, kept);
    }

    /// One warn per snapshot chunk, not per topic — the per-topic detail is at debug
    /// and the count is on the metric (bounded logging, ADR 0003-T6 style). The counts
    /// must say what actually happened: this line used to claim "converged to the
    /// higher-token committed value" unconditionally, while every repair in the chunk
    /// was being refused as stale — which is how a permanent divergence wore
    /// convergence's clothes for two CI runs (#214). A diverged entry whose store
    /// write failed appears in neither count; it has its own warn and
    /// `retained_apply_failed_total`.
    pub(super) fn warn_on_divergence(
        node: &NodeId,
        durable: bool,
        diverged: u64,
        applied: u64,
        kept: u64,
    ) {
        if diverged == 0 {
            return;
        }
        if durable {
            warn!(
                node = %node.0,
                topics = diverged,
                applied,
                kept,
                "retained values DIVERGED from peer (same topic, different value) — \
                 applied the incoming value where its committed token was strictly \
                 newer; kept ours where the incoming entry was stale or untokened \
                 (ADR 0037 P5)"
            );
        } else {
            warn!(
                node = %node.0,
                topics = diverged,
                "retained values DIVERGE from peer (same topic, different value) — \
                 best-effort replication kept each side's own value (ADR 0037 P1 detection)"
            );
        }
    }

    /// Whether an incoming snapshot entry for `topic` applies under durable retained
    /// — the ADR 0037 P5 token rule, decided against the strongest fence available:
    ///
    /// * **untokened** (`(0, 0)`, an uncommitted / durable-off cache): gap-fill an
    ///   ABSENT topic only — never overwrite, a tombstone has nothing to fill — and
    ///   only where no committed record fences the absence (a committed clear stays
    ///   cleared, issue #87 item 4);
    /// * a token this process **already applied** must be strictly beaten — an equal
    ///   one is a duplicate;
    /// * against the durable AUTHORITY (no in-memory token — a fresh process) an
    ///   EQUAL token is this very commit — the owner applying its own write, or
    ///   repairing one whose store write failed — so only strictly older is stale
    ///   (issue #87 item 4). A stale entry also means the reopened cache may predate
    ///   the committed record (the crash landed between the commit and the owner's
    ///   own cache apply): the refusal re-adopts the authority's record, so this node
    ///   serves the committed value and the next digest exports it instead of
    ///   re-detecting the same divergence (issue #214);
    /// * **no fence readable here** (never committed, or the authority lives on
    ///   another node): a committed token beats nothing. Treating unreadable as
    ///   repairable keeps the topic healable — the same principle as not recording a
    ///   token on a failed write.
    pub(super) async fn snapshot_entry_applies(
        &mut self,
        topic: &str,
        token: (u64, u64),
        value_held: bool,
        payload_is_empty: bool,
    ) -> bool {
        if token == (0, 0) {
            return !value_held
                && !payload_is_empty
                && self.durable_retained_authority(topic).await.is_none();
        }
        if let Some(applied) = self.retained_tokens.get(topic).copied() {
            return token > applied;
        }
        let Some(authority) = self.durable_retained_authority(topic).await else {
            return true;
        };
        if token < authority.token() {
            let repair = Bytes::from(authority.payload.clone());
            let app = AppProperties::from(authority.props.clone());
            self.apply_retained_update(
                topic,
                &repair,
                authority.qos,
                &app,
                authority.token(),
                authority.expires_at,
            )
            .await;
            return false;
        }
        true
    }

    /// The retained-handoff bookkeeping tied to a peer's **link session** (T8),
    /// dropped when the link goes: a handoff awaiting that peer's ack returns to the
    /// queue (the queue-until-heal path takes over), and the owner-side dedup state
    /// for the peer is cleared — a restarted peer restarts its seq counter, and a
    /// stale dedup entry could wrongly swallow its first new handoff. The cost of
    /// clearing is bounded and benign: a retransmission across the flap may commit
    /// the same value twice (idempotent, higher token).
    pub(super) fn drop_retained_handoff_state(&mut self, node: &NodeId) {
        if self
            .retained_handoff
            .as_ref()
            .is_some_and(|(owner, ..)| owner == node)
        {
            if let Some((_, _, mutation)) = self.retained_handoff.take() {
                self.retained_queue.push_front(mutation);
            }
        }
        self.retained_handoff_seen.remove(node);
        self.retained_handoff_pending.remove(node);
    }

    /// Enqueue a retained mutation for its authority commit (ADR 0037 §1/§5). With
    /// durable off (`durable_retained` unset) this is a no-op and retained keeps the
    /// ADR 0014 best-effort behaviour. Every mutation — locally published or routed
    /// here by a peer — passes through the bounded per-node queue, which serializes
    /// commits (per-node order holds even for rapid same-topic publishes) and lets a
    /// mutation that cannot reach its owner wait for a heal instead of being dropped.
    /// At the bound the **oldest** is dropped, loudly.
    #[allow(clippy::too_many_arguments)] // the mutation's fields, plus the gate
    pub(super) fn route_retained_commit(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: u8,
        app: &AppProperties,
        gate: Option<u64>,
        expires_at: Option<u64>,
        restore: bool,
    ) {
        if self.durable_retained.is_none() {
            return; // durable off: ADR 0014 behaviour, unchanged (ADR 0037 §6)
        }
        // The gated publish's ack now waits for this authority commit (ADR 0042 T9,
        // exhibit ⑦) — the obligation rides the mutation through re-queues and the
        // handoff hold, however long the commit takes.
        if let Some(id) = gate {
            if let Some(p) = self.pending_publishes.get_mut(&id) {
                p.awaiting_retained = true;
            }
        }
        self.enqueue_retained_mutation(RetainedMutation {
            topic: topic.to_string(),
            payload: payload.clone(),
            qos,
            app: app.clone(),
            reply: None,
            publish: gate,
            expires_at,
            restore,
        });
        self.kick_retained_queue();
    }

    /// Write one restored retained value as retained state, with NO subscriber fan-out
    /// (ADR 0062 — see [`HubCommand::RestoreRetained`] for why a publish cannot be used).
    ///
    /// Durable retained ON: the ordinary authority route (owner-routed, quorum-committed),
    /// with the caller's completion hung on the SAME gate a gated publish uses for its
    /// retained obligation — so the answer means "durably the topic's retained value
    /// cluster-wide", and a mutation dropped at the queue bound withholds it (the restore
    /// then fails loudly rather than reporting a value it did not store).
    ///
    /// Durable retained OFF: the node-local cache, write-through, answered directly.
    ///
    /// Either way the only outward traffic is the token-carrying fan-out to peer CACHES that
    /// `RetainedCommitDone` already performs. No session queue is touched.
    pub(super) async fn restore_retained(
        &mut self,
        topic: String,
        payload: Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: AppProperties,
        done: oneshot::Sender<PublishOutcome>,
    ) {
        let expires_at = message_expiry.map(|s| self.clock.now_epoch_secs() + u64::from(s));
        if self.durable_retained.is_some() {
            // Reuse the gate machinery, then correct the two fields that only make sense
            // for a publish: there is no local fan-out to complete (`local_done`) and no
            // takeover window to wait for (`awaiting_settle`) — a restore runs before any
            // listener binds, and holding the answer for a settle that no publish will
            // trigger would stall the restore for nothing.
            let id = self.register_pending(done, &topic, &payload, qos, true, message_expiry, &app);
            if let Some(p) = self.pending_publishes.get_mut(&id) {
                p.local_done = true;
                p.awaiting_settle = false;
            }
            self.route_retained_commit(
                &topic,
                &payload,
                qos_num(qos),
                &app,
                Some(id),
                expires_at,
                true,
            );
            return;
        }
        let message = Message {
            topic,
            payload,
            qos,
            retain: true,
            app,
            expires_at,
        };
        self.retained_may_expire |= message.expires_at.is_some();
        match self.retained.set(&message).await {
            Ok(()) => {
                let _ = done.send(PublishOutcome::Accepted);
            }
            Err(e) => {
                warn!(topic = %message.topic, error = %e, "restore: retained write failed");
                // Dropping `done` withholds: the importer reports "never answered" and the
                // restore fails. A restore that silently skipped a retained value would be
                // the half-true backup this feature exists not to be.
            }
        }
    }

    /// Every retained topic this node holds, paired with its convergence token — the
    /// export's atomic cut (ADR 0062; see [`HubCommand::RetainedExportSnapshot`]).
    ///
    /// Tombstones are included as empty-payload entries: the cache drops a cleared topic, so
    /// the clear exists only as a token here, and without it a value another node still
    /// caches would be resurrected by the restore's union.
    pub(super) async fn retained_export_snapshot(&mut self) -> RetainedExportAnswer {
        let values = self.retained.all().await.map_err(|e| {
            warn!(error = %e, "retained export snapshot failed; the export will fail rather \
                  than report an empty retained set");
            format!("backup: the retained store could not be read: {e}")
        })?;
        let mut out: RetainedExportCut = values
            .into_iter()
            .map(|m| {
                let token = self.retained_tokens.get(&m.topic).copied();
                (m, token)
            })
            .collect();
        for topic in self.retained_tombstone_observed_at.keys() {
            let token = self.retained_tokens.get(topic).copied();
            out.push((
                Message {
                    topic: topic.clone(),
                    payload: Bytes::new(),
                    qos: QoS::AtMostOnce,
                    retain: true,
                    app: AppProperties::default(),
                    expires_at: None,
                },
                token,
            ));
        }
        Ok(out)
    }

    /// Admit a mutation to the bounded queue, dropping the **oldest** loudly at the
    /// cap (ADR 0037 §5). A dropped peer-routed mutation also clears its pending
    /// marker, so the sender's retransmission can be admitted again later.
    pub(super) fn enqueue_retained_mutation(&mut self, mutation: RetainedMutation) {
        if self.retained_queue.len() >= RETAINED_QUEUE_CAP {
            if let Some(dropped) = self.retained_queue.pop_front() {
                warn!(
                    topic = %dropped.topic,
                    cap = RETAINED_QUEUE_CAP,
                    "retained mutation queue full; dropped the OLDEST queued mutation \
                     (ADR 0037 §5 — the partition has outlasted the queue bound)"
                );
                if let Some((node, seq)) = dropped.reply {
                    if self.retained_handoff_pending.get(&node) == Some(&seq) {
                        self.retained_handoff_pending.remove(&node);
                    }
                }
                // A gated publish whose authority commit was dropped never acks
                // (ADR 0042 T9, exhibit ⑦): the publisher retries.
                if let Some(id) = dropped.publish {
                    self.drop_pending(id);
                }
            }
            if let Some(m) = &self.metrics {
                m.retained_queue_dropped();
            }
        }
        self.retained_queue.push_back(mutation);
    }

    /// Accept a retained mutation a peer routed to this node (ADR 0037 §1/T8):
    /// dedup retransmissions against the last committed handoff (re-ack, don't
    /// recommit) and against one still queued/committing, then run it through the
    /// same queue as local mutations.
    #[allow(clippy::too_many_arguments)] // the mutation's fields, plus the handoff key
    pub(super) fn accept_routed_retained(
        &mut self,
        node: NodeId,
        topic: String,
        payload: Bytes,
        qos: u8,
        app: AppProperties,
        seq: u64,
        expires_at: Option<u64>,
    ) {
        if self.durable_retained.is_none() {
            return;
        }
        // The commit landed but the ack was lost: answer again, commit nothing.
        if let Some((last_seq, token)) = self.retained_handoff_seen.get(&node) {
            if *last_seq == seq {
                let token = *token;
                self.send_retained_ack(&node, seq, Some(token));
                return;
            }
        }
        // The original is still queued or committing: ignore the retransmission.
        if self.retained_handoff_pending.get(&node) == Some(&seq) {
            return;
        }
        self.retained_handoff_pending.insert(node.clone(), seq);
        self.enqueue_retained_mutation(RetainedMutation {
            topic,
            payload,
            qos,
            app,
            reply: Some((node, seq)),
            publish: None,
            expires_at,
            // A peer-routed mutation is a publish on its origin node; the origin's own
            // restore suppresses its own delivery, and this node has no window to serve
            // for a value it never subscribed to.
            restore: false,
        });
        self.kick_retained_queue();
    }

    /// Send a commit-gated handoff ack (T8) back to `node`, if its link is up. A
    /// missing link is fine: the committed `(seq, token)` stays recorded in
    /// `retained_handoff_seen`, so the sender's retransmission gets the ack then.
    pub(super) fn send_retained_ack(&self, node: &NodeId, seq: u64, token: Option<(u64, u64)>) {
        if let Some(peer) = self.peers.get(node) {
            let _ = peer.tx.send(PeerMessage::RetainedCommitAck { seq, token });
        }
    }

    /// Drive the retained mutation queue (ADR 0037 §5/T8): drain entries in order —
    /// an owner-local head starts the (single) off-loop commit; a peer-owned head is
    /// handed to its linked owner and **held until the commit-gated ack** (one
    /// handoff in flight, retransmitted by the sweep tick) — and stop at an entry
    /// whose owner is unreachable, leaving it queued for the next heal trigger (a
    /// peer link coming up, the sweep tick, or the next enqueue).
    pub(super) fn kick_retained_queue(&mut self) {
        if self.retained_handoff.is_some() {
            return; // a handoff is awaiting its ack: order requires we wait
        }
        while !self.retained_commit_inflight {
            let Some(mutation) = self.retained_queue.pop_front() else {
                return;
            };
            // The owner of the topic's placement group; with no ring (single node /
            // no cluster), this node is trivially the owner. Resolved at drain time,
            // not enqueue time, so a lease that moved while queued re-routes.
            let owner = self.placement.as_ref().map_or_else(
                || self.node_id.clone(),
                |p| {
                    p.read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .owner(&mutation.topic)
                },
            );
            if owner == self.node_id {
                self.retained_commit_inflight = true;
                self.spawn_retained_commit(mutation);
                return;
            }
            // Peer-owned. A mutation a peer routed HERE for a group this node no
            // longer owns is NACKed back so the sender re-resolves (T8) — this node
            // must not relay it onward (the ack chain would break).
            if let Some((node, seq)) = mutation.reply {
                if self.retained_handoff_pending.get(&node) == Some(&seq) {
                    self.retained_handoff_pending.remove(&node);
                }
                self.send_retained_ack(&node, seq, None);
                continue;
            }
            if self.peers.contains_key(&owner) {
                // Hand the mutation to its owner and hold it until the commit-gated
                // ack (T8): a frame lost to a dying link is retransmitted, never
                // silently lost. One in flight keeps per-node publish order.
                self.retained_handoff_seq += 1;
                let seq = self.retained_handoff_seq;
                self.send_retained_handoff(&owner, seq, &mutation);
                self.retained_handoff = Some((owner, seq, mutation));
                return;
            }
            // Owner unreachable (partitioned or dead): queue-until-heal. Put the
            // entry back and wait for a trigger — never dropped silently.
            debug!(
                topic = %mutation.topic,
                owner = %owner.0,
                queued = self.retained_queue.len() + 1,
                "retained mutation owner unreachable; queued until heal (ADR 0037 §5)"
            );
            self.retained_queue.push_front(mutation);
            return;
        }
    }

    /// Write one handoff frame toward `owner` (first send and retransmissions alike).
    pub(super) fn send_retained_handoff(
        &self,
        owner: &NodeId,
        seq: u64,
        mutation: &RetainedMutation,
    ) {
        if let Some(peer) = self.peers.get(owner) {
            let _ = peer.tx.send(PeerMessage::RetainedCommit {
                topic: mutation.topic.clone(),
                payload: mutation.payload.to_vec(),
                qos: mutation.qos,
                props: app_to_wire(&mutation.app),
                seq,
                expires_at: mutation.expires_at,
            });
        }
    }

    /// The sweep-tick half of the handoff protocol (T8): retransmit an unanswered
    /// handoff (same `seq` — the owner dedups), or reclaim it into the queue if the
    /// owner's link is gone (the regular queue-until-heal path takes over).
    pub(super) fn retry_retained_handoff(&mut self) {
        let Some((owner, seq, mutation)) = self.retained_handoff.take() else {
            return;
        };
        if self.peers.contains_key(&owner) {
            debug!(topic = %mutation.topic, owner = %owner.0, seq, "retransmitting unanswered retained handoff");
            self.send_retained_handoff(&owner, seq, &mutation);
            self.retained_handoff = Some((owner, seq, mutation));
        } else {
            self.retained_queue.push_front(mutation);
        }
    }

    /// Start the off-loop durable commit for an owner-local retained mutation: the
    /// quorum round-trip must not stall the hub actor, and exactly one runs at a time
    /// (`retained_commit_inflight`) so commits keep queue order. A zero-length
    /// payload is the MQTT clear [MQTT-3.3.1-10] → a versioned tombstone (ADR 0037
    /// P2). Completion posts [`HubCommand::RetainedCommitDone`] back to the loop.
    pub(super) fn spawn_retained_commit(&self, mutation: RetainedMutation) {
        let Some(durable) = self.durable_retained.clone() else {
            return;
        };
        let self_tx = self.self_tx.clone();
        let RetainedMutation {
            topic,
            payload,
            qos,
            app,
            reply,
            publish,
            expires_at,
            restore,
        } = mutation;
        tokio::spawn(async move {
            let result = if payload.is_empty() {
                durable.clear(&topic).await
            } else {
                durable
                    .set(&topic, &payload, qos, &AppProps::from(&app), expires_at)
                    .await
            };
            let token = match result {
                Ok((epoch, offset)) => {
                    debug!(topic = %topic, epoch, offset, "retained mutation committed");
                    Some((epoch, offset))
                }
                // NotOwner: the lease moved after routing (the re-queued entry
                // re-resolves its owner on the next drain). NoQuorum: this side of a
                // partition cannot commit durably — queue until it heals.
                Err(e) => {
                    warn!(
                        topic = %topic,
                        error = %e,
                        "retained durable commit failed; mutation queued until heal (ADR 0037 §5)"
                    );
                    None
                }
            };
            let _ = self_tx.send(HubCommand::RetainedCommitDone {
                topic,
                payload,
                qos,
                app,
                token,
                reply,
                publish,
                expires_at,
                restore,
            });
        });
    }

    /// Apply a **committed** retained value to the local cache iff its token exceeds
    /// the held one (ADR 0037 §3): monotonic per topic, idempotent, order-insensitive.
    /// Whether `token` is STALE for `topic` — i.e. superseded, and must not be applied.
    ///
    /// The two sources answer different questions, and conflating them is a bug:
    ///
    /// * `retained_tokens` is "what this node has already APPLIED SUCCESSFULLY", so an
    ///   equal token is a duplicate and is skipped. It is in-memory and does not survive a
    ///   restart.
    /// * the durable record is "what the cluster has COMMITTED". An equal token there is
    ///   *this very commit* — the owner applying its own write, or a repair of one whose
    ///   store write failed — so it must be APPLIED, not skipped. Only a strictly older
    ///   token is stale.
    ///
    /// Reading the durable record is what makes tombstone fences survive a restart (issue
    /// #87 item 4). They used to live only in the in-memory map, so a clear committed while
    /// one node was down, followed by a restart of the survivors, left nobody holding the
    /// fence — and the absent node's stale value was re-applied cluster-wide, resurrecting
    /// a retained message the user had deleted. The periodic digest (0014-T10) spread it
    /// faster, not slower. No schema change was needed: `RetainedEntry` already carries
    /// `tombstone`, `epoch` and `offset`; `DurableRetained::get` simply had no production
    /// caller, so the keyspace was written and never read.
    pub(super) async fn retained_is_stale(&self, topic: &str, token: (u64, u64)) -> bool {
        if let Some(held) = self.retained_tokens.get(topic) {
            return token <= *held;
        }
        match self.durable_retained_authority(topic).await {
            Some(entry) => token < entry.token(),
            None => false,
        }
    }

    /// The durable authority's committed record for `topic`, when it is readable from
    /// this node. The keyspace routes per key to the topic's group owner, so a
    /// non-owner — like a routing/quorum error, or durable off — gets `None` and the
    /// caller falls back to what it holds. A read failure must not invent a fence:
    /// answering `None` keeps the topic repairable, the same principle as not
    /// recording a token on a failed write.
    pub(super) async fn durable_retained_authority(
        &self,
        topic: &str,
    ) -> Option<mqtt_storage::retained_log::RetainedEntry> {
        match self.durable_retained.as_ref()?.get(topic).await {
            Ok(entry) => entry,
            Err(e) => {
                debug!(topic = %topic, error = %e, "durable retained authority lookup failed");
                None
            }
        }
    }

    /// Re-adopt every committed retained record this node can read but no longer
    /// remembers — the restart-recovery seam for durable retraction (issue #183).
    ///
    /// `retained_tokens` is in-memory and empty after a restart. For a topic with a
    /// live VALUE the persistent cache still names it, and the snapshot sender
    /// re-reads its token per topic (#214). A **tombstone** leaves nothing behind at
    /// all — no cache entry, no token — so a restarted node stopped advertising its
    /// clears entirely: `local_retained_digest` saw nothing to offer, and snapshot
    /// tombstone entries (built from the token map) omitted them. A peer that was
    /// down for the clear then kept serving the deleted value until the topic's next
    /// committed write — retraction was not durable in any exportable sense.
    ///
    /// Enumerate the keyspace and re-apply each readable committed record through
    /// the ordinary idempotent apply: a value re-warms cache + token; a tombstone
    /// re-arms the fence (and drops a stale pre-clear cache value the crash may have
    /// stranded). `NotOwner`/read failures skip — that topic's owner exports it —
    /// and are retried on the next call. Called on the anti-entropy cadence and
    /// before building a snapshot, so the cost (one key enumeration + one authority
    /// read per still-unknown topic) is off every hot path and quickly reaches a
    /// fixed point: a topic with an in-memory token is never re-read.
    pub(super) async fn warm_retained_tokens_from_authority(&mut self) {
        let Some(durable) = self.durable_retained.clone() else {
            return;
        };
        let topics = match durable.topics().await {
            Ok(topics) => topics,
            Err(e) => {
                debug!(error = %e, "retained keyspace enumeration failed; next cadence retries");
                return;
            }
        };
        for topic in topics {
            if self.retained_tokens.contains_key(&topic) {
                continue; // already known to this process — warm is a fixed point
            }
            let Some(entry) = self.durable_retained_authority(&topic).await else {
                continue; // not readable here (NotOwner / transient): the owner exports it
            };
            let payload = Bytes::from(entry.payload.clone());
            let app = AppProperties::from(entry.props.clone());
            self.apply_retained_update(
                &topic,
                &payload,
                entry.qos,
                &app,
                entry.token(),
                entry.expires_at,
            )
            .await;
        }
    }

    /// Reap retained values whose Message Expiry Interval has passed (issue #227) —
    /// the spec deletes the retained copy at expiry, not merely hides it. Under
    /// durable retained the reap is an ordinary owner-committed CLEAR, so every
    /// cache converges by token exactly as for a client's zero-length publish —
    /// never by comparing clocks across nodes (a non-owner leaves the row for the
    /// owner's clear to fan out, and merely filters it from reads meanwhile).
    /// Durable off deletes locally: each node is its own retained island there
    /// (ADR 0014), and every island carries the same absolute deadline.
    pub(super) async fn reap_expired_retained(&mut self) {
        let Ok(all) = self.retained.all().await else {
            return;
        };
        let now = self.clock.now_epoch_secs();
        let mut deadlines_remain = false;
        for m in all {
            if m.expires_at.is_none_or(|d| d > now) {
                deadlines_remain |= m.expires_at.is_some();
                continue;
            }
            if self.durable_retained.is_some() {
                let owned = self.placement.as_ref().is_some_and(|p| {
                    p.read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .owner(&m.topic)
                        == self.node_id
                });
                if owned {
                    debug!(topic = %m.topic, "retained value expired; committing the reap as a clear");
                    self.route_retained_commit(
                        &m.topic,
                        &Bytes::new(),
                        0,
                        &AppProperties::default(),
                        None,
                        None,
                        false,
                    );
                } else {
                    // Not ours to commit: keep watching until the owner's clear
                    // fans out and removes the row.
                    deadlines_remain = true;
                }
            } else {
                debug!(topic = %m.topic, "retained value expired; deleted");
                let clear = Message::new(m.topic.clone(), Bytes::new(), QoS::AtMostOnce, true);
                if let Err(e) = self.retained.set(&clear).await {
                    warn!(topic = %m.topic, error = %e, "failed to delete an expired retained value");
                    deadlines_remain = true; // still there; retry next tick
                }
            }
        }
        // Under durable, an owner-committed reap lands back through the fan-out
        // (which re-arms the flag if a deadline remains); locally nothing is left.
        self.retained_may_expire = deadlines_remain;
    }

    /// Discharge retained tombstones the cluster has observably converged past
    /// (issue #229) — the growth bound on durable retraction. A tombstone's fence
    /// exists to stop an absent node's pre-clear value from resurrecting; once
    /// every member that could still return has been SEEN converged (its digest
    /// matched ours after the clear was observed), the fence is discharged: the
    /// in-memory token drops here, and the topic's group owner also removes the
    /// keyspace record. Gated on the durable membership ROSTER (voters and
    /// learners — crashed members stay on it until decommissioned, and members
    /// this process cannot even name block the reap outright), never on live
    /// gossip, which forgets the absent. Each node discharges independently off
    /// the same signals; a record resurfacing from an old replica after a reap
    /// merely re-arms a redundant fence. Runs per tick, pay-for-use (no
    /// tombstones = no work); one full anti-entropy period must also have passed
    /// so a match instant is never stale-by-construction.
    pub(super) async fn reap_discharged_tombstones(&mut self) {
        let Some(durable) = self.durable_retained.clone() else {
            return;
        };
        let Some((roster, unknown)) = self.placement.as_ref().and_then(|p| {
            p.read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .durable_roster()
                .cloned()
        }) else {
            return; // the roster has never been pushed: nothing is dischargeable
        };
        if unknown > 0 {
            return; // a member we cannot even name may still hold the value
        }
        let now = self.clock.now_epoch_secs();
        let period = u64::from(RETAINED_ANTIENTROPY_EVERY);
        let discharged: Vec<String> = self
            .retained_tombstone_observed_at
            .iter()
            .filter(|(_, observed)| now.saturating_sub(**observed) >= period)
            .filter(|(_, observed)| {
                roster.iter().filter(|n| **n != self.node_id).all(|n| {
                    self.retained_digest_matched_at
                        .get(n)
                        .is_some_and(|m| m > *observed)
                })
            })
            .map(|(t, _)| t.clone())
            .collect();
        for topic in discharged {
            let owned = self.placement.as_ref().is_some_and(|p| {
                p.read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .owner(&topic)
                    == self.node_id
            });
            if owned {
                if let Err(e) = durable.reap(&topic).await {
                    // Keep the fence and the clock; the next tick retries.
                    debug!(topic = %topic, error = %e, "tombstone reap deferred");
                    continue;
                }
            }
            self.retained_tokens.remove(&topic);
            self.retained_tombstone_observed_at.remove(&topic);
            info!(
                topic = %topic,
                "retained tombstone DISCHARGED: every durable-roster member converged \
                 past the clear (issue #229)"
            );
        }
    }

    /// An empty payload is a committed clear — the cache drops the topic, but the
    /// tombstone's token is kept so a staler value cannot resurrect it.
    pub(super) async fn apply_retained_update(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: u8,
        app: &AppProperties,
        token: (u64, u64),
        expires_at: Option<u64>,
    ) {
        self.apply_committed_retained(topic, payload, qos, app, token, expires_at, true)
            .await;
    }

    /// The body of [`apply_retained_update`], with the window-scoped delivery (issue #219)
    /// made a CHOICE — `deliver_windowed = false` for a mutation a RESTORE originated.
    ///
    /// The cache write, the token fence and the tombstone bookkeeping are identical either
    /// way: a restored value is ordinary committed retained state. What a restore must not
    /// do is *deliver*. The window back-fill exists for a subscription so fresh that its
    /// interest had not reached the publish's landing node — a live-client situation with no
    /// analogue during a restore, where no listener is bound and every session's own export
    /// already accounts for what it was owed. Delivering there would add messages to a
    /// restored queue that were in no backup, which is the one thing a restore may never do.
    #[allow(clippy::too_many_arguments)] // the committed record's fields, plus the choice
    pub(super) async fn apply_committed_retained(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: u8,
        app: &AppProperties,
        token: (u64, u64),
        expires_at: Option<u64>,
        deliver_windowed: bool,
    ) {
        if self.retained_is_stale(topic, token).await {
            debug!(topic = %topic, ?token, "stale/duplicate retained update skipped");
            return;
        }
        let message = Message {
            topic: topic.to_string(),
            payload: payload.clone(),
            qos: QoS::from_u8(qos).unwrap_or(QoS::AtMostOnce),
            retain: true,
            app: app.clone(),
            expires_at,
        };
        // STORE FIRST, then record the token — and never the other way round (issue #87).
        //
        // The token is the fence that makes this idempotent: anything at or below it is
        // skipped above. Recording it before the write meant a FAILED write left the node
        // holding a fence with no value behind it, and nothing could ever get past:
        // re-delivery of the same commit is skipped by the guard, and the periodic digest
        // cannot repair it either, because the repairing snapshot carries the SAME token
        // and `token > held` is false. One transient store error blackholed one topic on
        // one node permanently, while its peers served the value normally — with no metric
        // and no divergence signal, because a node with no value has nothing to compare.
        self.retained_may_expire |= message.expires_at.is_some();
        let tombstone = message.payload.is_empty();
        if let Err(e) = self.retained.set(&message).await {
            // No token recorded: the next commit for this topic, and the next digest
            // repair, both still apply.
            warn!(topic = %topic, error = %e, "failed to apply committed retained update");
            if let Some(m) = &self.metrics {
                m.retained_apply_failed();
            }
            return;
        }
        self.retained_tokens.insert(topic.to_string(), token);
        // Tombstone bookkeeping (issue #229): a clear starts the discharge clock;
        // a value re-taking the topic ends it (the fence is a value token again).
        if tombstone {
            self.retained_tombstone_observed_at
                .entry(topic.to_string())
                .or_insert_with(|| self.clock.now_epoch_secs());
        } else {
            self.retained_tombstone_observed_at.remove(topic);
        }

        // #87 item 3, delivered WINDOW-SCOPED (#219). Live delivery to an established
        // subscriber is the interest-forward path's job (`forward_to_peers` →
        // `deliver_local`), and delivering from here as well would double every
        // retained update in the steady state — that rejection stands. The one
        // delivery the forward structurally cannot make is to a subscription so
        // fresh that its interest had not reached the publish's landing node: for
        // exactly those (open windows, ledger-deduped), this apply IS the vehicle.
        if deliver_windowed {
            self.deliver_to_windowed_subscribers(topic, payload, qos, app, expires_at);
        }
    }

    /// Deliver a just-applied committed retained value to local subscribers whose
    /// subscription is younger than the interest-propagation horizon (issue #219) —
    /// the delivery the interest-forward path structurally cannot make. Deduped per
    /// (client, topic) through the window's ledger, so a copy the live path (or the
    /// subscribe replay) already delivered is not repeated; the reverse race (this
    /// path first, a forwarded copy second) is deliberately NOT suppressed and stays
    /// within `QoS` 1's at-least-once. A clear (empty payload) is delivered like any
    /// zero-length publish [MQTT-3.3.1-10]. An offline persistent subscriber gets
    /// the queue semantics `deliver_to_client` always applies, closing the same
    /// window for the resume replay. No-op in the steady state (no open windows).
    pub(super) fn deliver_to_windowed_subscribers(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: u8,
        app: &AppProperties,
        expires_at: Option<u64>,
    ) {
        if self.retained_windows.is_empty() {
            return;
        }
        // An already-expired value is not delivered [MQTT-3.3.2-5]; the remaining
        // interval rides the delivery (issue #227).
        let epoch_now = self.clock.now_epoch_secs();
        if expires_at.is_some_and(|d| d <= epoch_now) {
            return;
        }
        let remaining =
            expires_at.map(|d| u32::try_from(d.saturating_sub(epoch_now)).unwrap_or(u32::MAX));
        let now = Instant::now();
        let id = retained_value_id(topic, payload.as_ref(), qos, &AppProps::from(app).encode());
        let targets: Vec<(ClientId, QoS, bool)> = self
            .table
            .matching_clients(topic)
            .into_iter()
            .filter(|c| {
                self.retained_windows
                    .get(c)
                    .is_some_and(|w| w.until > now && w.seen.get(topic) != Some(&id))
            })
            .map(|c| {
                let granted = self.granted_qos(&c, topic);
                // Retained-originated: RAP subscribers keep the flag, everyone else
                // gets 0 — the live path's rule (#198).
                let retain = self.keeps_retain_flag(&c, topic);
                (c, granted, retain)
            })
            .collect();
        for (c, granted, retain) in targets {
            let delivery_qos = min_qos(QoS::from_u8(qos).unwrap_or(QoS::AtMostOnce), granted);
            // A failed enqueue leaves the value in the cache for the next
            // subscribe's replay — the replay's own posture (#124).
            // Unanswerable (issue #238): this back-fill has no publisher behind it, so a
            // refused durable copy must not cost the live delivery too — the message
            // would simply be gone, with nobody to retry it.
            let _ = self.deliver_to_client(
                &c,
                topic,
                payload,
                delivery_qos,
                remaining,
                app,
                retain,
                &AppendGate::None,
            );
            if let Some(w) = self.retained_windows.get_mut(&c) {
                w.seen.insert(topic.to_string(), id);
            }
        }
    }
}
