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
    origin: None,
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

    /// Test-only reads; production code goes through `get`/`iter`.
    #[cfg(test)]
    pub(super) fn contains_key(&self, id: u64) -> bool {
        self.entries.contains_key(&id)
    }

    #[cfg(test)]
    pub(super) fn keys(&self) -> impl Iterator<Item = &u64> {
        self.entries.keys()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &PendingPublish> {
        self.entries.values()
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

    /// Record that entry `id` has been handed to `clients` ([`PendingPublish::reached`]),
    /// charging what the record costs against the byte bound like the rest of the
    /// entry. A high fan-out held open by a long settle window is exactly what
    /// this list can grow with, so it must not sit outside the bound: the next
    /// insert evicts for it as it would for a larger payload.
    pub(super) fn note_reached(
        &mut self,
        id: u64,
        clients: impl ExactSizeIterator<Item = ClientId>,
    ) {
        let n = clients.len();
        if n == 0 {
            return;
        }
        let Some(entry) = self.entries.get_mut(&id) else {
            return;
        };
        let mut charge = n * std::mem::size_of::<ClientId>();
        if let Some(reached) = &mut entry.reached {
            reached.extend(clients);
        } else {
            charge += std::mem::size_of::<Vec<ClientId>>();
            entry.reached = Some(Box::new(clients.collect()));
        }
        let before = entry.cost;
        entry.cost = u32::try_from(before as usize + charge).unwrap_or(u32::MAX);
        self.bytes += (entry.cost - before) as usize;
    }

    /// Evict the OLDEST entry (the lowest id): the overflow policy of both bounds.
    pub(super) fn pop_first(&mut self) -> Option<(u64, PendingPublish)> {
        let (id, entry) = self.entries.pop_first()?;
        self.bytes -= entry.cost as usize;
        Some((id, entry))
    }
}

/// `table[&id]`, for the tests and assertions that read an entry they know is
/// there. Panics like `BTreeMap`'s own index on a missing id.
impl std::ops::Index<&u64> for PendingTable {
    type Output = PendingPublish;
    fn index(&self, id: &u64) -> &PendingPublish {
        &self.entries[id]
    }
}

/// How many settle passes a forward is held for a takeover that stays open (issue
/// #1031) before it is answered `Failed` instead. A pass runs every tick and again
/// at every scan landing, and during an open window a scan lands about every tick,
/// so in practice this is nearer 15 s than 30 s. A takeover's scan lands in well
/// under a second on a healthy node; this bounds only a takeover that keeps
/// re-arming.
pub(super) const HELD_FORWARD_MAX_TICKS: u16 = 30;

/// The most forwards one node holds for a takeover at once (issue #1031). Each holds
/// its payload; past this a matched forward is answered `Failed` at once ("cannot
/// say, retry") rather than held, so a burst during a takeover costs bounded memory.
pub(super) const MAX_HELD_FORWARDS: usize = 4096;

/// An acked forward whose answer waits for the scan of this node's open takeover
/// (issue #1031): what it carried, who its fan-out reached, when it arrived, and
/// which scan can release it. See [`Hub::hold_forward_for_takeover`].
#[derive(Debug)]
pub(super) struct HeldForward {
    pub(super) node: NodeId,
    pub(super) seq: u64,
    pub(super) topic: String,
    pub(super) payload: Bytes,
    pub(super) qos: QoS,
    pub(super) message_expiry: Option<u32>,
    pub(super) app: AppProperties,
    /// The sessions the fan-out handed it at arrival.
    pub(super) reached: FastSet<ClientId>,
    pub(super) since: Instant,
    /// Settle passes it has waited through.
    pub(super) ticks: u16,
    /// The number of the latest scan started before the hold: only a scan numbered
    /// above it can release it (a scan already in flight at the hold may have read
    /// before the session's group was this node's).
    pub(super) after_scan: u64,
    /// A complete scan numbered above `after_scan`, started under the ownership
    /// that holds now, has landed: the sessions the takeover was bringing are
    /// materialised, which is what the hold waited for.
    pub(super) scan_landed: bool,
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
    // A std map, not a `FastMap`: one per pending publish, so its hasher's state is
    // paid per entry (`a_pending_publish_stays_small`), and it is empty on a single
    // node and answered once per peer ack on a cluster — not the per-message path.
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
    /// When this publish's most recent forward obligation was registered (and its
    /// frame first sent), as milliseconds after `created_at` — what the sweep's
    /// "overdue" is measured from (issue #644). A `u32` offset, not an `Instant`,
    /// so it fits the entry's size budget; it saturates after 49 days.
    ///
    /// Not `created_at` itself: a takeover re-route registers a NEW obligation on a
    /// publish that may be many sweeps old, and judging that obligation by the
    /// publish's age retransmitted it on the very next sweep — milliseconds after
    /// the first copy, before any answer could exist. The receiver then applied
    /// every copy, so each premature retransmit became a second durable enqueue
    /// and a second `DUP = 0` delivery of the same message to the subscriber
    /// [MQTT-4.4.0-1]. (A receiver now recognises the repeat — issue #648 — but a
    /// retransmit that is not due is still traffic offered to a peer already
    /// behind.)
    pub(super) forwarded_after_ms: u32,
    /// Engaged when a forward's target was erased — held dead, and out of reach
    /// until it was confirmed gone or, with a durable plane, its countdown ran out
    /// ([`Hub::erase_peer`]): the obligation is gone, and
    /// this counts down the sweep ticks left to find a successor or a session
    /// materialised here before the publisher is answered. Since ADR 0087 D1 the
    /// waiting is done while the obligation is still held, so this is armed with
    /// one tick: the last look.
    pub(super) reroute_grace: Option<u8>,
    /// Every local recipient this publish has already been handed to — by its
    /// original fan-out and by each settle-window re-delivery since — so
    /// [`redeliver_pending`](Hub::redeliver_pending) never hands one session a
    /// second copy. `None` = nobody yet.
    ///
    /// The re-delivery's target filter ("offline, or attached after the
    /// publish") is a superset of "missed the original fan-out": an offline
    /// persistent session the fan-out ENQUEUED to passes it, and so does a
    /// session re-delivered to on an earlier pass. Since the settle pass runs on
    /// every sweep tick (issue #613 item 2.2 follow-up), each pass appended one
    /// more copy of every held publish to every such session's durable log —
    /// one per second for as long as the window stayed open — and each copy
    /// replays as a first delivery with `DUP = 0` [MQTT-4.4.0-1]. The nightly
    /// seed sweep caught ~25 copies per message after a SIGKILL behind a
    /// severed link.
    ///
    /// Boxed: 8 bytes on the entry, where a `Vec` (24) or a boxed slice (16)
    /// would break the size budget `a_pending_publish_stays_small` holds it to.
    /// Written once by the original fan-out and extended only by a re-delivery,
    /// off the message path.
    #[allow(clippy::box_collection)] // deliberate: the box is what keeps the entry small
    pub(super) reached: Option<Box<Vec<ClientId>>>,
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
    /// thing that makes dropping this entry harmless, and what the cap uses to
    /// count an evicted entry as `pending-cap-replay` rather than a withhold.
    pub(super) fn ack_released(&self) -> bool {
        self.done.is_none()
    }

    /// Take this publish out of the settle window: it owes the window no replay
    /// AND its ack no longer waits on it (issue #613 item 2.1).
    ///
    /// The ONLY way to clear either settle flag. Item 2.1 split one flag into two
    /// — `awaiting_settle` (the replay work set) and `ack_awaits_settle` (the ack
    /// hold) — and `try_complete_pending` retires an entry once `awaiting_settle`
    /// is false. A site that cleared only that one, as the retained RESTORE path
    /// did, retired the entry with its ack still held: the sender was dropped
    /// unsent, which is a silent withhold, and every restore on an unsettled
    /// clustered node failed with "never answered by the hub". One method that
    /// clears both makes that split unrepresentable at a call site.
    pub(super) fn leave_settle_window(&mut self) {
        self.awaiting_settle = false;
        self.ack_awaits_settle = false;
    }
}

/// How many of one origin's most recent acked forwards a receiver remembers, to
/// recognise a retransmission (issue #648).
///
/// A retransmission is of a forward the sender still holds unanswered, sent again
/// one sweep interval after the last copy; the window only has to reach back past
/// the forwards that arrived from the same origin in between. It advances only as
/// forwards ARRIVE, so a link outage does not age it: the copies the sender
/// re-sends when the link returns meet the same window they left. At 16,384
/// entries it spans several seconds of a heavily loaded origin, at roughly 40 bytes
/// an entry — about 640 KiB per peer, bounded, and freed when the peer's routing
/// state is erased (not at a `Dead` verdict: ADR 0087 D6).
/// A retransmission older than the window is applied again, as before.
pub(super) const FORWARD_WINDOW: usize = 16_384;

/// What a receiver remembers of one acked forward it applied (issue #648).
#[derive(Debug, Clone, Copy)]
pub(super) struct SeenForward {
    /// [`forward_fingerprint`] of the frame, so a restarted origin re-using a seq
    /// for a different message is never mistaken for a repeat.
    pub(super) fingerprint: u64,
    /// The answer sent, once there is one; `None` while its appends are in flight.
    pub(super) verdict: Option<ForwardVerdict>,
}

/// One origin's recent acked forwards, oldest first, capped at [`FORWARD_WINDOW`].
#[derive(Debug, Default)]
pub(super) struct ForwardWindow {
    seen: FastMap<u64, SeenForward>,
    order: VecDeque<u64>,
}

impl ForwardWindow {
    pub(super) fn seen(&self, seq: u64) -> Option<SeenForward> {
        self.seen.get(&seq).copied()
    }

    /// Remember an applied forward (replacing a same-seq entry of a restarted
    /// origin in place), forgetting the oldest beyond the cap.
    pub(super) fn record(&mut self, seq: u64, fingerprint: u64) {
        let fresh = SeenForward {
            fingerprint,
            verdict: None,
        };
        if self.seen.insert(seq, fresh).is_none() {
            self.order.push_back(seq);
            if self.order.len() > FORWARD_WINDOW {
                if let Some(oldest) = self.order.pop_front() {
                    self.seen.remove(&oldest);
                }
            }
        }
    }

    /// Record the answer sent for `seq`, if it is still remembered.
    pub(super) fn answered(&mut self, seq: u64, verdict: ForwardVerdict) {
        if let Some(s) = self.seen.get_mut(&seq) {
            s.verdict = Some(verdict);
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.seen.len()
    }
}

/// The identity of an acked forward's content (issue #648): what a
/// retransmission — byte-identical by construction ([`forward_frame`]) — always
/// shares with its original, and a different message almost never does. `client`
/// is the named member of a shared delivery, `None` for a fan-out forward.
pub(super) fn forward_fingerprint(client: Option<&str>, topic: &str, payload: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    client.hash(&mut h);
    topic.hash(&mut h);
    payload.hash(&mut h);
    h.finish()
}

/// Where this process's forward sequence starts (issue #648): a random point in a
/// 47-bit space, so a restarted node does not re-issue the seqs its previous life
/// used — which a receiver's [`ForwardWindow`] may still remember, and which that
/// life's late answers may still carry. Below 2^47, a seq stays within the 7-byte
/// varint postcard puts on the wire for the first ~4×10^14 forwards of a process's
/// life, and the `u64` counter cannot wrap.
///
/// In-crate unit tests start at 0: they correlate answers by the literal seqs
/// 1, 2, 3 of a fresh hub. Every spawned or in-process node the integration
/// suites run takes the random start.
pub(super) fn initial_forward_seq() -> u64 {
    if cfg!(test) {
        return 0;
    }
    let mut raw = [0u8; 8];
    // A failed draw keeps today's start at 0; the window's fingerprint still
    // keeps a re-issued seq with different content from being dropped.
    match aws_lc_rs::rand::fill(&mut raw) {
        Ok(()) => u64::from_le_bytes(raw) >> 17,
        Err(_) => 0,
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

impl ForwardObligation {
    /// Whether the frame has been written to a link, at least once. `false` for an
    /// obligation recorded while its node had no link, or had not yet announced on
    /// a new one — nobody can have received it, so a `$share` delivery in that
    /// state can move to another member without becoming a duplicate
    /// ([`Hub::move_shared_from_unreachable`]).
    ///
    /// The flag lives in each [`ForwardKind`] variant, where there was padding for
    /// it; beside `kind` it would have grown every obligation by eight bytes.
    pub(super) fn sent(&self) -> bool {
        match self.kind {
            ForwardKind::Ordinary { sent, .. } | ForwardKind::Shared { sent, .. } => sent,
        }
    }

    /// Record whether the frame has been written to a link. Only
    /// [`register_forward`](Hub::register_forward) and
    /// [`send_owed_to`](Hub::send_owed_to) call this; what a registration passes in
    /// is overwritten.
    pub(super) fn mark_sent(&mut self, now: bool) {
        match &mut self.kind {
            ForwardKind::Ordinary { sent, .. } | ForwardKind::Shared { sent, .. } => *sent = now,
        }
    }
}

/// Why a `$share` delivery is leaving the member it was placed with
/// ([`Hub::reselect_shared`]).
#[derive(Debug, Clone, Copy)]
pub(super) enum Reselect {
    /// The member's node refused it, or cannot be reached: the member is still in
    /// its group and may yet take a later message, so the delivery MOVES — only to
    /// a member that can take it now, since the one it leaves is not tried again.
    /// With none, the publisher hears this last answer.
    Elsewhere(DurableOutcome),
    /// The member is gone: its node said so, its node's membership snapshot no
    /// longer lists it, or its node's routing state was erased. Nothing was stored
    /// and there is nothing to come back to, so the delivery is PLACED again, as a
    /// first placement is — with a member that can take it now, else with one that
    /// cannot be reached, where it is owed and held. A group with nobody left ends
    /// as a publish nobody subscribes to does: acknowledged.
    MemberGone,
}

/// The two things a gated publish can owe a peer an answer for.
#[derive(Debug, Clone)]
pub(super) enum ForwardKind {
    /// An interest-driven fan-out forward ([`PeerMessage::PublishAcked`]): the peer
    /// delivers to whichever of its own subscribers match.
    Ordinary {
        /// A takeover re-route (#784): the publish was already forwarded to a node
        /// that may have stored it before dying, so the receiver skips a session
        /// whose queue already holds it. Retransmits carry the same flag.
        replay: bool,
        /// See [`ForwardObligation::sent`].
        sent: bool,
    },
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
        /// See [`ForwardObligation::sent`].
        sent: bool,
    },
}

impl Hub {
    /// A takeover-window re-delivery of pending publish `id` (ADR 0042 T9):
    /// deliver the frame ONLY to routing state that could have missed the
    /// original fan-out — offline persistent sessions (materialized since) and
    /// clients that RESUMED a session (`session_present = 1`) after the publish.
    /// A session CREATED after the publish — a clean session, or a new persistent
    /// one (`session_present = 0`) — subscribed after it and is owed nothing
    /// (#1011); in plain mode (`MQTTD_DURABLE=0`) that includes a client of a dead
    /// or falsely-dead peer reconnecting here with `clean_session = 0`, since
    /// no session moves between nodes there. Clients online since BEFORE the
    /// publish already received it live; re-sending would duplicate (dups are
    /// legal at `QoS` 1, but a boot-window re-send to a steady subscriber is a
    /// gratuitous one — observed as duplicate bridge forwards). A session this
    /// publish already reached — by the original fan-out or an earlier pass — is
    /// skipped too ([`PendingPublish::reached`]): each such session gets at most one
    /// copy, however many passes the window lasts. Returns a non-`Ok`
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
        // A session this publish already reached — the original fan-out's
        // enqueue, or an earlier pass of this same re-delivery — did not miss
        // it, and a second copy would reach it as a new message with DUP = 0
        // [MQTT-4.4.0-1]. Only what nobody has handed it yet is owed here.
        let reached: HashSet<&ClientId> = p.reached.iter().flat_map(|r| r.iter()).collect();
        let targets: Vec<(ClientId, QoS)> = self
            .table
            .matching_clients(&topic)
            .into_iter()
            .filter(|c| !reached.contains(c))
            // Offline (an inherited session the scan materialised), or attached
            // since the publish by RESUMING a session that existed before it. A
            // session created since — a clean session, or a new persistent one —
            // did not exist when this was published and subscribed after it: it
            // is owed nothing, retained aside, and a copy here is a message from
            // before its time (issue #1011).
            .filter(|c| {
                self.online
                    .get(c)
                    .is_none_or(|o| o.resumed && o.attached_at > since)
            })
            .map(|c| {
                let granted = self.granted_qos(&c, &topic);
                (c, granted)
            })
            .collect();
        drop(reached);
        if targets.is_empty() {
            return DurableOutcome::Ok;
        }
        self.pending_reached(id, targets.iter().map(|(c, _)| c.clone()));
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
                // lane append), which `refuse_pending` enforces. A REPLAY gate
                // (#784): the session may already hold this publish — stored by a
                // node that died before the session moved here — and the append
                // is skipped if it does.
                &AppendGate::Replay(id),
            ));
        }
        all_durable
    }

    /// How many sessions pending publish `id` has been handed to so far (see
    /// [`PendingPublish::reached`]).
    fn reached_count(&self, id: u64) -> usize {
        self.pending_publishes
            .get(id)
            .and_then(|p| p.reached.as_ref())
            .map_or(0, |r| r.len())
    }

    /// Record that pending publish `id` has been handed to `clients` (see
    /// [`PendingPublish::reached`]). Recorded at SUBMIT, like the lane
    /// obligations: a delivery whose append later fails drops the whole entry,
    /// so there is no "recorded but not delivered" state to replay into.
    pub(super) fn pending_reached(
        &mut self,
        id: u64,
        clients: impl ExactSizeIterator<Item = ClientId>,
    ) {
        self.pending_publishes.note_reached(id, clients);
    }

    /// Hold the answer to acked forward `(node, seq)` (issue #1031): its fan-out
    /// matched a subscriber here while this node's takeover is open, so answering
    /// `Reached` now would release the origin's ack although a session this node is
    /// still materialising may be owed the publish too. The live deliveries have
    /// happened; what they reached is recorded, the verdict gate is marked held (its
    /// appends then answer nothing), and [`settle_held_forwards`](Self::settle_held_forwards)
    /// finishes the job once a complete scan started after the hold has landed —
    /// not the whole settle window, which outlives the scan by seconds. A retransmit
    /// of the forward meanwhile finds the gate in flight and is absorbed, as today.
    /// Past [`MAX_HELD_FORWARDS`] the forward is answered `Failed` instead.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn hold_forward_for_takeover(
        &mut self,
        node: NodeId,
        seq: u64,
        topic: String,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: AppProperties,
    ) {
        if self.held_forwards.len() >= MAX_HELD_FORWARDS {
            debug!(origin = %node.0, seq, "held-forward cap reached; answered Failed (#1031)");
            self.finish_peer_verdict(&node, seq, DurableOutcome::Failed, false, true);
            return;
        }
        let reached: FastSet<ClientId> = self
            .ordinary_targets(&topic, None, retain)
            .into_iter()
            .map(|(c, _, _)| c)
            .collect();
        let gate = self
            .remote_append_pending
            .entry((node.clone(), seq))
            .or_insert(RemoteAppendGate {
                awaiting: 0,
                worst: DurableOutcome::Ok,
                congested: false,
                answered: false,
                reached: true,
                held: false,
            });
        gate.reached = true;
        gate.held = true;
        debug!(
            origin = %node.0, seq, topic = %topic, matched = reached.len(),
            "forward matched during an open takeover; answer held until the scan lands (#1031)"
        );
        self.held_forwards.push(HeldForward {
            node,
            seq,
            topic,
            payload,
            qos,
            message_expiry,
            app,
            reached,
            since: Instant::now(),
            ticks: 0,
            after_scan: self.scan_started.0,
            scan_landed: false,
        });
    }

    /// Complete scan number `scan` landed under the ownership that holds now: every
    /// forward held before it started may be released by the next settle pass
    /// (issue #1031).
    pub(super) fn note_scan_landed(&mut self, scan: u64) {
        for h in &mut self.held_forwards {
            if scan > h.after_scan {
                h.scan_landed = true;
            }
        }
    }

    /// Release the forwards held for an open takeover (issue #1031) once a complete
    /// scan started after the hold has landed, or the takeover is no longer open
    /// (then nothing more is coming, and the answer is what it is outside one):
    /// each is re-applied to the sessions materialised since it arrived —
    /// the ones its fan-out did not reach, that existed when it was made (#1011) —
    /// as a replay (#784: a session already holding the publish is skipped), and
    /// then answered with the combined verdict; a re-apply that submitted appends is
    /// answered by its last one. Called wherever the settle pass is, so a window
    /// that never closes answers `Failed` after [`HELD_FORWARD_MAX_TICKS`]: "cannot
    /// say, retry", today's answer to a fan-out that matched nobody mid-takeover.
    pub(super) fn settle_held_forwards(&mut self) {
        if self.held_forwards.is_empty() {
            return;
        }
        let takeover_over = !self.takeover_open();
        let held = std::mem::take(&mut self.held_forwards);
        let mut timed_out = 0usize;
        for mut h in held {
            h.ticks += 1;
            let release = takeover_over || h.scan_landed;
            if !release && h.ticks < HELD_FORWARD_MAX_TICKS {
                self.held_forwards.push(h);
                continue;
            }
            let out = if release {
                self.reapply_held_forward(&h)
            } else {
                timed_out += 1;
                DurableOutcome::Failed
            };
            let key = (h.node.clone(), h.seq);
            let Some(g) = self.remote_append_pending.get_mut(&key) else {
                self.answer_forward(&h.node, h.seq, out.to_verdict_reached(true));
                continue;
            };
            g.held = false;
            g.worst = g.worst.and(out);
            if g.awaiting == 0 {
                if let Some(g) = self.remote_append_pending.remove(&key) {
                    self.answer_forward(&h.node, h.seq, g.worst.to_verdict_reached(true));
                }
            }
        }
        if timed_out > 0 {
            warn!(
                forwards = timed_out,
                "takeover still open after {HELD_FORWARD_MAX_TICKS} settle passes; held \
                 forwards answered Failed rather than held longer (their publishers retry)"
            );
        }
    }

    /// Hand held forward `h` to the sessions it is still owed to (see
    /// [`settle_held_forwards`](Self::settle_held_forwards)).
    fn reapply_held_forward(&mut self, h: &HeldForward) -> DurableOutcome {
        let targets: Vec<(ClientId, QoS)> = self
            .table
            .matching_clients(&h.topic)
            .into_iter()
            .filter(|c| !h.reached.contains(c))
            .filter(|c| {
                self.online
                    .get(c)
                    .is_none_or(|o| o.resumed && o.attached_at > h.since)
            })
            .map(|c| {
                let granted = self.granted_qos(&c, &h.topic);
                (c, granted)
            })
            .collect();
        debug!(
            origin = %h.node.0, seq = h.seq, topic = %h.topic, sessions = targets.len(),
            "takeover closed; a held forward is handed to the sessions materialised since (#1031)"
        );
        let mut all = DurableOutcome::Ok;
        for (c, granted) in targets {
            all = all.and(self.deliver_to_client(
                &c,
                &h.topic,
                &h.payload,
                min_qos(h.qos, granted),
                h.message_expiry,
                &h.app,
                false,
                &AppendGate::Peer {
                    node: h.node.clone(),
                    seq: h.seq,
                    origin: None,
                    replay: true,
                },
            ));
        }
        all
    }

    /// The takeover window closed for this node (an inherited-session scan just
    /// landed): every held pending publish re-delivers **locally** against the
    /// just-materialized subscriptions (each session at most once — see
    /// [`redeliver_pending`](Self::redeliver_pending) — the alternative was an
    /// ack into the void, exhibit ⑥), then re-checks remote
    /// interest via the sweep's re-route path before its ack can release.
    pub(super) fn settle_pending_publishes(&mut self) {
        // A publish that owes a forward to a peer held dead and out of reach is in
        // the work set too (ADR 0087 D1): the obligation is kept, and this pass is
        // where a successor that materialised the subscriber — here, or on another
        // node — is found, as it was when the verdict discarded the obligation and
        // armed the grace instead.
        let unreachable = self.unreachable_peers();
        let held: Vec<u64> = self
            .pending_publishes
            .iter()
            .filter(|(_, p)| {
                p.awaiting_settle || p.reroute_grace.is_some() || owes_ordinary_to(p, &unreachable)
            })
            .map(|(id, _)| *id)
            .collect();
        // The hold clears only when the whole takeover WINDOW is over: one scan
        // is not enough — the group leases reassign for seconds after the death,
        // and a scan that ran before a lease landed saw nothing. Every pass in
        // the window re-delivers to whoever it has not reached yet; the last one
        // releases.
        // Never on a broken mesh (an unreachable-but-alive peer may hold
        // interest this node cannot see — T4 seed 4), and never on TIME alone
        // while the last scan still SKIPPED sessions (0043-P4 exhibit ②: a
        // restarted owner whose lease reassignment outlives the tick window
        // must keep holding, not ack into the void) — `routing_unsettled` is
        // the one observable-state predicate for all of it.
        let window_over = !self.routing_unsettled();
        for id in held {
            let reached_before = self.reached_count(id);
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
            // The re-delivery reached a session nobody had handed this publish: the
            // dead target's subscriber has materialized HERE — inherited by the scan
            // or resumed by its client. That is the evidence the re-route grace
            // waits for, just as a remote re-route is (the sweep ends the grace
            // there too), so the ack need not wait out the rest of the grace (#738:
            // about five seconds of every failover pause). A session CREATED on this
            // node since the publish is not re-delivered to (#1011), so it is never
            // this evidence: it releases no ack early and discharges nothing the
            // unreachable peer is owed.
            if self.reached_count(id) > reached_before {
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    p.reroute_grace = None;
                    // And it is fan-out evidence, exactly as a local match at publish
                    // time is: the ack no longer needs the window to close.
                    p.ack_awaits_settle = false;
                }
                // The subscriber is here now, so what was owed to the unreachable
                // peer for it is discharged ([`reroute`](Self::reroute) says why).
                self.discharge_unreachable(id);
            }
            // The successor may have materialized the subscriber on ANOTHER node
            // and advertised its interest since this publish's original fan-out
            // (which found nothing): forward to it now — a publish that arrived
            // after the death dropped the dead node's interest has no obligation
            // to re-route, so this is where it re-targets.
            let candidates = self.reroute_candidates(id);
            if !candidates.is_empty() {
                // A re-route answers the grace's question exactly as the sweep's
                // re-route does: the subscriber has a new home. Its forward is now
                // the obligation, so the ack waits on that, not on the grace (#738).
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    p.reroute_grace = None;
                }
            }
            self.reroute(id, candidates);
            if window_over {
                if let Some(p) = self.pending_publishes.get_mut(id) {
                    // The window closing ends the replay obligation and, for a
                    // publish whose fan-out never produced evidence, the ack hold
                    // it has been waiting out (issue #613 item 2.1).
                    p.leave_settle_window();
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
    ///
    /// Returns whether any peer has a subscriber for the topic — whether the publish
    /// was owed to a remote subscriber, sent or not.
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
    ) -> bool {
        let retain_broadcasts = retain && self.durable_retained.is_none();
        // A gated QoS ≥ 1 forward is ACKED (ADR 0042 T9, exhibit ⑤): the
        // publisher's ack waits for each target's durability-gated answer, and
        // the sweep retransmits while unanswered. Targets come from the INTEREST
        // map, not the connected-peer map: a link-down (but not dead) peer's
        // subscribers are still owed the publish — the obligation is recorded
        // now, and the frame flows when the link returns. The same holds for a
        // peer membership holds dead (ADR 0087 D1): a verdict keeps the interest,
        // so the publish is owed to it too, until it answers, a successor does,
        // or it is confirmed gone (ADR 0087 D2).
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
        // Whether a peer has a subscriber for it: what "reached somebody" means for
        // `$events/message/dropped` (a retained broadcast to an uninterested peer
        // reaches nobody).
        let wanted_remotely = !interested_nodes.is_empty();
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
                self.send_acked_forward(id, &node, false);
            }
            if !retain_broadcasts {
                self.interest_scratch = interested_nodes;
                self.observe_fanout(fanout_started, peer_visits);
                return wanted_remotely;
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
            return wanted_remotely;
        }
        // Interested peers reached over a link; the rest have none, and an unanswered
        // forward is not held for them (ADR 0087 D9).
        let mut linked_interested = 0usize;
        for (node, peer) in &self.peers {
            peer_visits += 1;
            #[cfg(test)]
            PEER_FANOUT_VISITS.with(|c| c.set(c.get() + 1));
            let interested = InterestIndex::contains(&interested_nodes, node);
            linked_interested += usize::from(interested);
            if gated && interested {
                continue; // already handled (acked or legacy) above
            }
            if !(retain_broadcasts || interested) {
                continue;
            }
            // Issue #504: an ungated QoS 0 forward onto a link that cannot drain is
            // shed and counted rather than queued without bound. A retained
            // broadcast is never shed: peer caches must converge.
            if qos == QoS::AtMostOnce && !retain && super::peer_backlogged(peer) {
                if let Some(m) = &self.metrics {
                    m.publish_dropped("peer-backlog");
                }
                continue;
            }
            if let Some(m) = &self.metrics {
                m.publish_forwarded("subscriber-remote");
            }
            let _ = peer.tx.send(events::origin_frame(
                peer,
                PeerMessage::Publish {
                    topic: topic.to_string(),
                    payload: payload.to_vec(),
                    qos: qos as u8,
                    retain,
                    message_expiry,
                    app: app_to_wire(app),
                },
                app,
            ));
        }
        // A forward nobody answers — `QoS` 0, a Will, an event-derived message — is
        // not held for a peer with matching interest and no link, as it never was;
        // since ADR 0087 D9 it is counted, once per such peer.
        if !gated {
            if let Some(m) = &self.metrics {
                let unreachable = interested_nodes.len().saturating_sub(linked_interested);
                m.publish_dropped_by("peer-unreachable", unreachable as u64);
            }
        }
        self.interest_scratch = interested_nodes;
        self.observe_fanout(fanout_started, peer_visits);
        wanted_remotely
    }

    /// Send pending publish `id` to `node` as a plain, unanswered forward. Not counted
    /// in `publish_forwarded` again: it is the same message to the same node.
    fn reforward_plain(&self, id: u64, node: &NodeId) {
        let (Some(p), Some(peer)) = (self.pending_publishes.get(id), self.peers.get(node)) else {
            return;
        };
        let _ = peer.tx.send(events::origin_frame(
            peer,
            PeerMessage::Publish {
                topic: p.topic.clone(),
                payload: p.payload.to_vec(),
                qos: p.qos as u8,
                retain: p.retain,
                message_expiry: p.message_expiry,
                app: app_to_wire(p.app()),
            },
            p.app(),
        ));
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
        let Some(p) = self.pending_publishes.get(id) else {
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

    /// The peers membership holds `Dead` that have no link and whose routing state
    /// is still being kept ([`VerdictHold::Unreachable`]). Empty in the steady
    /// state — and then nothing that takes this list does any work.
    pub(super) fn unreachable_peers(&self) -> Vec<NodeId> {
        self.held_dead
            .iter()
            .filter(|(_, hold)| matches!(hold, VerdictHold::Unreachable { .. }))
            .map(|(node, _)| node.clone())
            .collect()
    }

    /// Re-route pending publish `id` to `candidates` — peers that announce matching
    /// interest now and were not sent it before — and let those forwards stand in
    /// for what the publish still owes, as an ordinary forward, to a peer held dead
    /// and out of reach.
    ///
    /// That discharge is the durable takeover path as it has always worked: the
    /// subscriber's session moved to the lease's new owner, the new owner announced
    /// its filters, and its answer is the evidence. It is what keeps a real owner
    /// death at the failover pause instead of the whole hold.
    ///
    /// **For 0087-T5 (D8):** the candidates are matched on the TOPIC, not on the
    /// subscriber. Interest does not yet say whether the lease plane owns it, so
    /// this cannot tell the unreachable peer's session, moved to a new owner, from
    /// any other subscription that happens to match — a monitor on `#` that
    /// reconnects on another node is a candidate too, and so is a matching session
    /// that attaches on this node (the settle pass discharges on that as well). A
    /// subscriber on a peer falsely held dead therefore still loses a publish when
    /// ANY newly matching interest appears, anywhere, before the peer's link is
    /// back — half a second, with the dialer of D5. With D8 the discharge applies
    /// to interest the lease plane owns, announced by its new owner, and to
    /// nothing else.
    pub(super) fn reroute(&mut self, id: u64, candidates: Vec<NodeId>) {
        if candidates.is_empty() {
            return;
        }
        self.discharge_unreachable(id);
        for node in candidates {
            self.send_acked_forward(id, &node, true);
        }
    }

    /// Drop the ordinary forward obligations pending publish `id` holds toward peers
    /// held dead and out of reach. See [`reroute`](Self::reroute).
    ///
    /// Only where a session can move between nodes — with a durable plane. Without
    /// one a session lives and dies on the node that hosts it (the store is local:
    /// nothing is inherited, nothing re-homed), so a newly matching interest is
    /// never the unreachable peer's subscriber under a new owner; it is somebody
    /// else, and letting its forward stand in for what the peer is owed can only
    /// lose the publish for a subscriber on a peer that was not dead. There the
    /// obligation stays until the peer answers or is confirmed gone, and the
    /// newcomer is still sent the publish.
    pub(super) fn discharge_unreachable(&mut self, id: u64) {
        if self.durable_plane.is_none() {
            return;
        }
        let unreachable = self.unreachable_peers();
        if unreachable.is_empty() {
            return;
        }
        let Some(p) = self.pending_publishes.get_mut(id) else {
            return;
        };
        let seqs: Vec<u64> = p
            .awaiting
            .iter()
            .filter(|(_, o)| {
                matches!(o.kind, ForwardKind::Ordinary { .. }) && unreachable.contains(&o.node)
            })
            .map(|(seq, _)| *seq)
            .collect();
        for seq in seqs {
            p.awaiting.remove(&seq);
            self.forward_index.remove(&seq);
        }
    }

    /// Send every forward of one kind — `$share` deliveries if `shared`, ordinary
    /// forwards if not — that this node owes `node` and has not had answered, under
    /// the seq each was recorded with: a forward the peer did apply before its link
    /// went is recognised as a repeat and answered again, not applied again
    /// (issue #648), and one it never saw is applied once.
    ///
    /// **In the order the publishes were accepted** (the pending table is ordered
    /// by publish id). That, with [`register_forward`](Self::register_forward)
    /// sending nothing to a linked peer before this has run for its link, is what
    /// keeps one publisher's messages in publish order for a subscriber across an
    /// outage (ADR 0087; [MQTT-4.6.0-6] for a non-shared subscription).
    ///
    /// Called when the peer's first interest snapshot (ordinary) and first `$share`
    /// snapshot (shared) arrive on a new link, and by the sweep — ordinary forwards
    /// only — for a peer that has stayed silent on a link a sweep interval old
    /// ([`sweep_pending_forwards`](Self::sweep_pending_forwards)).
    pub(super) fn send_owed_to(&mut self, node: &NodeId, shared: bool) {
        let Some(peer) = self.peers.get(node) else {
            return;
        };
        for (id, p) in self.pending_publishes.iter_mut() {
            let mut sent = false;
            for (seq, obligation) in &p.awaiting {
                if &obligation.node != node
                    || matches!(obligation.kind, ForwardKind::Shared { .. }) != shared
                {
                    continue;
                }
                let _ = peer.tx.send(forward_frame(
                    *id,
                    p,
                    *seq,
                    obligation,
                    peer.proto,
                    peer.wants_origin,
                ));
                sent = true;
            }
            if sent {
                for obligation in p.awaiting.values_mut() {
                    if &obligation.node == node
                        && matches!(obligation.kind, ForwardKind::Shared { .. }) == shared
                    {
                        obligation.mark_sent(true);
                    }
                }
                // Sent now: not overdue at the sweep a moment from now (issue #644).
                p.forwarded_after_ms =
                    u32::try_from(p.created_at.elapsed().as_millis()).unwrap_or(u32::MAX);
            }
        }
    }

    /// How many publishes are held for a peer that cannot be reached: gated
    /// publishes with a forward outstanding toward a node that has no link —
    /// its link is down, or membership holds it dead (ADR 0087 D1, D9). The
    /// pending table is walked only when some node this hub knows of has no link.
    pub(super) fn publishes_held_for_unreachable(&self) -> usize {
        let unlinked = |node: &NodeId| !self.peers.contains_key(node);
        if !self.interest.any_node(unlinked) && !self.remote_shared.keys().any(unlinked) {
            return 0;
        }
        self.pending_publishes
            .values()
            .filter(|p| p.awaiting.values().any(|o| unlinked(&o.node)))
            .count()
    }

    /// Forget the forward seqs of a pending publish that is leaving the table.
    /// Every seq in the index is a key of its entry's `awaiting` (they are written
    /// together in [`register_forward`](Self::register_forward)), so this is the
    /// whole of what the entry has there — without walking the index, which at the
    /// table's bound is tens of thousands of seqs per evicted entry.
    fn forget_forwards(&mut self, entry: &PendingPublish) {
        for seq in entry.awaiting.keys() {
            self.forward_index.remove(seq);
        }
    }

    /// Send (or re-send, on re-route) one acked forward of pending publish `id` to
    /// `node`, recording the obligation (ADR 0042 T9, exhibit ⑤). `replay` marks a
    /// takeover re-route (#784).
    pub(super) fn send_acked_forward(&mut self, id: u64, node: &NodeId, replay: bool) {
        self.register_forward(
            id,
            ForwardObligation {
                node: node.clone(),
                kind: ForwardKind::Ordinary {
                    replay,
                    sent: false,
                },
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
    ///
    /// And only when the link is OPEN for that kind of forward: the peer has
    /// announced on it — its interest snapshot for an ordinary forward, its `$share`
    /// snapshot for a shared delivery — or, for ordinary forwards only, the sweep
    /// has sent it everything it was owed after a sweep interval of silence
    /// (ADR 0087). A link that has just come back
    /// still has everything recorded during the outage to carry, and that goes
    /// out, oldest first, when the peer announces
    /// ([`send_owed_to`](Self::send_owed_to)); a forward registered in the instant
    /// between was sent at once and overtook all of it, so a subscriber saw one
    /// publisher's `QoS` 1 messages out of publish order. Now it is recorded
    /// behind them. For a `$share` delivery the snapshot is also what says whether
    /// the named member is still there. In the steady state every linked peer has
    /// announced, and this is the one peer lookup it always was.
    pub(super) fn register_forward(&mut self, id: u64, mut obligation: ForwardObligation) {
        self.forward_seq += 1;
        let seq = self.forward_seq;
        let Some(p) = self.pending_publishes.get_mut(id) else {
            return;
        };
        let ready = self.peers.get(&obligation.node).filter(|peer| {
            if matches!(obligation.kind, ForwardKind::Shared { .. }) {
                peer.shared_synced
            } else {
                peer.forwards_open
            }
        });
        if let Some(peer) = ready {
            let _ = peer.tx.send(forward_frame(
                id,
                p,
                seq,
                &obligation,
                peer.proto,
                peer.wants_origin,
            ));
        }
        obligation.mark_sent(ready.is_some());
        // Issue #480. Counted where the obligation is RECORDED rather than where
        // the frame is written, so a forward to a link that is momentarily down
        // still counts: the sweep will send it when the link returns, and it
        // crossed a node boundary either way. Counting at the write would
        // undercount exactly during the link flaps worth investigating.
        if let Some(m) = &self.metrics {
            m.publish_forwarded(match obligation.kind {
                ForwardKind::Shared { .. } => "shared-remote",
                ForwardKind::Ordinary { .. } => "subscriber-remote",
            });
        }
        debug!(publish = id, seq, target = %obligation.node.0, "forward obligation recorded");
        p.awaiting.insert(seq, obligation);
        // The overdue clock restarts at every registration (issue #644).
        p.forwarded_after_ms =
            u32::try_from(p.created_at.elapsed().as_millis()).unwrap_or(u32::MAX);
        self.forward_index.insert(seq, id);
    }

    /// Register a `QoS` 1 publish whose acknowledgement is gated on cluster-wide
    /// durability (ADR 0042 T9). At either bound the OLDEST entry is dropped
    /// loudly — its ack withheld, so its publisher retries.
    ///
    /// Issue #613 item 2.4 once proposed refusing the ARRIVING publish instead.
    /// It was withdrawn when this merged with the byte-bounded table: for a
    /// v3.1.1 publisher a refusal is a connection CLOSE (there is no reason
    /// byte), which is a reconnect storm where eviction leaves an ack pending on
    /// a live connection; at a 65,536-entry bound eviction is already rare; and
    /// preferring already-answered victims would have put an O(table) scan on
    /// exactly the overload path. What survives of it is honesty: an evicted
    /// entry that item 2.1 had already answered is counted as
    /// `pending-cap-replay`, never as a withheld ack.
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
        // Evict until the newcomer fits BOTH bounds (3571682). A loop, not an
        // `if`: under the byte bound one large publish can need several small
        // ones gone, and stopping after one eviction would admit it over the
        // bound anyway.
        let cost = pending_cost(topic, payload);
        while self.pending_publishes.is_full_for(cost) {
            let Some((old_id, old)) = self.pending_publishes.pop_first() else {
                break;
            };
            self.forget_forwards(&old);
            // Issue #613 item 2.1 makes a second kind of entry possible: one
            // already ANSWERED and kept only for the settle window's replay. The
            // oldest-first policy is unchanged, but dropping such an entry
            // withholds nothing — its sender was consumed by the answer — so it
            // must not be reported, or counted, as a withheld ack. What it does
            // lose is the re-delivery to a session materialised during the
            // window, which is said out loud rather than hidden in the other
            // number.
            if old.ack_released() {
                warn!(
                    publish = old_id,
                    topic = %old.topic,
                    cap = PENDING_PUBLISH_CAP,
                    max_bytes = PENDING_PUBLISH_MAX_BYTES,
                    "pending-publish bound: dropped the OLDEST entry, which was already \
                     ACKNOWLEDGED and held only for the settle window's replay; no \
                     publisher lost an answer, but a session materialised during this \
                     window will not receive it (issue #613 item 2.1)"
                );
                if let Some(m) = &self.metrics {
                    m.publish_dropped("pending-cap-replay");
                }
                continue;
            }
            warn!(
                topic = %old.topic,
                cap = PENDING_PUBLISH_CAP,
                max_bytes = PENDING_PUBLISH_MAX_BYTES,
                "pending-publish bound: dropped the OLDEST unacknowledged publish \
                 (ack withheld; its publisher retries — ADR 0042 T9)"
            );
            if let Some(m) = &self.metrics {
                m.publish_dropped("pending-cap");
            }
            // A message the hub gated during the drain has nobody to retry it, and the
            // drain barrier no longer waits for it: the drain reports how many. (A
            // client's evicted publish is withheld, and its publisher retries.)
            if self.drain_gated.contains(&old_id) {
                self.drain_evicted += 1;
            }
        }
        self.publish_ids += 1;
        let id = self.publish_ids;
        self.pending_publishes.insert(
            id,
            PendingPublish {
                cost: 0, // charged by `PendingTable::insert`
                done: Some(done),
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain,
                message_expiry,
                app: (!app.is_empty()).then(|| Box::new(app.clone())),
                awaiting: HashMap::default(),
                stored: false,
                acked_nodes: Vec::new(),
                awaiting_retained: false,
                local_done: false,
                appends_outstanding: 0,
                created_at: Instant::now(),
                forwarded_after_ms: 0,
                reroute_grace: None,
                reached: None,
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
        id
    }

    /// Mark a pending publish RELAXED (ADR 0072): its ack releases at
    /// `local_done` instead of waiting for the durability obligations.
    pub(super) fn pending_mark_relaxed(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(id) {
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
        if let Some(p) = self.pending_publishes.get_mut(id) {
            p.ack_awaits_settle = false;
        }
    }

    /// The local fan-out obligation resolved OK (durable appends included).
    pub(super) fn pending_local_done(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(id) {
            p.local_done = true;
        }
        self.try_complete_pending(id);
    }

    /// The peer-bus proto this link negotiated; with no link, the one the node's
    /// last link negotiated (kept until the node's routing state is erased); and
    /// [`peer::PROTO_MIN`](mqtt_cluster::peer::PROTO_MIN) for a node never linked —
    /// fail safe toward the OLD frame, which every peer can decode (0041-T12).
    ///
    /// The middle case is what makes a `$share` delivery to a member behind a lost
    /// link an obligation (ADR 0087 D1): answering "proto 6" for it chose the
    /// fire-and-forget frame, which a missing link drops, with the publisher
    /// acknowledged all the same. Nothing is sent on this answer — a frame is built
    /// against the live link's own proto when there is a link to send it on.
    pub(super) fn peer_proto(&self, node: &NodeId) -> u32 {
        self.peers
            .get(node)
            .map(|p| p.proto)
            .or_else(|| self.known_peer_protos.get(node).copied())
            .unwrap_or(mqtt_cluster::peer::PROTO_MIN)
    }

    /// Drop a pending publish, WITHHOLDING its acknowledgement (the sender side
    /// of fail-closed: the publisher's connection sees no ack and retries).
    pub(super) fn drop_pending(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.remove(id) {
            self.forget_forwards(&p);
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
        let Some(mut p) = self.pending_publishes.remove(id) else {
            return;
        };
        self.forget_forwards(&p);
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

    /// Whether an acked forward that just ARRIVED is a repeat of one this node has
    /// already applied (issue #648) — in which case it has been dealt with here and
    /// the caller must not apply it again.
    ///
    /// The sender retransmits an unanswered forward under the SAME seq on every
    /// sweep once it is overdue, and that is legitimate: the answer can be late (a
    /// durable append slower than one [`SESSION_SWEEP_INTERVAL`] under load),
    /// cross the retransmission in flight, or be lost with the link. Applying every
    /// copy appended the message to the subscriber's session log once per copy, and
    /// replay then delivered each as a first delivery with `DUP = 0`
    /// [MQTT-4.4.0-1]. A repeat is keyed on `(origin, seq)` — the identity the frame
    /// already carries, so no wire change — and is:
    ///
    /// - **still being applied** (its lane appends have not landed): dropped, since
    ///   the first copy's completion sends the answer — the `(origin, seq)` verdict
    ///   aggregate is exactly the proof that it will. Checked against that
    ///   aggregate directly, so it holds even after the window has forgotten the seq;
    /// - **already answered**: answered AGAIN with the recorded verdict, never
    ///   re-applied — the sender needs an answer to retire the obligation, and the
    ///   first one is what it missed.
    ///
    /// `fingerprint` guards the one way the key could name two messages: an origin
    /// that restarted and re-issued a seq. A build with this change starts its seq
    /// space at a random point ([`initial_forward_seq`]) so that does not happen;
    /// an OLDER origin (a rolling upgrade, ADR 0039) restarts at 1, and a seq whose
    /// topic and payload differ from what the window remembers is a new message and
    /// is applied. What remains is an older origin re-issuing a remembered seq with
    /// the SAME topic and payload within the window — treated as the repeat it is
    /// indistinguishable from.
    pub(super) fn forward_is_repeat(&mut self, node: &NodeId, seq: u64, fingerprint: u64) -> bool {
        let seen = self.forward_windows.get(node).and_then(|w| w.seen(seq));
        match seen {
            Some(seen) if seen.fingerprint == fingerprint => match seen.verdict {
                Some(verdict) => {
                    debug!(origin = %node.0, seq, "repeated forward re-answered, not re-applied");
                    self.answer_forward(node, seq, verdict);
                    return true;
                }
                None if self.forward_in_flight(node, seq) => {
                    debug!(origin = %node.0, seq, "repeated forward still being applied; dropped");
                    return true;
                }
                // Recorded, never answered, nothing in flight: no path leaves a
                // forward in this state (the verdict is answered or aggregated in
                // the same dispatch that applied it). Apply rather than go silent —
                // a duplicate is recoverable, a forward nobody answers is not.
                None => {}
            },
            // Forgotten by the window while its appends are still running.
            None if self.forward_in_flight(node, seq) => return true,
            // A new forward — or the same seq naming a DIFFERENT message, from an
            // origin that restarted: applied.
            Some(_) | None => {}
        }
        if let Some(window) = self.forward_windows.get_mut(node) {
            window.record(seq, fingerprint);
        } else {
            let mut window = ForwardWindow::default();
            window.record(seq, fingerprint);
            self.forward_windows.insert(node.clone(), window);
        }
        false
    }

    /// Whether forward `(node, seq)` still has lane appends outstanding here — its
    /// verdict aggregate exists, so its answer is still to come.
    fn forward_in_flight(&self, node: &NodeId, seq: u64) -> bool {
        self.remote_append_pending
            .contains_key(&(node.clone(), seq))
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
    ///
    /// The verdict is also recorded in the origin's [`ForwardWindow`] BEFORE the
    /// link is consulted (issue #648): an answer lost with the link is exactly the
    /// one whose retransmission must be re-answered rather than re-applied.
    pub(super) fn answer_forward(&mut self, node: &NodeId, seq: u64, verdict: ForwardVerdict) {
        if let Some(w) = self.forward_windows.get_mut(node) {
            w.answered(seq, verdict);
        }
        let Some(peer) = self.peers.get(node) else {
            return; // link gone: the sender's sweep will retransmit
        };
        // A peer older than proto 11 cannot decode `Reached`; to it, it is `Stored`.
        // And one older than proto 13 does not know "the named `$share` member is
        // gone" (ADR 0087): it would read the code as an answer it cannot interpret
        // and withhold, closing its publisher's connection. It is told what is true
        // and what it already re-selects on — nothing was stored here, in the
        // brownout's code — so its delivery moves to another member of the group,
        // and only with none left does its publisher hear `0x97` and retry.
        let verdict = match verdict {
            ForwardVerdict::Reached if peer.proto < PROTO_FORWARD_REACHED => ForwardVerdict::Stored,
            ForwardVerdict::Refused { code }
                if code == mqtt_cluster::peer::REFUSAL_SHARED_MEMBER_GONE
                    && peer.proto < mqtt_cluster::peer::PROTO_SHARED_MEMBER_GONE =>
            {
                ForwardVerdict::Refused {
                    code: PublishRefusal::Brownout.wire_code(),
                }
            }
            verdict => verdict,
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
                // Fan-out evidence, as a local match is (#738): the receiver stored it
                // for a subscriber, so the ack need not wait for the window to close.
                if verdict == ForwardVerdict::Reached {
                    p.ack_awaits_settle = false;
                }
                if !p.acked_nodes.iter().any(|a| a == node) {
                    p.acked_nodes.push(node.clone());
                }
                self.try_complete_pending(id);
            }
            // The peer no longer has the member this `$share` delivery named (ADR
            // 0087): nothing was stored there, and the delivery is placed with
            // another member of the group. Not a refusal said to anyone — the code
            // has no `PublishRefusal` — so it arrives in this arm.
            DurableOutcome::Failed
                if matches!(obligation.kind, ForwardKind::Shared { .. })
                    && verdict
                        == (ForwardVerdict::Refused {
                            code: mqtt_cluster::peer::REFUSAL_SHARED_MEMBER_GONE,
                        }) =>
            {
                debug!(publish = id, seq, from = %node.0, "shared member gone; placing again");
                self.reselect_shared(id, obligation, Reselect::MemberGone);
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
                    self.reselect_shared(
                        id,
                        obligation,
                        Reselect::Elsewhere(DurableOutcome::Refused(r)),
                    );
                }
                // Gated by the hub itself during the drain, with nobody to retry it:
                // the peer refused it before any side effect, so send it again as a
                // plain forward, which it delivers live with its durable copy a
                // counted drop — what it would have done had the drain not gated it.
                ForwardKind::Ordinary { .. } if self.drain_gated.contains(&id) => {
                    self.reforward_plain(id, node);
                    self.try_complete_pending(id);
                }
                ForwardKind::Ordinary { .. } => self.refuse_pending(id, r),
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    /// The sweep-tick half of acked forwards (ADR 0042 T9, exhibit ⑤): retransmit
    /// unanswered forwards whose target link is up (same seq — duplicates are
    /// legal at `QoS` 1), and drive takeover re-routes: a forward whose target is
    /// held DEAD and out of reach re-forwards to whichever peers now advertise
    /// matching interest (the dead owner's successor, once it materializes
    /// inherited sessions — exhibit ⑥). When the target is confirmed gone (or,
    /// with a durable plane, its [`REROUTE_GRACE_TICKS`] countdown runs out first)
    /// the obligation is discharged ([`Hub::erase_peer`]), counted, and the ack
    /// releases.
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
        // Peers held dead and out of reach (ADR 0087 D1): empty in the steady state,
        // and then the test below is one `is_empty` per entry.
        let unreachable = self.unreachable_peers();
        let ids: Vec<(u64, bool)> = self
            .pending_publishes
            .iter()
            .filter_map(|(id, p)| {
                // Measured from the LAST forward sent, not from the publish's
                // creation: a re-route on an old publish is not overdue the moment
                // it is sent (issue #644).
                let last_forward =
                    p.created_at + Duration::from_millis(u64::from(p.forwarded_after_ms));
                let overdue = !p.awaiting.is_empty()
                    && now.duration_since(last_forward) >= super::SESSION_SWEEP_INTERVAL;
                (overdue || p.reroute_grace.is_some() || owes_ordinary_to(p, &unreachable))
                    .then_some((*id, overdue))
            })
            .collect();
        // Linked peers that have not announced on their link, have been linked for
        // a sweep interval, and are owed an overdue forward. See below.
        let mut silent: Vec<NodeId> = Vec::new();
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
                // A linked peer that has not announced on this link has been sent
                // nothing on it ([`register_forward`](Self::register_forward)).
                //
                // A `$share` delivery waits for its membership snapshot, however
                // long: only that says whether the named member is still there, and
                // a peer that has not sent it cannot yet say who is on it — it
                // could only answer `Failed`, withholding an acknowledgement that
                // the snapshot would have let this node place again. Every build
                // sends it within ten ticks of starting.
                //
                // An ORDINARY forward still goes to a peer that stays silent, as it
                // always has — a peer that restarted and cannot yet say what it
                // routes answers with a failure, and the publisher retries rather
                // than waiting on it — but only once the LINK has been there for a
                // sweep interval (what was held across the outage is overdue the
                // moment the link forms, and the snapshot is the first thing a
                // peer says on it), and never one forward ahead of the older ones
                // recorded beside it: EVERYTHING owed to the peer goes, oldest
                // first, after this pass — once: the link is then open, what is
                // registered later is sent as it is registered, and the sweep
                // retransmits on it as on any other.
                if matches!(obligation.kind, ForwardKind::Shared { .. }) {
                    if !peer.shared_synced {
                        continue;
                    }
                } else if !peer.forwards_open {
                    if now.duration_since(peer.linked_at) >= super::SESSION_SWEEP_INTERVAL
                        && !silent.contains(&obligation.node)
                    {
                        silent.push(obligation.node.clone());
                    }
                    continue;
                }
                let Some(p) = self.pending_publishes.get(id) else {
                    continue;
                };
                // The SAME frame the original forward sent, under the outstanding
                // seq — the key the receiver recognises a repeat by (issue #648)
                // — and byte-identical, so its content fingerprint matches: built by the shared
                // constructor so a retransmitted copy can never drift semantically
                // from the first send — and, since 0041-T12, so a SHARED obligation
                // retransmits `SharedDeliverAcked` rather than a fan-out
                // `PublishAcked` that would deliver to the wrong subscribers.
                let _ = peer.tx.send(forward_frame(
                    id,
                    p,
                    *seq,
                    obligation,
                    peer.proto,
                    peer.wants_origin,
                ));
            }
            // Re-route when a forward's target is held dead and out of reach, or
            // was (the grace `erase_peer` leaves behind): a peer that announces the
            // interest now is its successor.
            let Some(p) = self.pending_publishes.get(id) else {
                continue;
            };
            let grace = p.reroute_grace;
            if grace.is_none() && !owes_ordinary_to(p, &unreachable) {
                continue;
            }
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
                self.reroute(id, candidates);
                continue;
            }
            // Nothing to re-route to. A publish that is only OWED waits for its
            // peer or for the hold to end; one in its grace counts the tick.
            let Some(grace) = grace else {
                continue;
            };
            let Some(p) = self.pending_publishes.get(id) else {
                continue;
            };
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
                // takeover scan or its client RESUMING the session here
                // (`session_present = 1`; a session created here since the
                // publish is owed nothing, #1011) — after this publish's
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
        for node in silent {
            self.send_owed_to(&node, false);
            if let Some(peer) = self.peers.get_mut(&node) {
                peer.forwards_open = true;
            }
        }
    }
}

/// Whether pending publish `p` has an ordinary forward outstanding toward one of
/// `peers`. With no such peers — the steady state — it reads nothing of `p`.
pub(super) fn owes_ordinary_to(p: &PendingPublish, peers: &[NodeId]) -> bool {
    !peers.is_empty()
        && p.awaiting
            .values()
            .any(|o| matches!(o.kind, ForwardKind::Ordinary { .. }) && peers.contains(&o.node))
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
    nodes: FastSet<NodeId>,
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
    pub(super) fn nodes_matching(&self, topic: &str) -> FastSet<NodeId> {
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

    /// Whether any node that has advertised interest satisfies `pred`.
    pub(super) fn any_node(&self, pred: impl Fn(&NodeId) -> bool) -> bool {
        self.nodes.iter().any(pred)
    }
}

#[cfg(test)]
mod interest_index_tests {
    use super::*;

    /// The linear scan the index replaced: every node whose advertised filters
    /// contain one matching `topic`. The reference the corpus is checked against.
    fn reference(topic: &str, per_node: &FastMap<NodeId, Vec<String>>) -> FastSet<NodeId> {
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
        let mut per_node: FastMap<NodeId, Vec<String>> = HashMap::default();
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
        let mut per_node: FastMap<NodeId, Vec<String>> = HashMap::default();
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
            let fast: FastSet<NodeId> = scratch
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
        assert_eq!(idx.nodes_matching("b/c"), FastSet::from_iter([n.clone()]));
        idx.replace(n.clone(), vec!["a/#".into()]);
        assert!(
            idx.nodes_matching("b/c").is_empty(),
            "a filter absent from the new snapshot must stop matching"
        );
        assert_eq!(idx.nodes_matching("a/x"), FastSet::from_iter([n]));
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
            FastSet::from_iter([n1.clone(), n2.clone()])
        );
        idx.remove(&n1);
        assert_eq!(idx.nodes_matching("s/x/t"), FastSet::from_iter([n2]));
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
                announced(hub, &node);
                PeerLink {
                    node,
                    rx,
                    _ctl_rx: ctl_rx,
                }
            })
            .collect()
    }

    /// Mark `node`'s link as one the peer has announced on — its interest and its
    /// `$share` membership — which is the state of every link in a running
    /// cluster a moment after it forms, and what a forward to it waits for.
    fn announced(hub: &mut Hub, node: &NodeId) {
        let peer = hub.peers.get_mut(node).expect("the peer is linked");
        peer.interest_synced = true;
        peer.shared_synced = true;
        peer.forwards_open = true;
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

    fn register(hub: &mut Hub, topic: &str) -> (u64, oneshot::Receiver<PublishOutcome>) {
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
        h.pending_local_done(id);
        assert!(
            h.pending_publishes.contains_key(id),
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
            !h.pending_publishes.contains_key(id),
            "the entry must retire once the window has no further claim on it"
        );
        assert!(
            matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
            "the ack held for the window must be released when it closes"
        );
    }

    // ---- #738: a failover's acks wait only for evidence, not for the window ----

    /// One peer link at an explicit proto, advertising `filter`, on which the peer
    /// has announced.
    fn attach_peer_at(hub: &mut Hub, name: &str, proto: u32, filter: &str) -> PeerLink {
        let link = link_peer_at(hub, name, proto);
        hub.interest
            .replace(link.node.clone(), vec![filter.to_string()]);
        announced(hub, &link.node);
        link
    }

    /// One peer link at an explicit proto on which the peer has said NOTHING yet:
    /// a link that has just formed, or formed again. What the peer announced on an
    /// earlier link is whatever the hub still holds.
    fn link_peer_at(hub: &mut Hub, name: &str, proto: u32) -> PeerLink {
        let node = NodeId(name.into());
        let (tx, rx) = mpsc::unbounded_channel();
        let (ctl, ctl_rx) = mpsc::unbounded_channel();
        hub.peer_connected(
            node.clone(),
            0,
            tx,
            ctl,
            None,
            proto,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );
        PeerLink {
            node,
            rx,
            _ctl_rx: ctl_rx,
        }
    }

    /// The acked forwards `link` was sent since it was last read, in order: each
    /// frame's seq, and the named member for a `$share` delivery.
    fn forwards_sent(link: &mut PeerLink) -> Vec<(u64, Option<String>)> {
        std::iter::from_fn(|| link.rx.try_recv().ok())
            .filter_map(|frame| match frame {
                PeerMessage::PublishAcked { seq, .. }
                | PeerMessage::PublishAckedTagged { seq, .. } => Some((seq, None)),
                PeerMessage::SharedDeliverAcked { seq, client, .. } => Some((seq, Some(client))),
                _ => None,
            })
            .collect()
    }

    /// A publish whose only route is a forward, registered while the view is
    /// unsettled (so its ack is held), with the forward sent and its seq returned.
    fn held_forward(h: &mut Hub, peer: &NodeId) -> (u64, u64, oneshot::Receiver<PublishOutcome>) {
        let (id, rx) = register(h, "t/x");
        h.send_acked_forward(id, peer, false);
        h.pending_local_done(id);
        let seq = *h.pending_publishes[&id]
            .awaiting
            .keys()
            .next()
            .expect("the forward is outstanding");
        (id, seq, rx)
    }

    /// #738: during a takeover window a forward answered `Reached` is evidence the
    /// message reached a subscriber, so the ack is released at once; one answered
    /// `Stored` is not (a settled receiver says `Stored` for a fan-out that matched
    /// nobody — issue #294), so it keeps waiting for the window to close.
    #[test]
    fn a_reached_forward_releases_a_held_ack_and_a_stored_one_does_not() {
        for (verdict, released) in [
            (ForwardVerdict::Reached, true),
            (ForwardVerdict::Stored, false),
        ] {
            let mut h = hub();
            h.set_cluster_configured();
            assert!(h.routing_unsettled(), "the rig must be unsettled");
            let peer = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            let (_id, seq, mut rx) = held_forward(&mut h, &peer.node);
            h.forward_answered(&peer.node, seq, verdict);
            assert_eq!(
                matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
                released,
                "{verdict:?}: released must be {released}"
            );
        }
    }

    /// #738: `Reached` goes on the wire only to a peer that can decode it. A
    /// proto-10 peer hears `Stored`, a proto-6 peer `PublishAck { ok: true }`.
    #[test]
    fn a_reached_answer_is_downgraded_for_an_older_peer() {
        let mut h = hub();
        let mut links: Vec<_> = [
            ("new", PROTO_FORWARD_REACHED),
            ("ten", PROTO_FORWARD_REACHED - 1),
            ("six", PROTO_FORWARD_VERDICT - 1),
        ]
        .into_iter()
        .map(|(name, proto)| attach_peer_at(&mut h, name, proto, "t/x"))
        .collect();
        for link in &links {
            h.answer_forward(&link.node, 5, ForwardVerdict::Reached);
        }
        let sent: Vec<_> = links
            .iter_mut()
            .map(|l| l.rx.try_recv().expect("an answer was sent"))
            .collect();
        assert!(matches!(
            sent[0],
            PeerMessage::PublishVerdict {
                seq: 5,
                verdict: ForwardVerdict::Reached
            }
        ));
        assert!(matches!(
            sent[1],
            PeerMessage::PublishVerdict {
                seq: 5,
                verdict: ForwardVerdict::Stored
            }
        ));
        assert!(matches!(
            sent[2],
            PeerMessage::PublishAck { seq: 5, ok: true }
        ));
    }

    /// #738: a publish whose forward target died waits out the re-route grace for
    /// the subscriber to reappear. When the takeover scan materializes it HERE and
    /// the settle pass re-delivers to it, that is the answer: the grace and the ack
    /// hold both end, instead of the ack waiting for the grace and the window.
    #[tokio::test]
    async fn a_takeover_redelivery_that_reaches_a_session_ends_the_grace_and_the_hold() {
        for materialized in [true, false] {
            let mut h = hub();
            h.set_cluster_configured();
            let (id, _rx) = register(&mut h, "t/x");
            h.pending_publishes.get_mut(id).unwrap().reroute_grace = Some(REROUTE_GRACE_TICKS);
            let sessions = if materialized {
                vec![mqtt_storage::ScannedSession::new(
                    ClientId("moved".into()),
                    vec![mqtt_core::Subscription {
                        filter: "t/x".into(),
                        max_qos: QoS::AtLeastOnce,
                        no_local: false,
                        sub_id: None,
                    }],
                    None,
                )]
            } else {
                Vec::new()
            };
            h.inherit_sessions(sessions, true);
            let p = &h.pending_publishes[&id];
            assert_eq!(
                p.reroute_grace.is_none(),
                materialized,
                "materialized={materialized}: the grace ends only when the re-delivery reached someone"
            );
            assert_eq!(
                !p.ack_awaits_settle, materialized,
                "materialized={materialized}: the ack hold ends only on that evidence"
            );
        }
    }

    /// #738: the settle pass re-routing a died-target publish to a peer that now
    /// advertises the topic ends the grace, as the sweep's own re-route does; the
    /// ack then waits on that forward's answer, not on the grace running out.
    #[test]
    fn a_takeover_reroute_ends_the_grace() {
        for advertised in [true, false] {
            let mut h = hub();
            h.set_cluster_configured();
            let filter = if advertised { "t/x" } else { "other" };
            let mut peer = attach_peer_at(&mut h, "succ", PROTO_FORWARD_REACHED, filter);
            let (id, _rx) = register(&mut h, "t/x");
            h.pending_publishes.get_mut(id).unwrap().reroute_grace = Some(REROUTE_GRACE_TICKS);
            h.settle_pending_publishes();
            assert_eq!(
                h.pending_publishes[&id].reroute_grace.is_none(),
                advertised,
                "advertised={advertised}: the grace ends only on a re-route"
            );
            assert_eq!(
                matches!(peer.rx.try_recv(), Ok(PeerMessage::PublishAcked { .. })),
                advertised,
                "advertised={advertised}: the re-route forward is sent"
            );
        }
    }

    // ---- issue #973 / ADR 0087 D1: a peer held dead is unreachable, not erased ----

    /// One sweep tick as the hub runs it: the holds first, then the pending table.
    fn sweep_tick(h: &mut Hub) {
        h.sweep_verdict_holds();
        h.sweep_pending_forwards();
    }

    /// Whether `link` was sent an acked forward since it was last read.
    fn was_sent_a_forward(link: &mut PeerLink) -> bool {
        std::iter::from_fn(|| link.rx.try_recv().ok()).any(|frame| {
            matches!(
                frame,
                PeerMessage::PublishAcked { .. }
                    | PeerMessage::PublishAckedTagged { .. }
                    | PeerMessage::SharedDeliverAcked { .. }
            )
        })
    }

    /// The gap of issue #973, closed: a publish toward a peer is an obligation that
    /// holds the publisher's acknowledgement whether the peer's link is merely down
    /// or the peer has been declared dead. The forward is derived from the interest
    /// table, and a verdict no longer empties it. (This was
    /// `a_publish_for_a_peer_declared_dead_owes_it_nothing_where_a_link_down_peer_is_owed`,
    /// which pinned the dead arm as owing nothing.)
    #[test]
    fn a_publish_for_a_peer_declared_dead_is_owed_to_it_as_it_is_to_a_link_down_peer() {
        for dead in [false, true] {
            let mut h = hub();
            let peer = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            if dead {
                h.peer_dead(&peer.node);
            } else {
                h.peer_disconnected(&peer.node, 0);
            }
            assert!(h.interest.has_node(&peer.node), "dead={dead}");

            let (id, mut rx) = register(&mut h, "t/x");
            fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(id));
            h.pending_local_done(id);
            assert!(
                h.pending_publishes
                    .get(id)
                    .is_some_and(|p| p.awaiting.values().any(|o| o.node == peer.node)),
                "dead={dead}: a forward obligation toward the peer"
            );
            assert!(
                rx.try_recv().is_err(),
                "dead={dead}: the publisher's acknowledgement is held"
            );
            assert_eq!(h.publishes_held_for_unreachable(), 1, "dead={dead}");

            // The link returns and the peer says what it routes: the forward goes
            // out under the seq it was recorded with, and the peer's own verdict
            // is what acknowledges the publisher.
            let seq = *h.pending_publishes[&id].awaiting.keys().next().unwrap();
            let mut back = link_peer_at(&mut h, "p", PROTO_FORWARD_REACHED);
            assert!(
                !was_sent_a_forward(&mut back),
                "dead={dead}: nothing before the peer has announced"
            );
            h.peer_announced_interest(&back.node, vec!["t/x".to_string()]);
            assert!(was_sent_a_forward(&mut back), "dead={dead}");
            assert_eq!(h.publishes_held_for_unreachable(), 0, "dead={dead}");
            h.forward_answered(&back.node, seq, ForwardVerdict::Reached);
            assert!(
                matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
                "dead={dead}: acknowledged on the peer's answer"
            );
        }
    }

    /// The in-flight face, closed: a forward that was SENT and is still unanswered
    /// when its target is declared dead stays an obligation. When the peer's link
    /// returns it is sent again under the same seq, and the peer's answer — not the
    /// passing of eight ticks — acknowledges the publisher. (This was
    /// `a_forward_unanswered_when_its_target_is_declared_dead_is_acked_when_the_grace_runs_out`.)
    #[test]
    fn a_forward_unanswered_when_its_target_is_declared_dead_stays_owed_until_the_peer_answers() {
        let mut h = hub();
        let peer = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        let (id, seq, mut rx) = held_forward(&mut h, &peer.node);
        assert!(rx.try_recv().is_err(), "held for the peer's answer");

        h.peer_dead(&peer.node);
        let p = &h.pending_publishes[&id];
        assert!(
            p.awaiting.get(&seq).is_some_and(|o| o.node == peer.node),
            "the obligation is kept, under its seq"
        );

        // Most of the hold passes; nothing is said to the publisher.
        for tick in 1..REROUTE_GRACE_TICKS {
            sweep_tick(&mut h);
            assert!(rx.try_recv().is_err(), "still held after sweep {tick}");
        }

        let mut back = link_peer_at(&mut h, "p", PROTO_FORWARD_REACHED);
        h.peer_announced_interest(&back.node, vec!["t/x".to_string()]);
        assert!(
            was_sent_a_forward(&mut back),
            "retransmitted on the new link"
        );
        // The link is back, so no number of further ticks ends the hold.
        for _ in 0..REROUTE_GRACE_TICKS * 2 {
            h.sweep_verdict_holds();
        }
        assert!(
            rx.try_recv().is_err(),
            "held for the answer, not for a clock"
        );
        h.forward_answered(&back.node, seq, ForwardVerdict::Reached);
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
    }

    /// ADR 0087 D2 (0087-T3), without a durable plane: a peer that stays dead and
    /// out of reach is held — a forward in flight at the verdict and a publish that
    /// arrived during the hold alike — for as long as membership has not confirmed
    /// it gone, however many ticks that is; no clock ends it. At confirmation what
    /// it was owed is resolved: discharged, its publishers acknowledged on the
    /// sweep that follows (one last look for a successor), and each publish counted
    /// `resolved_undeliverable{reason="peer-gone"}` — not before, and not silently.
    #[test]
    fn a_peer_that_stays_dead_is_held_until_confirmed_gone_and_then_resolved_and_counted() {
        let mut h = hub();
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("gone"));
        h.attach_metrics(metrics.clone());
        let peer = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        let (_in_flight, _seq, mut before) = held_forward(&mut h, &peer.node);

        h.peer_dead(&peer.node);
        sweep_tick(&mut h);
        sweep_tick(&mut h);
        let (late, mut during) = register(&mut h, "t/x");
        fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(late));
        h.pending_local_done(late);

        // Thirty ticks — the production `dead_ttl` — and still held.
        for tick in 3..=30 {
            sweep_tick(&mut h);
            assert!(
                before.try_recv().is_err() && during.try_recv().is_err(),
                "both still held after sweep {tick}"
            );
        }
        assert_eq!(h.publishes_held_for_unreachable(), 2);
        assert!(!metrics.render().contains("resolved_undeliverable_total{"));

        h.peer_gone(&peer.node);
        sweep_tick(&mut h);
        assert!(matches!(before.try_recv(), Ok(PublishOutcome::Accepted)));
        assert!(matches!(during.try_recv(), Ok(PublishOutcome::Accepted)));
        assert!(
            metrics
                .render()
                .contains("mqttd_publish_resolved_undeliverable_total{reason=\"peer-gone\"} 2"),
            "counted: {}",
            metrics.render()
        );
        assert!(!h.interest.has_node(&peer.node), "and the peer is erased");
        assert!(
            !h.forward_windows.contains_key(&peer.node),
            "with its forward window (D4)"
        );
        assert!(h.pending_publishes.is_empty());
        assert_eq!(h.publishes_held_for_unreachable(), 0);

        // What arrives after the confirmation finds nobody to owe.
        let (id, mut after) = register(&mut h, "t/x");
        fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(id));
        h.pending_local_done(id);
        assert!(matches!(after.try_recv(), Ok(PublishOutcome::Accepted)));
    }

    /// ADR 0087 D2, from review of #1021: a confirmation that lands while the peer
    /// still has a link — a connection not yet torn down — leaves the link to answer
    /// for it, but not for ever. Membership has written the node off and its dialer
    /// has stopped, so when that link ends no hold starts and no confirmation
    /// follows: what the node was owed is resolved there and then — its interest
    /// erased, its publishers answered on the next sweep, counted `peer-gone`.
    #[test]
    fn a_peer_confirmed_gone_while_linked_is_resolved_when_that_link_ends() {
        let mut h = hub();
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("gl"));
        h.attach_metrics(metrics.clone());
        let first = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        h.peer_dead(&first.node);
        // Linked again under the verdict, and owed a publish over that link.
        let mut again = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        let (id, mut rx) = register(&mut h, "t/x");
        fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(id));
        h.pending_local_done(id);
        assert!(was_sent_a_forward(&mut again));

        h.peer_gone(&again.node);
        sweep_tick(&mut h);
        assert!(
            rx.try_recv().is_err() && h.interest.has_node(&again.node),
            "while the link lasts it answers for the peer; nothing is resolved"
        );

        h.peer_disconnected(&again.node, 0);
        sweep_tick(&mut h);
        assert!(!h.interest.has_node(&again.node), "the interest is gone");
        assert!(
            matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
            "the publisher is answered"
        );
        assert!(
            metrics
                .render()
                .contains("mqttd_publish_resolved_undeliverable_total{reason=\"peer-gone\"} 1"),
            "and counted: {}",
            metrics.render()
        );
        assert!(
            !h.held_dead.contains_key(&again.node),
            "and the verdict is over"
        );
        assert!(h.pending_publishes.is_empty());
    }

    /// The bound on that link, from the re-review of #1021: nothing ends a
    /// half-open peer link in a bounded time — no keepalive frame, no read deadline,
    /// no TCP keepalive, and in plain mode no raft traffic either — so a link to a
    /// node confirmed gone that stays up with no `Alive` is given the hold's
    /// countdown and no more. Then it is dropped, and what the node was owed is
    /// resolved and counted `peer-gone`. Without a durable plane and with one alike.
    #[tokio::test]
    async fn a_link_to_a_peer_confirmed_gone_that_stays_up_is_dropped_after_the_countdown() {
        for durable in [false, true] {
            let mut h = if durable {
                durable_hub().await.0
            } else {
                hub()
            };
            let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("glc"));
            h.attach_metrics(metrics.clone());
            let first = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            h.peer_dead(&first.node);
            let mut zombie = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            let (id, mut rx) = register(&mut h, "t/x");
            fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(id));
            h.pending_local_done(id);
            assert!(was_sent_a_forward(&mut zombie), "durable={durable}");
            h.peer_gone(&zombie.node);

            for tick in 1..REROUTE_GRACE_TICKS {
                sweep_tick(&mut h);
                assert!(
                    rx.try_recv().is_err() && h.peers.contains_key(&zombie.node),
                    "durable={durable}: the link still answers for the peer at sweep {tick}"
                );
            }
            sweep_tick(&mut h);
            assert!(
                !h.peers.contains_key(&zombie.node),
                "durable={durable}: the link is dropped at the end of the countdown"
            );
            sweep_tick(&mut h);
            assert!(
                matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
                "durable={durable}: the publisher is answered"
            );
            assert!(!h.interest.has_node(&zombie.node), "durable={durable}");
            assert!(
                metrics
                    .render()
                    .contains("mqttd_publish_resolved_undeliverable_total{reason=\"peer-gone\"} 1"),
                "durable={durable}: counted: {}",
                metrics.render()
            );
            assert!(!h.held_dead.contains_key(&zombie.node), "durable={durable}");
            // The pump's sender is gone with the peer entry: the link closes.
            while let Ok(_frame) = zombie.rx.try_recv() {}
            assert!(
                matches!(
                    zombie.rx.try_recv(),
                    Err(mpsc::error::TryRecvError::Disconnected)
                ),
                "durable={durable}: the link's sender is dropped"
            );
        }
    }

    /// The other side of it: a peer confirmed gone whose link stays up — it was
    /// alive after all — and that membership then holds alive again is a member.
    /// Nothing is resolved while the link lasts, the `Alive` clears the mark, and a
    /// later loss of its link is a member's lost link: interest and what it is owed
    /// are kept, and nothing is counted.
    #[test]
    fn a_peer_confirmed_gone_whose_link_stays_up_and_that_is_alive_again_resolves_nothing() {
        let mut h = hub();
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("gla"));
        h.attach_metrics(metrics.clone());
        let first = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        h.peer_dead(&first.node);
        let mut again = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        h.peer_gone(&again.node);
        let (id, mut rx) = register(&mut h, "t/x");
        fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(id));
        h.pending_local_done(id);
        assert!(was_sent_a_forward(&mut again), "the live link is routed to");
        // Inside the countdown a live peer's `Alive` arrives (it answers the parting
        // greeting in a round trip).
        for _ in 1..REROUTE_GRACE_TICKS {
            sweep_tick(&mut h);
        }
        assert!(rx.try_recv().is_err(), "held for the peer's answer");

        h.peer_alive(&again.node);
        for _ in 0..REROUTE_GRACE_TICKS * 2 {
            sweep_tick(&mut h);
        }
        assert!(
            h.peers.contains_key(&again.node),
            "after the Alive, no countdown drops the live link"
        );
        h.peer_disconnected(&again.node, 0);
        for _ in 0..REROUTE_GRACE_TICKS * 2 {
            sweep_tick(&mut h);
        }
        assert!(
            h.interest.has_node(&again.node),
            "a member's lost link keeps its interest"
        );
        assert!(
            h.pending_publishes[&id]
                .awaiting
                .values()
                .any(|o| o.node == again.node),
            "and what it is owed"
        );
        assert!(
            rx.try_recv().is_err(),
            "the publisher is held, not answered"
        );
        assert!(
            !metrics.render().contains("resolved_undeliverable_total{"),
            "nothing is counted: {}",
            metrics.render()
        );
    }

    /// ADR 0087 D9: a forward nobody answers — here `QoS` 0 — is not held for a peer
    /// with matching interest and no link, and since 0087-T3 that is counted,
    /// `publish_dropped{reason="peer-unreachable"}`, once per such peer. A linked
    /// peer is forwarded to and nothing is counted.
    #[test]
    fn qos0_for_a_peer_with_no_link_is_counted_peer_unreachable() {
        let mut h = hub();
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("q0"));
        h.attach_metrics(metrics.clone());
        let mut linked = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/#");
        let away = attach_peer_at(&mut h, "q", PROTO_FORWARD_REACHED, "t/#");
        let counted = |m: &mqtt_observability::metrics::Metrics| {
            m.render()
                .lines()
                .find(|l| l.starts_with("mqttd_publish_dropped_total{reason=\"peer-unreachable\"}"))
                .map(str::to_string)
        };
        fan_out(&mut h, "t/x", QoS::AtMostOnce, false, None);
        assert_eq!(counted(&metrics), None, "both linked: nothing counted");

        h.peer_disconnected(&away.node, 0);
        fan_out(&mut h, "t/x", QoS::AtMostOnce, false, None);
        h.peer_dead(&away.node);
        fan_out(&mut h, "t/x", QoS::AtMostOnce, false, None);
        assert_eq!(
            counted(&metrics).as_deref(),
            Some("mqttd_publish_dropped_total{reason=\"peer-unreachable\"} 2"),
            "once per publish for the peer with no link, link lost or held dead"
        );
        assert!(
            std::iter::from_fn(|| linked.rx.try_recv().ok())
                .filter(|f| matches!(f, PeerMessage::Publish { .. }))
                .count()
                == 3,
            "the linked peer got all three"
        );
    }

    /// ADR 0087 D2 (0087-T3), WITH a durable plane, until 0087-T5: the hold also
    /// ends at today's countdown — `REROUTE_GRACE_TICKS` sweeps with no link —
    /// whichever comes first, because until interest says whether the lease plane
    /// owns it a publish made after a durable session's takeover is owed to the
    /// dead node and to the new owner alike, and nothing else discharges what the
    /// dead node is owed. A forward in flight at the verdict is answered on exactly
    /// the sweep it was answered on before this work, a publish made during the
    /// hold with it — so a durable failover holds a publisher no longer than it did
    /// — and both are now counted, `reason="hold-expired"`, where they were silent.
    /// A confirmation that arrives after the countdown has nothing left to resolve.
    #[tokio::test]
    async fn with_a_durable_plane_the_hold_ends_at_the_countdown_and_is_counted() {
        let (mut h, _store) = durable_hub().await;
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("cap"));
        h.attach_metrics(metrics.clone());
        let peer = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        let (_in_flight, _seq, mut before) = held_forward(&mut h, &peer.node);

        h.peer_dead(&peer.node);
        sweep_tick(&mut h);
        sweep_tick(&mut h);
        let (late, mut during) = register(&mut h, "t/x");
        fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(late));
        h.pending_local_done(late);
        assert!(during.try_recv().is_err(), "owed to the unreachable peer");

        for tick in 3..REROUTE_GRACE_TICKS {
            sweep_tick(&mut h);
            assert!(
                before.try_recv().is_err() && during.try_recv().is_err(),
                "both still held after sweep {tick}"
            );
        }
        sweep_tick(&mut h);
        assert!(
            matches!(before.try_recv(), Ok(PublishOutcome::Accepted)),
            "answered on sweep {REROUTE_GRACE_TICKS}, as before 0087-T3"
        );
        assert!(matches!(during.try_recv(), Ok(PublishOutcome::Accepted)));
        assert!(
            metrics
                .render()
                .contains("mqttd_publish_resolved_undeliverable_total{reason=\"hold-expired\"} 2"),
            "counted: {}",
            metrics.render()
        );
        assert!(!h.interest.has_node(&peer.node));

        h.peer_gone(&peer.node);
        assert!(
            !metrics.render().contains("reason=\"peer-gone\""),
            "nothing left for the confirmation to resolve"
        );
        assert!(
            !h.held_dead.contains_key(&peer.node),
            "and the verdict ends"
        );
    }

    /// A hub with a durable plane attached (idle: its driver is stopped) and the
    /// store its sessions are kept in — the mode in which a session can move from
    /// one node to another, and so the only one in which a newly announced
    /// interest can be an unreachable peer's subscriber under a new owner.
    async fn durable_hub() -> (Hub, Arc<MemorySessionStore>) {
        let local = NodeId("fwd-proofs".into());
        let placement = Arc::new(std::sync::RwLock::new(
            mqtt_cluster::placement::Placement::new(
                local.clone(),
                mqtt_cluster::placement::DEFAULT_REPLICAS,
            ),
        ));
        let (_store, _retained, plane, driver) = mqtt_cluster::durable_node::build_durable_node(
            local.clone(),
            placement,
            false,
            5,
            &std::collections::BTreeMap::new(),
            None,
            None,
            false,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            None,
        )
        .await;
        driver.abort();
        let store = Arc::new(MemorySessionStore::new());
        let (mut hub, _) = Hub::with_config(local, store.clone());
        hub.attach_durable_plane(plane);
        (hub, store)
    }

    /// A publish for `t/x` made while `dead` is held dead: owed to it, and held.
    fn published_under_the_verdict(h: &mut Hub) -> (u64, oneshot::Receiver<PublishOutcome>) {
        let (id, rx) = register(h, "t/x");
        fan_out(h, "t/x", QoS::AtLeastOnce, false, Some(id));
        h.pending_local_done(id);
        (id, rx)
    }

    /// One session to materialise on this node, subscribed to `t/x`.
    fn a_session_for_t_x() -> Vec<mqtt_storage::ScannedSession> {
        vec![mqtt_storage::ScannedSession::new(
            ClientId("moved".into()),
            vec![mqtt_core::Subscription {
                filter: "t/x".into(),
                max_qos: QoS::AtLeastOnce,
                no_local: false,
                sub_id: None,
            }],
            None,
        )]
    }

    /// The durable takeover path is unchanged: a peer that newly announces the
    /// interest while the target is held dead is sent the publish as a re-route,
    /// and that forward stands in for what the unreachable peer was owed — so a
    /// real owner death costs its publishers the failover, not the whole hold.
    ///
    /// CHARACTERISATION, for 0087-T5: the "successor" here is any peer whose newly
    /// announced filter matches the topic. Nothing tells this hub that it took over
    /// the unreachable peer's subscriber rather than being an unrelated one, so an
    /// unrelated one discharges the obligation just the same — and if the peer was
    /// falsely held dead, its subscriber loses the publish. D8 narrows the
    /// discharge to interest the lease plane owns; this test then needs both kinds.
    /// (Since 0087-T2b the discharge exists only with a durable plane: see
    /// `without_a_durable_plane_nothing_stands_in_for_what_an_unreachable_peer_is_owed`.)
    #[tokio::test]
    async fn a_successor_that_announces_the_interest_discharges_what_the_unreachable_peer_was_owed()
    {
        for at_verdict in [true, false] {
            let (mut h, _store) = durable_hub().await;
            let dead = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            let mut successor = attach_peer_at(&mut h, "succ", PROTO_FORWARD_REACHED, "other");
            let (id, mut rx) = if at_verdict {
                // In flight at the verdict.
                let (id, _seq, rx) = held_forward(&mut h, &dead.node);
                h.peer_dead(&dead.node);
                (id, rx)
            } else {
                // Published under the verdict.
                h.peer_dead(&dead.node);
                published_under_the_verdict(&mut h)
            };
            sweep_tick(&mut h);
            assert!(rx.try_recv().is_err(), "at_verdict={at_verdict}: held");
            assert!(!was_sent_a_forward(&mut successor));

            // The lease's new owner materialises the session and announces it.
            h.interest
                .replace(successor.node.clone(), vec!["t/x".to_string()]);
            sweep_tick(&mut h);
            assert!(
                was_sent_a_forward(&mut successor),
                "at_verdict={at_verdict}: re-routed to the successor"
            );
            let p = &h.pending_publishes[&id];
            assert!(
                p.awaiting.values().all(|o| o.node == successor.node),
                "at_verdict={at_verdict}: and only the successor is still owed an answer"
            );
            let seq = *p.awaiting.keys().next().unwrap();
            h.forward_answered(&successor.node, seq, ForwardVerdict::Reached);
            assert!(
                matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
                "at_verdict={at_verdict}: acknowledged on the successor's answer"
            );
        }
    }

    /// The same discharge, on this node: a matching session that materialises HERE
    /// during the hold — the takeover scan handing this node the unreachable peer's
    /// durable session — is delivered to by the settle pass, and that ends what the
    /// peer was owed. CHARACTERISATION for 0087-T5, as above: any matching session
    /// that appears here does it, the peer's own or not.
    ///
    /// What is asserted is the whole outcome, not only that nothing is awaited: the
    /// message is in the materialised session's queue, once; the unreachable peer
    /// was sent nothing; and the publisher is acknowledged when that append has
    /// landed, not before.
    #[tokio::test]
    async fn a_session_that_materialises_here_discharges_what_the_unreachable_peer_was_owed() {
        let (mut h, store) = durable_hub().await;
        let mut dead = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        h.peer_dead(&dead.node);
        let (id, mut rx) = published_under_the_verdict(&mut h);
        sweep_tick(&mut h);
        assert!(
            rx.try_recv().is_err(),
            "owed to the unreachable peer, and held"
        );
        assert_eq!(h.publishes_held_for_unreachable(), 1);

        h.inherit_sessions(a_session_for_t_x(), true);
        assert!(
            h.pending_publishes
                .get(id)
                .is_none_or(|p| p.awaiting.is_empty()),
            "nothing is owed to the unreachable peer any more"
        );
        assert!(
            rx.try_recv().is_err(),
            "not acknowledged before the session's copy is stored"
        );
        crate::hub::tests::settle_gate::quiesce(&mut h).await;
        let queued = store
            .pending(&ClientId("moved".into()), 0, 16)
            .await
            .unwrap();
        assert_eq!(queued.len(), 1, "stored for the session, once");
        assert_eq!(queued[0].message.topic, "t/x");
        assert!(
            matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
            "acknowledged on the append that stored it here"
        );
        assert!(
            !was_sent_a_forward(&mut dead),
            "the unreachable peer was sent nothing"
        );
        assert!(h.pending_publishes.is_empty() && h.forward_index.is_empty());
        assert_eq!(h.publishes_held_for_unreachable(), 0);
    }

    /// 0087-T2b, item 4 of the read-through recorded on issue #973. Without a
    /// durable plane a session never moves between nodes — the store is local,
    /// nothing is inherited and nothing re-homed — so a newly matching interest is
    /// never the unreachable peer's subscriber under a new owner. It is somebody
    /// else: it gets the publish, and what the peer is owed STAYS owed, until the
    /// peer answers for itself. (With the discharge, a monitor on `#` reconnecting
    /// anywhere inside the half second a redial takes lost the publish for a
    /// subscriber on a peer that was never dead.)
    ///
    /// The durable-mode gap is the two characterisation tests above, for 0087-T5.
    /// 0087-T2b, pinned in the direction #1011 set. In plain mode a client of a
    /// peer held dead that reconnects HERE with `clean_session = 0` is answered
    /// `session_present = 0`: no session moves between nodes, so its session here
    /// is new. A publish held for the unreachable peer is not handed to it — MQTT
    /// owes a new session nothing published before it — so its attach is no
    /// evidence either: it releases no ack early and discharges nothing the peer
    /// is owed, which stays owed until the peer answers for itself.
    ///
    /// Before #1013 the settle pass's re-delivery took any session attached after
    /// the publish, and this client was handed the message.
    #[tokio::test]
    async fn without_a_durable_plane_a_client_reconnecting_here_from_the_unreachable_peer_is_not_handed_the_held_publish(
    ) {
        let store = Arc::new(MemorySessionStore::new());
        let (mut h, _) = Hub::with_config(NodeId("fwd-proofs".into()), store.clone());
        assert!(h.durable_plane.is_none(), "fixture: plain mode");
        let dead = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
        h.peer_dead(&dead.node);
        let (id, mut rx) = published_under_the_verdict(&mut h);
        let owed = *h.pending_publishes[&id].awaiting.keys().next().unwrap();

        // The peer's subscriber reconnects here, asking for its session back.
        let (mut client, _meter, present) =
            crate::hub::tests::settle_gate::attach_watched(&mut h, "from-p", false, 3600, 1, "t/x")
                .await;
        assert!(
            !present,
            "plain mode: its session is not here, so it is new"
        );

        // The settle pass runs (it does on every sweep tick).
        h.settle_pending_publishes();
        crate::hub::tests::settle_gate::quiesce(&mut h).await;

        assert_eq!(
            crate::hub::tests::settle_gate::published_topics(&mut client),
            Vec::<String>::new(),
            "a session created after the publish was handed it (#1011)"
        );
        assert!(
            store
                .pending(&ClientId("from-p".into()), 0, 16)
                .await
                .unwrap()
                .is_empty(),
            "nor queued for it"
        );
        assert!(
            rx.try_recv().is_err(),
            "its attach is no evidence: the ack is still held"
        );
        assert!(
            h.pending_publishes[&id]
                .awaiting
                .get(&owed)
                .is_some_and(|o| o.node == dead.node),
            "and the unreachable peer is still owed the publish, under its seq"
        );

        // The peer was never dead: its link returns and it answers for itself.
        let mut back = link_peer_at(&mut h, "p", PROTO_FORWARD_REACHED);
        h.peer_announced_interest(&back.node, vec!["t/x".to_string()]);
        assert_eq!(forwards_sent(&mut back), vec![(owed, None)]);
        h.forward_answered(&back.node, owed, ForwardVerdict::Reached);
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
        assert!(
            crate::hub::tests::settle_gate::published_topics(&mut client).is_empty(),
            "still nothing for the new session"
        );
    }

    #[tokio::test]
    async fn without_a_durable_plane_nothing_stands_in_for_what_an_unreachable_peer_is_owed() {
        for newcomer_is_local in [false, true] {
            let case = format!("newcomer_is_local={newcomer_is_local}");
            let store = Arc::new(MemorySessionStore::new());
            let (mut h, _) = Hub::with_config(NodeId("fwd-proofs".into()), store.clone());
            assert!(h.durable_plane.is_none(), "fixture: plain mode");
            let dead = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            let mut other = attach_peer_at(&mut h, "other", PROTO_FORWARD_REACHED, "none");
            h.peer_dead(&dead.node);
            let (id, mut rx) = published_under_the_verdict(&mut h);
            let owed = *h.pending_publishes[&id].awaiting.keys().next().unwrap();

            if newcomer_is_local {
                h.inherit_sessions(a_session_for_t_x(), true);
                crate::hub::tests::settle_gate::quiesce(&mut h).await;
                let queued = store
                    .pending(&ClientId("moved".into()), 0, 16)
                    .await
                    .unwrap();
                assert_eq!(queued.len(), 1, "{case}: the newcomer gets the publish");
            } else {
                h.interest
                    .replace(other.node.clone(), vec!["t/x".to_string()]);
                sweep_tick(&mut h);
                let sent = forwards_sent(&mut other);
                assert_eq!(sent.len(), 1, "{case}: the newcomer gets the publish");
                h.forward_answered(&other.node, sent[0].0, ForwardVerdict::Reached);
            }
            assert!(
                rx.try_recv().is_err(),
                "{case}: the newcomer's answer acknowledges nothing owed to the peer"
            );
            assert!(
                h.pending_publishes[&id]
                    .awaiting
                    .get(&owed)
                    .is_some_and(|o| o.node == dead.node),
                "{case}: the peer is still owed the publish, under its seq"
            );

            // The peer was never dead: its link returns and it answers for itself.
            let mut back = link_peer_at(&mut h, "p", PROTO_FORWARD_REACHED);
            h.peer_announced_interest(&back.node, vec!["t/x".to_string()]);
            assert_eq!(
                forwards_sent(&mut back),
                vec![(owed, None)],
                "{case}: sent to the peer when it is back"
            );
            h.forward_answered(&back.node, owed, ForwardVerdict::Reached);
            assert!(
                matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
                "{case}: acknowledged on the peer's own answer"
            );
        }
    }

    /// A `$share` delivery owed to an unreachable member does not move to a member
    /// that cannot be reached either. Both of the group's nodes are held dead; the
    /// delivery stays with the member it was owed to, that node's link returns and
    /// it is delivered there — where a move to the other node's member would have
    /// left it with a peer that never returned, acknowledged when that peer's hold
    /// ran out, with the member that did return never tried again.
    #[test]
    fn a_share_delivery_does_not_move_from_one_unreachable_member_to_another() {
        let mut h = hub();
        let group = |member: &str| RemoteSharedGroup {
            group: "g".into(),
            filter: "s/#".into(),
            members: vec![(ClientId(member.into()), QoS::AtLeastOnce, true)],
        };
        let b = attach_peer_at(&mut h, "b", PROTO_FORWARD_REACHED, "none");
        let c = attach_peer_at(&mut h, "c", PROTO_FORWARD_REACHED, "none");
        h.remote_shared.insert(b.node.clone(), vec![group("m-b")]);
        h.remote_shared.insert(c.node.clone(), vec![group("m-c")]);
        h.rebuild_remote_shared_index();

        // In flight to b's member when this node is cut off from both.
        let (id, mut rx) = register(&mut h, "s/t");
        let _ = h.deliver_shared(
            "s/t",
            &Bytes::from_static(b"x"),
            QoS::AtLeastOnce,
            None,
            &AppProperties::default(),
            Some(id),
            false,
        );
        h.pending_local_done(id);
        let owed_to = |h: &Hub| {
            h.pending_publishes[&id]
                .awaiting
                .values()
                .next()
                .expect("owed to a member")
                .node
                .clone()
        };
        assert_eq!(owed_to(&h), b.node, "the rotation starts at b's member");
        h.peer_dead(&c.node);
        h.peer_dead(&b.node);
        sweep_tick(&mut h);
        sweep_tick(&mut h);
        assert_eq!(
            owed_to(&h),
            b.node,
            "c's member cannot be reached either: not somewhere to move to"
        );

        // b returns; c never does, and is confirmed gone.
        let mut b_again = link_peer_at(&mut h, "b", PROTO_FORWARD_REACHED);
        h.peer_announced_shared(&b_again.node, vec![group("m-b")]);
        let seq = loop {
            let frame = b_again.rx.try_recv().expect("sent to b's member");
            if let PeerMessage::SharedDeliverAcked { seq, client, .. } = frame {
                assert_eq!(client, "m-b");
                break seq;
            }
        };
        h.peer_gone(&c.node);
        for _ in 0..REROUTE_GRACE_TICKS {
            sweep_tick(&mut h);
        }
        assert!(
            rx.try_recv().is_err(),
            "c's confirmation answers nothing that is owed to b"
        );
        h.forward_answered(&b_again.node, seq, ForwardVerdict::Stored);
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
    }

    /// Nor does a refusal move it there. The delivery owed to b's member moves to
    /// d's, on a node with a link, at b's verdict; d refuses (its node in a
    /// brownout). The only member not yet tried is c's, and c cannot be reached: the
    /// pass is exhausted, and the publisher is told of the refusal and retries —
    /// where holding the delivery for c would have ended, once c was confirmed gone, in an
    /// acknowledgement for a publish nobody received, with b never tried again.
    #[test]
    fn a_refused_share_delivery_is_not_moved_to_a_member_that_cannot_be_reached() {
        let mut h = hub();
        let group = |member: &str| RemoteSharedGroup {
            group: "g".into(),
            filter: "s/#".into(),
            members: vec![(ClientId(member.into()), QoS::AtLeastOnce, true)],
        };
        let b = attach_peer_at(&mut h, "b", PROTO_FORWARD_REACHED, "none");
        let c = attach_peer_at(&mut h, "c", PROTO_FORWARD_REACHED, "none");
        let mut d = attach_peer_at(&mut h, "d", PROTO_FORWARD_REACHED, "none");
        for (link, member) in [(&b, "m-b"), (&c, "m-c"), (&d, "m-d")] {
            h.remote_shared
                .insert(link.node.clone(), vec![group(member)]);
        }
        h.rebuild_remote_shared_index();
        let (id, mut rx) = register(&mut h, "s/t");
        let _ = h.deliver_shared(
            "s/t",
            &Bytes::from_static(b"x"),
            QoS::AtLeastOnce,
            None,
            &AppProperties::default(),
            Some(id),
            false,
        );
        h.pending_local_done(id);

        h.peer_dead(&c.node);
        h.peer_dead(&b.node);
        let moved = loop {
            let frame =
                d.rx.try_recv()
                    .expect("moved to d's member, on a linked node");
            if let PeerMessage::SharedDeliverAcked { seq, client, .. } = frame {
                assert_eq!(client, "m-d");
                break seq;
            }
        };
        h.forward_answered(
            &d.node,
            moved,
            ForwardVerdict::Refused {
                code: PublishRefusal::Brownout.wire_code(),
            },
        );
        assert!(
            matches!(
                rx.try_recv(),
                Ok(PublishOutcome::Refused(PublishRefusal::Brownout))
            ),
            "the publisher is told, and nothing is held for the unreachable member"
        );
        assert!(h.pending_publishes.is_empty() && h.forward_index.is_empty());
    }

    /// `$share` membership survives a lost link and a verdict (finding 3 of issue
    /// #973: it was dropped by both, so a link flap alone lost `$share` deliveries).
    /// A member on a node with a link is always preferred; when the group has none,
    /// the publish is OWED to a member that cannot be reached, and held.
    #[test]
    fn a_share_group_keeps_its_members_across_a_lost_link_and_a_verdict() {
        let group = |members: &[&str]| RemoteSharedGroup {
            group: "g".into(),
            filter: "s/#".into(),
            members: members
                .iter()
                .map(|m| (ClientId((*m).into()), QoS::AtLeastOnce, true))
                .collect(),
        };
        for (dead, elsewhere) in [(false, false), (true, false), (false, true), (true, true)] {
            let case = format!("dead={dead} elsewhere={elsewhere}");
            let mut h = hub();
            let away = attach_peer_at(&mut h, "away", PROTO_FORWARD_REACHED, "none");
            h.remote_shared
                .insert(away.node.clone(), vec![group(&["m-away"])]);
            let mut other = attach_peer_at(&mut h, "other", PROTO_FORWARD_REACHED, "none");
            if elsewhere {
                h.remote_shared
                    .insert(other.node.clone(), vec![group(&["m-other"])]);
            }
            h.rebuild_remote_shared_index();

            if dead {
                h.peer_dead(&away.node);
            } else {
                h.peer_disconnected(&away.node, 0);
            }

            // Several publishes, so the rotation would reach the unreachable member
            // if it were still taking turns.
            let mut held = Vec::new();
            for _ in 0..4 {
                let (id, mut rx) = register(&mut h, "s/t");
                let (durable, placed) = h.deliver_shared(
                    "s/t",
                    &Bytes::from_static(b"x"),
                    QoS::AtLeastOnce,
                    None,
                    &AppProperties::default(),
                    Some(id),
                    false,
                );
                assert!(matches!(durable, DurableOutcome::Ok) && placed, "{case}");
                h.pending_local_done(id);
                assert!(rx.try_recv().is_err(), "{case}: owed to a member");
                let target = h.pending_publishes[&id]
                    .awaiting
                    .values()
                    .next()
                    .expect("a shared obligation")
                    .node
                    .clone();
                assert_eq!(
                    target == other.node,
                    elsewhere,
                    "{case}: the reachable member takes it whenever there is one"
                );
                held.push((id, rx));
            }
            assert_eq!(was_sent_a_forward(&mut other), elsewhere, "{case}");
            assert_eq!(
                h.publishes_held_for_unreachable(),
                if elsewhere { 0 } else { 4 },
                "{case}"
            );
            if elsewhere {
                continue;
            }

            // The member's node is linked again: every held delivery goes out to it,
            // and its answer acknowledges the publisher.
            let mut back = link_peer_at(&mut h, "away", PROTO_FORWARD_REACHED);
            h.peer_announced_interest(&back.node, vec!["none".to_string()]);
            assert!(
                !was_sent_a_forward(&mut back),
                "{case}: not before the peer has said who its members are"
            );
            h.peer_announced_shared(&back.node, vec![group(&["m-away"])]);
            let mut sent = 0;
            while let Ok(frame) = back.rx.try_recv() {
                if let PeerMessage::SharedDeliverAcked { seq, client, .. } = frame {
                    assert_eq!(client, "m-away", "{case}");
                    h.forward_answered(&back.node, seq, ForwardVerdict::Stored);
                    sent += 1;
                }
            }
            assert_eq!(sent, 4, "{case}: each held delivery was sent once");
            for (_, mut rx) in held {
                assert!(
                    matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
                    "{case}"
                );
            }
        }
    }

    /// A `$share` delivery in flight to a member whose node is declared dead moves
    /// at once to a member on another node, when the group has one (0041-T12: the
    /// rebalance a refusal triggers) — and stays owed to the unreachable member
    /// when it has none, where it used to be withheld from the publisher.
    #[test]
    fn a_share_delivery_in_flight_at_a_verdict_moves_to_a_reachable_member_or_stays_owed() {
        for elsewhere in [true, false] {
            let mut h = hub();
            let mut links = Vec::new();
            for (node, member) in [("n1", "m1"), ("n2", "m2")] {
                let link = attach_peer_at(&mut h, node, PROTO_FORWARD_REACHED, "none");
                if node == "n1" || elsewhere {
                    h.remote_shared.insert(
                        link.node.clone(),
                        vec![RemoteSharedGroup {
                            group: "g".into(),
                            filter: "s/#".into(),
                            members: vec![(ClientId(member.into()), QoS::AtLeastOnce, true)],
                        }],
                    );
                }
                links.push(link);
            }
            h.rebuild_remote_shared_index();
            let (id, mut rx) = register(&mut h, "s/t");
            let _ = h.deliver_shared(
                "s/t",
                &Bytes::from_static(b"x"),
                QoS::AtLeastOnce,
                None,
                &AppProperties::default(),
                Some(id),
                false,
            );
            h.pending_local_done(id);
            let first = h.pending_publishes[&id]
                .awaiting
                .values()
                .next()
                .expect("a shared obligation")
                .node
                .clone();

            h.peer_dead(&first);
            assert!(
                rx.try_recv().is_err(),
                "elsewhere={elsewhere}: neither acknowledged nor withheld"
            );
            let now = h.pending_publishes[&id]
                .awaiting
                .values()
                .next()
                .expect("still owed to a member")
                .node
                .clone();
            assert_eq!(
                now != first,
                elsewhere,
                "elsewhere={elsewhere}: moved exactly when there is somewhere to move to"
            );
        }
    }

    /// What becomes of a `$share` delivery owed to a member on a node that stays
    /// dead and out of reach. If a member appears on another node while the hold
    /// runs — the group's consumers reconnecting elsewhere — the delivery moves to
    /// it at the next tick. If none does, the hold ends when the node is confirmed
    /// gone (ADR 0087 D2) and the publisher is acknowledged, as for any publish
    /// nobody is left to receive; its connection is not closed on it.
    #[test]
    fn a_share_delivery_owed_to_a_dead_node_moves_to_a_new_member_or_ends_with_the_hold() {
        for reappears in [true, false] {
            let mut h = hub();
            let away = attach_peer_at(&mut h, "away", PROTO_FORWARD_REACHED, "none");
            let mut other = attach_peer_at(&mut h, "other", PROTO_FORWARD_REACHED, "none");
            let group = |member: &str| RemoteSharedGroup {
                group: "g".into(),
                filter: "s/#".into(),
                members: vec![(ClientId(member.into()), QoS::AtLeastOnce, true)],
            };
            h.remote_shared
                .insert(away.node.clone(), vec![group("consumer")]);
            h.rebuild_remote_shared_index();
            h.peer_dead(&away.node);

            let (id, mut rx) = register(&mut h, "s/t");
            let _ = h.deliver_shared(
                "s/t",
                &Bytes::from_static(b"x"),
                QoS::AtLeastOnce,
                None,
                &AppProperties::default(),
                Some(id),
                false,
            );
            h.pending_local_done(id);
            sweep_tick(&mut h);
            assert!(rx.try_recv().is_err(), "reappears={reappears}: held");

            if reappears {
                // The same consumer, connected to another node now.
                h.remote_shared
                    .insert(other.node.clone(), vec![group("consumer")]);
                h.rebuild_remote_shared_index();
                sweep_tick(&mut h);
                let seq = loop {
                    let frame = other
                        .rx
                        .try_recv()
                        .expect("the delivery moved to the new member");
                    if let PeerMessage::SharedDeliverAcked { seq, .. } = frame {
                        break seq;
                    }
                };
                h.forward_answered(&other.node, seq, ForwardVerdict::Stored);
                assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
            } else {
                for _ in 2..REROUTE_GRACE_TICKS * 2 {
                    sweep_tick(&mut h);
                    assert!(rx.try_recv().is_err(), "still held until confirmation");
                }
                h.peer_gone(&away.node);
                sweep_tick(&mut h);
                assert!(
                    matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
                    "acknowledged when the node is confirmed gone, not withheld"
                );
                assert!(h.pending_publishes.is_empty() && h.forward_index.is_empty());
            }
        }
    }

    /// At the pending table's bound a hold behaves as the table always has: the
    /// OLDEST entry is evicted with its acknowledgement withheld and counted
    /// (`pending-cap`), the newest is admitted, and neither bound is exceeded —
    /// 65,536 entries and 64 MiB hold whatever is owed to a peer that cannot be
    /// reached, and no more. An evicted publish leaves nothing behind in the index
    /// of outstanding forwards.
    #[test]
    fn a_hold_at_the_pending_table_bound_evicts_the_oldest_and_withholds_its_ack() {
        let mut h = hub();
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("held"));
        h.attach_metrics(metrics.clone());
        let peer = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/#");
        h.peer_dead(&peer.node);

        let over = 64;
        let mut waiting = Vec::new();
        for _ in 0..PENDING_PUBLISH_CAP + over {
            let (id, rx) = register(&mut h, "t/x");
            fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(id));
            h.pending_local_done(id);
            waiting.push(rx);
        }
        assert_eq!(h.pending_publishes.len(), PENDING_PUBLISH_CAP);
        assert!(h.pending_publishes.bytes() <= PENDING_PUBLISH_MAX_BYTES);
        assert_eq!(
            h.forward_index.len(),
            PENDING_PUBLISH_CAP,
            "one outstanding forward per entry held, none for an evicted one"
        );
        assert_eq!(h.publishes_held_for_unreachable(), PENDING_PUBLISH_CAP);
        for (i, rx) in waiting.iter_mut().enumerate() {
            let evicted = i < over;
            assert_eq!(
                matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed)),
                evicted,
                "publish {i}: the {over} oldest are withheld, the rest still held"
            );
        }
        assert!(
            metrics.render().contains(&format!(
                "mqttd_publish_dropped_total{{reason=\"pending-cap\"}} {over}"
            )),
            "each eviction is counted"
        );

        // The byte bound, with payloads large enough for it to bind first.
        let mut h = hub();
        let peer = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/#");
        h.peer_dead(&peer.node);
        let payload = Bytes::from(vec![0u8; 1 << 20]);
        let mut waiting = Vec::new();
        for _ in 0..80 {
            let (done, rx) = oneshot::channel();
            let id = h.register_pending(
                done,
                "t/x",
                &payload,
                QoS::AtLeastOnce,
                false,
                None,
                &AppProperties::default(),
            );
            h.send_acked_forward(id, &peer.node, false);
            h.pending_local_done(id);
            waiting.push(rx);
        }
        assert!(h.pending_publishes.bytes() <= PENDING_PUBLISH_MAX_BYTES);
        assert!(h.pending_publishes.len() < 80, "the byte bound evicted");
        assert_eq!(h.forward_index.len(), h.pending_publishes.len());
        let mut withheld = 0;
        for rx in &mut waiting {
            if matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed)) {
                withheld += 1;
            }
        }
        assert_eq!(withheld, 80 - h.pending_publishes.len());
    }

    // ---- ADR 0087 D5: a link under a `Dead` verdict --------------------------

    /// A link is admitted by its handshake, not by membership, so a peer this hub
    /// was told is dead can be linked again, announce its interest, and be routed
    /// to — the verdict still standing. What the verdict then decides is what the
    /// loss of THAT link means. Membership says `Dead` once: no second verdict
    /// follows when the link goes, so the hold starts again, and when the peer is
    /// confirmed gone the interest the link brought back goes — or a node that has
    /// finished the graceful stop it announced would be owed every matching publish
    /// for good. Once membership holds the peer alive again, a lost link keeps its
    /// interest for as long as it takes, as for any member (and there is no verdict
    /// left for a confirmation to end).
    #[test]
    fn a_link_under_a_dead_verdict_takes_its_interest_with_it_until_the_verdict_is_withdrawn() {
        for withdrawn in [false, true] {
            let mut h = hub();
            let first = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            h.peer_dead(&first.node);

            // Linked again under the verdict, and routed to.
            let mut again = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            let (id, mut rx) = register(&mut h, "t/x");
            fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(id));
            h.pending_local_done(id);
            assert!(
                was_sent_a_forward(&mut again),
                "withdrawn={withdrawn}: a peer held dead with a link is forwarded to"
            );

            if withdrawn {
                h.peer_alive(&again.node);
            }
            h.peer_disconnected(&again.node, 0);
            for _ in 0..REROUTE_GRACE_TICKS {
                sweep_tick(&mut h);
            }
            assert!(
                h.interest.has_node(&again.node),
                "withdrawn={withdrawn}: held"
            );
            h.peer_gone(&again.node);
            sweep_tick(&mut h);
            assert_eq!(
                h.interest.has_node(&again.node),
                withdrawn,
                "withdrawn={withdrawn}: the interest outlives the hold only for a member"
            );
            assert_eq!(
                rx.try_recv().is_err(),
                withdrawn,
                "withdrawn={withdrawn}: and so does what the peer is owed"
            );
        }
    }

    /// The same, end to end from the membership events: the link manager turns
    /// `Dead`, `Suspect`, `Alive` into what it tells the hub, and the hub applies
    /// it. SWIM raises that sequence for one id in two shapes — a relayed suspicion
    /// re-adding a member whose tombstone was pruned, and one superseding the
    /// tombstone of a member that has started a new life (here: at a new address).
    /// After either, the member is a member: its link is lost, and its interest and
    /// the forward in flight to it are kept. (The link manager used to withdraw the
    /// verdict only when `Dead` was the event just before `Alive`; with `Suspect`
    /// in between the hub held the verdict for good, and every later flap that
    /// outlasted the hold erased both.)
    #[tokio::test]
    async fn after_dead_suspect_alive_a_lost_link_is_a_members_lost_link() {
        use mqtt_cluster::swim::MemberState;
        use mqtt_cluster::swim_driver::MembershipEvent;
        for returns_at in ["p:7000", "p-reborn:7000"] {
            let (events, event_rx) = mpsc::unbounded_channel();
            let (to_hub, mut from_link_manager) = mpsc::unbounded_channel();
            // "z" > "p": this node does not dial the pair, so no socket is involved.
            let link_manager = tokio::spawn(crate::cluster::maintain_peer_links(
                event_rx,
                NodeId("z".into()),
                to_hub,
                None,
                None,
                None,
                None,
            ));
            let event = |peer_addr: &str, state| MembershipEvent {
                id: NodeId("p".into()),
                addr: "p-swim".into(),
                peer_addr: peer_addr.into(),
                state,
                domain: None,
                gone: false,
            };

            let mut h = hub();
            let first = attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x");
            events.send(event("p:7000", MemberState::Alive)).unwrap();
            events.send(event("p:7000", MemberState::Dead)).unwrap();
            events
                .send(event(returns_at, MemberState::Suspect))
                .unwrap();
            events.send(event(returns_at, MemberState::Alive)).unwrap();
            // What the hub is told, applied as its own dispatch applies it; the
            // member's link is back between the verdict and its withdrawal.
            let mut again = None;
            for expected in ["dead", "alive"] {
                let told = tokio::time::timeout(Duration::from_secs(20), from_link_manager.recv())
                    .await
                    .unwrap_or_else(|_| panic!("{returns_at}: the hub was never told `{expected}`"))
                    .expect("the link manager is running");
                match told {
                    HubCommand::PeerDead { node } => {
                        assert_eq!((expected, &node), ("dead", &first.node));
                        h.peer_dead(&node);
                        again = Some(attach_peer_at(&mut h, "p", PROTO_FORWARD_REACHED, "t/x"));
                    }
                    HubCommand::PeerAlive { node } => {
                        assert_eq!((expected, &node), ("alive", &first.node));
                        h.peer_alive(&node);
                    }
                    other => panic!("{returns_at}: unexpected {other:?}"),
                }
            }
            let again = again.expect("the verdict was applied");
            let (id, mut rx) = register(&mut h, "t/x");
            fan_out(&mut h, "t/x", QoS::AtLeastOnce, false, Some(id));
            h.pending_local_done(id);

            // An ordinary link flap, some time later — and the link stays down for
            // longer than a hold lasts. A node still held dead would be erased by
            // then; a member is not.
            h.peer_disconnected(&again.node, 0);
            for _ in 0..REROUTE_GRACE_TICKS * 2 {
                sweep_tick(&mut h);
            }
            assert!(
                h.interest.has_node(&again.node),
                "{returns_at}: a member's lost link keeps its interest"
            );
            assert!(
                h.pending_publishes[&id]
                    .awaiting
                    .values()
                    .any(|o| o.node == again.node),
                "{returns_at}: and the forward in flight to it"
            );
            assert!(
                rx.try_recv().is_err(),
                "{returns_at}: the publisher is held"
            );
            link_manager.abort();
        }
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
            assert!(h
                .pending_publishes
                .get_mut(id)
                .expect("just registered")
                .answer(PublishOutcome::Accepted));
            assert!(h.pending_publishes[&id].ack_released());
            assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));

            if withhold {
                h.drop_pending(id);
            } else {
                h.refuse_pending(id, PublishRefusal::Brownout);
            }

            assert!(!h.pending_publishes.contains_key(id));
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
        let p = h.pending_publishes.get_mut(id).expect("just registered");
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

    // ---- item 2.4, as merged: honest accounting of an answered victim ------

    /// What survives of item 2.4 after merging the byte-bounded table
    /// (3571682). The eviction POLICY is main's — oldest first — and is proven
    /// in `pending_bounds`. What 2.4 still contributes is the COUNT: item 2.1
    /// makes an entry possible that was already answered and survives only for
    /// the settle window's replay. Evicting it withholds nothing, so it must be
    /// counted `pending-cap-replay`, never `pending-cap`; the unanswered entry
    /// evicted next is the control and must still count, and withhold, as before.
    ///
    /// Fails without the `ack_released()` branch in `register_pending`: the
    /// first eviction lands in `pending-cap` and `replay` reads 0.
    #[test]
    fn an_already_answered_victim_is_not_counted_as_a_withheld_ack() {
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("fwd-proofs"));
        let mut h = hub();
        h.attach_metrics(metrics.clone());
        let count = |reason: &str| -> u64 {
            let series = format!("mqttd_publish_dropped_total{{reason=\"{reason}\"}} ");
            metrics
                .render()
                .lines()
                .find_map(|l| l.strip_prefix(&series))
                .map_or(0, |v| v.trim().parse().unwrap())
        };

        // The oldest entry: answered early, as item 2.1 answers it, and still in
        // the settle window's work set.
        let (answered, mut answered_rx) = register(&mut h, "t/0");
        {
            let p = h.pending_publishes.get_mut(answered).unwrap();
            p.awaiting_settle = true;
            assert!(p.answer(PublishOutcome::Accepted), "fixture: first answer");
        }
        // The second-oldest: a publisher still waiting.
        let (waiting, mut waiting_rx) = register(&mut h, "t/1");
        for i in 2..PENDING_PUBLISH_CAP {
            let _ = register(&mut h, &format!("t/{i}"));
        }
        assert_eq!(h.pending_publishes.len(), PENDING_PUBLISH_CAP);

        // One arrival past the cap evicts the oldest: the answered one.
        let _ = register(&mut h, "t/over-1");
        assert!(
            h.pending_publishes.get(answered).is_none(),
            "oldest evicted"
        );
        assert_eq!(
            answered_rx.try_recv(),
            Ok(PublishOutcome::Accepted),
            "its publisher already had its answer; nothing was withheld"
        );
        assert_eq!(count("pending-cap-replay"), 1);
        assert_eq!(
            count("pending-cap"),
            0,
            "an answered entry was reported as a WITHHELD ack"
        );

        // The control: the next arrival evicts an entry whose publisher is still
        // waiting. That is a real withhold and counts as one.
        let _ = register(&mut h, "t/over-2");
        assert!(h.pending_publishes.get(waiting).is_none());
        assert!(matches!(
            waiting_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        assert_eq!(count("pending-cap"), 1);
        assert_eq!(count("pending-cap-replay"), 1);
    }

    /// ADR 0083: the drain barrier cannot wait for a publish the pending bound has
    /// evicted, so it says how many of the messages the hub gated during the drain it
    /// lost that way — nobody retries those. A client's publish evicted while draining is
    /// not counted (its ack is withheld and its publisher retries), nor is an answered
    /// entry (it never held the barrier).
    #[test]
    fn the_drain_barrier_reports_the_drain_gated_publishes_the_bound_evicted() {
        let mut h = hub();
        h.draining = true;
        let (answered, _answered_rx) = register(&mut h, "t/0");
        assert!(h
            .pending_publishes
            .get_mut(answered)
            .unwrap()
            .answer(PublishOutcome::Accepted));
        let _client = register(&mut h, "t/1");
        let (gated, _gated_rx) = register(&mut h, "t/2");
        h.drain_gated.insert(gated);
        for i in 3..PENDING_PUBLISH_CAP {
            let _ = register(&mut h, &format!("t/{i}"));
        }
        let _ = register(&mut h, "t/over-1"); // evicts t/0, answered
        let _ = register(&mut h, "t/over-2"); // evicts t/1, a client's
        assert_eq!(h.drain_evicted, 0);
        let _ = register(&mut h, "t/over-3"); // evicts t/2, drain-gated
        assert_eq!(h.drain_evicted, 1);

        while h.pending_publishes.pop_first().is_some() {}
        let (reply, mut answer) = oneshot::channel();
        h.drained_waiters.push(reply);
        h.wake_drained();
        assert_eq!(answer.try_recv(), Ok(1));
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

    /// The settle window's record of whom a publish reached is charged to the
    /// byte bound and leaves with its entry: a high fan-out held open by a long
    /// window must not grow outside the bound.
    #[test]
    fn the_reached_record_is_charged_and_released_with_its_entry() {
        let mut hub = hub();
        let payload = Bytes::from_static(b"x");
        let (a, _wa) = register(&mut hub, "t/a", &payload);
        let base = pending_cost("t/a", &payload);
        let clients = |n: usize| (0..n).map(|i| ClientId(format!("c{i}").into()));

        hub.pending_reached(a, clients(3));
        let first = std::mem::size_of::<Vec<ClientId>>() + 3 * std::mem::size_of::<ClientId>();
        assert_eq!(hub.pending_publishes.bytes(), base + first);
        hub.pending_reached(a, clients(2));
        assert_eq!(
            hub.pending_publishes.bytes(),
            base + first + 2 * std::mem::size_of::<ClientId>()
        );
        assert_eq!(
            hub.pending_publishes[&a].reached.as_ref().map(|r| r.len()),
            Some(5)
        );

        hub.pending_publishes.remove(a);
        assert_eq!(
            hub.pending_publishes.bytes(),
            0,
            "the charge leaves with the entry"
        );
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
        // The peer has announced on the link, as every peer has a moment after it
        // forms: a forward to it is sent, not recorded for its announcement.
        hub.peer_announced_interest(&peer, Vec::new());
        let (id, _wait) = register(&mut hub, "t", &Bytes::from_static(b"x"));
        hub.send_acked_forward(id, &peer, false);
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

    /// Issue #644: a forward is overdue by the time since it was SENT, not since its
    /// publish was created. A takeover re-route registers a fresh obligation on a
    /// publish that is already several sweeps old; judged by `created_at`, the very
    /// next sweep re-sent it milliseconds after the first copy — and the receiver,
    /// which applies every copy, enqueued the message twice, so the subscriber got
    /// it twice with `DUP = 0`. (The nightly `cluster_proc` seed 2: a re-route and
    /// a back-to-back sweep tick 6 ms apart.)
    #[test]
    fn a_re_routed_forward_on_an_old_publish_is_not_retransmitted_before_it_is_due() {
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
        // The peer has announced on the link, as every peer has a moment after it
        // forms: a forward to it is sent, not recorded for its announcement.
        hub.peer_announced_interest(&peer, Vec::new());
        let (id, _wait) = register(&mut hub, "t", &Bytes::from_static(b"x"));
        // The publish has been pending for several sweeps (its first target died,
        // and its obligation was dropped with it).
        let back_date = |hub: &mut Hub, by: Duration| {
            let p = hub.pending_publishes.get_mut(id).unwrap();
            p.created_at = p
                .created_at
                .checked_sub(by)
                .expect("the clock is past the back-dated instant");
        };
        back_date(&mut hub, crate::hub::SESSION_SWEEP_INTERVAL * 8);

        // The re-route: a NEW obligation, sent now.
        hub.send_acked_forward(id, &peer, false);
        assert!(rx.try_recv().is_ok(), "the re-routed forward is sent");
        assert!(rx.try_recv().is_err());

        hub.sweep_pending_forwards();
        assert!(
            rx.try_recv().is_err(),
            "a forward sent milliseconds ago is not overdue, however old its publish \
             is: retransmitting it now puts a second copy on the receiver, which \
             applies both — a duplicate delivery with DUP = 0 (#644)"
        );

        // Once it has ITSELF waited a sweep interval unanswered, it is retransmitted.
        back_date(&mut hub, crate::hub::SESSION_SWEEP_INTERVAL);
        hub.sweep_pending_forwards();
        assert!(
            rx.try_recv().is_ok(),
            "an overdue re-route is retransmitted"
        );
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
            hub.peer_announced_interest(&peer, Vec::new());
            let payload = Bytes::from(vec![0u8; 216]);
            let mut waits = Vec::with_capacity(entries);
            for _ in 0..entries {
                let (id, wait) = register(&mut hub, "fleet/site/1/telemetry", &payload);
                hub.send_acked_forward(id, &peer, false);
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
