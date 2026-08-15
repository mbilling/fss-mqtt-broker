//! Per-subscriber backpressure bounds: the operator-settable limits on the
//! **in-memory** structures one stalled subscriber can grow, and the exact byte
//! accounting the byte-shaped ones rest on (issue #241, ADR 0041 T10).
//!
//! ## What this bounds — RAM, per online subscriber, per node
//!
//! A single stalled subscriber holds three per-subscriber in-memory structures, and
//! before this module only the first was bounded at all, in messages, by a hard-coded
//! constant:
//!
//! | structure | bound | mechanism |
//! |---|---|---|
//! | flow-control backlog ([`BacklogQueue`]) | [`SubscriberLimits::max_backlog_messages`] + [`max_backlog_bytes`](SubscriberLimits::max_backlog_bytes) | drop-oldest (ADR 0012, unchanged) |
//! | in-flight window (`Inflight::pending`) | [`SubscriberLimits::max_inflight_messages`] | a pure GATE on the effective outbound Receive Maximum — the surplus diverts into the backlog, nothing is dropped |
//! | outbound socket channel (`Outbound`) | [`SubscriberLimits::max_outbound_bytes`] | shed `QoS` 0 only, at the existing gate site (#123) |
//!
//! **None of these bound disk.** The per-session *durable offline queue* is a different
//! structure with its own count cap (`MQTTD_MAX_QUEUED_MESSAGES`) and overflow policy
//! (`MQTTD_QUEUE_OVERFLOW`), enforced in the store; the aggregate disk bound is the
//! `MQTTD_STORE_MAX_BYTES` watermark. A byte cap for the durable queue is deliberately
//! still open (0041-T6): the store enforces its count in O(1) from the log's live range
//! without ever materializing the queue, and an exact byte total there needs a
//! *persisted* per-session counter that stays exact across append, truncate, crash
//! recovery, quorum replication and follower nodes — a counter that drifts fires the cap
//! at the wrong time and makes the operator's disk arithmetic wrong, which is worse than
//! no counter. A byte eviction here does release its entry's offset and truncate, so the
//! RAM cap shrinks the durable log *earlier*; it does not bound it.
//!
//! ## What a message's bytes ARE
//!
//! ```text
//! message_bytes(m) = ENTRY_OVERHEAD + m.topic.len() + m.payload.len() + m.app.accounted_bytes()
//! ```
//!
//! Not payload-only: topics and user properties are publisher-controlled and forwarded
//! verbatim, so a payload-only counter could be evaded by a factor of hundreds. Not the
//! encoded packet length either: the encoding is version-dependent (a v5 property block,
//! a topic-alias substitution, v3.1.1's absence of both), and ONE queued entry is
//! delivered to subscribers on different versions with different Maximum Packet Sizes —
//! so "the encoded size" is a property of a `(subscriber, entry)` pair, not of the
//! entry, and a total that must be recomputed per observer cannot be the single running
//! number the operator's arithmetic needs. The accounted size sits within a few dozen
//! bytes of the v5 encoding for any realistic message.
//!
//! The counter is exactly the **sum of `message_bytes` over the resident entries** — it
//! is not a heap measurement. Real RSS per entry is higher (allocator rounding, the
//! `VecDeque`'s spare capacity), so `MQTTD_MAX_BACKLOG_BYTES` bounds *message bytes held
//! for one subscriber*, and `docs/SIZING.md` keeps its RSS allowance rather than
//! pretending the cap is an RSS ceiling.
//!
//! ## Why the queue lives here and not in `hub.rs`
//!
//! Rust privacy is per-module. A `bytes` counter next to a `VecDeque` in an
//! 18 000-line module would be writable by all of that module, and exactness would rest
//! on every future edit remembering to adjust it. Here [`BacklogQueue`]'s fields are
//! private, the mutators are the only way in, and each pairs its `q` mutation with its
//! `bytes` adjustment in the same expression. The compiler, not a convention, is what
//! keeps the total exact — and a mutation site that does not exist as a method cannot be
//! added by accident.

use std::collections::VecDeque;

use mqtt_codec::Packet;
use mqtt_core::Message;
use mqtt_storage::Offset;

/// The fixed per-entry envelope charged on top of a message's variable-length bytes:
/// the queue slot, the [`Message`] struct, the `Bytes` header, and the
/// expiry/offset/retain fields around it.
///
/// Guarded by the compile-time asserts below so it cannot silently become an
/// under-count as either struct grows. If one trips, raise this constant **and update
/// the worst-case arithmetic in `README.md` and `docs/SIZING.md` in the same commit** —
/// the number is documented, so a change to it is a documentation change.
pub const ENTRY_OVERHEAD: usize = 256;

const _: () = assert!(std::mem::size_of::<BacklogEntry>() <= ENTRY_OVERHEAD);
const _: () = assert!(std::mem::size_of::<Packet>() <= ENTRY_OVERHEAD);

/// Today's hard-coded `MAX_BACKLOG`, now the default of
/// [`SubscriberLimits::max_backlog_messages`].
///
/// One definition in the tree: the constant moved here rather than being copied, so the
/// "unset config behaves exactly as before" claim is structural rather than a matching
/// pair of literals.
pub const DEFAULT_MAX_BACKLOG_MESSAGES: usize = 10_000;

/// The operator-settable bounds on what ONE online subscriber may hold in memory
/// (issue #241). Read at startup; a reload reports `limits` as requires-restart
/// (ADR 0041 §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriberLimits {
    /// Messages the flow-control backlog may hold before drop-oldest evicts
    /// (`MQTTD_MAX_BACKLOG_MESSAGES`). There is deliberately no "unbounded" setting:
    /// ADR 0012 requires this structure be bounded.
    pub max_backlog_messages: usize,
    /// Accounted bytes the flow-control backlog may hold before drop-oldest evicts
    /// (`MQTTD_MAX_BACKLOG_BYTES`). `None` = off, which is exactly today's behaviour.
    pub max_backlog_bytes: Option<usize>,
    /// Accounted bytes that may sit unwritten in one client's outbound channel before
    /// `QoS` 0 is shed (`MQTTD_MAX_OUTBOUND_BYTES`). `None` = off; the packet-count cap
    /// (`MAX_OUTBOUND_QUEUE`) applies either way.
    pub max_outbound_bytes: Option<usize>,
    /// A ceiling on the effective outbound Receive Maximum
    /// (`MQTTD_MAX_INFLIGHT_MESSAGES`): the broker sends at most
    /// `min(client Receive Maximum, this)` unacked `QoS` > 0 publishes. `None` = the
    /// client's own value verbatim (`u16::MAX` for every v3.1.1 client and any v5 client
    /// that sends no property). A pure gate — the surplus waits in the backlog.
    pub max_inflight_messages: Option<u16>,
}

impl Default for SubscriberLimits {
    /// Exactly today's behaviour: the former `MAX_BACKLOG` count, and the byte
    /// dimension **off** rather than defaulted.
    ///
    /// The byte cap's only enforcement mechanism is evicting already-acked, already
    /// durable messages. Any finite default would mean an operator upgrades, changes no
    /// configuration, and the broker starts silently discarding messages it previously
    /// delivered — the one direction that also breaks a durability claim. There is no
    /// safe number to guess, so there is no number.
    fn default() -> Self {
        Self {
            max_backlog_messages: DEFAULT_MAX_BACKLOG_MESSAGES,
            max_backlog_bytes: None,
            max_outbound_bytes: None,
            max_inflight_messages: None,
        }
    }
}

/// Which bound fired an eviction — a log field, never a metric label (the counter stays
/// `publish_dropped{reason="backlog-overflow"}` with its existing label set).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacklogBound {
    /// The message-count bound.
    Messages,
    /// The byte bound.
    Bytes,
}

impl BacklogBound {
    /// The word used in the log line.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::Bytes => "bytes",
        }
    }
}

/// A `QoS` > 0 message held back because the session's Receive Maximum quota is full
/// (ADR 0012) — or, for a `QoS` 0, parked purely to keep per-client wire order
/// (issue #242 finding A). It has no packet id yet: one is assigned when it is finally
/// sent.
#[derive(Debug)]
pub struct BacklogEntry {
    pub(crate) message: Message,
    pub(crate) retain: bool,
    pub(crate) message_expiry: Option<u32>,
    /// Its durable log offset, as for `PendingOut::offset` (#124).
    pub(crate) offset: Option<Offset>,
}

impl BacklogEntry {
    /// Assemble an entry. The only constructor, so every entry that can enter a
    /// [`BacklogQueue`] is one whose size the queue can account for.
    #[must_use]
    pub fn new(
        message: Message,
        retain: bool,
        message_expiry: Option<u32>,
        offset: Option<Offset>,
    ) -> Self {
        Self {
            message,
            retain,
            message_expiry,
            offset,
        }
    }

    /// Its accounted size, by the definition in the module docs.
    #[must_use]
    pub fn accounted_bytes(&self) -> usize {
        message_bytes(&self.message)
    }
}

/// One subscriber's flow-control backlog: a FIFO of not-yet-sent `QoS` > 0 deliveries
/// (plus order-parked `QoS` 0s), bounded in **both** messages and bytes, with a running
/// byte total that cannot drift.
///
/// The fields are private to this module — that is the point of the module. Every
/// mutator below adjusts `bytes` in the same expression as its `q` mutation, so the
/// total is exact at every observation point, and `hub.rs` cannot reach past them.
#[derive(Debug, Default)]
pub struct BacklogQueue {
    q: VecDeque<BacklogEntry>,
    bytes: usize,
}

impl BacklogQueue {
    /// Append `entry`, then evict from the FRONT until both bounds hold (drop-oldest,
    /// ADR 0012, policy unchanged). Returns the evicted entries oldest-first — the
    /// caller must release each one's durable offset, since nothing will deliver it and
    /// an offset owed forever would stop the log ever being truncated — paired with the
    /// bound that evicted it.
    ///
    /// The byte bound may evict several entries where the count bound evicts exactly
    /// one. `q.len() > 1` is a **forward-progress invariant**: the just-pushed entry is
    /// never evicted, so a message larger than the whole byte cap is delivered rather
    /// than dropped forever (and the overshoot it costs is in the documented
    /// arithmetic). With the defaults the boundary is bit-identical to the former
    /// `len() >= MAX_BACKLOG` pre-check: 10 000 pushes evict nothing, the 10 001st
    /// evicts exactly the oldest.
    pub fn push_back_capped(
        &mut self,
        entry: BacklogEntry,
        limits: &SubscriberLimits,
    ) -> Vec<(BacklogEntry, BacklogBound)> {
        self.bytes += entry.accounted_bytes();
        self.q.push_back(entry);
        let mut evicted = Vec::new();
        while self.q.len() > 1 {
            let bound = if self.q.len() > limits.max_backlog_messages {
                BacklogBound::Messages
            } else if limits.max_backlog_bytes.is_some_and(|c| self.bytes > c) {
                BacklogBound::Bytes
            } else {
                break;
            };
            match self.pop_front() {
                Some(e) => evicted.push((e, bound)),
                None => break,
            }
        }
        evicted
    }

    /// Return an ALREADY-ADMITTED delivery to the front, without evicting anything.
    ///
    /// Every caller is re-parking a delivery that was admitted and then could not go
    /// out (its packet-id block was spent, its outbound-id record write failed, its lane
    /// was full). Evicting here would either lose an already-acked message or invert
    /// per-subscriber wire order, so it does not — the total stays exact, and the
    /// overshoot is at most one entry per subscriber because each of those gates is
    /// single-flight.
    pub fn push_front_admitted(&mut self, entry: BacklogEntry) {
        self.bytes += entry.accounted_bytes();
        self.q.push_front(entry);
    }

    /// Take the oldest entry.
    pub fn pop_front(&mut self) -> Option<BacklogEntry> {
        let entry = self.q.pop_front()?;
        self.bytes -= entry.accounted_bytes();
        Some(entry)
    }

    /// Take every entry, leaving the queue empty and its total at zero (the detach
    /// spill).
    pub fn drain_all(&mut self) -> Vec<BacklogEntry> {
        self.bytes = 0;
        self.q.drain(..).collect()
    }

    /// Entries resident.
    #[must_use]
    pub fn len(&self) -> usize {
        self.q.len()
    }

    /// Whether the backlog is empty (the ordering gate every `QoS` > 0 send consults).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }

    /// Accounted bytes resident: exactly the sum of [`message_bytes`] over the entries
    /// currently held.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The oldest entry, for tests that assert FIFO order survived an eviction.
    #[cfg(test)]
    #[must_use]
    pub fn front(&self) -> Option<&BacklogEntry> {
        self.q.front()
    }

    /// The newest entry, for tests that assert FIFO order survived an eviction.
    #[cfg(test)]
    #[must_use]
    pub fn back(&self) -> Option<&BacklogEntry> {
        self.q.back()
    }

    /// The total recomputed from the resident entries — the independent witness the
    /// exactness test compares [`bytes`](Self::bytes) against. Deliberately walks the
    /// queue rather than trusting the counter.
    #[cfg(test)]
    #[must_use]
    pub fn recomputed_bytes(&self) -> usize {
        self.q.iter().map(BacklogEntry::accounted_bytes).sum()
    }
}

/// A message's accounted size: the per-entry envelope plus every variable-length,
/// publisher-controlled byte the broker holds for it. See the module docs for why this
/// and not the payload alone or the encoded packet.
#[must_use]
pub fn message_bytes(m: &Message) -> usize {
    ENTRY_OVERHEAD + m.topic.len() + m.payload.len() + m.app.accounted_bytes()
}

/// A queued packet's accounted size, by the same definition — so what the outbound
/// channel adds and what its reader subtracts are the same pure function of the same
/// packet.
///
/// Control packets take the envelope alone: they are small, and they are never shed, so
/// the only thing their size has to do is not lie about the class that IS shed.
#[must_use]
pub fn packet_bytes(p: &Packet) -> usize {
    match p {
        Packet::Publish(p) => {
            ENTRY_OVERHEAD + p.topic.len() + p.payload.len() + p.properties.accounted_bytes()
        }
        _ => ENTRY_OVERHEAD,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        message_bytes, packet_bytes, BacklogBound, BacklogEntry, BacklogQueue, SubscriberLimits,
        DEFAULT_MAX_BACKLOG_MESSAGES, ENTRY_OVERHEAD,
    };
    use bytes::Bytes;
    use mqtt_codec::{Packet, QoS};
    use mqtt_core::{AppProperties, Message};

    /// A message whose accounted size is exactly `want` bytes.
    fn sized(topic: &str, want: usize) -> Message {
        let fixed = ENTRY_OVERHEAD + topic.len();
        assert!(
            want >= fixed,
            "asked for a message smaller than its envelope"
        );
        Message::new(
            topic.to_string(),
            Bytes::from(vec![0u8; want - fixed]),
            QoS::AtLeastOnce,
            false,
        )
    }

    fn entry(topic: &str, want: usize) -> BacklogEntry {
        BacklogEntry::new(sized(topic, want), false, None, None)
    }

    fn limits(messages: usize, bytes: Option<usize>) -> SubscriberLimits {
        SubscriberLimits {
            max_backlog_messages: messages,
            max_backlog_bytes: bytes,
            ..SubscriberLimits::default()
        }
    }

    /// Issue #241's acceptance criterion, literally: N messages of KNOWN size must be
    /// evicted at the configured BYTE bound, with the count bound never approached.
    #[test]
    fn the_byte_bound_evicts_before_the_count_bound() {
        let kib = 1024;
        let lim = limits(DEFAULT_MAX_BACKLOG_MESSAGES, Some(8 * kib));
        let mut q = BacklogQueue::default();
        let mut evictions = 0;
        for i in 0..40 {
            for (_, bound) in q.push_back_capped(entry(&format!("t{i}"), kib), &lim) {
                assert_eq!(bound, BacklogBound::Bytes, "the byte bound is what fired");
                evictions += 1;
            }
        }
        assert_eq!(
            q.len(),
            8,
            "the queue rests at the byte bound, not the count"
        );
        assert_eq!(q.bytes(), 8 * kib);
        assert_eq!(evictions, 32, "every push past the 8th evicted exactly one");
        assert!(
            q.len() < lim.max_backlog_messages,
            "the count bound was never approached"
        );
        // Drop-oldest: the survivors are the NEWEST 8, still in FIFO order.
        assert_eq!(q.front().unwrap().message.topic, "t32");
        assert_eq!(q.back().unwrap().message.topic, "t39");
    }

    /// The count bound still bites when IT is the tighter one — the byte cap is an
    /// additional dimension, not a replacement.
    #[test]
    fn the_count_bound_still_bites_when_it_is_the_tighter_one() {
        let lim = limits(3, Some(1 << 20));
        let mut q = BacklogQueue::default();
        let mut bounds = Vec::new();
        for i in 0..6 {
            for (_, bound) in q.push_back_capped(entry(&format!("t{i}"), 512), &lim) {
                bounds.push(bound);
            }
        }
        assert_eq!(q.len(), 3);
        assert_eq!(bounds, vec![BacklogBound::Messages; 3]);
        assert_eq!(q.front().unwrap().message.topic, "t3");
        assert_eq!(q.bytes(), q.recomputed_bytes());
    }

    /// The load-bearing invariant: the running total equals a recomputed sum after
    /// EVERY mutation. A counter that drifts fires the cap at the wrong time and makes
    /// the operator's arithmetic wrong, which is worse than no counter — so this walks
    /// every mutation path the queue has.
    #[test]
    fn the_byte_counter_equals_a_recomputed_sum_after_every_mutation() {
        let lim = limits(4, Some(4096));
        let mut q = BacklogQueue::default();
        let check = |q: &BacklogQueue, step: &str| {
            assert_eq!(
                q.bytes(),
                q.recomputed_bytes(),
                "counter drifted after {step}"
            );
        };

        // push_back of three different sizes.
        q.push_back_capped(entry("a", 300), &lim);
        check(&q, "push a/300");
        q.push_back_capped(entry("bb", 700), &lim);
        check(&q, "push bb/700");
        q.push_back_capped(entry("ccc", 1500), &lim);
        check(&q, "push ccc/1500");

        // A re-parked already-admitted delivery (the deferred-send paths).
        q.push_front_admitted(entry("front", 400));
        check(&q, "push_front_admitted front/400");

        // A drain step (the ack-driven send).
        assert_eq!(q.pop_front().unwrap().message.topic, "front");
        check(&q, "pop_front");

        // A push that must evict several entries to satisfy the byte bound.
        let evicted = q.push_back_capped(entry("big", 3000), &lim);
        check(&q, "push big/3000");
        assert!(
            evicted.len() >= 2,
            "the byte bound evicts as many as it needs: {}",
            evicted.len()
        );
        assert!(evicted.iter().all(|(_, b)| *b == BacklogBound::Bytes));

        // A push that trips the COUNT bound instead.
        for i in 0..6 {
            q.push_back_capped(entry(&format!("z{i}"), 300), &lim);
            check(&q, "count-bound push");
        }
        assert_eq!(q.len(), 4);

        // The detach spill.
        let all = q.drain_all();
        assert!(!all.is_empty());
        check(&q, "drain_all");
        assert_eq!(q.bytes(), 0, "an emptied queue holds zero bytes");
        assert!(q.is_empty());

        // And it starts counting again from zero after the spill.
        q.push_back_capped(entry("after", 900), &lim);
        check(&q, "push after the spill");
        assert_eq!(q.bytes(), 900);
    }

    /// Forward progress: a message larger than the whole byte cap must be DELIVERED,
    /// not dropped forever. The just-pushed entry is never evicted.
    #[test]
    fn an_entry_larger_than_the_whole_byte_cap_is_kept_so_delivery_progresses() {
        let lim = limits(DEFAULT_MAX_BACKLOG_MESSAGES, Some(4096));
        let mut q = BacklogQueue::default();
        let evicted = q.push_back_capped(entry("huge", 64 * 1024), &lim);
        assert!(evicted.is_empty(), "nothing to evict, and not itself");
        assert_eq!(q.len(), 1);
        assert_eq!(q.bytes(), 64 * 1024);

        // The next push evicts the oversized entry and keeps itself, by the same rule.
        let evicted = q.push_back_capped(entry("next", 64 * 1024), &lim);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].0.message.topic, "huge");
        assert_eq!(q.len(), 1);
        assert_eq!(q.back().unwrap().message.topic, "next");
        assert_eq!(q.bytes(), q.recomputed_bytes());
    }

    /// The no-silent-change contract, asserted rather than assumed: unset limits are
    /// today's hard-coded count with the byte dimension OFF.
    #[test]
    fn the_default_limits_are_todays_hard_coded_bound() {
        let d = SubscriberLimits::default();
        assert_eq!(d.max_backlog_messages, 10_000, "the former MAX_BACKLOG");
        assert_eq!(
            d.max_backlog_bytes, None,
            "a byte cap defaults to OFF: any finite default would start shedding \
             already-acked messages in a deployment that changed no configuration"
        );
        assert_eq!(d.max_outbound_bytes, None);
        assert_eq!(d.max_inflight_messages, None);

        // And with the defaults, no number of pushes of any size evicts before the
        // 10 001st — the byte dimension is genuinely inert.
        let mut q = BacklogQueue::default();
        for i in 0..DEFAULT_MAX_BACKLOG_MESSAGES {
            assert!(
                q.push_back_capped(entry(&format!("t{i}"), 4096), &d)
                    .is_empty(),
                "eviction at push {i} — an unset byte cap changed behaviour"
            );
        }
        let evicted = q.push_back_capped(entry("overflow", 4096), &d);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].1, BacklogBound::Messages);
    }

    /// The documented size definition, pinned to the code: the env table's arithmetic
    /// is only trustworthy if this is what the counter counts.
    #[test]
    fn message_bytes_counts_topic_payload_and_forwarded_properties() {
        let mut m = Message::new(
            "a/b/c".to_string(), // 5
            Bytes::from(vec![7u8; 100]),
            QoS::AtLeastOnce,
            false,
        );
        m.app = AppProperties {
            payload_format: Some(1),                       // 1
            content_type: Some("application/json".into()), // 16
            response_topic: None,
            correlation_data: None,
            user_properties: vec![
                ("k1".to_string(), "value-one".to_string()), // 2 + 9
                ("k2".to_string(), "v2".to_string()),        // 2 + 2
            ],
        };
        let props = 1 + 16 + (2 + 9) + (2 + 2);
        assert_eq!(message_bytes(&m), ENTRY_OVERHEAD + 5 + 100 + props);
        // The evasion this definition exists to close: a payload-only count would
        // ignore every one of those publisher-controlled bytes.
        assert_ne!(
            message_bytes(&m),
            ENTRY_OVERHEAD + 5 + 100,
            "property bytes must be counted"
        );
    }

    /// The channel counter's size function is the same definition, and a control packet
    /// costs the envelope alone.
    #[test]
    fn packet_bytes_matches_message_bytes_for_a_publish() {
        use mqtt_codec::packet::{Ack, Publish};
        let m = sized("t/1", 2048);
        let pkt = Packet::Publish(Publish {
            dup: false,
            qos: m.qos,
            retain: false,
            topic: m.topic.clone(),
            pkid: Some(1),
            properties: mqtt_codec::Properties::new(),
            payload: m.payload.clone(),
        });
        assert_eq!(packet_bytes(&pkt), message_bytes(&m));
        assert_eq!(
            packet_bytes(&Packet::PubAck(Ack::new(1))),
            ENTRY_OVERHEAD,
            "a control packet costs the envelope alone"
        );

        // …AND with every forwardable application property set, which is the case the
        // identity above could not observe: built with `Properties::new()`, it was blind to
        // any per-property disagreement. It found one — `AppProperties` counted a
        // payload-format indicator as 1 byte while `Properties` counted it as 0, so the two
        // "one shared definition" functions differed by exactly 1 for every message carrying
        // that property, which `hub.rs` sets on forwarded publishes.
        let app = mqtt_core::AppProperties {
            payload_format: Some(1),
            content_type: Some("application/json".into()),
            response_topic: Some("reply/here".into()),
            correlation_data: Some(bytes::Bytes::from_static(b"corr-id")),
            user_properties: vec![("tenant".into(), "acme".into())],
        };
        // Note there is no `..default()`: every field of `AppProperties` is set above, so
        // this identity covers EVERY forwardable property. If a field is added later this
        // stops compiling, which is the right failure — a new property that only one of the
        // two byte definitions counts is exactly the divergence this test now exists for.
        let m2 = Message {
            app: app.clone(),
            ..sized("t/2", 512)
        };
        // Built by the broker's OWN constructor, so the identity is asserted against the
        // packet that actually goes on the wire rather than a hand-assembled lookalike.
        let pkt2 = crate::hub::publish_packet(
            &m2.topic,
            m2.payload.clone(),
            m2.qos,
            Some(2),
            false,
            false,
            None,
            &app,
        );
        assert_eq!(
            packet_bytes(&pkt2),
            message_bytes(&m2),
            "the RAM definition and the wire definition must agree on EVERY forwardable \
             property, or a cap sized from one fires at the wrong size on the other"
        );
    }
}
