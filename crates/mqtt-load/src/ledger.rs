//! The identity ledger (ADR 0090 D1).
//!
//! Every message has an identity — (publisher, sequence) — and the ledger records,
//! per publisher, which identities were **sent** and which were **acked**, and per
//! delivery stream, which were **received**. A *stream* is one place every message on
//! a topic must arrive exactly once: a `$share` group (whichever member gets it), or
//! one plain fan-out subscriber. At the end of a run, after a drain:
//!
//! - **missing**: sent, and not received on a stream that should have received it;
//! - **unexpected**: received with an identity of this run that was never sent (or on
//!   a stream whose topic its publisher does not publish to);
//! - **unacked**: sent at `QoS` 1 and never acknowledged (a negative PUBACK leaves the
//!   identity unacked; those are also counted on their own);
//! - **unexpected acks**: a PUBACK for a packet id with nothing in flight;
//! - **duplicates**: a stream received an identity it already had. Counted, and do not
//!   open the ledger: `QoS` 1 may redeliver.
//!
//! The same four zero-checks as the lane E ledger (`bench/scale/lane-e-evidence.py`
//! `reconcile`), exact rather than sampled: a bit per identity, sized from the schedule,
//! which bounds every publisher's sequence numbers in advance.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

/// A fixed-size bitset whose bits can be set concurrently.
#[derive(Debug)]
pub struct AtomicBitset {
    words: Box<[AtomicU64]>,
    bits: u64,
}

/// The outcome of setting a bit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    /// The bit was clear.
    New,
    /// The bit was already set.
    Again,
    /// The index is outside the set.
    OutOfRange,
}

impl AtomicBitset {
    /// A set of `bits` clear bits.
    ///
    /// # Panics
    /// If the set would not fit in memory's address space.
    #[must_use]
    pub fn new(bits: u64) -> Self {
        let n = usize::try_from(bits.div_ceil(64)).expect("bitset fits in memory");
        Self {
            words: (0..n).map(|_| AtomicU64::new(0)).collect(),
            bits,
        }
    }

    /// Set bit `i`.
    #[must_use = "whether the bit was new is the duplicate check"]
    #[allow(clippy::cast_possible_truncation)] // i < bits, and bits / 64 words fit usize
    pub fn set(&self, i: u64) -> Mark {
        if i >= self.bits {
            return Mark::OutOfRange;
        }
        let w = &self.words[(i / 64) as usize];
        let mask = 1u64 << (i % 64);
        if w.fetch_or(mask, Ordering::Relaxed) & mask == 0 {
            Mark::New
        } else {
            Mark::Again
        }
    }

    /// Whether bit `i` is set.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // i < bits, and bits / 64 words fit usize
    pub fn get(&self, i: u64) -> bool {
        i < self.bits
            && self.words[(i / 64) as usize].load(Ordering::Relaxed) & (1 << (i % 64)) != 0
    }

    /// Set bits.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.words
            .iter()
            .map(|w| u64::from(w.load(Ordering::Relaxed).count_ones()))
            .sum()
    }

    /// Bits set here and in `also`, and clear in `not`.
    #[must_use]
    pub fn count_and_and_not(&self, also: &AtomicBitset, not: &AtomicBitset) -> u64 {
        self.words
            .iter()
            .enumerate()
            .map(|(i, w)| {
                let a = also.words.get(i).map_or(0, |o| o.load(Ordering::Relaxed));
                let n = not.words.get(i).map_or(0, |o| o.load(Ordering::Relaxed));
                u64::from((w.load(Ordering::Relaxed) & a & !n).count_ones())
            })
            .sum()
    }

    /// Bits set here and clear in `other`.
    #[must_use]
    pub fn count_and_not(&self, other: &AtomicBitset) -> u64 {
        self.words
            .iter()
            .enumerate()
            .map(|(i, w)| {
                let o = other.words.get(i).map_or(0, |o| o.load(Ordering::Relaxed));
                u64::from((w.load(Ordering::Relaxed) & !o).count_ones())
            })
            .sum()
    }
}

/// One publisher's sent and acked identities.
#[derive(Debug)]
pub struct PublisherLedger {
    /// Identities sent.
    pub sent: AtomicBitset,
    /// Identities positively acknowledged.
    pub acked: AtomicBitset,
}

/// One delivery stream's received identities, per publisher on its topic.
#[derive(Debug)]
pub struct StreamLedger {
    /// The topic index this stream receives.
    pub topic: usize,
    /// `received[k]` is for publisher `topic + k·topics` (publishers are assigned to
    /// topics round-robin).
    received: Vec<AtomicBitset>,
    /// Every delivery of this run's identities, duplicates included.
    pub deliveries: AtomicU64,
    /// First deliveries of an identity.
    pub unique: AtomicU64,
    /// Repeat deliveries of an identity.
    pub duplicates: AtomicU64,
    /// Deliveries of an identity this stream cannot have been sent.
    pub unexpected: AtomicU64,
}

/// The whole run's ledger.
#[derive(Debug)]
pub struct Ledger {
    /// Per publisher, by id.
    pub publishers: Vec<PublisherLedger>,
    /// Per stream.
    pub streams: Vec<StreamLedger>,
    topics: usize,
    /// PUBACKs for a packet id with nothing in flight.
    pub unexpected_acks: AtomicU64,
    /// PUBACKs with a failure reason code (>= 0x80).
    pub negative_acks: AtomicU64,
    /// Deliveries carrying another run's id.
    pub other_run: AtomicU64,
    /// Deliveries that are not generator payloads.
    pub foreign: AtomicU64,
    /// `QoS` 1 deliveries whose packet id was not the previous one's successor on their
    /// connection.
    pub pkid_gaps: AtomicU64,
}

/// The ledger at the end of a run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct LedgerReport {
    /// Identities sent.
    pub sent: u64,
    /// Identities positively acknowledged (`QoS` 1).
    pub acked: u64,
    /// Deliveries of this run's identities, duplicates included.
    pub received: u64,
    /// Distinct (stream, identity) deliveries.
    pub unique_received: u64,
    /// (stream, identity) pairs that should have been delivered.
    pub expected: u64,
    /// Expected and not delivered.
    pub missing: u64,
    /// The part of `missing` whose publish was positively acknowledged (`QoS` 1): the
    /// broker took responsibility for it and it never arrived. This is loss. The rest
    /// of `missing` was never acknowledged, which `QoS` 1 lets the publisher resend
    /// (for example on a connection the broker closed under overload). Always 0 at
    /// `QoS` 0, where nothing is acknowledged.
    pub missing_acked: u64,
    /// Delivered and never sent to that stream.
    pub unexpected: u64,
    /// Sent at `QoS` 1 and not positively acknowledged.
    pub unacked: u64,
    /// PUBACKs for nothing in flight.
    pub unexpected_acks: u64,
    /// PUBACKs with a failure reason code (their identities are also unacked).
    pub negative_acks: u64,
    /// Repeat deliveries (counted; they do not open the ledger).
    pub duplicates: u64,
    /// Deliveries from another run (a reused durable session's leftovers; not counted
    /// against this run).
    pub other_run: u64,
    /// Deliveries that were not generator payloads.
    pub foreign: u64,
    /// `QoS` 1 deliveries whose packet id skipped on their connection. Diagnostic,
    /// not loss by itself (a broker may skip an id still in flight, and a resumed
    /// session's redeliveries reuse old ids), and it does not open the ledger; but a
    /// broker that allocates ids in order and lost a PUBLISH after allocating its id
    /// shows each lost message here, so `missing` with as many `pkid_gaps` locates
    /// the loss on the broker's side of the subscriber's socket.
    pub pkid_gaps: u64,
    /// Nothing missing, unexpected, unacked, or acked unexpectedly; and something sent.
    pub closed: bool,
}

/// One missing identity, for diagnosis: where it should have arrived and what its
/// publisher's other messages did on that stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MissingId {
    /// The stream that should have received it.
    pub stream: usize,
    /// The stream's topic index.
    pub topic: usize,
    /// Publisher id.
    pub publisher: u32,
    /// Sequence number.
    pub seq: u64,
    /// Messages this publisher sent.
    pub publisher_sent: u64,
    /// Whether the stream received a later message from the same publisher (a gap,
    /// not a tail still in transit).
    pub later_received: bool,
}

impl Ledger {
    /// A ledger for `capacity.len()` publishers (publisher `i` can send at most
    /// `capacity[i]` messages), `topics` topics (publisher `i` publishes to topic
    /// `i % topics`), and one stream per entry of `stream_topics`.
    ///
    /// # Panics
    /// If `topics` is zero or a stream names a topic out of range.
    #[must_use]
    pub fn new(capacity: &[u64], topics: usize, stream_topics: &[usize]) -> Self {
        assert!(topics > 0);
        let publishers = capacity
            .iter()
            .map(|&c| PublisherLedger {
                sent: AtomicBitset::new(c),
                acked: AtomicBitset::new(c),
            })
            .collect();
        let streams = stream_topics
            .iter()
            .map(|&t| {
                assert!(t < topics, "stream topic out of range");
                StreamLedger {
                    topic: t,
                    received: (t..capacity.len())
                        .step_by(topics)
                        .map(|p| AtomicBitset::new(capacity[p]))
                        .collect(),
                    deliveries: AtomicU64::new(0),
                    unique: AtomicU64::new(0),
                    duplicates: AtomicU64::new(0),
                    unexpected: AtomicU64::new(0),
                }
            })
            .collect();
        Self {
            publishers,
            streams,
            topics,
            unexpected_acks: AtomicU64::new(0),
            negative_acks: AtomicU64::new(0),
            other_run: AtomicU64::new(0),
            foreign: AtomicU64::new(0),
            pkid_gaps: AtomicU64::new(0),
        }
    }

    /// The topic publisher `publisher` publishes to.
    #[must_use]
    pub fn topic_of(&self, publisher: usize) -> usize {
        publisher % self.topics
    }

    /// Record a delivery of (`publisher`, `seq`) on stream `stream`.
    pub fn receive(&self, stream: usize, publisher: u32, seq: u64) -> Mark {
        let s = &self.streams[stream];
        s.deliveries.fetch_add(1, Ordering::Relaxed);
        let p = publisher as usize;
        let mark = if p < self.publishers.len() && self.topic_of(p) == s.topic {
            s.received[p / self.topics].set(seq)
        } else {
            Mark::OutOfRange
        };
        match mark {
            Mark::New => s.unique.fetch_add(1, Ordering::Relaxed),
            Mark::Again => s.duplicates.fetch_add(1, Ordering::Relaxed),
            Mark::OutOfRange => s.unexpected.fetch_add(1, Ordering::Relaxed),
        };
        mark
    }

    /// Up to `limit` missing identities, in (stream, publisher, seq) order.
    #[must_use]
    pub fn missing_sample(&self, limit: usize) -> Vec<MissingId> {
        let mut out = Vec::new();
        for (si, s) in self.streams.iter().enumerate() {
            for (k, recv) in s.received.iter().enumerate() {
                let pi = s.topic + k * self.topics;
                let p = &self.publishers[pi];
                if p.sent.count_and_not(recv) == 0 {
                    continue;
                }
                let sent = p.sent.count();
                let last_recv = (0..p.sent.bits).rev().find(|&q| recv.get(q));
                for seq in 0..p.sent.bits {
                    if out.len() >= limit {
                        return out;
                    }
                    if p.sent.get(seq) && !recv.get(seq) {
                        out.push(MissingId {
                            stream: si,
                            topic: s.topic,
                            publisher: u32::try_from(pi).unwrap_or(u32::MAX),
                            seq,
                            publisher_sent: sent,
                            later_received: last_recv.is_some_and(|l| l > seq),
                        });
                    }
                }
            }
        }
        out
    }

    /// Reconcile. `qos1` decides whether unacked identities count.
    #[must_use]
    pub fn report(&self, qos1: bool) -> LedgerReport {
        let mut r = LedgerReport::default();
        for p in &self.publishers {
            r.sent += p.sent.count();
            if qos1 {
                r.acked += p.acked.count();
                r.unacked += p.sent.count_and_not(&p.acked);
                // An ack is only recorded for an identity in flight, so acked ⊆ sent
                // by construction; this checks it rather than assuming it.
                r.unexpected_acks += p.acked.count_and_not(&p.sent);
            }
        }
        for s in &self.streams {
            r.received += s.deliveries.load(Ordering::Relaxed);
            r.duplicates += s.duplicates.load(Ordering::Relaxed);
            r.unexpected += s.unexpected.load(Ordering::Relaxed);
            for (k, recv) in s.received.iter().enumerate() {
                let p = &self.publishers[s.topic + k * self.topics];
                r.expected += p.sent.count();
                r.unique_received += recv.count();
                r.missing += p.sent.count_and_not(recv);
                if qos1 {
                    r.missing_acked += p.sent.count_and_and_not(&p.acked, recv);
                }
                r.unexpected += recv.count_and_not(&p.sent);
            }
        }
        r.unexpected_acks += self.unexpected_acks.load(Ordering::Relaxed);
        r.negative_acks = self.negative_acks.load(Ordering::Relaxed);
        r.other_run = self.other_run.load(Ordering::Relaxed);
        r.foreign = self.foreign.load(Ordering::Relaxed);
        r.pkid_gaps = self.pkid_gaps.load(Ordering::Relaxed);
        r.closed = r.sent > 0
            && r.missing == 0
            && r.unexpected == 0
            && r.unacked == 0
            && r.unexpected_acks == 0;
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitset_marks_new_again_and_out_of_range() {
        let b = AtomicBitset::new(130);
        assert_eq!(b.set(0), Mark::New);
        assert_eq!(b.set(129), Mark::New);
        assert_eq!(b.set(129), Mark::Again);
        assert_eq!(b.set(130), Mark::OutOfRange);
        assert!(b.get(0) && b.get(129) && !b.get(64) && !b.get(1000));
        assert_eq!(b.count(), 2);
        let c = AtomicBitset::new(130);
        let _ = c.set(0);
        assert_eq!(b.count_and_not(&c), 1);
        assert_eq!(c.count_and_not(&b), 0);
    }

    /// Two topics, four publishers (0, 2 → topic 0; 1, 3 → topic 1), and three
    /// streams: a share group on each topic, plus a second subscriber on topic 0.
    fn ledger() -> Ledger {
        Ledger::new(&[10, 10, 10, 10], 2, &[0, 1, 0])
    }

    fn send_all(l: &Ledger, n: u64, ack: bool) {
        for p in &l.publishers {
            for s in 0..n {
                let _ = p.sent.set(s);
                if ack {
                    let _ = p.acked.set(s);
                }
            }
        }
    }

    fn deliver_all(l: &Ledger, n: u64) {
        for (i, s) in l.streams.iter().enumerate() {
            for p in (s.topic..4).step_by(2) {
                for seq in 0..n {
                    l.receive(i, u32::try_from(p).unwrap(), seq);
                }
            }
        }
    }

    #[test]
    fn a_complete_run_closes() {
        let l = ledger();
        send_all(&l, 10, true);
        deliver_all(&l, 10);
        let r = l.report(true);
        assert_eq!(r.sent, 40);
        assert_eq!(r.acked, 40);
        assert_eq!(r.expected, 60, "20 per stream");
        assert_eq!(r.unique_received, 60);
        assert_eq!(r.received, 60);
        assert!(r.closed, "{r:?}");
    }

    #[test]
    fn missing_is_exact_and_per_stream() {
        let l = ledger();
        send_all(&l, 10, true);
        deliver_all(&l, 10);
        // Stream 2 (the second topic-0 subscriber) lost publisher 2's seq 7, and the
        // topic-1 group never got publisher 3's seq 9 — they were never "delivered".
        let l2 = ledger();
        send_all(&l2, 10, true);
        for (i, s) in l2.streams.iter().enumerate() {
            for p in (s.topic..4).step_by(2) {
                for seq in 0..10 {
                    if (i, p, seq) == (2, 2, 7) || (i, p, seq) == (1, 3, 9) {
                        continue;
                    }
                    l2.receive(i, u32::try_from(p).unwrap(), seq);
                }
            }
        }
        let r = l2.report(true);
        assert_eq!(r.missing, 2);
        assert!(!r.closed);
        assert!(l.report(true).closed);
    }

    #[test]
    fn missing_sample_names_each_missing_identity_and_whether_it_is_a_gap() {
        let l = ledger();
        send_all(&l, 10, true);
        for (i, s) in l.streams.iter().enumerate() {
            for p in (s.topic..4).step_by(2) {
                for seq in 0..10 {
                    // Stream 1 lost publisher 3's seq 4 (a gap: 5..9 arrived) and seq 9
                    // (the tail).
                    if (i, p) == (1, 3) && (seq == 4 || seq == 9) {
                        continue;
                    }
                    l.receive(i, u32::try_from(p).unwrap(), seq);
                }
            }
        }
        let m = l.missing_sample(10);
        assert_eq!(m.len(), 2, "{m:?}");
        assert_eq!(
            (m[0].stream, m[0].topic, m[0].publisher, m[0].seq),
            (1, 1, 3, 4)
        );
        assert!(m[0].later_received);
        assert_eq!(m[0].publisher_sent, 10);
        assert_eq!((m[1].publisher, m[1].seq), (3, 9));
        assert!(!m[1].later_received);
        assert_eq!(l.missing_sample(1).len(), 1, "bounded");
        assert_eq!(l.report(true).missing, 2);
    }

    #[test]
    fn duplicates_are_counted_and_do_not_open_the_ledger() {
        let l = ledger();
        send_all(&l, 10, true);
        deliver_all(&l, 10);
        assert_eq!(l.receive(0, 2, 3), Mark::Again);
        assert_eq!(l.receive(0, 2, 3), Mark::Again);
        let r = l.report(true);
        assert_eq!(r.duplicates, 2);
        assert_eq!(r.received, 62);
        assert_eq!(r.unique_received, 60);
        assert!(r.closed);
    }

    #[test]
    fn unexpected_deliveries_open_the_ledger() {
        // Received but never sent; a publisher on the wrong topic; a seq past capacity.
        for (stream, publisher, seq) in [(0, 0, 5), (0, 1, 0), (1, 1, 99)] {
            let l = ledger();
            send_all(&l, 5, true);
            for (i, s) in l.streams.iter().enumerate() {
                for p in (s.topic..4).step_by(2) {
                    for q in 0..5 {
                        l.receive(i, u32::try_from(p).unwrap(), q);
                    }
                }
            }
            l.receive(stream, publisher, seq);
            let r = l.report(true);
            assert_eq!(r.unexpected, 1, "{stream} {publisher} {seq}: {r:?}");
            assert_eq!(r.missing, 0);
            assert!(!r.closed);
        }
    }

    #[test]
    fn missing_acked_counts_only_acknowledged_identities_that_never_arrived() {
        // Publisher 0 publishes to topic 0, which streams 0 and 2 both receive.
        let l = ledger();
        for seq in 0..10 {
            let _ = l.publishers[0].sent.set(seq);
        }
        for seq in 0..6 {
            let _ = l.publishers[0].acked.set(seq);
        }
        for stream in [0, 2] {
            for seq in (0..3).chain(6..8) {
                let _ = l.receive(stream, 0, seq);
            }
        }
        let r = l.report(true);
        // Each stream misses 3, 4, 5 (acknowledged: loss) and 8, 9 (never acknowledged).
        assert_eq!(r.missing, 10, "{r:?}");
        assert_eq!(r.missing_acked, 6, "{r:?}");
        assert_eq!(r.unacked, 4, "{r:?}");
        assert_eq!(
            l.report(false).missing_acked,
            0,
            "QoS 0 acknowledges nothing"
        );
    }

    #[test]
    fn unacked_counts_only_at_qos1() {
        let l = ledger();
        send_all(&l, 10, false);
        let _ = l.publishers[1].acked.set(0);
        deliver_all(&l, 10);
        let r1 = l.report(true);
        assert_eq!(r1.unacked, 39);
        assert!(!r1.closed);
        let r0 = l.report(false);
        assert_eq!(r0.unacked, 0);
        assert!(r0.closed);
    }

    #[test]
    fn unexpected_acks_open_the_ledger() {
        let l = ledger();
        send_all(&l, 10, true);
        deliver_all(&l, 10);
        l.unexpected_acks.fetch_add(1, Ordering::Relaxed);
        assert_eq!(l.report(true).unexpected_acks, 1);
        assert!(!l.report(true).closed);
        // An ack recorded for an identity never sent is caught too.
        let l = ledger();
        send_all(&l, 5, true);
        deliver_all(&l, 5);
        let _ = l.publishers[0].acked.set(8);
        assert_eq!(l.report(true).unexpected_acks, 1);
    }

    #[test]
    fn nothing_sent_is_not_a_closed_ledger() {
        assert!(!ledger().report(true).closed);
    }
}
