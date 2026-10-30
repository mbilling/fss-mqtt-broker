//! Per-shard counters and histograms, and the collector's view of them.
//!
//! Connections record into one of a fixed number of shards (assigned round-robin),
//! once per batch — a batch being one socket read's worth of acks or deliveries, or
//! one burst of sends — so a lock is taken per batch, not per message, and shards
//! rarely contend. The collector drains the shards' histograms every tick; the
//! counters are cumulative and only read.

use std::sync::{Mutex, MutexGuard};

use crate::hist::Latency;

/// Counters and histograms one shard accumulates.
#[derive(Debug, Default, Clone)]
pub struct ShardData {
    /// Cumulative: PUBLISH packets written.
    pub sent: u64,
    /// Cumulative: positive PUBACKs read.
    pub acked: u64,
    /// Cumulative: deliveries of this run's messages read.
    pub received: u64,
    /// Cumulative: ns publishers spent with due messages and no send credit (the
    /// broker's Receive Maximum or the configured in-flight window was full).
    pub credit_stall_ns: u64,
    /// Cumulative: times a write found the socket buffer full.
    pub socket_blocked: u64,
    /// Cumulative: ns writers spent waiting for the socket to drain.
    pub socket_blocked_ns: u64,
    /// Drained per tick: PUBLISH written → PUBACK read.
    pub ack: Latency,
    /// Drained per tick: PUBLISH written → delivery read.
    pub e2e: Latency,
    /// Drained per tick: due → written, for sends nothing outside the generator held
    /// back (no credit wait, no full socket): the generator's own lateness.
    pub sched_lag: Latency,
    /// Drained per tick: due → written, every send.
    pub send_delay: Latency,
    /// Drained per tick: delivery read → its PUBACK written. The consumer's own part of
    /// the broker's write-elision hold (ADR 0089).
    pub puback_turnaround: Latency,
}

impl ShardData {
    /// Move this shard's histograms into `into`, leaving them empty.
    pub fn drain_into(&mut self, into: &mut Histos) {
        into.ack.add(&self.ack);
        into.e2e.add(&self.e2e);
        into.sched_lag.add(&self.sched_lag);
        into.send_delay.add(&self.send_delay);
        into.puback_turnaround.add(&self.puback_turnaround);
        self.ack.reset();
        self.e2e.reset();
        self.sched_lag.reset();
        self.send_delay.reset();
        self.puback_turnaround.reset();
    }
}

/// The latency histograms, accumulated.
#[derive(Debug, Default, Clone)]
pub struct Histos {
    /// Ack latency.
    pub ack: Latency,
    /// End-to-end latency.
    pub e2e: Latency,
    /// Generator scheduling lag.
    pub sched_lag: Latency,
    /// Due-to-sent delay, all sends.
    pub send_delay: Latency,
    /// Delivery read → PUBACK written.
    pub puback_turnaround: Latency,
}

impl Histos {
    /// Add `other`'s samples.
    pub fn add(&mut self, other: &Histos) {
        self.ack.add(&other.ack);
        self.e2e.add(&other.e2e);
        self.sched_lag.add(&other.sched_lag);
        self.send_delay.add(&other.send_delay);
        self.puback_turnaround.add(&other.puback_turnaround);
    }
}

/// Summed cumulative counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counters {
    /// PUBLISH packets written.
    pub sent: u64,
    /// Positive PUBACKs.
    pub acked: u64,
    /// Deliveries read.
    pub received: u64,
    /// Credit-stall ns.
    pub credit_stall_ns: u64,
    /// Full-socket events.
    pub socket_blocked: u64,
    /// Full-socket ns.
    pub socket_blocked_ns: u64,
}

impl Counters {
    /// `self - earlier`, saturating.
    #[must_use]
    pub fn since(&self, earlier: &Counters) -> Counters {
        Counters {
            sent: self.sent.saturating_sub(earlier.sent),
            acked: self.acked.saturating_sub(earlier.acked),
            received: self.received.saturating_sub(earlier.received),
            credit_stall_ns: self.credit_stall_ns.saturating_sub(earlier.credit_stall_ns),
            socket_blocked: self.socket_blocked.saturating_sub(earlier.socket_blocked),
            socket_blocked_ns: self
                .socket_blocked_ns
                .saturating_sub(earlier.socket_blocked_ns),
        }
    }
}

/// The shards.
#[derive(Debug)]
pub struct Stats {
    shards: Vec<Mutex<ShardData>>,
}

impl Stats {
    /// `n` empty shards.
    #[must_use]
    pub fn new(n: usize) -> Self {
        Self {
            shards: (0..n.max(1))
                .map(|_| Mutex::new(ShardData::default()))
                .collect(),
        }
    }

    /// How many shards there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards.len()
    }

    /// Never: there is always at least one shard.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Lock shard `i % len`.
    ///
    /// # Panics
    /// If a recording thread panicked while holding the lock.
    pub fn shard(&self, i: usize) -> MutexGuard<'_, ShardData> {
        self.shards[i % self.shards.len()]
            .lock()
            .expect("stats shard poisoned")
    }

    /// Drain every shard's histograms into `into` and return the summed counters.
    pub fn drain(&self, into: &mut Histos) -> Counters {
        let mut c = Counters::default();
        for i in 0..self.shards.len() {
            let mut s = self.shard(i);
            s.drain_into(into);
            c.sent += s.sent;
            c.acked += s.acked;
            c.received += s.received;
            c.credit_stall_ns += s.credit_stall_ns;
            c.socket_blocked += s.socket_blocked;
            c.socket_blocked_ns += s.socket_blocked_ns;
        }
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_moves_histograms_and_sums_counters() {
        let st = Stats::new(3);
        for i in 0..6 {
            let mut s = st.shard(i);
            s.sent += 2;
            s.acked += 1;
            s.ack.record_ns(1_000_000);
        }
        let mut h = Histos::default();
        let c = st.drain(&mut h);
        assert_eq!((c.sent, c.acked), (12, 6));
        assert_eq!(h.ack.count(), 6);
        // A second drain finds the histograms empty, the counters unchanged.
        let mut h2 = Histos::default();
        assert_eq!(st.drain(&mut h2), c);
        assert_eq!(h2.ack.count(), 0);
        assert_eq!(c.since(&Counters::default()), c);
    }
}
