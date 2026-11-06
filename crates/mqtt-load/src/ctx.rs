//! State every connection task of a run shares.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::Serialize;

use crate::config::Config;
use crate::hist::Latency;
use crate::ledger::Ledger;
use crate::stats::Stats;
use crate::summary::{PublisherReconnects, ReconnectReport};

/// Connection-level failures during a run.
#[derive(Debug, Default)]
pub struct Errors {
    /// Connections the broker closed (or that failed) before the run ended them.
    pub disconnects: AtomicU64,
    /// DISCONNECT packets the broker sent.
    pub server_disconnects: AtomicU64,
    /// Malformed or unexpected packets from the broker.
    pub protocol: AtomicU64,
    /// Socket write failures.
    pub write: AtomicU64,
}

/// [`Errors`], read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ErrorCounts {
    /// Connections that failed to establish (the run does not start with any).
    pub connect: u64,
    /// Connections lost during the run.
    pub disconnects: u64,
    /// DISCONNECTs from the broker.
    pub server_disconnects: u64,
    /// Protocol errors.
    pub protocol: u64,
    /// Socket write failures.
    pub write: u64,
}

impl Errors {
    /// Read the counters.
    #[must_use]
    pub fn counts(&self) -> ErrorCounts {
        ErrorCounts {
            connect: 0,
            disconnects: self.disconnects.load(Ordering::Relaxed),
            server_disconnects: self.server_disconnects.load(Ordering::Relaxed),
            protocol: self.protocol.load(Ordering::Relaxed),
            write: self.write.load(Ordering::Relaxed),
        }
    }
}

/// What consumers that reconnect (`--reconnect`) went through: a broker closing a
/// consumer's connection is then not the run's error but an event, counted here, and
/// the consumer resumes its session.
#[derive(Debug, Default)]
pub struct Reconnects {
    /// Connections re-established.
    pub reconnects: AtomicU64,
    /// Durable resumes the broker answered with no session (session present 0).
    pub sessions_lost: AtomicU64,
    /// Consumers that gave up reconnecting.
    pub failed: AtomicU64,
    /// Closes by what the broker said ([`close_label`]).
    pub by_reason: Mutex<BTreeMap<String, u64>>,
    /// Each consumer's time without a connection, from the close to the session back.
    pub gaps: Mutex<Latency>,
    /// The publishers' side.
    pub publishers: PubReconnects,
}

/// What publishers that reconnect went through.
#[derive(Debug, Default)]
pub struct PubReconnects {
    /// Connections re-established.
    pub reconnects: AtomicU64,
    /// Messages in flight sent again on a new connection.
    pub resent: AtomicU64,
    /// Publishers that gave up reconnecting.
    pub failed: AtomicU64,
    /// Closes by what the broker said ([`close_label`]).
    pub by_reason: Mutex<BTreeMap<String, u64>>,
    /// Each publisher's time without a connection.
    pub gaps: Mutex<Latency>,
}

/// How a consumer's connection ended, as [`ReconnectReport::by_reason`] names it: the
/// broker's DISCONNECT reason code (`0x9c`, Use another server: a cluster moved the
/// session to another node, ADR 0005), or `closed` when it closed the socket without
/// one, or `write` when the consumer could not write to it.
#[must_use]
pub fn close_label(reason: Option<u8>, write_failed: bool) -> String {
    match reason {
        Some(r) => format!("0x{r:02x}"),
        None if write_failed => "write".to_string(),
        None => "closed".to_string(),
    }
}

impl Reconnects {
    /// Count a close.
    ///
    /// # Panics
    /// If another task panicked holding the lock.
    pub fn closed(&self, label: String) {
        *self
            .by_reason
            .lock()
            .expect("reconnect reasons poisoned")
            .entry(label)
            .or_default() += 1;
    }

    /// Count a publisher's close.
    ///
    /// # Panics
    /// If another task panicked holding the lock.
    pub fn publisher_closed(&self, label: String) {
        *self
            .publishers
            .by_reason
            .lock()
            .expect("reconnect reasons poisoned")
            .entry(label)
            .or_default() += 1;
    }

    /// The report; `enabled` is whether clients reconnect at all.
    ///
    /// # Panics
    /// If another task panicked holding a lock.
    #[must_use]
    pub fn report(&self, enabled: bool) -> ReconnectReport {
        ReconnectReport {
            enabled,
            reconnects: self.reconnects.load(Ordering::Relaxed),
            by_reason: self
                .by_reason
                .lock()
                .expect("reconnect reasons poisoned")
                .clone(),
            sessions_lost: self.sessions_lost.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            gap: self.gaps.lock().expect("reconnect gaps poisoned").summary(),
            publishers: PublisherReconnects {
                reconnects: self.publishers.reconnects.load(Ordering::Relaxed),
                resent: self.publishers.resent.load(Ordering::Relaxed),
                failed: self.publishers.failed.load(Ordering::Relaxed),
                by_reason: self
                    .publishers
                    .by_reason
                    .lock()
                    .expect("reconnect reasons poisoned")
                    .clone(),
                gap: self
                    .publishers
                    .gaps
                    .lock()
                    .expect("reconnect gaps poisoned")
                    .summary(),
            },
        }
    }
}

/// The run's shared state.
#[derive(Debug)]
pub struct Ctx {
    /// The workload.
    pub cfg: Config,
    /// This run's id, stamped into every payload.
    pub run: u32,
    /// The identity ledger.
    pub ledger: Ledger,
    /// Counters and histograms.
    pub stats: Stats,
    /// Payload padding.
    pub filler: Vec<u8>,
    /// Messages written, per topic (the drain's "expected" figure).
    pub topic_sent: Vec<AtomicU64>,
    /// Deliveries read, per consumer connection (in [`Config::consumer_specs`] order):
    /// how evenly the broker spread them ([`crate::summary::Spread`]).
    pub consumer_received: Vec<AtomicU64>,
    /// Last time an ack or delivery was read, ns.
    pub last_progress_ns: AtomicU64,
    /// Failures.
    pub errors: Errors,
    /// Consumer reconnects (`--reconnect`).
    pub reconnects: Reconnects,
    /// Set once the run is ending connections itself, so their closing is not an error.
    pub stopping: std::sync::atomic::AtomicBool,
    /// Set once the run is ending its publishers' connections, the teardown's first half:
    /// the consumers stay connected through the settle, where the broker closing one is
    /// still an error.
    pub publishers_stopping: std::sync::atomic::AtomicBool,
}

impl Ctx {
    /// Note that an ack or delivery was read at `now`.
    pub fn progress(&self, now: u64) {
        self.last_progress_ns.fetch_max(now, Ordering::Relaxed);
    }

    /// Whether the run is ending connections itself.
    #[must_use]
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Relaxed)
    }

    /// Whether the run is ending its publishers' connections itself.
    #[must_use]
    pub fn publishers_stopping(&self) -> bool {
        self.publishers_stopping.load(Ordering::Relaxed) || self.is_stopping()
    }

    /// Deliveries the streams should have, given what was written so far.
    #[must_use]
    pub fn expected_deliveries(&self) -> u64 {
        let every_topic = || {
            self.topic_sent
                .iter()
                .map(|n| n.load(Ordering::Relaxed))
                .sum::<u64>()
        };
        self.ledger
            .streams
            .iter()
            .map(|s| match s.topic {
                Some(t) => self.topic_sent[t].load(Ordering::Relaxed),
                None => every_topic(),
            })
            .sum()
    }

    /// Distinct deliveries so far.
    #[must_use]
    pub fn unique_deliveries(&self) -> u64 {
        self.ledger
            .streams
            .iter()
            .map(|s| s.unique.load(Ordering::Relaxed))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_close_is_named_by_the_brokers_reason_or_how_the_socket_ended() {
        assert_eq!(close_label(Some(0x9c), false), "0x9c");
        assert_eq!(
            close_label(Some(0x8e), true),
            "0x8e",
            "the broker's word wins"
        );
        assert_eq!(close_label(None, false), "closed");
        assert_eq!(close_label(None, true), "write");
        let r = Reconnects::default();
        r.closed(close_label(Some(0x9c), false));
        r.closed(close_label(Some(0x9c), false));
        r.closed(close_label(None, false));
        r.reconnects.fetch_add(3, Ordering::Relaxed);
        r.gaps.lock().unwrap().record_ns(12_000_000);
        r.publisher_closed(close_label(None, false));
        r.publishers.resent.fetch_add(40, Ordering::Relaxed);
        let rep = r.report(true);
        assert_eq!(rep.publishers.by_reason.get("closed"), Some(&1));
        assert_eq!(rep.publishers.resent, 40);
        assert!(rep.enabled);
        assert_eq!(rep.reconnects, 3);
        assert_eq!(rep.by_reason.get("0x9c"), Some(&2));
        assert_eq!(rep.by_reason.get("closed"), Some(&1));
        assert_eq!(rep.gap.count, 1);
        assert!((11_900..=12_100).contains(&rep.gap.max_us), "{:?}", rep.gap);
    }
}
