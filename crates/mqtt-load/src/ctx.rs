//! State every connection task of a run shares.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

use crate::config::Config;
use crate::ledger::Ledger;
use crate::stats::Stats;

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
    /// Set once the run is ending connections itself, so their closing is not an error.
    pub stopping: std::sync::atomic::AtomicBool,
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

    /// Deliveries the streams should have, given what was written so far.
    #[must_use]
    pub fn expected_deliveries(&self) -> u64 {
        self.ledger
            .streams
            .iter()
            .map(|s| self.topic_sent[s.topic].load(Ordering::Relaxed))
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
