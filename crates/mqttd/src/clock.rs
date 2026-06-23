//! An injectable wall-clock seam.
//!
//! Absolute-deadline logic — message expiry (ADR 0009 §3) computes and compares
//! whole Unix-epoch seconds — must be testable without real time passing. tokio's
//! paused-time test clock virtualizes timers (`sleep`, `interval`, `tokio::time::Instant`)
//! but **not** `SystemTime`, so epoch-second reads need their own seam. Monotonic
//! waits keep using tokio's time and need no abstraction here.

use std::sync::Arc;

/// A source of wall-clock time in whole Unix epoch seconds.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// The current time as whole seconds since the Unix epoch.
    fn now_epoch_secs(&self) -> u64;
}

/// The production clock: the system wall clock.
#[derive(Debug, Default, Clone)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_epoch_secs(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }
}

/// A shared handle to the production system clock — the default for a live broker.
#[must_use]
pub fn system_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}
