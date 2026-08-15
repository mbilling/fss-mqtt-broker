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

    /// The current time as whole MILLISECONDS since the Unix epoch.
    ///
    /// Second granularity is enough to *compare* absolute deadlines (expiry: a session that
    /// dies a fraction of a second late is nobody's incident), but not to *convert* one into
    /// a delay, because the conversion truncates twice and the two truncations do not
    /// cancel. Issue #299: a Will Delay Interval persisted as
    /// `floor(now) + delay` and re-armed as `deadline − floor(now)` elapsed
    /// `delay − frac(arm) + frac(re-arm)` — measured 7.312 s for an 8 s delay across a
    /// restart, i.e. 0.69 s EARLY. A will is a death announcement, so early is the one
    /// direction that lies; the persisted deadline is now rounded UP from this reading and
    /// the remaining time computed from it in millis, which puts every rounding error late.
    ///
    /// Defaulted as `now_epoch_secs() * 1000` so a whole-second test clock stays exactly
    /// whole-second — deterministic deadline tests must not acquire a sub-second component
    /// they cannot advance.
    fn now_epoch_millis(&self) -> u64 {
        self.now_epoch_secs().saturating_mul(1_000)
    }
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

    fn now_epoch_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

/// A shared handle to the production system clock — the default for a live broker.
#[must_use]
pub fn system_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}
