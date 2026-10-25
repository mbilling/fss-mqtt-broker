//! The one clock (ADR 0090 D1/D2).
//!
//! Every timestamp the generator takes — the send time stamped into a payload, the
//! moment a PUBACK is read, the moment a delivery is read, a pacing deadline — is
//! nanoseconds of the monotonic clock since one process-wide epoch. Publishers and
//! subscribers live in this process, so ack latency and end-to-end latency are both
//! differences of readings of the same clock: no clock synchronisation, no error
//! bound.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

static EPOCH: OnceLock<Instant> = OnceLock::new();

fn epoch() -> Instant {
    *EPOCH.get_or_init(Instant::now)
}

/// Fix the epoch now (idempotent). Called once at start so the first reading is
/// not also the moment the epoch is taken.
pub fn init() {
    let _ = epoch();
}

/// Nanoseconds since the process epoch.
#[must_use]
pub fn now_ns() -> u64 {
    u64::try_from(epoch().elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// The [`Instant`] at `ns` nanoseconds after the epoch, for `sleep_until`.
#[must_use]
pub fn instant_at(ns: u64) -> Instant {
    epoch() + Duration::from_nanos(ns)
}

/// Sleep until `ns` on the process clock.
pub async fn sleep_until_ns(ns: u64) {
    tokio::time::sleep_until(tokio::time::Instant::from_std(instant_at(ns))).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_is_monotonic_and_maps_back_to_instants() {
        init();
        let a = now_ns();
        let b = now_ns();
        assert!(b >= a);
        let at = instant_at(b);
        assert!(at >= instant_at(a));
        assert!(Instant::now() >= at);
    }
}
