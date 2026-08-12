//! Capped exponential backoff with jitter — the one retry-pacing policy for
//! every reconnect/recover loop in the workspace (bridge reconnects, OIDC
//! discovery, durable-session recovery, …).
//!
//! Before this type each loop hand-rolled `backoff = (backoff * 2).min(MAX)`
//! with no jitter, so replicas that failed together retried in lockstep —
//! exactly the thundering herd a backoff exists to avoid. [`Backoff`] applies
//! **equal jitter**: each delay is drawn uniformly from `[d/2, d]` where `d`
//! is the current exponential step, so retries stay bounded (never sooner than
//! half the nominal step, never later than the cap) but desynchronize.
//!
//! Jitter entropy comes from [`std::hash::RandomState`] — the same randomly
//! seeded hasher `HashMap` uses — so this crate takes no `rand` dependency.

use std::time::Duration;

/// Capped exponential backoff: `base, base*2, base*4, … max`, each step
/// jittered into `[step/2, step]`. `Iterator`-free by design — callers own the
/// loop (they interleave selects, deadlines, and shutdown checks).
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    /// A policy starting at `base` and doubling up to `max` (inclusive cap).
    #[must_use]
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            current: base,
        }
    }

    /// Back to the first step — call after a success so the next failure
    /// starts the ramp from `base` again.
    pub fn reset(&mut self) {
        self.current = self.base;
    }

    /// The delay to sleep before the next attempt (jittered into
    /// `[step/2, step]`), advancing the nominal step for the one after.
    pub fn next_delay(&mut self) -> Duration {
        let step = self.current;
        self.current = (self.current * 2).min(self.max);
        let half = step / 2;
        half + step.saturating_sub(half).mul_f64(random_fraction())
    }
}

/// A uniform-ish fraction in `[0, 1]` from `RandomState`'s per-instance seed.
/// Statistical quality far beyond "spread retries out" is not needed here.
// After `>> 11` both operands fit in f64's 53-bit mantissa: the casts are exact.
#[allow(clippy::cast_precision_loss)]
fn random_fraction() -> f64 {
    use std::hash::{BuildHasher, Hasher, RandomState};
    let bits = RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::Backoff;
    use std::time::Duration;

    #[test]
    fn delays_ramp_within_jitter_bounds_and_cap() {
        let base = Duration::from_millis(100);
        let max = Duration::from_millis(400);
        let mut b = Backoff::new(base, max);
        for expected_step in [100u64, 200, 400, 400, 400] {
            let d = b.next_delay();
            let step = Duration::from_millis(expected_step);
            assert!(d >= step / 2, "delay {d:?} below half of step {step:?}");
            assert!(d <= step, "delay {d:?} above step {step:?}");
        }
    }

    #[test]
    fn reset_returns_to_base() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_secs(10));
        for _ in 0..5 {
            let _ = b.next_delay();
        }
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(100));
    }
}
