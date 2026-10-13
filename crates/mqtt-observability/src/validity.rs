//! A validity window and the two numbers read off it
//! ([ADR 0088](../../../docs/adr/0088-credential-lifetimes-and-rotation.md) D2).
//!
//! A certificate is valid from `notBefore` to `notAfter`; a revocation list is current
//! from `thisUpdate` to `nextUpdate`. Either way the operator needs two numbers: how long
//! is left, and how much of the window is spent. The second is what lets one alert rule
//! serve a certificate that lives for a day and a CA that lives for ten years.
//!
//! The arithmetic lives here, once, so the metric, the log line and the offline checks
//! cannot disagree about it.

/// A validity window in seconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidityWindow {
    /// `notBefore` (a certificate) or `thisUpdate` (a revocation list).
    pub not_before: i64,
    /// `notAfter` (a certificate) or `nextUpdate` (a revocation list).
    pub not_after: i64,
}

impl ValidityWindow {
    /// Seconds until the window ends: positive while it runs, `0` in its last second,
    /// negative once it is over (by how long).
    #[must_use]
    pub fn seconds_left(&self, now: i64) -> i64 {
        self.not_after.saturating_sub(now)
    }

    /// Whether the window is over. Its last second, `notAfter` itself, still counts as
    /// inside, as it does for the TLS handshake.
    #[must_use]
    pub fn ended(&self, now: i64) -> bool {
        now > self.not_after
    }

    /// Whether the window has not begun.
    #[must_use]
    pub fn not_yet_begun(&self, now: i64) -> bool {
        now < self.not_before
    }

    /// The fraction of the window that is spent: `0.0` at or before its start, `1.0` at
    /// its end, and above `1.0` after it (a window twice as old as it was long reads
    /// `2.0`). A window with no length (`notAfter` at or before `notBefore`) reads `0.0`
    /// up to its start and `1.0` after it: it is never usable, so it is never fresh.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // seconds of a validity window, far below 2^52
    pub fn spent(&self, now: i64) -> f64 {
        if now <= self.not_before {
            return 0.0;
        }
        let length = self.not_after.saturating_sub(self.not_before);
        if length <= 0 {
            return 1.0;
        }
        now.saturating_sub(self.not_before) as f64 / length as f64
    }

    /// Whether at least `permille` thousandths of the window are spent. Decided in whole
    /// seconds, so the answer at the boundary does not depend on rounding.
    #[must_use]
    pub fn past(&self, permille: u16, now: i64) -> bool {
        if now <= self.not_before {
            return false;
        }
        let length = i128::from(self.not_after) - i128::from(self.not_before);
        if length <= 0 {
            return true;
        }
        let spent = i128::from(now) - i128::from(self.not_before);
        spent * 1000 >= i128::from(permille) * length
    }
}

/// Seconds since the Unix epoch at `t` (`0` for a clock before it).
#[must_use]
pub fn unix_secs(t: std::time::SystemTime) -> i64 {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::ValidityWindow;

    const DAY: i64 = 86_400;

    #[test]
    fn the_fraction_spent_runs_from_zero_through_one_and_past_it() {
        let w = ValidityWindow {
            not_before: 1_000 * DAY,
            not_after: 1_090 * DAY,
        };
        assert!(w.spent(999 * DAY).abs() < f64::EPSILON, "not yet valid");
        assert!(w.spent(1_000 * DAY).abs() < f64::EPSILON);
        assert!((w.spent(1_030 * DAY) - 1.0 / 3.0).abs() < 1e-12);
        assert!((w.spent(1_090 * DAY) - 1.0).abs() < 1e-12);
        assert!((w.spent(1_180 * DAY) - 2.0).abs() < 1e-12, "expired");
        // The same fraction of a lifetime of one hour.
        let short = ValidityWindow {
            not_before: 0,
            not_after: 3_600,
        };
        assert!((short.spent(2_400) - 2.0 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn seconds_left_go_negative_after_the_end_and_the_last_second_is_inside() {
        let w = ValidityWindow {
            not_before: 100,
            not_after: 200,
        };
        assert_eq!(w.seconds_left(150), 50);
        assert_eq!(w.seconds_left(200), 0);
        assert!(!w.ended(200), "notAfter itself is still valid");
        assert_eq!(w.seconds_left(260), -60);
        assert!(w.ended(201));
        assert!(w.not_yet_begun(99));
        assert!(!w.not_yet_begun(100));
    }

    #[test]
    fn the_threshold_is_met_at_exactly_its_fraction() {
        let w = ValidityWindow {
            not_before: 0,
            not_after: 3_000,
        };
        assert!(!w.past(667, 2_000));
        assert!(w.past(667, 2_001));
        assert!(w.past(1000, 3_000));
        assert!(!w.past(1000, 2_999));
    }

    #[test]
    fn a_window_with_no_length_is_spent_from_its_start() {
        let w = ValidityWindow {
            not_before: 500,
            not_after: 500,
        };
        assert!(w.spent(499).abs() < f64::EPSILON);
        assert!(w.spent(500).abs() < f64::EPSILON);
        assert!((w.spent(501) - 1.0).abs() < f64::EPSILON);
        let backwards = ValidityWindow {
            not_before: 500,
            not_after: 400,
        };
        assert!((backwards.spent(600) - 1.0).abs() < f64::EPSILON);
    }
}
