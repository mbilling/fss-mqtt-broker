//! Open-loop pacing (ADR 0090 D2, D4).
//!
//! A run is a sequence of [`Phase`]s, each a constant total offered rate held for a
//! warm-up and a measured span. A phase's rate is split evenly over the run's
//! publishers: publisher `i` of `n` at total rate `R` sends every `n / R` seconds,
//! offset by `i / R` so the publishers' sends interleave evenly instead of arriving
//! in bursts of `n`.
//!
//! The schedule is a pure function of time. Message `m` of publisher `i` in a phase
//! is due at `phase_start + i/R + m·n/R` whatever happened to the messages before it,
//! so a slow broker can delay *sends* (they queue in the generator and the backlog is
//! reported) but never moves the *offer*. "Offered" in every report is computed from
//! this schedule, not counted by the publishers.

/// One step of the schedule: a total offered rate held for `warmup_ns + measure_ns`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Phase {
    /// Total offered rate over all publishers, messages per second. Zero is a pause.
    pub rate: f64,
    /// Warm-up span: offered and sent like the rest, excluded from measured windows.
    pub warmup_ns: u64,
    /// Measured span.
    pub measure_ns: u64,
}

impl Phase {
    /// The phase's whole length.
    #[must_use]
    pub fn len_ns(&self) -> u64 {
        self.warmup_ns + self.measure_ns
    }
}

#[derive(Debug, Clone)]
struct Span {
    start_ns: u64,
    len_ns: u64,
    rate: f64,
}

/// The run's schedule, anchored at `start_ns` on the process clock.
#[derive(Debug, Clone)]
pub struct Schedule {
    spans: Vec<Span>,
    publishers: u32,
    start_ns: u64,
}

impl Schedule {
    /// A schedule of `phases` back to back from `start_ns`, over `publishers` publishers.
    ///
    /// # Panics
    /// If `publishers` is zero or a rate is negative or not finite.
    #[must_use]
    pub fn new(phases: &[Phase], publishers: u32, start_ns: u64) -> Self {
        assert!(publishers > 0, "a schedule needs at least one publisher");
        let mut at = start_ns;
        let spans = phases
            .iter()
            .map(|p| {
                assert!(p.rate.is_finite() && p.rate >= 0.0, "rate must be >= 0");
                let s = Span {
                    start_ns: at,
                    len_ns: p.len_ns(),
                    rate: p.rate,
                };
                at += p.len_ns();
                s
            })
            .collect();
        Self {
            spans,
            publishers,
            start_ns,
        }
    }

    /// When the first phase starts.
    #[must_use]
    pub fn start_ns(&self) -> u64 {
        self.start_ns
    }

    /// When the last phase ends: no message is due at or after this.
    #[must_use]
    pub fn end_ns(&self) -> u64 {
        self.spans
            .last()
            .map_or(self.start_ns, |s| s.start_ns + s.len_ns)
    }

    /// Start of phase `phase`.
    #[must_use]
    pub fn phase_start_ns(&self, phase: usize) -> u64 {
        self.spans[phase].start_ns
    }

    /// The publisher's send interval and its offset within the phase, in ns.
    fn interval_offset(&self, span: &Span, publisher: u32) -> Option<(f64, f64)> {
        if span.rate <= 0.0 {
            return None;
        }
        let interval = f64::from(self.publishers) * 1e9 / span.rate;
        let offset = f64::from(publisher) * 1e9 / span.rate;
        Some((interval, offset))
    }

    /// How many messages publisher `publisher` is due to send in `span`.
    fn count_in(&self, span: &Span, publisher: u32) -> u64 {
        let Some((interval, offset)) = self.interval_offset(span, publisher) else {
            return 0;
        };
        #[allow(clippy::cast_precision_loss)]
        let len = span.len_ns as f64;
        if len <= offset {
            return 0;
        }
        // m is due iff offset + m·interval < len.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let n = ((len - offset) / interval).ceil() as u64;
        n
    }

    /// Every message publisher `publisher` is scheduled to send over the whole run:
    /// the exact upper bound on its sequence numbers, so the ledger can be sized once.
    #[must_use]
    pub fn total_for(&self, publisher: u32) -> u64 {
        self.spans.iter().map(|s| self.count_in(s, publisher)).sum()
    }

    /// Messages publisher `publisher` was due to have sent by `t_ns` (inclusive).
    #[must_use]
    pub fn offered_by(&self, publisher: u32, t_ns: u64) -> u64 {
        let mut total = 0;
        for s in &self.spans {
            if t_ns >= s.start_ns + s.len_ns {
                total += self.count_in(s, publisher);
                continue;
            }
            if t_ns < s.start_ns {
                break;
            }
            let Some((interval, offset)) = self.interval_offset(s, publisher) else {
                break;
            };
            let count = self.count_in(s, publisher);
            let rel = t_ns - s.start_ns;
            let due = |m: u64| due_rel(interval, offset, m);
            // Estimate, then settle against the cursor's own due times, so offered and
            // the cursor agree exactly whatever the floating-point rounding.
            #[allow(clippy::cast_precision_loss)]
            let est = ((rel as f64 - offset) / interval).floor() + 1.0;
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let mut n = if est > 0.0 {
                (est as u64).min(count)
            } else {
                0
            };
            while n < count && due(n) <= rel {
                n += 1;
            }
            while n > 0 && due(n - 1) > rel {
                n -= 1;
            }
            total += n;
            break;
        }
        total
    }

    /// Messages all publishers were due to have sent by `t_ns`.
    #[must_use]
    pub fn offered_total_by(&self, t_ns: u64) -> u64 {
        (0..self.publishers).map(|p| self.offered_by(p, t_ns)).sum()
    }

    /// A cursor over publisher `publisher`'s due times, in order.
    #[must_use]
    pub fn cursor(&self, publisher: u32) -> Cursor {
        Cursor {
            schedule: self.clone(),
            publisher,
            span: 0,
            m: 0,
        }
    }
}

/// Message `m`'s due time relative to its span's start: the one definition the cursor
/// and [`Schedule::offered_by`] share.
fn due_rel(interval: f64, offset: f64, m: u64) -> u64 {
    #[allow(clippy::cast_precision_loss)]
    let x = offset + m as f64 * interval;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let ns = x as u64;
    ns
}

/// Iterates one publisher's due times (ns on the process clock).
#[derive(Debug, Clone)]
pub struct Cursor {
    schedule: Schedule,
    publisher: u32,
    span: usize,
    m: u64,
}

impl Iterator for Cursor {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        loop {
            let s = self.schedule.spans.get(self.span)?;
            // Validity by count (not by comparing the due time with the span end), so
            // the cursor yields exactly `count_in` messages per span, by construction.
            if self.m < self.schedule.count_in(s, self.publisher) {
                let (interval, offset) = self
                    .schedule
                    .interval_offset(s, self.publisher)
                    .expect("a span with messages has a rate");
                let rel = due_rel(interval, offset, self.m);
                self.m += 1;
                return Some(s.start_ns + rel);
            }
            self.span += 1;
            self.m = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_000_000_000;

    fn phase(rate: f64, secs: u64) -> Phase {
        Phase {
            rate,
            warmup_ns: 0,
            measure_ns: secs * S,
        }
    }

    #[test]
    fn rate_is_split_evenly_and_interleaved() {
        // 1000 msg/s over 10 publishers: each every 10 ms, offset by 1 ms apiece.
        let s = Schedule::new(&[phase(1000.0, 1)], 10, 0);
        for p in 0..10 {
            assert_eq!(s.total_for(p), 100, "publisher {p}");
        }
        let firsts: Vec<u64> = (0..10).map(|p| s.cursor(p).next().unwrap()).collect();
        assert_eq!(firsts, (0..10).map(|p| p * 1_000_000).collect::<Vec<_>>());
        let p3: Vec<u64> = s.cursor(3).take(3).collect();
        assert_eq!(p3, vec![3_000_000, 13_000_000, 23_000_000]);
        assert_eq!(s.offered_total_by(s.end_ns()), 1000);
    }

    #[test]
    fn cursor_yields_exactly_the_offered_count() {
        let phases = [phase(333.0, 2), phase(0.0, 1), phase(1777.7, 3)];
        let s = Schedule::new(&phases, 7, 5 * S);
        for p in 0..7 {
            let dues: Vec<u64> = s.cursor(p).collect();
            assert_eq!(dues.len() as u64, s.total_for(p));
            assert!(dues.windows(2).all(|w| w[0] < w[1]), "strictly increasing");
            assert!(dues.iter().all(|&d| d >= s.start_ns() && d < s.end_ns()));
            // offered_by agrees with the cursor at every due time and between them.
            for (k, &d) in dues.iter().enumerate() {
                assert_eq!(s.offered_by(p, d), k as u64 + 1, "at due {k}");
                assert_eq!(s.offered_by(p, d - 1), k as u64, "just before due {k}");
            }
        }
        // Nothing is offered in the pause.
        let pause_start = s.phase_start_ns(1);
        let pause_end = s.phase_start_ns(2);
        assert_eq!(
            s.offered_total_by(pause_start - 1),
            s.offered_total_by(pause_end - 1)
        );
    }

    #[test]
    fn offered_total_matches_rate_times_time() {
        let s = Schedule::new(&[phase(10_000.0, 3), phase(25_000.0, 2)], 400, 0);
        assert_eq!(s.offered_total_by(s.end_ns()), 30_000 + 50_000);
        // Halfway through phase one: half its messages, within one per publisher.
        let half = s.offered_total_by(S * 3 / 2);
        assert!((14_600..=15_400).contains(&half), "{half}");
        assert_eq!(s.offered_total_by(3 * S - 1), 30_000);
    }

    #[test]
    fn step_changes_rate_per_publisher_without_changing_publishers() {
        let s = Schedule::new(&[phase(100.0, 1), phase(200.0, 1)], 10, 0);
        let dues: Vec<u64> = s.cursor(0).collect();
        assert_eq!(dues.len(), 30);
        // 100 ms apart in the first second, 50 ms apart in the second.
        assert_eq!(dues[1] - dues[0], 100_000_000);
        assert_eq!(dues[11] - dues[10], 50_000_000);
        assert_eq!(dues[10], S);
    }

    #[test]
    fn low_rates_with_many_publishers_send_nothing_rather_than_too_much() {
        // 5 msg/s over 10 publishers for 1 s: only the first five fire, once each.
        let s = Schedule::new(&[phase(5.0, 1)], 10, 0);
        let per: Vec<u64> = (0..10).map(|p| s.total_for(p)).collect();
        assert_eq!(per, vec![1, 1, 1, 1, 1, 0, 0, 0, 0, 0]);
        assert_eq!(s.offered_total_by(s.end_ns()), 5);
    }
}
