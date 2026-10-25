//! Latency histograms: HDR, microsecond resolution, three significant figures.

use hdrhistogram::Histogram;
use serde::Serialize;

/// The largest recordable latency, µs (ten minutes). Larger values saturate here.
pub const MAX_US: u64 = 600_000_000;

/// Significant figures every [`Latency`] keeps.
pub const SIGFIG: u8 = 3;

/// A latency histogram in microseconds.
#[derive(Debug, Clone)]
pub struct Latency(Histogram<u64>);

/// A histogram's summary, microseconds.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct LatencySummary {
    /// Samples.
    pub count: u64,
    /// Mean, µs.
    pub mean_us: f64,
    /// Median, µs.
    pub p50_us: u64,
    /// 90th percentile, µs.
    pub p90_us: u64,
    /// 99th percentile, µs.
    pub p99_us: u64,
    /// 99.9th percentile, µs.
    pub p999_us: u64,
    /// Largest sample, µs.
    pub max_us: u64,
}

/// A histogram's recorded buckets, so that histograms from several generator processes
/// can be merged exactly outside the process (schema `mqtt-load.summary.v2`).
///
/// Every [`Latency`] has the same bounds and precision, so two exports merge by adding
/// the counts of equal `value_us`. A quantile of the merge is the first bucket whose
/// cumulative count reaches `ceil(q * total)` — what `hdrhistogram` itself reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistogramExport {
    /// Always `us`.
    pub unit: &'static str,
    /// [`SIGFIG`].
    pub significant_figures: u8,
    /// [`MAX_US`]; larger samples were recorded here.
    pub highest_trackable_us: u64,
    /// `[value_us, count]` for every bucket with samples, ascending. `value_us` is the
    /// highest value equivalent to the bucket, the figure a percentile reports.
    pub buckets: Vec<[u64; 2]>,
}

impl Default for Latency {
    fn default() -> Self {
        Self::new()
    }
}

impl Latency {
    /// An empty histogram.
    ///
    /// # Panics
    /// Never: the bounds are constants hdrhistogram accepts.
    #[must_use]
    pub fn new() -> Self {
        Self(Histogram::new_with_bounds(1, MAX_US, SIGFIG).expect("valid histogram bounds"))
    }

    /// Record a latency given in nanoseconds (rounded to the nearest µs).
    pub fn record_ns(&mut self, ns: u64) {
        self.0.saturating_record(ns.saturating_add(500) / 1000);
    }

    /// Add every sample of `other`.
    ///
    /// # Panics
    /// Never: every [`Latency`] has the same bounds.
    pub fn add(&mut self, other: &Latency) {
        // Both have the same bounds, so this cannot fail.
        self.0.add(&other.0).expect("histograms share bounds");
    }

    /// Drop every sample.
    pub fn reset(&mut self) {
        self.0.reset();
    }

    /// Samples recorded.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.0.len()
    }

    /// Samples at or below `us` microseconds.
    #[must_use]
    pub fn count_at_or_below(&self, us: u64) -> u64 {
        self.0.count_between(0, us)
    }

    /// The `q` quantile, µs.
    #[must_use]
    pub fn quantile_us(&self, q: f64) -> u64 {
        if self.0.is_empty() {
            0
        } else {
            self.0.value_at_quantile(q)
        }
    }

    /// Sum of all samples, µs (approximate to the bucket resolution).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sum_us(&self) -> f64 {
        self.0.mean() * self.0.len() as f64
    }

    /// Every recorded bucket, for merging outside the process.
    #[must_use]
    pub fn export(&self) -> HistogramExport {
        HistogramExport {
            unit: "us",
            significant_figures: SIGFIG,
            highest_trackable_us: MAX_US,
            buckets: self
                .0
                .iter_recorded()
                .map(|v| [v.value_iterated_to(), v.count_at_value()])
                .collect(),
        }
    }

    /// Rebuild a histogram from an export (the inverse of [`Latency::export`]).
    #[must_use]
    pub fn from_export(e: &HistogramExport) -> Self {
        let mut h = Self::new();
        for [v, c] in &e.buckets {
            h.0.saturating_record_n(*v, *c);
        }
        h
    }

    /// Summary statistics.
    #[must_use]
    pub fn summary(&self) -> LatencySummary {
        if self.0.is_empty() {
            return LatencySummary::default();
        }
        LatencySummary {
            count: self.0.len(),
            mean_us: (self.0.mean() * 10.0).round() / 10.0,
            p50_us: self.0.value_at_quantile(0.5),
            p90_us: self.0.value_at_quantile(0.9),
            p99_us: self.0.value_at_quantile(0.99),
            p999_us: self.0.value_at_quantile(0.999),
            max_us: self.0.max(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_within_a_tenth_of_a_percent() {
        let mut h = Latency::new();
        for ms in 1..=1000u64 {
            h.record_ns(ms * 1_000_000);
        }
        let s = h.summary();
        assert_eq!(s.count, 1000);
        let near = |got: u64, want: u64| got.abs_diff(want) * 1000 <= want;
        assert!(near(s.p50_us, 500_000), "{s:?}");
        assert!(near(s.p90_us, 900_000), "{s:?}");
        assert!(near(s.p99_us, 990_000), "{s:?}");
        assert!(near(s.p999_us, 999_000), "{s:?}");
        assert!(near(s.max_us, 1_000_000), "{s:?}");
        assert!((s.mean_us - 500_500.0).abs() < 600.0, "{s:?}");
    }

    #[test]
    fn export_round_trips_and_merges_by_bucket() {
        let mut a = Latency::new();
        let mut b = Latency::new();
        for i in 1..=5000u64 {
            a.record_ns(i * 7_919_000 % 3_000_000_000);
            b.record_ns(i * 104_729 % 40_000_000);
        }
        a.record_ns(u64::MAX);
        let ea = a.export();
        assert_eq!(ea.unit, "us");
        assert_eq!(ea.buckets.iter().map(|[_, c]| c).sum::<u64>(), a.count());
        assert!(ea.buckets.windows(2).all(|w| w[0][0] < w[1][0]));
        // Lossless: the rebuilt histogram reports exactly the same figures.
        assert_eq!(Latency::from_export(&ea).summary(), a.summary());
        // Merging exports bucket-wise is merging the histograms.
        let mut merged = std::collections::BTreeMap::<u64, u64>::new();
        for [v, c] in ea.buckets.iter().chain(b.export().buckets.iter()) {
            *merged.entry(*v).or_default() += c;
        }
        let me = HistogramExport {
            buckets: merged.into_iter().map(|(v, c)| [v, c]).collect(),
            ..ea.clone()
        };
        let mut ab = a.clone();
        ab.add(&b);
        assert_eq!(Latency::from_export(&me).summary(), ab.summary());
        assert_eq!(me, ab.export());
    }

    #[test]
    fn rounding_saturation_merge_and_reset() {
        let mut a = Latency::new();
        a.record_ns(1_499); // 1 µs
        a.record_ns(1_500); // 2 µs
        a.record_ns(u64::MAX); // saturates at ten minutes
        assert_eq!(a.count(), 3);
        assert_eq!(a.quantile_us(0.0), 1);
        assert!(a.summary().max_us >= MAX_US * 999 / 1000);
        let mut b = Latency::new();
        b.record_ns(10_000);
        b.add(&a);
        assert_eq!(b.count(), 4);
        assert_eq!(b.count_at_or_below(10), 3);
        b.reset();
        assert_eq!(b.count(), 0);
        assert_eq!(b.summary(), LatencySummary::default());
    }
}
