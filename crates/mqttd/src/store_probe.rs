//! Boot-time volume self-measurement (ADR 0076 T1).
//!
//! The durable path's ceiling is the data-dir volume's **barrier rate** — how
//! many fsync round trips it serves per second — times the group-commit batch
//! depth. Until now only the bench rig could measure it; this module makes the
//! broker measure its OWN volume once, shortly after start, and publish the
//! result (`/metrics` gauges + the `/statusz` `store` block), so an operator
//! learns what their disk actually does without a load test, and drift (a
//! noisy neighbor, a volume migration) is visible against the boot figure.
//!
//! The probe is deliberately tiny — tens of small write+fsync pairs, well
//! under a second even on barrier-expensive volumes — runs in the blocking
//! pool a couple of seconds after start (never contending with recovery
//! reads), and writes only under its own scratch directory, removed on
//! completion.

use std::io::Write as _;
use std::path::Path;
use std::sync::OnceLock;

/// The boot probe's result: single-writer barriers/s and the 4-stream
/// aggregate (separate files) — the pair that says both what ONE fsync stream
/// can do and whether the device serves parallel streams (the ADR 0076
/// sharding headroom signal).
#[derive(Debug, Clone, Copy)]
pub struct BarrierProbe {
    /// Single-writer barrier rate (write+fsync round trips per second).
    pub single_per_sec: u64,
    /// Aggregate barrier rate across 4 concurrent writers on separate files.
    pub four_stream_per_sec: u64,
    /// When the probe ran (Unix epoch seconds).
    pub probed_epoch_secs: u64,
}

/// Write-once slot the probe task fills and `/statusz` reads.
#[derive(Debug, Default)]
pub struct ProbeSlot(OnceLock<BarrierProbe>);

impl ProbeSlot {
    /// The probe result, once measured.
    pub fn get(&self) -> Option<BarrierProbe> {
        self.0.get().copied()
    }

    /// Record the result (first write wins; the probe runs once).
    pub fn set(&self, probe: BarrierProbe) {
        let _ = self.0.set(probe);
    }
}

/// One stream's barrier loop: `ops` little write+fsync round trips against
/// `path`, returning the elapsed time. The file is created fresh and left for
/// the caller's directory cleanup.
fn barrier_loop(path: &Path, ops: usize) -> std::io::Result<std::time::Duration> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let payload = [0u8; 64];
    let started = std::time::Instant::now();
    for _ in 0..ops {
        f.write_all(&payload)?;
        f.sync_data()?;
    }
    Ok(started.elapsed())
}

fn rate(ops: usize, elapsed: std::time::Duration) -> u64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0;
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    {
        (ops as f64 / secs).round() as u64
    }
}

/// Measure the volume under `data_dir`: `ops` barriers single-writer, then
/// `ops` split across 4 concurrent writers on separate files. Blocking — run
/// on the blocking pool. Scratch lives in `data_dir/.barrier-probe/` and is
/// removed before returning.
pub fn measure(data_dir: &Path, ops: usize) -> std::io::Result<BarrierProbe> {
    let scratch = data_dir.join(".barrier-probe");
    std::fs::create_dir_all(&scratch)?;
    let result = (|| -> std::io::Result<(u64, u64)> {
        let single = rate(ops, barrier_loop(&scratch.join("s"), ops)?);
        // Four concurrent streams, separate files: the device's parallel
        // barrier capacity, which one redb file can never reach (ADR 0076).
        let per_stream = ops.div_ceil(4).max(1);
        let started = std::time::Instant::now();
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let path = scratch.join(format!("c{i}"));
                std::thread::spawn(move || barrier_loop(&path, per_stream))
            })
            .collect();
        let mut ok = true;
        for h in handles {
            match h.join() {
                Ok(Ok(_)) => {}
                _ => ok = false,
            }
        }
        let four = if ok {
            rate(per_stream * 4, started.elapsed())
        } else {
            0
        };
        Ok((single, four))
    })();
    // Best-effort cleanup either way; the scratch is tiny and namespaced.
    let _ = std::fs::remove_dir_all(&scratch);
    let (single_per_sec, four_stream_per_sec) = result?;
    Ok(BarrierProbe {
        single_per_sec,
        four_stream_per_sec,
        probed_epoch_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
    })
}

/// The stream counts the first-boot calibration tries, in order. Powers of two
/// up to the design bound (`R_MAX_SHARDS`) — enough resolution to find a knee,
/// few enough to stay well under a second on a slow volume.
const CALIBRATION_STREAMS: [usize; 4] = [1, 2, 4, 8];

/// How close `P(K)` must come to `K` before sharding into K files could pay
/// (ADR 0076 T2, amended). Sharding divides the group-commit batch depth by K
/// and multiplies the barrier rate by P(K), so throughput scales by `P(K)/K`:
/// break-even is `P(K) == K`, and 0.9 is the margin at which it is worth an
/// operator's A/B rather than a certain loss.
const SHARDING_BREAK_EVEN: f64 = 0.9;

/// Measure this volume's parallel-barrier curve: `(streams, aggregate
/// barriers/s)` at 1, 2, 4 and 8 concurrent writers on separate files.
///
/// This is the input to the ADR 0076 T2 question — *would splitting the store
/// into K files help?* — and, since the measurement rejected sharding as a
/// default, its output is a REPORT rather than a decision. See
/// [`sharding_would_pay`] for the rule the numbers feed.
///
/// Blocking, a few hundred milliseconds.
///
/// # Errors
/// Propagates the probe's IO error.
pub fn parallel_barrier_curve(data_dir: &Path, ops: usize) -> std::io::Result<Vec<(usize, u64)>> {
    let scratch = data_dir.join(".shard-calibration");
    std::fs::create_dir_all(&scratch)?;
    let sweep = (|| -> std::io::Result<Vec<(usize, u64)>> {
        let mut out = Vec::with_capacity(CALIBRATION_STREAMS.len());
        for streams in CALIBRATION_STREAMS {
            let per_stream = ops.div_ceil(streams).max(1);
            let started = std::time::Instant::now();
            let handles: Vec<_> = (0..streams)
                .map(|i| {
                    let path = scratch.join(format!("s{streams}-{i}"));
                    std::thread::spawn(move || barrier_loop(&path, per_stream))
                })
                .collect();
            let mut ok = true;
            for h in handles {
                if !matches!(h.join(), Ok(Ok(_))) {
                    ok = false;
                }
            }
            if !ok {
                return Err(std::io::Error::other("a calibration stream failed"));
            }
            out.push((streams, rate(per_stream * streams, started.elapsed())));
        }
        Ok(out)
    })();
    let _ = std::fs::remove_dir_all(&scratch);
    sweep
}

/// The largest K on this volume's measured curve for which splitting the store
/// into K files could pay — `None` (the normal answer) when none can.
///
/// The rule is arithmetic, not a heuristic. A group-commit writer converts
/// in-flight work into batch depth `D`, and throughput is `D × barriers/s`.
/// Splitting into K files gives each shard depth `D/K` while the device serves
/// `P(K)` times the barrier rate, so:
///
/// ```text
///     sharded / single  =  P(K) / K
/// ```
///
/// Sharding therefore needs `P(K) ≈ K` — a device with K genuinely independent
/// queues. Real volumes measure far below that (`P(2)≈1.7`, `P(4)≈2.3` on the
/// campaign hosts and on developer laptops alike), which is why the store is
/// one file by default and this function almost always says `None`.
#[must_use]
pub fn sharding_would_pay(curve: &[(usize, u64)]) -> Option<usize> {
    let single = curve
        .iter()
        .find(|(streams, _)| *streams == 1)
        .map(|(_, rate)| *rate)
        .filter(|r| *r > 0)?;
    #[allow(clippy::cast_precision_loss)]
    curve
        .iter()
        .filter(|(streams, _)| *streams > 1)
        .filter(|(streams, rate)| {
            let parallel_gain = *rate as f64 / single as f64;
            parallel_gain / *streams as f64 >= SHARDING_BREAK_EVEN
        })
        .map(|(streams, _)| *streams)
        .max()
}

#[cfg(test)]
mod tests {
    use super::{measure, parallel_barrier_curve, sharding_would_pay};

    /// The probe measures a real rate on a real filesystem, cleans up its
    /// scratch, and the 4-stream aggregate is at least a meaningful fraction
    /// of the single rate (a device can serve parallel streams no worse than
    /// ~half of one stream even fully serialized at the platter).
    #[test]
    fn probes_a_volume_and_cleans_up() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let probe = measure(dir.path(), 16).expect("probe");
        assert!(probe.single_per_sec > 0, "a real volume has a barrier rate");
        assert!(
            probe.four_stream_per_sec > probe.single_per_sec / 4,
            "4 streams cannot aggregate to less than a quarter of one \
             (got {} vs single {})",
            probe.four_stream_per_sec,
            probe.single_per_sec
        );
        assert!(
            !dir.path().join(".barrier-probe").exists(),
            "the probe scratch must be removed"
        );
    }

    /// The curve measures every stream count in order, on a real filesystem,
    /// and leaves no scratch behind. The rates themselves are a property of the
    /// machine's disk, so the assertion is the SHAPE, not a number.
    #[test]
    fn the_parallel_barrier_curve_covers_every_stream_count() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let curve = parallel_barrier_curve(dir.path(), 16).expect("curve");
        assert_eq!(
            curve.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![1, 2, 4, 8],
            "the curve must cover the sweep, in order"
        );
        assert!(
            curve.iter().all(|(_, rate)| *rate > 0),
            "every stream count measures a real rate: {curve:?}"
        );
        assert!(
            !dir.path().join(".shard-calibration").exists(),
            "the curve's scratch must be removed"
        );
    }

    /// The sharding rule is `P(K)/K >= break-even`, and it is the reason the
    /// store is one file: a device would have to serve K nearly-independent
    /// queues before splitting the group-commit batch K ways could pay.
    #[test]
    fn sharding_pays_only_when_parallel_streams_are_nearly_independent() {
        // A real volume: 4 streams give 2.3x, not 4x. P(4)/4 = 0.58 — sharding
        // would LOSE, which is what the campaign measured end to end.
        let real = vec![(1, 2162), (2, 3600), (4, 4900), (8, 8041)];
        assert_eq!(
            sharding_would_pay(&real),
            None,
            "no stream count on a real volume reaches break-even: {real:?}"
        );
        // A hypothetical device with independent queues: P(K) tracks K.
        let independent = vec![(1, 2000), (2, 3960), (4, 7900), (8, 15800)];
        assert_eq!(
            sharding_would_pay(&independent),
            Some(8),
            "with truly independent queues the largest paying K is advised"
        );
        // A device that measures nothing advises nothing.
        assert_eq!(sharding_would_pay(&[]), None);
        assert_eq!(sharding_would_pay(&[(1, 0), (4, 9000)]), None);
    }
}
