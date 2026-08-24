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

#[cfg(test)]
mod tests {
    use super::measure;

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
}
