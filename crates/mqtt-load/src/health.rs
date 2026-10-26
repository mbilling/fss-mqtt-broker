//! The generator's own CPU (ADR 0090 D2: generator health is its own check).
//!
//! Linux reads `/proc/self/stat` for the process and `/proc/self/task/*/` for each
//! thread, so the busiest runtime worker can be reported beside the process total: one
//! hot thread saturates a generator long before the process total does. Elsewhere (the
//! macOS developer tier) the process total comes from `ps`, and per-thread figures are
//! absent. No `unsafe` and no new dependency for either.

use std::process::Command;

/// The runtime's worker threads are named this, so their CPU can be told apart from
/// the collector's and the blocking pool's.
pub const WORKER_THREAD_NAME: &str = "mqload-worker";

/// One reading.
#[derive(Debug, Clone, Default)]
pub struct CpuSample {
    /// Process CPU (user + system), ns.
    pub process_ns: u64,
    /// Per runtime worker thread: (thread id, CPU ns). Empty where not available.
    pub workers: Vec<(u64, u64)>,
}

/// Utilisation between two readings.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CpuUse {
    /// Process CPU, in cores.
    pub cores: f64,
    /// The busiest worker thread, as a fraction of one core; `None` where per-thread
    /// CPU is not available.
    pub busiest_worker: Option<f64>,
}

/// Reads the process's CPU.
#[derive(Debug)]
pub struct CpuSampler {
    clk_tck: u64,
    linux: bool,
}

impl Default for CpuSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl CpuSampler {
    /// A sampler for this platform.
    #[must_use]
    pub fn new() -> Self {
        let linux = std::path::Path::new("/proc/self/stat").exists();
        let clk_tck = if linux {
            Command::new("getconf")
                .arg("CLK_TCK")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .and_then(|s| s.trim().parse().ok())
                .filter(|&t: &u64| t > 0)
                .unwrap_or(100)
        } else {
            100
        };
        Self { clk_tck, linux }
    }

    /// Whether per-thread CPU is read on this platform.
    #[must_use]
    pub fn per_thread(&self) -> bool {
        self.linux
    }

    /// Take a reading; `None` if the platform's source could not be read.
    #[must_use]
    pub fn sample(&self) -> Option<CpuSample> {
        if self.linux {
            self.sample_linux()
        } else {
            sample_ps()
        }
    }

    fn ticks_to_ns(&self, ticks: u64) -> u64 {
        ticks.saturating_mul(1_000_000_000) / self.clk_tck
    }

    fn sample_linux(&self) -> Option<CpuSample> {
        let process_ns = self.ticks_to_ns(stat_ticks(
            &std::fs::read_to_string("/proc/self/stat").ok()?,
        )?);
        let mut workers = Vec::new();
        if let Ok(dir) = std::fs::read_dir("/proc/self/task") {
            for e in dir.flatten() {
                let path = e.path();
                let Some(tid) = e.file_name().to_str().and_then(|s| s.parse().ok()) else {
                    continue;
                };
                let comm = std::fs::read_to_string(path.join("comm")).unwrap_or_default();
                // The name fits the kernel's 15-byte limit, so it is compared whole.
                if comm.trim() != WORKER_THREAD_NAME {
                    continue;
                }
                if let Some(t) = std::fs::read_to_string(path.join("stat"))
                    .ok()
                    .and_then(|s| stat_ticks(&s))
                {
                    workers.push((tid, self.ticks_to_ns(t)));
                }
            }
        }
        Some(CpuSample {
            process_ns,
            workers,
        })
    }
}

/// utime + stime, in clock ticks, from a `/proc/<pid>/stat` line.
fn stat_ticks(stat: &str) -> Option<u64> {
    // The command name is parenthesised and may contain spaces: split after the last ')'.
    let rest = stat.rsplit_once(')')?.1;
    let f: Vec<&str> = rest.split_whitespace().collect();
    // After the name: state(0) ppid(1) ... utime(11) stime(12).
    Some(f.get(11)?.parse::<u64>().ok()? + f.get(12)?.parse::<u64>().ok()?)
}

fn sample_ps() -> Option<CpuSample> {
    let out = Command::new("ps")
        .args(["-o", "time=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    let process_ns = parse_ps_time(String::from_utf8(out.stdout).ok()?.trim())?;
    Some(CpuSample {
        process_ns,
        workers: Vec::new(),
    })
}

/// `ps -o time=`: `[[dd-]hh:]mm:ss[.ss]`.
fn parse_ps_time(s: &str) -> Option<u64> {
    let (days, rest) = match s.split_once('-') {
        Some((d, r)) => (d.parse::<u64>().ok()?, r),
        None => (0, s),
    };
    let parts: Vec<&str> = rest.split(':').collect();
    let (h, m, sec) = match parts.as_slice() {
        [m, s] => (0, m.parse::<u64>().ok()?, *s),
        [h, m, s] => (h.parse::<u64>().ok()?, m.parse::<u64>().ok()?, *s),
        _ => return None,
    };
    let (whole, frac) = sec.split_once('.').unwrap_or((sec, "0"));
    let whole: u64 = whole.parse().ok()?;
    let mut frac_ns: u64 = 0;
    for (i, c) in frac.chars().take(9).enumerate() {
        frac_ns += u64::from(c.to_digit(10)?) * 10u64.pow(8 - u32::try_from(i).ok()?);
    }
    Some(((days * 24 + h) * 3600 + m * 60 + whole) * 1_000_000_000 + frac_ns)
}

/// Utilisation between `a` and `b`, taken `wall_ns` apart.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn utilisation(a: &CpuSample, b: &CpuSample, wall_ns: u64, per_thread: bool) -> CpuUse {
    if wall_ns == 0 {
        return CpuUse::default();
    }
    let wall = wall_ns as f64;
    let cores = b.process_ns.saturating_sub(a.process_ns) as f64 / wall;
    let busiest_worker = per_thread.then(|| {
        b.workers
            .iter()
            .map(|&(tid, ns)| {
                let before = a
                    .workers
                    .iter()
                    .find(|&&(t, _)| t == tid)
                    .map_or(0, |&(_, n)| n);
                ns.saturating_sub(before) as f64 / wall
            })
            .fold(0.0, f64::max)
    });
    CpuUse {
        cores,
        busiest_worker,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proc_stat_fields_survive_odd_command_names() {
        let line = "1234 (my (odd) prog) S 1 1234 1234 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 9 0";
        assert_eq!(stat_ticks(line), Some(300));
    }

    #[test]
    fn ps_time_formats() {
        assert_eq!(parse_ps_time("0:01.50"), Some(1_500_000_000));
        assert_eq!(parse_ps_time("12:03"), Some((12 * 60 + 3) * 1_000_000_000));
        assert_eq!(parse_ps_time("01:00:00"), Some(3_600_000_000_000));
        assert_eq!(parse_ps_time("1-00:00:01"), Some(86_401_000_000_000));
        assert_eq!(parse_ps_time("garbage"), None);
    }

    #[test]
    fn utilisation_is_cpu_over_wall() {
        let a = CpuSample {
            process_ns: 1_000,
            workers: vec![(1, 0), (2, 100)],
        };
        let b = CpuSample {
            process_ns: 3_001_000,
            workers: vec![(1, 900_000), (2, 200), (3, 50)],
        };
        let u = utilisation(&a, &b, 1_000_000, true);
        assert!((u.cores - 3.0).abs() < 1e-9);
        assert!((u.busiest_worker.unwrap() - 0.9).abs() < 1e-9);
        assert_eq!(utilisation(&a, &b, 1_000_000, false).busiest_worker, None);
    }

    #[test]
    fn this_platform_can_be_sampled() {
        let s = CpuSampler::new();
        let a = s.sample().expect("a CPU reading on this platform");
        // Burn a little CPU so the second reading can only be larger or equal.
        let mut x = 0u64;
        for i in 0..2_000_000u64 {
            x = x.wrapping_mul(31).wrapping_add(i);
        }
        std::hint::black_box(x);
        let b = s.sample().unwrap();
        assert!(b.process_ns >= a.process_ns);
    }
}
