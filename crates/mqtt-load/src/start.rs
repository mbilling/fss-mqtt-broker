//! A start shared by several generator processes (ADR 0090 T5).
//!
//! A rig running one mqtt-load per host needs their measured windows to coincide; each
//! process starting its schedule as soon as its own clients are connected does not do
//! that when one host connects seconds later than the others (the 10-node run of
//! 2026-10-27: 13.8 s against 5.2 s, the windows overlapped 51.6 s of 60). The handshake:
//! with `--ready-file`, the process writes that file (Unix ms, atomically) once every
//! client is connected; with `--start-file`, it then waits for that file to name the
//! Unix ms at which the schedule starts, and starts it then. The rig waits for every
//! ready file and writes one start time a moment ahead into every start file. Host
//! clocks agree to NTP's milliseconds, against windows of seconds.
//!
//! The wait is bounded (`--start-wait`): a rig that never writes the start does not
//! hang the generator, which then starts on its own and says so in the summary
//! (`start.timed_out`), so the rig's overlap check can judge it.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::clock::now_ns;

/// How often the start file is looked for.
const POLL: Duration = Duration::from_millis(10);
/// The lead a start of its own gives the publishers' writers.
pub const OWN_LEAD_NS: u64 = 200_000_000;

/// Wall-clock now, Unix ms.
#[must_use]
pub fn unix_ms_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0)
}

/// Write `contents` to `path` atomically: a sibling temporary file, renamed into place,
/// so a reader never sees half of it.
///
/// # Errors
/// The write or the rename failed.
pub fn write_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    std::fs::write(&tmp, contents)?;
    std::fs::rename(&tmp, path)
}

/// The start time a start file names: one integer, Unix ms. `None` while the file is
/// absent or does not hold one (yet).
#[must_use]
pub fn read_start(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// The schedule's start on the process clock for a start at `requested_unix_ms`, read
/// when the wall clock said `now_unix_ms` and the process clock `now_ns`; and how many
/// ms late that is. A start already past begins `OWN_LEAD_NS` from now.
#[must_use]
pub fn start_ns_for(requested_unix_ms: u64, now_unix_ms: u64, now_ns: u64) -> (u64, u64) {
    if requested_unix_ms >= now_unix_ms {
        (now_ns + (requested_unix_ms - now_unix_ms) * 1_000_000, 0)
    } else {
        (now_ns + OWN_LEAD_NS, now_unix_ms - requested_unix_ms)
    }
}

/// How the schedule's start was set, for the summary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Waited {
    /// The start time the start file named, Unix ms (`None`: no start file, or it never
    /// came).
    pub requested_unix_ms: Option<u64>,
    /// Waited for the start file, ns.
    pub waited_ns: u64,
    /// The start file did not name a start within the wait.
    pub timed_out: bool,
}

/// Wait up to `wait_ns` for `path` to name a start; returns it with the process-clock
/// start and how late it is.
pub async fn await_start(path: &Path, wait_ns: u64) -> (u64, u64, Waited) {
    let t0 = now_ns();
    loop {
        if let Some(at) = read_start(path) {
            let now = now_ns();
            let (start, late_ms) = start_ns_for(at, unix_ms_now(), now);
            let waited = Waited {
                requested_unix_ms: Some(at),
                waited_ns: now - t0,
                timed_out: false,
            };
            return (start, late_ms, waited);
        }
        let now = now_ns();
        if now.saturating_sub(t0) >= wait_ns {
            let waited = Waited {
                requested_unix_ms: None,
                waited_ns: now - t0,
                timed_out: true,
            };
            return (now + OWN_LEAD_NS, 0, waited);
        }
        tokio::time::sleep(POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_future_start_maps_onto_the_process_clock() {
        assert_eq!(start_ns_for(10_500, 10_000, 7), (7 + 500_000_000, 0));
        assert_eq!(start_ns_for(10_000, 10_000, 7), (7, 0));
    }

    #[test]
    fn a_start_already_past_begins_now_and_says_how_late() {
        assert_eq!(start_ns_for(9_000, 10_000, 7), (7 + OWN_LEAD_NS, 1_000));
    }

    #[test]
    fn a_start_file_is_read_whole_or_not_at_all() {
        let dir = std::env::temp_dir().join(format!("mqtt-load-start-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("start");
        assert_eq!(read_start(&p), None);
        std::fs::write(&p, "17930").unwrap();
        assert_eq!(read_start(&p), Some(17930));
        std::fs::write(&p, "").unwrap();
        assert_eq!(read_start(&p), None);
        write_atomic(&p, "1793069435641\n").unwrap();
        assert_eq!(read_start(&p), Some(1_793_069_435_641));
        assert!(!dir.join("start.tmp").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn the_wait_ends_at_the_start_file_or_at_its_bound() {
        crate::clock::init();
        let dir = std::env::temp_dir().join(format!("mqtt-load-wait-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("start");
        let (_, late, w) = await_start(&p, 30_000_000).await;
        assert!(
            w.timed_out && w.requested_unix_ms.is_none() && late == 0,
            "{w:?}"
        );
        assert!(w.waited_ns >= 30_000_000, "{w:?}");
        let at = unix_ms_now() + 2_000;
        let before = now_ns();
        // The wait runs on its own task; the start file appears while it polls (or
        // before its first look: the result is the same).
        let waiter = {
            let p = p.clone();
            tokio::spawn(async move { await_start(&p, 5_000_000_000).await })
        };
        write_atomic(&p, &at.to_string()).unwrap();
        let (start, late, w) = waiter.await.unwrap();
        assert_eq!(w.requested_unix_ms, Some(at));
        assert!(!w.timed_out && late == 0, "{w:?}");
        // The start lies ahead of the moment the file was read, by at most the 2 s.
        assert!(
            start > before && start <= now_ns() + 2_000_000_000,
            "{start} {before}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
