//! Process-memory visibility + watermark brownout
//! ([ADR 0041](../../../docs/adr/0041-resource-governance.md) T8).
//!
//! The RSS analogue of the T5 disk watermark, and deliberately the same shape: a poller
//! samples this process's resident set, exports it as `process_resident_bytes`, and —
//! when `MQTTD_MEMORY_MAX_BYTES` is set — drives the hub's **brownout** flag on
//! watermark transitions. Above it, writes that *grow* state (new sessions, new retained
//! topics, offline enqueues) are refused with the quota behaviours, while acks, reads,
//! deletes, expiry and resumes continue.
//!
//! ## Why a watermark and not a heap cap
//!
//! Mosquitto's `memory_limit` denies allocations at a heap cap; EMQX's `force_shutdown`
//! kills the connection process over a per-connection bound. Both destroy standing state:
//! an allocation failure lands wherever it lands, and killing a session throws away
//! durable work already acknowledged. Brownout refuses *new growth* and keeps everything
//! already promised — which is this ADR's founding principle, and the reason the disk
//! axis is built the same way.
//!
//! It is a **watermark, not a ceiling.** Nothing here can stop memory rising: the broker
//! can still be OOM-killed if a single burst outruns the poll interval. The container
//! `MemoryMax` / cgroup limit remains the hard bound. What this buys is that the common
//! case — pressure building over minutes — degrades to read-mostly with a metric and a
//! log line, instead of arriving as an OOM kill with no warning.
//!
//! ## Where RSS comes from
//!
//! `/proc/self/status`'s `VmRSS` line, in kB. No new dependency for one number, and no
//! `libc` call. Released binaries are Linux-only (see the README's supported platforms),
//! so this is the platform that matters; elsewhere the sampler returns `None` and the
//! watcher **logs once and exits** rather than silently pretending to enforce a bound.

use crate::hub::{BrownoutAxis, HubCommand};
use mqtt_observability::metrics::Metrics;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// How often the watcher re-samples RSS. Matches the disk watcher's cadence: fast enough
/// that pressure building over minutes is caught, slow enough to cost nothing.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// This process's resident set size in bytes, or `None` where it cannot be read.
///
/// `None` is not zero. A caller must not treat "unknown" as "using no memory" — the
/// watcher stops instead, because a watermark that silently never fires is worse than
/// one that was never configured.
#[must_use]
pub fn resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_vm_rss(&status)
}

/// Pull `VmRSS` (kB) out of `/proc/self/status` and return it in bytes.
fn parse_vm_rss(status: &str) -> Option<u64> {
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kb.checked_mul(1024)
}

/// Run the memory watcher until the hub goes away: export RSS every poll and, with a
/// watermark configured, send [`HubCommand::SetBrownout`] on transitions (edge-triggered
/// — an unchanged state sends nothing).
///
/// `sample` is injectable so the transition logic can be tested without allocating
/// gigabytes; production passes [`resident_bytes`].
pub async fn watch(
    max_bytes: Option<u64>,
    hub: mpsc::UnboundedSender<HubCommand>,
    metrics: Option<Arc<Metrics>>,
    poll: Option<Duration>,
    sample: Option<Box<dyn Fn() -> Option<u64> + Send>>,
) {
    let poll = poll.unwrap_or(POLL_INTERVAL);
    let sample = sample.unwrap_or_else(|| Box::new(resident_bytes));
    let mut brownout = false;

    if let (Some(m), Some(max)) = (&metrics, max_bytes) {
        m.set_memory_max_bytes(max); // constant per process; exported once
    }

    loop {
        let Some(rss) = sample() else {
            // Unreadable RSS is a dead end, not a transient: /proc/self/status either
            // exists or it does not. Say so once, loudly if a watermark was configured
            // (the operator believes they have a bound and they do not), and stop.
            if max_bytes.is_some() {
                warn!(
                    "MQTTD_MEMORY_MAX_BYTES is set but this process's RSS cannot be read \
                     (/proc/self/status unavailable — non-Linux?). The memory watermark is \
                     NOT being enforced; use the container/cgroup memory limit instead."
                );
            } else {
                info!("process RSS is not readable on this platform; memory metrics disabled");
            }
            return;
        };

        if let Some(m) = &metrics {
            m.set_process_resident_bytes(rss);
        }

        if let Some(max) = max_bytes {
            let now_over = rss > max;
            if now_over != brownout {
                brownout = now_over;
                let cmd = HubCommand::SetBrownout {
                    axis: BrownoutAxis::Memory,
                    on: now_over,
                };
                if hub.send(cmd).is_err() {
                    return; // hub gone: shutting down
                }
            }
        }
        tokio::time::sleep(poll).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The real format, verbatim from a Linux box — including the surrounding lines, so
    /// the parser is proven to pick the right one rather than the first number it sees.
    const STATUS: &str = "\
Name:\tmqttd
Umask:\t0022
State:\tS (sleeping)
VmPeak:\t 1234567 kB
VmSize:\t 1234000 kB
VmLck:\t       0 kB
VmHWM:\t   65432 kB
VmRSS:\t   40960 kB
RssAnon:\t   30000 kB
Threads:\t8
";

    #[test]
    fn vm_rss_is_parsed_in_bytes_from_the_right_line() {
        assert_eq!(parse_vm_rss(STATUS), Some(40960 * 1024));
    }

    /// `VmSize` and `VmHWM` sit above `VmRSS` and are larger; picking either would
    /// over-report and brown out early, so the prefix match must be exact.
    #[test]
    fn a_similar_looking_line_is_not_mistaken_for_vm_rss() {
        assert_ne!(parse_vm_rss(STATUS), Some(1_234_000 * 1024), "VmSize");
        assert_ne!(parse_vm_rss(STATUS), Some(65432 * 1024), "VmHWM");
    }

    #[test]
    fn a_status_without_vm_rss_yields_none_not_zero() {
        assert_eq!(parse_vm_rss("Name:\tmqttd\nThreads:\t8\n"), None);
        assert_eq!(parse_vm_rss(""), None);
    }

    /// The watcher is edge-triggered: crossing the watermark sends one
    /// `SetBrownout{on:true}`, dropping back under sends one `{on:false}`, and steady
    /// states send nothing — otherwise a busy broker would spam the hub every poll.
    #[tokio::test]
    async fn the_watcher_drives_brownout_on_watermark_transitions() {
        static RSS: AtomicU64 = AtomicU64::new(10);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _watch = tokio::spawn(watch(
            Some(100),
            tx,
            None,
            Some(Duration::from_millis(20)),
            Some(Box::new(|| Some(RSS.load(Ordering::Relaxed)))),
        ));

        // Steady and under: nothing is sent.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            rx.try_recv().is_err(),
            "an unchanged, under-watermark state must send nothing"
        );

        // Cross it: exactly one transition.
        RSS.store(500, Ordering::Relaxed);
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout {
                axis: BrownoutAxis::Memory,
                on: true,
            })) => {}
            other => panic!("expected SetBrownout{{Memory, true}}, got {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            rx.try_recv().is_err(),
            "staying over the watermark must not re-send"
        );

        // Recover: exactly one transition back.
        RSS.store(10, Ordering::Relaxed);
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout {
                axis: BrownoutAxis::Memory,
                on: false,
            })) => {}
            other => panic!("expected SetBrownout{{Memory, false}}, got {other:?}"),
        }
    }

    /// With no watermark the watcher still exports the gauge, and never browns out —
    /// so turning on memory *visibility* cannot accidentally turn on enforcement.
    #[tokio::test]
    async fn without_a_watermark_nothing_is_ever_browned_out() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _watch = tokio::spawn(watch(
            None,
            tx,
            None,
            Some(Duration::from_millis(10)),
            Some(Box::new(|| Some(u64::MAX))), // absurdly over any plausible bound
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            rx.try_recv().is_err(),
            "no watermark configured means no brownout, however large RSS is"
        );
    }

    /// An unreadable RSS stops the watcher rather than reporting zero — a zero would
    /// read as "plenty of headroom" and the watermark would never fire.
    #[tokio::test]
    async fn an_unreadable_rss_stops_the_watcher_instead_of_reporting_zero() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(watch(
            Some(100),
            tx,
            None,
            Some(Duration::from_millis(10)),
            Some(Box::new(|| None)),
        ));
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("the watcher must exit, not spin")
            .expect("no panic");
        assert!(rx.try_recv().is_err(), "and it must not claim a brownout");
    }

    /// On Linux the real sampler must work — if this ever returns `None` in CI, the
    /// feature is silently inert there and the test says so.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_real_sampler_reports_a_plausible_rss_on_linux() {
        let rss = resident_bytes().expect("RSS must be readable on Linux");
        assert!(
            rss > 1024 * 1024,
            "a running test process should hold more than 1 MiB; got {rss}"
        );
    }
}
