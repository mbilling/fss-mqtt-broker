//! Disk visibility + watermark brownout
//! ([ADR 0041](../../../docs/adr/0041-resource-governance.md) T5).
//!
//! A small poller stats each redb store file under the data directory, exports
//! its size as the `store_bytes{store}` gauge (ADR 0020), and — when
//! `MQTTD_STORE_MAX_BYTES` is set — drives the hub's **brownout** flag on
//! watermark transitions: above it, writes that *grow* durable state are refused
//! while acks, deletes, expiry, and resumes continue. A broker approaching
//! disk-full degrades to read-mostly instead of hitting the cliff where redb
//! commits start failing mid-write.
//!
//! **What "refused" means to the client**, per growth write — the ambiguity in the
//! sentence above is what produced issue #238, so it is spelled out here rather
//! than left to the reader:
//!
//! - a NEW SESSION: the CONNECT is refused (`0x97` for v5, `0x03` for v3.1.1);
//! - an OFFLINE/DURABLE ENQUEUE a `QoS` ≥ 1 subscriber is owed: the PUBLISHER is
//!   refused, not acked (0041-T11) — a v5 publisher is told `0x97`, a v3.1.1
//!   publisher gets no PUBACK and a close, and nothing is stored or delivered.
//!   Cross-node, the refusal travels as a peer-bus verdict at proto ≥ 7; against an
//!   older peer it degrades to a withheld ack (0041-T12);
//! - a NEW RETAINED TOPIC: refused as over-quota (`0x97` for v5; a v3.1.1 retained
//!   publish is delivered live and answered with a plain PUBACK, its retained value
//!   simply not stored);
//! - an UNGATED publish (a Will, a retained-window back-fill): there is no publisher
//!   to refuse, so the durable copy is dropped and COUNTED as a drop while the live
//!   delivery still happens — suppressing it would destroy the message outright.
//!
//! Nothing that owes no durable growth is affected: `QoS` 0, clean sessions, and
//! every ack, read, delete, expiry and resume continue.
//!
//! **"Growth is refused" is not airtight**, and sizing headroom against it as if it
//! were would repeat #238's mistake in the other direction. Three growth writes are
//! deliberately NOT gated, because each protects an honesty property worth more than
//! the bytes: the inbound `QoS` 2 dedup record (session metadata written BEFORE the
//! hub decides, so a refusal can never race a duplicate — issue #165's ordering);
//! SUBSCRIBE persistence (a session's subscription set, bounded by
//! `max_subscriptions_per_client`); and the detach backlog spill (messages already
//! accepted while the subscriber was attached — refusing them at detach would drop
//! acked messages). All three are small (metadata and already-owed entries, never new
//! message payloads at publish time), but a sustained brownout with active clients
//! does keep growing the sessions store slowly. The watermark must be set with real
//! headroom below disk-full, not at it.

use crate::hub::{BrownoutAxis, HubCommand};
use mqtt_observability::metrics::Metrics;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// The redb store files a node may hold (ADR 0018): gauge label per store.
const STORE_FILES: [(&str, &str); 4] = [
    ("sessions", "sessions.redb"),
    ("retained", "retained.redb"),
    ("replicas", "replicas.redb"),
    ("lease", "lease.redb"),
];

/// How often the watcher re-stats the store files.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// A shared snapshot of the last store scan, read by the `/statusz` body
/// (ADR 0054). Updated every poll; all zeros until the first scan (or forever,
/// without a data dir). Order matches [`STORE_FILES`].
#[derive(Debug, Default)]
pub struct StoreSnapshot {
    /// Per-store bytes, in [`STORE_FILES`] order.
    sizes: [std::sync::atomic::AtomicU64; 4],
    /// The configured watermark (0 = none).
    max_bytes: std::sync::atomic::AtomicU64,
}

impl StoreSnapshot {
    /// `(store name, bytes)` for every store, plus the configured watermark
    /// (`None` when no watermark is set).
    #[must_use]
    pub fn read(&self) -> (Vec<(&'static str, u64)>, Option<u64>) {
        use std::sync::atomic::Ordering::Relaxed;
        let sizes = STORE_FILES
            .iter()
            .zip(&self.sizes)
            .map(|((name, _), v)| (*name, v.load(Relaxed)))
            .collect();
        let max = self.max_bytes.load(Relaxed);
        (sizes, (max > 0).then_some(max))
    }
}

/// Stat every store file under `dir`; absent files count as zero bytes.
/// Returns `(store name, bytes)` pairs plus the total.
#[must_use]
pub fn scan(dir: &Path) -> (Vec<(&'static str, u64)>, u64) {
    let mut total = 0;
    let sizes = STORE_FILES
        .iter()
        .map(|(name, file)| {
            let bytes = std::fs::metadata(dir.join(file)).map_or(0, |m| m.len());
            total += bytes;
            (*name, bytes)
        })
        .collect();
    (sizes, total)
}

/// Run the store watcher until the hub goes away: export sizes every poll and,
/// with a watermark configured, send [`HubCommand::SetBrownout`] on transitions
/// (edge-triggered — an unchanged state sends nothing).
pub async fn watch(
    dir: std::path::PathBuf,
    max_bytes: Option<u64>,
    hub: mpsc::UnboundedSender<HubCommand>,
    metrics: Option<Arc<Metrics>>,
    poll: Option<Duration>,
    snapshot: Option<Arc<StoreSnapshot>>,
) {
    let poll = poll.unwrap_or(POLL_INTERVAL);
    let mut brownout = false;
    if let (Some(m), Some(max)) = (&metrics, max_bytes) {
        m.set_store_max_bytes(max); // constant per process; exported once (ADR 0054)
    }
    if let Some(s) = &snapshot {
        s.max_bytes
            .store(max_bytes.unwrap_or(0), std::sync::atomic::Ordering::Relaxed);
    }
    loop {
        let (sizes, total) = scan(&dir);
        if let Some(m) = &metrics {
            for (store, bytes) in &sizes {
                m.set_store_bytes(store, *bytes);
            }
        }
        if let Some(s) = &snapshot {
            for ((_, bytes), slot) in sizes.iter().zip(&s.sizes) {
                slot.store(*bytes, std::sync::atomic::Ordering::Relaxed);
            }
        }
        if let Some(max) = max_bytes {
            let now_over = total > max;
            if now_over != brownout {
                brownout = now_over;
                let cmd = HubCommand::SetBrownout {
                    axis: BrownoutAxis::Disk,
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

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("mqttd-watch-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// `scan` reports each store's size (absent files as zero) and the total.
    #[test]
    fn scan_reports_per_store_sizes_and_the_total() {
        let dir = temp_dir("scan");
        std::fs::write(dir.join("sessions.redb"), vec![0u8; 300]).unwrap();
        std::fs::write(dir.join("retained.redb"), vec![0u8; 200]).unwrap();
        let (sizes, total) = scan(&dir);
        assert_eq!(total, 500);
        assert!(sizes.contains(&("sessions", 300)));
        assert!(sizes.contains(&("retained", 200)));
        assert!(sizes.contains(&("replicas", 0)), "absent files count zero");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The watcher is edge-triggered: crossing the watermark sends
    /// `SetBrownout(true)`, dropping back under it sends `SetBrownout(false)`,
    /// and steady states send nothing further.
    #[tokio::test]
    async fn the_watcher_drives_brownout_on_watermark_transitions() {
        let dir = temp_dir("edge");
        std::fs::write(dir.join("sessions.redb"), vec![0u8; 10]).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        // The shared snapshot (ADR 0054) fills alongside the metrics/brownout path.
        let snapshot = Arc::new(StoreSnapshot::default());
        let _watch = tokio::spawn(watch(
            dir.clone(),
            Some(100),
            tx,
            None,
            Some(Duration::from_millis(20)),
            Some(snapshot.clone()),
        ));

        // Under the watermark: no command arrives.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(rx.try_recv().is_err(), "under the mark, nothing is sent");

        // Cross it: exactly one SetBrownout(true).
        std::fs::write(dir.join("retained.redb"), vec![0u8; 200]).unwrap();
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout {
                axis: BrownoutAxis::Disk,
                on: true,
            })) => {}
            other => panic!("expected SetBrownout(true), got {other:?}"),
        }

        // Recover below it: exactly one SetBrownout(false).
        std::fs::remove_file(dir.join("retained.redb")).unwrap();
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout {
                axis: BrownoutAxis::Disk,
                on: false,
            })) => {}
            other => panic!("expected SetBrownout(false), got {other:?}"),
        }
        // The /statusz snapshot tracked the scans: watermark recorded, sessions
        // sized, the removed store back at zero (ADR 0054).
        let (sizes, max) = snapshot.read();
        assert_eq!(max, Some(100));
        assert!(sizes.contains(&("sessions", 10)), "{sizes:?}");
        assert!(sizes.contains(&("retained", 0)), "{sizes:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
