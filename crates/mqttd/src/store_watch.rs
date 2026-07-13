//! Disk visibility + watermark brownout
//! ([ADR 0041](../../../docs/adr/0041-resource-governance.md) T5).
//!
//! A small poller stats each redb store file under the data directory, exports
//! its size as the `store_bytes{store}` gauge (ADR 0020), and — when
//! `MQTTD_STORE_MAX_BYTES` is set — drives the hub's **brownout** flag on
//! watermark transitions: above it, writes that *grow* durable state (new
//! retained topics, new sessions, offline enqueues) are refused with the quota
//! behaviors while acks, deletes, expiry, and resumes continue. A broker
//! approaching disk-full degrades to read-mostly instead of hitting the cliff
//! where redb commits start failing mid-write.

use crate::hub::HubCommand;
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
) {
    let poll = poll.unwrap_or(POLL_INTERVAL);
    let mut brownout = false;
    loop {
        let (sizes, total) = scan(&dir);
        if let Some(m) = &metrics {
            for (store, bytes) in &sizes {
                m.set_store_bytes(store, *bytes);
            }
        }
        if let Some(max) = max_bytes {
            let now_over = total > max;
            if now_over != brownout {
                brownout = now_over;
                if hub.send(HubCommand::SetBrownout(now_over)).is_err() {
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
        let _watch = tokio::spawn(watch(
            dir.clone(),
            Some(100),
            tx,
            None,
            Some(Duration::from_millis(20)),
        ));

        // Under the watermark: no command arrives.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(rx.try_recv().is_err(), "under the mark, nothing is sent");

        // Cross it: exactly one SetBrownout(true).
        std::fs::write(dir.join("retained.redb"), vec![0u8; 200]).unwrap();
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout(true))) => {}
            other => panic!("expected SetBrownout(true), got {other:?}"),
        }

        // Recover below it: exactly one SetBrownout(false).
        std::fs::remove_file(dir.join("retained.redb")).unwrap();
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout(false))) => {}
            other => panic!("expected SetBrownout(false), got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
