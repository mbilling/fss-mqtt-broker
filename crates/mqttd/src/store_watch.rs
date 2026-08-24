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
//! **The cadence, and the overshoot it costs** (issue #243, 0041-T14). The scan runs every
//! `MQTTD_WATERMARK_POLL` seconds (`[limits] watermark_poll_secs`, default 10, range
//! 1..=300), and within 10% of the mark every `poll / 10` (floor 1 s) — see
//! [`WatermarkPoll`], the one policy this watcher and [`memory_watch`](crate::memory_watch)
//! share. Because the mark is checked on a scan and not charged at the write, the store can
//! overshoot it by `interval x growth rate` plus the write already in flight: at the default
//! that is one second's worth of growth in the band that matters, ten seconds' worth from a
//! standing start. The same interval bounds RECOVERY — a browned-out node is always inside
//! the band, so this is how long the 0041-T11 publish refusal outlives the pressure.
//! Charging the mark at append time (bounding overshoot by ONE write) is the correct fix for
//! the residual and lives in the hub's append path, not here (0041-T9's sibling).
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
//!
//! A FOURTH edge, and the only one that can be full message payloads: **a browned-out node
//! keeps growing `replicas.redb` for groups it merely FOLLOWS.** The refusal is decided at
//! the group's session owner, so a follower applies its peers' already-committed appends
//! unconditionally — `brownout` is consulted nowhere in `mqtt-cluster` (the inbound
//! `PeerMessage::Replicate` arm hands straight to the replica writer, and
//! `cluster_log::ReplicaState::apply`/`apply_batch` have no watermark input). This is
//! deliberate rather than an oversight: refusing entries a quorum has already committed
//! would not enforce a watermark, it would silently thin the group's replica count, which
//! is `min_replicas`' business. On a cluster node — the default — it means the *dominant*
//! store's growth is not gated locally at all, so headroom below the mark must cover
//! peer-driven growth for the whole detect-and-recover window.

use crate::hub::{BrownoutAxis, HubCommand};
use mqtt_observability::metrics::Metrics;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// The redb store files a node may hold (ADR 0018): gauge label per store.
///
/// The `replicas` entry names the single-file layout; since ADR 0076 T2 that
/// store may instead span K shard files, so [`store_bytes`] measures a store by
/// **every file it owns**, not by one name. A watcher that stopped counting the
/// dominant store would silently stop protecting the disk — the failure mode is
/// invisible (an absent file reads as zero bytes), which is why the shard sweep
/// lives here rather than in each caller.
pub const STORE_FILES: [(&str, &str); 4] = [
    ("sessions", "sessions.redb"),
    ("retained", "retained.redb"),
    ("replicas", "replicas.redb"),
    ("lease", "lease.redb"),
];

/// Bytes held under `dir` by the store whose single-file name is `file` —
/// summing its shards when it has them (ADR 0076 T2). Absent files count zero.
#[must_use]
pub fn store_bytes(dir: &Path, file: &str) -> u64 {
    let one = |p: std::path::PathBuf| std::fs::metadata(p).map_or(0, |m| m.len());
    let single = one(dir.join(file));
    if single > 0 || file != "replicas.redb" {
        return single;
    }
    (0..mqtt_cluster::cluster_log::R_MAX_SHARDS)
        .map(|shard| one(dir.join(mqtt_cluster::cluster_log::shard_file_name(shard))))
        .sum()
}

/// The share of the aggregate mark one store may hold before it is named in a WARN.
const SKEW_ON_PCT: u64 = 70;
/// …and the share it must fall back below before that report re-arms (hysteresis: a
/// store parked at the threshold must not log every poll).
const SKEW_OFF_PCT: u64 = 60;

/// The poll policy **both** watermark watchers share (`MQTTD_WATERMARK_POLL`,
/// ADR 0041 T14). Stated once here, like the refusal enumeration above, because two
/// axes sampling at different rates would be two different overshoot bounds to
/// document.
///
/// A steady cadence (default 10 s), shortened to `steady / 10` while the last sample sat
/// within 10% of the mark — the band where overshoot is decided, and equally the band a
/// browned-out node is always in, so the accelerated interval also bounds how long the
/// brownout outlives the pressure that caused it. The clamp is
/// `min(1 s, steady) ..= steady`: it can only ever SHORTEN an interval, so an injected
/// sub-second test cadence is untouched and nothing new becomes observable — only sooner.
#[derive(Debug, Clone, Copy)]
pub struct WatermarkPoll {
    /// The cadence away from the mark.
    steady: Duration,
}

impl Default for WatermarkPoll {
    fn default() -> Self {
        Self::from_secs(10)
    }
}

impl WatermarkPoll {
    /// A policy with an explicit steady cadence (tests inject milliseconds).
    #[must_use]
    pub fn new(steady: Duration) -> Self {
        Self { steady }
    }

    /// A policy from the configured `limits.watermark_poll_secs`. A zero is clamped to
    /// one second defensively — `Config::validate` refuses it long before here, and a
    /// spin loop is not a behaviour worth reproducing if that ever changes.
    #[must_use]
    pub fn from_secs(secs: u64) -> Self {
        Self::new(Duration::from_secs(secs.max(1)))
    }

    /// Is `value` within 10% of `max` — at, over, or just under it?
    #[must_use]
    pub fn near(value: u64, max: u64) -> bool {
        value >= max - max / 10
    }

    /// The next sleep, given whether the last sample was [`near`](Self::near) the mark.
    #[must_use]
    pub fn interval(&self, near: bool) -> Duration {
        if near {
            (self.steady / 10).clamp(Duration::from_secs(1).min(self.steady), self.steady)
        } else {
            self.steady
        }
    }
}

/// Edge-triggered "one store holds most of the aggregate mark" reporting.
///
/// The mark is aggregate on purpose (see the module doc), which leaves the operator one
/// real deficit: WHICH store is eating the budget. `store_bytes{store}` answers that for
/// someone already looking at Prometheus; this answers it in the log, before brownout,
/// at zero config surface. Deliberately un-knobbed: a threshold that only moves a log
/// line is a knob nobody can tune.
#[derive(Debug, Default)]
struct SkewWatch {
    /// The stores currently reported, latched PER STORE rather than one at a time: the
    /// claim four documents make is that *any* single store passing the mark is named, and
    /// a single latch silently swallowed the second one — a two-store squeeze named only
    /// the first, which is the case where knowing both matters most. At most four stores
    /// exist ([`STORE_FILES`]), so a `Vec` is the whole data structure.
    reported: Vec<&'static str>,
}

impl SkewWatch {
    /// `Some((store, bytes))` on a FRESH crossing above [`SKEW_ON_PCT`] of `max` — the
    /// largest store not already latched — and `None` when there is nothing new to say.
    /// Each store is latched independently and silently re-armed once IT falls below
    /// [`SKEW_OFF_PCT`], so one loud store cannot mask another (recovery is not news, and
    /// one report per scan means a squeeze across every store is named within four scans).
    fn update(&mut self, sizes: &[(&'static str, u64)], max: u64) -> Option<(&'static str, u64)> {
        let share =
            |bytes: u64, pct: u64| u128::from(bytes) * 100 >= u128::from(max) * u128::from(pct);
        // Re-arm every latched store that has fallen back under the OFF threshold.
        self.reported.retain(|prev| {
            let held = sizes
                .iter()
                .find(|(name, _)| name == prev)
                .map_or(0, |(_, bytes)| *bytes);
            share(held, SKEW_OFF_PCT)
        });
        let (name, bytes) = sizes
            .iter()
            .copied()
            .filter(|(name, _)| !self.reported.contains(name))
            .filter(|(_, bytes)| share(*bytes, SKEW_ON_PCT))
            .max_by_key(|(_, bytes)| *bytes)?;
        self.reported.push(name);
        Some((name, bytes))
    }
}

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
            let bytes = store_bytes(dir, file);
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
    poll: Option<WatermarkPoll>,
    snapshot: Option<Arc<StoreSnapshot>>,
) {
    let poll = poll.unwrap_or_default();
    let mut brownout = false;
    let mut skew = SkewWatch::default();
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
            // One store eating the aggregate budget is the diagnosis an operator needs
            // BEFORE brownout, and the aggregate mark cannot give it to them.
            if let Some((store, bytes)) = skew.update(&sizes, max) {
                tracing::warn!(
                    store,
                    bytes,
                    max,
                    "one store holds most of the disk watermark: the mark is AGGREGATE, so \
                     this store's growth will brown out the others (ADR 0041 T9) — alert per \
                     store with store_bytes{{store}} / store_max_bytes"
                );
            }
        }
        // Near the mark, re-check sooner: that interval is the overshoot bound AND the
        // recovery lag. With no watermark configured `near` is never true, so
        // visibility-only polling costs exactly what it did before (issue #243).
        tokio::time::sleep(
            poll.interval(max_bytes.is_some_and(|max| WatermarkPoll::near(total, max))),
        )
        .await;
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

    /// The poll policy: a plain steady cadence, shortened only while the last sample
    /// sat within 10% of the mark, and never *lengthened* — an injected sub-second
    /// test cadence must survive the clamp untouched.
    #[test]
    fn the_poll_policy_only_accelerates_near_the_mark() {
        assert!(!WatermarkPoll::near(89, 100), "89% is not yet near");
        assert!(WatermarkPoll::near(90, 100), "90% of the mark is near");
        assert!(
            WatermarkPoll::near(150, 100),
            "over the mark is inside the band, not outside it — that is where recovery \
             is waiting to be seen"
        );
        let ten = WatermarkPoll::from_secs(10);
        assert_eq!(ten.interval(false), Duration::from_secs(10));
        assert_eq!(ten.interval(true), Duration::from_secs(1));
        let fast = WatermarkPoll::new(Duration::from_millis(20));
        assert_eq!(fast.interval(false), Duration::from_millis(20));
        assert_eq!(
            fast.interval(true),
            Duration::from_millis(20),
            "the 1 s floor must never lengthen a configured interval"
        );
    }

    /// Approaching the mark shortens the next scan, so the crossing is caught an order
    /// of magnitude inside the configured cadence — this is the whole overshoot bound
    /// the docs quote (issue #243).
    // Virtual clock (`start_paused`): every wait in this test is then an exact number of the
    // watcher's own injected poll cycles rather than a wall-clock guess, and it costs no real
    // time. Tokio advances to the next timer only when the runtime is idle, so "N cycles have
    // happened" becomes deterministic — which is what a test about poll cadence needs.
    #[tokio::test(start_paused = true)]
    async fn nearing_the_watermark_shortens_the_poll_so_overshoot_is_bounded() {
        let dir = temp_dir("near");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // 95 of a 100-byte mark: inside the band, still under it.
        std::fs::write(dir.join("sessions.redb"), vec![0u8; 95]).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _watch = tokio::spawn(watch(
            dir.clone(),
            Some(100),
            tx,
            None,
            Some(WatermarkPoll::from_secs(20)),
            None,
        ));
        // The first scan is under the mark, so nothing is sent — but it scheduled the
        // accelerated interval, not the 20 s one.
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::write(dir.join("retained.redb"), vec![0u8; 60]).unwrap();
        match tokio::time::timeout(Duration::from_secs(6), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout {
                axis: BrownoutAxis::Disk,
                on: true,
            })) => {}
            other => panic!("expected an accelerated SetBrownout(true), got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The same acceleration decides how long the brownout OUTLIVES the pressure: above
    /// the mark the node is by definition inside the band, so this interval is the tail
    /// of the #238 publish outage after the store drains.
    #[tokio::test]
    async fn a_recovered_watermark_lifts_the_brownout_at_the_accelerated_cadence() {
        let dir = temp_dir("recover-fast");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("sessions.redb"), vec![0u8; 200]).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _watch = tokio::spawn(watch(
            dir.clone(),
            Some(100),
            tx,
            None,
            Some(WatermarkPoll::from_secs(20)),
            None,
        ));
        match tokio::time::timeout(Duration::from_secs(6), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout { on: true, .. })) => {}
            other => panic!("expected SetBrownout(true) on the first scan, got {other:?}"),
        }
        std::fs::remove_file(dir.join("sessions.redb")).unwrap();
        match tokio::time::timeout(Duration::from_secs(6), rx.recv()).await {
            Ok(Some(HubCommand::SetBrownout {
                axis: BrownoutAxis::Disk,
                on: false,
            })) => {}
            other => panic!("expected an accelerated SetBrownout(false), got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The per-store skew report says its piece ONCE per crossing and clears with
    /// hysteresis — a poll-rate log line about a store parked at the threshold would be
    /// noise, and noise in the operator's own diagnosis path is what #238 taught.
    #[test]
    fn the_store_skew_report_is_edge_triggered_with_hysteresis() {
        let sizes = |sessions: u64, retained: u64| {
            vec![
                ("sessions", sessions),
                ("retained", retained),
                ("replicas", 0),
                ("lease", 0),
            ]
        };
        let mut skew = SkewWatch::default();
        assert_eq!(
            skew.update(&sizes(75, 0), 100),
            Some(("sessions", 75)),
            "75% of the aggregate mark in one store is the crossing"
        );
        assert_eq!(
            skew.update(&sizes(75, 0), 100),
            None,
            "reported once, not per poll"
        );
        assert_eq!(
            skew.update(&sizes(65, 0), 100),
            None,
            "between OFF and ON it stays reported, without repeating"
        );
        assert_eq!(
            skew.update(&sizes(75, 0), 100),
            None,
            "a store oscillating inside the band must not re-report — this is the whole \
             point of the OFF threshold sitting below the ON one"
        );
        assert_eq!(
            skew.update(&sizes(55, 0), 100),
            None,
            "below OFF it clears silently"
        );
        assert_eq!(
            skew.update(&sizes(75, 0), 100),
            Some(("sessions", 75)),
            "a fresh crossing reports again"
        );
        // A different store taking over the share is a NEW report, not silence behind
        // the old one.
        assert_eq!(
            skew.update(&sizes(10, 75), 100),
            Some(("retained", 75)),
            "the report must follow whichever store holds the share"
        );
        // A balanced spread is never reported, however full the aggregate is.
        let mut balanced = SkewWatch::default();
        assert_eq!(balanced.update(&sizes(45, 45), 100), None);

        // ONE LOUD STORE MUST NOT MASK ANOTHER (issue #243 review). A single latch made
        // the second store's crossing silent until the first fell under OFF, while README,
        // SIZING, COMPARISON and ADR 0041 all promise that ANY single store passing the
        // mark is named — and a two-store squeeze is exactly when the operator needs both
        // names. Latching is therefore per store.
        let mut two = SkewWatch::default();
        assert_eq!(two.update(&sizes(75, 0), 100), Some(("sessions", 75)));
        assert_eq!(
            two.update(&sizes(65, 80), 100),
            Some(("retained", 80)),
            "a second store over the mark must be named even while the first is still \
             latched above the OFF threshold"
        );
        assert_eq!(
            two.update(&sizes(65, 80), 100),
            None,
            "...and then both stay latched: still one report per crossing, not per poll"
        );
    }

    /// The watcher is edge-triggered: crossing the watermark sends
    /// `SetBrownout(true)`, dropping back under it sends `SetBrownout(false)`,
    /// and steady states send nothing further.
    // Virtual clock (`start_paused`): every wait in this test is then an exact number of the
    // watcher's own injected poll cycles rather than a wall-clock guess, and it costs no real
    // time. Tokio advances to the next timer only when the runtime is idle, so "N cycles have
    // happened" becomes deterministic — which is what a test about poll cadence needs.
    #[tokio::test(start_paused = true)]
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
            Some(WatermarkPoll::new(Duration::from_millis(20))),
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
