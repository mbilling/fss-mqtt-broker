//! Filesystem-watch auto-reload of the security policy
//! ([ADR 0033](../../../docs/adr/0033-config-file-watch-reload.md)).
//!
//! ADR 0032 made the policy hot-reloadable on `SIGHUP`. This adds an **opt-in** watcher that
//! reloads when a configured policy file changes on disk — for declarative/GitOps operation
//! (a Kubernetes ConfigMap/Secret is updated on disk with no process signal). It is off by
//! default (`MQTTD_CONFIG_WATCH=<seconds>` enables it) and reuses the **same**
//! [`Reloader::reload`](crate::reload::Reloader::reload) — so it inherits ADR 0032's
//! validate-before-swap fail-safe verbatim: a partial/malformed write is *rejected* and the
//! running policy is kept.
//!
//! Detection is **stat-stamp polling** (no new dependency): each poll compares a stamp =
//! `(mtime, length, inode)` per watched file against the last *successfully applied* stamp.
//! Any difference — including an atomic-rename, which swaps the inode — triggers a reload;
//! recording the last *applied* (not merely last *seen*) stamp means a rejected reload is
//! **retried on the next poll until it parses**, converging on one apply per settled edit.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tracing::debug;

use crate::reload::Reloader;

/// A stat stamp of a watched file — enough to detect any content change, including a
/// same-second / same-length edit and an atomic-rename (which replaces the inode). A missing
/// or unreadable file stamps as `None`, which differs from a present file, so its appearance
/// or disappearance is itself a change.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stamp {
    mtime: Option<std::time::SystemTime>,
    len: u64,
    inode: u64,
}

fn stamp(path: &Path) -> Option<Stamp> {
    let md = std::fs::metadata(path).ok()?;
    Some(Stamp {
        mtime: md.modified().ok(),
        len: md.len(),
        inode: inode_of(&md),
    })
}

#[cfg(unix)]
fn inode_of(md: &std::fs::Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(md)
}

#[cfg(not(unix))]
fn inode_of(_md: &std::fs::Metadata) -> u64 {
    0 // no portable inode; mtime + length still catch edits
}

/// Stat-stamp poller over the configured policy files (ADR 0033). Holds the stamps that were
/// last **successfully applied**, so a rejected reload is retried until it parses.
#[derive(Debug)]
pub struct ConfigWatcher {
    paths: Vec<PathBuf>,
    applied: Vec<Option<Stamp>>,
}

impl ConfigWatcher {
    /// Seed from the current on-disk state — the policy the broker already loaded at startup —
    /// so only a *later* change triggers a reload.
    #[must_use]
    pub fn new(paths: Vec<PathBuf>) -> Self {
        let applied = paths.iter().map(|p| stamp(p)).collect();
        Self { paths, applied }
    }

    /// One poll. If any watched file's stamp differs from the last applied set, call `reload`;
    /// advance the applied stamps **only if it returned `true`** (a successful swap), so a
    /// partial/malformed write that `reload` rejects is retried on the next poll. Returns
    /// whether a change was detected (i.e. a reload was attempted).
    pub fn tick(&mut self, reload: impl FnOnce() -> bool) -> bool {
        let current: Vec<Option<Stamp>> = self.paths.iter().map(|p| stamp(p)).collect();
        if current == self.applied {
            return false;
        }
        debug!("config-file change detected; reloading security policy (ADR 0033)");
        if reload() {
            self.applied = current;
        }
        true
    }
}

/// The watcher task: poll every `interval`, reloading via `reloader` (trigger `"watch"`) on a
/// detected change, until `shutdown`. Spawned only when `MQTTD_CONFIG_WATCH` is set.
pub async fn watch(
    reloader: Arc<Reloader>,
    paths: Vec<PathBuf>,
    interval: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut watcher = ConfigWatcher::new(paths);
    let mut ticker = tokio::time::interval(interval);
    // A slow reload must not bunch up missed ticks into a burst.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = ticker.tick() => {
                watcher.tick(|| reloader.reload("watch"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigWatcher;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Write `content` to a unique temp file and return its path.
    fn temp_file(tag: &str, content: &str) -> std::path::PathBuf {
        use std::sync::atomic::AtomicU64;
        static UNIQUE: AtomicU64 = AtomicU64::new(0);
        let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mqttd-watch-{}-{tag}-{n}", std::process::id()));
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn no_change_does_not_reload() {
        // Seeded from the on-disk state, an unchanged file never triggers a reload — the
        // watcher is inert until something actually changes.
        let path = temp_file("inert", "init");
        let mut w = ConfigWatcher::new(vec![path]);
        assert!(
            !w.tick(|| panic!("must not reload when nothing changed")),
            "no change detected"
        );
    }

    #[test]
    fn a_settled_edit_applies_exactly_once() {
        let path = temp_file("edit", "initial-content");
        let mut w = ConfigWatcher::new(vec![path.clone()]);

        // Edit the file (different length): the next poll detects it and applies.
        std::fs::write(&path, "v2").unwrap();
        let calls = AtomicU32::new(0);
        assert!(w.tick(|| {
            calls.fetch_add(1, Ordering::Relaxed);
            true
        }));
        assert_eq!(calls.load(Ordering::Relaxed), 1, "applied once");

        // No further change → no further reload.
        assert!(!w.tick(|| panic!("must not reload a settled file again")));
    }

    #[test]
    fn a_rejected_reload_is_retried_until_it_parses() {
        // The retry-until-parse property (ADR 0033 §4): a partial/malformed write that the
        // reload *rejects* must not advance the applied marker — it is retried every poll
        // until a clean read swaps, after which the file is inert again.
        let path = temp_file("retry", "initial-content");
        let mut w = ConfigWatcher::new(vec![path.clone()]);

        std::fs::write(&path, "partial").unwrap();
        // First poll: reload rejects (e.g. half-written file fails to parse).
        assert!(w.tick(|| false), "change detected, reload attempted");
        // Second poll: the file is unchanged but was never applied → attempted AGAIN, not
        // silently skipped. This is the retry the marker enables.
        assert!(w.tick(|| false), "rejected reload is retried, not skipped");
        // Third poll: the reload now succeeds → the marker advances.
        assert!(w.tick(|| true), "a clean read finally applies");
        // Settled: no more attempts.
        assert!(!w.tick(|| panic!("must not retry after a successful apply")));
    }

    #[test]
    fn an_atomic_rename_is_detected() {
        // A ConfigMap update / atomic rename swaps the inode even if length and mtime match;
        // the inode in the stamp catches it.
        let path = temp_file("rename", "same-length-a");
        let mut w = ConfigWatcher::new(vec![path.clone()]);
        // Replace via rename of a same-length file (new inode).
        let replacement = temp_file("rename-src", "same-length-b");
        std::fs::rename(&replacement, &path).unwrap();
        assert!(
            w.tick(|| true),
            "an atomic rename (new inode) is detected even at equal length"
        );
    }
}
