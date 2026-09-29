//! A temporary log filter (ADR 0081 T9): raise verbosity for an incident without a restart,
//! and have it fall back on its own.
//!
//! The process's filter sits behind a `tracing_subscriber` reload layer. An override
//! replaces it until its TTL (at most an hour) runs out, then the configured filter
//! (`RUST_LOG`, default `info`) is restored — by a timer, so an operator who forgets does
//! not leave a node logging at `trace`. Only the most recent override's timer restores:
//! setting a new one supersedes the old timer.
//!
//! **The audit trail is never filtered away.** Audit records are `tracing` events with
//! target `audit` (ADR 0066); an override that names that target is refused, and every
//! override keeps `audit=info`, so a quieter filter cannot hide an audit record from the
//! log shipper.

use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::time::Instant;
use tracing_subscriber::reload;
use tracing_subscriber::{EnvFilter, Registry};

/// The longest an override may last.
pub const MAX_TTL: Duration = Duration::from_secs(3600);

/// The target audit records are logged under; an override never touches it.
const AUDIT_TARGET: &str = "audit";

/// The process-wide filter, installed at startup by the binary.
static GLOBAL: OnceLock<LogFilter> = OnceLock::new();

/// The installed filter, if the binary installed one (tests and embedders may not).
#[must_use]
pub fn global() -> Option<&'static LogFilter> {
    GLOBAL.get()
}

/// Install `filter` as the process-wide one. A second install is ignored.
pub fn install(filter: LogFilter) {
    let _ = GLOBAL.set(filter);
}

/// An active override.
#[derive(Debug, Clone)]
struct Override {
    filter: String,
    until: Instant,
    generation: u64,
}

/// The reloadable filter: the configured base and at most one override.
pub struct LogFilter {
    handle: reload::Handle<EnvFilter, Registry>,
    base: String,
    state: Mutex<(Option<Override>, u64)>,
}

impl std::fmt::Debug for LogFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFilter")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

/// What `status` reports.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FilterStatus {
    /// The configured filter, restored when an override ends.
    pub base: String,
    /// The override in force, if any.
    pub override_filter: Option<String>,
    /// Seconds until it ends.
    pub override_remaining_secs: Option<u64>,
}

impl LogFilter {
    /// Wrap the reload `handle` of a filter layer built from `base`.
    #[must_use]
    pub fn new(handle: reload::Handle<EnvFilter, Registry>, base: String) -> Self {
        Self {
            handle,
            base,
            state: Mutex::new((None, 0)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, (Option<Override>, u64)> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Replace the filter with `directives` for `ttl` (clamped to 1 s ..= [`MAX_TTL`]),
    /// then restore the base. Returns the effective filter string.
    ///
    /// # Errors
    /// A message when `directives` do not parse, name the audit target, or the reload
    /// layer is gone.
    pub fn set(&'static self, directives: &str, ttl: Duration) -> Result<String, String> {
        let directives = directives.trim();
        if directives.is_empty() {
            return Err("the filter must not be empty".to_string());
        }
        if names_audit_target(directives) {
            return Err(format!(
                "the {AUDIT_TARGET:?} target cannot be filtered: audit records always log"
            ));
        }
        let effective = format!("{directives},{AUDIT_TARGET}=info");
        let filter = EnvFilter::try_new(&effective).map_err(|e| format!("invalid filter: {e}"))?;
        let ttl = ttl.clamp(Duration::from_secs(1), MAX_TTL);
        self.handle
            .reload(filter)
            .map_err(|e| format!("the log filter could not be replaced: {e}"))?;
        let generation = {
            let mut state = self.lock();
            state.1 += 1;
            state.0 = Some(Override {
                filter: effective.clone(),
                until: Instant::now() + ttl,
                generation: state.1,
            });
            state.1
        };
        tracing::warn!(filter = %effective, ttl_secs = ttl.as_secs(), "log filter overridden by an admin action");
        tokio::spawn(async move {
            tokio::time::sleep(ttl).await;
            self.expire(generation);
        });
        Ok(effective)
    }

    /// Restore the base filter now (ends any override).
    ///
    /// # Errors
    /// A message when the reload layer is gone.
    pub fn reset(&self) -> Result<(), String> {
        self.lock().0 = None;
        self.restore_base()
    }

    /// The timer's end of override `generation`: restores only if it is still current.
    fn expire(&self, generation: u64) {
        let current = {
            let mut state = self.lock();
            if state.0.as_ref().map(|o| o.generation) == Some(generation) {
                state.0 = None;
                true
            } else {
                false
            }
        };
        if current {
            if let Err(e) = self.restore_base() {
                tracing::warn!(error = %e, "could not restore the log filter after its override");
            } else {
                tracing::info!(filter = %self.base, "log filter override expired; configured filter restored");
            }
        }
    }

    fn restore_base(&self) -> Result<(), String> {
        let filter = EnvFilter::try_new(&self.base).map_err(|e| e.to_string())?;
        self.handle
            .reload(filter)
            .map_err(|e| format!("the log filter could not be restored: {e}"))
    }

    /// The base filter and the override in force.
    #[must_use]
    pub fn status(&self) -> FilterStatus {
        let state = self.lock();
        let now = Instant::now();
        FilterStatus {
            base: self.base.clone(),
            override_filter: state.0.as_ref().map(|o| o.filter.clone()),
            override_remaining_secs: state
                .0
                .as_ref()
                .map(|o| o.until.saturating_duration_since(now).as_secs()),
        }
    }
}

/// Whether any directive in `directives` targets `audit` (or a module under it).
fn names_audit_target(directives: &str) -> bool {
    directives.split(',').any(|d| {
        let target = d.trim().split(['=', '[']).next().unwrap_or_default().trim();
        target == AUDIT_TARGET || target.starts_with("audit::")
    })
}

/// Build the process's subscriber: a reloadable `EnvFilter` over the default formatter,
/// installed globally, with the handle stored for [`global`]. `base` is the configured
/// filter (`RUST_LOG`, or `info`).
pub fn init(base: &str) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let filter = EnvFilter::try_new(base).unwrap_or_else(|_| EnvFilter::new("info"));
    let (layer, handle) = reload::Layer::new(filter);
    tracing_subscriber::registry()
        .with(layer)
        .with(tracing_subscriber::fmt::layer())
        .init();
    install(LogFilter::new(handle, base.to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A filter whose reload layer is not installed globally: the handle works on the
    /// layer the test keeps alive.
    fn leaked_filter(base: &str) -> (&'static LogFilter, reload::Layer<EnvFilter, Registry>) {
        let (layer, handle) = reload::Layer::new(EnvFilter::new(base));
        let filter: &'static LogFilter = Box::leak(Box::new(LogFilter::new(handle, base.into())));
        (filter, layer)
    }

    fn current(f: &LogFilter) -> String {
        f.handle.with_current(ToString::to_string).unwrap()
    }

    #[test]
    fn audit_directives_are_recognized() {
        assert!(names_audit_target("audit=off"));
        assert!(names_audit_target("info, audit=warn"));
        assert!(names_audit_target("audit::x=trace"));
        assert!(names_audit_target("audit"));
        assert!(!names_audit_target("mqttd::hub=debug,auditor=trace"));
    }

    #[tokio::test(start_paused = true)]
    async fn an_override_applies_then_expires_back_to_the_base() {
        let (f, _layer) = leaked_filter("info");
        let effective = f.set("mqttd::hub=debug", Duration::from_secs(60)).unwrap();
        assert_eq!(effective, "mqttd::hub=debug,audit=info");
        assert!(current(f).contains("mqttd::hub=debug"), "{}", current(f));
        assert!(current(f).contains("audit=info"), "{}", current(f));
        let s = f.status();
        assert_eq!(
            s.override_filter.as_deref(),
            Some("mqttd::hub=debug,audit=info")
        );
        assert_eq!(s.override_remaining_secs, Some(60));

        tokio::time::sleep(Duration::from_secs(61)).await;
        assert_eq!(current(f), "info");
        assert_eq!(f.status().override_filter, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_newer_override_is_not_ended_by_the_older_timer() {
        let (f, _layer) = leaked_filter("warn");
        f.set("debug", Duration::from_secs(10)).unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
        f.set("trace", Duration::from_secs(30)).unwrap();
        // The first timer fires at t=10 and must not restore the base under the second.
        tokio::time::sleep(Duration::from_secs(6)).await;
        // (`EnvFilter` prints its directives in its own order.)
        assert!(
            current(f).split(',').any(|d| d == "trace"),
            "{}",
            current(f)
        );
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(current(f), "warn");
    }

    #[tokio::test(start_paused = true)]
    async fn bad_filters_and_the_audit_target_are_refused_and_ttl_is_capped() {
        let (f, _layer) = leaked_filter("info");
        assert!(f.set("audit=off", Duration::from_secs(10)).is_err());
        assert!(f.set("", Duration::from_secs(10)).is_err());
        assert!(f.set("mqttd=notalevel", Duration::from_secs(10)).is_err());
        assert_eq!(current(f), "info", "a refused override changes nothing");
        f.set("debug", Duration::from_secs(99_999)).unwrap();
        assert_eq!(f.status().override_remaining_secs, Some(MAX_TTL.as_secs()));
        f.reset().unwrap();
        assert_eq!(current(f), "info");
        assert_eq!(f.status().override_filter, None);
    }
}
