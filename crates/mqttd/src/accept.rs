//! What every accept loop does when `accept()` fails (issue #504): continue, never stop.
//!
//! A listener whose loop exits on an accept error is a silent, permanent outage of that
//! listener: the process stays up and nothing else notices. Every TCP listener in the
//! broker (clients, health and metrics, the cluster bus, the admin API) goes through
//! [`pause_after_error`] so none can end that way.

use std::time::Duration;
use tracing::{debug, warn};

/// How long an accept loop pauses after `accept()` returns an error.
///
/// Fixed rather than exponential on purpose: the failure this exists for is a
/// full fd table, which clears when connections drain and not as a function of
/// how long we have been failing. 100 ms is ten attempts a second — free at this
/// scale — so the listener is back within 100 ms of the resource returning,
/// while a tight retry loop would burn a core and free nothing.
pub const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// How long to wait before accepting again after `accept()` failed.
///
/// A listener must NEVER exit on an accept error (issue #504). The client
/// listener used to `return` on any error, which killed it for the life of the
/// process: the broker stayed up, kept answering `/metrics`, drained its
/// existing connections to zero and never accepted another client — observed on
/// v1.0.13 as `connections_total` frozen across two rungs while
/// `connections_active` fell 4,099 → 1,171 → 0. The health, cluster-bus and
/// admin listeners had the same `return` until it was found under a local
/// overload: one fd squeeze left `/metrics`, `/readyz` and `/livez` dead for good.
///
/// Every accept error is either per-connection or transient, so the loop always
/// continues; the only question is whether to pause first.
///
/// - `ConnectionAborted` and `Interrupted` are routine, not faults: the peer
///   vanished between its SYN and our `accept()`, or a signal landed. The next
///   accept is unaffected, so retry immediately — pausing here would add latency
///   for every other waiting client in response to a non-event.
/// - Everything else is treated as resource exhaustion — EMFILE/ENFILE (the
///   per-process and system fd limits) and ENOBUFS are what an overloaded broker
///   actually hits — and pauses, because retrying an exhausted fd table in a
///   tight loop empties nothing.
#[must_use]
pub fn accept_backoff(e: &std::io::Error) -> Duration {
    match e.kind() {
        std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::Interrupted => Duration::ZERO,
        _ => ACCEPT_ERROR_BACKOFF,
    }
}

/// Record an accept error on `listener` and wait out its [`accept_backoff`]; the caller
/// then accepts again. A routine error is logged at debug and not paused; anything else
/// is a warning naming the listener, then a pause.
pub async fn pause_after_error(e: &std::io::Error, listener: &'static str) {
    let pause = accept_backoff(e);
    if pause.is_zero() {
        debug!(error = %e, listener, "accept skipped a dead peer");
        return;
    }
    warn!(
        error = %e,
        listener,
        backoff_ms = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX),
        "listener accept failed; pausing, listener stays up"
    );
    tokio::time::sleep(pause).await;
}
