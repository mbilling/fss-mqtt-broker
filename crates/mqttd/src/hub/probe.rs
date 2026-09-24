//! Test-only instrumentation counters (issue #613).
//!
//! These exist so the waves' claims can be PROVEN rather than benchmarked. Every
//! item in waves A and B is of the form "work X stops happening per publish / per
//! tick", and the honest test of that is a COUNT of how many times X ran — which
//! is deterministic, runs in CI, and fails loudly if a later change puts the work
//! back. A latency assertion would be a wall-clock assertion, which is exactly
//! what a CI gate must not depend on.
//!
//! They are also what stands in for the counting global allocator the test survey
//! wanted (issue #613 CORRECTION 5): the workspace sets `unsafe_code = "forbid"`
//! and `mqttd` opts in, `forbid` cannot be lifted by an inner `#[allow]`, and
//! `impl GlobalAlloc` needs `unsafe impl`. So items 1.4 and 1.5 are proven by
//! counting the WORK, not the bytes — and, as the surviving design says, by
//! asserting a SLOPE in peer count rather than any absolute number.
//!
//! `#[cfg(test)]`, so this compiles only for the crate's own unit tests. The
//! integration tests under `crates/mqttd/tests/` link the library built WITHOUT
//! `cfg(test)` and therefore cannot see any of it — Zone TESTS must prove its
//! claims through observable behaviour or metrics instead.

use std::sync::atomic::AtomicUsize;

/// One hub's work counters. Every field is `AtomicUsize` because the hub hands
/// `Arc<HubProbe>` to a test that keeps reading it after `run()` has consumed
/// the hub by value.
#[derive(Debug, Default)]
pub(super) struct HubProbe {
    /// How many times [`Hub::peers_all`](super::Hub::peers_all) walked the
    /// member set (item 1.1: this must stop scaling with publish count).
    pub(super) peers_all_evals: AtomicUsize,
    /// How many inherited-session scans were STARTED — counted where the scan is
    /// actually spawned, so a suppressed duplicate is not counted (items 2.2 and
    /// 2.3).
    pub(super) scans_started: AtomicUsize,
    /// How many times shared planning took a fresh scratch collection for its
    /// `decided` set (item 1.5). Same slope framing: the count per publish must
    /// stop growing with the number of matching groups.
    pub(super) shared_plan_scratch_uses: AtomicUsize,
}

#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258).
use super::*;

impl Hub {
    /// A handle onto this hub's counters, taken BEFORE `run()` consumes the hub.
    pub(super) fn probe(&self) -> std::sync::Arc<HubProbe> {
        self.probe.clone()
    }
}
