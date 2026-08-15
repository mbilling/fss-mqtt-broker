//! The one sanctioned way for a test to stop early — and the reason it cannot pass in CI.
//!
//! A test that returns instead of asserting reports success. `cargo test` prints `ok`, the
//! summary counts it, and nothing anywhere says the coverage did not run. That is how a suite
//! quietly stops testing the thing it was written for (issue #260), and it is invisible in
//! exactly the place it matters most: a green check on the pull request that removed the
//! coverage.
//!
//! Skipping is still the right answer *locally*. `127.0.0.2` is not bindable on stock macOS
//! (issue #217); `cosign` is not on every laptop. It is never the right answer on the platform
//! that gates merges. So: allowed locally, fatal under `CI`. GitHub Actions sets `CI=true` on
//! every runner, so this needs no workflow wiring — the assertion is simply true on a laptop
//! and false in the job that would otherwise have merged the loss.
//!
//! This file is byte-identical in every workspace that needs it: mqttui is a separate
//! workspace with no lib target (ADR 0056 §1 keeps ratatui out of the broker's dependency
//! graph), so it cannot import the broker's copy. `scripts/check-test-hygiene.py` compares the
//! copies and fails on any drift.
//!
//! That gate's check B1 rejects a bare early `return` in a test function, with two exemptions it
//! names: a bounded poll's success exit (a loop whose exhaustion fails loudly) and a `return`
//! inside a task handed to `spawn`. Neither is an environmental skip, so for an environmental
//! skip this macro is the only way out — but "the only remaining way out of an early return" was
//! too strong a claim to leave here, and the gate's own limits are listed in docs/TEST-PLAN.md
//! § "What this gate detects, and what it cannot" rather than implied by silence. What is NOT checked
//! anywhere is this macro's behaviour: no test in the repository runs it with `CI=true` and
//! observes the failure. B3 checks that the guard is exactly the `CI` check, in code, in every
//! copy — a structural argument, not an executed one.

/// Skip the calling test — locally. Under `CI`, fail instead.
///
/// The reason must say what is missing **and** how to get it: it is the only thing a reader
/// has when a test they expected to run did not.
///
/// ```ignore
/// if std::net::TcpListener::bind(("127.0.0.2", 0)).is_err() {
///     skip_locally_or_fail_in_ci!(
///         "127.0.0.2 is not bindable here (stock macOS loopback carries only 127.0.0.1 — \
///          issue #217); `sudo ifconfig lo0 alias 127.0.0.2 up` enables it locally"
///     );
/// }
/// ```
#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! skip_locally_or_fail_in_ci {
    ($($reason:tt)+) => {{
        let reason: String = format!($($reason)+);
        assert!(
            std::env::var_os("CI").is_none(),
            "environmental self-skip taken under CI, so coverage that gates merges would have \
             silently vanished. Either provision what is missing in the workflow, or restructure \
             the test so it does not need it. Reason given: {reason}"
        );
        eprintln!("SKIPPED (local only; this is a hard failure under CI): {reason}");
        return;
    }};
}
