//! The vanishing a runtime check can never see (issue #260).
//!
//! `skip_locally_or_fail_in_ci!` makes a *runtime* skip fatal under CI. It cannot reach a
//! suite that was never compiled. A `#![cfg(…)]` at the top of a test file excludes the whole
//! file off-platform: the binary is built with **zero** tests in it, `cargo test` prints
//! `0 passed`, and the job is green. Nothing distinguishes that from a suite that ran.
//!
//! An assertion inside the gated file cannot help, because the same `cfg` excludes it. So the
//! predicates are mirrored HERE, in a file that always compiles, and
//! `scripts/check-test-hygiene.py` (check B4) fails if a `#![cfg(…)]` appears under `tests/`
//! without a matching predicate below. Locally these pass — running the macOS-irrelevant
//! Linux suites on a Mac is not the goal. Under `CI` they are hard requirements, because CI
//! is the run whose green check merges the change.
//!
//! Adding a platform-gated suite therefore costs one line here, and forgetting it fails the
//! build rather than silently shrinking the suite.

mod common;

/// This file's own source, for the wiring check in `the_skip_macro_helper_do_not_run_directly`.
const SELF_SRC: &str = include_str!("platform_coverage.rs");

/// Every `#![cfg]`-gated suite compiled on this runner — or CI says which one did not.
#[test]
fn every_platform_gated_suite_compiled_on_this_runner() {
    let in_ci = std::env::var_os("CI").is_some();

    // crates/mqttd/tests/memory_watermark.rs — #![cfg(target_os = "linux")]
    assert!(
        !in_ci || cfg!(target_os = "linux"),
        "memory_watermark.rs is #![cfg(target_os = \"linux\")] and this CI runner is not \
         Linux, so the suite compiled to nothing and the run reported success. ADR 0041 T8 \
         (memory watermark -> brownout, end to end) is UNTESTED on this run. Either restore a \
         Linux runner for the `test` job or port the suite."
    );

    // crates/mqttd/tests/decommission.rs — #![cfg(unix)]
    assert!(
        !in_ci || cfg!(unix),
        "decommission.rs is #![cfg(unix)] and this CI runner is not unix, so the suite \
         compiled to nothing and the run reported success. ADR 0047 T4 (`--decommission`, the \
         Kubernetes preStop primitive) is UNTESTED on this run."
    );
}

/// The guard above must be able to fail, and the shape it guards must still exist.
///
/// If both suites lost their `#![cfg]` gates, the assertions would pass forever while
/// checking nothing — so read the files and require the predicates to be there. This is the
/// same non-vacuity move `check-test-hygiene.py` makes from outside; having it here too means
/// a green `cargo test` alone tells you the mirror is real.
#[test]
fn the_mirrored_predicates_still_describe_the_suites() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    for (file, pred) in [
        ("memory_watermark.rs", "#![cfg(target_os = \"linux\")]"),
        ("decommission.rs", "#![cfg(unix)]"),
    ] {
        let src = std::fs::read_to_string(dir.join(file))
            .unwrap_or_else(|e| panic!("read tests/{file}: {e}"));
        assert!(
            src.contains(pred),
            "tests/{file} no longer carries `{pred}`. If the gate is gone, delete its \
             assertion above — leaving it makes this file claim coverage it no longer \
             mirrors. If the gate CHANGED, update both."
        );
    }
}

/// THE MACRO'S BEHAVIOUR, EXECUTED — not argued from its text (issue #260 round 3).
///
/// `check-test-hygiene.py`'s check B3 proves structurally that the guard's condition is
/// exactly the `CI` check and that it is not nested inside a conditional. Both are textual,
/// and two independent reviewers defeated earlier textual versions while leaving the text
/// byte-identical: first with an always-true disjunct, then by wrapping the assertion in
/// `if false { … }`. The lesson of this whole issue applies to its own gate — an executed
/// check beats a read one — so this test RUNS the macro with `CI` set and observes the panic.
///
/// It runs the macro in a subprocess (the harness would count a panic here as a failure), via
/// this same test binary with a filter that matches only the helper below.
#[test]
fn the_skip_macro_is_fatal_under_ci() {
    // The helper is invoked as a child; when run normally it is a no-op, so it costs the
    // suite nothing.
    let exe = std::env::current_exe().expect("this test binary's path");
    let out = std::process::Command::new(&exe)
        .args([
            "the_skip_macro_helper_do_not_run_directly",
            "--exact",
            "--nocapture",
        ])
        .env("MQTTD_SKIP_MACRO_CHILD", "1")
        .env("CI", "true")
        .output()
        .expect("re-run this test binary as a child");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "the macro took a skip under CI=true and the child SUCCEEDED — every environmental \
         skip in this tree is silent on the platform that gates merges (issue #260 reopened). \
         Child output:\n{text}"
    );
    assert!(
        text.contains("environmental self-skip taken under CI"),
        "the child failed, but not with the macro's own message — so this test is not \
         observing what it claims to. Child output:\n{text}"
    );
    // …and locally (no CI in the environment) the same helper must SKIP rather than fail,
    // because a macro that failed everywhere would just delete the local affordance.
    let out_local = std::process::Command::new(&exe)
        .args([
            "the_skip_macro_helper_do_not_run_directly",
            "--exact",
            "--nocapture",
        ])
        .env("MQTTD_SKIP_MACRO_CHILD", "1")
        .env_remove("CI")
        .env_remove("GITHUB_ACTIONS")
        .output()
        .expect("re-run this test binary as a child");
    assert!(
        out_local.status.success(),
        "with CI unset the macro must skip, not fail — otherwise it is not a local \
         affordance at all. Child output:\n{}{}",
        String::from_utf8_lossy(&out_local.stdout),
        String::from_utf8_lossy(&out_local.stderr)
    );
}

/// The child half of [`the_skip_macro_is_fatal_under_ci`].
///
/// Driven (the parent sets `MQTTD_SKIP_MACRO_CHILD`) it takes the skip path, which is the
/// behaviour the parent observes. Undriven it asserts the pair is still WIRED — that the
/// parent still invokes this helper by name. That is deliberately not a no-op: an early
/// `return` here would be the very shape this whole issue exists to eliminate, and the gate
/// rejected it when it was written that way. The orphan risk is real, because a rename or a
/// deletion in the parent would otherwise leave this helper passing forever while proving
/// nothing.
#[test]
fn the_skip_macro_helper_do_not_run_directly() {
    if std::env::var_os("MQTTD_SKIP_MACRO_CHILD").is_some() {
        crate::skip_locally_or_fail_in_ci!(
            "the_skip_macro_is_fatal_under_ci drives this deliberately; nothing is missing"
        );
    }
    // `include_str!` rather than reading `file!()`: the path in `file!()` is relative to the
    // workspace root while a test's CWD is its crate directory, and a wiring check that can
    // fail on a path lookup is a flake wearing an assertion's clothes.
    // Look for the helper's NAME inside the parent's body, not for a multi-token snippet:
    // the first version of this check spelled out `"…helper…", "--exact"` and rustfmt promptly
    // reflowed the argument list across lines, breaking it. A wiring check that formatting can
    // break is the same spelling-dependence this issue spent three rounds removing from the
    // gate.
    // Anchored on the DEFINITION (newline + `fn` + open paren), which the search key itself
    // cannot look like. The first version searched for the parent's bare name and found it in
    // THIS line — the check matched its own source and then read its own assertion text as
    // evidence, so it passed with the parent's invocations renamed away. Self-matching is the
    // same defect class as a gate that finds its own pattern in a comment.
    let parent = SELF_SRC
        .split_once("\nfn the_skip_macro_is_fatal_under_ci(")
        .map_or("", |(_, rest)| rest.split("\n}").next().unwrap_or(""));
    assert!(
        parent.contains("the_skip_macro_helper_do_not_run_directly"),
        "nothing drives this helper any more: `the_skip_macro_is_fatal_under_ci` no longer \
         invokes it by name, so the macro's CI-fatality is proved by no executed test and \
         only by check B3's structural argument. Re-wire the pair or delete both."
    );
}
