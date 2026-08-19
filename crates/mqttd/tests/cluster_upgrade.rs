//! The **two-binary rolling upgrade** over real spawned processes
//! ([ADR 0044](../../docs/adr/0044-release-readiness-assurance.md) P3;
//! closes the [ADR 0043](../../docs/adr/0043-elastic-cluster-resize.md)
//! recorded gap; builds the machinery [ADR 0039](../../docs/adr/0039-versioning-and-upgrade-policy.md)
//! T3's CI skew test rides once two releases exist).
//!
//! A cluster of BASELINE-version nodes serves acked durable load while every
//! node is rolled to HEAD **one node at a time** — the operator's motion:
//! `SIGTERM` (the ADR 0019 graceful stop), swap the binary, restart over the
//! SAME data dir, wait for `/readyz` re-admission, next node. Then the same
//! motion **back down** (HEAD → baseline): ADR 0058 clause 2 promises the roll
//! in both directions, and the reopen-across-versions is what fires the
//! ADR 0038 schema gates for real. Acked publishes flow through every phase
//! of both rolls — mixed-binary windows included — and every ack anywhere in
//! the story is a hard obligation at the end.
//!
//! The BASELINE is a **pinned ref** (`BASELINE_REF`) — since the 1.0 freeze,
//! the previous release's commit, advancing only with each release cut along
//! ADR 0039's skew policy (previous minor; gateway minor across majors). An
//! incompatible reshape of wire or schema FAILS this test, and post-freeze the
//! repair is a migration or a new negotiated frame — never moving the baseline
//! past the break. The baseline binary is built from a git worktree of that
//! ref into a cached target dir, or supplied prebuilt via
//! `MQTTD_BASELINE_BIN` (the nightly tier's path).
//!
//! `#[ignore]` in the per-PR profile: building a second binary costs minutes.
//! The nightly tier (0044-P4) runs it with `--ignored`.

mod common;
mod proc_common;

use std::path::PathBuf;
use std::time::Duration;

use proc_common::{
    build_topology, establish_subscribers, oracle_acked_facts, proc_over, wait_all_ready,
};

/// The pinned baseline: the **`v1.0.0` release commit** — the previous release,
/// as ADR 0039's skew policy demands from 1.0 on (0058-T3/T5). The roll this
/// test proves is exactly the one an operator performs: previous release ↔ the
/// 1.0 line, both directions, under acked load. From here the ref advances only
/// with each release cut (RELEASING.md): the previous minor within a major, the
/// previous major's gateway minor across a boundary — never bumped to absorb a
/// reshape, because the reshape-in-place motion closed at the freeze.
/// Previous baseline: the `v0.9.1` release commit (`0f7042c2…`), which proved
/// the roll INTO the 1.0 line before the freeze tag was cut.
/// Pre-1.0 history (kept as the process scar it is): the baseline was a
/// hand-bumped commit pin — last `c6b84f23…` (the issue #227/#232 retained-expiry
/// reshape, which landed WITHOUT its bump and broke the next oracle run), before
/// that `e39b6e13…` (the issue #92 SWIM generation reshape).
const BASELINE_REF: &str = "101554fcabae36cd271570ece037cd7f9764f296";

/// The baseline `mqttd` binary: `MQTTD_BASELINE_BIN` if set (nightly / CI
/// supplies a prebuilt one), else built from [`BASELINE_REF`] via a git
/// worktree into a per-ref cached target dir (so repeat runs pay nothing).
///
/// Serialized: two tests in this binary need the baseline, and the test
/// harness runs them on parallel threads — unguarded, both raced into
/// `git worktree add` and each corrupted the other's checkout.
fn baseline_binary() -> PathBuf {
    static BASELINE_BUILD: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _serialized = BASELINE_BUILD.lock().unwrap();
    if let Ok(p) = std::env::var("MQTTD_BASELINE_BIN") {
        return PathBuf::from(p);
    }
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf();
    let short = &BASELINE_REF[..12];
    let target = repo_root.join("target").join(format!("baseline-{short}"));
    let bin = target.join("debug").join("mqttd");
    if bin.exists() {
        return bin;
    }
    let worktree = target.join("src");
    let run = |args: &[&str], cwd: &std::path::Path| {
        let out = std::process::Command::new(args[0])
            .args(&args[1..])
            .current_dir(cwd)
            .output()
            .unwrap_or_else(|e| panic!("spawn {args:?}: {e}"));
        assert!(
            out.status.success(),
            "{args:?} failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    if !worktree.exists() {
        run(
            &[
                "git",
                "worktree",
                "add",
                "--detach",
                worktree.to_str().unwrap(),
                BASELINE_REF,
            ],
            &repo_root,
        );
    }
    // Build the baseline as it was: its own sources and lockfile, its own
    // target dir (never contaminating the primary build cache).
    let out = std::process::Command::new("cargo")
        .args(["build", "-p", "mqttd", "--bin", "mqttd"])
        .env("CARGO_TARGET_DIR", &target)
        .current_dir(&worktree)
        .output()
        .expect("spawn cargo build for baseline");
    assert!(
        out.status.success(),
        "baseline build failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(bin.exists(), "baseline build produced no binary");
    bin
}

/// One node's roll (ADR 0044 P3): graceful stop (SIGTERM, the operator's
/// motion), swap the binary, restart over the SAME data dir, wait for
/// re-admission. Acked load flows before and after each swap — the
/// mixed-binary window is exactly where upgrade bugs live.
async fn roll(proc: &mut proc_common::Proc, i: usize, to: &std::path::Path, label: &str) {
    proc.publish_step().await;
    let rolled_at = std::time::Instant::now();
    proc.nodes[i].terminate().await;
    let stop_secs = rolled_at.elapsed().as_secs_f64();
    // The roll's client cost, measured (issue #248): only the subscribers whose
    // connection went THROUGH the rolled node lose it; everyone else's live
    // connection is untouched by the roll.
    let mut dropped = 0usize;
    for sub in &mut proc.subs {
        if sub.conn.is_some() && sub.via_node == i {
            sub.conn = None;
            dropped += 1;
        }
    }
    let kept = proc.subs.iter().filter(|s| s.conn.is_some()).count();
    proc.nodes[i].binary = to.to_path_buf();
    // Rejoin via the whole topology (the restarted-founder rule).
    proc.nodes[i].swim_seeds = proc
        .nodes
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != i)
        .map(|(_, n)| n.swim_bind.clone())
        .collect::<Vec<_>>()
        .join(",");
    proc.nodes[i].spawn();
    let id = proc.nodes[i].id.clone();
    proc.note(format!("ROLLED {id} to {label}"));
    proc.publish_step().await;
    if !proc.wait_node_serving(i, Duration::from_secs(30)).await && proc.nodes[i].died() {
        proc.nodes[i].spawn(); // lost the port-rebind race: once more
    }
    assert!(
        proc.wait_node_serving(i, Duration::from_secs(60)).await,
        "rolled node {id} never re-admitted ({label})"
    );
    // The per-roll numbers OPERATIONS.md cites (issue #248): graceful-stop
    // time, stop-to-readmission time, and how many subscribers lost their
    // connection (vs how many rode through untouched).
    eprintln!(
        "roll_cost({label}): {id}: stop={stop_secs:.1}s, \
         stop-to-readmission={:.1}s, subscribers dropped={dropped}, \
         untouched live connections={kept}",
        rolled_at.elapsed().as_secs_f64(),
    );
}

/// The rolling upgrade and rollback (ADR 0044 P3): baseline cluster → HEAD one
/// node at a time under acked load, then HEAD → baseline the same way. Every
/// phase's acks are hard obligations; the oracle runs after both rolls.
// One linear story — baseline bring-up, roll up, roll down, oracle — like the
// other schedules; splitting it would scatter the acked facts from the checks.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "builds a second binary (minutes); run explicitly or in the nightly tier (0044-P4)"]
async fn a_rolling_upgrade_and_rollback_lose_no_acked_fact() {
    let seed = 3939;
    let baseline = baseline_binary();
    let head = PathBuf::from(env!("CARGO_BIN_EXE_mqttd"));

    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.binary.clone_from(&baseline);
        n.spawn();
    }
    wait_all_ready(&mut nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 2).await;

    // Roll UP: baseline → HEAD, one node at a time.
    for i in 0..3 {
        roll(&mut proc, i, &head, "HEAD").await;
    }
    proc.quiesce().await;
    proc.note("upgrade roll complete: every node on HEAD".into());

    // Roll BACK: HEAD → baseline, one node at a time (ADR 0058 clause 2: the
    // roll holds in BOTH directions; the baseline binary must reopen dirs HEAD
    // wrote — the ADR 0038 schema gates fire here if a reshape forgot the
    // baseline bump).
    for i in 0..3 {
        roll(&mut proc, i, &baseline, "baseline").await;
    }
    proc.quiesce().await;
    proc.note("rollback roll complete: every node on baseline".into());

    // The oracle: every ack from every phase — baseline-only, mixed windows,
    // HEAD-only, and back — replays to the resumed subscribers.
    oracle_acked_facts(&mut proc).await;
    let count = |needle: &str| proc.trace.iter().filter(|l| l.contains(needle)).count();
    eprintln!(
        "cluster_upgrade: seed {seed}: 6 rolls (3 up, 3 back), {} publishes ({} owed)",
        count("publish #"),
        count("ACKED (obligation)"),
    );
    for node in &mut proc.nodes {
        node.kill().await;
    }
}

/// The config half of the 0039-T3 skew story (ADR 0058 §E residual): an operator
/// rolls a node BACK, and the rolled-back binary reads the config the newer
/// release rendered — containing a key it has never heard of. Under the default
/// posture that boot refuses loudly, naming the key; under the documented hatch
/// (`MQTTD_CONFIG_UNKNOWN_KEYS=warn`, issue #230) it validates and serves. Run
/// against the REAL previous release ([`BASELINE_REF`]) via `--check-config`, so
/// the smoke needs no ports and no cluster — just the released binary's parser
/// meeting the future's file.
#[test]
#[ignore = "builds a second binary from BASELINE_REF; the nightly tier runs it (0044-P4)"]
fn a_rolled_back_binary_reads_a_newer_config_under_warn() {
    let baseline = baseline_binary();
    let path = std::env::temp_dir().join(format!("skew-config-{}.toml", std::process::id()));
    // A config a NEWER minor would render: everything the baseline knows, plus a
    // key it does not. Unknown-KEY tolerance is the promise; a changed value
    // *type* is deliberately outside it (ADR 0058 §E names that residual).
    std::fs::write(
        &path,
        "[node]\nid = \"skew-smoke\"\n[durable]\nenabled = false\nknob_from_the_future = true\n",
    )
    .unwrap();

    let run = |warn: bool| {
        let mut c = std::process::Command::new(&baseline);
        for (k, _) in std::env::vars() {
            if k.starts_with("MQTTD_") {
                c.env_remove(k);
            }
        }
        if warn {
            c.env("MQTTD_CONFIG_UNKNOWN_KEYS", "warn");
        }
        c.arg("--check-config")
            .arg("--config")
            .arg(&path)
            .output()
            .unwrap()
    };

    let refused = run(false);
    assert_eq!(
        refused.status.code(),
        Some(1),
        "the previous release must refuse an unknown key by default; stderr={}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("knob_from_the_future"),
        "the refusal must name the key; stderr={}",
        String::from_utf8_lossy(&refused.stderr)
    );

    let warned = run(true);
    assert!(
        warned.status.success(),
        "under warn the previous release must validate the newer config; stderr={}",
        String::from_utf8_lossy(&warned.stderr)
    );
    let _ = std::fs::remove_file(&path);
}
