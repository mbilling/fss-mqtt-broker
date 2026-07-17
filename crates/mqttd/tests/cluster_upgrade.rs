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
//! motion **back down** (HEAD → baseline): pre-1.0 a rollback must work too,
//! and the reopen-across-versions in both directions is what fires the
//! ADR 0038 schema gates for real. Acked publishes flow through every phase
//! of both rolls — mixed-binary windows included — and every ack anywhere in
//! the story is a hard obligation at the end.
//!
//! The BASELINE is a **pinned ref** (`BASELINE_REF`), deliberately bumped —
//! never floated — so an incompatible reshape of wire or schema FAILS this
//! test until the baseline is consciously moved with the reshape (pre-1.0's
//! honest substitute for released-version skew; post-1.0 the baseline becomes
//! the previous release tag). The baseline binary is built from a git
//! worktree of that ref into a cached target dir, or supplied prebuilt via
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

/// The pinned baseline: the 0044-P2 merge — the newest ref carrying everything
/// the harness itself needs (`MQTTD_PEER_ADVERTISE`, the SWIM seed re-greeting
/// fix). Bump DELIBERATELY, together with any pre-1.0 wire/schema reshape.
const BASELINE_REF: &str = "20cae2c7aa6f31f8cb14fee1065affe375a14268";

/// The baseline `mqttd` binary: `MQTTD_BASELINE_BIN` if set (nightly / CI
/// supplies a prebuilt one), else built from [`BASELINE_REF`] via a git
/// worktree into a per-ref cached target dir (so repeat runs pay nothing).
fn baseline_binary() -> PathBuf {
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
    proc.nodes[i].terminate().await;
    for sub in &mut proc.subs {
        if sub.conn.is_some() && sub.via_node == i {
            sub.conn = None;
        }
    }
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
    wait_all_ready(&nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 2).await;

    // Roll UP: baseline → HEAD, one node at a time.
    for i in 0..3 {
        roll(&mut proc, i, &head, "HEAD").await;
    }
    proc.quiesce().await;
    proc.note("upgrade roll complete: every node on HEAD".into());

    // Roll BACK: HEAD → baseline, one node at a time (pre-1.0 a rollback is
    // the operator's escape hatch; the baseline binary must reopen dirs HEAD
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
