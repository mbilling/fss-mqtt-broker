//! `needs_checkout` is a hand-written claim about each script. This catches it going stale.
//!
//! The claim is easy to get wrong in the direction that hurts: a script gains a
//! `cargo build`, nobody updates the manifest, and a standalone user is offered a task that
//! dies with `could not find Cargo.toml`. That is exactly the failure this field was added
//! for, so leaving the field unguarded would just move the bug rather than fix it.
//!
//! The test is a grep, which is crude, but it is crude in the safe direction: for each task
//! the manifest says a standalone binary may run, it reads that script **and the scripts it
//! invokes**, and fails if any of them reaches for the repository. It cannot prove a script
//! is portable — only that the obvious ways of not being portable are absent.
//!
//! It took three attempts to make it able to fail at all, each verified by deleting a
//! `needs_checkout = true` and re-running. The first read only the entry point and passed;
//! `render-parity.sh` contains no `cargo`, it runs `render-parity-one.sh` twice. The second
//! followed invocations but resolved them literally, and `"$HERE/render-parity-one.sh"`
//! tokenises to a path that exists nowhere. A guard nobody has watched fail is a guard that
//! reports what you hoped for.

mod common;

use std::path::{Path, PathBuf};

/// Ways a script reaches for the repository it lives in. Not exhaustive by construction;
/// each is a thing that cannot work when the only files present are the unpacked examples.
const REPO_ONLY_SIGNALS: &[&str] = &["cargo build", "cargo run", "REPO_ROOT", "target/release"];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tools/mqttui sits two levels below the root")
        .to_path_buf()
}

/// Signals reachable from `script`, **following the scripts it invokes**.
///
/// Transitive because the first version of this test was not, and it let through the exact
/// case that motivated the whole field: `render-parity.sh` contains no `cargo` at all — it
/// runs `render-parity-one.sh` twice, and *that* is what needs the operator crate. A guard
/// that reads only the entry point calls such a task portable. Verified by deleting the
/// declaration and watching this fail.
fn signals_in(root: &Path, script: &Path) -> Vec<&'static str> {
    let mut found = Vec::new();
    let mut seen = Vec::new();
    walk(root, script, &mut found, &mut seen, 0);
    found.sort_unstable();
    found.dedup();
    found
}

fn walk(
    root: &Path,
    script: &Path,
    found: &mut Vec<&'static str>,
    seen: &mut Vec<PathBuf>,
    depth: u8,
) {
    // 4 is well past the deepest chain here (entry → helper); the bound only stops a cycle
    // of scripts that call each other from recursing forever.
    if depth > 4 || seen.contains(&script.to_path_buf()) {
        return;
    }
    seen.push(script.to_path_buf());
    let Ok(text) = std::fs::read_to_string(script) else {
        return;
    };
    found.extend(
        REPO_ONLY_SIGNALS
            .iter()
            .copied()
            .filter(|s| text.contains(s)),
    );

    for token in text.split(|c: char| !(c.is_alphanumeric() || "._/-".contains(c))) {
        let is_script = Path::new(token)
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("sh") || e.eq_ignore_ascii_case("py"));
        if !is_script {
            continue;
        }
        // Resolve the way the shell would: beside the script, or from the repository root.
        // The basename is tried too, because these paths are nearly always written through a
        // variable — `"$HERE/render-parity-one.sh"` tokenises as `HERE/render-parity-one.sh`,
        // which resolves to nothing, and skipping it was why the first transitive version of
        // this test still missed the case it was written for.
        let dir = script.parent();
        let base = Path::new(token).file_name().map(PathBuf::from);
        let candidates = [
            dir.map(|d| d.join(token)),
            Some(root.join(token)),
            base.as_ref().and_then(|b| dir.map(|d| d.join(b))),
        ];
        for candidate in candidates.into_iter().flatten() {
            if candidate.is_file() {
                walk(root, &candidate, found, seen, depth + 1);
                break;
            }
        }
    }
}

/// Every task the manifest offers to a standalone user must have a script that does not
/// reach for the repository.
#[test]
fn tasks_offered_standalone_do_not_reach_for_the_repository() {
    let root = repo_root();
    if !root.join("Cargo.toml").is_file() {
        crate::skip_locally_or_fail_in_ci!(
            "not in a checkout, so there are no scripts to read and the needs_checkout claims \
             went unchecked — in CI the checkout is always present, so this is fatal there"
        );
    }
    let manifest = read_manifest(&root);

    let mut wrong = Vec::new();
    let mut checked = 0;
    for task in manifest
        .tasks
        .iter()
        .filter(|t| !t.hidden && !t.needs_checkout)
    {
        let path = root.join(&task.script);
        // Only the bundled ones matter here: an unbundled task is already unavailable, and
        // whether it happens to invoke cargo is not what makes it so.
        if !is_bundled(&task.script) {
            continue;
        }
        checked += 1;
        let found = signals_in(&root, &path);
        if !found.is_empty() {
            wrong.push(format!("  {} ({}) — {:?}", task.id, task.script, found));
        }
    }

    assert!(
        wrong.is_empty(),
        "\nthese tasks are offered to standalone users but their scripts reach for the \
         repository:\n{}\n\n\
         A standalone run would fail with something like `could not find Cargo.toml`, which \
         is the failure `needs_checkout` exists to prevent. Either set \
         `needs_checkout = true` in tasks.toml, or make the script work without a \
         checkout.\n",
        wrong.join("\n")
    );
    // Down to 2 since demo-stack/demo-scale were declared checkout-only (the demo image is
    // built from repository source via `build: context: ..` — which this walk cannot see,
    // because it reads scripts, not compose files; the declaration is the authority).
    assert!(
        checked >= 2,
        "only {checked} bundled tasks were examined — if the embedded set shrank, this test \
         is passing because it checked almost nothing"
    );
}

/// The guard must be able to fail. If the signal list stopped matching anything, the test
/// above would pass for every manifest, including a wrong one — so assert the signals do
/// fire on the scripts that genuinely are repo-only.
#[test]
fn the_guard_is_not_vacuous() {
    let root = repo_root();
    if !root.join("Cargo.toml").is_file() {
        crate::skip_locally_or_fail_in_ci!(
            "not in a checkout, so the non-vacuity guard for the needs_checkout claims went \
             unchecked — which would make a vacuity check itself vacuous"
        );
    }
    let manifest = read_manifest(&root);

    let repo_only: Vec<_> = manifest.tasks.iter().filter(|t| t.needs_checkout).collect();
    assert!(!repo_only.is_empty(), "no task declares needs_checkout");

    let detected = repo_only
        .iter()
        .filter(|t| !signals_in(&root, &root.join(&t.script)).is_empty())
        .count();
    assert!(
        detected >= 3,
        "the signal list fired on only {detected} of {} declared repo-only scripts — it has \
         drifted away from how these scripts actually reach for the repo, so the guard above \
         proves nothing",
        repo_only.len()
    );
}

/// Whether a script travels inside the binary. Mirrors the `include_dir!` set in
/// `src/embedded.rs`; a test-local copy so a change there shows up as a failure here rather
/// than being silently followed.
fn is_bundled(script: &str) -> bool {
    ["demo/", "deploy/", "scripts/migrate/", "scripts/k8s/"]
        .iter()
        .any(|p| script.starts_with(p))
}

// ── a minimal manifest reader ────────────────────────────────────────────────────────
// The binary's own parser is not reachable from an integration test, and the fields needed
// here are three.

struct Task {
    id: String,
    script: String,
    hidden: bool,
    needs_checkout: bool,
}
struct Manifest {
    tasks: Vec<Task>,
}

fn read_manifest(root: &Path) -> Manifest {
    let text = std::fs::read_to_string(root.join("tools/mqttui/tasks.toml")).expect("tasks.toml");
    let value: toml::Value = text.parse().expect("tasks.toml parses");
    let tasks = value["task"]
        .as_array()
        .expect("[[task]] array")
        .iter()
        .map(|t| Task {
            id: t["id"].as_str().expect("id").to_string(),
            script: t["script"].as_str().expect("script").to_string(),
            hidden: t
                .get("hidden")
                .and_then(toml::Value::as_bool)
                .unwrap_or(false),
            needs_checkout: t
                .get("needs_checkout")
                .and_then(toml::Value::as_bool)
                .unwrap_or(false),
        })
        .collect();
    Manifest { tasks }
}
