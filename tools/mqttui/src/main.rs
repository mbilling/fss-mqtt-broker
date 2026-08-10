//! `mqttui` — run this repository's demo, migration and test scripts from one place
//! ([ADR 0056](../../../docs/adr/0056-mqttui.md)).
//!
//! **T1: the manifest and a headless runner.** The terminal UI is T2. This is deliberately
//! first and useful without it — the manifest is machine-checked documentation of the
//! operational surface, and `--list` / `--run` are enough to be worth installing.
//!
//! ```text
//! mqttui --list                 every task, grouped
//! mqttui --list --all           including CI plumbing
//! mqttui --show deploy-smoke    what it does, what it needs, what it costs
//! mqttui --run deploy-smoke     run it, from the repository root
//! mqttui --check                the manifest covers every script in the tree
//! mqttui                        the terminal UI (T2)
//! mqttui migrate mosquitto …    convert a Mosquitto deployment, no Python needed
//! ```

mod embedded;
mod env;
mod manifest;
mod migrate;
mod preflight;
mod runner;
mod teardown;
mod ui;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use manifest::{Manifest, Task};

/// Where a clone comes from. Printed rather than assumed, because the people who hit these
/// messages are by definition running a binary with no repository next to it.
const REPO: &str = "https://github.com/mbilling/fss-mqtt-broker";

fn main() -> ExitCode {
    restore_sigpipe();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        usage();
        return ExitCode::SUCCESS;
    }

    // `migrate` reads the USER's mosquitto.conf, not anything of ours, so it works with no
    // checkout at all — which is the point of it being built in (ADR 0056 T10).
    if args.first().is_some_and(|a| a == "migrate") {
        let sub = args.get(1).map(String::as_str);
        if sub != Some("mosquitto") {
            eprintln!("mqttui: usage: mqttui migrate mosquitto <mosquitto.conf> [--out-config P] [--out-acl P]");
            return ExitCode::from(2);
        }
        return match migrate::run(&args[2..]) {
            Ok(report) => {
                print!("{report}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("mqttui: {e}");
                ExitCode::from(1)
            }
        };
    }

    // In a checkout, everything is available from disk. Standalone, the embedded examples
    // are unpacked and only what travelled can run (ADR 0056 T7) — and the difference is
    // stated, never hidden behind a shorter list.
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let source = match manifest::find_repo_root(&cwd) {
        Some(root) => embedded::Source::Checkout(root),
        None => match embedded::unpack() {
            Ok(root) => embedded::Source::Embedded(root),
            Err(e) => {
                eprintln!("mqttui: could not unpack the bundled examples: {e}");
                return ExitCode::from(2);
            }
        },
    };
    let root = source.root().to_path_buf();

    // The manifest ships with the binary, so it is the same list either way; what differs
    // is which of its tasks this machine can actually run.
    let loaded = match &source {
        // In a checkout the on-disk manifest is authoritative and every script it names
        // must exist — a manifest pointing at a missing script would fail at the worst
        // possible moment.
        embedded::Source::Checkout(_) => Manifest::load(&manifest_path(&root), &root),
        // Standalone, repo-only tasks are legitimately absent; they are reported as
        // unavailable, not treated as a broken manifest.
        embedded::Source::Embedded(_) => Manifest::parse(manifest::EMBEDDED),
    };
    let manifest = match loaded {
        Ok(m) => m,
        Err(e) => {
            eprintln!("mqttui: {e}");
            return ExitCode::from(2);
        }
    };

    if args.is_empty() {
        // The UI needs a terminal. Without one — a pipe, a CI step, an editor's output
        // pane — say so and point at the headless commands, rather than failing with
        // "Device not configured" from deep inside the terminal library.
        if !stdout_is_a_terminal() {
            eprintln!(
                "mqttui: the terminal UI needs a terminal (stdout is not a tty).\n\n\
                 Use the headless commands instead:\n  \
                   mqttui --list\n  \
                   mqttui --show <id>\n  \
                   mqttui --run <id>\n  \
                   mqttui --check"
            );
            return ExitCode::from(2);
        }
        return match ui::App::new(&manifest, root).run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("mqttui: {e}");
                ExitCode::from(1)
            }
        };
    }

    match args[0].as_str() {
        "--list" => {
            list(&manifest, args.iter().any(|a| a == "--all"), &source);
            ExitCode::SUCCESS
        }
        "--show" => match args.get(1).and_then(|id| manifest.get(id)) {
            Some(task) => {
                show(task, &root);
                ExitCode::SUCCESS
            }
            None => unknown_task(args.get(1), &manifest),
        },
        "--run" => match args.get(1).and_then(|id| manifest.get(id)) {
            Some(task) => run(task, &root, &source),
            None => unknown_task(args.get(1), &manifest),
        },
        "--check" => check_complete(&manifest, &root),
        other => {
            eprintln!("mqttui: unknown option '{other}'\n");
            usage();
            ExitCode::from(2)
        }
    }
}

/// Is the manifest still a complete picture of the tree?
///
/// A launcher that silently shows a subset becomes the list people trust (ADR 0056 §3), so
/// this is CI-gated. Exposed as a command as well as a test, because the person who needs
/// it most is whoever just added a script.
fn check_complete(manifest: &Manifest, root: &Path) -> ExitCode {
    let found = manifest::scripts_on_disk(root);
    let missing = manifest::missing_from_manifest(manifest, root);
    if missing.is_empty() {
        println!(
            "tasks.toml covers all {} scripts under scripts/, demo/ and bench/ ({} offered, {} internal).",
            found.len(),
            manifest.tasks.iter().filter(|t| !t.hidden).count(),
            manifest.tasks.iter().filter(|t| t.hidden).count(),
        );
        return ExitCode::SUCCESS;
    }
    eprintln!("tasks.toml is missing {} script(s):\n", missing.len());
    for m in &missing {
        eprintln!("  {m}");
    }
    eprintln!(
        "\nAdd each one. If it is CI plumbing nobody should run by hand, declare it with \
         `hidden = true` rather than leaving it out — the point is that the manifest knows \
         about everything."
    );
    ExitCode::from(1)
}

/// Restore the default `SIGPIPE` behaviour.
///
/// Rust ignores `SIGPIPE`, which turns `mqttui --list | head` — an entirely ordinary thing
/// to do — into a panic on a broken pipe instead of a quiet exit. Every other command-line
/// tool exits silently there, so this one does too.
#[cfg(unix)]
fn restore_sigpipe() {
    extern "C" {
        fn signal(sig: i32, handler: usize) -> usize;
    }
    const SIGPIPE: i32 = 13;
    const SIG_DFL: usize = 0;
    unsafe {
        signal(SIGPIPE, SIG_DFL);
    }
}

#[cfg(not(unix))]
fn restore_sigpipe() {}

/// Is stdout a terminal? The UI is unusable without one.
#[cfg(unix)]
fn stdout_is_a_terminal() -> bool {
    extern "C" {
        fn isatty(fd: i32) -> i32;
    }
    unsafe { isatty(1) == 1 }
}

#[cfg(not(unix))]
fn stdout_is_a_terminal() -> bool {
    true
}

fn manifest_path(root: &Path) -> PathBuf {
    root.join("tools").join("mqttui").join("tasks.toml")
}

fn usage() {
    println!(
        "mqttui — run the mqttd demo, migration and test scripts\n\n\
         USAGE\n  \
           mqttui --list [--all]        list tasks ( --all includes CI plumbing )\n  \
           mqttui --show <id>           what a task does, needs, and costs\n  \
           mqttui --run <id>            run it\n  \
           mqttui --check               the manifest covers every script in the tree\n  \
           mqttui                       the terminal UI\n  \
           mqttui migrate mosquitto <conf> [--out-config P] [--out-acl P]\n"
    );
}

fn unknown_task(id: Option<&String>, manifest: &Manifest) -> ExitCode {
    match id {
        Some(id) => {
            eprintln!("mqttui: no task with id '{id}'");
            // Anything sharing a prefix or substring is almost certainly what was meant.
            let near: Vec<&str> = manifest
                .tasks
                .iter()
                .map(|t| t.id.as_str())
                .filter(|c| c.contains(id.as_str()) || id.contains(*c))
                .collect();
            if !near.is_empty() {
                eprintln!("did you mean: {}", near.join(", "));
            }
            eprintln!("\n`mqttui --list` shows every task.");
        }
        None => eprintln!("mqttui: this option needs a task id — see `mqttui --list`"),
    }
    ExitCode::from(2)
}

fn list(manifest: &Manifest, all: bool, source: &embedded::Source) {
    let groups = manifest.visible_by_group();
    let mut hidden_count = 0;
    for (group, tasks) in groups {
        println!("\n{group}");
        for t in tasks {
            let mark = if !source.can_run(t) {
                "-"
            } else if preflight::missing_required(t).is_empty() {
                " "
            } else {
                "!"
            };
            println!("  {mark} {:<20} {}", t.id, t.name);
        }
    }
    if all {
        let internal: Vec<&Task> = manifest.tasks.iter().filter(|t| t.hidden).collect();
        if !internal.is_empty() {
            println!("\nInternal (CI plumbing — declared, not offered)");
            for t in internal {
                println!("    {:<20} {}", t.id, t.name);
            }
        }
    } else {
        hidden_count = manifest.tasks.iter().filter(|t| t.hidden).count();
    }

    println!();
    if hidden_count > 0 {
        println!("{hidden_count} CI-plumbing tasks hidden; --all shows them.");
    }
    println!(
        "`!` marks a task whose required tools are missing — `mqttui --show <id>` says which."
    );
    if matches!(source, embedded::Source::Embedded(_)) {
        // Said plainly, because a shorter list with no explanation is the silent-subset
        // failure the manifest exists to prevent — the user must know something is absent
        // and why, not merely not see it.
        let repo_only = manifest
            .visible()
            .filter(|t| source.availability(t) == embedded::Availability::NeedsCheckout)
            .count();
        let unbundled = manifest
            .visible()
            .filter(|t| source.availability(t) == embedded::Availability::NotBundled)
            .count();
        println!(
            "`-` marks a task this standalone binary cannot run: {repo_only} operate on the\n\
             repository itself, {unbundled} are not bundled. All of them run from a clone:\n  \
               git clone {REPO}"
        );
    }
}

fn show(task: &Task, root: &Path) {
    println!("{}  —  {}", task.id, task.name);
    println!("{}", task.script);
    if !task.duration.is_empty() {
        println!("takes {}", task.duration);
    }
    if !task.about.trim().is_empty() {
        println!("\n{}", task.about.trim());
    }
    if let Some(caution) = &task.caution {
        println!("\n!  {caution}");
    }

    let report = preflight::check(task);
    if !report.required.is_empty() {
        println!("\nRequires");
        for (tool, present) in &report.required {
            println!("   {} {tool}", if *present { '+' } else { '-' });
        }
    }
    if !report.optional.is_empty() {
        println!("\nOptional (absent = that part is skipped)");
        for (tool, present) in &report.optional {
            println!("   {} {tool}", if *present { '+' } else { '-' });
        }
    }
    if !task.env.is_empty() {
        println!("\nEnvironment");
        for e in &task.env {
            let shown = if e.default.is_empty() {
                "(unset)".to_string()
            } else {
                e.default.clone()
            };
            println!("   {:<16} {:<28} {}", e.name, shown, e.help);
        }
    }
    let _ = root;
}

/// Run a task from the repository root, inheriting stdio.
///
/// Inheriting rather than piping is deliberate for T1: the child keeps the terminal, so
/// its colours and progress work, and — because it stays in this process group — a
/// `Ctrl-C` reaches the whole group, which is what makes each script's own `trap EXIT`
/// run. The full teardown story, where the runner signals and then *verifies* what
/// survived, is T6.
fn run(task: &Task, root: &Path, source: &embedded::Source) -> ExitCode {
    match source.availability(task) {
        embedded::Availability::Yes => {}
        embedded::Availability::NeedsCheckout => {
            eprintln!(
                "mqttui: '{}' operates on the repository itself — it builds it, or diffs its\n\
                 rendered output — so no release of mqttui can carry it. Run it from a clone:\n  \
                   git clone {REPO} && cd fss-mqtt-broker && mqttui --run {}",
                task.id, task.id
            );
            return ExitCode::from(2);
        }
        embedded::Availability::NotBundled => {
            eprintln!(
                "mqttui: '{}' is not part of the bundled examples — its fixtures are far larger\n\
                 than the examples this binary carries. It runs from a clone:\n  \
                   git clone {REPO} && cd fss-mqtt-broker && mqttui --run {}",
                task.id, task.id
            );
            return ExitCode::from(2);
        }
    }
    let missing = preflight::missing_required(task);
    if !missing.is_empty() {
        eprintln!(
            "mqttui: cannot run '{}' — missing: {}\n\n\
             These are required, not optional. Install them and try again;\n\
             `mqttui --show {}` lists everything this task needs.",
            task.id,
            missing.join(", "),
            task.id
        );
        return ExitCode::from(2);
    }
    for (tool, present) in preflight::check(task).optional {
        if !present {
            eprintln!("mqttui: note — '{tool}' is absent; the parts needing it will be skipped");
        }
    }
    if let Some(caution) = &task.caution {
        eprintln!("mqttui: {caution}");
    }

    let script = root.join(&task.script);
    let is_python = Path::new(&task.script)
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("py"));
    let mut cmd = if is_python {
        let mut c = Command::new("python3");
        c.arg(&script);
        c
    } else {
        Command::new(&script)
    };
    // Headless, the user's own environment IS the set of overrides — then resolved by the
    // same function the UI uses, so what `--show` prints and what a run gets cannot drift.
    let overrides: std::collections::BTreeMap<String, String> = task
        .env
        .iter()
        .filter_map(|e| std::env::var(&e.name).ok().map(|v| (e.name.clone(), v)))
        .collect();
    cmd.args(env::args(task, &overrides));
    // Every script does `cd "$(dirname "$0")/.."`, so the repository root is the only
    // working directory they are written for.
    cmd.current_dir(root);
    for (k, v) in env::resolve(task, &overrides) {
        cmd.env(k, v);
    }

    eprintln!("mqttui: running {} ({})\n", task.id, task.script);
    match cmd.status() {
        Ok(status) => {
            let code = status.code().unwrap_or(1);
            eprintln!("\nmqttui: {} exited {code}", task.id);
            ExitCode::from(u8::try_from(code).unwrap_or(1))
        }
        Err(e) => {
            eprintln!("mqttui: could not start {}: {e}", task.script);
            ExitCode::from(2)
        }
    }
}
