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
//! ```

mod manifest;
mod preflight;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use manifest::{Manifest, Task};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "-h" || a == "--help") {
        usage();
        return ExitCode::SUCCESS;
    }

    // T7 will embed the examples so this works outside a checkout. Until then, say what is
    // wrong and how to fix it rather than failing obscurely.
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let Some(root) = manifest::find_repo_root(&cwd) else {
        eprintln!(
            "mqttui: not inside a checkout of fss-mqtt-broker.\n\n\
             This build runs the repository's scripts, so it needs the repository:\n  \
             git clone https://github.com/mbilling/fss-mqtt-broker\n\n\
             (A standalone build that carries the demo and Kubernetes examples with it is \
             ADR 0056 T7.)"
        );
        return ExitCode::from(2);
    };

    let manifest_path = manifest_path(&root);
    let manifest = match Manifest::load(&manifest_path, &root) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("mqttui: {e}");
            return ExitCode::from(2);
        }
    };

    match args[0].as_str() {
        "--list" => {
            list(&manifest, args.iter().any(|a| a == "--all"));
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
            Some(task) => run(task, &root),
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
           mqttui --check               the manifest covers every script in the tree\n\n\
         The terminal UI is ADR 0056 T2; this build is the manifest and the runner."
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

fn list(manifest: &Manifest, all: bool) {
    let groups = manifest.visible_by_group();
    let mut hidden_count = 0;
    for (group, tasks) in groups {
        println!("\n{group}");
        for t in tasks {
            let missing = preflight::missing_required(t);
            let mark = if missing.is_empty() { " " } else { "!" };
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
fn run(task: &Task, root: &Path) -> ExitCode {
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
    // Every script does `cd "$(dirname "$0")/.."`, so the repository root is the only
    // working directory they are written for.
    cmd.current_dir(root);
    for e in &task.env {
        if !e.default.is_empty() && std::env::var_os(&e.name).is_none() {
            cmd.env(&e.name, &e.default);
        }
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
