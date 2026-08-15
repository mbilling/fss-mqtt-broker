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
mod update;

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

    // `update` manages the examples themselves, so it runs before source detection — it
    // must work from anywhere, including a machine that has never unpacked anything.
    if args.first().is_some_and(|a| a == "update") {
        return match update::run(&args[1..]) {
            Ok(report) => {
                println!("{report}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("mqttui: {e}");
                ExitCode::from(1)
            }
        };
    }

    let (source, manifest) = match resolve() {
        Ok(pair) => pair,
        Err(code) => return code,
    };
    let root = source.root().to_path_buf();

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
        return match ui::App::new(&manifest, root, source.clone()).run() {
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

/// Where the examples come from, and the task list that goes with them.
///
/// In a checkout, everything is available from disk and the on-disk manifest is
/// authoritative — a manifest naming a missing script must fail here, not mid-run.
/// Standalone: an installed `mqttui update` bundle takes precedence and carries its OWN
/// manifest, so tasks declared after this binary shipped still appear (serde ignores
/// fields this binary predates); else the embedded examples are unpacked and the embedded
/// manifest is the list. In every case the difference is stated, never hidden behind a
/// shorter list (ADR 0056 T7/T8).
fn resolve() -> Result<(embedded::Source, Manifest), ExitCode> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let source = match manifest::find_repo_root(&cwd) {
        Some(root) => embedded::Source::Checkout(root),
        None => match update::installed_dir() {
            Ok(dir) if dir.join("tasks.toml").is_file() => embedded::Source::Updated(dir),
            _ => match embedded::unpack() {
                Ok(root) => embedded::Source::Embedded(root),
                Err(e) => {
                    eprintln!("mqttui: could not unpack the bundled examples: {e}");
                    return Err(ExitCode::from(2));
                }
            },
        },
    };
    let root = source.root();
    let loaded = match &source {
        embedded::Source::Checkout(_) => Manifest::load(&manifest_path(root), root),
        embedded::Source::Embedded(_) => Manifest::parse(manifest::EMBEDDED),
        // If the update's manifest is broken, say so and name the way out — silently
        // falling back to the embedded manifest would run updated scripts against a stale
        // task list.
        embedded::Source::Updated(dir) => match std::fs::read_to_string(dir.join("tasks.toml")) {
            Ok(text) => Manifest::parse(&text),
            Err(e) => {
                eprintln!(
                    "mqttui: the installed update's manifest is unreadable ({e}).\n\
                     `mqttui update --clear` returns to the embedded examples."
                );
                return Err(ExitCode::from(2));
            }
        },
    };
    match loaded {
        Ok(m) => Ok((source, m)),
        Err(e) => {
            eprintln!("mqttui: {e}");
            Err(ExitCode::from(2))
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
           mqttui migrate mosquitto <conf> [--out-config P] [--out-acl P]\n  \
           mqttui update                fetch the latest examples as a SIGNED bundle\n  \
           mqttui update --clear        back to the examples this binary shipped with\n  \
           mqttui update --channel main raw main branch — UNVERIFIED, maintainers only\n\n\
         MIGRATE PROVENANCE\n  \
           `migrate mosquitto` is a byte-for-byte port of \
         scripts/migrate/from-mosquitto.py.\n  \
           Its mappings are written against mosquitto.conf(5) from \
         eclipse-mosquitto/mosquitto\n  \
           @ v2.0.22; NO vendor config file is pinned as a fixture for it and no live\n  \
           Mosquitto broker has ever been converted by it. The version RANGE in\n  \
           docs/MIGRATION.md's What-ships table is a PARSER claim and nothing more.\n\n  \
           WHAT IT PRODUCES: a reviewed DRAFT, not `your config, translated`. Anything it\n  \
           could not DERIVE from your input is emitted INERT — commented out, beside a\n  \
           TODO naming the decision you have to make — so an unread construct can leave\n  \
           the output INCOMPLETE but can never leave a live security setting nobody\n  \
           derived. Every live security-relevant line (`*_bind`, `[tls]` paths,\n  \
           client_ca, crl, acl_file, allow_anonymous, the ACL `default`) carries\n  \
           `# from: <the input key it came from>`; --provenance-json writes the same\n  \
           ledger as JSON. Mosquitto scopes password_file, acl_file, psk_file,\n  \
           allow_anonymous, allow_zero_length_clientid, auto_id_prefix, plugin and\n  \
           plugin_opt_* PER LISTENER when per_listener_settings is true (mosquitto.conf(5)\n  \
           @ v2.0.22 names those eight) and mqttd has no per-listener security at all —\n  \
           that collapse is reported, not taken silently. An include_dir is NOT followed\n  \
           and a plugin's own config file is NOT opened: their contents are never read.\n\n  \
           VERIFIED, for THIS converter: the provenance, no-live-without-source, drop,\n  \
           contradiction and validity invariants of scripts/migrate/property_sweep.py\n  \
           over generated and mechanically mutated inputs; `mqttd --check-config` on\n  \
           every generated config plus the ACL loaded by the real broker; and this port\n  \
           compared BYTE FOR BYTE with the Python original. NOT diffed against vendor\n  \
           bytes: there is NO pinned Mosquitto fixture at all (the EMQX and HiveMQ\n  \
           converters do have vendor fixtures with re-derivable SHA-256s; this one does\n  \
           not), so every mapping rests on mosquitto.conf(5) alone. NOT VERIFIED: no\n  \
           live Mosquitto was ever run, and NO claim of total coverage over\n  \
           mosquitto.conf(5) is made — a construct it has never seen is one it cannot\n  \
           report, and a construct whose MEANING it misreads is one it can still\n  \
           translate wrongly (docs/MIGRATION.md's KNOWN GAPS lists the misreadings\n  \
           found so far).\n"
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
    // Where the examples come from, FIRST — an installed update changes what every line
    // below means, and an unverified one must be impossible to scroll past: a warning
    // printed once at install time is a warning forgotten, so it repeats here every run.
    if let embedded::Source::Updated(dir) = source {
        println!(
            "examples: installed update — {}",
            update::provenance(dir).unwrap_or_else(|| "provenance file missing".into())
        );
    }
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
    if matches!(
        source,
        embedded::Source::Embedded(_) | embedded::Source::Updated(_)
    ) {
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
