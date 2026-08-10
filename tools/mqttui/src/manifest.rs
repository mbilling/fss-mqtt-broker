//! The task manifest: `tasks.toml` parsed, validated, and answered questions about.
//!
//! Tasks are **declared, not discovered** (ADR 0056 §2). A directory walk cannot know a
//! description, cannot tell a user-facing script from CI plumbing, and cannot know what a
//! script needs *before* it runs. The cost of declaring is that the file can fall behind
//! the tree — which is why `completeness` exists and why CI runs it.

use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// One tuneable environment variable.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvVar {
    /// The variable name, e.g. `MQTTD_BIN`.
    pub name: String,
    /// Value used when the user does not override it. Empty means "leave unset".
    #[serde(default)]
    pub default: String,
    /// One line explaining what it changes.
    pub help: String,
}

/// One runnable task.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    /// Stable handle for `mqttui --run <id>`. Never renamed once published.
    pub id: String,
    /// Heading it appears under.
    pub group: String,
    /// One imperative line.
    pub name: String,
    /// Path from the repository root.
    pub script: String,
    /// A paragraph: what it proves or produces, and anything surprising.
    #[serde(default)]
    pub about: String,
    /// Tools that must be present; a missing one blocks the run.
    #[serde(default)]
    pub requires: Vec<String>,
    /// Tools whose absence degrades the run rather than preventing it.
    #[serde(default)]
    pub optional: Vec<String>,
    /// Rough wall-clock, so nobody starts a 15-minute job by accident.
    #[serde(default)]
    pub duration: String,
    /// Shown before running, for tasks that disturb the machine.
    #[serde(default)]
    pub caution: Option<String>,
    /// CI plumbing: declared for completeness, never offered.
    #[serde(default)]
    pub hidden: bool,
    /// This task operates ON the repository — building it, diffing its rendered output,
    /// checking its own documentation — so it cannot run from the bundled examples however
    /// much is embedded (ADR 0056 amendment).
    ///
    /// **Declared, not inferred.** Inferring it from whether a script happened to be
    /// embedded is the same discovery mistake §2 rejects: `render-parity.sh` travels
    /// perfectly well and still cannot run, because what it needs is the operator crate.
    #[serde(default)]
    pub needs_checkout: bool,
    /// Command-line arguments, with `${VAR}` substituted from the resolved environment.
    ///
    /// Exists because a declared variable is not automatically an argument: `INPUT` for the
    /// Mosquitto converter was documented as "passed as the first argument" while the
    /// runner only ever exported it, so the task could not work as described. Arguments are
    /// declared for the same reason everything else here is — the alternative is each task
    /// growing its own special case in the runner.
    #[serde(default)]
    pub args: Vec<String>,

    /// Tuneable environment.
    #[serde(default, rename = "env")]
    pub env: Vec<EnvVar>,
}

/// The parsed manifest.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    #[serde(rename = "task")]
    pub tasks: Vec<Task>,
}

/// What went wrong loading or validating a manifest.
#[derive(Debug)]
pub enum Error {
    Read(String),
    Parse(String),
    /// The manifest is internally inconsistent — duplicate ids, a script that is not there.
    Invalid(Vec<String>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(e) => write!(f, "cannot read tasks.toml: {e}"),
            Self::Parse(e) => write!(f, "tasks.toml is not valid: {e}"),
            Self::Invalid(problems) => {
                writeln!(f, "tasks.toml is inconsistent with the repository:")?;
                for p in problems {
                    writeln!(f, "  - {p}")?;
                }
                Ok(())
            }
        }
    }
}

/// The manifest ships inside the binary, so a standalone `mqttui` offers the same task
/// list as one in a checkout — what differs is which of them this machine can run.
pub const EMBEDDED: &str = include_str!("../tasks.toml");

impl Manifest {
    /// Parse without touching the filesystem — the standalone path, where repo-only tasks
    /// are legitimately absent and are reported as unavailable rather than rejected.
    ///
    /// # Errors
    /// If the TOML is malformed or has duplicate ids.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let manifest: Self = toml::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        let dups: Vec<String> = manifest
            .tasks
            .iter()
            .filter(|t| !seen.insert(&t.id))
            .map(|t| format!("id '{}' is declared twice", t.id))
            .collect();
        if dups.is_empty() {
            Ok(manifest)
        } else {
            Err(Error::Invalid(dups))
        }
    }

    /// Load and validate against `repo_root`.
    ///
    /// # Errors
    /// If the file cannot be read or parsed, or if it declares something the repository
    /// does not have — a duplicate id, or a script that is not on disk. A manifest that
    /// points at a missing script would fail at the worst moment, so it fails here.
    pub fn load(path: &Path, repo_root: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path).map_err(|e| Error::Read(e.to_string()))?;
        let manifest: Self = toml::from_str(&text).map_err(|e| Error::Parse(e.to_string()))?;

        let mut problems = Vec::new();
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for t in &manifest.tasks {
            if !seen.insert(&t.id) {
                problems.push(format!("id '{}' is declared twice", t.id));
            }
            if !repo_root.join(&t.script).is_file() {
                problems.push(format!(
                    "task '{}' points at '{}', which does not exist",
                    t.id, t.script
                ));
            }
        }
        if problems.is_empty() {
            Ok(manifest)
        } else {
            Err(Error::Invalid(problems))
        }
    }

    /// Every task a user is offered, in declaration order.
    pub fn visible(&self) -> impl Iterator<Item = &Task> {
        self.tasks.iter().filter(|t| !t.hidden)
    }

    /// Tasks a user may pick, grouped, in manifest order within each group.
    #[must_use]
    pub fn visible_by_group(&self) -> Vec<(&str, Vec<&Task>)> {
        let mut order: Vec<&str> = Vec::new();
        let mut by_group: BTreeMap<&str, Vec<&Task>> = BTreeMap::new();
        for t in self.tasks.iter().filter(|t| !t.hidden) {
            if !by_group.contains_key(t.group.as_str()) {
                order.push(&t.group);
            }
            by_group.entry(&t.group).or_default().push(t);
        }
        order
            .into_iter()
            .map(|g| (g, by_group.remove(g).unwrap_or_default()))
            .collect()
    }

    /// Look a task up by id, hidden ones included — `--run gen-status` should work for
    /// somebody who knows what they are asking for, even though it is not offered.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&Task> {
        self.tasks.iter().find(|t| t.id == id)
    }
}

/// Executable scripts found under the directories the manifest is meant to cover.
///
/// Deliberately a *separate* walk from anything the manifest informs: if the listing and
/// the check derived from the same source they could never disagree, and the guard would
/// be a check that cannot fail (ADR 0056 §3).
#[must_use]
pub fn scripts_on_disk(repo_root: &Path) -> Vec<String> {
    const ROOTS: [&str; 3] = ["scripts", "demo", "bench"];
    let mut found = Vec::new();
    for root in ROOTS {
        walk(&repo_root.join(root), repo_root, &mut found);
    }
    found.sort();
    found
}

fn walk(dir: &Path, repo_root: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Generated output and caches are not scripts; `results/` alone is 12 MB.
        if name == "results" || name == "__pycache__" || name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            walk(&path, repo_root, out);
        } else if is_script(&path) {
            if let Ok(rel) = path.strip_prefix(repo_root) {
                out.push(rel.to_string_lossy().into_owned());
            }
        }
    }
}

/// Any `.sh` or `.py` file under the walked roots.
///
/// Note what this does **not** require: the executable bit. That was the first rule here,
/// and it was wrong — `scripts/gen-status.py` and `scripts/gen-bridge-dashboard.py` are
/// mode 644 and invoked as `python3 <file>`, so an executable-only walk could not see them.
/// A completeness guard with a blind spot is not a completeness guard: a new script landing
/// without `chmod +x` would have slipped past in silence, which is precisely the failure
/// this exists to prevent.
///
/// The cost is that a Python module meant only for import would be flagged. There are none
/// today, and if one appears the answer is one `hidden = true` line — cheap, and it errs
/// towards the manifest knowing about more rather than less.
fn is_script(path: &Path) -> bool {
    matches!(path.extension().and_then(|e| e.to_str()), Some("sh" | "py"))
}

/// Scripts present on disk but absent from the manifest.
///
/// The whole point of the manifest being CI-gated: a launcher that silently shows a
/// subset becomes the list people trust.
#[must_use]
pub fn missing_from_manifest(manifest: &Manifest, repo_root: &Path) -> Vec<String> {
    let declared: Vec<&str> = manifest.tasks.iter().map(|t| t.script.as_str()).collect();
    scripts_on_disk(repo_root)
        .into_iter()
        .filter(|s| !declared.contains(&s.as_str()))
        .collect()
}

/// Find the repository root by walking up from `start` looking for the marker every
/// checkout has. `None` means we are running standalone, outside a checkout.
#[must_use]
pub fn find_repo_root(start: &Path) -> Option<PathBuf> {
    let mut dir = Some(start);
    while let Some(d) = dir {
        if d.join("Cargo.toml").is_file() && d.join("crates").join("mqttd").is_dir() {
            return Some(d.to_path_buf());
        }
        dir = d.parent();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        // The crate lives at <root>/tools/mqttui.
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("tools/mqttui sits two levels below the root")
            .to_path_buf()
    }

    fn manifest_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tasks.toml")
    }

    fn load() -> Manifest {
        Manifest::load(&manifest_path(), &repo_root()).expect("the shipped manifest must load")
    }

    /// The shipped manifest parses, has no duplicate ids, and every script it names exists.
    #[test]
    fn the_shipped_manifest_is_valid() {
        let m = load();
        assert!(m.tasks.len() > 10, "expected the real manifest, not a stub");
    }

    /// **The guard.** Every executable script under scripts/, demo/ and bench/ is declared.
    ///
    /// Without this the manifest quietly falls behind the tree and `mqttui` shows fourteen
    /// of twenty-three scripts while looking complete — which is worse than no tool,
    /// because it becomes the list people trust.
    #[test]
    fn every_script_on_disk_is_declared() {
        let missing = missing_from_manifest(&load(), &repo_root());
        assert!(
            missing.is_empty(),
            "these scripts exist but are not in tasks.toml:\n  {}\n\nAdd each one. If it is \
             CI plumbing nobody should run by hand, declare it with `hidden = true` rather \
             than leaving it out — the point is that the manifest knows about everything.",
            missing.join("\n  ")
        );
    }

    /// The guard must be able to fail. A walk that found nothing would make the test above
    /// pass vacuously — the exact shape it exists to prevent.
    #[test]
    fn the_completeness_walk_actually_finds_scripts() {
        let found = scripts_on_disk(&repo_root());
        assert!(
            found.len() >= 15,
            "the walk found only {} scripts, so the completeness test proves nothing",
            found.len()
        );
        assert!(found.iter().any(|s| s == "scripts/deploy-smoke.sh"));
        assert!(
            !found.iter().any(|s| s.contains("__pycache__")),
            "caches must not be offered as tasks"
        );
    }

    /// Hidden tasks are declared but never offered.
    #[test]
    fn ci_plumbing_is_declared_but_not_offered() {
        let m = load();
        assert!(m.get("gen-status").is_some(), "declared");
        let offered: Vec<&str> = m
            .visible_by_group()
            .into_iter()
            .flat_map(|(_, ts)| ts)
            .map(|t| t.id.as_str())
            .collect();
        assert!(
            !offered.contains(&"gen-status"),
            "CI plumbing must not be offered"
        );
        assert!(offered.contains(&"deploy-smoke"), "real tasks must be");
    }

    /// A manifest naming a script that is not there fails at load, not at run time.
    #[test]
    fn a_missing_script_is_rejected_at_load() {
        let dir = std::env::temp_dir().join(format!("mqttui-t-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("tasks.toml");
        std::fs::write(
            &p,
            "[[task]]\nid=\"x\"\ngroup=\"g\"\nname=\"n\"\nscript=\"nope/absent.sh\"\n",
        )
        .unwrap();
        match Manifest::load(&p, &repo_root()) {
            Err(Error::Invalid(problems)) => {
                assert!(problems[0].contains("does not exist"), "{problems:?}");
            }
            other => panic!("expected an Invalid error, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two tasks sharing an id would make `--run` ambiguous.
    #[test]
    fn duplicate_ids_are_rejected() {
        let dir = std::env::temp_dir().join(format!("mqttui-d-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("tasks.toml");
        let one =
            "[[task]]\nid=\"dup\"\ngroup=\"g\"\nname=\"n\"\nscript=\"scripts/gen-status.py\"\n";
        std::fs::write(&p, format!("{one}{one}")).unwrap();
        match Manifest::load(&p, &repo_root()) {
            Err(Error::Invalid(problems)) => {
                assert!(
                    problems.iter().any(|p| p.contains("declared twice")),
                    "{problems:?}"
                );
            }
            other => panic!("expected an Invalid error, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unknown key is a typo, and a typo that is silently ignored is a setting that
    /// does not do what its author thinks.
    #[test]
    fn an_unknown_field_is_rejected() {
        let dir = std::env::temp_dir().join(format!("mqttui-u-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("tasks.toml");
        std::fs::write(
            &p,
            "[[task]]\nid=\"x\"\ngroup=\"g\"\nname=\"n\"\nscript=\"scripts/gen-status.py\"\nrequries=[\"typo\"]\n",
        )
        .unwrap();
        assert!(matches!(
            Manifest::load(&p, &repo_root()),
            Err(Error::Parse(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
