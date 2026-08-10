//! The examples, carried inside the binary (ADR 0056 T7, amendment of 2026-08-10).
//!
//! `cargo install mqttui` produces no checkout, and the scripts *are* the product — they
//! also do not stand alone: `deploy-smoke.sh` reads two files from `deploy/`,
//! `kind-smoke.sh` needs the whole Helm chart. So the parts that can travel do.
//!
//! **Embedded, not fetched.** The whole surface measures 190 KB compressed. That buys
//! offline operation, version-locking to the binary that was tested with it, and — the
//! property that matters — **executing nothing that arrived over the network**. Fetching a
//! branch tarball at runtime was rejected as a default: this project cosign-signs its
//! releases with SLSA provenance and an SBOM, and downloading shell from a mutable branch
//! and running it would discard that with one command, on every launch.
//!
//! ## What cannot travel
//!
//! Four tasks operate *on the repository* — `build-repro.sh` builds it,
//! `render-parity.sh` diffs the chart against the operator, `gen-status.py` and
//! `check-readme-facts.py` check its own documentation. No amount of embedding frees them,
//! and the ADR says so rather than letting it be discovered.

use include_dir::{include_dir, Dir};
use std::path::{Path, PathBuf};

/// The example surface: `demo/`, `deploy/`, `scripts/migrate/` and `scripts/k8s/`, laid out
/// exactly as they sit in the repository because `tasks.toml` addresses scripts by their
/// repository path. Each directory travels whole — these scripts read their siblings, and
/// shipping a script without the files it opens would be worse than not shipping it.
///
/// Read from `bundle/`, a **generated copy** maintained by
/// `scripts/vendor-mqttui-examples.sh`, not from `../../` directly. `cargo package` includes
/// only files beneath the package root, so pointing at the originals produces a crate that
/// builds from a checkout and cannot compile once published — confirmed by
/// `cargo publish --dry-run` before this was changed. CI runs the script's `--check` mode,
/// so the copy cannot go stale unnoticed.
static BUNDLE: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/bundle");

/// Why a task cannot run here.
///
/// Two reasons, kept apart because the user does something different about each: a
/// repo-only task needs a clone and always will; an unbundled one is simply not in this
/// binary's example set. Collapsing them into one "unavailable" would tell someone to
/// clone the repository when what they actually need is on their disk already.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Availability {
    /// Runnable here.
    Yes,
    /// Operates on the repository — building it, diffing its rendered output, checking its
    /// own docs. No amount of embedding frees it.
    NeedsCheckout,
    /// Not part of the embedded example set (the benchmark harness, the interop suites,
    /// the OIDC fixture — all of which pull in fixtures far larger than the examples).
    NotBundled,
}

/// Where the binary is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// Inside a checkout: everything is available, from disk.
    Checkout(PathBuf),
    /// Standalone: only the embedded examples, unpacked to a cache directory.
    Embedded(PathBuf),
    /// Standalone, with an installed `mqttui update` bundle taking precedence over the
    /// embedded copy. Availability follows the same rules as [`Self::Embedded`]; what
    /// differs is provenance, which the list's source line states on every run.
    Updated(PathBuf),
}

impl Source {
    /// The directory tasks should run from.
    #[must_use]
    pub fn root(&self) -> &Path {
        match self {
            Self::Checkout(p) | Self::Embedded(p) | Self::Updated(p) => p,
        }
    }

    /// Whether a task can run here, and if not, why.
    ///
    /// In a checkout, everything can. Standalone the **declaration** decides first: a
    /// script can travel perfectly well and still be unrunnable, which is why this is not
    /// inferred from the file being present. `render-parity.sh` is the case that taught it
    /// — it is embedded, it existed, it ran, and it died with `could not find Cargo.toml`,
    /// because what it needs is the operator crate rather than itself.
    ///
    /// The file check comes second, and catches the manifest drifting ahead of what is
    /// actually bundled.
    #[must_use]
    pub fn availability(&self, task: &crate::manifest::Task) -> Availability {
        match self {
            Self::Checkout(_) => Availability::Yes,
            Self::Embedded(root) | Self::Updated(root) => {
                if task.needs_checkout {
                    Availability::NeedsCheckout
                } else if root.join(&task.script).is_file() {
                    Availability::Yes
                } else {
                    Availability::NotBundled
                }
            }
        }
    }

    /// Shorthand for [`Self::availability`] being [`Availability::Yes`].
    #[must_use]
    pub fn can_run(&self, task: &crate::manifest::Task) -> bool {
        self.availability(task) == Availability::Yes
    }
}

/// Unpack the embedded examples into a cache directory and return it.
///
/// Written to disk rather than executed from memory because these are shell scripts that
/// read their siblings by relative path; a script cannot `source` a byte slice.
///
/// # Errors
/// If the cache directory cannot be created or written.
pub fn unpack() -> Result<PathBuf, String> {
    let base = cache_root()?;
    // Version-stamped, so upgrading the binary cannot leave an older example behind — the
    // examples are only trustworthy as the set the binary was tested with.
    let root = base.join(format!("examples-{}", env!("CARGO_PKG_VERSION")));
    if root.join(".complete").is_file() {
        return Ok(root);
    }
    let _ = std::fs::remove_dir_all(&root);

    write_dir(&BUNDLE, &root)?;

    // Only stamped once everything landed, so an interrupted unpack is redone rather than
    // trusted.
    std::fs::write(root.join(".complete"), env!("CARGO_PKG_VERSION"))
        .map_err(|e| format!("could not finish unpacking examples: {e}"))?;
    Ok(root)
}

fn write_dir(dir: &Dir<'_>, dest: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dest).map_err(|e| format!("{}: {e}", dest.display()))?;
    for file in dir.files() {
        let path = dest.join(file.path().file_name().unwrap_or_default());
        std::fs::write(&path, file.contents()).map_err(|e| format!("{}: {e}", path.display()))?;
        // Shell scripts must be executable, or the whole point is lost.
        if path.extension().and_then(|e| e.to_str()) == Some("sh") {
            make_executable(&path);
        }
    }
    for sub in dir.dirs() {
        let name = sub.path().file_name().unwrap_or_default();
        write_dir(sub, &dest.join(name))?;
    }
    Ok(())
}

/// Walk `dir` and mark every `.sh` executable — an installed update goes through the same
/// treatment as the embedded unpack, or its scripts fail at exec with a permission error.
pub fn make_scripts_executable(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            make_scripts_executable(&path);
        } else if path.extension().and_then(|e| e.to_str()) == Some("sh") {
            make_executable(&path);
        }
    }
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    if let Ok(meta) = std::fs::metadata(path) {
        let mut perms = meta.permissions();
        perms.set_mode(perms.mode() | 0o755);
        let _ = std::fs::set_permissions(path, perms);
    }
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}

/// The mqttui cache root — the embedded unpacks and any installed update both live here,
/// so `update --clear` and cache cleanup have one place to look.
pub fn cache_root() -> Result<PathBuf, String> {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .ok_or_else(|| "no HOME or XDG_CACHE_HOME to unpack examples into".to_string())?;
    let dir = base.join("mqttui");
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The embedded set must actually contain the things the standalone tasks name. An
    /// empty `include_dir!` compiles perfectly happily, so this is the check that the
    /// examples really travelled.
    #[test]
    fn the_examples_are_actually_embedded() {
        for path in [
            "demo/docker-compose.yml",
            "deploy/compose/compose.yaml",
            "deploy/helm/mqttd/Chart.yaml",
            "scripts/migrate/from-mosquitto.py",
            "scripts/k8s/check-bridge-chart.sh",
        ] {
            assert!(
                BUNDLE.get_file(path).is_some(),
                "{path} must travel with the binary — if bundle/ went missing or stale, \
                 `cargo install mqttui` ships a launcher with nothing to launch"
            );
        }
    }

    /// Unpacking produces real files, and a shell script comes out executable — otherwise
    /// every standalone task fails at exec with a permission error.
    #[test]
    fn unpacking_writes_runnable_files() {
        let root = unpack().expect("unpack");
        let compose = root.join("deploy/compose/compose.yaml");
        assert!(compose.is_file(), "{} missing", compose.display());

        let script = root.join("deploy/compose/bootstrap.sh");
        assert!(script.is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&script)
                .expect("meta")
                .permissions()
                .mode();
            assert!(
                mode & 0o111 != 0,
                "a shell script must be executable: {mode:o}"
            );
        }
    }

    /// Standalone, a repo-only task must report as unavailable — the alternative is
    /// offering it and failing at exec with something unhelpful, which is what happened
    /// when availability was inferred from the file being present: `render-parity.sh`
    /// travels fine and still dies with "could not find Cargo.toml".
    #[test]
    fn standalone_availability_follows_the_declaration_not_the_file() {
        let manifest = crate::manifest::Manifest::parse(crate::manifest::EMBEDDED).expect("parse");
        let root = unpack().expect("unpack");
        let standalone = Source::Embedded(root.clone());

        let parity = manifest.get("k8s-render-parity").expect("declared");
        assert!(
            root.join(&parity.script).is_file(),
            "render-parity.sh DOES travel — which is exactly why presence cannot be the test"
        );
        assert!(
            !standalone.can_run(parity),
            "...and it still cannot run standalone: it needs the operator crate"
        );

        let migrate = manifest.get("migrate-mosquitto").expect("declared");
        assert!(
            standalone.can_run(migrate),
            "the converter reads the USER's config"
        );

        // In a checkout everything is available, including the repo-only tasks.
        let checkout = Source::Checkout(PathBuf::from("/anywhere"));
        assert!(checkout.can_run(parity));
    }

    /// "Needs a clone" and "is not in this bundle" must not collapse into one answer:
    /// telling someone to clone the repository when the benchmark harness merely did not
    /// travel sends them to do work that will not help.
    #[test]
    fn the_two_reasons_for_unavailable_stay_apart() {
        let manifest = crate::manifest::Manifest::parse(crate::manifest::EMBEDDED).expect("parse");
        let standalone = Source::Embedded(unpack().expect("unpack"));

        assert_eq!(
            standalone.availability(manifest.get("k8s-render-parity").expect("declared")),
            Availability::NeedsCheckout
        );
        assert_eq!(
            standalone.availability(manifest.get("bench-run").expect("declared")),
            Availability::NotBundled,
            "the benchmark harness is not repo-only — it simply is not bundled"
        );
    }
}
