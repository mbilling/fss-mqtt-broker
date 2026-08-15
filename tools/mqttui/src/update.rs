//! `mqttui update` — fetch the latest examples as a *signed artifact*, never as a branch
//! (ADR 0056 Decision B, T8).
//!
//! CI publishes a rolling bundle on every merge to main (`examples-main` release), signed
//! keylessly with cosign. This downloads it, **verifies it before a single byte is
//! unpacked**, and installs it beside the embedded copy. The embedded examples stay the
//! default; an installed update takes precedence and `--list` says so, with its commit.
//!
//! Verification is not optional and not configurable: the expected signing identity — this
//! repository's GitHub Actions workflows — and the OIDC issuer are hard-coded. The one
//! thing an environment variable can move is the download URL (`MQTTUI_UPDATE_BASE_URL`,
//! for tests), which is safe precisely *because* verification is pinned: a different
//! server cannot mint a certificate for this repository's identity.
//!
//! `--channel main` fetches the raw branch tarball instead — explicitly, loudly marked
//! unverified, for maintainers testing unreleased examples. It is never a default and the
//! installed result carries an UNVERIFIED marker that the source line repeats on every
//! `--list`.
//!
//! Downloading is `curl`, verifying is `cosign`, unpacking is `tar` — shelled out, like
//! every task this tool runs, rather than pulled in as TLS and sigstore dependency trees.
//! All three are required and their absence refuses the update by name; a missing verifier
//! must never degrade into not verifying.

use std::path::{Path, PathBuf};
use std::process::Command;

const RELEASE_BASE: &str =
    "https://github.com/mbilling/fss-mqtt-broker/releases/download/examples-main";
const BRANCH_TARBALL: &str =
    "https://codeload.github.com/mbilling/fss-mqtt-broker/tar.gz/refs/heads/main";
const IDENTITY_REGEXP: &str = "^https://github.com/mbilling/fss-mqtt-broker/";
const OIDC_ISSUER: &str = "https://token.actions.githubusercontent.com";

/// Where an installed update lives: beside the version-stamped embedded unpacks, under the
/// same cache root, so `--clear` and cache cleanup have one place to look.
pub fn installed_dir() -> Result<PathBuf, String> {
    Ok(crate::embedded::cache_root()?.join("examples-updated"))
}

/// The provenance line recorded at install time, shown in `--list`'s source line.
pub fn provenance(dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(dir.join("BUNDLE_PROVENANCE")).ok()?;
    let commit = text
        .lines()
        .find_map(|l| l.strip_prefix("commit="))
        .unwrap_or("unknown");
    let commit_short = commit.get(..12).unwrap_or(commit);
    if text.lines().any(|l| l == "verified=false") {
        // The branch tarball names no commit, so say "the main branch" rather than the
        // redundant main@main-branch.
        Some("the raw main branch — UNVERIFIED (--channel main)".to_string())
    } else {
        Some(format!("main@{commit_short}, signature verified"))
    }
}

/// Entry point for `mqttui update [--clear | --channel main]`.
pub fn run(args: &[String]) -> Result<String, String> {
    match args.first().map(String::as_str) {
        None => install_verified(),
        Some("--clear") => clear(),
        Some("--channel") => match args.get(1).map(String::as_str) {
            Some("main") => install_unverified_main(),
            _ => Err("usage: mqttui update --channel main (the only channel)".into()),
        },
        Some(other) => Err(format!(
            "unknown option '{other}'\nusage: mqttui update [--clear | --channel main]"
        )),
    }
}

fn clear() -> Result<String, String> {
    let dir = installed_dir()?;
    if !dir.exists() {
        return Ok("no installed update; the embedded examples are already in use".into());
    }
    std::fs::remove_dir_all(&dir)
        .map_err(|e| format!("could not remove {}: {e}", dir.display()))?;
    Ok("removed the installed update; back to the embedded examples".into())
}

fn install_verified() -> Result<String, String> {
    require(&["curl", "cosign", "tar"])?;
    let base = std::env::var("MQTTUI_UPDATE_BASE_URL").unwrap_or_else(|_| RELEASE_BASE.to_string());

    let work = mktemp()?;
    let result = (|| {
        for f in [
            "examples.tar.gz",
            "examples.tar.gz.sig",
            "examples.tar.gz.pem",
        ] {
            download(&format!("{base}/{f}"), &work.join(f))?;
        }

        // THE gate. Nothing is unpacked, and nothing is installed, unless this passes.
        verify(&work)?;

        let unpacked = work.join("unpacked");
        std::fs::create_dir(&unpacked).map_err(|e| e.to_string())?;
        run_tar(&work.join("examples.tar.gz"), &unpacked)?;
        sanity_check(&unpacked)?;
        install(&unpacked)?;
        let dir = installed_dir()?;
        Ok(format!(
            "installed the signed examples bundle ({}).\n\
             `mqttui --list` now serves it; `mqttui update --clear` returns to the embedded copy.",
            provenance(&dir).unwrap_or_else(|| "provenance file missing".into())
        ))
    })();
    let _ = std::fs::remove_dir_all(&work);
    result
}

fn install_unverified_main() -> Result<String, String> {
    require(&["curl", "tar"])?;
    eprintln!(
        "mqttui: WARNING — fetching the RAW main branch. This tarball is NOT signature-\n\
         verified: you are trusting the transport and the branch, which is exactly what\n\
         `mqttui update` exists to avoid. Meant for maintainers testing unreleased examples."
    );

    let work = mktemp()?;
    let result = (|| {
        let tarball = work.join("main.tar.gz");
        download(BRANCH_TARBALL, &tarball)?;
        let raw = work.join("raw");
        std::fs::create_dir(&raw).map_err(|e| e.to_string())?;
        run_tar(&tarball, &raw)?;
        // codeload wraps everything in <repo>-main/.
        let repo = std::fs::read_dir(&raw)
            .map_err(|e| e.to_string())?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.is_dir())
            .ok_or("the branch tarball contained no directory")?;

        // Reassemble the bundle layout the same way scripts/vendor-mqttui-examples.sh does.
        let stage = work.join("stage");
        for (src, dst) in [
            ("demo", "demo"),
            ("deploy", "deploy"),
            ("scripts/migrate", "scripts/migrate"),
            ("scripts/k8s", "scripts/k8s"),
        ] {
            copy_tree(&repo.join(src), &stage.join(dst))?;
        }
        std::fs::copy(
            repo.join("tools/mqttui/tasks.toml"),
            stage.join("tasks.toml"),
        )
        .map_err(|e| format!("tasks.toml: {e}"))?;
        std::fs::write(
            stage.join("BUNDLE_PROVENANCE"),
            "commit=main-branch\nverified=false\n",
        )
        .map_err(|e| e.to_string())?;
        sanity_check(&stage)?;
        install(&stage)?;
        Ok(
            "installed the UNVERIFIED main-branch examples. `mqttui --list` will say so on \
            every run; `mqttui update` replaces it with the signed bundle, `--clear` with \
            the embedded copy."
                .into(),
        )
    })();
    let _ = std::fs::remove_dir_all(&work);
    result
}

/// Run cosign over the downloaded triple in `work`. The identity and issuer are constants
/// of this module — verification has no configuration surface, because a configurable
/// verifier is a disableable one.
fn verify(work: &Path) -> Result<(), String> {
    verify_with(work, "cosign")
}

/// The verifier, with the verifying program named.
///
/// [`verify`] pins it to `cosign`; the parameter exists so the **refusal** path can be tested
/// without a cosign install. It could not be, and so the module's only refusal test guarded
/// itself with `if !on_path("cosign") { return; }` — and because no CI job installs cosign,
/// that guard fired on every CI run. The test reported success without running for the whole
/// life of the file (issue #260). One parameter buys back the coverage.
fn verify_with(work: &Path, program: &str) -> Result<(), String> {
    let out = Command::new(program)
        .args(["verify-blob", "--certificate"])
        .arg(work.join("examples.tar.gz.pem"))
        .arg("--signature")
        .arg(work.join("examples.tar.gz.sig"))
        .args(["--certificate-identity-regexp", IDENTITY_REGEXP])
        .args(["--certificate-oidc-issuer", OIDC_ISSUER])
        .arg(work.join("examples.tar.gz"))
        .output()
        .map_err(|e| format!("could not run cosign: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "signature verification FAILED — nothing was installed.\n{}",
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

// ── plumbing ─────────────────────────────────────────────────────────────────────────

fn require(tools: &[&str]) -> Result<(), String> {
    let missing: Vec<&str> = tools
        .iter()
        .copied()
        .filter(|t| !crate::preflight::on_path(t))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "update needs {} — refusing rather than proceeding without. A missing verifier \
             must never mean 'skip verification'.",
            missing.join(", ")
        ))
    }
}

fn download(url: &str, dest: &Path) -> Result<(), String> {
    let out = Command::new("curl")
        .args(["-fsSL", "--proto", "=https", "--max-time", "120", "-o"])
        .arg(dest)
        .arg(url)
        .output()
        .map_err(|e| format!("could not run curl: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "download failed: {url}\n{}\n(no bundle published yet? CI publishes it on the \
             first merge to main after this feature lands)",
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

fn run_tar(archive: &Path, into: &Path) -> Result<(), String> {
    let out = Command::new("tar")
        .arg("-xzf")
        .arg(archive)
        .arg("-C")
        .arg(into)
        .output()
        .map_err(|e| format!("could not run tar: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "unpack failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// A bundle that unpacked but is missing its skeleton would install as an empty example
/// set — refuse it rather than let `--list` quietly shrink.
fn sanity_check(dir: &Path) -> Result<(), String> {
    for p in [
        "tasks.toml",
        "demo",
        "deploy",
        "scripts/migrate",
        "scripts/k8s",
    ] {
        if !dir.join(p).exists() {
            return Err(format!(
                "the bundle is missing {p} — refusing to install it"
            ));
        }
    }
    Ok(())
}

/// Swap the staged tree into place. Remove-then-rename, not rename-over: a torn state here
/// is an absent directory, which reads as "no update installed" — the safe failure.
fn install(staged: &Path) -> Result<(), String> {
    let dir = installed_dir()?;
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    copy_tree(staged, &dir)?;
    // Scripts must come out executable, exactly as embedded::unpack does for its copy.
    crate::embedded::make_scripts_executable(&dir);
    Ok(())
}

fn copy_tree(src: &Path, dst: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dst).map_err(|e| format!("{}: {e}", dst.display()))?;
    for entry in std::fs::read_dir(src).map_err(|e| format!("{}: {e}", src.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), &to).map_err(|e| format!("{}: {e}", to.display()))?;
        }
    }
    Ok(())
}

fn mktemp() -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join(format!("mqttui-update-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the whole module exists for: garbage in place of a signature must be
    /// an error, so the caller's `?` stops anything after it — unpacking included.
    #[test]
    fn a_bad_signature_is_an_error() {
        let work = std::env::temp_dir().join(format!("mqttui-verify-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&work);
        std::fs::create_dir_all(&work).expect("temp dir");
        std::fs::write(work.join("examples.tar.gz"), b"payload").unwrap();
        std::fs::write(work.join("examples.tar.gz.sig"), b"bm90IGEgc2ln").unwrap();
        std::fs::write(work.join("examples.tar.gz.pem"), b"not a certificate").unwrap();

        // No environmental skip. This test used to begin `if !on_path("cosign") { return; }`,
        // and no CI job installs cosign — so the module's only refusal test reported success
        // without running on every CI run (issue #260). What is being checked is mqttui's, not
        // cosign's cryptography: a verifier that refuses must produce an error whose text says
        // so, and a verifier that is ABSENT must be an error too, never a pass. `require()`
        // states the second in prose ("a missing verifier must never mean 'skip verification'");
        // here it is asserted.
        let absent = work.join("no-such-verifier");
        let err = verify_with(&work, &absent.display().to_string())
            .expect_err("a missing verifier must never verify");
        assert!(
            err.contains("could not run"),
            "the error must name the missing verifier: {err}"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let stub = |name: &str, body: &str| {
                let p = work.join(name);
                std::fs::write(&p, format!("#!/bin/sh\n{body}")).unwrap();
                std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
                p.display().to_string()
            };
            let refuser = stub("refuser", "echo 'stub: signature refused' >&2\nexit 1\n");
            let err = verify_with(&work, &refuser).expect_err("garbage must never verify");
            assert!(
                err.contains("FAILED"),
                "the error must say what happened: {err}"
            );
            // Non-vacuity: without this, every assertion above would also hold for a
            // `verify_with` that can only ever fail — including one that never runs the
            // verifier at all.
            //
            // And the accepter asserts its OWN ARGV, because "the verifier ran and said yes" is
            // the cheap half of the property: WHO is trusted to sign is the security-relevant
            // half, and a stub that ignores argv leaves the pins untested — mutating
            // `IDENTITY_REGEXP` to `.*` and `OIDC_ISSUER` to an attacker's issuer kept the whole
            // suite green (issue #260 round 2, finding 9). The pinned values are written out
            // here in full, deliberately: a test that read them from the constants it is
            // checking would agree with any mutation of them.
            let accepter = stub(
                "accepter",
                r#"case " $* " in
  *" --certificate-identity-regexp ^https://github.com/mbilling/fss-mqtt-broker/ "*) ;;
  *) echo "stub: releases must be pinned to this repository's signing identity: $*" >&2
     exit 3 ;;
esac
case " $* " in
  *" --certificate-oidc-issuer https://token.actions.githubusercontent.com "*) ;;
  *) echo "stub: the OIDC issuer must be pinned to GitHub Actions: $*" >&2
     exit 3 ;;
esac
exit 0
"#,
            );
            verify_with(&work, &accepter)
                .expect("a verifier that accepts must verify — and be handed both pins");
        }

        // The strong form, when the environment allows it: the REAL cosign must refuse the
        // same garbage. Added on top of assertions that always run, so nothing is skipped
        // when cosign is absent — the coverage above does not depend on it.
        if crate::preflight::on_path("cosign") {
            let err = verify(&work).expect_err("cosign must never verify garbage");
            assert!(err.contains("FAILED"), "{err}");
        }
        let _ = std::fs::remove_dir_all(&work);
    }

    /// A bundle that unpacked but lacks its skeleton would install as an empty example
    /// set — `--list` would quietly shrink, which is the silent-subset defect again.
    #[test]
    fn a_hollow_bundle_is_refused() {
        let dir = std::env::temp_dir().join(format!("mqttui-hollow-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("demo")).unwrap();
        let err = sanity_check(&dir).expect_err("a bundle without its skeleton must be refused");
        assert!(err.contains("tasks.toml"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unverified install must READ as unverified everywhere it is reported, not only
    /// in a warning printed once at install time.
    #[test]
    fn provenance_keeps_unverified_loud() {
        let dir = std::env::temp_dir().join(format!("mqttui-prov-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("BUNDLE_PROVENANCE"), "commit=abcdef0123456789\n").unwrap();
        assert_eq!(
            provenance(&dir).as_deref(),
            Some("main@abcdef012345, signature verified")
        );

        std::fs::write(
            dir.join("BUNDLE_PROVENANCE"),
            "commit=main-branch\nverified=false\n",
        )
        .unwrap();
        let line = provenance(&dir).expect("provenance");
        assert!(line.contains("UNVERIFIED"), "{line}");
        assert!(
            !line.contains("main@main-branch"),
            "redundant commit label: {line}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Refusal names what is missing — and a missing verifier refuses rather than skips.
    #[test]
    fn missing_tools_refuse_by_name() {
        let err = require(&["definitely-not-a-real-tool-xyzzy"]).expect_err("must refuse");
        assert!(err.contains("definitely-not-a-real-tool-xyzzy"), "{err}");
        assert!(err.contains("skip verification"), "{err}");
    }
}
