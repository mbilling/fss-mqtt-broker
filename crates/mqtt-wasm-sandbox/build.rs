//! Finds the jq module for this build (ADR 0086 D4, D11).
//!
//! `jq.wasm` is not in the source tree: `jq-module/build.sh` builds it from pinned
//! sources, byte for byte the file whose sha256 `jq-module/jq.wasm.sha256` records. A
//! build that finds the file embeds it (`jq::embedded()`), after comparing it with
//! that hash; a build that does not find it is whole without it, and `jq` then says it
//! is not built in. So `cargo build` works on a machine without the module's
//! toolchain, and a release cannot ship without the module by accident:
//!
//! * `MQTTD_JQ_WASM` names the file (default: `jq-module/jq.wasm` in this crate);
//! * `MQTTD_JQ_WASM_REQUIRED=1` makes its absence an error (the release recipe and CI
//!   set it);
//! * a file that is not the recorded build is an error either way.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use sha2::Digest as _;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(jq_wasm)");
    println!("cargo::rerun-if-env-changed=MQTTD_JQ_WASM");
    println!("cargo::rerun-if-env-changed=MQTTD_JQ_WASM_REQUIRED");
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed=jq-module/jq.wasm.sha256");
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let path = std::env::var_os("MQTTD_JQ_WASM")
        .filter(|p| !p.is_empty())
        .map_or_else(|| manifest.join("jq-module/jq.wasm"), PathBuf::from);
    let required = std::env::var_os("MQTTD_JQ_WASM_REQUIRED").is_some_and(|v| v == "1");
    watch(&path);
    let module = match std::fs::read(&path) {
        Ok(module) => module,
        Err(e) if required => {
            fail(&format!(
                "MQTTD_JQ_WASM_REQUIRED=1 and {} cannot be read ({e}): build it with \
                 crates/mqtt-wasm-sandbox/jq-module/build.sh",
                path.display()
            ));
            return;
        }
        // Built without the module: `jq` reports that it is not built in.
        Err(_) => return,
    };
    let recorded =
        std::fs::read_to_string(manifest.join("jq-module/jq.wasm.sha256")).unwrap_or_default();
    let recorded = recorded.split_whitespace().next().unwrap_or_default();
    let found = sha2::Sha256::digest(&module)
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        });
    if found != recorded {
        fail(&format!(
            "{} has sha256 {found}, and jq-module/jq.wasm.sha256 records {recorded}: it is \
             not what jq-module/build.sh builds at this revision (rebuild it, or remove it \
             to build without jq)",
            path.display()
        ));
        return;
    }
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap_or_default()).join("jq.wasm");
    if let Err(e) = std::fs::write(&out, &module) {
        fail(&format!("{}: {e}", out.display()));
        return;
    }
    println!("cargo::rustc-cfg=jq_wasm");
}

/// Reruns this script when the module appears, changes or goes. A path that does not
/// exist cannot be watched (cargo would then rerun the script, and rebuild everything
/// that depends on this crate, on every build), so its directory is.
fn watch(path: &Path) {
    let watched = if path.exists() {
        Some(path)
    } else {
        path.parent().filter(|dir| dir.is_dir())
    };
    if let Some(watched) = watched {
        println!("cargo::rerun-if-changed={}", watched.display());
    }
}

fn fail(message: &str) {
    println!("cargo::error={message}");
}
