//! `mqttd --check-config` (ADR 0046 T3): validates the effective config and exits without
//! binding a port. These drive the real binary — the whole point is that no listener is bound
//! and the exit code + message are the GitOps/pre-rollout contract.

use std::io::Write as _;
use std::process::Command;

fn mqttd() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    // A hermetic environment: strip any MQTTD_* the runner might carry so each case controls
    // its own overlay. (Only MQTTD_* matters; RUST_LOG etc. are harmless.)
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            c.env_remove(k);
        }
    }
    c
}

fn write_tmp(name: &str, body: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("mqttd-checkcfg-{}-{name}", std::process::id()));
    let mut f = std::fs::File::create(&p).unwrap();
    f.write_all(body.as_bytes()).unwrap();
    p
}

#[test]
fn no_config_file_validates_defaults_plus_env() {
    // Defaults are secure and valid, so a bare check passes and binds nothing.
    let out = mqttd().arg("--check-config").output().unwrap();
    assert!(
        out.status.success(),
        "expected exit 0, got {:?}; stderr={}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("config OK"), "stdout was: {stdout}");
}

#[test]
fn a_valid_file_validates_and_reports_the_path() {
    let path = write_tmp(
        "ok.toml",
        "[node]\nid = \"checked\"\n[durable]\nenabled = false\n",
    );
    let out = mqttd()
        .arg("--check-config")
        .arg("--config")
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("config OK"), "stdout was: {stdout}");
    assert!(
        stdout.contains(&path.display().to_string()),
        "the OK line should name the checked file; stdout was: {stdout}"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn an_unknown_key_fails_with_a_located_error_and_exit_1() {
    let path = write_tmp("bad.toml", "[node]\nid = \"x\"\nbogus_key = 1\n");
    let out = mqttd()
        .arg("--check-config")
        .arg("--config")
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "expected exit 1 for an invalid config"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("config INVALID"), "stderr was: {stderr}");
    // The parse error is located (TOML line/column + the offending key).
    assert!(
        stderr.contains("bogus_key"),
        "expected a located error; stderr was: {stderr}"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_bad_env_value_fails_check_config() {
    // An out-of-range env overlay (0 voters is un-electable) is caught by the same check.
    let out = mqttd()
        .arg("--check-config")
        .env("MQTTD_LEASE_VOTERS", "0")
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("lease_voters"), "stderr was: {stderr}");
}

#[test]
fn a_config_flag_without_a_value_is_a_usage_error_exit_2() {
    let out = mqttd()
        .arg("--check-config")
        .arg("--config")
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "a malformed invocation should exit 2"
    );
}
