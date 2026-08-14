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

/// Issue #240: durable sessions are ON by default, and with no data dir the replicated
/// state is RAM-only — a correlated restart of a quorum loses acked messages. That
/// configuration is now REFUSED (a warning log is not a substitute for refusing the
/// configuration), so the bare-defaults check fails and names both ways out.
#[test]
fn bare_defaults_are_refused_naming_both_remedies() {
    let out = mqttd().arg("--check-config").output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "bare defaults (durable on, no data dir) must be refused; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stdout.contains("config OK"), "stdout was: {stdout}");
    for remedy in ["MQTTD_DATA_DIR", "MQTTD_ALLOW_EPHEMERAL_DURABILITY"] {
        assert!(
            stderr.contains(remedy),
            "the refusal must name {remedy}; stderr was: {stderr}"
        );
    }
}

/// Issue #240: each of the three explicit postures validates — the ephemeral opt-in,
/// a real data dir, and durable explicitly OFF (the lightweight in-memory store is an
/// explicit choice already and needs no flag).
#[test]
fn each_posture_validates_under_check_config() {
    let tempdir = std::env::temp_dir().join(format!("mqttd-checkcfg-data-{}", std::process::id()));
    std::fs::create_dir_all(&tempdir).unwrap();
    let postures: [(&str, String); 3] = [
        ("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1".to_string()),
        ("MQTTD_DATA_DIR", tempdir.display().to_string()),
        ("MQTTD_DURABLE_SESSIONS", "0".to_string()),
    ];
    for (key, value) in &postures {
        let out = mqttd()
            .arg("--check-config")
            .env(key, value)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{key}={value} must validate; stderr={}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("config OK"), "{key}: stdout was: {stdout}");
    }
    let _ = std::fs::remove_dir_all(&tempdir);
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
    // The ephemeral opt-in (#240) is set so the failure is for THIS reason, not the
    // missing data dir.
    let out = mqttd()
        .arg("--check-config")
        .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
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

/// Issue #243: the watermark cadence is a knob with a documented floor and ceiling, and
/// `--check-config` is where a bad one must be caught — a broker that booted with a 0 s
/// poll would spin, and one with a 1-hour poll would carry a watermark that cannot bound
/// anything. Runs against the REAL binary, so it also proves the env var reaches
/// `validate()` at all.
#[test]
fn check_config_rejects_a_watermark_poll_outside_its_range() {
    for bad in ["0", "301"] {
        let out = mqttd()
            .arg("--check-config")
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("MQTTD_WATERMARK_POLL", bad)
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(1),
            "MQTTD_WATERMARK_POLL={bad} must fail the check; stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("watermark_poll_secs must be between 1 and 300"),
            "the refusal must state the range; stderr was: {stderr}"
        );
    }
    for good in ["1", "10", "300"] {
        let out = mqttd()
            .arg("--check-config")
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("MQTTD_WATERMARK_POLL", good)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "MQTTD_WATERMARK_POLL={good} must validate; stderr={}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// Issue #239: an *unsatisfiable* min-replicas floor (above the replication factor)
/// would refuse every durable write forever. `--check-config` is the pre-rollout gate,
/// so it must catch that here rather than deferring it to a broker that boots and then
/// refuses its first write. Both valid spellings — the derived `majority` posture and a
/// satisfiable integer — pass.
#[test]
fn check_config_rejects_a_min_replicas_floor_above_the_replication_factor() {
    // The ephemeral opt-in (#240) is set so the failure below is the floor's, not the
    // missing data dir's — and the assertion on the message text pins that.
    let out = mqttd()
        .arg("--check-config")
        .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
        .env("MQTTD_MIN_REPLICAS", "9")
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "an unsatisfiable floor must fail the check; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("exceeds the replication factor"),
        "stderr was: {stderr}"
    );

    for value in ["majority", "2"] {
        let out = mqttd()
            .arg("--check-config")
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("MQTTD_MIN_REPLICAS", value)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "MQTTD_MIN_REPLICAS={value} must validate; stderr={}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
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
