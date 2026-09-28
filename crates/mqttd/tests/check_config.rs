//! `mqttd --check-config` (ADR 0046 T3): validates the effective config and exits without
//! binding a port. These drive the real binary — the whole point is that no listener is bound
//! and the exit code + message are the GitOps/pre-rollout contract.

use std::io::Write as _;
use std::process::Command;

/// The CI-fatal environmental skip (issue #260) — just the macro, not the whole harness.
#[path = "common/skip.rs"]
mod skip;

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

// ---------------------------------------------------------------------------------------
// Issue #671: the gate refuses what startup refuses. Each case below booted a dead broker
// behind a `config OK` before — the check stopped at `Config::load`, and binds, referenced
// files and the startup-time cross-checks were first looked at by the broker itself.
// ---------------------------------------------------------------------------------------

/// A durable-off node: everything below adds to this so the failure is for its own reason.
const BASE: &str = "[node]\nid = \"checked\"\n[durable]\nenabled = false\n";

/// Run `--check-config` on `toml` written into `dir`; `(exit code, stdout, stderr)`.
fn check(dir: &std::path::Path, toml: &str) -> (Option<i32>, String, String) {
    let path = dir.join("mqttd.toml");
    std::fs::write(&path, toml).unwrap();
    let out = mqttd()
        .arg("--check-config")
        .arg("--config")
        .arg(&path)
        .output()
        .unwrap();
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Assert the check refuses `toml` with exit 1 and an error containing `needle`.
fn assert_refused(dir: &std::path::Path, toml: &str, needle: &str) {
    let (code, stdout, stderr) = check(dir, toml);
    assert_eq!(
        code,
        Some(1),
        "must be refused ({needle}); stdout={stdout} stderr={stderr}\n--- config ---\n{toml}"
    );
    assert!(!stdout.contains("config OK"), "stdout was: {stdout}");
    assert!(stderr.contains("config INVALID"), "stderr was: {stderr}");
    assert!(
        stderr.contains(needle),
        "the refusal must name {needle:?}; stderr was: {stderr}"
    );
}

/// Assert the check passes `toml`.
fn assert_passes(dir: &std::path::Path, toml: &str) {
    let (code, stdout, stderr) = check(dir, toml);
    assert_eq!(
        code,
        Some(0),
        "must validate; stdout={stdout} stderr={stderr}\n--- config ---\n{toml}"
    );
    assert!(stdout.contains("config OK"), "stdout was: {stdout}");
}

/// Real throwaway PKI: `(ca, cert, key)` paths inside `dir`.
fn mint_pki(dir: &std::path::Path) -> (String, String, String) {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".into()])
        .unwrap()
        .signed_by(&leaf_key, &ca)
        .unwrap();
    let paths = ["ca.pem", "cert.pem", "key.pem"].map(|f| dir.join(f));
    std::fs::write(&paths[0], ca.pem()).unwrap();
    std::fs::write(&paths[1], leaf.pem()).unwrap();
    std::fs::write(&paths[2], leaf_key.serialize_pem()).unwrap();
    let [ca, cert, key] = paths.map(|p| p.display().to_string());
    (ca, cert, key)
}

/// A real Argon2id `username:phc` line.
fn password_line(user: &str, password: &str) -> String {
    use argon2::password_hash::phc::Salt;
    use argon2::{Argon2, PasswordHasher};
    let salt = Salt::new(b"fixed-salt-bytes").unwrap();
    let phc = Argon2::default()
        .hash_password_with_salt(password.as_bytes(), &salt)
        .unwrap();
    format!("{user}:{phc}\n")
}

/// The issue's first case: an unparseable bind validated, then the broker exited at
/// startup. Every bind is now resolved (nothing is bound), and the refusal names the key.
#[test]
fn an_unparseable_bind_is_refused_naming_the_key() {
    let dir = tempfile::tempdir().unwrap();
    for key in [
        "tls_bind",
        "plaintext_bind",
        "ws_bind",
        "wss_bind",
        "health_bind",
        "metrics_bind",
    ] {
        assert_refused(
            dir.path(),
            &format!("{BASE}[listeners]\n{key} = \"not-an-address\"\n"),
            &format!("listeners.{key}"),
        );
    }
    assert_refused(
        dir.path(),
        &format!("{BASE}[cluster]\npeer_bind = \"not-an-address\"\n"),
        "cluster.peer_bind",
    );
    // QUIC binds a literal address: its own parse, its own message.
    assert_refused(
        dir.path(),
        &format!(
            "{BASE}[listeners]\nquic_bind = \"not-an-address\"\n\
             [tls]\ncert = \"/nonexistent/c.pem\"\nkey = \"/nonexistent/k.pem\"\n"
        ),
        "MQTTD_QUIC_BIND",
    );
}

/// Name resolution is what `bind` itself does, so a resolvable host name is not refused —
/// the check must not be stricter than the broker.
#[test]
fn a_resolvable_bind_passes() {
    let dir = tempfile::tempdir().unwrap();
    assert_passes(
        dir.path(),
        &format!(
            "{BASE}[listeners]\nplaintext_bind = \"localhost:1883\"\n\
             health_bind = \"0.0.0.0:8080\"\n\
             [security]\nallow_anonymous = true\n"
        ),
    );
}

/// The issue's second case: a referenced file the service account cannot read. A missing
/// file is the portable form of it; the `0600 root:root` form follows on Unix.
#[test]
fn an_unreadable_referenced_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent").display().to_string();
    for (section, key, needle) in [
        ("security", "password_file", "MQTTD_PASSWORD_FILE"),
        ("security", "acl_file", "MQTTD_ACL_FILE"),
    ] {
        assert_refused(
            dir.path(),
            &format!("{BASE}[{section}]\n{key} = \"{missing}\"\n"),
            needle,
        );
    }
    assert_refused(
        dir.path(),
        &format!(
            "{BASE}[listeners]\ntls_bind = \"127.0.0.1:8883\"\n\
             [tls]\ncert = \"{missing}\"\nkey = \"{missing}\"\n"
        ),
        "MQTTD_TLS_CERT",
    );
}

/// The exact report: a password file the running user may not open. Skipped when the
/// test runs as a user that can open it anyway (root ignores mode bits).
#[cfg(unix)]
#[test]
fn a_password_file_without_read_permission_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let pw = dir.path().join("passwd");
    std::fs::write(&pw, password_line("alice", "s3cret")).unwrap();
    std::fs::set_permissions(&pw, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&pw).is_ok() {
        crate::skip_locally_or_fail_in_ci!(
            "this user can open a mode-000 file (running as root?), so an unreadable file \
             cannot be made here; run the suite as an unprivileged user"
        );
    }
    assert_refused(
        dir.path(),
        &format!("{BASE}[security]\npassword_file = \"{}\"\n", pw.display()),
        "MQTTD_PASSWORD_FILE",
    );
}

/// Readable is not enough: the material is parsed exactly as a reload parses it.
#[test]
fn malformed_referenced_material_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let garbage = dir.path().join("garbage");
    std::fs::write(&garbage, "this is not what the broker expects\n").unwrap();
    let garbage = garbage.display().to_string();
    for key in ["password_file", "acl_file"] {
        let (code, stdout, stderr) = check(
            dir.path(),
            &format!("{BASE}[security]\n{key} = \"{garbage}\"\n"),
        );
        assert_eq!(
            code,
            Some(1),
            "a malformed {key} must be refused; stdout={stdout} stderr={stderr}"
        );
    }
    let (_, cert, key) = mint_pki(dir.path());
    // A key where the certificate belongs, and a certificate where the key belongs.
    for (c, k) in [(&garbage, &key), (&cert, &garbage), (&key, &cert)] {
        let (code, stdout, stderr) = check(
            dir.path(),
            &format!(
                "{BASE}[listeners]\ntls_bind = \"127.0.0.1:8883\"\n\
                 [tls]\ncert = \"{c}\"\nkey = \"{k}\"\n"
            ),
        );
        assert_eq!(
            code,
            Some(1),
            "cert={c} key={k} must be refused; stdout={stdout} stderr={stderr}"
        );
    }
}

/// The positive control: a complete, correct secured config — TLS with client CA,
/// password file, ACL — still validates, so the new checks carry no false refusals.
#[test]
fn a_complete_secured_config_passes() {
    let dir = tempfile::tempdir().unwrap();
    let (ca, cert, key) = mint_pki(dir.path());
    let pw = dir.path().join("passwd");
    std::fs::write(&pw, password_line("alice", "s3cret")).unwrap();
    let acl = dir.path().join("acl.toml");
    std::fs::write(
        &acl,
        "[[rules]]\nactions = [\"publish\", \"subscribe\"]\ntopics = [\"#\"]\n",
    )
    .unwrap();
    assert_passes(
        dir.path(),
        &format!(
            "{BASE}[listeners]\ntls_bind = \"127.0.0.1:8883\"\nwss_bind = \"127.0.0.1:8884\"\n\
             quic_bind = \"127.0.0.1:14567\"\nhealth_bind = \"127.0.0.1:8080\"\n\
             [tls]\ncert = \"{cert}\"\nkey = \"{key}\"\nclient_ca = \"{ca}\"\n\
             [security]\npassword_file = \"{}\"\nacl_file = \"{}\"\n",
            pw.display(),
            acl.display()
        ),
    );
}

/// Cross-checks startup made after `Config::load` are made by the gate too.
#[test]
fn startup_cross_checks_are_made_by_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    // TLS listener with no certificate.
    assert_refused(
        dir.path(),
        &format!("{BASE}[listeners]\ntls_bind = \"127.0.0.1:8883\"\n"),
        "require a TLS cert and key",
    );
    // Gossip with no peer listener to advertise.
    assert_refused(
        dir.path(),
        &format!("{BASE}[cluster.swim]\nbind = \"127.0.0.1:7946\"\n"),
        "cluster.swim.bind requires cluster.peer_bind",
    );
    // OIDC over plaintext http without the test override.
    assert_refused(
        dir.path(),
        &format!("{BASE}[security.oidc]\nissuer = \"http://idp.example\"\naudience = \"mqtt\"\n"),
        "MQTTD_OIDC_ISSUER must be https",
    );
    // OIDC beside a static JWT verifier.
    let secret = dir.path().join("hs256");
    std::fs::write(&secret, "0123456789abcdef0123456789abcdef").unwrap();
    assert_refused(
        dir.path(),
        &format!(
            "{BASE}[security.oidc]\nissuer = \"https://idp.example\"\naudience = \"mqtt\"\n\
             [security.jwt]\nhs256_secret_file = \"{}\"\n",
            secret.display()
        ),
        "mutually exclusive",
    );
}
