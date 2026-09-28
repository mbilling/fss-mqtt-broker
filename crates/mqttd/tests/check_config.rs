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

/// A config that already passes `Config::validate`, plus one extra fragment.
fn durable_off(extra: &str) -> String {
    format!("[node]\nid = \"checked\"\n[durable]\nenabled = false\n{extra}")
}

fn assert_check_fails(name: &str, body: &str, needles: &[&str]) {
    let path = write_tmp(name, body);
    let out = mqttd()
        .arg("--check-config")
        .arg("--config")
        .arg(&path)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(1),
        "{name} must fail the check; stdout={stdout} stderr={stderr}"
    );
    assert!(
        !stdout.contains("config OK"),
        "{name} must not report config OK; stdout={stdout}"
    );
    assert!(
        stderr.contains("config INVALID"),
        "{name}: stderr was: {stderr}"
    );
    for needle in needles {
        assert!(
            stderr.contains(needle),
            "{name}: expected {needle:?} in stderr: {stderr}"
        );
    }
    let _ = std::fs::remove_file(&path);
}

fn assert_check_ok(name: &str, body: &str) {
    let path = write_tmp(name, body);
    let out = mqttd()
        .arg("--check-config")
        .arg("--config")
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{name} must validate; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("config OK"), "{name}: stdout was: {stdout}");
    let _ = std::fs::remove_file(&path);
}

/// Issue #671: a bind string `Config::validate` accepts is refused here, the same
/// way startup refuses it, and no socket is bound. QUIC parses (`SocketAddr`) and
/// therefore also refuses a hostname the TCP listeners would resolve.
#[test]
fn check_config_rejects_an_unparseable_bind_address() {
    let cases = [
        (
            "tls",
            "[listeners]\ntls_bind = \"not-an-address\"\n",
            "listeners.tls_bind",
        ),
        (
            "plaintext",
            "[listeners]\nplaintext_bind = \"not-an-address\"\n",
            "listeners.plaintext_bind",
        ),
        (
            "ws",
            "[listeners]\nws_bind = \"not-an-address\"\n",
            "listeners.ws_bind",
        ),
        (
            "wss",
            "[listeners]\nwss_bind = \"not-an-address\"\n",
            "listeners.wss_bind",
        ),
        (
            "quic",
            "[listeners]\nquic_bind = \"not-an-address\"\n",
            "listeners.quic_bind",
        ),
        (
            "health",
            "[listeners]\nhealth_bind = \"not-an-address\"\n",
            "listeners.health_bind",
        ),
        (
            "metrics",
            "[listeners]\nmetrics_bind = \"not-an-address\"\n",
            "listeners.metrics_bind",
        ),
        (
            "peer",
            "[cluster]\npeer_bind = \"not-an-address\"\n",
            "cluster.peer_bind",
        ),
        (
            "swim",
            "[cluster.swim]\nbind = \"not-an-address\"\n",
            "cluster.swim.bind",
        ),
        (
            "quic-host",
            "[listeners]\nquic_bind = \"localhost:14567\"\n",
            "listeners.quic_bind",
        ),
    ];
    for (name, fragment, field) in cases {
        assert_check_fails(
            &format!("bind-{name}.toml"),
            &durable_off(fragment),
            &[field, "not a"],
        );
    }
}

/// Addresses startup can bind still pass: an IP socket address, a hostname the
/// TCP/UDP binders resolve, and an IPv6 socket address. Nothing is bound.
#[test]
fn check_config_accepts_a_resolvable_bind_address() {
    assert_check_ok(
        "bind-ok.toml",
        &durable_off(
            "[listeners]\n\
             plaintext_bind = \"127.0.0.1:1883\"\n\
             tls_bind = \"0.0.0.0:8883\"\n\
             ws_bind = \"localhost:8083\"\n\
             health_bind = \"[::1]:8080\"\n\
             quic_bind = \"127.0.0.1:14567\"\n\
             [cluster]\n\
             peer_bind = \"127.0.0.1:7001\"\n\
             [cluster.swim]\n\
             bind = \"127.0.0.1:7946\"\n",
        ),
    );
}

/// Issue #671: a `password_file` the process cannot open, or that does not parse,
/// fails the gate. A readable `username:hash` line still passes.
#[test]
fn check_config_rejects_an_unreadable_or_unparsed_password_file() {
    let missing =
        std::env::temp_dir().join(format!("mqttd-checkcfg-missing-pw-{}", std::process::id()));
    let _ = std::fs::remove_file(&missing);
    assert_check_fails(
        "pw-missing.toml",
        &durable_off(&format!(
            "[security]\npassword_file = \"{}\"\n",
            missing.display()
        )),
        &["MQTTD_PASSWORD_FILE", &missing.display().to_string()],
    );

    let bad = write_tmp("pw-bad.txt", "this line has no separator\n");
    assert_check_fails(
        "pw-bad.toml",
        &durable_off(&format!(
            "[security]\npassword_file = \"{}\"\n",
            bad.display()
        )),
        &["MQTTD_PASSWORD_FILE", "password line missing"],
    );
    let _ = std::fs::remove_file(&bad);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let unreadable = write_tmp("pw-mode.txt", "alice:hash\n");
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
        // Root bypasses mode bits. Only assert when this uid actually cannot open it.
        if std::fs::File::open(&unreadable).is_err() {
            assert_check_fails(
                "pw-mode.toml",
                &durable_off(&format!(
                    "[security]\npassword_file = \"{}\"\n",
                    unreadable.display()
                )),
                &["MQTTD_PASSWORD_FILE", "Permission denied"],
            );
        }
        let _ = std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o600));
        let _ = std::fs::remove_file(&unreadable);
    }

    let ok = write_tmp("pw-ok.txt", "alice:$argon2id$not-verified-at-load\n");
    assert_check_ok(
        "pw-ok.toml",
        &durable_off(&format!(
            "[security]\npassword_file = \"{}\"\n",
            ok.display()
        )),
    );
    let _ = std::fs::remove_file(&ok);
}

/// Issue #671: `[tls]` `cert` / `key` / `client_ca` (and a missing CRL) are opened and parsed
/// with the same acceptor builder startup and reload use. A readable matching pair
/// still passes, including an ACL file beside it.
#[test]
fn check_config_rejects_missing_or_unparsed_tls_material() {
    let missing = std::env::temp_dir().join(format!(
        "mqttd-checkcfg-missing-cert-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&missing);
    assert_check_fails(
        "tls-missing.toml",
        &durable_off(&format!("[tls]\ncert = \"{}\"\n", missing.display())),
        &["MQTTD_TLS_CERT", &missing.display().to_string()],
    );

    let garbage = write_tmp("tls-garbage.pem", "this is not a certificate\n");
    assert_check_fails(
        "tls-garbage.toml",
        &durable_off(&format!("[tls]\ncert = \"{}\"\n", garbage.display())),
        &["MQTTD_TLS_CERT"],
    );

    let dir = std::env::temp_dir().join(format!("mqttd-checkcfg-pki-{}", std::process::id()));
    let (ca, cert, key) = mint_pki(&dir);
    let missing_crl = dir.join("no-such.crl");
    assert_check_fails(
        "tls-crl-missing.toml",
        &durable_off(&format!(
            "[tls]\ncert = \"{}\"\nkey = \"{}\"\nclient_ca = \"{}\"\ncrl = \"{}\"\n",
            cert.display(),
            key.display(),
            ca.display(),
            missing_crl.display()
        )),
        &["MQTTD_TLS_CRL"],
    );

    let acl = write_tmp("acl-ok.toml", "default = \"deny\"\n");
    let pw = write_tmp("pw-with-tls.txt", "alice:hash\n");
    assert_check_ok(
        "tls-ok.toml",
        &durable_off(&format!(
            "[listeners]\ntls_bind = \"127.0.0.1:8883\"\n\
             [tls]\ncert = \"{}\"\nkey = \"{}\"\nclient_ca = \"{}\"\n\
             [security]\npassword_file = \"{}\"\nacl_file = \"{}\"\n",
            cert.display(),
            key.display(),
            ca.display(),
            pw.display(),
            acl.display()
        )),
    );
    let _ = std::fs::remove_file(&garbage);
    let _ = std::fs::remove_file(&acl);
    let _ = std::fs::remove_file(&pw);
    let _ = std::fs::remove_dir_all(&dir);
}

fn mint_pki(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    std::fs::create_dir_all(dir).unwrap();
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert).unwrap();
    let ca = dir.join("ca.pem");
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    std::fs::write(&ca, ca_cert.pem()).unwrap();
    std::fs::write(&cert, leaf_cert.pem()).unwrap();
    std::fs::write(&key, leaf_key.serialize_pem()).unwrap();
    (ca, cert, key)
}
