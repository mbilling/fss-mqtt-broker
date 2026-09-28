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
// files and the startup-time cross-checks were first looked at by the broker itself. The
// static gate takes what needs nothing from the host; `--preflight` adds the host.
// ---------------------------------------------------------------------------------------

/// A durable-off node: everything below adds to this so the failure is for its own reason.
const BASE: &str = "[node]\nid = \"checked\"\n[durable]\nenabled = false\n";

/// Which gate to run.
#[derive(Clone, Copy, Debug)]
enum Gate {
    /// `--check-config`: the config alone.
    Static,
    /// `--check-config --preflight`: the config and this host.
    Preflight,
}

/// Run the gate on `toml` written into `dir`; `(exit code, stdout, stderr)`.
fn check(gate: Gate, dir: &std::path::Path, toml: &str) -> (Option<i32>, String, String) {
    let path = dir.join("mqttd.toml");
    std::fs::write(&path, toml).unwrap();
    let mut cmd = mqttd();
    cmd.arg("--check-config").arg("--config").arg(&path);
    if matches!(gate, Gate::Preflight) {
        cmd.arg("--preflight");
    }
    let out = cmd.output().unwrap();
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Assert `gate` refuses `toml` with exit 1 and an error containing `needle`.
fn assert_refused(gate: Gate, dir: &std::path::Path, toml: &str, needle: &str) {
    let (code, stdout, stderr) = check(gate, dir, toml);
    assert_eq!(
        code,
        Some(1),
        "{gate:?} must refuse ({needle}); stdout={stdout} stderr={stderr}"
    );
    assert!(!stdout.contains("config OK"), "stdout was: {stdout}");
    assert!(stderr.contains("config INVALID"), "stderr was: {stderr}");
    assert!(
        stderr.contains(needle),
        "the refusal must name {needle:?}; stderr was: {stderr}"
    );
}

/// Assert `gate` passes `toml`.
fn assert_passes(gate: Gate, dir: &std::path::Path, toml: &str) {
    let (code, stdout, stderr) = check(gate, dir, toml);
    assert_eq!(
        code,
        Some(0),
        "{gate:?} must validate; stdout={stdout} stderr={stderr}"
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

/// A real `username:phc` password-file line, made the way an operator makes one — by
/// `mqttd --hash-password`, with the secret on stdin.
fn password_line(user: &str, secret: &[u8]) -> String {
    use std::process::Stdio;
    let mut child = mqttd()
        .arg("--hash-password")
        .arg(user)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(secret).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "--hash-password failed");
    String::from_utf8(out.stdout).unwrap()
}

/// The issue's first case: an unparseable bind validated, then the broker exited at
/// startup. Its SHAPE is config, so the static gate refuses it, naming the key.
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
        for bad in ["not-an-address", ":1883", "host:port", "/tmp/mosq.sock:0"] {
            assert_refused(
                Gate::Static,
                dir.path(),
                &format!("{BASE}[listeners]\n{key} = \"{bad}\"\n"),
                &format!("listeners.{key}"),
            );
        }
    }
    assert_refused(
        Gate::Static,
        dir.path(),
        &format!("{BASE}[cluster]\npeer_bind = \"not-an-address\"\n"),
        "cluster.peer_bind",
    );
    // QUIC binds a literal address: its own parse, its own message.
    assert_refused(
        Gate::Static,
        dir.path(),
        &format!(
            "{BASE}[listeners]\nquic_bind = \"not-an-address\"\n\
             [tls]\ncert = \"/nonexistent/c.pem\"\nkey = \"/nonexistent/k.pem\"\n"
        ),
        "MQTTD_QUIC_BIND",
    );
}

/// Host names are what `bind` resolves, so the static gate accepts any well-formed one
/// (it may only resolve inside the target cluster), and `--preflight` accepts one that
/// resolves here. Neither may be stricter than the broker.
#[test]
fn well_formed_binds_pass() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        "{BASE}[listeners]\nplaintext_bind = \"localhost:1883\"\n\
         health_bind = \"0.0.0.0:8080\"\nmetrics_bind = \"[::1]:9090\"\n\
         [security]\nallow_anonymous = true\n"
    );
    assert_passes(Gate::Static, dir.path(), &toml);
    assert_passes(Gate::Preflight, dir.path(), &toml);
    // Resolvable only elsewhere: the static gate's business is shape, not DNS; the
    // preflight's is this host, so it refuses.
    let elsewhere = format!(
        "{BASE}[listeners]\nplaintext_bind = \"mqttd-0.invalid:1883\"\n\
         [security]\nallow_anonymous = true\n"
    );
    assert_passes(Gate::Static, dir.path(), &elsewhere);
    assert_refused(
        Gate::Preflight,
        dir.path(),
        &elsewhere,
        "listeners.plaintext_bind",
    );
}

/// The issue's second case: a referenced file the checking user cannot read. The static
/// gate runs where the secrets are NOT (a deploy pipeline, the chart's CI), so it must
/// pass; `--preflight` runs where they are, so it must refuse, naming the setting.
#[test]
fn a_missing_referenced_file_is_refused_by_the_preflight_only() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent").display().to_string();
    for (toml, needle) in [
        (
            format!("{BASE}[security]\npassword_file = \"{missing}\"\n"),
            "MQTTD_PASSWORD_FILE",
        ),
        (
            format!("{BASE}[security]\nacl_file = \"{missing}\"\n"),
            "MQTTD_ACL_FILE",
        ),
        (
            format!(
                "{BASE}[listeners]\ntls_bind = \"127.0.0.1:8883\"\n\
                 [tls]\ncert = \"{missing}\"\nkey = \"{missing}\"\n"
            ),
            "MQTTD_TLS_CERT",
        ),
    ] {
        assert_passes(Gate::Static, dir.path(), &toml);
        assert_refused(Gate::Preflight, dir.path(), &toml, needle);
    }
}

/// The exact report: a password file the running user may not open.
#[cfg(unix)]
#[test]
fn a_password_file_without_read_permission_is_refused_by_the_preflight() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let pw = dir.path().join("passwd");
    std::fs::write(&pw, password_line("alice", b"s3cret")).unwrap();
    std::fs::set_permissions(&pw, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&pw).is_ok() {
        crate::skip_locally_or_fail_in_ci!(
            "this user can open a mode-000 file (running as root?), so an unreadable file \
             cannot be made here; run the suite as an unprivileged user"
        );
    }
    assert_refused(
        Gate::Preflight,
        dir.path(),
        &format!("{BASE}[security]\npassword_file = \"{}\"\n", pw.display()),
        "MQTTD_PASSWORD_FILE",
    );
}

/// Readable is not enough: `--preflight` parses the material exactly as a reload does.
#[test]
fn malformed_referenced_material_is_refused_by_the_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let garbage = dir.path().join("garbage");
    std::fs::write(&garbage, "this is not what the broker expects\n").unwrap();
    let garbage = garbage.display().to_string();
    for key in ["password_file", "acl_file"] {
        let (code, stdout, stderr) = check(
            Gate::Preflight,
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
    // Garbage for the certificate, garbage for the key, and the two swapped.
    for (case, (c, k)) in [(&garbage, &key), (&cert, &garbage), (&key, &cert)]
        .into_iter()
        .enumerate()
    {
        let (code, stdout, stderr) = check(
            Gate::Preflight,
            dir.path(),
            &format!(
                "{BASE}[listeners]\ntls_bind = \"127.0.0.1:8883\"\n\
                 [tls]\ncert = \"{c}\"\nkey = \"{k}\"\n"
            ),
        );
        assert_eq!(
            code,
            Some(1),
            "TLS material case {case} must be refused; stdout={stdout} stderr={stderr}"
        );
    }
}

/// The positive control: a complete, correct secured config — TLS with client CA, WSS,
/// QUIC, password file, ACL — passes both gates, so the new checks refuse nothing real.
#[test]
fn a_complete_secured_config_passes_both_gates() {
    let dir = tempfile::tempdir().unwrap();
    let (ca, cert, key) = mint_pki(dir.path());
    let pw = dir.path().join("passwd");
    std::fs::write(&pw, password_line("alice", b"s3cret")).unwrap();
    let acl = dir.path().join("acl.toml");
    std::fs::write(
        &acl,
        "[[rules]]\nactions = [\"publish\", \"subscribe\"]\ntopics = [\"#\"]\n",
    )
    .unwrap();
    let toml = format!(
        "{BASE}[listeners]\ntls_bind = \"127.0.0.1:8883\"\nwss_bind = \"127.0.0.1:8884\"\n\
         quic_bind = \"127.0.0.1:14567\"\nhealth_bind = \"127.0.0.1:8080\"\n\
         [tls]\ncert = \"{cert}\"\nkey = \"{key}\"\nclient_ca = \"{ca}\"\n\
         [security]\npassword_file = \"{}\"\nacl_file = \"{}\"\n",
        pw.display(),
        acl.display()
    );
    assert_passes(Gate::Static, dir.path(), &toml);
    assert_passes(Gate::Preflight, dir.path(), &toml);
}

/// Cross-checks startup made after `Config::load` are pure config, so the static gate
/// makes them.
#[test]
fn startup_cross_checks_are_made_by_the_static_gate() {
    let dir = tempfile::tempdir().unwrap();
    // TLS listener with no certificate.
    assert_refused(
        Gate::Static,
        dir.path(),
        &format!("{BASE}[listeners]\ntls_bind = \"127.0.0.1:8883\"\n"),
        "require a TLS cert and key",
    );
    // Gossip with no peer listener to advertise.
    assert_refused(
        Gate::Static,
        dir.path(),
        &format!("{BASE}[cluster.swim]\nbind = \"127.0.0.1:7946\"\n"),
        "cluster.swim.bind requires cluster.peer_bind",
    );
    // OIDC over plaintext http without the test override.
    assert_refused(
        Gate::Static,
        dir.path(),
        &format!("{BASE}[security.oidc]\nissuer = \"http://idp.example\"\naudience = \"mqtt\"\n"),
        "MQTTD_OIDC_ISSUER must be https",
    );
    // OIDC beside a static JWT verifier (the secret file need not exist: it is the
    // combination that is refused).
    assert_refused(
        Gate::Static,
        dir.path(),
        &format!(
            "{BASE}[security.oidc]\nissuer = \"https://idp.example\"\naudience = \"mqtt\"\n\
             [security.jwt]\nhs256_secret_file = \"/run/secrets/hs256\"\n"
        ),
        "mutually exclusive",
    );
}

/// SWIM startup's gossip prerequisites — rotation keys, signing, anti-replay — are made by
/// `--preflight` too, without opening the anti-replay sequence store (review of #674).
#[test]
fn gossip_prerequisites_are_made_by_the_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let swim = format!(
        "{BASE}[cluster]\npeer_bind = \"127.0.0.1:7001\"\n\
         [cluster.swim]\nbind = \"127.0.0.1:7946\"\n"
    );
    // Anti-replay with no gossip key at all.
    let replay = format!("{swim}replay = \"require\"\n");
    assert_passes(Gate::Static, dir.path(), &replay);
    assert_refused(
        Gate::Preflight,
        dir.path(),
        &replay,
        "MQTTD_SWIM_REPLAY requires",
    );
    // Anti-replay over a keyed but UNSIGNED mesh (no cluster-bus TLS to sign with).
    let key = "ab".repeat(32);
    let unsigned = format!("{swim}key = \"{key}\"\nreplay = \"require\"\n");
    assert_refused(
        Gate::Preflight,
        dir.path(),
        &unsigned,
        "requires cluster.swim.signed=require",
    );
    // Anti-replay over a keyed, SIGNED mesh (real cluster-bus PKI, so signing defaults on)
    // but with no data dir for the persisted sequence counter — the last prerequisite.
    let (ca, cert, tls_key) = mint_pki(dir.path());
    let signed = format!(
        "{BASE}[cluster]\npeer_bind = \"127.0.0.1:7001\"\n\
         [cluster.peer_tls]\nca = \"{ca}\"\ncert = \"{cert}\"\nkey = \"{tls_key}\"\n\
         [cluster.swim]\nbind = \"127.0.0.1:7946\"\nkey = \"{key}\"\nreplay = \"require\"\n"
    );
    assert_passes(Gate::Static, dir.path(), &signed);
    assert_refused(Gate::Preflight, dir.path(), &signed, "requires a data dir");
    // Rotation keys with no primary key.
    let rotation = format!("{swim}key_accept = [\"{key}\"]\n");
    assert_refused(
        Gate::Preflight,
        dir.path(),
        &rotation,
        "swim.key_accept requires swim.key",
    );
}

/// `--preflight` modifies `--check-config` and nothing else.
#[test]
fn preflight_without_check_config_is_a_usage_error_exit_2() {
    let out = mqttd().arg("--preflight").output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
}
