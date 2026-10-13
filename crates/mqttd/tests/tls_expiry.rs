//! Expiry is visible ([ADR 0088](../../docs/adr/0088-credential-lifetimes-and-rotation.md)
//! D2), end to end against the real binary: a node with cluster-bus TLS exports the time
//! to expiry of its certificate and its CA on `/metrics`, warns in its log about a
//! certificate that is past the threshold, and after the certificate is replaced on disk
//! the gauges are the new certificate's — with no restart.
//!
//! The arithmetic, the chain and bundle rules, the revocation list and the log's rate are
//! pinned under a driven clock in `mqttd::tls_expiry`'s unit tests. What only the running
//! process can show is the wiring: that startup loads the windows, that the reload which
//! swaps the TLS contexts also refreshes them, and that the numbers are computed when
//! they are read. The broker reads the real clock, so the certificates here are minted
//! around it, with margins of days where the assertions need minutes.

mod common;
mod listen_wait;
mod proc_common;

use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant, SystemTime};

use proc_common::free_tcp_port;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

const DAY: Duration = Duration::from_hours(24);
const NODE: &str = "expiry-node";

struct Broker(Child);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A leaf for [`NODE`] under `key`, valid from `from` to `to`, issued by `ca`.
fn leaf(
    ca: &rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
    key: &rcgen::KeyPair,
    from: SystemTime,
    to: SystemTime,
) -> String {
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, NODE);
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    params.not_before = from.into();
    params.not_after = to.into();
    params.signed_by(key, ca).unwrap().pem()
}

async fn metrics(health: SocketAddr) -> String {
    let mut stream = TcpStream::connect(health).await.expect("connect to health");
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .await
        .expect("send request");
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.expect("read response");
    String::from_utf8_lossy(&body).into_owned()
}

/// The value of the sample line that starts with `series` (name and labels).
fn sample(body: &str, series: &str) -> Option<f64> {
    body.lines()
        .find_map(|l| l.strip_prefix(series)?.trim().parse().ok())
}

/// The TLS expiry lines alone: what a failure needs to show.
fn tls_lines(body: &str) -> String {
    body.lines()
        .filter(|l| l.starts_with("mqttd_tls_"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Scrape until `series` satisfies `wanted`, at most `budget`. `Err` carries the last
/// TLS lines seen.
async fn until_sample(
    health: SocketAddr,
    series: &str,
    budget: Duration,
    wanted: impl Fn(f64) -> bool,
) -> Result<(f64, String), String> {
    let started = Instant::now();
    let mut last = String::new();
    while started.elapsed() < budget {
        last = metrics(health).await;
        if let Some(v) = sample(&last, series).filter(|v| wanted(*v)) {
            return Ok((v, last));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(tls_lines(&last))
}

const PEER_SECONDS: &str = "mqttd_tls_expiry_seconds{role=\"peer\"}";
const PEER_SPENT: &str = "mqttd_tls_lifetime_spent_ratio{role=\"peer\"}";

/// A CA valid from `from` to `to`.
fn test_ca(from: SystemTime, to: SystemTime) -> rcgen::CertifiedIssuer<'static, rcgen::KeyPair> {
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "expiry-test-ca");
    params.not_before = from.into();
    params.not_after = to.into();
    rcgen::CertifiedIssuer::self_signed(params, rcgen::KeyPair::generate().unwrap()).unwrap()
}

/// Boot the real binary as a node with cluster-bus TLS from these files, a health
/// endpoint, and the file watcher at one second. Returns its log and health address.
async fn start_node(
    ca_path: &Path,
    cert_path: &Path,
    key_path: &Path,
) -> (Broker, listen_wait::Log, SocketAddr) {
    let (child, log, health) = listen_wait::spawn_listening_logged(|| {
        let client: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
        let health: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        for (k, _) in std::env::vars() {
            if k.starts_with("MQTTD_") {
                cmd.env_remove(k);
            }
        }
        cmd.env("MQTTD_NODE_ID", NODE)
            .env("MQTTD_PLAINTEXT_BIND", client.to_string())
            .env("MQTTD_HEALTH_BIND", health.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_DURABLE_SESSIONS", "0")
            .env("MQTTD_PEER_TLS_CA", ca_path)
            .env("MQTTD_PEER_TLS_CERT", cert_path)
            .env("MQTTD_PEER_TLS_KEY", key_path)
            // The file watcher: a replaced certificate reloads within a second.
            .env("MQTTD_CONFIG_WATCH", "1")
            .stderr(std::process::Stdio::null());
        (cmd, vec![client, health], health)
    })
    .await;
    (Broker(child), log, health)
}

#[tokio::test]
async fn a_node_exports_its_certificates_time_to_expiry_and_follows_a_rotation() {
    let now = SystemTime::now();
    let dir = tempfile::tempdir().expect("temp pki dir");
    let ca = test_ca(now - DAY, now + 999 * DAY);
    let key = rcgen::KeyPair::generate().unwrap();
    let write = |name: &str, body: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, body).unwrap();
        path
    };
    let ca_path = write("ca.crt", &ca.pem());
    let key_path = write("node.key", &key.serialize_pem());
    // Thirty days long with one left: 96.7% spent, far past the default two thirds.
    let cert_path = write("node.crt", &leaf(&ca, &key, now - 29 * DAY, now + DAY));
    let (_broker, log, health) = start_node(&ca_path, &cert_path, &key_path).await;

    // The certificate as loaded: a day left, nearly all of its lifetime spent.
    let (left, body) = until_sample(health, PEER_SECONDS, Duration::from_secs(30), |_| true)
        .await
        .unwrap_or_else(|m| panic!("no expiry series for the cluster certificate:\n{m}"));
    let day = DAY.as_secs_f64();
    assert!(
        left > day - 600.0 && left <= day,
        "a day left, give or take the boot: {left}\n{}",
        tls_lines(&body)
    );
    let spent = sample(&body, PEER_SPENT).expect("the fraction spent");
    assert!(
        spent > 0.96 && spent < 0.97,
        "29 of 30 days: {spent}\n{}",
        tls_lines(&body)
    );
    // The cluster CA beside it, and the threshold the node warns at.
    let ca_left = sample(&body, "mqttd_tls_expiry_seconds{role=\"peer_ca\"}").expect("the CA");
    assert!(ca_left > 998.0 * day, "{ca_left}\n{}", tls_lines(&body));
    assert_eq!(
        sample(&body, "mqttd_tls_expiry_warn_ratio"),
        Some(0.667),
        "{}",
        tls_lines(&body)
    );
    // Only what is loaded is exported, and only by role.
    for absent in ["role=\"listener\"", "role=\"admin\"", "role=\"peer_crl\""] {
        assert!(!body.contains(absent), "{absent}\n{}", tls_lines(&body));
    }
    assert!(
        !tls_lines(&body).contains(&*cert_path.to_string_lossy()),
        "a path is never a label"
    );

    // Computed when read: the same certificate has less time left a moment later.
    until_sample(health, PEER_SECONDS, Duration::from_secs(15), |v| v < left)
        .await
        .unwrap_or_else(|m| panic!("the gauge did not move between scrapes:\n{m}"));

    // The node says so in its log, naming the role and the file. (The log is coloured,
    // so a field is matched by its quoted value, not as `key=value`.)
    let warned = |text: &str| {
        text.lines()
            .filter(|l| l.contains("TLS material") && l.contains("\"cluster.peer_tls.cert\""))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    let started = Instant::now();
    while warned(&log.text()).is_empty() && started.elapsed() < Duration::from_secs(15) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let lines = warned(&log.text());
    assert_eq!(lines.len(), 1, "one warning at load:\n{}", log.tail(40));
    assert!(
        lines[0].contains("WARN")
            && lines[0].contains("\"peer\"")
            && lines[0].contains(&*cert_path.to_string_lossy())
            && lines[0].contains("has spent 96.")
            && lines[0].contains("warning threshold 66.7%"),
        "{}",
        lines[0]
    );
    assert!(
        !log.text().contains("\"peer_ca\""),
        "the CA is young: nothing is said about it\n{}",
        log.tail(40)
    );

    // Rotate: a new certificate for the same key, thirty days ahead of it.
    rotate(&cert_path, &leaf(&ca, &key, now - DAY / 24, now + 30 * DAY));
    let (left, body) = until_sample(health, PEER_SECONDS, Duration::from_secs(30), |v| {
        v > 29.0 * day
    })
    .await
    .unwrap_or_else(|m| {
        panic!(
            "the reload did not refresh the expiry gauges:\n{m}\nlog:\n{}",
            log.tail(40)
        )
    });
    assert!(left <= 30.0 * day, "{left}");
    let spent = sample(&body, PEER_SPENT).expect("the fraction spent");
    assert!(spent < 0.01, "an hour of thirty days: {spent}");
    assert!(
        body.contains("mqttd_security_reloads_total{outcome=\"ok\",trigger=\"watch\"}"),
        "it was the watcher's reload that did it"
    );
    // The new certificate is healthy: nothing more is said.
    assert_eq!(
        warned(&log.text()).len(),
        1,
        "no warning about the new certificate:\n{}",
        log.tail(40)
    );
}

/// `mqttd <mode>` over a client-facing certificate valid from `from` to `to`: its exit
/// status and its standard output.
fn check(mode: &str, from: SystemTime, to: SystemTime, extra: &[(&str, &str)]) -> (i32, String) {
    let dir = tempfile::tempdir().expect("temp pki dir");
    let ca = test_ca(from - DAY, to + 999 * DAY);
    let key = rcgen::KeyPair::generate().unwrap();
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");
    std::fs::write(&cert_path, leaf(&ca, &key, from, to)).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            cmd.env_remove(k);
        }
    }
    cmd.arg(mode)
        .env("MQTTD_DURABLE_SESSIONS", "0")
        .env("MQTTD_TLS_BIND", "127.0.0.1:8883")
        .env("MQTTD_TLS_CERT", &cert_path)
        .env("MQTTD_TLS_KEY", &key_path);
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("run mqttd");
    (
        out.status.code().expect("an exit status"),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

/// `--check-tls` and `--check-config` judge a certificate against the threshold the
/// running broker warns at, say what it is, and keep their exit statuses: a warning
/// changes neither, an expired certificate fails `--check-tls` as it always did, and
/// `--check-config` reports without refusing (the configuration is valid).
#[test]
fn the_offline_checks_apply_the_same_threshold_and_say_so() {
    let now = SystemTime::now();

    // Young: no warning from either, and both state the threshold.
    let (status, out) = check("--check-tls", now - DAY, now + 29 * DAY, &[]);
    assert_eq!(status, 0, "{out}");
    assert!(!out.contains("[warn] tls.cert: leaf"), "{out}");
    assert!(
        out.contains("[ ok ] tls.expiry_warn_permille: 667:") && out.contains("66.7%"),
        "{out}"
    );
    let (status, out) = check("--check-config", now - DAY, now + 29 * DAY, &[]);
    assert_eq!(status, 0, "{out}");
    assert!(out.contains("config OK"), "{out}");
    assert!(
        out.contains("TLS expiry: the broker warns once 66.7%")
            && out.contains("(tls.expiry_warn_permille = 667)"),
        "{out}"
    );
    assert!(!out.contains("warning:"), "{out}");

    // Past two thirds: a warning line with the fraction and the time left, exit 0.
    let (status, out) = check("--check-tls", now - 27 * DAY, now + 3 * DAY, &[]);
    assert_eq!(status, 0, "a warning does not fail the check:\n{out}");
    assert!(
        out.contains("[warn] tls.cert: leaf CN=expiry-node has spent 90.0% of its lifetime")
            && out.contains("(warning threshold 66.7%): it expires in "),
        "{out}"
    );
    assert!(out.contains("TLS check OK: "), "{out}");
    let (status, out) = check("--check-config", now - 27 * DAY, now + 3 * DAY, &[]);
    assert_eq!(status, 0, "{out}");
    assert!(
        out.contains("warning: tls.cert (")
            && out.contains("leaf CN=expiry-node has spent 90.0% of its lifetime"),
        "{out}"
    );
    // The same certificate under a later threshold: nothing to say, and it says which.
    let later = [("MQTTD_TLS_EXPIRY_WARN_PERMILLE", "950")];
    let (status, out) = check("--check-tls", now - 27 * DAY, now + 3 * DAY, &later);
    assert_eq!(status, 0, "{out}");
    assert!(!out.contains("[warn] tls.cert: leaf"), "{out}");
    assert!(out.contains("tls.expiry_warn_permille: 950:"), "{out}");
    let (_, out) = check("--check-config", now - 27 * DAY, now + 3 * DAY, &later);
    assert!(
        out.contains("warns once 95.0%") && !out.contains("warning:"),
        "{out}"
    );

    // Expired: `--check-tls` fails as before; `--check-config` reports and passes.
    let (status, out) = check("--check-tls", now - 30 * DAY, now - DAY, &[]);
    assert_eq!(status, 1, "{out}");
    assert!(
        out.contains("[FAIL] tls.cert: leaf CN=expiry-node EXPIRED 1d 0h ago")
            && out.contains("TLS check FAILED"),
        "{out}"
    );
    let (status, out) = check("--check-config", now - 30 * DAY, now - DAY, &[]);
    assert_eq!(status, 0, "{out}");
    assert!(
        out.contains("config OK") && out.contains("warning: tls.cert (") && out.contains("EXPIRED"),
        "{out}"
    );
}

/// Replace `path` with `body` in one step, as a mounted Secret or cert-manager does: the
/// watcher never reads half a file.
fn rotate(path: &Path, body: &str) {
    let staged = path.with_extension("next");
    std::fs::write(&staged, body).unwrap();
    std::fs::rename(&staged, path).unwrap();
}
