//! `mqttd --probe [/readyz|/livez]`: the health check for orchestrators that run a
//! *command* rather than performing the HTTP GET themselves.
//!
//! Kubernetes probes with `httpGet`, performed by the kubelet outside the container, so
//! the image never needed an HTTP client. Docker Compose, Podman and systemd all express
//! health as a command the container runs — and the image is distroless: no shell, no
//! `curl`, no `wget`. Without this subcommand a compose healthcheck can only run
//! `--check-config`, which passes whether or not the broker is serving anything. A health
//! check that cannot fail is worse than none: the orchestrator reports a wedged broker as
//! healthy and nothing restarts it.
//!
//! The case that earns the feature is the last one here — **`/livez` 200 while `/readyz`
//! is 503**. A node that is alive but in a minority must be pulled from rotation and
//! *not* killed, and only a probe that tells those apart can express that.

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use proc_common::free_tcp_port;
use tokio::net::TcpStream;

fn mqttd() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            c.env_remove(k);
        }
    }
    c
}

struct Broker(Child);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Boot a broker with a health endpoint; `min_members` drives whether it can be Ready
/// alone. Returns the broker and its health address.
async fn start_broker(min_members: u32) -> (Broker, SocketAddr, tempfile::TempDir) {
    let client: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let health: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let swim = format!("127.0.0.1:{}", free_tcp_port());
    let peer = format!("127.0.0.1:{}", free_tcp_port());
    let dir = tempfile::tempdir().expect("temp data dir");

    let child = mqttd()
        .env("MQTTD_NODE_ID", "probe-node")
        .env("MQTTD_PLAINTEXT_BIND", client.to_string())
        .env("MQTTD_HEALTH_BIND", health.to_string())
        .env("MQTTD_SWIM_BIND", &swim)
        .env("MQTTD_PEER_BIND", &peer)
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_READY_MIN_MEMBERS", min_members.to_string())
        .env("MQTTD_DATA_DIR", dir.path())
        .env("RUST_LOG", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mqttd");
    let broker = Broker(child);

    for _ in 0..300 {
        if TcpStream::connect(health).await.is_ok() {
            return (broker, health, dir);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("mqttd never served health on {health}");
}

/// Run `mqttd --probe <args>` and return `(exit code, stdout+stderr)`.
fn probe(args: &[&str]) -> (i32, String) {
    let out = mqttd()
        .arg("--probe")
        .args(args)
        .output()
        .expect("run --probe");
    let mut text = String::from_utf8_lossy(&out.stdout).to_string();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.code().unwrap_or(-1), text)
}

/// Poll `--probe` until it reports the expected code, so a slow lease-group formation is
/// waited out rather than raced. Returns the final `(code, output)`.
fn probe_until(args: &[&str], want: i32) -> (i32, String) {
    let mut last = (i32::MIN, String::new());
    for _ in 0..200 {
        last = probe(args);
        if last.0 == want {
            return last;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    last
}

#[tokio::test]
async fn a_ready_broker_probes_zero_and_an_unreachable_one_probes_one() {
    let (_broker, health, _dir) = start_broker(1).await;
    let url = health.to_string();

    let (code, out) = probe_until(&["--url", &url], 0);
    assert_eq!(code, 0, "a healthy single node must probe 0; got: {out}");
    assert!(out.contains("/readyz 200"), "output was: {out}");

    // The negative case, without which the assertion above proves only that the command
    // exits 0 — the failure mode this whole subcommand exists to avoid.
    let dead = format!("127.0.0.1:{}", free_tcp_port());
    let (code, out) = probe(&["--url", &dead]);
    assert_eq!(code, 1, "an unreachable endpoint must probe 1; got: {out}");
    assert!(out.contains("unreachable"), "output was: {out}");
}

/// The reason the subcommand exists: alive is not the same as ready, and the probe must
/// say which. A node below its readiness floor is serving `/livez` (do not restart me)
/// while failing `/readyz` (do not send me clients).
#[tokio::test]
async fn a_minority_node_is_live_but_not_ready() {
    // A floor of 3 with one node running: permanently a minority.
    let (_broker, health, _dir) = start_broker(3).await;
    let url = health.to_string();

    let (code, out) = probe_until(&["/livez", "--url", &url], 0);
    assert_eq!(
        code, 0,
        "the process is up, so /livez must pass; got: {out}"
    );
    assert!(out.contains("/livez 200"), "output was: {out}");

    let (code, out) = probe(&["--url", &url]);
    assert_eq!(
        code, 1,
        "a node below its readiness floor must FAIL /readyz — otherwise an orchestrator \
         keeps sending it clients; got: {out}"
    );
    assert!(
        out.contains("503") && out.contains("not ready"),
        "the failure must name the status so an operator can tell 'not ready' from \
         'unreachable'; output was: {out}"
    );
}

/// With no `MQTTD_HEALTH_BIND` and no `--url` there is nothing to probe; that is a usage
/// error (2), not a health verdict — an orchestrator must not read a misconfigured probe
/// as "unhealthy" and restart-loop the container.
#[test]
fn a_probe_with_no_endpoint_is_a_usage_error() {
    let (code, out) = probe(&[]);
    assert_eq!(code, 2, "expected a usage error; got: {out}");
    assert!(
        out.contains("MQTTD_HEALTH_BIND"),
        "the error must say how to fix it; output was: {out}"
    );
}

/// The configured bind is usually a wildcard, which is not an address you can dial. The
/// probe rewrites it to loopback rather than failing on `0.0.0.0`.
#[tokio::test]
async fn a_wildcard_health_bind_is_probed_on_loopback() {
    let port = free_tcp_port();
    let client: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let dir = tempfile::tempdir().expect("temp data dir");
    let child = mqttd()
        .env("MQTTD_NODE_ID", "wildcard-node")
        .env("MQTTD_PLAINTEXT_BIND", client.to_string())
        .env("MQTTD_HEALTH_BIND", format!("0.0.0.0:{port}"))
        .env("MQTTD_SWIM_BIND", format!("127.0.0.1:{}", free_tcp_port()))
        .env("MQTTD_PEER_BIND", format!("127.0.0.1:{}", free_tcp_port()))
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_READY_MIN_MEMBERS", "1")
        .env("MQTTD_DATA_DIR", dir.path())
        .env("RUST_LOG", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mqttd");
    let _broker = Broker(child);

    // The probe reads MQTTD_HEALTH_BIND from its own environment — exactly the situation
    // inside the container, where the broker and the healthcheck share one env.
    let mut code = -1;
    let mut text = String::new();
    for _ in 0..200 {
        let out = Command::new(env!("CARGO_BIN_EXE_mqttd"))
            .env("MQTTD_HEALTH_BIND", format!("0.0.0.0:{port}"))
            .arg("--probe")
            .output()
            .expect("run --probe");
        code = out.status.code().unwrap_or(-1);
        text = String::from_utf8_lossy(&out.stdout).to_string()
            + &String::from_utf8_lossy(&out.stderr);
        if code == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        code, 0,
        "a 0.0.0.0 bind must be probed on loopback, not dialled as a wildcard; got: {text}"
    );
}
