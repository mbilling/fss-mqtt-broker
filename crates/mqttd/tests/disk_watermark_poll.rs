//! The disk watermark's SAMPLING CADENCE, end to end against the real binary
//! ([ADR 0041](../../docs/adr/0041-resource-governance.md) §5/§6, issue #243).
//!
//! `MQTTD_WATERMARK_POLL` exists so an operator can trade polling cost for detection
//! lag. A knob that is parsed, validated and then never read would satisfy every
//! `Config`-level test while changing nothing about the running broker, so the property
//! worth pinning is not "the value is accepted" but **"the configured cadence is the one
//! the watcher actually samples at"** — which only a timing assertion against the real
//! process can show.
//!
//! Why the DISK axis: the memory watcher reads RSS from `/proc/self/status`, so
//! `memory_watermark.rs` is `#![cfg(target_os = "linux")]` in its entirety and cannot
//! guard this path on a developer's macOS machine. The disk watcher `stat`s files, so it
//! works everywhere — and this test was added precisely because the Linux-only sibling
//! (`a_configured_watermark_poll_is_accepted_and_still_browns_out`) turned out to prove
//! nothing about cadence: it sets a 1 KiB mark that a live broker's stores exceed on the
//! watcher's FIRST sample, so it passes identically with the wiring reverted.
//!
//! How the crossing is staged without corrupting anything: `store_watch::scan` sizes four
//! FIXED filenames with `metadata().len()`. The test EXTENDS one of them with `set_len`,
//! which reserves no blocks (a sparse hole, so this costs no disk and no time) and leaves
//! every existing byte untouched; truncating back to the recorded original length stages
//! the recovery edge and restores the file exactly. `replicas.redb` is the target because
//! it is the cluster replica store, which a single-node broker with no peers and no
//! replication traffic creates but never reads or writes again — the size the watcher
//! `stat`s is the only thing this test perturbs.

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use proc_common::free_tcp_port;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

const DISK_BROWNOUT_ON: &str = "mqttd_brownout{axis=\"disk\"} 1";
const DISK_BROWNOUT_OFF: &str = "mqttd_brownout{axis=\"disk\"} 0";
/// Evidence that a scan has happened at all: the replica store exists on a fresh node, so
/// its size gauge carrying a non-zero value means the watcher has `stat`ed the data dir.
const SIZE_SCANNED: &str = "mqttd_store_bytes{store=\"replicas\"} 1";

/// The aggregate mark: comfortably above a fresh node's real stores, so the first sample
/// is UNDER it and the crossing below is the only thing that moves the axis.
const MARK_BYTES: u64 = 100_000_000;
/// The sparse file's size — double the mark, so one file crosses it unambiguously.
const SPARSE_BYTES: u64 = 200_000_000;

/// The deadline every cadence assertion is measured against. It must sit BETWEEN the
/// configured 1 s cadence and the 10 s default, or the test cannot tell them apart: at
/// `poll=1` a transition lands in about a second, while an unread knob leaves the default
/// 10 s cadence and blows this budget. Reverting either `main.rs` wiring site to `None`
/// is the mutation this bound exists to catch.
const CADENCE_DEADLINE: Duration = Duration::from_secs(5);

struct Broker(Child);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Boot the real binary with a disk watermark and an explicit poll cadence.
async fn start_broker(poll_secs: &str) -> (Broker, SocketAddr, tempfile::TempDir) {
    let client: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let health: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let dir = tempfile::tempdir().expect("temp data dir");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            cmd.env_remove(k);
        }
    }
    cmd.env("MQTTD_NODE_ID", "disk-poll-node")
        .env("MQTTD_PLAINTEXT_BIND", client.to_string())
        .env("MQTTD_HEALTH_BIND", health.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_DATA_DIR", dir.path())
        .env("MQTTD_STORE_MAX_BYTES", MARK_BYTES.to_string())
        .env("MQTTD_WATERMARK_POLL", poll_secs)
        .env("RUST_LOG", "off");
    let child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mqttd");
    let broker = Broker(child);

    for _ in 0..300 {
        if TcpStream::connect(client).await.is_ok() {
            return (broker, health, dir);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("mqttd never listened on {client}");
}

/// The brownout/size lines alone — what a failure needs to show.
fn brownout_lines(body: &str) -> String {
    body.lines()
        .filter(|l| l.contains("brownout") || l.contains("store_bytes"))
        .collect::<Vec<_>>()
        .join("\n")
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

/// Poll `/metrics` until `needle` appears, returning how long it took — the measurement
/// the cadence assertions are made of. Gives up at `budget` and returns `None` with the
/// last body so a failure shows what was actually exported.
async fn time_until(
    health: SocketAddr,
    needle: &str,
    budget: Duration,
) -> Result<Duration, String> {
    let started = Instant::now();
    let mut last = String::new();
    while started.elapsed() < budget {
        last = metrics(health).await;
        if last.contains(needle) {
            return Ok(started.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last
        .lines()
        .filter(|l| l.contains("brownout") || l.contains("store_bytes"))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// Stage a store-size crossing by EXTENDING an existing store file into a sparse hole.
/// Returns its path and its original length, so the caller can restore it byte-for-byte.
fn stage_crossing(dir: &std::path::Path) -> (std::path::PathBuf, u64) {
    let path = dir.join("replicas.redb");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open the replica store for a sparse extension");
    let original_len = f.metadata().expect("stat the replica store").len();
    f.set_len(SPARSE_BYTES).expect("extend the file sparsely");
    f.sync_all().expect("publish the new length to stat");
    (path, original_len)
}

/// Issue #243: the configured cadence is the cadence the DISK watcher samples at, proven
/// in both directions — engaging brownout and clearing it — because the clearing edge is
/// what decides how long a publish outage lasts after pressure is gone (ADR 0041 T11 /
/// issue #238: above the mark a `QoS` ≥ 1 publisher owing a durable append is refused).
#[tokio::test]
async fn a_configured_poll_cadence_drives_the_disk_axis_in_both_directions() {
    let (_broker, health, dir) = start_broker("1").await;

    // Barrier: wait until the watcher has actually SCANNED, proven by the per-store size
    // gauge carrying a real size. Without this barrier a fast transition below could be
    // nothing but the first scan arriving late, which would measure boot latency rather
    // than cadence. Note the brownout gauge itself cannot serve as the barrier: it is set
    // on TRANSITIONS, so a node that has never crossed the mark exports no sample for it —
    // which is also why the axis being clear is asserted by that ABSENCE here.
    let scanned = time_until(health, SIZE_SCANNED, Duration::from_secs(30))
        .await
        .unwrap_or_else(|m| panic!("the disk watcher never scanned; metrics:\n{m}"));
    let body = metrics(health).await;
    assert!(
        !body.contains(DISK_BROWNOUT_ON),
        "a fresh node's stores must sit under a 100 MB mark, so the axis must not be \
         engaged before the crossing is staged; metrics:\n{}",
        brownout_lines(&body)
    );
    eprintln!("disk watcher first scan visible after {scanned:?}");

    let (staged, original_len) = stage_crossing(dir.path());
    let engaged = time_until(health, DISK_BROWNOUT_ON, CADENCE_DEADLINE)
        .await
        .unwrap_or_else(|m| {
            panic!(
                "a 1 s cadence must notice the crossing well inside {}s — an unread \
                 MQTTD_WATERMARK_POLL leaves the 10 s default (issue #243); metrics:\n{m}",
                CADENCE_DEADLINE.as_secs()
            )
        });

    // Recovery: removing the file must clear the axis on the same cadence. This is the
    // half an operator feels as downtime — publishes are refused until it clears.
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&staged)
        .expect("reopen the replica store");
    f.set_len(original_len)
        .expect("restore the original length");
    f.sync_all().expect("publish the restored length to stat");
    drop(f);
    let cleared = time_until(health, DISK_BROWNOUT_OFF, CADENCE_DEADLINE)
        .await
        .unwrap_or_else(|m| {
            panic!(
                "a 1 s cadence must CLEAR the axis well inside {}s — the recovery edge \
                 decides how long the refusal outage lasts (issue #243); metrics:\n{m}",
                CADENCE_DEADLINE.as_secs()
            )
        });

    // Reported so a run that only just fits the budget is visible rather than silently
    // marginal; the assertions above are the guard.
    eprintln!(
        "disk axis at poll=1s: engaged in {engaged:?}, cleared in {cleared:?} \
         (budget {CADENCE_DEADLINE:?} each, default cadence would be 10s)"
    );
}
