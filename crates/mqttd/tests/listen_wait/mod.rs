//! Readiness of a spawned broker, read from the broker's own log (#827).
//!
//! A harness picks a port with `free_tcp_port`, releases it, and hands it to the broker.
//! Every test process on the machine draws from the same band, so a bare "a TCP connect
//! to the port succeeded" can be answered by **another** process's listener: another
//! suite's broker, or its port probe, while ours is still booting or has already lost the
//! port and is exiting. The test then talks to a stranger and fails with a reset.
//!
//! The broker logs one line after each listener it binds, naming the address
//! (`accepting MQTT 3.1.1 clients addr=…`, `serving health endpoints bind=…`, and so on).
//! Only our child writes our child's stdout, so seeing that line for every address is
//! proof the broker holds them. A child that exits first lost a port, and
//! [`spawn_listening`] retries on fresh ones.
#![allow(dead_code)] // each test binary uses its own subset

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How long a broker may take to bind its listeners (a cold debug binary on a loaded CI
/// runner is the slow case).
pub const BIND_TIMEOUT: Duration = Duration::from_secs(30);

/// Attempts on fresh ports before giving up.
const ATTEMPTS: usize = 3;

/// Whether `line` is the broker's own post-bind line for `addr`. The address must not be
/// followed by another digit, so `127.0.0.1:2000` does not match `127.0.0.1:20001`.
pub fn reports_bound(line: &str, addr: &str) -> bool {
    if !(line.contains("accepting ") || line.contains("serving ")) {
        return false;
    }
    line.match_indices(addr).any(|(at, _)| {
        !line[at + addr.len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit())
    })
}

/// The log lines a [`wait_bound`] reader keeps (the most recent, capped), readable while
/// the broker runs and complete once it has exited.
#[derive(Clone, Default)]
pub struct Log {
    lines: Arc<Mutex<Vec<String>>>,
    eof: Arc<std::sync::atomic::AtomicBool>,
}

/// The most lines a [`Log`] keeps.
const LOG_CAP: usize = 10_000;

impl Log {
    /// Everything kept so far, one line per line.
    pub fn text(&self) -> String {
        self.lines.lock().unwrap().join("\n")
    }

    /// The last `n` lines kept.
    pub fn tail(&self, n: usize) -> String {
        let lines = self.lines.lock().unwrap();
        lines[lines.len().saturating_sub(n)..].join("\n")
    }

    /// The whole log once the broker's stdout closes (it exited, or was killed and
    /// reaped). Waits at most `timeout`, then returns what it has.
    pub async fn complete(&self, timeout: Duration) -> String {
        let deadline = std::time::Instant::now() + timeout;
        while !self.eof.load(std::sync::atomic::Ordering::Acquire)
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.text()
    }
}

/// Wait until `child` (stdout piped, `RUST_LOG` at info for `mqttd`) has logged binding
/// every address in `addrs`. A thread drains stdout to EOF, so the broker never blocks on
/// a full pipe, and keeps the lines in the returned [`Log`]. `Err` carries the log's tail
/// when the child exits first or `timeout` passes.
///
/// # Panics
/// If the child's stdout was not piped.
pub async fn wait_bound(
    child: &mut Child,
    addrs: &[SocketAddr],
    timeout: Duration,
) -> Result<Log, String> {
    let stdout = child
        .stdout
        .take()
        .expect("the broker's stdout must be piped");
    let mut pending: Vec<String> = addrs.iter().map(ToString::to_string).collect();
    let log = Log::default();
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel::<()>();
    let writer = log.clone();
    std::thread::spawn(move || {
        let mut bound_tx = Some(bound_tx);
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if bound_tx.is_some() {
                pending.retain(|addr| !reports_bound(&line, addr));
            }
            {
                let mut lines = writer.lines.lock().unwrap();
                if lines.len() == LOG_CAP {
                    lines.remove(0);
                }
                lines.push(line);
            }
            if pending.is_empty() {
                let _ = bound_tx.take().map(|tx| tx.send(()));
            }
        }
        writer.eof.store(true, std::sync::atomic::Ordering::Release);
    });
    let outcome = match tokio::time::timeout(timeout, bound_rx).await {
        Ok(Ok(())) => return Ok(log),
        Ok(Err(_)) => "exited before binding",
        Err(_) => "did not bind in time",
    };
    Err(format!(
        "mqttd {outcome} {addrs:?}; last log lines:\n{}",
        log.tail(20)
    ))
}

/// Spawn a broker from `make` until it reports binding every address `make` returned:
/// `make` builds the command on fresh ports and returns it, the addresses to wait for,
/// and any context the caller needs back (a temp dir, the ports). Stdout is piped. When
/// the command sets no `RUST_LOG`, it gets `mqttd=info`; one it sets must pass `mqttd`'s
/// info lines. A child that exits before binding (it lost a port) is reaped and `make` is
/// called again, up to three times.
///
/// # Panics
/// When every attempt fails.
pub async fn spawn_listening<T>(make: impl FnMut() -> (Command, Vec<SocketAddr>, T)) -> (Child, T) {
    let (child, _log, ctx) = spawn_listening_logged(make).await;
    (child, ctx)
}

/// As [`spawn_listening`], also returning the broker's [`Log`], which keeps collecting.
///
/// # Panics
/// When every attempt fails.
pub async fn spawn_listening_logged<T>(
    mut make: impl FnMut() -> (Command, Vec<SocketAddr>, T),
) -> (Child, Log, T) {
    let mut failures = Vec::new();
    for attempt in 1..=ATTEMPTS {
        let (mut cmd, addrs, ctx) = make();
        if !cmd.get_envs().any(|(k, _)| k == "RUST_LOG") {
            cmd.env("RUST_LOG", "mqttd=info");
        }
        let mut child = cmd.stdout(Stdio::piped()).spawn().expect("spawn mqttd");
        match wait_bound(&mut child, &addrs, BIND_TIMEOUT).await {
            Ok(log) => return (child, log, ctx),
            Err(why) => {
                let _ = child.kill();
                let _ = child.wait();
                eprintln!("attempt {attempt}: {why}");
                failures.push(why);
            }
        }
    }
    panic!(
        "mqttd failed to bind in {ATTEMPTS} attempts on fresh ports:\n{}",
        failures.join("\n---\n")
    );
}

/// As [`wait_bound`], for a broker whose stdout goes to the file at `path` (a harness that
/// reads the log while the broker runs). Polls the file; `Err` when the child exits first
/// or `timeout` passes.
pub async fn wait_logged_file(
    child: &mut Child,
    path: &std::path::Path,
    addrs: &[SocketAddr],
    timeout: Duration,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;
    let wanted: Vec<String> = addrs.iter().map(ToString::to_string).collect();
    loop {
        let log = std::fs::read_to_string(path).unwrap_or_default();
        if wanted
            .iter()
            .all(|addr| log.lines().any(|line| reports_bound(line, addr)))
        {
            return Ok(());
        }
        let outcome = match child.try_wait() {
            Ok(Some(status)) => format!("exited ({status}) before binding"),
            _ if std::time::Instant::now() >= deadline => "did not bind in time".to_string(),
            _ => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let lines: Vec<&str> = log.lines().collect();
        return Err(format!(
            "mqttd {outcome} {addrs:?}; last log lines:\n{}",
            lines[lines.len().saturating_sub(20)..].join("\n")
        ));
    }
}
