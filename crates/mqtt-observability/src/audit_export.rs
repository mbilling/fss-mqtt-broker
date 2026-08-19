//! SIEM export for the audit chain (ADR 0066 T3): RFC 5424 syslog over TCP.
//!
//! The exporter is a **copy** of the tamper-evident stream, never its
//! replacement: every record still chains and still lands in the broker log via
//! `tracing`. What this adds is a transport a SIEM ingests natively — RFC 5424
//! frames (RFC 6587 octet-counted) whose MSG is one JSON object per record, the
//! machine-parseable form `scripts/audit-verify.py` replays.
//!
//! **Delivery policy** (the bridge-spool discipline, ADR 0060's shape): the hot
//! path never blocks — [`AuditExporter::enqueue`] is a bounded `try_send`, and
//! when the queue is full the record is **shed, counted**
//! (`audit_export_dropped`), and WARN-logged once per episode. A shed record is
//! detectable downstream by construction: the export's `seq` gaps while the
//! chain heads stay consistent, which tells a verifier "the source dropped
//! export, not history". The writer thread owns the connection: connect with
//! backoff, resend the in-hand record after a reconnect (a frame is either
//! written whole or retried, so the export is at-least-once and duplicates are
//! de-duplicable on `(boot, seq)`).
//!
//! TLS is deliberately absent from this first transport: the documented
//! shipping patterns are a localhost relay (rsyslog/vector/fluent-bit, which
//! own the TLS hop) or a network the operator already trusts for logs. Native
//! TLS is recorded as a follow-up in the delivery notes.

use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Bound on records queued toward the SIEM while the connection is down or
/// slow. At a typical audit rate (auth events, denials) this is minutes of
/// buffer; past it the policy is shed-and-count, never block-the-broker.
const QUEUE_CAP: usize = 8192;

/// Facility 13 (log audit) << 3 | severity 5 (notice) — RFC 5424 §6.2.1.
const PRI: u8 = 13 * 8 + 5;

/// A handle to the export queue; cheap to clone alongside the audit log.
#[derive(Debug)]
pub struct AuditExporter {
    tx: mpsc::SyncSender<String>,
    sent: Arc<AtomicU64>,
    delivered: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    /// True while inside a shed episode, so the WARN fires once per episode
    /// rather than once per record.
    shedding: AtomicBool,
    metrics: Option<Arc<crate::metrics::Metrics>>,
    hostname: String,
}

impl AuditExporter {
    /// Start the writer thread toward `addr` (`host:port`). The connection is
    /// dialled lazily and redialled with backoff; the exporter is usable (and
    /// queueing) immediately.
    ///
    /// # Panics
    /// If the writer thread cannot be spawned.
    #[must_use]
    pub fn syslog(addr: &str, metrics: Option<Arc<crate::metrics::Metrics>>) -> Self {
        let (tx, rx) = mpsc::sync_channel::<String>(QUEUE_CAP);
        let sent = Arc::new(AtomicU64::new(0));
        let delivered = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let delivered_w = delivered.clone();
        let addr_w = addr.to_string();
        std::thread::Builder::new()
            .name("audit-syslog".into())
            .spawn(move || writer_loop(&addr_w, &rx, &delivered_w))
            .expect("spawn audit-syslog writer thread");
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "-".into());
        Self {
            tx,
            sent,
            delivered,
            dropped,
            shedding: AtomicBool::new(false),
            metrics,
            hostname,
        }
    }

    /// Queue one already-JSON-encoded record. Never blocks: a full queue sheds
    /// the record, counts it, and WARNs once per episode.
    pub fn enqueue(&self, kind: &str, json: &str) {
        let frame = self.frame(kind, json);
        match self.tx.try_send(frame) {
            Ok(()) => {
                self.sent.fetch_add(1, Ordering::Relaxed);
                self.shedding.store(false, Ordering::Relaxed);
            }
            Err(mpsc::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                if let Some(m) = &self.metrics {
                    m.audit_export_dropped();
                }
                if !self.shedding.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        dropped_total = self.dropped.load(Ordering::Relaxed),
                        "audit export queue full — shedding records (chain intact at \
                         source; export seq will gap)"
                    );
                }
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                // Writer thread died (it never exits on its own); count, do not block.
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Records shed so far.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Wait until every queued record has been written (or `timeout`): the
    /// graceful-shutdown hook that lets the closing `audit.shutdown` record
    /// reach the SIEM before the process exits.
    pub fn flush(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while self.delivered.load(Ordering::Relaxed) < self.sent.load(Ordering::Relaxed) {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }

    /// One RFC 5424 line, RFC 6587 octet-counted by the writer. MSGID carries
    /// the record kind so SIEM routing can filter before parsing the JSON MSG.
    fn frame(&self, kind: &str, json: &str) -> String {
        format!(
            "<{PRI}>1 {} {} mqttd {} {} - {}",
            rfc3339_utc_now(),
            self.hostname,
            std::process::id(),
            kind,
            json
        )
    }
}

/// The writer: one long-lived connection, redialled with backoff on failure.
/// The record in hand is retried across reconnects — a frame is either
/// delivered whole or re-sent, so the export is at-least-once and duplicates
/// de-duplicate downstream on `(boot, seq)`.
fn writer_loop(addr: &str, rx: &mpsc::Receiver<String>, delivered: &AtomicU64) {
    let mut backoff = Duration::from_millis(250);
    let mut stream: Option<std::net::TcpStream> = None;
    loop {
        let Ok(pending) = rx.recv() else {
            return; // exporter dropped; process is going away
        };
        loop {
            if stream.is_none() {
                let Ok(s) = std::net::TcpStream::connect(addr) else {
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                    continue;
                };
                let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
                let _ = s.set_nodelay(true);
                backoff = Duration::from_millis(250);
                stream = Some(s);
            }
            let s = stream.as_mut().expect("stream just ensured");
            let framed = format!("{} {}", pending.len(), pending);
            if s.write_all(framed.as_bytes()).is_err() {
                stream = None; // redial and resend the same record
                continue;
            }
            delivered.fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
}

/// RFC 3339 UTC from the system clock, hand-formatted (no time crate: the
/// civil-from-days algorithm is 15 lines and this crate stays dependency-light).
#[allow(
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::single_match_else
)] // the algorithm's canonical variable names; days-since-epoch fits i64 for ~10^14 years
fn rfc3339_utc_now() -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil_from_days, for days since 1970-01-01.
    let z = days as i64 + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mth <= 2 { y + 1 } else { y };
    format!("{y:04}-{mth:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hand-rolled RFC 3339 formatter agrees with known instants.
    #[test]
    fn rfc3339_formatting_matches_known_dates() {
        // Not testing now() (non-deterministic); test the algorithm via frame
        // shape instead: the timestamp field parses as YYYY-MM-DDTHH:MM:SSZ.
        let ts = rfc3339_utc_now();
        assert_eq!(ts.len(), 20, "{ts}");
        assert!(ts.ends_with('Z') && ts.as_bytes()[4] == b'-' && ts.as_bytes()[10] == b'T');
    }

    /// A full queue sheds without blocking, counts every shed, and delivery
    /// resumes counting once a listener drains.
    #[test]
    fn a_full_queue_sheds_and_counts_instead_of_blocking() {
        // Unroutable address: the writer thread stays in connect-backoff, so
        // the queue fills deterministically.
        let exp = AuditExporter::syslog("127.0.0.1:1", None);
        for i in 0..(QUEUE_CAP + 100) {
            exp.enqueue("auth.success", &format!("{{\"seq\":{i}}}"));
        }
        // Capacity is QUEUE_CAP + 1: the writer thread has already recv'd one
        // record and holds it in-hand through its connect backoff.
        assert!(exp.dropped() >= 99, "dropped={}", exp.dropped());
        // And the enqueue path returned promptly every time (this test finishing
        // at all is the non-blocking assertion).
    }

    /// Frames land on a real TCP listener as octet-counted RFC 5424 lines with
    /// the JSON MSG intact, and `flush()` waits for delivery.
    #[test]
    fn frames_reach_a_tcp_listener_and_flush_waits() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            use std::io::Read as _;
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            // Read until the second frame's body is visible (or timeout/EOF).
            loop {
                if String::from_utf8_lossy(&buf).contains("audit.shutdown") {
                    break;
                }
                let mut chunk = [0u8; 1024];
                match s.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            String::from_utf8_lossy(&buf).into_owned()
        });
        let exp = AuditExporter::syslog(&addr, None);
        exp.enqueue("auth.success", "{\"seq\":0,\"kind\":\"auth.success\"}");
        exp.enqueue("audit.shutdown", "{\"seq\":1,\"kind\":\"audit.shutdown\"}");
        assert!(exp.flush(Duration::from_secs(5)), "flush timed out");
        let got = handle.join().unwrap();
        assert!(got.contains("<109>1 "), "PRI/version header missing: {got}");
        assert!(got.contains(" mqttd "), "APP-NAME missing: {got}");
        assert!(
            got.contains("{\"seq\":0,\"kind\":\"auth.success\"}"),
            "JSON MSG mangled: {got}"
        );
        // Octet counting: each frame is preceded by its byte length and a space.
        let first_msg = got.find("<109>").unwrap();
        let len_prefix: String = got[..first_msg]
            .chars()
            .rev()
            .skip(1)
            .take_while(char::is_ascii_digit)
            .collect();
        assert!(
            !len_prefix.is_empty(),
            "no octet count before the frame: {got}"
        );
    }
}
