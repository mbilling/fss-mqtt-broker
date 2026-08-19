//! Observability: metrics, structured tracing, and a hash-chained audit log.
//!
//! Security-relevant events (auth success/failure, ACL denials, admin actions)
//! flow into a **hash-chained** audit log so that any after-the-fact tampering
//! with the record is detectable.

pub mod metrics;

/// A single audit record describing a security-relevant event.
#[derive(Debug, Clone)]
pub struct AuditEvent {
    /// Monotonic sequence number within this chain.
    pub seq: u64,
    /// Event category, e.g. "auth.success", "acl.deny", "admin.config.reload".
    pub kind: String,
    /// Subject the event pertains to (client id or operator), if any.
    pub subject: Option<String>,
    /// Human-readable detail. MUST NOT contain secrets.
    pub detail: String,
}

/// An append-only, SHA-256 hash-chained audit log (ADR 0004; ADR 0066 T3).
///
/// Each appended record's hash incorporates the previous head, so the integrity
/// of the entire chain is verifiable from the records plus the final head. The
/// verification model is **external anchoring**: every emitted record carries
/// the running head, so once a head has left the process (log shipper, SIEM),
/// rewriting any earlier record forces a recomputation the anchored heads
/// contradict. Verification needs no secret — anyone holding the records can
/// recompute the chain (SHA-256 via aws-lc-rs, the workspace's one crypto
/// provider, ADR 0053). A keyed (HMAC) variant for deployments that cannot
/// anchor heads externally ships with the SIEM export (0066-T3), where the
/// schema document explains what each mode does and does not prove.
#[derive(Debug)]
pub struct AuditChain {
    next_seq: u64,
    last_hash: [u8; 32],
}

/// Domain-separation label for the default (deterministic) genesis head.
const CHAIN_GENESIS: &[u8] = b"mqttd-audit-chain-genesis:v1";

impl Default for AuditChain {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditChain {
    /// Create an empty chain at the **deterministic** genesis head — the shape
    /// a verifier reconstructs. Boot-scoped chains (the production sink) seed
    /// via [`AuditChain::with_boot`] instead.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_seq: 0,
            last_hash: digest32(&[CHAIN_GENESIS]),
        }
    }

    /// Create an empty chain whose genesis head binds `boot_id`, so records
    /// from different boots can never be spliced into one chain: a restart is a
    /// NEW chain announced by its genesis line, distinguishable from truncation.
    #[must_use]
    pub fn with_boot(boot_id: &str) -> Self {
        Self {
            next_seq: 0,
            last_hash: digest32(&[CHAIN_GENESIS, b":boot:", boot_id.as_bytes()]),
        }
    }

    /// Append an event, advancing the chain head.
    ///
    /// The head is SHA-256 over the previous head and every field of the event,
    /// each length-prefixed (and the subject presence-tagged) so field
    /// boundaries are unambiguous — shifting bytes between kind and detail, or
    /// erasing a subject into the detail, changes the head.
    pub fn append(
        &mut self,
        kind: impl Into<String>,
        subject: Option<String>,
        detail: impl Into<String>,
    ) -> AuditEvent {
        let event = AuditEvent {
            seq: self.next_seq,
            kind: kind.into(),
            subject,
            detail: detail.into(),
        };
        self.last_hash = chain_step(&self.last_hash, &event);
        self.next_seq += 1;
        event
    }

    /// The current chain head; anchoring it externally is what makes the chain
    /// tamper-evident (see the type docs).
    #[must_use]
    pub fn head(&self) -> [u8; 32] {
        self.last_hash
    }

    /// The current head as lowercase hex — the form emitted on every record.
    #[must_use]
    pub fn head_hex(&self) -> String {
        hex(&self.last_hash)
    }
}

/// A destination for security-relevant audit events (ADR 0004 step 4).
///
/// Connection tasks record auth and authorization decisions here without
/// knowing whether the sink hash-chains them, ships them, or (in tests) buffers
/// them. `record` must be cheap and non-blocking — it is called on the hot path
/// of CONNECT/SUBSCRIBE/PUBLISH.
pub trait AuditSink: Send + Sync + std::fmt::Debug {
    /// Record one event. `subject` is the principal it pertains to (an identity
    /// or client id); `detail` MUST NOT contain secrets.
    fn record(&self, kind: &str, subject: Option<&str>, detail: &str);
}

/// The production [`AuditSink`]: appends to a tamper-evident [`AuditChain`]
/// scoped to this boot, and emits a structured `tracing` event (target
/// `audit`) carrying the boot id, sequence and running chain head — the record
/// a shipper forwards and a verifier replays.
#[derive(Debug)]
pub struct AuditLog {
    boot_id: String,
    chain: std::sync::Mutex<AuditChain>,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditLog {
    /// Create an empty audit log for this boot: a fresh random boot id, a
    /// chain seeded from it, and a genesis line announcing both — so a
    /// verifier can tell "the broker restarted" from "the tail was cut off".
    ///
    /// # Panics
    /// If the system randomness source fails — a machine on which no boot id
    /// (and no TLS key, and no token) can be trusted either.
    #[must_use]
    pub fn new() -> Self {
        let mut raw = [0u8; 16];
        aws_lc_rs::rand::fill(&mut raw).expect("system randomness for the audit boot id");
        let boot_id = hex(&raw);
        let chain = AuditChain::with_boot(&boot_id);
        tracing::info!(
            target: "audit",
            boot = %boot_id,
            head = %chain.head_hex(),
            "audit chain genesis"
        );
        Self {
            boot_id,
            chain: std::sync::Mutex::new(chain),
        }
    }

    /// This boot's chain id; a verifier reconstructs the genesis head from it
    /// via [`AuditChain::with_boot`].
    #[must_use]
    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    /// The current chain head as lowercase hex.
    #[must_use]
    pub fn head(&self) -> String {
        self.lock().head_hex()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AuditChain> {
        self.chain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl AuditSink for AuditLog {
    fn record(&self, kind: &str, subject: Option<&str>, detail: &str) {
        let (seq, head) = {
            let mut chain = self.lock();
            let event = chain.append(kind, subject.map(ToString::to_string), detail);
            (event.seq, chain.head_hex())
        };
        tracing::info!(
            target: "audit",
            boot = %self.boot_id,
            seq,
            kind,
            subject,
            head = %head,
            "{detail}"
        );
    }
}

/// A test [`AuditSink`] that buffers every event in memory.
#[derive(Debug, Default)]
pub struct RecordingAuditSink {
    events: std::sync::Mutex<Vec<AuditEvent>>,
}

impl RecordingAuditSink {
    /// Create an empty recording sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of the recorded events, in order.
    #[must_use]
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The recorded event kinds, in order — convenient for assertions.
    #[must_use]
    pub fn kinds(&self) -> Vec<String> {
        self.events().into_iter().map(|e| e.kind).collect()
    }
}

impl AuditSink for RecordingAuditSink {
    fn record(&self, kind: &str, subject: Option<&str>, detail: &str) {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let seq = events.len() as u64;
        events.push(AuditEvent {
            seq,
            kind: kind.to_string(),
            subject: subject.map(ToString::to_string),
            detail: detail.to_string(),
        });
    }
}

/// One chain step: SHA-256 over the previous head and the event, every field
/// length-prefixed and the subject presence-tagged, so field boundaries are
/// part of the hash.
fn chain_step(prev: &[u8; 32], event: &AuditEvent) -> [u8; 32] {
    let lp = |b: &[u8]| {
        let mut v = (b.len() as u64).to_be_bytes().to_vec();
        v.extend_from_slice(b);
        v
    };
    let subject = match event.subject.as_deref() {
        Some(s) => {
            let mut v = vec![1u8];
            v.extend_from_slice(&lp(s.as_bytes()));
            v
        }
        None => vec![0u8],
    };
    digest32(&[
        prev,
        &event.seq.to_be_bytes(),
        &lp(event.kind.as_bytes()),
        &subject,
        &lp(event.detail.as_bytes()),
    ])
}

/// SHA-256 over the concatenation of `parts` (aws-lc-rs, ADR 0053).
fn digest32(parts: &[&[u8]]) -> [u8; 32] {
    let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    for p in parts {
        ctx.update(p);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(ctx.finish().as_ref());
    out
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use super::{AuditChain, AuditLog, AuditSink, RecordingAuditSink};

    /// The production sink hash-chains everything it records: its head advances
    /// per event and matches a chain fed the same events directly.
    #[test]
    fn audit_log_hash_chains_recorded_events() {
        let log = AuditLog::new();
        let genesis = AuditChain::with_boot(log.boot_id()).head_hex();
        assert_eq!(
            log.head(),
            genesis,
            "empty log is at its boot's genesis head"
        );
        log.record("auth.success", Some("alice"), "CONNECT accepted");
        let after_one = log.head();
        assert_ne!(after_one, genesis);
        log.record("acl.deny.publish", Some("alice"), "topic forbidden/x");
        assert_ne!(log.head(), after_one, "each event advances the head");

        let mut reference = AuditChain::with_boot(log.boot_id());
        reference.append("auth.success", Some("alice".into()), "CONNECT accepted");
        reference.append(
            "acl.deny.publish",
            Some("alice".into()),
            "topic forbidden/x",
        );
        assert_eq!(
            log.head(),
            reference.head_hex(),
            "the sink's chain must match a directly-fed chain seeded from the same boot"
        );
    }

    /// Two boots can never be spliced into one chain: the same events under
    /// different boot ids produce disjoint heads, and a verifier reconstructs
    /// each boot's genesis from its announced id alone.
    #[test]
    fn a_restart_is_a_new_chain_not_a_truncation() {
        let mut a = AuditChain::with_boot("boot-a");
        let mut b = AuditChain::with_boot("boot-b");
        assert_ne!(a.head(), b.head(), "genesis binds the boot id");
        a.append("auth.success", None, "x");
        b.append("auth.success", None, "x");
        assert_ne!(a.head(), b.head(), "identical events, disjoint chains");
        assert_eq!(
            AuditChain::with_boot("boot-a").head(),
            {
                let c = AuditChain::with_boot("boot-a");
                c.head()
            },
            "a verifier reconstructs the genesis deterministically from the id"
        );
    }

    /// The recording sink preserves order, kinds, subjects, and details.
    #[test]
    fn recording_sink_captures_events_in_order() {
        let sink = RecordingAuditSink::new();
        sink.record("auth.failure", Some("mallory"), "bad credentials");
        sink.record("acl.deny.subscribe", None, "secret/#");

        assert_eq!(sink.kinds(), vec!["auth.failure", "acl.deny.subscribe"]);
        let events = sink.events();
        assert_eq!(events[0].subject.as_deref(), Some("mallory"));
        assert_eq!(events[0].detail, "bad credentials");
        assert_eq!(events[1].subject, None);
    }

    /// Recording through the `&dyn AuditSink` connection tasks hold is observable
    /// via a second handle to the same sink — the shape integration tests use.
    #[test]
    fn sink_records_through_a_trait_object() {
        let recorder = std::sync::Arc::new(RecordingAuditSink::new());
        let sink: std::sync::Arc<dyn AuditSink> = recorder.clone();
        sink.record("auth.success", Some("dev-7"), "mTLS");
        sink.record("acl.deny.publish", Some("dev-7"), "forbidden/x");
        assert_eq!(recorder.kinds(), vec!["auth.success", "acl.deny.publish"]);
    }

    #[test]
    fn chain_advances_and_is_order_sensitive() {
        let mut a = AuditChain::new();
        a.append("auth.success", Some("alice".into()), "login");
        a.append("acl.deny", Some("bob".into()), "publish a/b");
        let head_ab = a.head();

        let mut b = AuditChain::new();
        b.append("acl.deny", Some("bob".into()), "publish a/b");
        b.append("auth.success", Some("alice".into()), "login");

        // Different ordering of the same events yields a different head hash.
        assert_ne!(head_ab, b.head());
    }

    /// Tampering with **any** field of a recorded event — including the subject
    /// — must change the chain head. This is the property the audit log exists
    /// to provide.
    #[test]
    fn tampering_with_any_field_changes_the_head() {
        let baseline = |kind: &str, subject: Option<&str>, detail: &str| {
            let mut c = AuditChain::new();
            c.append("auth.success", Some("alice".into()), "login");
            c.append(kind, subject.map(String::from), detail);
            c.head()
        };
        let original = baseline("acl.deny", Some("bob"), "publish a/b");

        assert_ne!(original, baseline("acl.allow", Some("bob"), "publish a/b"));
        assert_ne!(original, baseline("acl.deny", Some("eve"), "publish a/b"));
        assert_ne!(original, baseline("acl.deny", None, "publish a/b"));
        assert_ne!(original, baseline("acl.deny", Some("bob"), "publish a/c"));
    }

    /// Field boundaries are part of the hash: moving bytes between kind and
    /// detail (same concatenation) must not collide.
    #[test]
    fn field_boundaries_are_unambiguous() {
        let mut a = AuditChain::new();
        a.append("ab", None, "c");
        let mut b = AuditChain::new();
        b.append("a", None, "bc");
        assert_ne!(a.head(), b.head());

        // A subject of "x" differs from no subject with "x" prepended to detail.
        let mut c = AuditChain::new();
        c.append("k", Some("x".into()), "d");
        let mut d = AuditChain::new();
        d.append("k", None, "xd");
        assert_ne!(c.head(), d.head());
    }
}
