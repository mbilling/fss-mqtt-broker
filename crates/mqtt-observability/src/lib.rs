//! Observability: metrics, structured tracing, and a tamper-evident audit log.
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

/// An append-only, hash-chained audit log.
///
/// Each appended record's hash incorporates the previous record's hash, so the
/// integrity of the entire chain can be verified from the latest hash alone.
#[derive(Debug, Default)]
pub struct AuditChain {
    next_seq: u64,
    last_hash: u64,
}

impl AuditChain {
    /// Create an empty chain with a genesis hash of zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an event, returning the new chain head hash.
    ///
    /// NOTE: this uses a placeholder non-cryptographic mixing function. The
    /// production implementation will use a keyed cryptographic hash (e.g.
    /// BLAKE3 keyed mode) — tracked in the security-depth phase.
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
        self.last_hash = mix(self.last_hash, &event);
        self.next_seq += 1;
        event
    }

    /// The current chain head hash; persisting this lets integrity be re-verified.
    #[must_use]
    pub fn head(&self) -> u64 {
        self.last_hash
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

/// The production [`AuditSink`]: appends to a tamper-evident [`AuditChain`] and
/// emits a structured `tracing` event (target `audit`) carrying the running
/// chain head, so operators can persist and later re-verify the head.
#[derive(Debug, Default)]
pub struct AuditLog {
    chain: std::sync::Mutex<AuditChain>,
}

impl AuditLog {
    /// Create an empty audit log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The current chain head hash.
    #[must_use]
    pub fn head(&self) -> u64 {
        self.lock().head()
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
            (event.seq, chain.head())
        };
        tracing::info!(target: "audit", seq, kind, subject, head, "{detail}");
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

/// Placeholder chaining function — replaced by a cryptographic hash later.
///
/// Even as a placeholder it must absorb **every** field and keep field
/// boundaries unambiguous (length-prefixed), or tampering with a subject — or
/// shifting bytes between kind and detail — would go undetected.
fn mix(prev: u64, event: &AuditEvent) -> u64 {
    let mut h = prev ^ 0x9e37_79b9_7f4a_7c15;
    h = absorb(h, event.kind.as_bytes());
    h = absorb(
        h,
        event.subject.as_deref().unwrap_or("\u{0}none").as_bytes(),
    );
    h = absorb(h, event.detail.as_bytes());
    h ^ event.seq
}

fn absorb(mut h: u64, bytes: &[u8]) -> u64 {
    h = h.rotate_left(11) ^ (bytes.len() as u64);
    for &b in bytes {
        h = h.rotate_left(5) ^ u64::from(b);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::{AuditChain, AuditLog, AuditSink, RecordingAuditSink};

    /// The production sink hash-chains everything it records: its head advances
    /// per event and matches a chain fed the same events directly.
    #[test]
    fn audit_log_hash_chains_recorded_events() {
        let log = AuditLog::new();
        assert_eq!(log.head(), 0, "empty log is at the genesis head");
        log.record("auth.success", Some("alice"), "CONNECT accepted");
        let after_one = log.head();
        assert_ne!(after_one, 0);
        log.record("acl.deny.publish", Some("alice"), "topic forbidden/x");
        assert_ne!(log.head(), after_one, "each event advances the head");

        let mut reference = AuditChain::new();
        reference.append("auth.success", Some("alice".into()), "CONNECT accepted");
        reference.append(
            "acl.deny.publish",
            Some("alice".into()),
            "topic forbidden/x",
        );
        assert_eq!(
            log.head(),
            reference.head(),
            "the sink's chain must match a directly-fed chain"
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
