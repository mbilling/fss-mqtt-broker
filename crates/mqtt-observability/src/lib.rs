//! Observability: metrics, structured tracing, and a tamper-evident audit log.
//!
//! Security-relevant events (auth success/failure, ACL denials, admin actions)
//! flow into a **hash-chained** audit log so that any after-the-fact tampering
//! with the record is detectable.

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
    use super::AuditChain;

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
