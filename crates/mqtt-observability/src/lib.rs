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
fn mix(prev: u64, event: &AuditEvent) -> u64 {
    let mut h = prev ^ 0x9e37_79b9_7f4a_7c15;
    for b in event.kind.bytes().chain(event.detail.bytes()) {
        h = h.rotate_left(5) ^ u64::from(b);
    }
    h ^ event.seq
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
}
