//! Async UDP driver for the [`crate::swim`] state machine.
//!
//! This is the thin I/O shell around the pure protocol: it owns a UDP socket and
//! a tick timer, feeds `tick(now)` and `handle(msg, now)` into the [`Swim`] state
//! machine, serializes outgoing datagrams, and republishes membership changes on
//! an event channel for the routing layer to consume.
//!
//! With a [`SwimAuth`] key, every outgoing datagram is sealed and every incoming
//! one is verified **before** deserialization (ADR 0003) — unauthenticated bytes
//! never reach the protocol state machine.

use crate::replay::{ReplayWindow, SeqStore, SequenceAllocator};
use crate::swim::{Action, Kind, MemberState, Message, Swim};
use crate::swim_auth::{Opened, SwimAuth};
use crate::NodeId;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, trace};

/// The per-node monotonic sequence source for outgoing v3 datagrams (ADR 0023). The driver
/// holds it behind a boxed [`SeqStore`] so the broker can inject the file-backed persistence.
pub type SeqAlloc = SequenceAllocator<Box<dyn SeqStore>>;

/// A sink for **dropped-gossip** events, called with a bounded reason class (`"auth"`,
/// `"decode"`, `"identity"`, `"replay"`, `"expired"`, `"revoked"`, `"domain"`,
/// `"cert-miss"`) each time an inbound datagram is rejected (ADR 0003). A callback rather
/// than a metrics handle keeps this crate free of any observability backend — the broker
/// passes a closure that bumps its counter.
pub type RejectCounter = std::sync::Arc<dyn Fn(&'static str) + Send + Sync>;

/// Maximum SWIM datagram size we are willing to receive.
const RECV_BUF: usize = 64 * 1024;

/// A membership change observed by SWIM, emitted to the routing layer.
#[derive(Debug, Clone)]
pub struct MembershipEvent {
    /// The member whose state changed.
    pub id: NodeId,
    /// Its SWIM datagram address.
    pub addr: String,
    /// Its inter-node routing (TCP peer-link) address; empty if not yet learned.
    pub peer_addr: String,
    /// Its new state.
    pub state: MemberState,
    /// Its self-advertised failure-domain label (ADR 0016 T5); `None` if not yet learned.
    pub domain: Option<String>,
}

/// Drive SWIM over `socket` until `shutdown` resolves.
///
/// `tick_interval` should be no larger than the configured ack timeout so probe
/// deadlines are observed promptly. Membership changes are sent on `events`.
///
/// With `auth`, datagrams failing authentication are dropped before decode
/// (logged at `debug` — per-packet `warn` would be a log-flooding lever; the
/// `reject` sink counts each drop by bounded reason instead, ADR 0003). `None`
/// runs the gossip plane unauthenticated, which the caller must opt into loudly.
///
/// When `shutdown` resolves, the node announces a **graceful leave** (ADR 0019 §2):
/// it gossips itself `Dead` directly to its peers so they drop it from the ring at
/// once, then the driver returns. Pass `std::future::pending()` for a driver that
/// only stops by being aborted.
// The driver's full I/O context: socket, state machine, timer, event sink, auth, the
// sequence allocator, the reject sink, and the shutdown signal — distinct collaborators,
// not a refactor smell.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    socket: UdpSocket,
    mut swim: Swim,
    tick_interval: Duration,
    events: mpsc::UnboundedSender<MembershipEvent>,
    auth: Option<SwimAuth>,
    mut seq_alloc: Option<SeqAlloc>,
    reject: Option<RejectCounter>,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let start = Instant::now();
    // Count one dropped-gossip event under its bounded reason class, if a sink is set.
    let count_reject = |reason: &'static str| {
        if let Some(r) = &reject {
            r(reason);
        }
    };
    let mut ticker = tokio::time::interval(tick_interval);
    let mut buf = vec![0u8; RECV_BUF];
    // Per-sender replay windows, keyed by the authenticated sender id (ADR 0023).
    let mut windows: HashMap<String, ReplayWindow> = HashMap::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => {
                // Graceful leave: announce our departure, flush it, and stop driving.
                for action in swim.leave() {
                    apply(&socket, action, &events, auth.as_ref(), &mut seq_alloc).await;
                }
                debug!("SWIM graceful leave announced; stopping driver");
                return;
            }
            _ = ticker.tick() => {
                let now = elapsed_ms(start);
                for action in swim.tick(now) {
                    apply(&socket, action, &events, auth.as_ref(), &mut seq_alloc).await;
                }
            }
            recv = socket.recv_from(&mut buf) => {
                let Ok((n, src)) = recv else { continue };
                // Authenticate before a single byte is deserialized.
                let opened = if let Some(a) = &auth {
                    match a.open(&buf[..n]) {
                        Ok(o) => o,
                        Err(reject) => {
                            debug!(%src, reason = reject.reason(),
                                "dropping SWIM datagram failing authentication");
                            count_reject(reject.reason());
                            continue;
                        }
                    }
                } else {
                    Opened { payload: &buf[..n], identity: None, seq: None, domain: None }
                };
                let Ok(msg) = bincode::deserialize::<Message>(opened.payload) else {
                    trace!(%src, "dropping undecodable SWIM datagram");
                    count_reject("decode");
                    continue;
                };
                // A signed datagram binds to its sender: the authenticated certificate
                // Common Name must equal the claimed `from`, so a node cannot gossip as
                // another (ADR 0022). Unsigned datagrams (no identity) fall through.
                if let Some(cn) = &opened.identity {
                    if cn != &msg.from {
                        debug!(%src, claimed = %msg.from, authenticated = %cn,
                            "dropping SWIM datagram whose signed identity does not match its sender");
                        count_reject("identity");
                        continue;
                    }
                }
                // Failure-domain attestation (ADR 0016 T6): in the signed posture a
                // member's label is accepted only from the member itself, and when the
                // sender's certificate attests a label it is authoritative — a
                // disagreeing self-claim is a lie and the datagram is dropped.
                let msg = match enforce_domain(msg, opened.identity.as_deref(), opened.domain.as_deref()) {
                    Ok(m) => m,
                    Err(claimed) => {
                        debug!(%src, claimed = %claimed,
                            attested = opened.domain.as_deref().unwrap_or(""),
                            "dropping SWIM datagram whose claimed failure domain contradicts its certificate");
                        count_reject("domain");
                        continue;
                    }
                };
                // Anti-replay (ADR 0023): a sequenced datagram (always authenticated, so the
                // window cannot be poisoned) is dropped if its sequence is a replay for that
                // sender. The first sequence per sender seeds the window.
                if let Some(seq) = opened.seq {
                    let fresh = windows.entry(msg.from.clone()).or_default().check_and_set(seq);
                    if !fresh {
                        debug!(%src, from = %msg.from, seq, "dropping replayed SWIM datagram");
                        count_reject("replay");
                        continue;
                    }
                }
                let now = elapsed_ms(start);
                for action in swim.handle(msg, now) {
                    apply(&socket, action, &events, auth.as_ref(), &mut seq_alloc).await;
                }
            }
        }
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

async fn apply(
    socket: &UdpSocket,
    action: Action,
    events: &mpsc::UnboundedSender<MembershipEvent>,
    auth: Option<&SwimAuth>,
    seq_alloc: &mut Option<SeqAlloc>,
) {
    match action {
        Action::Send { to, msg } => {
            // First-contact / full-state kinds carry the full certificate (0022-T6
            // priming); routine probe traffic rides on the fingerprint.
            let prime = matches!(msg.kind, Kind::Join | Kind::Sync);
            if let Ok(bytes) = bincode::serialize(&msg) {
                // With a sequence allocator configured, seal a signed+sequenced v3 datagram
                // (ADR 0023); otherwise the signed/plain seal (ADR 0022/0003).
                let datagram = match (auth, seq_alloc.as_mut()) {
                    (Some(a), Some(alloc)) => a.seal_sequenced(&bytes, alloc.allocate(), prime),
                    (Some(a), None) => a.seal(&bytes, prime),
                    (None, _) => bytes,
                };
                let _ = socket.send_to(&datagram, &to).await;
            }
        }
        Action::StateChange {
            id,
            addr,
            peer_addr,
            state,
            domain,
        } => {
            let _ = events.send(MembershipEvent {
                id,
                addr,
                peer_addr,
                state,
                domain,
            });
        }
    }
}

/// Apply the failure-domain trust rules (ADR 0016 T6) to an inbound, already
/// identity-bound message (`identity`, when present, equals `msg.from`).
///
/// In the signed posture (`identity` present):
/// - a label claimed **about a third party** is stripped — a member's label is accepted
///   only from the member itself, so a relay cannot forge topology for its peers;
/// - when the sender's certificate **attests** a label, it is authoritative: a
///   disagreeing self-claim (message header or self-update) rejects the message
///   (returning the offending claim), and the attested label replaces the self-claims —
///   so a relabel needs only a reissued certificate, no env change.
///
/// Unsigned (v1) messages pass through unchanged: the shared-key posture has no per-node
/// identity to bind labels to (its trust model already trusts any key holder for
/// everything, addresses included).
fn enforce_domain(
    mut msg: Message,
    identity: Option<&str>,
    attested: Option<&str>,
) -> Result<Message, String> {
    if identity.is_none() {
        return Ok(msg);
    }
    if let Some(att) = attested {
        if msg.from_domain.as_deref().is_some_and(|d| d != att) {
            return Err(msg.from_domain.unwrap_or_default());
        }
        msg.from_domain = Some(att.to_string());
    }
    for u in &mut msg.gossip {
        if u.id == msg.from {
            if let Some(att) = attested {
                if u.failure_domain.as_deref().is_some_and(|d| d != att) {
                    return Err(u.failure_domain.clone().unwrap_or_default());
                }
                u.failure_domain = Some(att.to_string());
            }
        } else {
            u.failure_domain = None;
        }
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::enforce_domain;
    use crate::swim::{Kind, MemberState, Message, Update};

    fn update(id: &str, domain: Option<&str>) -> Update {
        Update {
            id: id.to_string(),
            addr: format!("{id}:1"),
            peer_addr: String::new(),
            incarnation: 1,
            state: MemberState::Alive,
            suspecter: None,
            failure_domain: domain.map(str::to_string),
        }
    }

    fn msg(from: &str, from_domain: Option<&str>, gossip: Vec<Update>) -> Message {
        Message {
            from: from.to_string(),
            from_addr: format!("{from}:1"),
            from_peer_addr: String::new(),
            from_domain: from_domain.map(str::to_string),
            kind: Kind::Sync,
            gossip,
        }
    }

    #[test]
    fn an_unsigned_message_passes_through_unchanged() {
        let m = msg("a", Some("z1"), vec![update("b", Some("z2"))]);
        let out = enforce_domain(m.clone(), None, None).unwrap();
        assert_eq!(out, m);
    }

    #[test]
    fn a_signed_relay_cannot_assert_a_third_partys_domain() {
        let m = msg(
            "a",
            None,
            vec![update("b", Some("forged")), update("a", None)],
        );
        let out = enforce_domain(m, Some("a"), None).unwrap();
        assert_eq!(
            out.gossip[0].failure_domain, None,
            "third-party label stripped"
        );
    }

    #[test]
    fn an_attested_domain_overrides_and_fills_the_self_claims() {
        // No self-claim at all: the cert alone labels the sender (relabel-by-reissue).
        let m = msg("a", None, vec![update("a", None)]);
        let out = enforce_domain(m, Some("a"), Some("rack-a")).unwrap();
        assert_eq!(out.from_domain.as_deref(), Some("rack-a"));
        assert_eq!(out.gossip[0].failure_domain.as_deref(), Some("rack-a"));
    }

    #[test]
    fn a_self_claim_contradicting_the_certificate_is_rejected() {
        let m = msg("a", Some("liar"), vec![]);
        assert_eq!(
            enforce_domain(m, Some("a"), Some("rack-a")),
            Err("liar".to_string())
        );
        // The same lie inside the self-update is equally rejected.
        let m = msg("a", None, vec![update("a", Some("liar"))]);
        assert_eq!(
            enforce_domain(m, Some("a"), Some("rack-a")),
            Err("liar".to_string())
        );
    }

    #[test]
    fn an_agreeing_self_claim_passes_and_an_unattested_one_is_kept() {
        // Claim matches the cert: fine.
        let m = msg("a", Some("rack-a"), vec![]);
        assert!(enforce_domain(m, Some("a"), Some("rack-a")).is_ok());
        // No attestation in the cert: the self-claim is kept (documented residual trust).
        let m = msg("a", Some("rack-b"), vec![update("a", Some("rack-b"))]);
        let out = enforce_domain(m, Some("a"), None).unwrap();
        assert_eq!(out.from_domain.as_deref(), Some("rack-b"));
        assert_eq!(out.gossip[0].failure_domain.as_deref(), Some("rack-b"));
    }
}
