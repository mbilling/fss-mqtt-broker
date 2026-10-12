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
//!
//! Everything but the socket and the timer lives in [`Gossip`], which has no I/O: the
//! inbound pipeline, the outbound seal, and the three rules that decide when a peer is
//! shown this node's certificate (issue #971, ADR 0022's 2026-10-12 amendment) — on
//! the first datagram to it, again when it restarts, and on request when this node
//! drops a datagram as `cert-miss`.

use crate::replay::{ReplayWindow, SeqStore, SequenceAllocator};
use crate::swim::{Action, Kind, MemberState, Message, Swim};
use crate::swim_auth::{OpenReject, Opened, SwimAuth};
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
/// `"cert-miss"`, `"cluster-mismatch"`) each time an inbound datagram is rejected (ADR 0003). A callback rather
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

/// A sink called once per **certificate request** this node sends: a `Join` greeting
/// to the source of a datagram it had to drop as `cert-miss` (issue #971). A callback
/// for the same reason as [`RejectCounter`].
pub type CertRequestCounter = std::sync::Arc<dyn Fn() + Send + Sync>;

/// At most one certificate request per source address in this many milliseconds: the
/// protocol period, so a peer whose `Sync` reply was lost is asked again on its next
/// datagram a round later, and a flood from one address draws one reply per second.
const CERT_REQUEST_PER_SOURCE_MS: u64 = 1000;
/// At most this many certificate requests per [`CERT_REQUEST_PER_SOURCE_MS`] window in
/// total, whatever the number of sources: the ceiling on what a holder of the gossip
/// key who forges source addresses can make this node emit. A node that restarts
/// into a cluster of N hears from at most N−1 members, a few per second.
const CERT_REQUEST_BURST: u32 = 16;
/// Bound on the per-source table. Sources are remembered only for one window, so a
/// full table means more than this many distinct sources inside a second — far more
/// than [`CERT_REQUEST_BURST`] would serve anyway; new sources are then refused.
const CERT_REQUEST_SOURCES: usize = 256;

/// The limiter on certificate requests (issue #971): decides whether a datagram dropped
/// as `cert-miss` may be answered with a `Join` to its source.
///
/// What it bounds. A `cert-miss` datagram has passed the cluster-key HMAC — it was
/// sealed by a holder of the gossip key, or is a replay of a datagram that was — but
/// nothing binds it to its UDP source: the per-node signature cannot be checked
/// without the certificate, which is the thing missing. So the reply goes to an
/// address the sender chose. The limiter caps that reflection in rate (one per source
/// per second, [`CERT_REQUEST_BURST`] per second overall) and the table in memory
/// ([`CERT_REQUEST_SOURCES`] entries, each one address string and a timestamp).
#[derive(Debug, Default)]
pub struct CertRequests {
    /// Source address → when it was last sent a request (driver clock, ms).
    last: HashMap<String, u64>,
    /// Start of the current burst window and the requests granted in it.
    window_start: u64,
    in_window: u32,
}

impl CertRequests {
    /// Whether a certificate request may be sent to `src` at `now` (ms). A `true`
    /// is recorded as sent.
    pub fn allow(&mut self, src: &str, now: u64) -> bool {
        if now.saturating_sub(self.window_start) >= CERT_REQUEST_PER_SOURCE_MS {
            self.window_start = now;
            self.in_window = 0;
        }
        if self.in_window >= CERT_REQUEST_BURST {
            return false;
        }
        if let Some(at) = self.last.get(src) {
            if now.saturating_sub(*at) < CERT_REQUEST_PER_SOURCE_MS {
                return false;
            }
        } else if self.last.len() >= CERT_REQUEST_SOURCES {
            // Forget every source whose interval has passed; refuse if none has.
            self.last
                .retain(|_, at| now.saturating_sub(*at) < CERT_REQUEST_PER_SOURCE_MS);
            if self.last.len() >= CERT_REQUEST_SOURCES {
                return false;
            }
        }
        self.last.insert(src.to_string(), now);
        self.in_window += 1;
        true
    }
}

/// Something the gossip endpoint wants done: a datagram put on the wire, or a
/// membership change handed to the routing layer.
#[derive(Debug)]
pub enum Output {
    /// Send `bytes` (already sealed) to datagram address `to`.
    Datagram {
        /// Destination SWIM address.
        to: String,
        /// The sealed datagram.
        bytes: Vec<u8>,
    },
    /// A membership change for the routing layer.
    Event(MembershipEvent),
}

/// One node's gossip endpoint without the socket: the [`Swim`] state machine behind
/// the whole inbound pipeline (authenticate → decode → bind the identity → failure
/// domain → anti-replay → cluster guard) and the outbound seal. [`run`] is the UDP
/// and timer shell around it; the formation simulation (`tests/swim_formation_sim.rs`)
/// drives the very same pipeline over a virtual clock and an in-memory network, which
/// is the point of keeping it free of I/O (ADR 0024).
pub struct Gossip {
    swim: Swim,
    auth: Option<SwimAuth>,
    seq_alloc: Option<SeqAlloc>,
    reject: Option<RejectCounter>,
    cert_requested: Option<CertRequestCounter>,
    identity: Option<std::sync::Arc<crate::cluster_identity::ClusterIdentity>>,
    /// Per-sender replay windows, keyed by the authenticated sender id (ADR 0023).
    windows: HashMap<String, ReplayWindow>,
    cert_requests: CertRequests,
}

impl std::fmt::Debug for Gossip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gossip")
            .field("local", self.swim.local())
            .finish_non_exhaustive()
    }
}

impl Gossip {
    /// An endpoint for `swim`. With `auth`, datagrams are sealed on the way out and
    /// verified before decode on the way in (ADR 0003); with `seq_alloc` too, they are
    /// sequenced (ADR 0023).
    #[must_use]
    pub fn new(swim: Swim, auth: Option<SwimAuth>, seq_alloc: Option<SeqAlloc>) -> Self {
        Self {
            swim,
            auth,
            seq_alloc,
            reject: None,
            cert_requested: None,
            identity: None,
            windows: HashMap::new(),
            cert_requests: CertRequests::default(),
        }
    }

    /// Count dropped datagrams by bounded reason (ADR 0003-T6).
    #[must_use]
    pub fn with_reject_counter(mut self, reject: Option<RejectCounter>) -> Self {
        self.reject = reject;
        self
    }

    /// Count certificate requests sent (issue #971).
    #[must_use]
    pub fn with_cert_request_counter(mut self, counter: Option<CertRequestCounter>) -> Self {
        self.cert_requested = counter;
        self
    }

    /// Stamp, check and adopt the cluster identity (ADR 0054 T2).
    #[must_use]
    pub fn with_identity(
        mut self,
        identity: Option<std::sync::Arc<crate::cluster_identity::ClusterIdentity>>,
    ) -> Self {
        self.identity = identity;
        self
    }

    /// The state machine, for reading the membership view.
    #[must_use]
    pub fn swim(&self) -> &Swim {
        &self.swim
    }

    /// Advance the protocol clock to `now` (ms).
    pub fn tick(&mut self, now: u64) -> Vec<Output> {
        let actions = self.swim.tick(now);
        self.outputs(actions)
    }

    /// Announce a graceful leave (ADR 0019 §2).
    pub fn leave(&mut self) -> Vec<Output> {
        let actions = self.swim.leave();
        self.outputs(actions)
    }

    fn count_reject(&self, reason: &'static str) {
        if let Some(r) = &self.reject {
            r(reason);
        }
    }

    /// Seal and serialize the state machine's actions.
    fn outputs(&mut self, actions: Vec<Action>) -> Vec<Output> {
        // A member that restarted lost every certificate it had cached, ours with the
        // rest (issue #971): un-prime it BEFORE sealing, so the first datagram it gets
        // from us — usually among `actions`, the reply to the datagram that showed us
        // its new life — carries our certificate, and it never has to drop one.
        let restarted = self.swim.take_new_lives();
        if let Some(a) = &self.auth {
            for addr in &restarted {
                a.unprime(addr);
            }
        }
        let mut out = Vec::with_capacity(actions.len());
        for action in actions {
            match action {
                Action::Send { to, mut msg } => {
                    // Stamp the cluster identity at send time (ADR 0054 T2): the pure
                    // state machine never learns it; every outgoing datagram carries it
                    // once this node knows one.
                    if let Some(identity) = &self.identity {
                        msg.cluster_id = identity.get();
                    }
                    // First-contact / full-state kinds carry the full certificate
                    // (0022-T6 priming), and so does the first datagram to each
                    // destination (issue #971, `SwimAuth::seal_to`); routine probe
                    // traffic rides on the fingerprint.
                    let prime = matches!(msg.kind, Kind::Join | Kind::Sync);
                    let Ok(bytes) = postcard::to_allocvec(&msg) else {
                        continue;
                    };
                    // With a sequence allocator configured, a signed+sequenced v3
                    // datagram (ADR 0023); otherwise the signed/plain seal (ADR 0022/0003).
                    let bytes = match &self.auth {
                        Some(a) => {
                            let seq = self.seq_alloc.as_mut().map(SequenceAllocator::allocate);
                            a.seal_to(&to, &bytes, seq, prime)
                        }
                        None => bytes,
                    };
                    out.push(Output::Datagram { to, bytes });
                }
                Action::StateChange {
                    id,
                    addr,
                    peer_addr,
                    state,
                    domain,
                } => out.push(Output::Event(MembershipEvent {
                    id,
                    addr,
                    peer_addr,
                    state,
                    domain,
                })),
            }
        }
        out
    }

    /// Handle one inbound datagram that arrived from UDP source `src` at `now` (ms).
    ///
    /// With authentication configured, nothing in a datagram reaches the state machine
    /// unless it passes every gate below. The one thing a datagram can cause *without*
    /// passing them all is a certificate request (issue #971): a datagram that carries
    /// a valid cluster-key HMAC and names, by fingerprint, a certificate this node has
    /// not cached is dropped as before — its payload is never decoded, no membership
    /// statement in it is read — and its source is sent one `Join`, subject to
    /// [`CertRequests`]. A datagram that fails the HMAC causes nothing at all.
    // The driver's whole inbound pipeline: sequential gates, not extractable complexity.
    #[allow(clippy::too_many_lines)]
    pub fn receive(&mut self, datagram: &[u8], src: &str, now: u64) -> Vec<Output> {
        // Authenticate before a single byte is deserialized.
        let opened = if let Some(a) = &self.auth {
            match a.open(datagram) {
                Ok(o) => o,
                Err(reject) => {
                    debug!(%src, reason = reject.reason(),
                        "dropping SWIM datagram failing authentication");
                    self.count_reject(reject.reason());
                    if reject == OpenReject::UnknownCert && self.cert_requests.allow(src, now) {
                        // The sender is (or replays) a key-holder whose certificate we
                        // do not hold. Ask for it the way a joiner does: our `Join`
                        // carries our full certificate, and the `Sync` it draws carries
                        // theirs — in every release that signs gossip, so a v1.1.0
                        // peer answers it too. Until then we drop everything it sends.
                        debug!(%src, "requesting the certificate of a cert-miss sender");
                        if let Some(c) = &self.cert_requested {
                            c();
                        }
                        let greet = self.swim.greet(src);
                        return self.outputs(vec![greet]);
                    }
                    return Vec::new();
                }
            }
        } else {
            Opened {
                payload: datagram,
                identity: None,
                seq: None,
                domain: None,
            }
        };
        let Some(msg) = Message::decode(opened.payload) else {
            trace!(%src, "dropping undecodable SWIM datagram");
            self.count_reject("decode");
            return Vec::new();
        };
        // A signed datagram binds to its sender: the authenticated certificate
        // Common Name must equal the claimed `from`, so a node cannot gossip as
        // another (ADR 0022). Unsigned datagrams (no identity) fall through.
        if let Some(cn) = &opened.identity {
            if cn != &msg.from {
                debug!(%src, claimed = %msg.from, authenticated = %cn,
                    "dropping SWIM datagram whose signed identity does not match its sender");
                self.count_reject("identity");
                return Vec::new();
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
                self.count_reject("domain");
                return Vec::new();
            }
        };
        // Anti-replay (ADR 0023): a sequenced datagram (always authenticated, so the
        // window cannot be poisoned) is dropped if its sequence is a replay for that
        // sender. The first sequence per sender seeds the window.
        if let Some(seq) = opened.seq {
            let fresh = self
                .windows
                .entry(msg.from.clone())
                .or_default()
                .check_and_set(seq);
            if !fresh {
                debug!(%src, from = %msg.from, seq, "dropping replayed SWIM datagram");
                self.count_reject("replay");
                return Vec::new();
            }
        }
        // Cluster identity (ADR 0054 T2): gossip from a separately-founded
        // cluster is CONTAINED, not merged — a datagram carrying a different
        // identity than ours is dropped and counted. A joiner that knows no
        // identity yet ADOPTS the first authenticated one it hears (and
        // persists it); a sender that carries none (another pre-adoption
        // joiner, or a pre-0054 build) falls through — authentication
        // already gated it into the cluster.
        if let Some(identity) = &self.identity {
            if let Some(theirs) = &msg.cluster_id {
                match identity.get() {
                    Some(ours) if *theirs != ours => {
                        debug!(%src, from = %msg.from, theirs = %theirs, ours = %ours,
                            "dropping SWIM datagram from a FOREIGN cluster (split-brain contained)");
                        self.count_reject("cluster-mismatch");
                        return Vec::new();
                    }
                    None => {
                        if identity.adopt(theirs) {
                            tracing::info!(cluster_id = %theirs, learned_from = %msg.from,
                                "adopted the cluster identity from first authenticated contact (ADR 0054)");
                        }
                    }
                    Some(_) => {}
                }
            }
        }
        // Learn the direct sender's gossip address from where the datagram
        // ACTUALLY came from, not from its self-claimed `from_addr` (2026-07-20
        // post-mortem). A node that binds/advertises `0.0.0.0:<port>` (the k8s
        // default) would otherwise be recorded at an unroutable address, so every
        // gossip reply and dissemination about it is black-holed — the founder
        // learns joiners from their greets but no one can gossip back, and joiners
        // stay isolated (`members=[self]`). The UDP source is authoritative for
        // the peer we are talking to directly; gossip *about* third parties keeps
        // its relayed address until we hear from them first-hand. `from_peer_addr`
        // (the TCP peer-link address) is NOT derivable from a UDP source and stays
        // as advertised. Applies in every posture: even signed datagrams bind only
        // `from` (the id) to the certificate, never the address.
        let mut msg = msg;
        msg.from_addr = src.to_string();
        let actions = self.swim.handle(msg, now);
        self.outputs(actions)
    }
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
    swim: Swim,
    tick_interval: Duration,
    events: mpsc::UnboundedSender<MembershipEvent>,
    auth: Option<SwimAuth>,
    seq_alloc: Option<SeqAlloc>,
    reject: Option<RejectCounter>,
    identity: Option<std::sync::Arc<crate::cluster_identity::ClusterIdentity>>,
    isolated: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let gossip = Gossip::new(swim, auth, seq_alloc)
        .with_reject_counter(reject)
        .with_identity(identity);
    run_gossip(socket, gossip, tick_interval, events, isolated, shutdown).await;
}

/// [`run`] over an endpoint the caller assembled — for a caller that attaches more
/// than `run`'s parameters carry (the certificate-request counter).
pub async fn run_gossip(
    socket: UdpSocket,
    mut gossip: Gossip,
    tick_interval: Duration,
    events: mpsc::UnboundedSender<MembershipEvent>,
    isolated: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let start = Instant::now();
    // Publish isolation flips (issue #368): the flag is read by /statusz and the
    // metrics gauge, and the transition logs are the operator's only WARN-level
    // trace of a one-way network failure — the state machine's own view keeps
    // looking healthy under exactly that failure, which is the point.
    let mut was_isolated = false;
    let publish_isolation = |swim: &Swim, was: &mut bool| {
        let now_isolated = swim.isolated();
        if now_isolated != *was {
            *was = now_isolated;
            if let Some(flag) = &isolated {
                flag.store(now_isolated, std::sync::atomic::Ordering::Relaxed);
            }
            if now_isolated {
                tracing::warn!(
                    failed_probe_rounds = swim.failed_probe_rounds(),
                    "SWIM isolation: this node's own probes are going unanswered while \
                     inbound gossip still arrives — the local membership view is \
                     UNCONFIRMED (one-way network failure?); peers are likely evicting \
                     this node (issue #368)"
                );
            } else {
                tracing::warn!("SWIM isolation cleared: a probe of ours was acked again");
            }
        }
    };
    let mut ticker = tokio::time::interval(tick_interval);
    let mut buf = vec![0u8; RECV_BUF];
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => {
                // Graceful leave: announce our departure, flush it, and stop driving.
                let outputs = gossip.leave();
                emit(&socket, &events, outputs).await;
                debug!("SWIM graceful leave announced; stopping driver");
                return;
            }
            _ = ticker.tick() => {
                let outputs = gossip.tick(elapsed_ms(start));
                emit(&socket, &events, outputs).await;
                // Probe rounds conclude on ticks — isolation can only TRIP here.
                publish_isolation(gossip.swim(), &mut was_isolated);
            }
            recv = socket.recv_from(&mut buf) => {
                let Ok((n, src)) = recv else { continue };
                let outputs = gossip.receive(&buf[..n], &src.to_string(), elapsed_ms(start));
                emit(&socket, &events, outputs).await;
                // Acks land in handle() — isolation can only CLEAR here.
                publish_isolation(gossip.swim(), &mut was_isolated);
            }
        }
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Put the endpoint's outputs on the socket and the event channel, in order.
async fn emit(
    socket: &UdpSocket,
    events: &mpsc::UnboundedSender<MembershipEvent>,
    outputs: Vec<Output>,
) {
    for output in outputs {
        match output {
            Output::Datagram { to, bytes } => {
                let _ = socket.send_to(&bytes, &to).await;
            }
            Output::Event(event) => {
                let _ = events.send(event);
            }
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
            generation: 1,
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
            from_generation: 1,
            cluster_id: None,
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

/// The gossip endpoint's certificate requests (issue #971): what a `cert-miss` datagram
/// can and cannot cause, and that it heals the pair the issue describes.
#[cfg(test)]
mod cert_request_tests {
    use super::{
        CertRequests, Gossip, Output, CERT_REQUEST_BURST, CERT_REQUEST_PER_SOURCE_MS,
        CERT_REQUEST_SOURCES,
    };
    use crate::swim::{Config, Kind, MemberState, Message, Swim, Update};
    use crate::swim_auth::{
        GossipSign, GossipVerify, OpenReject, SwimAuth, VerifiedIdentity, KEY_LEN,
    };
    use crate::NodeId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// A real leaf's size, so the request's size is an honest number.
    const CERT_LEN: usize = 560;
    /// A 1500-byte MTU less the IPv6 and UDP headers.
    const MTU_BUDGET: usize = 1500 - 40 - 8;

    struct Signer {
        cn: String,
        cert: Vec<u8>,
    }
    fn sig(cn: &str, payload: &[u8]) -> Vec<u8> {
        let mut s = format!("sig:{cn}:").into_bytes();
        s.extend(payload.iter().take(48));
        s.resize(64, 0);
        s
    }
    impl GossipSign for Signer {
        fn cert_der(&self) -> &[u8] {
            &self.cert
        }
        fn sign(&self, payload: &[u8]) -> Vec<u8> {
            sig(&self.cn, payload)
        }
    }
    struct Verifier;
    impl GossipVerify for Verifier {
        fn verify(
            &self,
            cert_der: &[u8],
            payload: &[u8],
            signature: &[u8],
        ) -> Result<VerifiedIdentity, OpenReject> {
            let rest = cert_der.strip_prefix(b"cert:").ok_or(OpenReject::Auth)?;
            let end = rest.iter().position(|b| *b == 0).ok_or(OpenReject::Auth)?;
            let cn = std::str::from_utf8(&rest[..end]).map_err(|_| OpenReject::Auth)?;
            if signature == sig(cn, payload) {
                Ok(VerifiedIdentity {
                    cn: cn.to_string(),
                    failure_domain: None,
                })
            } else {
                Err(OpenReject::Auth)
            }
        }
    }

    /// Node `cn`'s signing context on cluster key `key`.
    fn auth_on(key: u8, cn: &str) -> SwimAuth {
        let mut cert = format!("cert:{cn}\0").into_bytes();
        cert.resize(CERT_LEN, 0x5A);
        SwimAuth::new(&[key; KEY_LEN]).with_signing(
            Arc::new(Signer {
                cn: cn.to_string(),
                cert,
            }),
            Arc::new(Verifier),
        )
    }
    fn auth(cn: &str) -> SwimAuth {
        auth_on(7, cn)
    }

    fn cfg() -> Config {
        Config {
            protocol_period_ms: 100,
            ack_timeout_ms: 20,
            suspicion_timeout_ms: 5000,
            suspicion_min_timeout_ms: 5000,
            dead_ttl_ms: 2000,
            indirect_probes: 1,
            ..Config::default()
        }
    }

    fn swim(id: &str) -> Swim {
        Swim::new(
            NodeId(id.to_string()),
            format!("{id}:1"),
            format!("{id}:7001"),
            None,
            1,
            cfg(),
            Vec::new(),
        )
    }

    type Counts = Arc<Mutex<HashMap<&'static str, u64>>>;

    /// A current endpoint for node `id`, with its reject and request counters.
    fn endpoint(id: &str) -> (Gossip, Counts) {
        let counts: Counts = Arc::new(Mutex::new(HashMap::new()));
        let (rejects, requests) = (counts.clone(), counts.clone());
        let gossip = Gossip::new(swim(id), Some(auth(id)), None)
            .with_reject_counter(Some(Arc::new(move |reason| {
                *rejects.lock().unwrap().entry(reason).or_default() += 1;
            })))
            .with_cert_request_counter(Some(Arc::new(move || {
                *requests.lock().unwrap().entry("requested").or_default() += 1;
            })));
        (gossip, counts)
    }
    fn count(counts: &Counts, what: &'static str) -> u64 {
        counts.lock().unwrap().get(what).copied().unwrap_or(0)
    }

    fn update(id: &str, state: MemberState) -> Update {
        Update {
            id: id.to_string(),
            addr: format!("{id}:1"),
            peer_addr: format!("{id}:7001"),
            incarnation: 1,
            generation: 1,
            state,
            suspecter: None,
            failure_domain: None,
        }
    }

    fn message(from: &str, kind: Kind, gossip: Vec<Update>) -> Vec<u8> {
        postcard::to_allocvec(&Message {
            from: from.to_string(),
            from_addr: format!("{from}:1"),
            from_peer_addr: format!("{from}:7001"),
            from_domain: None,
            from_generation: 1,
            kind,
            gossip,
            cluster_id: None,
        })
        .unwrap()
    }

    /// A wire peer played by hand: node `id`'s signing context, with its every-16th
    /// slot already spent so `fp()` really is the fingerprint form.
    struct Wire {
        id: String,
        auth: SwimAuth,
    }
    impl Wire {
        fn new(id: &str) -> Self {
            let auth = auth(id);
            let _ = auth.seal(b"spend the first (full-certificate) slot", false);
            Self {
                id: id.to_string(),
                auth,
            }
        }
        /// Sealed with the full certificate (as a `Join`/`Sync` is).
        fn full(&self, kind: Kind, gossip: Vec<Update>) -> Vec<u8> {
            self.auth.seal(&message(&self.id, kind, gossip), true)
        }
        /// Sealed with the fingerprint only (as a ping or an ack is).
        fn fp(&self, kind: Kind, gossip: Vec<Update>) -> Vec<u8> {
            self.fp_of(&message(&self.id, kind, gossip))
        }
        /// `payload` sealed with the fingerprint only.
        fn fp_of(&self, payload: &[u8]) -> Vec<u8> {
            // Every 16th seal carries the full certificate (0022-T6); skip that one.
            loop {
                let sealed = self.auth.seal(payload, false);
                if sealed.len() < CERT_LEN {
                    return sealed;
                }
            }
        }
        /// Open and decode a datagram addressed to this peer.
        fn read(&self, datagram: &[u8]) -> Message {
            Message::decode(self.auth.open(datagram).expect("opens").payload).expect("decodes")
        }
    }

    fn datagrams(outputs: Vec<Output>) -> Vec<(String, Vec<u8>)> {
        outputs
            .into_iter()
            .filter_map(|o| match o {
                Output::Datagram { to, bytes } => Some((to, bytes)),
                Output::Event(_) => None,
            })
            .collect()
    }

    fn state_of(g: &Gossip, id: &str) -> Option<MemberState> {
        g.swim()
            .members()
            .into_iter()
            .find(|m| m.id.0 == id)
            .map(|m| m.state)
    }

    /// What an unauthenticated datagram can and cannot trigger (issue #971).
    ///
    /// Without the cluster key: nothing. With a valid cluster-key HMAC but a
    /// certificate fingerprint this node has not cached: the datagram is dropped
    /// unread — no statement in it reaches the membership view — and its source gets
    /// exactly one `Join`, of bounded size, at most once a second per source and
    /// `CERT_REQUEST_BURST` times a second in all.
    #[test]
    fn a_cert_miss_draws_one_bounded_join_and_nothing_in_the_datagram_is_believed() {
        let (mut a, counts) = endpoint("a");
        let c = Wire::new("c");
        // a knows c, first-hand, with its certificate.
        a.receive(&c.full(Kind::Sync, vec![]), "c:1", 0);
        assert_eq!(state_of(&a, "c"), Some(MemberState::Alive));

        // b is a real cluster member whose certificate a has never seen. Its
        // datagram says c is dead and introduces d.
        let b = Wire::new("b");
        let unread = b.fp(
            Kind::Sync,
            vec![
                update("c", MemberState::Dead),
                update("d", MemberState::Alive),
            ],
        );
        let source = "203.0.113.5:7946";
        let out = datagrams(a.receive(&unread, source, 5000));

        // Dropped, counted, and not believed: c is still alive, neither b nor d exists.
        assert_eq!(count(&counts, "cert-miss"), 1);
        assert_eq!(state_of(&a, "c"), Some(MemberState::Alive));
        assert_eq!(state_of(&a, "b"), None);
        assert_eq!(state_of(&a, "d"), None);

        // One datagram, to the source: a Join that says who a is and nothing else.
        assert_eq!(out.len(), 1, "exactly one reply");
        assert_eq!(out[0].0, source);
        assert_eq!(count(&counts, "requested"), 1);
        let request = b.read(&out[0].1);
        assert_eq!(request.kind, Kind::Join);
        assert_eq!(request.from, "a");
        assert_eq!(request.gossip.len(), 1);
        assert_eq!(request.gossip[0].id, "a");
        assert_eq!(request.gossip[0].state, MemberState::Alive);
        assert!(
            out[0].1.len() <= MTU_BUDGET && out[0].1.len() < 800,
            "the request is one certificate and a header: {} B",
            out[0].1.len()
        );

        // The same source, again and again inside the interval: dropped, no reply.
        for i in 1..CERT_REQUEST_PER_SOURCE_MS {
            let again = b.fp(Kind::Ping { seq: i }, vec![]);
            assert!(datagrams(a.receive(&again, source, 5000 + i)).is_empty());
        }
        assert_eq!(count(&counts, "requested"), 1);
        // A round later it may be asked once more (the reply may have been lost).
        let later = b.fp(Kind::Ping { seq: 0 }, vec![]);
        let out = datagrams(a.receive(&later, source, 5000 + CERT_REQUEST_PER_SOURCE_MS));
        assert_eq!(out.len(), 1);

        // Many sources at once (a key-holder forging addresses): the total is capped.
        let mut replies = 0;
        for i in 0..200u32 {
            let forged = b.fp(Kind::Ping { seq: 1 }, vec![]);
            let src = format!("198.51.100.{}:{}", i % 250, 1000 + i);
            replies += datagrams(a.receive(&forged, &src, 9000 + u64::from(i % 5))).len();
        }
        assert_eq!(replies, CERT_REQUEST_BURST as usize);

        // WITHOUT the cluster key: nothing at all — not even from an address that
        // was just answered, and whatever the bytes are.
        let before = count(&counts, "requested");
        let outsider = Wire {
            id: "b".to_string(),
            auth: auth_on(99, "b"),
        };
        for (i, bytes) in [
            outsider.fp(Kind::Ping { seq: 1 }, vec![]),
            outsider.full(Kind::Join, vec![update("c", MemberState::Dead)]),
            vec![0u8; 0],
            vec![2u8; 33],
            vec![2u8; 1200],
        ]
        .iter()
        .enumerate()
        {
            let src = format!("192.0.2.{i}:7946");
            assert!(datagrams(a.receive(bytes, &src, 20_000)).is_empty());
        }
        assert_eq!(count(&counts, "requested"), before);
        assert_eq!(count(&counts, "auth"), 5);
        assert_eq!(state_of(&a, "c"), Some(MemberState::Alive));
        assert_eq!(a.swim().members().len(), 1, "the view never changed");
    }

    /// A member that restarts comes back with an empty certificate cache, while this
    /// node remembers having shown the OLD process its certificate. The datagram that
    /// reveals the new life (a higher generation) un-primes the member before the
    /// reply is sealed — so the reply carries our certificate, and the restarted
    /// member never has to drop a datagram of ours (issue #971: this is what kept a
    /// re-armed founder from being suspected by the cluster it rejoined).
    #[test]
    fn a_restarted_member_is_shown_our_certificate_again_in_the_very_next_datagram() {
        let (mut a, counts) = endpoint("a");
        let b = Wire::new("b");
        let is_full = |datagram: &[u8]| datagram.len() > CERT_LEN;

        // First contact with b's first life: our reply primes it, and from then on
        // our datagrams to it ride the fingerprint.
        let reply = datagrams(a.receive(&b.full(Kind::Join, vec![]), "b:1", 0));
        assert_eq!(reply.len(), 1);
        assert!(is_full(&reply[0].1), "the Sync carries our certificate");
        for seq in 1..=3 {
            let ack = datagrams(a.receive(&b.fp(Kind::Ping { seq }, vec![]), "b:1", seq));
            assert!(!is_full(&ack[0].1), "routine acks carry the fingerprint");
        }

        // b restarts: same id, same address, a new generation, an empty cache. Its
        // first datagram to us is an ordinary ping (full certificate: we are new to it).
        let restarted = Wire::new("b");
        let ping = |seq| {
            let mut m = Message::decode(&message("b", Kind::Ping { seq }, vec![])).unwrap();
            m.from_generation = 2;
            postcard::to_allocvec(&m).unwrap()
        };
        let ack = datagrams(a.receive(&restarted.auth.seal(&ping(1), true), "b:1", 100));
        assert_eq!(ack.len(), 1);
        assert!(
            is_full(&ack[0].1),
            "the ack to a restarted member carries our certificate again"
        );
        // The new process opens it — it holds nothing of ours but what this carries.
        assert!(matches!(
            restarted.read(&ack[0].1).kind,
            Kind::Ack { seq: 1 }
        ));
        // Once. The next ack is back on the fingerprint, which b now resolves.
        let next = datagrams(a.receive(&restarted.fp_of(&ping(2)), "b:1", 200));
        assert!(!is_full(&next[0].1));
        assert!(matches!(
            restarted.read(&next[0].1).kind,
            Kind::Ack { seq: 2 }
        ));
        assert_eq!(count(&counts, "cert-miss"), 0, "nothing was ever dropped");
    }

    /// The limiter's memory is bounded however many sources appear, and it never
    /// grants more than the burst in one window.
    #[test]
    fn the_request_limiter_bounds_its_rate_and_its_table() {
        let mut limiter = CertRequests::default();
        let mut granted_in_window = 0u32;
        let mut window = 0;
        let mut total = 0u32;
        for i in 0..200_000u64 {
            let now = i / 10; // ten new sources every millisecond, for 20 s
            if now / CERT_REQUEST_PER_SOURCE_MS != window {
                window = now / CERT_REQUEST_PER_SOURCE_MS;
                granted_in_window = 0;
            }
            if limiter.allow(&format!("src-{i}"), now) {
                granted_in_window += 1;
                total += 1;
            }
            assert!(granted_in_window <= CERT_REQUEST_BURST);
            assert!(limiter.last.len() <= CERT_REQUEST_SOURCES);
        }
        assert_eq!(
            total,
            20 * CERT_REQUEST_BURST,
            "every window grants its burst"
        );

        // One source is served once per interval, exactly.
        let mut limiter = CertRequests::default();
        let granted = (0..3 * CERT_REQUEST_PER_SOURCE_MS)
            .filter(|now| limiter.allow("one", *now))
            .count();
        assert_eq!(granted, 3);
    }

    /// The state the issue found on a 14-node cluster, in miniature. `a` pruned `b`
    /// after a false `Dead` and never held `b`'s certificate; `b` still holds `a`
    /// alive and keeps probing it. Before: `b`'s ping was dropped as cert-miss, `c`
    /// answered `b`'s `PingReq`, and nothing ever changed.
    ///
    /// Returns `(a, b, c)`: `a` as an endpoint built by `evictor`, with `b` pruned.
    fn one_sided<E>(
        evictor: impl FnOnce(Swim, SwimAuth) -> E,
        tick: impl Fn(&mut E, u64),
    ) -> (E, Gossip, Wire)
    where
        E: Evictor,
    {
        let c = Wire::new("c");
        let mut a = evictor(swim("a"), auth("a"));
        tick(&mut a, 0);
        // a learns of b through c, never from b itself — so it holds no certificate
        // for b — and then holds b falsely dead.
        a.take(
            &c.full(Kind::Sync, vec![update("b", MemberState::Alive)]),
            "c:1",
            1,
        );
        assert_eq!(a.view("b"), Some(MemberState::Alive));
        a.take(
            &c.fp(Kind::Sync, vec![update("b", MemberState::Dead)]),
            "c:1",
            10,
        );
        assert_eq!(a.view("b"), Some(MemberState::Dead));
        // The tombstone is pruned. (Its parting greeting to b is lost: this is UDP,
        // and the test is about what is left when it is.)
        tick(&mut a, 10 + cfg().dead_ttl_ms);
        assert_eq!(a.view("b"), None, "a has pruned b");

        // b holds a and c alive, with their certificates; its Sync replies are lost.
        let (mut b, _) = endpoint("b");
        b.tick(0);
        let a_earlier = Wire::new("a");
        b.receive(&a_earlier.full(Kind::Join, vec![]), "a:1", 1);
        b.receive(&c.full(Kind::Sync, vec![]), "c:1", 1);
        assert_eq!(state_of(&b, "a"), Some(MemberState::Alive));
        assert_eq!(state_of(&b, "c"), Some(MemberState::Alive));
        (a, b, c)
    }

    /// The evicting node, as this tree's endpoint or as v1.1.0's driver.
    trait Evictor {
        /// Handle an inbound datagram; returns the datagrams it sends.
        fn take(&mut self, datagram: &[u8], src: &str, now: u64) -> Vec<(String, Vec<u8>)>;
        fn view(&self, id: &str) -> Option<MemberState>;
    }
    impl Evictor for Gossip {
        fn take(&mut self, datagram: &[u8], src: &str, now: u64) -> Vec<(String, Vec<u8>)> {
            datagrams(self.receive(datagram, src, now))
        }
        fn view(&self, id: &str) -> Option<MemberState> {
            state_of(self, id)
        }
    }
    /// v1.1.0's driver: a cert-miss is dropped and nothing more; `Join`/`Sync` carry
    /// the full certificate, everything else the fingerprint.
    struct V1_1 {
        swim: Swim,
        auth: SwimAuth,
        cert_miss: u32,
    }
    impl V1_1 {
        fn send(&self, actions: Vec<crate::swim::Action>) -> Vec<(String, Vec<u8>)> {
            actions
                .into_iter()
                .filter_map(|a| match a {
                    crate::swim::Action::Send { to, msg } => {
                        let prime = matches!(msg.kind, Kind::Join | Kind::Sync);
                        let bytes = postcard::to_allocvec(&msg).unwrap();
                        Some((to, self.auth.seal(&bytes, prime)))
                    }
                    crate::swim::Action::StateChange { .. } => None,
                })
                .collect()
        }
    }
    impl Evictor for V1_1 {
        fn take(&mut self, datagram: &[u8], src: &str, now: u64) -> Vec<(String, Vec<u8>)> {
            let opened = match self.auth.open(datagram) {
                Ok(o) => o,
                Err(OpenReject::UnknownCert) => {
                    self.cert_miss += 1;
                    return Vec::new();
                }
                Err(e) => panic!("unexpected {e:?}"),
            };
            let mut msg = Message::decode(opened.payload).unwrap();
            msg.from_addr = src.to_string();
            let actions = self.swim.handle(msg, now);
            self.send(actions)
        }
        fn view(&self, id: &str) -> Option<MemberState> {
            self.swim
                .members()
                .into_iter()
                .find(|m| m.id.0 == id)
                .map(|m| m.state)
        }
    }

    /// Run `b`'s probe rounds against evictor `a` until `a` holds `b` alive again.
    /// `c` answers `b`'s own probes of it and rescues `b`'s probes of `a` (it reports
    /// `a` reachable every time). Returns how many times `b` had probed `a` when `a`
    /// re-admitted it. Bounded at 20 rounds.
    fn probes_until_readmitted<E: Evictor>(a: &mut E, b: &mut Gossip, c: &Wire) -> u32 {
        let mut probes_of_a = 0;
        for round in 1..=20u64 {
            let t = round * 100;
            // The round's probe and everything it draws, then — past the 20 ms ack
            // deadline — whatever b does about a probe that was not answered.
            for now in [t, t + 21] {
                let mut from_b = datagrams(b.tick(now));
                while let Some((to, bytes)) = from_b.pop() {
                    match to.as_str() {
                        "a:1" => {
                            // a's replies go back to b; b's replies to those, to a.
                            for (back, reply) in a.take(&bytes, "b:1", now) {
                                assert_eq!(back, "b:1");
                                from_b.extend(datagrams(b.receive(&reply, "a:1", now)));
                            }
                        }
                        "c:1" => match c.read(&bytes).kind {
                            Kind::Ping { seq } => {
                                let ack = c.fp(Kind::Ack { seq }, vec![]);
                                from_b.extend(datagrams(b.receive(&ack, "c:1", now + 1)));
                            }
                            Kind::PingReq { target, .. } => {
                                assert_eq!(target, "a");
                                probes_of_a += 1;
                                let rescued = c.fp(Kind::IndirectAck { target }, vec![]);
                                from_b.extend(datagrams(b.receive(&rescued, "c:1", now + 4)));
                            }
                            other => panic!("unexpected datagram to c: {other:?}"),
                        },
                        other => panic!("unexpected destination {other}"),
                    }
                }
            }
            assert_eq!(
                state_of(b, "a"),
                Some(MemberState::Alive),
                "b never doubts a: its helper reaches it every round"
            );
            if a.view("b") == Some(MemberState::Alive) {
                return probes_of_a;
            }
        }
        panic!("a never re-admitted b in 20 rounds ({probes_of_a} probes of a)");
    }

    /// The unit test issue #971 asks for. `a` asks the cert-miss sender for its
    /// certificate, so `b`'s FIRST probe of `a` heals the pair: `b`'s `Sync` reply
    /// carries its certificate and re-admits it first-hand.
    #[test]
    fn a_peer_pruned_after_a_false_dead_is_readmitted_on_its_next_probe() {
        let (mut a, mut b, c) = one_sided(
            |swim, auth| Gossip::new(swim, Some(auth), None),
            |a, now| {
                a.tick(now);
            },
        );
        let probes = probes_until_readmitted(&mut a, &mut b, &c);
        assert_eq!(
            probes, 1,
            "one probe of a: its ping was dropped (and drew a's request, which b \
             answered with its certificate), and the helper rescued that one round"
        );
        assert_eq!(state_of(&a, "b"), Some(MemberState::Alive));
        assert_eq!(state_of(&b, "a"), Some(MemberState::Alive));
    }

    /// The same pair when the evictor still runs v1.1.0 and asks for nothing: `b`
    /// must get its certificate across on its own. Its probes of `a` are rescued by
    /// `c` every round — which used to reset the count that triggers the re-greet —
    /// and now count: the probe after `reprime_after` (3) of them carries a `Join`.
    #[test]
    fn a_v1_1_evictor_is_reprimed_after_three_probes_rescued_only_by_a_helper() {
        let (mut a, mut b, c) = one_sided(
            |swim, auth| V1_1 {
                swim,
                auth,
                cert_miss: 0,
            },
            |a, now| {
                let _ = a.swim.tick(now);
            },
        );
        let probes = probes_until_readmitted(&mut a, &mut b, &c);
        assert_eq!(
            probes,
            cfg().reprime_after,
            "three rescued probes, then the fourth carries the certificate"
        );
        assert_eq!(
            a.cert_miss,
            cfg().reprime_after,
            "one dropped ping per probe"
        );
        assert_eq!(a.view("b"), Some(MemberState::Alive));
    }
}
