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

//!
//! The shell also answers for the one thing the state machine cannot know: whether the
//! node it drives is running late. See [`run_gossip`] and [`StallWatch`].

use crate::replay::{ReplayWindow, SeqStore, SequenceAllocator};
use crate::swim::{Action, Kind, LastProbes, MemberState, Message, Swim};
use crate::swim_auth::{OpenReject, Opened, SwimAuth};
use crate::NodeId;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, trace};

/// The interval at which the broker ticks the state machine. It must stay below the ack
/// timeout (250 ms), so that a probe's deadline is noticed promptly. One constant for the
/// broker and for the formation simulation (`tests/swim_formation_sim.rs`), so that what
/// the simulation measures is what the broker runs.
pub const TICK: Duration = Duration::from_millis(100);

/// A wake-up of the driver's loop more than this many ticks after the one before it is
/// a stall: the loop wakes at least once a tick, so a whole tick went missing.
const STALL_TICKS: u64 = 2;

/// The most datagrams read off the socket ahead of one tick ([`run_gossip`]). More
/// than Linux's default receive buffer holds of gossip-sized datagrams, so in a
/// cluster going about its business the read ends because the socket is empty.
pub const DRAIN_MAX_DATAGRAMS: usize = 1024;

/// The datagrams read ahead of a tick whatever the time: the time budget below is
/// wall time, and a loop that is preempted in the middle of its read must not find
/// the budget spent with an `Ack` one datagram further on. Twice the largest cluster
/// the project has formed, so every member's answer fits.
pub const DRAIN_MIN_DATAGRAMS: usize = 64;

/// The most time spent reading ahead of one tick once [`DRAIN_MIN_DATAGRAMS`] are
/// read, as a fraction of the tick: a quarter.
const DRAIN_TICK_FRACTION: u32 = 4;

/// Notices that the driver's loop was not scheduled.
///
/// The loop wakes at least once a tick. It [`observe`](Self::observe)s the monotonic
/// clock on every wake-up, and a wake-up more than two ticks after the one before it
/// is a stall: for that long this node read nothing and sent nothing.
///
/// It only notices. The state machine is fed the monotonic clock as it is: a stall
/// is counted and reported ([`Gossip::note_stall`]) and changes no timer. (A first
/// version of this left the stalled time out of the clock. That lag was never made
/// up, so a node that was persistently late ran all of its failure detection, the
/// tombstone included, at a fraction of real time: a real death took it tens of
/// seconds to see. Reading the socket before a tick is what keeps a late node from
/// misjudging a peer, and it costs no time.)
#[derive(Debug)]
pub struct StallWatch {
    tick_ms: u64,
    /// The monotonic reading at the last wake-up.
    last: u64,
}

impl StallWatch {
    /// A watch on a loop that wakes every `tick`.
    #[must_use]
    pub fn new(tick: Duration) -> Self {
        Self {
            tick_ms: u64::try_from(tick.as_millis()).unwrap_or(u64::MAX).max(1),
            last: 0,
        }
    }

    /// Record a wake-up at monotonic time `real_ms` (ms since the driver started).
    /// Returns how long the loop was stalled before this wake-up, if it was: the time
    /// since the last wake-up, less the tick that was due anyway.
    pub fn observe(&mut self, real_ms: u64) -> Option<u64> {
        let real_ms = real_ms.max(self.last);
        let gap = real_ms - self.last;
        self.last = real_ms;
        (gap > STALL_TICKS.saturating_mul(self.tick_ms)).then(|| gap - self.tick_ms)
    }
}

/// How much one tick may read off the socket before it runs: a count it reads
/// whatever the time, and beyond that a larger count and a deadline.
struct DrainBudget {
    read: usize,
    until: std::time::Instant,
}

impl DrainBudget {
    fn for_tick(tick: Duration) -> Self {
        Self {
            read: 0,
            // Real time, whatever clock the runtime keeps: this bounds work done.
            until: std::time::Instant::now() + tick / DRAIN_TICK_FRACTION,
        }
    }

    /// Whether one more datagram may be read; a `true` is spent.
    fn take(&mut self) -> bool {
        let over_time = || std::time::Instant::now() >= self.until;
        if self.read >= DRAIN_MAX_DATAGRAMS || (self.read >= DRAIN_MIN_DATAGRAMS && over_time()) {
            return false;
        }
        self.read += 1;
        true
    }
}

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
    /// The member is **confirmed gone** ([`Action::Gone`], ADR 0087 D2) — its
    /// tombstone was pruned with the parting greeting unanswered, or a new
    /// generation of the id joined. `state` is then `Dead`: this is not a change of
    /// state but the end of the life the verdict was about.
    pub gone: bool,
}

/// A sink called once per **certificate request** this node sends: a `Join` greeting
/// to the source of a datagram it had to drop as `cert-miss` (issue #971). A callback
/// for the same reason as [`RejectCounter`].
pub type CertRequestCounter = std::sync::Arc<dyn Fn() + Send + Sync>;

/// A sink for what becomes of the **last direct probes** this node sends before
/// declaring a suspect `Dead` (ADR 0016's amendment of 2026-10-13), called with a
/// bounded outcome — `"sent"`, `"spared"` (the suspect answered) or `"silent"` (it was
/// declared `Dead`) — and how many. See [`LastProbes`] for what each counts. A callback
/// for the same reason as [`RejectCounter`]; no member is named, so the series stay
/// three however large the cluster.
pub type LastProbeCounter = std::sync::Arc<dyn Fn(&'static str, u64) + Send + Sync>;

/// A sink called once per **stall** of this node that the driver observed
/// ([`StallWatch`]), with how long it lasted in milliseconds.
pub type StallCounter = std::sync::Arc<dyn Fn(u64) + Send + Sync>;

/// A sink for this node's **local-health awareness** (ADR 0016 §2), called with the new
/// score whenever it changes: `0` for a healthy node, up to `Config::awareness_max`.
pub type AwarenessGauge = std::sync::Arc<dyn Fn(u8) + Send + Sync>;

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
    last_probe: Option<LastProbeCounter>,
    stalled: Option<StallCounter>,
    awareness: Option<AwarenessGauge>,
    /// The state machine's last-probe counts as last handed to `last_probe`, and its
    /// awareness as last handed to `awareness`.
    last_probes_reported: LastProbes,
    awareness_reported: u8,
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
            last_probe: None,
            stalled: None,
            awareness: None,
            last_probes_reported: LastProbes::default(),
            awareness_reported: 0,
            identity: None,
            windows: HashMap::new(),
            cert_requests: CertRequests::default(),
        }
    }

    /// Count the last direct probes sent, spared and silent (ADR 0016).
    #[must_use]
    pub fn with_last_probe_counter(mut self, counter: Option<LastProbeCounter>) -> Self {
        self.last_probe = counter;
        self
    }

    /// Count the stalls of this node the driver observes ([`StallWatch`]).
    #[must_use]
    pub fn with_stall_counter(mut self, counter: Option<StallCounter>) -> Self {
        self.stalled = counter;
        self
    }

    /// Publish the local-health awareness whenever it changes (ADR 0016 §2).
    #[must_use]
    pub fn with_awareness_gauge(mut self, gauge: Option<AwarenessGauge>) -> Self {
        self.awareness = gauge;
        self
    }

    /// This node's loop was not scheduled for `stalled_ms` before the wake-up now
    /// being handled ([`StallWatch::observe`]): count it. Nothing else follows from it
    /// — no timer moves and the state machine is not told. What protects a peer from
    /// a node that was away is that the node reads its socket before it ticks.
    pub fn note_stall(&mut self, stalled_ms: u64) {
        debug!(stalled_ms, "the SWIM driver was not scheduled");
        if let Some(c) = &self.stalled {
            c(stalled_ms);
        }
    }

    /// Hand the last-probe counts that changed since the last call to the sink.
    fn report_last_probes(&mut self) {
        let now = self.swim.last_probes();
        let was = std::mem::replace(&mut self.last_probes_reported, now);
        let Some(c) = &self.last_probe else { return };
        for (outcome, now, was) in [
            ("sent", now.sent, was.sent),
            ("spared", now.spared, was.spared),
            ("silent", now.silent, was.silent),
        ] {
            if now > was {
                c(outcome, now - was);
            }
        }
    }

    /// Hand the awareness to its gauge if it has changed.
    fn report_awareness(&mut self) {
        let now = self.swim.awareness();
        if now != self.awareness_reported {
            self.awareness_reported = now;
            if let Some(g) = &self.awareness {
                g(now);
            }
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
        self.report_last_probes();
        self.report_awareness();
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
                    gone: false,
                })),
                Action::Gone {
                    id,
                    addr,
                    peer_addr,
                } => out.push(Output::Event(MembershipEvent {
                    id,
                    addr,
                    peer_addr,
                    state: MemberState::Dead,
                    domain: None,
                    gone: true,
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
/// than `run`'s parameters carry (the certificate-request, last-probe and stall
/// counters).
///
/// # A node that was itself late
///
/// This task shares its runtime with whatever else the process runs (in the broker:
/// the data path), so it can be kept off the CPU for longer than a tick — and a
/// stopped process or a suspended VM is kept off for as long as it lasts. One rule
/// keeps such a node from turning its own absence into a verdict about a peer, and
/// it costs no time:
///
/// **Read before expiring.** A tick first reads what the socket holds, then advances
/// the state machine. Deadlines are only ever acted on by a tick, so an `Ack` that
/// arrived is never beaten by the tick that would have given up on it. (`select!`
/// picks at random among ready branches: without this, a late tick and the `Ack`
/// queued behind it were a coin toss.)
///
/// The read asks the KERNEL, on a handle of its own ([`direct_handle`]), not the
/// runtime: tokio's socket reads only once its reactor has seen the socket become
/// readable, and there is a wake-up on which it has not. A process resumed from a
/// stop (`SIGCONT`) finds its reactor's poll interrupted — no events — and its timers
/// due: the tick runs with a socket full of datagrams that the runtime believes
/// empty. Two real brokers showed exactly that: the stopped prober woke, "read" its
/// socket through the runtime, found nothing, and declared dead a suspect whose `Ack`
/// was sitting there.
///
/// The read is bounded so that a flood cannot keep the tick from running:
/// [`DRAIN_MIN_DATAGRAMS`] are read whatever the time, then up to
/// [`DRAIN_MAX_DATAGRAMS`] within a quarter of the tick. A flood that outruns the
/// bound leaves datagrams unread behind it at that tick, which is the order every
/// tick had before. Without the gossip key nothing a sender queues gets past
/// authentication, so all it can do is spend this budget; forging the `Ack` takes
/// the suspect's own key.
///
/// **The clock is the monotonic clock**, late or not: a node that wakes late acts on
/// each deadline at its first wake-up at or after it, so each of the five steps to a
/// verdict (the probe round begins, ack timeout, indirect timeout, suspicion window,
/// last probe) comes at most one gap between wake-ups — the tick plus the lateness —
/// after it was due, and each step's deadline is set from when the step before was
/// taken: five gaps at most, and no lag that builds up beyond them. (Probe rounds
/// are kept to their schedule by the state machine, so the rounds before a member's
/// turn do not add their own.) A stall is counted ([`Gossip::note_stall`]) and changes nothing else. Missed ticks
/// are not made up ([`MissedTickBehavior::Delay`]): they would all read the same
/// clock.
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
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut watch = StallWatch::new(tick_interval);
    let mut buf = vec![0u8; RECV_BUF];
    let direct = direct_handle(&socket);
    if direct.is_none() {
        // Once, at start: this is not a state the node recovers from by itself.
        tracing::warn!(
            "SWIM: no second handle on the gossip socket (out of file descriptors?): \
             the read ahead of each tick goes through the runtime, which after this \
             process has been stopped or starved can report an empty socket that is \
             not — a peer whose answer was waiting may then be declared DEAD"
        );
    }
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
                let now = wake(&mut watch, &mut gossip, start);
                // Everything already received, before this tick may expire anything.
                let mut budget = DrainBudget::for_tick(tick_interval);
                while budget.take() {
                    // Empty (`WouldBlock`), or an error `recv_from` will meet again.
                    let Ok((n, src)) = read_now(&socket, direct.as_ref(), &mut buf) else {
                        break;
                    };
                    let outputs = gossip.receive(&buf[..n], &src.to_string(), now);
                    emit(&socket, &events, outputs).await;
                }
                let outputs = gossip.tick(now);
                emit(&socket, &events, outputs).await;
                // Probe rounds conclude on ticks, and acks were read just above:
                // isolation can trip or clear here.
                publish_isolation(gossip.swim(), &mut was_isolated);
            }
            recv = socket.recv_from(&mut buf) => {
                let Ok((n, src)) = recv else { continue };
                let now = wake(&mut watch, &mut gossip, start);
                let outputs = gossip.receive(&buf[..n], &src.to_string(), now);
                emit(&socket, &events, outputs).await;
                // Acks land in handle() — isolation can only CLEAR here.
                publish_isolation(gossip.swim(), &mut was_isolated);
            }
        }
    }
}

/// One datagram off the socket if the KERNEL holds one, without waiting: the read
/// ahead of a tick ([`run_gossip`]). Through `direct` where there is one — straight to
/// the kernel, whatever the runtime believes about the socket — and through the
/// runtime's own non-waiting read otherwise, which answers "nothing" until its reactor
/// has seen the socket become readable. What is taken here the runtime's read then
/// does not find, which is all it notices.
fn read_now(
    socket: &UdpSocket,
    direct: Option<&std::net::UdpSocket>,
    buf: &mut [u8],
) -> std::io::Result<(usize, std::net::SocketAddr)> {
    match direct {
        Some(direct) => direct.recv_from(buf),
        None => socket.try_recv_from(buf),
    }
}

/// A second handle on the driver's socket — the same socket, non-blocking as tokio made
/// it — to read with directly, past the runtime's idea of whether there is anything to
/// read ([`run_gossip`]). `None` where the platform offers no such duplicate or the
/// duplication fails (out of descriptors); the driver then reads through the runtime.
#[cfg(unix)]
fn direct_handle(socket: &UdpSocket) -> Option<std::net::UdpSocket> {
    use std::os::fd::AsFd;
    let owned = socket.as_fd().try_clone_to_owned().ok()?;
    Some(std::net::UdpSocket::from(owned))
}

#[cfg(not(unix))]
fn direct_handle(_socket: &UdpSocket) -> Option<std::net::UdpSocket> {
    None
}

/// One wake-up of the loop: the state machine's `now` — the monotonic clock — with a
/// stall, if this wake-up came after one, counted first.
fn wake(watch: &mut StallWatch, gossip: &mut Gossip, start: Instant) -> u64 {
    let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    if let Some(stalled_ms) = watch.observe(now) {
        gossip.note_stall(stalled_ms);
    }
    now
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
        // a asks b before it adopts the relayed verdict (`check_relayed_dead`), and
        // the probe is lost: b is declared dead when it runs out.
        let dead_at = 10 + cfg().ack_timeout_ms;
        tick(&mut a, dead_at);
        assert_eq!(a.view("b"), Some(MemberState::Dead));
        // The tombstone is pruned. (Its parting greeting to b is lost: this is UDP,
        // and the test is about what is left when it is.)
        tick(&mut a, dead_at + cfg().dead_ttl_ms);
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
                    crate::swim::Action::StateChange { .. } | crate::swim::Action::Gone { .. } => {
                        None
                    }
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

    /// `a`, holding `b` and `c` alive with their certificates, is told by `c` that `b`
    /// is suspected, and its window lapses: returns `a`, its counters, the wire peers
    /// `b` and `c`, the sequence number of the last direct probe `a` sent `b`, and the
    /// moment that probe gives up.
    fn a_asking_a_suspect() -> (Gossip, Counts, Wire, Wire, u64, u64) {
        let (mut a, counts) = endpoint("a");
        let (b, c) = (Wire::new("b"), Wire::new("c"));
        a.receive(&b.full(Kind::Sync, vec![]), "b:1", 0);
        a.receive(&c.full(Kind::Sync, vec![]), "c:1", 0);
        a.receive(
            &c.fp(Kind::Sync, vec![update("b", MemberState::Suspect)]),
            "c:1",
            10,
        );
        assert_eq!(state_of(&a, "b"), Some(MemberState::Suspect));

        // The window lapses. `a` does not declare `b` dead: it asks `b`.
        let lapsed = 10 + cfg().suspicion_timeout_ms;
        let sent = datagrams(a.tick(lapsed));
        assert_eq!(state_of(&a, "b"), Some(MemberState::Suspect));
        let seq = sent
            .iter()
            .filter(|(to, _)| to == "b:1")
            .find_map(|(_, bytes)| match b.read(bytes).kind {
                Kind::Ping { seq } => Some(seq),
                _ => None,
            })
            .expect("the last direct probe");
        (a, counts, b, c, seq, lapsed + cfg().ack_timeout_ms)
    }

    /// The last direct probe before `Dead` is answered only by the suspect (issue
    /// #975). With signed gossip an `Ack` is the suspect's when it is signed by the
    /// suspect's certificate — the same check every datagram passes, which is what an
    /// ordinary probe's `Ack` has always rested on. So a member that really is dead
    /// cannot be kept by anyone else:
    ///
    /// - a holder of the cluster key with no certificate for `b` cannot sign as `b`
    ///   (dropped: `auth`), and with a certificate of its own cannot claim to be `b`
    ///   (dropped: `identity`);
    /// - a member in good standing can answer with the probe's sequence number, but
    ///   as itself, which is not the suspect's answer;
    /// - an `Ack` of `b`'s own to some other probe does not carry this probe's
    ///   sequence number. (Within one life of `a`: its counter starts over when it
    ///   restarts, so an `Ack` `b` sent an earlier life of `a` can carry the same
    ///   number. Without anti-replay that is a replay the ordinary probe is open to
    ///   as well; ADR 0016's amendment of 2026-10-13 states it.)
    ///
    /// `b` is declared `Dead` when the probe's ack timeout runs out.
    #[test]
    fn only_a_datagram_signed_by_the_suspect_answers_its_last_probe() {
        let (mut a, counts, b, c, seq, gives_up) = a_asking_a_suspect();
        let now = gives_up - 5;
        let ack = message(
            "b",
            Kind::Ack { seq },
            vec![update("b", MemberState::Alive)],
        );

        // The cluster key and b's certificate (it is public), but not b's key.
        let mut b_cert = b"cert:b\0".to_vec();
        b_cert.resize(CERT_LEN, 0x5A);
        let no_key = SwimAuth::new(&[7; KEY_LEN]).with_signing(
            Arc::new(Signer {
                cn: "mallory".to_string(),
                cert: b_cert,
            }),
            Arc::new(Verifier),
        );
        assert!(datagrams(a.receive(&no_key.seal(&ack, true), "b:1", now)).is_empty());
        assert_eq!(count(&counts, "auth"), 1);

        // The cluster key and a certificate of its own: it is not b.
        assert!(datagrams(a.receive(&c.auth.seal(&ack, true), "b:1", now)).is_empty());
        assert_eq!(count(&counts, "identity"), 1);

        // A member answering as itself, with the right sequence number.
        a.receive(&c.fp(Kind::Ack { seq }, vec![]), "c:1", now);
        // b's own Ack, to another probe.
        a.receive(&b.fp(Kind::Ack { seq: seq + 1 }, vec![]), "b:1", now);
        assert_eq!(state_of(&a, "b"), Some(MemberState::Suspect));

        a.tick(gives_up);
        assert_eq!(state_of(&a, "b"), Some(MemberState::Dead));
    }

    /// The other half: the suspect's own signed `Ack` to that probe keeps it, even
    /// when it says nothing about itself — and then it is asked to, with a `Join`.
    #[test]
    fn the_suspects_own_signed_ack_answers_its_last_probe() {
        let (mut a, _, b, _, seq, gives_up) = a_asking_a_suspect();
        let sent = datagrams(a.receive(&b.fp(Kind::Ack { seq }, vec![]), "b:1", gives_up - 5));
        assert!(
            sent.iter()
                .any(|(to, bytes)| to == "b:1" && b.read(bytes).kind == Kind::Join),
            "b is asked for its own claim"
        );
        a.tick(gives_up);
        a.tick(gives_up + 10 * cfg().ack_timeout_ms);
        assert_eq!(state_of(&a, "b"), Some(MemberState::Suspect));

        // Its Sync says it is alive, first-hand, an incarnation on.
        let mut alive = update("b", MemberState::Alive);
        alive.incarnation += 1;
        a.receive(&b.full(Kind::Sync, vec![alive]), "b:1", gives_up + 300);
        assert_eq!(state_of(&a, "b"), Some(MemberState::Alive));
    }
}

/// The driver when the node it drives was itself stalled (ADR 0016's amendment for
/// issue #1002): the real loop ([`run_gossip`]) over a real loopback socket, on tokio's
/// paused clock, so the test decides when the loop runs and what time it finds.
#[cfg(test)]
mod stall_tests {
    use super::{direct_handle, read_now};
    use super::{run_gossip, DrainBudget, Gossip, MembershipEvent, StallWatch, TICK};
    use super::{DRAIN_MAX_DATAGRAMS, DRAIN_MIN_DATAGRAMS, STALL_TICKS};
    use crate::swim::{Config, Kind, MemberState, Message, Swim, Update};
    use crate::NodeId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::mpsc;

    type Counts = Arc<Mutex<HashMap<&'static str, u64>>>;

    /// Node `a`, driven by the real loop with the broker's timings, and its one peer
    /// `b`, played by the test on a plain socket.
    struct Rig {
        b: std::net::UdpSocket,
        a_addr: std::net::SocketAddr,
        /// A second handle on `a`'s socket, only ever peeked: how the test knows that a
        /// datagram it sent is in `a`'s receive buffer (loopback delivery is not part
        /// of `send_to` on every kernel).
        a_socket: std::net::UdpSocket,
        events: mpsc::UnboundedReceiver<MembershipEvent>,
        counts: Counts,
        driver: tokio::task::JoinHandle<()>,
    }

    /// Wait, in real time and bounded, for the kernel: `ready` is about a loopback
    /// datagram already sent, which arrives within microseconds.
    fn until(what: &str, mut ready: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !ready() {
            assert!(std::time::Instant::now() < deadline, "{what}");
            std::thread::yield_now();
        }
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            self.driver.abort();
        }
    }

    impl Rig {
        fn count(&self, what: &'static str) -> u64 {
            self.counts.lock().unwrap().get(what).copied().unwrap_or(0)
        }

        fn send(&self, kind: Kind, gossip: Vec<Update>) {
            let msg = Message {
                from: "b".to_string(),
                from_addr: String::new(), // the driver stamps the UDP source
                from_peer_addr: "b:7001".to_string(),
                from_domain: None,
                from_generation: 1,
                kind,
                gossip,
                cluster_id: None,
            };
            self.b
                .send_to(&postcard::to_allocvec(&msg).unwrap(), self.a_addr)
                .unwrap();
            // In `a`'s receive buffer before the test goes on. (`a` is not running:
            // the test is the one task on this runtime that is.)
            until("the datagram never reached a's socket", || {
                self.a_socket.peek_from(&mut [0u8; 1]).is_ok()
            });
        }

        /// Everything `a` has sent `b` so far, in order. `b` pings `a` with a sequence
        /// number `a` has never used and lets it run without any time passing; `a`'s
        /// `Ack` to that is the last datagram it has sent, and when it is here so is
        /// everything before it.
        async fn inbox(&self) -> Vec<Message> {
            const MARK: u64 = u64::MAX;
            self.send(Kind::Ping { seq: MARK }, Vec::new());
            turn().await;
            let mut buf = vec![0u8; 64 * 1024];
            let mut out = Vec::new();
            let mut marked = false;
            until("a never answered b's ping", || {
                while let Ok((n, _)) = self.b.recv_from(&mut buf) {
                    let msg = Message::decode(&buf[..n]).expect("a's datagrams decode");
                    if msg.kind == (Kind::Ack { seq: MARK }) {
                        marked = true;
                    } else {
                        out.push(msg);
                    }
                }
                marked
            });
            out
        }

        /// Whether `a` has declared `b` dead since this was last asked.
        fn declared_dead(&mut self) -> bool {
            let mut dead = false;
            while let Ok(event) = self.events.try_recv() {
                dead |= event.id.0 == "b" && event.state == MemberState::Dead;
            }
            dead
        }
    }

    /// Let `a`'s loop run: the test yields, the runtime turns its I/O and timer
    /// drivers and polls the loop, which runs until it has nothing ready.
    async fn turn() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    /// `ms` pass with `a`'s loop not running, and then it runs: on the paused clock
    /// nothing is scheduled between the two halves of this.
    async fn pass(ms: u64) {
        tokio::time::advance(Duration::from_millis(ms)).await;
        turn().await;
    }

    /// `a`, running, and `b`, which has introduced itself and will answer no probe.
    async fn a_knowing_b() -> Rig {
        let a_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        a_socket.set_nonblocking(true).unwrap();
        let socket = tokio::net::UdpSocket::from_std(a_socket.try_clone().unwrap()).unwrap();
        let a_addr = socket.local_addr().unwrap();
        let b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        b.set_nonblocking(true).unwrap();
        let swim = Swim::new(
            NodeId("a".to_string()),
            a_addr.to_string(),
            "a:7001".to_string(),
            None,
            1,
            Config::default(),
            Vec::new(),
        );
        let counts: Counts = Arc::new(Mutex::new(HashMap::new()));
        let (probes, stalls) = (counts.clone(), counts.clone());
        let gossip = Gossip::new(swim, None, None)
            .with_last_probe_counter(Some(Arc::new(move |outcome, n| {
                *probes.lock().unwrap().entry(outcome).or_default() += n;
            })))
            .with_stall_counter(Some(Arc::new(move |stalled_ms| {
                let mut counts = stalls.lock().unwrap();
                *counts.entry("stalls").or_default() += 1;
                *counts.entry("stalled_ms").or_default() += stalled_ms;
            })));
        let (tx, events) = mpsc::unbounded_channel();
        let driver = tokio::spawn(run_gossip(
            socket,
            gossip,
            TICK,
            tx,
            None,
            std::future::pending(),
        ));
        let rig = Rig {
            b,
            a_addr,
            a_socket,
            events,
            counts,
            driver,
        };

        rig.send(Kind::Sync, Vec::new());
        turn().await;
        rig
    }

    /// `a` holds `b` suspect, its window has lapsed, and the last direct probe has
    /// just gone out, on the tick that just ran. Returns the rig and that probe's
    /// sequence number.
    async fn a_with_its_last_probe_out() -> (Rig, u64) {
        // `b` answers no probe: probed, suspected (one suspecter: the full 4 s
        // window), and asked once more when that lapses.
        let mut rig = a_knowing_b().await;
        for _ in 0..200 {
            pass(100).await;
            if rig.count("sent") == 0 {
                continue;
            }
            // The tick that just ran sent the last probe, and nothing after it: with
            // these timings the window lapses between two rounds of ordinary probes.
            let sent = rig.inbox().await;
            let probe = sent.last().expect("a sent b its last probe");
            let Kind::Ping { seq } = probe.kind else {
                panic!("the last datagram is not the last probe: {probe:?}");
            };
            assert_eq!(probe.gossip.len(), 1, "{probe:?}");
            assert_eq!(probe.gossip[0].state, MemberState::Suspect);
            assert_eq!(probe.gossip[0].suspecter.as_deref(), Some("a"));
            assert!(!rig.declared_dead());
            assert_eq!(rig.count("sent"), 1);
            assert_eq!(rig.count("stalls"), 0, "a ran on time up to here");
            return (rig, seq);
        }
        panic!("a never sent b a last probe");
    }

    /// The finding this change closes. `a`'s last probe is out; `b` answers at once;
    /// and `a` does not run again for three seconds — twelve ack timeouts. When it
    /// does, the tick that is due and the `Ack` in its socket are both ready, and on
    /// `a`'s clock, which is the real one, the probe's deadline is long past. `b` is
    /// not declared dead: the `Ack` is read before the tick. `a` counts the stall.
    #[tokio::test(start_paused = true)]
    async fn a_prober_stalled_past_the_ack_timeout_reads_the_ack_before_it_gives_up() {
        // The loop picks between its ready branches at random: often enough that a
        // loop which could give up first would.
        for _ in 0..24 {
            let (mut rig, seq) = a_with_its_last_probe_out().await;
            rig.send(Kind::Ack { seq }, Vec::new());
            pass(3000).await;
            assert!(!rig.declared_dead(), "declared dead with its ack unread");
            assert_eq!((rig.count("spared"), rig.count("silent")), (1, 0));
            assert_eq!(rig.count("stalls"), 1);

            // And it stays that way: `b` is asked for its claim, and answers it.
            let asked = rig.inbox().await;
            assert!(asked.iter().any(|m| m.kind == Kind::Join), "{asked:?}");
            rig.send(
                Kind::Sync,
                vec![Update {
                    id: "b".to_string(),
                    addr: String::new(),
                    peer_addr: "b:7001".to_string(),
                    incarnation: 1,
                    generation: 1,
                    state: MemberState::Alive,
                    suspecter: None,
                    failure_domain: None,
                }],
            );
            for _ in 0..10 {
                pass(100).await;
            }
            assert!(!rig.declared_dead());
            assert_eq!(rig.count("silent"), 0);
        }
    }

    /// The same race at its narrowest: the `Ack` arrives inside the ack timeout, and
    /// the next time `a` runs is the tick at which that timeout has passed — two ticks
    /// on, late but not a stall. With the read ahead of the tick taken out, this fails.
    #[tokio::test(start_paused = true)]
    async fn an_ack_already_in_the_socket_beats_the_tick_that_would_give_up_on_it() {
        let ack_timeout = Config::default().ack_timeout_ms;
        let late = STALL_TICKS * 100;
        assert!(
            100 + late >= ack_timeout,
            "the tick below is past the deadline"
        );
        for _ in 0..24 {
            let (mut rig, seq) = a_with_its_last_probe_out().await;
            pass(100).await;
            assert_eq!(rig.count("spared") + rig.count("silent"), 0);
            rig.send(Kind::Ack { seq }, Vec::new());
            pass(late).await;
            assert!(!rig.declared_dead(), "the tick won against a queued ack");
            assert_eq!((rig.count("spared"), rig.count("silent")), (1, 0));
            assert_eq!(rig.count("stalls"), 0, "not a stall: the read decided it");
        }
    }

    /// None of this keeps a member that is not there, or keeps it a moment longer.
    /// The same stall with no `Ack`: when `a` wakes its socket is empty and the probe's
    /// deadline has passed, so `b` is declared dead on that wake-up.
    #[tokio::test(start_paused = true)]
    async fn a_suspect_that_stays_silent_is_declared_dead_when_the_stalled_prober_wakes() {
        let (mut rig, _) = a_with_its_last_probe_out().await;
        pass(3000).await;
        assert!(rig.declared_dead(), "a silent suspect outlived the stall");
        assert_eq!(
            (rig.count("sent"), rig.count("spared"), rig.count("silent")),
            (1, 0, 1)
        );
        assert_eq!((rig.count("stalls"), rig.count("stalled_ms")), (1, 2900));
    }

    /// How long `a`, whose loop wakes every `gap` ms, takes to declare a `b` that
    /// never answers dead, and how long after that it prunes the tombstone — both in
    /// real time, counted by the test; and the rig, for its counters.
    async fn time_to_dead_and_to_prune(gap: u64) -> (u64, u64, Rig) {
        let mut rig = a_knowing_b().await;
        let mut elapsed = 0;
        while !rig.declared_dead() {
            assert!(elapsed < 60_000, "a never declared a silent member dead");
            pass(gap).await;
            elapsed += gap;
        }
        let dead_at = elapsed;
        // Pruned: `a` greets the member once as it forgets it (issue #971). Nothing
        // else is sent to a member held dead.
        rig.inbox().await;
        loop {
            assert!(elapsed < dead_at + 120_000, "a never pruned the tombstone");
            pass(gap).await;
            elapsed += gap;
            if rig.inbox().await.iter().any(|m| m.kind == Kind::Join) {
                return (dead_at, elapsed - dead_at, rig);
            }
        }
    }

    /// Detection of a real death does not degrade when the node doing the detecting is
    /// persistently late: the state machine runs on the real clock. `a`'s only peer
    /// dies; from then on every wake-up of `a`'s loop comes 370 ms late — each one a
    /// stall. `a` has nobody else to probe, so nothing here can rest on a probe being
    /// answered.
    ///
    /// The bound: a late node acts on a deadline at its first wake-up at or after it,
    /// so each of the five steps to the verdict (the probe round begins; ack timeout;
    /// indirect timeout; suspicion window; last probe) comes at most one gap between
    /// wake-ups — the tick plus the lateness — after it was due, and nothing builds up
    /// beyond those five. The lateness here is chosen so that the gaps line up with
    /// nothing: not with the probe period, not with a timeout. The tombstone is not stretched
    /// either: it is pruned `dead_ttl` after the verdict, give or take one wake-up —
    /// which is what bounds how long a peer held dead is held before it is
    /// confirmed gone (ADR 0087 D2).
    #[tokio::test(start_paused = true)]
    async fn a_persistently_late_node_declares_a_dead_member_within_five_wake_ups_of_on_time() {
        let cfg = Config::default();
        let tick = u64::try_from(TICK.as_millis()).unwrap();
        let (on_time, pruned_after, rig) = time_to_dead_and_to_prune(tick).await;
        assert_eq!(rig.count("stalls"), 0);
        assert_eq!(pruned_after, cfg.dead_ttl_ms);

        let late_by = 370;
        let gap = tick + late_by;
        let (late, pruned_after, rig) = time_to_dead_and_to_prune(gap).await;
        assert!(rig.count("stalls") > 10, "every wake-up was a stall");
        assert_eq!(rig.count("silent"), 1);
        assert!(
            late <= on_time + 5 * gap,
            "on time: dead after {on_time} ms; waking every {gap} ms: after {late} ms"
        );
        eprintln!(
            "a dead peer: declared after {on_time} ms on time, {late} ms waking every {gap} ms"
        );
        assert!(
            (cfg.dead_ttl_ms..=cfg.dead_ttl_ms + tick + late_by).contains(&pruned_after),
            "the tombstone lasted {pruned_after} ms on a late node; dead_ttl is {} ms",
            cfg.dead_ttl_ms
        );
    }

    /// The read ahead of a tick finds a datagram the RUNTIME has not noticed. This is
    /// the state a process is in when it resumes from a stop with its tick overdue:
    /// the datagram is in the kernel's buffer and tokio's reactor has not turned since
    /// it arrived, so tokio's own non-waiting read says the socket is empty. The
    /// driver's read ([`read_now`], on its direct handle) must return it all the same.
    ///
    /// Nothing is awaited between the datagram's arrival and the two reads, so the
    /// reactor cannot have turned. With `read_now` switched to the runtime's read this
    /// fails; so it does where a second handle cannot be had — which is why the driver
    /// warns when it cannot have one.
    #[tokio::test]
    async fn the_read_ahead_of_a_tick_finds_a_datagram_the_runtime_has_not_noticed() {
        let ours = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        ours.set_nonblocking(true).unwrap();
        let addr = ours.local_addr().unwrap();
        let socket = tokio::net::UdpSocket::from_std(ours.try_clone().unwrap()).unwrap();
        let direct = direct_handle(&socket).expect("a second handle on the socket");

        let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.send_to(b"an ack that arrived in time", addr).unwrap();
        until("the datagram never reached the socket", || {
            ours.peek_from(&mut [0u8; 1]).is_ok()
        });

        let mut buf = [0u8; 64];
        let through_the_runtime = socket.try_recv_from(&mut buf);
        assert!(
            matches!(&through_the_runtime, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
            "the premise of this test: tokio reports nothing until its reactor has \
             turned, and here it has not: {through_the_runtime:?}"
        );
        let (n, from) = read_now(&socket, Some(&direct), &mut buf)
            .expect("the driver's read finds what the kernel holds");
        assert_eq!(&buf[..n], b"an ack that arrived in time");
        assert_eq!(from, peer.local_addr().unwrap());
        // And without the handle the driver's read is the runtime's: nothing.
        peer.send_to(b"another", addr).unwrap();
        until("the second datagram never reached the socket", || {
            ours.peek_from(&mut [0u8; 1]).is_ok()
        });
        assert!(read_now(&socket, None, &mut buf).is_err());
    }

    /// The watch: a wake-up more than two ticks after the last is a stall of the gap
    /// less the tick that was due; anything up to that is not.
    #[test]
    fn the_stall_watch_reports_a_wake_up_more_than_two_ticks_late() {
        let tick = u64::try_from(TICK.as_millis()).unwrap();
        let mut watch = StallWatch::new(TICK);
        assert_eq!(watch.observe(0), None);
        assert_eq!(watch.observe(tick), None);
        assert_eq!(watch.observe(tick + 7), None);
        assert_eq!(watch.observe(3 * tick), None);
        assert_eq!(
            watch.observe(5 * tick),
            None,
            "two ticks: late, not a stall"
        );
        assert_eq!(watch.observe(7 * tick + 1), Some(tick + 1));
        assert_eq!(watch.observe(60_000), Some(60_000 - 8 * tick - 1));
        assert_eq!(watch.observe(60_040), None);
        // A monotonic clock does not go back; a reading that did is no stall.
        assert_eq!(watch.observe(59_000), None);
        assert_eq!(STALL_TICKS, 2);
    }

    /// The read ahead of a tick is bounded, so a flood cannot keep the tick from
    /// running: a minimum that is read whatever the time — a loop preempted in the
    /// middle of its read must not find its budget gone — and a maximum.
    #[test]
    fn the_read_ahead_of_a_tick_is_bounded() {
        let mut budget = DrainBudget::for_tick(Duration::from_hours(1));
        let granted = (0..2 * DRAIN_MAX_DATAGRAMS)
            .filter(|_| budget.take())
            .count();
        assert_eq!(granted, DRAIN_MAX_DATAGRAMS);

        // No time at all: the minimum, and no more.
        let mut budget = DrainBudget::for_tick(Duration::ZERO);
        let granted = (0..2 * DRAIN_MAX_DATAGRAMS)
            .filter(|_| budget.take())
            .count();
        assert_eq!(granted, DRAIN_MIN_DATAGRAMS);
    }
}
