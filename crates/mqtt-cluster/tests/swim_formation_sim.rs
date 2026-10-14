//! Deterministic **formation** simulation for signed SWIM gossip (issue #971).
//!
//! `swim_sim.rs` drives the pure [`Swim`] state machine alone. The formation storm of
//! #971 lives one layer down: in the signed posture a datagram names its sender's
//! certificate by fingerprint, and a receiver that has not cached that certificate drops
//! it (`cert-miss`) before the state machine sees anything. So this harness drives the
//! driver's real endpoint ([`Gossip`]: seal, open, certificate cache, certificate
//! requests, state machine) over a virtual clock and a lossless in-memory network, and
//! forms a cluster the way the scale rig starts one (`bench/scale/bootstrap-cluster.sh`):
//! a founder with no seeds, then followers one after another, each seeded to two nodes,
//! every certificate cache empty, and finally the founder restarted with seeds.
//!
//! A node can also run as **v1.1.0** did ([`Flavor::V1_1`]): that release's driver —
//! full certificate on `Join`/`Sync` and every 16th datagram, a `cert-miss` dropped and
//! nothing more — re-stated here from the same public pieces, with the two state-machine
//! rules of this change switched off. That is how the harness reproduces the issue,
//! measures each cause on its own, and runs a mixed-version cluster.
//!
//! Every choice (start stagger, tick phase, latency) comes from one seed, so a run is a
//! pure function of `(n, seed, variant)` and a failure reruns identically. Which
//! datagrams a lossy network drops is a function of the seed, the link and the moment
//! and of nothing a node does ([`Sim::lost`]), so two detectors run on one seed are run
//! on the same network.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use mqtt_cluster::swim::{Action, Config, Kind, MemberState, Message, Swim};
use mqtt_cluster::swim_auth::{
    GossipSign, GossipVerify, OpenReject, SwimAuth, VerifiedIdentity, KEY_LEN,
};
use mqtt_cluster::swim_driver::{
    CertRequests, Gossip, MembershipEvent, Output, DRAIN_MAX_DATAGRAMS, TICK,
};
use mqtt_cluster::NodeId;

/// The broker's driver tick: the constant the broker itself passes its driver, so the
/// simulation cannot measure one tick while the broker runs another.
#[allow(clippy::cast_possible_truncation)] // 100
const TICK_MS: u64 = TICK.as_millis() as u64;
/// The longest a datagram takes on the simulated network, ms ([`Sim::apply`]).
const MAX_LATENCY_MS: u64 = 1;
/// A realistic ECDSA P-256 leaf certificate size (DER), so datagram sizes are honest.
const CERT_LEN: usize = 560;
/// The size signed gossip must stay under: a 1500-byte Ethernet MTU less the IPv6 and
/// UDP headers (ADR 0022 T6's reason for fingerprinting at all).
const MTU_BUDGET: usize = 1500 - 40 - 8;

/// Seeded xorshift64 — the codebase's hand-rolled style, no `rand` dependency.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng((seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// A value in `[lo, hi]`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            lo
        } else {
            lo + self.next() % (hi - lo + 1)
        }
    }
}

/// Stand-in PKI: the certificate is `cert:<cn>\0` padded to a real leaf's size, the
/// signature a 64-byte value bound to `(cn, payload)`. The real crypto is exercised in
/// `mqtt-auth`; what matters here is the cert-ref / cache behaviour around it.
struct StubSigner {
    cn: String,
    cert: Vec<u8>,
}
impl StubSigner {
    fn new(cn: &str) -> Self {
        let mut cert = format!("cert:{cn}\0").into_bytes();
        cert.resize(CERT_LEN, 0xA5);
        Self {
            cn: cn.to_string(),
            cert,
        }
    }
}
fn stub_sig(cn: &str, payload: &[u8]) -> Vec<u8> {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in cn.bytes().chain(payload.iter().copied()) {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    let mut sig = Vec::with_capacity(64);
    for i in 0..8u64 {
        sig.extend_from_slice(&(h ^ i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).to_be_bytes());
    }
    sig
}
impl GossipSign for StubSigner {
    fn cert_der(&self) -> &[u8] {
        &self.cert
    }
    fn sign(&self, payload: &[u8]) -> Vec<u8> {
        stub_sig(&self.cn, payload)
    }
}
struct StubVerifier;
impl GossipVerify for StubVerifier {
    fn verify(
        &self,
        cert_der: &[u8],
        payload: &[u8],
        sig: &[u8],
    ) -> Result<VerifiedIdentity, OpenReject> {
        let rest = cert_der.strip_prefix(b"cert:").ok_or(OpenReject::Auth)?;
        let end = rest.iter().position(|b| *b == 0).ok_or(OpenReject::Auth)?;
        let cn = std::str::from_utf8(&rest[..end]).map_err(|_| OpenReject::Auth)?;
        if sig == stub_sig(cn, payload) {
            Ok(VerifiedIdentity {
                cn: cn.to_string(),
                failure_domain: None,
            })
        } else {
            Err(OpenReject::Auth)
        }
    }
}

fn signed_auth(cn: &str) -> SwimAuth {
    SwimAuth::new(&[7; KEY_LEN]).with_signing(Arc::new(StubSigner::new(cn)), Arc::new(StubVerifier))
}

/// Which gossip a node runs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Flavor {
    /// This tree: the real [`Gossip`] endpoint, default configuration.
    Current,
    /// v1.1.0, re-stated: its driver's seal/open rules and its state-machine rules.
    V1_1,
    /// v1.1.0 plus exactly one of this change's four mechanisms — for measuring what
    /// each contributes on its own (the report, not the regression tests).
    V1_1Plus(Fix),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fix {
    /// The first datagram to each destination carries the full certificate.
    PrimeFirstContact,
    /// A `cert-miss` draws a `Join` to the datagram's source.
    RequestOnMiss,
    /// A probe rescued only by helpers counts toward the deaf-peer re-greet.
    ReprimeOnRescuedProbe,
    /// A pruned tombstone's member gets one parting `Join`.
    GreetPruned,
}

/// Added to a real endpoint's drop counter for a drop that is NOT a `cert-miss`.
const OTHER_REJECT: u64 = 1 << 32;

/// v1.1.0's state-machine rules: what has been added since is off — the two rules of
/// issue #971, the last direct probe before `Dead` (issue #975), and the probe period
/// kept to its schedule (issue #1002).
fn v1_1_cfg() -> Config {
    Config {
        reprime_on_rescued_probe: false,
        greet_pruned: false,
        confirm_before_dead: false,
        period_from_schedule: false,
        ..Config::default()
    }
}

/// A node's gossip: the real endpoint, or the v1.1.0 model.
enum Endpoint {
    Real {
        gossip: Box<Gossip>,
        cert_miss: Arc<AtomicU64>,
    },
    Model {
        swim: Box<Swim>,
        auth: Box<SwimAuth>,
        prime_first_contact: bool,
        request_on_miss: bool,
        requests: Box<CertRequests>,
    },
}

/// What an endpoint emitted: sealed datagrams and membership changes.
enum Emit {
    Datagram { to: String, bytes: Vec<u8> },
    State { id: String, state: MemberState },
}

impl Endpoint {
    fn new(swim: Swim, auth: SwimAuth, flavor: Flavor) -> Self {
        match flavor {
            Flavor::Current => {
                let cert_miss = Arc::new(AtomicU64::new(0));
                let counter = cert_miss.clone();
                let gossip = Gossip::new(swim, Some(auth), None).with_reject_counter(Some(
                    Arc::new(move |reason: &'static str| {
                        // Any other drop is a bug: the high half makes `hand_over` panic
                        // with the datagram in hand.
                        let unit = if reason == "cert-miss" {
                            1
                        } else {
                            OTHER_REJECT
                        };
                        counter.fetch_add(unit, Ordering::Relaxed);
                    }),
                ));
                Endpoint::Real {
                    gossip: Box::new(gossip),
                    cert_miss,
                }
            }
            Flavor::V1_1 | Flavor::V1_1Plus(_) => Endpoint::Model {
                swim: Box::new(swim),
                auth: Box::new(auth),
                prime_first_contact: flavor == Flavor::V1_1Plus(Fix::PrimeFirstContact),
                request_on_miss: flavor == Flavor::V1_1Plus(Fix::RequestOnMiss),
                requests: Box::default(),
            },
        }
    }

    fn cfg(flavor: Flavor) -> Config {
        match flavor {
            Flavor::Current => Config::default(),
            Flavor::V1_1Plus(Fix::ReprimeOnRescuedProbe) => Config {
                reprime_on_rescued_probe: true,
                ..v1_1_cfg()
            },
            Flavor::V1_1Plus(Fix::GreetPruned) => Config {
                greet_pruned: true,
                ..v1_1_cfg()
            },
            Flavor::V1_1 | Flavor::V1_1Plus(_) => v1_1_cfg(),
        }
    }

    fn swim(&self) -> &Swim {
        match self {
            Endpoint::Real { gossip, .. } => gossip.swim(),
            Endpoint::Model { swim, .. } => swim,
        }
    }

    fn from_outputs(outputs: Vec<Output>) -> Vec<Emit> {
        outputs
            .into_iter()
            .map(|o| match o {
                Output::Datagram { to, bytes } => Emit::Datagram { to, bytes },
                Output::Event(MembershipEvent { id, state, .. }) => Emit::State { id: id.0, state },
            })
            .collect()
    }

    /// v1.1.0's `apply`: `Join`/`Sync` prime, everything else rides the fingerprint or
    /// the every-16th full certificate.
    fn model_emit(auth: &SwimAuth, prime_first_contact: bool, actions: Vec<Action>) -> Vec<Emit> {
        actions
            .into_iter()
            .map(|a| match a {
                Action::Send { to, msg } => {
                    let prime = matches!(msg.kind, Kind::Join | Kind::Sync);
                    let payload = postcard::to_allocvec(&msg).expect("encodes");
                    let bytes = if prime_first_contact {
                        auth.seal_to(&to, &payload, None, prime)
                    } else {
                        auth.seal(&payload, prime)
                    };
                    Emit::Datagram { to, bytes }
                }
                Action::StateChange { id, state, .. } => Emit::State { id: id.0, state },
            })
            .collect()
    }

    fn tick(&mut self, now: u64) -> Vec<Emit> {
        match self {
            Endpoint::Real { gossip, .. } => Self::from_outputs(gossip.tick(now)),
            Endpoint::Model {
                swim,
                auth,
                prime_first_contact,
                ..
            } => Self::model_emit(auth, *prime_first_contact, swim.tick(now)),
        }
    }

    /// Returns what the endpoint emitted and whether the datagram was a `cert-miss`.
    fn receive(&mut self, bytes: &[u8], src: &str, now: u64) -> (Vec<Emit>, bool) {
        match self {
            Endpoint::Real { gossip, cert_miss } => {
                let before = cert_miss.load(Ordering::Relaxed);
                let out = Self::from_outputs(gossip.receive(bytes, src, now));
                (out, cert_miss.load(Ordering::Relaxed) != before)
            }
            Endpoint::Model {
                swim,
                auth,
                prime_first_contact,
                request_on_miss,
                requests,
            } => {
                // v1.1.0's inbound pipeline: authenticate, decode, bind the identity,
                // stamp the source address, hand to the state machine.
                let opened = match auth.open(bytes) {
                    Ok(o) => o,
                    Err(OpenReject::UnknownCert) => {
                        let out = if *request_on_miss && requests.allow(src, now) {
                            Self::model_emit(auth, *prime_first_contact, vec![swim.greet(src)])
                        } else {
                            Vec::new()
                        };
                        return (out, true);
                    }
                    Err(e) => panic!("unexpected reject {e:?}"),
                };
                let mut msg = Message::decode(opened.payload).expect("decodes");
                assert_eq!(opened.identity.as_deref(), Some(msg.from.as_str()));
                msg.from_addr = src.to_string();
                let actions = swim.handle(msg, now);
                (Self::model_emit(auth, *prime_first_contact, actions), false)
            }
        }
    }
}

/// What a run varies.
#[derive(Clone)]
struct Variant {
    /// Which gossip node `i` (of `n`) runs.
    flavor: fn(usize, usize) -> Flavor,
    /// Every node starts holding every other node's certificate.
    preshared_certs: bool,
    /// Restart the founder with seeds once every follower is up, as the rig "arms" it.
    arm_founder: bool,
    /// Gap between successive follower starts, ms — `[lo, hi]`.
    stagger_ms: (u64, u64),
    /// After formation has had `at_ms`, put a fault on one node for `for_ms` — by
    /// default a clean cut (nothing in, nothing out). Times in the outcome are then
    /// measured from the end of the fault.
    isolate: Option<Isolate>,
    /// Every datagram is dropped with this probability, in thousandths, for the rest
    /// of the run: from [`LOSS_AFTER_MS`] after formation when no node is isolated,
    /// and the outcome is measured from that moment; from the moment the fault begins
    /// when one is — so that nothing the loss does goes uncounted before the fault,
    /// and the member the fault is put on is a member in good standing.
    loss_permille: u64,
    /// Applied to the configuration of every node on this tree ([`Flavor::Current`]):
    /// how a failure-detection variant is measured against the default (issue #973).
    tune: fn(Config) -> Config,
    /// The driver loop the nodes on this tree run when one of them is stalled.
    shell: Shell,
    /// Stall a PROBER at the worst moment: the first node on this tree, other than the
    /// isolated one, to send a last direct probe stops for this long from the instant
    /// it has sent it. One node, once per run.
    stall_prober_ms: Option<u64>,
    /// Make every node but the isolated one LATE, from the moment the fault begins.
    late: Option<Late>,
    /// The interval between a node's ticks, ms: the broker's ([`TICK_MS`]) unless a run
    /// asks what another grid would do to the bound.
    tick_ms: u64,
}

/// Nodes whose driver loop wakes late: each tick comes `lo..=hi` ms after it was due,
/// and the next is due a full interval after that (the driver does not make up ticks).
#[derive(Clone, Copy)]
struct Late {
    lo: u64,
    hi: u64,
    /// For how long, from the fault's start ([`Late::stalls`] only).
    for_ms: u64,
    /// Whether the node runs at all between its late wake-ups. `true`: it does not —
    /// a runtime that is not scheduling it — and what arrives queues on its socket
    /// until the next wake-up; this is put on every node but the isolated one, from
    /// the fault's start. `false`: only the ticks are late and datagrams are handled
    /// as they come — a loop whose timer is serviced late; this is put on every node
    /// for the whole run, so that no tick is on anybody's grid.
    stalls: bool,
}

/// What a node's driver loop does on the first tick after the node was stalled — its
/// ticks stopped while datagrams queued on its socket.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shell {
    /// This tree's (`swim_driver::run_gossip`): everything queued is read, then the
    /// tick. The clock is the monotonic one.
    Current,
    /// The loop as it was up to #1000, at its worst: the same clock, and the tick
    /// ahead of what is queued. (It picked between the two at random; a simulation
    /// that wants to see the bug takes the order that shows it.) v1.1.0 nodes always
    /// run this one.
    TickFirst,
}

#[derive(Clone, Copy)]
struct Isolate {
    at_ms: u64,
    for_ms: u64,
    shape: Fault,
}

/// What is wrong with the isolated node while its fault lasts.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fault {
    /// Nothing in, nothing out.
    Cut,
    /// Nothing reaches it; what it sends arrives.
    Deaf,
    /// It hears everything; nothing it sends arrives.
    Mute,
    /// One link, one way: what ONE other node sends it is lost — that node's probes
    /// and requests never arrive, while the member's own datagrams reach everyone
    /// and every other node reaches it.
    DeafToOne,
    /// Everything it sends arrives this many ms late: a node whose replies come, but
    /// after the deadline they were wanted by.
    Slow(u64),
    /// The node itself stops — a stopped process, a suspended VM, a runtime that does
    /// not schedule it: its ticks stop, and what is sent to it is not lost but queues
    /// on its socket (without limit here; a kernel's buffer holds a few hundred
    /// datagrams) until it runs again. It is then both things at once: a member that
    /// was silent, and a prober whose deadlines all passed while it was away.
    Stall,
}

/// How long after formation the reports put a fault on a member: a minute.
const FAULT_AFTER_MS: u64 = 60_000;
/// How long after formation the loss of [`Variant::loss_permille`] begins when no node
/// is isolated: the moment a fault would have begun. A lossy cell with a fault and the
/// one without are then the same network from the same moment — the loss is a function
/// of the time ([`Sim::lost`]) — and differ by the fault alone.
const LOSS_AFTER_MS: u64 = FAULT_AFTER_MS;
/// An [`Isolate::for_ms`] that outlasts every run: the fault never ends.
const FOREVER_MS: u64 = 10_000_000;

impl Variant {
    /// The scale rig's formation, every node on `flavor`.
    fn rig(flavor: fn(usize, usize) -> Flavor) -> Self {
        Self {
            flavor,
            preshared_certs: false,
            arm_founder: true,
            stagger_ms: (2500, 4500),
            isolate: None,
            loss_permille: 0,
            tune: |cfg| cfg,
            shell: Shell::Current,
            stall_prober_ms: None,
            late: None,
            tick_ms: TICK_MS,
        }
    }
}

fn all_current(_: usize, _: usize) -> Flavor {
    Flavor::Current
}
fn all_v1_1(_: usize, _: usize) -> Flavor {
    Flavor::V1_1
}
/// A rolling upgrade half done: odd nodes still on v1.1.0.
fn half_upgraded(i: usize, _: usize) -> Flavor {
    if i % 2 == 1 {
        Flavor::V1_1
    } else {
        Flavor::Current
    }
}
/// A rolling upgrade's last step: one node still on v1.1.0.
fn one_left_on_v1_1(i: usize, n: usize) -> Flavor {
    if i == n / 2 {
        Flavor::V1_1
    } else {
        Flavor::Current
    }
}
/// A rolling upgrade's first step: one node on this tree in a v1.1.0 cluster.
fn one_upgraded(i: usize, n: usize) -> Flavor {
    if i == n / 2 {
        Flavor::Current
    } else {
        Flavor::V1_1
    }
}

struct Node {
    endpoint: Endpoint,
    /// Global time this process started; its monotonic clock is `now - started`.
    started: u64,
    /// The loop this node's driver runs.
    shell: Shell,
    /// Datagrams that arrived while the node was stalled, oldest first: its socket's
    /// receive buffer.
    backlog: Vec<(usize, Vec<u8>)>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Ev {
    Start(usize),
    Arm,
    Tick(usize),
    Deliver {
        to: usize,
        from: usize,
        bytes: Vec<u8>,
    },
}

/// What one run observed.
#[derive(Debug, Default, Clone)]
struct Outcome {
    /// The moment times below are measured from (global ms): the last process start,
    /// or the end of the isolation.
    measured_from: u64,
    /// First moment every node held every other `Alive`, ms after `measured_from`.
    first_full: Option<u64>,
    /// The moment after which every node held every other `Alive` until the deadline.
    settled: Option<u64>,
    /// `Suspect` / `Dead` transitions each node raised about (healthy) peers, after
    /// `measured_from` — or over the whole run when nothing is isolated.
    suspects: Vec<u32>,
    deads: Vec<u32>,
    /// Every `Dead` transition any node raised while a member was cut off. Nearly all
    /// are by that member or about it — it really was unreachable at that moment —
    /// but the count does not check: a `Dead` between two other nodes in that
    /// interval is counted too.
    deads_during_cut: u32,
    /// How long after the cut began every OTHER node held the cut-off member `Dead`,
    /// ms — the detection time of a real death, when the cut never ends.
    buried_after: Option<u64>,
    /// `cert-miss` drops, `[source][destination]`.
    cert_miss: Vec<Vec<u32>>,
    /// Longest stretch one node held a healthy peer `Dead` or pruned, ms (open ones run
    /// to the deadline).
    longest_eviction: u64,
    /// Ordered pairs `(evictor, evicted)` still broken at the deadline.
    evicted_at_deadline: Vec<(usize, usize)>,
    /// Largest sealed datagram, bytes, and how many were sent.
    max_datagram: usize,
    datagrams: u64,
    /// Datagrams the lossy network dropped.
    lost: u64,
    /// `Dead` transitions that were a node's own declaration (out of its tick), over
    /// the whole run; and `Dead` transitions a node adopted from a datagram about a
    /// member that NO node had declared dead before. The second is zero in a cluster
    /// of honest nodes: a `Dead` has to start somewhere.
    declared_deads: u64,
    untraced_deads: u64,
    /// [`Variant::stall_prober_ms`]: a node sent a last probe and was stalled on it,
    /// and what came of that.
    stalled_prober: Option<StalledProber>,
    /// The isolated member was turned `Dead` by some node, from the fault's start on.
    dead_about_isolated: bool,
    /// Last direct probes sent, answered by the suspect, and run out in silence,
    /// summed over the nodes at the end of the run.
    last_probes: (u64, u64, u64),
}

/// What came of stalling a prober on its last probe.
#[derive(Debug, Default, Clone, Copy)]
struct StalledProber {
    /// It gave up on a last probe of its own (it counted one silent) by the end of
    /// the run.
    gave_up: bool,
    /// It was itself turned `Dead` by some node: silent while it was stopped.
    turned_dead: bool,
}

impl Outcome {
    fn total_cert_miss(&self) -> u32 {
        self.cert_miss.iter().flatten().sum()
    }
    fn total_suspects(&self) -> u32 {
        self.suspects.iter().sum()
    }
    fn total_deads(&self) -> u32 {
        self.deads.iter().sum()
    }
}

struct Sim {
    n: usize,
    variant: Variant,
    nodes: Vec<Option<Node>>,
    index: BTreeMap<String, usize>,
    queue: BinaryHeap<Reverse<(u64, u64, Ev)>>,
    seqno: u64,
    rng: Rng,
    now: u64,
    out: Outcome,
    /// `(evictor, evicted)` → when the evictor declared the peer `Dead`.
    evicted_since: BTreeMap<(usize, usize), u64>,
    last_not_full: u64,
    /// `(node, from, until)` in global ms while a node is cut off.
    cut: Option<(usize, u64, u64)>,
    /// The one node whose datagrams to the isolated node are lost ([`Fault::DeafToOne`]).
    deaf_to: usize,
    /// Global ms from which datagrams are lost at random; `u64::MAX` for never.
    loss_from: u64,
    /// The run's seed, for the lossy network's own choices ([`Sim::lost`]).
    seed: u64,
    /// Per directed link: the last millisecond a datagram was offered to the lossy
    /// network on it, and how many have been in that millisecond.
    offered: BTreeMap<(usize, usize), (u64, u64)>,
    /// `(node, from, until)` in global ms: the prober stalled by
    /// [`Variant::stall_prober_ms`], once it has been.
    prober_stall: Option<(usize, u64, u64)>,
    /// A tick's own output is being applied: a `Dead` in it is that node's own
    /// declaration, where a `Dead` out of a datagram is one it was told.
    declaring: bool,
    /// The members some node has declared `Dead` on its own account so far.
    declared: std::collections::BTreeSet<usize>,
    /// Per node: until when it is not running, between two late wake-ups
    /// ([`Late::stalls`]), and how many late ticks it has had.
    late_until: Vec<u64>,
    late_ticks: Vec<u64>,
    /// The nodes that have declared the cut-off member `Dead` since the cut began.
    buriers: std::collections::BTreeSet<usize>,
}

/// splitmix64 over four coordinates, chained from `seed`: the simulation's choices that
/// must not depend on what the nodes do.
fn mix(seed: u64, words: [u64; 4]) -> u64 {
    let mut h = seed;
    for word in words {
        h = h.wrapping_add(word).wrapping_add(0x9E37_79B9_7F4A_7C15);
        h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^= h >> 31;
    }
    h
}

fn addr_of(i: usize) -> String {
    format!("10.99.1.{}:7946", 10 + i)
}
fn id_of(i: usize) -> String {
    format!("mqttd-{}", i + 1)
}

impl Sim {
    fn new(n: usize, seed: u64, variant: Variant) -> Self {
        let mut sim = Sim {
            n,
            variant,
            nodes: (0..n).map(|_| None).collect(),
            index: (0..n).map(|i| (addr_of(i), i)).collect(),
            queue: BinaryHeap::new(),
            seqno: 0,
            rng: Rng::new(seed),
            now: 0,
            out: Outcome {
                suspects: vec![0; n],
                deads: vec![0; n],
                cert_miss: vec![vec![0; n]; n],
                ..Outcome::default()
            },
            evicted_since: BTreeMap::new(),
            last_not_full: 0,
            cut: None,
            // The founder: never the isolated node.
            deaf_to: 0,
            loss_from: u64::MAX,
            seed,
            offered: BTreeMap::new(),
            prober_stall: None,
            declaring: false,
            declared: std::collections::BTreeSet::new(),
            late_until: vec![0; n],
            late_ticks: vec![0; n],
            buriers: std::collections::BTreeSet::new(),
        };
        // Founder first; the rig waits for it to be ready before starting anyone else.
        sim.push(0, Ev::Start(0));
        let mut t = 3000;
        for i in 1..n {
            let (lo, hi) = sim.variant.stagger_ms;
            t += sim.rng.range(lo, hi);
            sim.push(t, Ev::Start(i));
        }
        if sim.variant.arm_founder && n > 1 {
            // The rig arms the founder once every follower reports ready (a majority of
            // members alive) — seconds after the last start.
            t += sim.rng.range(4000, 8000);
            sim.push(t, Ev::Arm);
        }
        sim.out.measured_from = t;
        if sim.variant.loss_permille > 0 {
            sim.loss_from = t + LOSS_AFTER_MS;
            sim.out.measured_from = sim.loss_from;
        }
        if let Some(iso) = sim.variant.isolate {
            // Cut off a node that is neither the founder nor a seed (node 0 and 1 are
            // every follower's seeds, and unacquainted seeds are re-greeted anyway).
            // (A cluster of two has only the one that is not the founder.)
            let node = if n >= 3 {
                2 + usize::try_from(sim.rng.range(0, (n - 3) as u64)).expect("small")
            } else {
                n - 1
            };
            let from = t + iso.at_ms;
            sim.cut = Some((node, from, from + iso.for_ms));
            if sim.variant.loss_permille > 0 {
                // The network turns lossy as the fault begins. (It used to turn lossy
                // 30 s earlier while the counting began at the fault's end: what the
                // loss did in between was in no column, and the member the fault was
                // put on could already be a tombstone.)
                sim.loss_from = from;
            }
            // A fault that never ends is measured from its start.
            sim.out.measured_from = if iso.for_ms >= FOREVER_MS {
                from
            } else {
                from + iso.for_ms
            };
        }
        sim
    }

    fn push(&mut self, at: u64, ev: Ev) {
        self.seqno += 1;
        self.queue.push(Reverse((at, self.seqno, ev)));
    }

    /// The rig's seeding (`seeds_for`): the founder none; everyone else the first two
    /// other nodes. The armed founder: node 1 and the last node.
    fn seeds_for(&self, i: usize, armed: bool) -> Vec<String> {
        if i == 0 {
            if armed {
                vec![addr_of(1), addr_of(self.n - 1)]
            } else {
                Vec::new()
            }
        } else {
            (0..self.n)
                .filter(|j| *j != i)
                .take(2)
                .map(addr_of)
                .collect()
        }
    }

    fn start(&mut self, i: usize, armed: bool) {
        let flavor = (self.variant.flavor)(i, self.n);
        let swim = Swim::new(
            NodeId(id_of(i)),
            addr_of(i),
            format!("10.99.1.{}:7001", 10 + i),
            None,
            // Wall-clock ms at start in the broker: strictly greater on a restart.
            1_000_000 + self.now,
            if flavor == Flavor::Current {
                (self.variant.tune)(Endpoint::cfg(flavor))
            } else {
                Endpoint::cfg(flavor)
            },
            self.seeds_for(i, armed),
        );
        let auth = signed_auth(&id_of(i));
        if self.variant.preshared_certs {
            for j in 0..self.n {
                if j != i {
                    let primer = signed_auth(&id_of(j)).seal(b"prime", true);
                    assert!(auth.open(&primer).is_ok());
                }
            }
        }
        let restarted = self.nodes[i].is_some();
        self.nodes[i] = Some(Node {
            endpoint: Endpoint::new(swim, auth, flavor),
            started: self.now,
            shell: if flavor == Flavor::Current {
                self.variant.shell
            } else {
                Shell::TickFirst
            },
            backlog: Vec::new(),
        });
        if !restarted {
            let phase = self.rng.range(0, TICK_MS - 1);
            self.push(self.now + phase, Ev::Tick(i));
        }
    }

    /// The fault on node `i` at this moment, if it is the isolated node and inside
    /// its window.
    fn fault_on(&self, i: usize) -> Option<Fault> {
        let (node, from, until) = self.cut?;
        (node == i && self.now >= from && self.now < until)
            .then(|| self.variant.isolate.map_or(Fault::Cut, |iso| iso.shape))
    }

    /// Whether a datagram from `from` to `to` is lost to the isolated node's fault.
    fn severed(&self, from: usize, to: usize) -> bool {
        let sender = self.fault_on(from);
        let receiver = self.fault_on(to);
        matches!(sender, Some(Fault::Cut | Fault::Mute))
            || matches!(receiver, Some(Fault::Cut | Fault::Deaf))
            || (receiver == Some(Fault::DeafToOne) && from == self.deaf_to)
    }

    /// Until when node `i` is stalled, if it is at this moment: the isolated node under
    /// [`Fault::Stall`], or the prober stalled by [`Variant::stall_prober_ms`].
    fn stalled_until(&self, i: usize) -> Option<u64> {
        if self.fault_on(i) == Some(Fault::Stall) {
            return self.cut.map(|(_, _, until)| until);
        }
        if self.late_until[i] > self.now {
            return Some(self.late_until[i]);
        }
        self.prober_stall
            .filter(|(node, from, until)| *node == i && self.now >= *from && self.now < *until)
            .map(|(_, _, until)| until)
    }

    /// Whether the lossy network drops the datagram `from` is sending `to` now.
    ///
    /// A function of the run's seed, the directed link, the millisecond, and the
    /// datagram's place among those offered on that link in that millisecond — and of
    /// nothing else. It draws on no generator, so what one node sends does not change
    /// which of another's datagrams are lost, and two runs of one seed under two
    /// detectors lose the same datagrams wherever their traffic is the same: an extra
    /// datagram in one of them (a last probe, say) meets its own fate and moves nobody
    /// else's. (The fate of each datagram used to be the next draw from the generator
    /// that also supplies latency, so one datagram more or less reshuffled every loss
    /// after it.) A lossless run draws nothing here, as before.
    fn lost(&mut self, from: usize, to: usize, nth: u64) -> bool {
        if self.variant.loss_permille == 0 || self.now < self.loss_from {
            return false;
        }
        let h = mix(
            self.seed ^ 0x6A09_E667_F3BC_C908,
            [from as u64, to as u64, self.now, nth],
        );
        let lost = h % 1000 < self.variant.loss_permille;
        self.out.lost += u64::from(lost);
        lost
    }

    /// The place of the datagram `from` is sending `to` now among those offered on
    /// that directed link in this millisecond.
    fn ordinal(&mut self, from: usize, to: usize) -> u64 {
        let offered = self.offered.entry((from, to)).or_insert((self.now, 0));
        if offered.0 != self.now {
            *offered = (self.now, 0);
        }
        offered.1 += 1;
        offered.1 - 1
    }

    /// How long the network takes over the datagram `from` is sending `to` now: well
    /// under a millisecond, sometimes over.
    ///
    /// On a LOSSY network this is a function of the same four coordinates as the loss
    /// ([`Sim::lost`]) and of nothing a node does, for the whole run: with both the
    /// fate and the delay of a datagram fixed by where and when it is sent, two
    /// detectors on one seed really are on one network. On a lossless network it is
    /// the next draw from the run's generator, as it has been since this harness was
    /// written — every lossless table on record was measured on those schedules, and
    /// its two arms are the same run only up to the first datagram one of them sends
    /// and the other does not.
    fn latency(&mut self, from: usize, to: usize, nth: u64) -> u64 {
        if self.variant.loss_permille == 0 {
            return self.rng.range(0, MAX_LATENCY_MS);
        }
        mix(
            self.seed ^ 0x3C6E_F372_FE94_F82B,
            [from as u64, to as u64, self.now, nth],
        ) % (MAX_LATENCY_MS + 1)
    }

    fn counting(&self) -> bool {
        (self.variant.isolate.is_none() && self.variant.loss_permille == 0)
            || self.now >= self.out.measured_from
    }

    fn apply(&mut self, from: usize, emitted: Vec<Emit>) {
        for e in emitted {
            match e {
                Emit::Datagram { to, bytes } => {
                    let Some(&j) = self.index.get(&to) else {
                        continue;
                    };
                    self.out.max_datagram = self.out.max_datagram.max(bytes.len());
                    self.out.datagrams += 1;
                    let nth = if self.variant.loss_permille > 0 {
                        self.ordinal(from, j)
                    } else {
                        0
                    };
                    if self.severed(from, j) || self.lost(from, j, nth) {
                        continue;
                    }
                    let mut lat = self.latency(from, j, nth);
                    if let Some(Fault::Slow(late)) = self.fault_on(from) {
                        lat += late;
                    }
                    self.push(self.now + lat, Ev::Deliver { to: j, from, bytes });
                }
                Emit::State { id, state } => {
                    let Some(subject) = (0..self.n).find(|j| id_of(*j) == id) else {
                        continue;
                    };
                    match state {
                        MemberState::Suspect => {
                            if self.counting() {
                                self.out.suspects[from] += 1;
                            }
                        }
                        MemberState::Dead => {
                            if self.declaring {
                                self.declared.insert(subject);
                                self.out.declared_deads += 1;
                            } else if !self.declared.contains(&subject) {
                                self.out.untraced_deads += 1;
                            }
                            if self.counting() {
                                self.out.deads[from] += 1;
                            }
                            if self.cut.is_some_and(|(_, from, until)| {
                                self.now >= from && self.now < until
                            }) {
                                self.out.deads_during_cut += 1;
                            }
                            if self
                                .prober_stall
                                .is_some_and(|(node, _, _)| node == subject)
                            {
                                if let Some(prober) = &mut self.out.stalled_prober {
                                    prober.turned_dead = true;
                                }
                            }
                            if let Some((node, cut_from, _)) = self.cut {
                                if subject == node && self.now >= cut_from {
                                    self.out.dead_about_isolated = true;
                                    self.buriers.insert(from);
                                    if self.buriers.len() == self.n - 1
                                        && self.out.buried_after.is_none()
                                    {
                                        self.out.buried_after = Some(self.now - cut_from);
                                    }
                                }
                            }
                            self.evicted_since
                                .entry((from, subject))
                                .or_insert(self.now);
                        }
                        MemberState::Alive => {
                            if let Some(since) = self.evicted_since.remove(&(from, subject)) {
                                let from_ms = since.max(self.out.measured_from);
                                self.out.longest_eviction = self
                                    .out
                                    .longest_eviction
                                    .max(self.now.saturating_sub(from_ms));
                            }
                        }
                    }
                }
            }
        }
    }

    /// A wake-up of node `i`'s driver loop: the `now` it feeds its state machine — its
    /// monotonic clock, however late the wake-up.
    fn wake(&self, i: usize) -> u64 {
        let node = self.nodes[i].as_ref().expect("a node that is up");
        self.now - node.started
    }

    /// A datagram reaches node `to`'s socket: handed to its driver, or — while the
    /// node is stalled, and until the tick that ends the stall has read what queued
    /// before it — left in the socket's buffer.
    fn arrive(&mut self, to: usize, from: usize, bytes: Vec<u8>) {
        if self.severed(from, to) {
            return;
        }
        let stalled = self.stalled_until(to).is_some();
        let Some(node) = self.nodes[to].as_mut() else {
            return; // not started yet: the datagram hits a closed port
        };
        if stalled || !node.backlog.is_empty() {
            node.backlog.push((from, bytes));
            return;
        }
        let now = self.wake(to);
        self.hand_over(to, from, &bytes, now);
    }

    /// One tick of node `i`'s driver loop.
    fn tick(&mut self, i: usize) {
        if let Some(until) = self.stalled_until(i) {
            // Stalled: no tick. The loop runs again when the stall ends, and does not
            // make up the ticks it missed.
            self.push(until, Ev::Tick(i));
            return;
        }
        let node = self.nodes[i].as_mut().expect("ticking node is up");
        let mut queued = std::mem::take(&mut node.backlog);
        let shell = node.shell;
        let now = self.wake(i);
        // This tree's loop reads what its socket holds before the tick, up to its
        // bound; the loop before it ticked first (at its worst).
        let after_tick = match shell {
            Shell::Current => queued.split_off(queued.len().min(DRAIN_MAX_DATAGRAMS)),
            Shell::TickFirst => std::mem::take(&mut queued),
        };
        for (from, bytes) in queued {
            self.hand_over(i, from, &bytes, now);
        }
        let sent_before = self.last_probes_sent(i);
        let node = self.nodes[i].as_mut().expect("ticking node is up");
        let emitted = node.endpoint.tick(now);
        self.declaring = true;
        self.apply(i, emitted);
        self.declaring = false;
        for (from, bytes) in after_tick {
            self.hand_over(i, from, &bytes, now);
        }
        let late = self.lateness(i);
        let next = self.now + self.variant.tick_ms + late.unwrap_or(0);
        self.push(next, Ev::Tick(i));
        if late.is_some() && self.variant.late.is_some_and(|l| l.stalls) {
            self.late_until[i] = next;
        }

        // The adversarial stall: this node has just sent a last probe, and stops.
        if let Some(for_ms) = self.variant.stall_prober_ms {
            let isolated = self.cut.is_some_and(|(node, _, _)| node == i);
            if self.prober_stall.is_none() && !isolated && self.last_probes_sent(i) > sent_before {
                self.prober_stall = Some((i, self.now, self.now + for_ms));
                self.out.stalled_prober = Some(StalledProber::default());
            }
        }
    }

    /// How late node `i`'s next tick will be, if it is one of the late nodes now
    /// ([`Variant::late`]): a function of the seed, the node and the tick's number.
    fn lateness(&mut self, i: usize) -> Option<u64> {
        let late = self.variant.late?;
        if late.stalls {
            let (isolated, from, _) = self.cut?;
            let over = self.now >= from.saturating_add(late.for_ms);
            if i == isolated || self.now < from || over {
                return None;
            }
        }
        self.late_ticks[i] += 1;
        let h = mix(
            self.seed ^ 0xBB67_AE85_84CA_A73B,
            [i as u64, self.late_ticks[i], 0, 0],
        );
        Some(late.lo + h % (late.hi - late.lo + 1))
    }

    /// Last direct probes node `i` has sent so far.
    fn last_probes_sent(&self, i: usize) -> u64 {
        self.nodes[i]
            .as_ref()
            .map_or(0, |n| n.endpoint.swim().last_probes().sent)
    }

    fn hand_over(&mut self, to: usize, from: usize, bytes: &[u8], now: u64) {
        let node = self.nodes[to].as_mut().expect("a node that is up");
        let (emitted, miss) = node.endpoint.receive(bytes, &addr_of(from), now);
        if let Endpoint::Real { cert_miss, .. } = &node.endpoint {
            // Nothing in a lossless cluster of honest nodes is dropped for any reason
            // but a certificate not yet cached. Anything else fails the run, with
            // what is needed to look at it.
            assert!(
                cert_miss.load(Ordering::Relaxed) < OTHER_REJECT,
                "{} dropped a datagram from {} at t={} ms for a reason other than cert-miss \
                 ({} B): {bytes:02x?}",
                id_of(to),
                id_of(from),
                self.now,
                bytes.len(),
            );
        }
        if miss {
            self.out.cert_miss[from][to] += 1;
        }
        self.apply(to, emitted);
    }

    fn full_view(&self) -> bool {
        self.nodes.iter().all(|n| {
            n.as_ref()
                .is_some_and(|n| n.endpoint.swim().alive().len() == self.n - 1)
        })
    }

    /// Run until `deadline_ms` after `measured_from`; returns the outcome.
    fn run(mut self, deadline_ms: u64) -> Outcome {
        let end = self.out.measured_from + deadline_ms;
        while let Some(Reverse((at, _, ev))) = self.queue.pop() {
            if at > end {
                break;
            }
            self.now = at;
            match ev {
                Ev::Start(i) => self.start(i, false),
                Ev::Arm => self.start(0, true),
                Ev::Tick(i) => self.tick(i),
                Ev::Deliver { to, from, bytes } => self.arrive(to, from, bytes),
            }
            if self.now >= self.out.measured_from {
                if self.full_view() {
                    if self.out.first_full.is_none() {
                        self.out.first_full = Some(self.now - self.out.measured_from);
                    }
                } else {
                    self.last_not_full = self.now;
                }
            }
        }
        if self.full_view() {
            self.out.settled = Some(self.last_not_full.saturating_sub(self.out.measured_from));
        }
        for (&pair, &since) in &self.evicted_since {
            let from_ms = since.max(self.out.measured_from);
            self.out.longest_eviction = self.out.longest_eviction.max(end - from_ms);
            self.out.evicted_at_deadline.push(pair);
        }
        for node in self.nodes.iter().flatten() {
            let probes = node.endpoint.swim().last_probes();
            self.out.last_probes.0 += probes.sent;
            self.out.last_probes.1 += probes.spared;
            self.out.last_probes.2 += probes.silent;
        }
        if let (Some((node, _, _)), Some(prober)) =
            (self.prober_stall, &mut self.out.stalled_prober)
        {
            prober.gave_up = self.nodes[node]
                .as_ref()
                .is_some_and(|n| n.endpoint.swim().last_probes().silent > 0);
        }
        self.out
    }
}

/// One line of the report: a size's outcome across seeds.
fn summarize(label: &str, n: usize, runs: &[Outcome]) -> String {
    let settled: Vec<u64> = runs.iter().filter_map(|o| o.settled).collect();
    let unsettled = runs.len() - settled.len();
    let med = |mut v: Vec<u64>| {
        v.sort_unstable();
        v.get(v.len() / 2).copied()
    };
    #[allow(clippy::cast_precision_loss)] // report formatting of small counts
    let per_node = |f: fn(&Outcome) -> u32| {
        let mut v: Vec<u32> = runs.iter().map(f).collect();
        v.sort_unstable();
        (
            f64::from(v[v.len() / 2]) / n as f64,
            f64::from(v[v.len() - 1]) / n as f64,
        )
    };
    let (sus_med, sus_max) = per_node(Outcome::total_suspects);
    let (dead_med, dead_max) = per_node(Outcome::total_deads);
    let (miss_med, miss_max) = per_node(Outcome::total_cert_miss);
    format!(
        "{label:<26} N={n:<3} settled med {:>6} max {:>6} ms, unsettled {unsettled:>2}/{} | \
         per node: SUSPECT {sus_med:5.1} (max {sus_max:5.1}) DEAD {dead_med:5.1} (max {dead_max:5.1}) \
         cert-miss {miss_med:6.1} (max {miss_max:6.1}) | longest eviction {:>6} ms, \
         evicted at deadline {:>4} | max datagram {} B",
        med(settled.clone()).map_or("-".into(), |v| v.to_string()),
        settled.iter().max().map_or("-".into(), ToString::to_string),
        runs.len(),
        runs.iter().map(|o| o.longest_eviction).max().unwrap_or(0),
        runs.iter()
            .map(|o| o.evicted_at_deadline.len())
            .sum::<usize>(),
        runs.iter().map(|o| o.max_datagram).max().unwrap_or(0),
    )
}

const SIZES: [usize; 7] = [3, 5, 10, 12, 14, 20, 30];
/// How long the report watches a run after its measuring point, virtual ms.
const REPORT_DEADLINE_MS: u64 = 300_000;

/// Every seed's run, in seed order. The runs are independent, so they go out over the
/// machine's cores; each is still a pure function of its seed.
fn sweep(n: usize, seeds: u64, variant: &Variant, deadline_ms: u64) -> Vec<Outcome> {
    let workers = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let next = AtomicU64::new(0);
    let mut runs: Vec<(u64, Outcome)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut mine = Vec::new();
                    loop {
                        let seed = next.fetch_add(1, Ordering::Relaxed);
                        if seed >= seeds {
                            return mine;
                        }
                        mine.push((seed, Sim::new(n, seed, variant.clone()).run(deadline_ms)));
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("a run panicked"))
            .collect()
    });
    runs.sort_by_key(|(seed, _)| *seed);
    runs.into_iter().map(|(_, outcome)| outcome).collect()
}

/// The measurement behind issue #971's analysis: prints, per size, what formation does
/// on v1.1.0, with each cause removed in turn, on this tree, and in a mixed cluster. Not
/// an assertion —
/// `cargo test -p mqtt-cluster --release --test swim_formation_sim -- --ignored --nocapture`.
#[test]
#[ignore = "measurement report for issue #971; run with --ignored --nocapture"]
fn formation_report() {
    let seeds = 16;
    let rows: [(&str, Variant); 14] = [
        ("v1.1.0", Variant::rig(all_v1_1)),
        (
            "v1.1.0, certs pre-shared",
            Variant {
                preshared_certs: true,
                ..Variant::rig(all_v1_1)
            },
        ),
        (
            "v1.1.0 + prime 1st contact",
            Variant::rig(|_, _| Flavor::V1_1Plus(Fix::PrimeFirstContact)),
        ),
        (
            "v1.1.0 + request on miss",
            Variant::rig(|_, _| Flavor::V1_1Plus(Fix::RequestOnMiss)),
        ),
        (
            "v1.1.0 + reprime rescued",
            Variant::rig(|_, _| Flavor::V1_1Plus(Fix::ReprimeOnRescuedProbe)),
        ),
        (
            "v1.1.0 + greet pruned",
            Variant::rig(|_, _| Flavor::V1_1Plus(Fix::GreetPruned)),
        ),
        ("this tree", Variant::rig(all_current)),
        (
            // The default without its last direct probe before `Dead` (issue #975):
            // a formation raises no suspicion, so the probe has nothing to confirm.
            "this tree, no last probe",
            Variant {
                tune: NO_PROBE.1,
                ..Variant::rig(all_current)
            },
        ),
        (
            "this tree, founder not re-armed",
            Variant {
                arm_founder: false,
                ..Variant::rig(all_current)
            },
        ),
        ("one node left on v1.1.0", Variant::rig(one_left_on_v1_1)),
        (
            "one left, no last probe",
            Variant {
                tune: NO_PROBE.1,
                ..Variant::rig(one_left_on_v1_1)
            },
        ),
        ("half upgraded", Variant::rig(half_upgraded)),
        (
            "half upgraded, no last probe",
            Variant {
                tune: NO_PROBE.1,
                ..Variant::rig(half_upgraded)
            },
        ),
        ("one node upgraded", Variant::rig(one_upgraded)),
    ];
    for (label, variant) in &rows {
        for n in SIZES {
            let runs = sweep(n, seeds, variant, REPORT_DEADLINE_MS);
            println!("{}", summarize(label, n, &runs));
        }
        println!();
    }
    // A member cut off after formation, for 1.5 s (suspected, should be refuted) and
    // for 8 s (declared dead on both sides): how long until every view is whole again.
    for (label, for_ms) in [("cut 1.5 s", 1500), ("cut 8 s", 8000)] {
        for (who, flavor) in [
            ("v1.1.0", all_v1_1 as fn(usize, usize) -> Flavor),
            ("this tree", all_current),
        ] {
            for n in [5, 14, 30] {
                let label = format!("{label}, {who}");
                let runs = sweep(n, seeds, &cut_off(flavor, for_ms), REPORT_DEADLINE_MS);
                println!("{}", summarize(&label, n, &runs));
            }
        }
        println!();
    }
}

/// A formed cluster (certificates known all round) in which one member is cut off for
/// `for_ms`, a minute after formation.
fn cut_off(flavor: fn(usize, usize) -> Flavor, for_ms: u64) -> Variant {
    Variant {
        preshared_certs: true,
        isolate: Some(Isolate {
            at_ms: FAULT_AFTER_MS,
            for_ms,
            shape: Fault::Cut,
        }),
        ..Variant::rig(flavor)
    }
}

// ---------------------------------------------------------------------------------
// Regression tests. Bounds are the worst of 48 seeds on this tree, with about a
// factor of two of margin; the measured figures are in ADR 0022's 2026-10-12 amendment.
// ---------------------------------------------------------------------------------

/// Seeds each regression test sweeps at size `n`: fewer where a run is long, so the
/// suite stays within a minute of an unoptimised build on a CI runner. The report
/// (16 seeds) and the bounds' own measurement (48) are the wide sweeps.
fn seeds(n: usize) -> u64 {
    if n >= 20 {
        3
    } else {
        6
    }
}

/// Formation must have settled — every node holding every other `Alive`, for good —
/// within this long of the last process start. Formation time is linear in the size
/// (the restarted founder re-learns one member a second or so; worst observed 10 s at
/// N=10, 13 s at 14, 38 s at 30), so the bound is too.
fn settle_bound_ms(n: usize) -> u64 {
    10_000 + 2_000 * n as u64
}

/// Issue #971, the regression: a signed-gossip cluster forms, at every size the rig
/// runs and well beyond, without one healthy node suspecting — let alone burying —
/// another, and settles within a bound. On v1.1.0 the same formations raise 50
/// suspicions and 8 false deaths per node at 14 nodes and do not settle within five
/// minutes in a quarter of the seeds (see the next test).
#[test]
fn a_signed_cluster_forms_with_no_false_suspicion_at_any_size() {
    for arm_founder in [true, false] {
        let variant = Variant {
            arm_founder,
            ..Variant::rig(all_current)
        };
        for n in SIZES {
            let bound = settle_bound_ms(n);
            for (seed, o) in sweep(n, seeds(n), &variant, bound + 10_000)
                .iter()
                .enumerate()
            {
                let ctx = format!("N={n} seed {seed} arm_founder={arm_founder}");
                assert_eq!(
                    o.total_suspects(),
                    0,
                    "{ctx}: SUSPECT raised: {:?}",
                    o.suspects
                );
                assert_eq!(o.total_deads(), 0, "{ctx}: DEAD raised: {:?}", o.deads);
                assert!(
                    o.settled.is_some_and(|s| s <= bound),
                    "{ctx}: settled {:?}, bound {bound} ms",
                    o.settled
                );
                // A handful of datagrams cross a restart or a first exchange; the
                // storm was 50-340 per node.
                assert!(
                    o.total_cert_miss() as usize <= n,
                    "{ctx}: {} cert-miss drops",
                    o.total_cert_miss()
                );
                // The full certificate now rides more datagrams; none may outgrow
                // the budget fingerprinting exists to keep (ADR 0022 T6).
                assert!(
                    o.max_datagram <= MTU_BUDGET,
                    "{ctx}: a {} B datagram",
                    o.max_datagram
                );
            }
        }
    }
}

/// The harness must be able to SEE the bug it guards against: the same 14-node
/// formation with every node on v1.1.0's rules shows issue #971's signature — the
/// storm of `cert-miss` drops and false suspicions the rig logged (19-73 SUSPECT and
/// 3-12 DEAD per broker; 100-260 cert-miss) — and with certificates handed out in
/// advance it shows none of it, which is what pins the cause on their distribution.
#[test]
fn the_v1_1_rules_reproduce_the_formation_storm_and_preshared_certificates_remove_it() {
    let n = 14;
    let storm = sweep(n, 2, &Variant::rig(all_v1_1), 90_000);
    for (seed, o) in storm.iter().enumerate() {
        let per_node = |total: u32| f64::from(total) / 14.0;
        assert!(
            per_node(o.total_suspects()) >= 15.0,
            "seed {seed}: {} SUSPECT per node",
            per_node(o.total_suspects())
        );
        assert!(
            per_node(o.total_deads()) >= 1.0,
            "seed {seed}: {} DEAD per node",
            per_node(o.total_deads())
        );
        assert!(
            per_node(o.total_cert_miss()) >= 50.0,
            "seed {seed}: {} cert-miss per node",
            per_node(o.total_cert_miss())
        );
    }
    let preshared = Variant {
        preshared_certs: true,
        ..Variant::rig(all_v1_1)
    };
    for (seed, o) in sweep(n, 2, &preshared, 60_000).iter().enumerate() {
        assert_eq!(o.total_cert_miss(), 0, "seed {seed}");
        assert_eq!(o.total_suspects(), 0, "seed {seed}");
        assert_eq!(o.total_deads(), 0, "seed {seed}");
    }
}

/// A member falsely declared `Dead` is re-admitted in bounded time, whichever side
/// would have had to make the first move. One member of a formed cluster is cut off
/// for 8 s: everyone buries it, and it buries the members it probed meanwhile — so
/// some pairs have evicted EACH OTHER and neither probes the other again. No node may
/// hold a healthy peer out for longer than the tombstone (30 s, plus the round trip of
/// the parting greeting), and every view must be whole again within 90 s (worst
/// observed: 59 s). On v1.1.0 the mutual evictions never heal: at 5 nodes, 13 of 16
/// seeds are still split five minutes later.
#[test]
fn a_member_falsely_declared_dead_is_readmitted_within_the_tombstone_ttl() {
    let dead_ttl = Config::default().dead_ttl_ms;
    for n in [5, 14, 30] {
        let runs = sweep(n, seeds(n), &cut_off(all_current, 8000), 100_000);
        for (seed, o) in runs.iter().enumerate() {
            let ctx = format!("N={n} seed {seed}");
            assert!(
                o.total_deads() > 0,
                "{ctx}: the cut must produce false deaths"
            );
            assert!(
                o.evicted_at_deadline.is_empty(),
                "{ctx}: still evicted: {:?}",
                o.evicted_at_deadline
            );
            assert!(
                o.longest_eviction <= dead_ttl + 1000,
                "{ctx}: a healthy peer was held out for {} ms",
                o.longest_eviction
            );
            assert!(
                o.settled.is_some_and(|s| s <= 90_000),
                "{ctx}: settled {:?}",
                o.settled
            );
        }
    }
}

/// Mixed versions, as far as a simulation can show them. The v1.1.0 nodes here are
/// that release's driver and state-machine rules; every datagram between the two
/// kinds must be accepted (the harness fails on any drop that is not a `cert-miss`,
/// and on any datagram either side cannot decode — there is no new wire element).
///
/// - The last step of a rolling upgrade — one node still on v1.1.0 — forms as cleanly
///   as an upgraded cluster: the upgraded nodes prime the old one on first contact,
///   and ask it for its certificate when its fingerprint datagrams arrive first.
/// - Half way through, the v1.1.0 nodes still storm among themselves, exactly as
///   before the upgrade. But no pair with an upgraded node in it is left broken: any
///   eviction still standing two minutes on is between two v1.1.0 nodes.
#[test]
fn a_cluster_part_way_through_the_upgrade_from_v1_1_forms() {
    for n in [10, 14] {
        let bound = settle_bound_ms(n);
        let runs = sweep(n, seeds(n), &Variant::rig(one_left_on_v1_1), bound + 10_000);
        for (seed, o) in runs.iter().enumerate() {
            let ctx = format!("one node left on v1.1.0, N={n} seed {seed}");
            assert_eq!(o.total_deads(), 0, "{ctx}: DEAD raised: {:?}", o.deads);
            assert!(
                o.settled.is_some_and(|s| s <= bound),
                "{ctx}: settled {:?}, bound {bound} ms",
                o.settled
            );
        }

        let runs = sweep(n, seeds(n), &Variant::rig(half_upgraded), 120_000);
        for (seed, o) in runs.iter().enumerate() {
            for &(evictor, evicted) in &o.evicted_at_deadline {
                assert!(
                    half_upgraded(evictor, n) == Flavor::V1_1
                        && half_upgraded(evicted, n) == Flavor::V1_1,
                    "half upgraded, N={n} seed {seed}: {} still holds {} out, and they are \
                     not both on v1.1.0",
                    id_of(evictor),
                    id_of(evicted)
                );
            }
        }
    }
}

/// The harness is deterministic: one seed, one schedule, one outcome — the property
/// that makes a failure above reproducible from its seed.
#[test]
fn a_formation_run_is_reproducible_from_its_seed() {
    let run = || {
        format!(
            "{:?}",
            Sim::new(12, 7, Variant::rig(half_upgraded)).run(60_000)
        )
    };
    assert_eq!(run(), run());
}

// ---------------------------------------------------------------------------------
// A false `Dead` after a short outage (issues #973 and #975, ADR 0087 §7): how often,
// under which way of deciding that a suspect is dead, and under which faults.
// ---------------------------------------------------------------------------------

/// One way of deciding that a suspect is dead: the default, or an alternative measured
/// against it. Each is a function of the default configuration.
type Detector = (&'static str, fn(Config) -> Config);

/// The default: 4 s for one suspecter, 1.5 s for three, whatever the cluster's size,
/// and one last direct probe of the suspect before it is declared dead.
const DEFAULT: Detector = ("default: 4 s / 1.5 s, last direct probe", |cfg| cfg);
/// The default without the last probe: the rule of v1.1.0, and of every release up to
/// ADR 0016's amendment of 2026-10-13.
const NO_PROBE: Detector = ("no last probe (v1.1.0's rule)", |cfg| Config {
    confirm_before_dead: false,
    ..cfg
});
/// The SWIM paper's rule applied to the window, no last probe: both ends ×
/// `max(1, log10 N)`.
const LOG_N: Detector = ("no last probe, x log10(N)", |cfg| Config {
    confirm_before_dead: false,
    suspicion_scales_with_log_n: true,
    ..cfg
});
/// memberlist's LAN defaults (Lifeguard's dynamic suspicion), no last probe: the floor
/// is `4 × max(1, log10 N)` protocol periods, the ceiling six times the floor.
const MEMBERLIST: Detector = ("no last probe, memberlist: 4 log10(N) s, x6", |cfg| {
    Config {
        confirm_before_dead: false,
        suspicion_min_timeout_ms: 4000,
        suspicion_timeout_ms: 24_000,
        suspicion_scales_with_log_n: true,
        ..cfg
    }
});
/// The window simply doubled, no last probe, as a control for "is it only about length".
const DOUBLED: Detector = ("no last probe, doubled: 8 s / 3 s", |cfg| Config {
    confirm_before_dead: false,
    suspicion_timeout_ms: 8000,
    suspicion_min_timeout_ms: 3000,
    ..cfg
});
/// The default with the scaled window as well.
const LAST_PROBE_LOG_N: Detector = ("default, x log10(N)", |cfg| Config {
    suspicion_scales_with_log_n: true,
    ..cfg
});

/// A formed cluster on this tree (certificates known all round) running `detector`, in
/// which one member is cut off for `for_ms` a minute after formation.
fn cut_under(detector: Detector, for_ms: u64) -> Variant {
    cut_under_mixed(detector, all_current, for_ms)
}

/// [`cut_under`] with only the nodes `flavor` puts on this tree running `detector`;
/// the rest run v1.1.0, which has no last probe and knows none of the alternatives.
fn cut_under_mixed(detector: Detector, flavor: fn(usize, usize) -> Flavor, for_ms: u64) -> Variant {
    Variant {
        tune: detector.1,
        ..cut_off(flavor, for_ms)
    }
}

/// [`cut_under`] with a fault of another shape on the member, on a network that loses
/// `loss_permille` of its datagrams.
fn fault_under(detector: Detector, shape: Fault, for_ms: u64, loss_permille: u64) -> Variant {
    Variant {
        isolate: Some(Isolate {
            at_ms: FAULT_AFTER_MS,
            for_ms,
            shape,
        }),
        loss_permille,
        ..cut_under(detector, for_ms)
    }
}

/// A formed cluster on this tree running `detector` with no member at fault, on a
/// network that loses `loss_permille` of its datagrams.
fn loss_under(detector: Detector, loss_permille: u64) -> Variant {
    Variant {
        isolate: None,
        loss_permille,
        ..cut_under(detector, 0)
    }
}

/// What a sweep of one `(detector, size, fault)` cell saw.
struct CutCell {
    runs: usize,
    /// Runs in which some node declared a healthy member `Dead` at any time.
    any_dead: usize,
    /// Runs in which some node turned a member `Dead` AFTER the fault ended — declared
    /// it then, or adopted then a `Dead` another node had declared during it — about
    /// a member that was reachable again at that moment. (With no member at fault:
    /// after the loss began.)
    dead_after_cut: usize,
    /// `Dead` transitions per run (during and after the fault), median and worst.
    transitions: (u32, u32),
    /// `Suspect` transitions per run after the fault ended, median.
    suspicions: u32,
    /// The longest any node held a healthy member out after the fault ended, ms.
    longest_eviction: u64,
    /// Runs whose views were not whole again at the deadline.
    unsettled: usize,
    /// Runs in which a prober was stalled on its last probe
    /// ([`Variant::stall_prober_ms`]).
    prober_stalled: usize,
    /// Of those: the runs in which that prober gave up on a last probe of its own; the
    /// runs in which the member that had been cut off was turned `Dead`; and the runs
    /// in which the stalled prober itself was.
    prober_gave_up: usize,
    dead_about_isolated: usize,
    dead_about_prober: usize,
    /// Over all runs: `Dead` transitions that were a node's own declaration, and
    /// `Dead` transitions adopted about a member nobody had declared. Under the
    /// default every declaration is a last probe that ran out in silence (the first
    /// equals the silent count) and every other `Dead` descends from one, member by
    /// member (the second is zero).
    declared_deads: u64,
    untraced_deads: u64,
    /// Last direct probes over all runs: sent, answered by the suspect, silent.
    last_probes: (u64, u64, u64),
}

fn cut_cell(detector: Detector, n: usize, for_ms: u64, seeds: u64) -> CutCell {
    cut_cell_of(&cut_under(detector, for_ms), n, seeds)
}

fn cut_cell_of(variant: &Variant, n: usize, seeds: u64) -> CutCell {
    // Long enough for a tombstone (30 s) to be pruned and the member greeted back.
    let runs = sweep(n, seeds, variant, 75_000);
    let mut transitions: Vec<u32> = runs
        .iter()
        .map(|o| o.deads_during_cut + o.total_deads())
        .collect();
    transitions.sort_unstable();
    let mut suspicions: Vec<u32> = runs.iter().map(Outcome::total_suspects).collect();
    suspicions.sort_unstable();
    CutCell {
        runs: runs.len(),
        any_dead: transitions.iter().filter(|t| **t > 0).count(),
        dead_after_cut: runs.iter().filter(|o| o.total_deads() > 0).count(),
        transitions: (
            transitions[transitions.len() / 2],
            transitions[transitions.len() - 1],
        ),
        suspicions: suspicions[suspicions.len() / 2],
        longest_eviction: runs.iter().map(|o| o.longest_eviction).max().unwrap_or(0),
        unsettled: runs.iter().filter(|o| o.settled.is_none()).count(),
        prober_stalled: runs.iter().filter(|o| o.stalled_prober.is_some()).count(),
        prober_gave_up: runs
            .iter()
            .filter(|o| o.stalled_prober.is_some_and(|p| p.gave_up))
            .count(),
        dead_about_isolated: runs
            .iter()
            .filter(|o| o.stalled_prober.is_some() && o.dead_about_isolated)
            .count(),
        dead_about_prober: runs
            .iter()
            .filter(|o| o.stalled_prober.is_some_and(|p| p.turned_dead))
            .count(),
        declared_deads: runs.iter().map(|o| o.declared_deads).sum(),
        untraced_deads: runs.iter().map(|o| o.untraced_deads).sum(),
        last_probes: runs.iter().fold((0, 0, 0), |sum, o| {
            (
                sum.0 + o.last_probes.0,
                sum.1 + o.last_probes.1,
                sum.2 + o.last_probes.2,
            )
        }),
    }
}

/// What holds in a broker, whose ticks are on nobody's grid: the length of the longest
/// clean cut that CANNOT end in a `Dead` under the default detector on a network that
/// loses nothing, whatever the phase of each node's ticks and however late they come.
/// A deadline is acted on at the first wake-up at or after it, which may be the
/// deadline itself: two ack timeouts and the window's floor, less a datagram's latency
/// each way. 2 s less twice the latency.
fn last_probe_floor_ms(cfg: &Config) -> u64 {
    let window = cfg.suspicion_min_timeout_ms.min(cfg.suspicion_timeout_ms);
    2 * cfg.ack_timeout_ms + window - 2 * MAX_LATENCY_MS
}

/// The same when every node's ticks land exactly on a grid of [`TICK_MS`], as this
/// simulation's do unless it is told otherwise ([`Late`]): each ack timeout is then in
/// effect rounded up to a tick. The SIMULATION's figure — a broker gets the margin
/// only in the schedules where its ticks happen to fall so.
/// [`BOUND_DERIVATION`] is the argument; this is its last line.
fn last_probe_bound_ms(cfg: &Config) -> u64 {
    // A deadline is acted on by the first tick at or after it.
    let on_a_tick = |ms: u64| ms.div_ceil(TICK_MS) * TICK_MS;
    let suspected_after = 2 * on_a_tick(cfg.ack_timeout_ms);
    let window = cfg.suspicion_min_timeout_ms.min(cfg.suspicion_timeout_ms);
    suspected_after + window - 2 * MAX_LATENCY_MS
}

/// Why a member cut off for no longer than [`last_probe_bound_ms`] is declared `Dead`
/// by no node. Printed when the bound test fails, which it does when a timeout or the
/// tick is changed.
const BOUND_DERIVATION: &str = "\
A node declares a member Dead on its own account only when a last direct probe of it
runs out unanswered; so if every last probe is sent after the cut has ended, the member
answers each, and there is no Dead for anyone to adopt. The cut begins at c.

  1. The earliest probe the cut leaves unanswered started at c - 2 x latency (its Ping
     arrived just before c and its Ack just after).
  2. Its prober acts on deadlines at its ticks. The ack timeout runs out and it asks its
     helpers at the first tick at or after start + ack timeout; the indirect timeout
     runs out one more such step later. So no node suspects the member before
         start + 2 x roundup(ack timeout, tick).
  3. The suspicion window is never shorter than its floor (three suspecters), and local
     health only lengthens it. So no last probe is sent before
         start + 2 x roundup(ack timeout, tick) + window floor.
  4. With the defaults: 2 x roundup(250, 100) + 1500 - 2 x 1 = 2098 ms after c.

A cut of up to 2098 ms has therefore ended before the first last probe is sent — ON THE
SIMULATION'S GRID, where every tick lands exactly 100 ms after the one before. The 98 ms
over a 2000 ms cut is all the tick, and a broker cannot count on it: a tick that comes
50 ms late acts on a 250 ms deadline at 250 ms, not 300. Whatever the ticks do, a
deadline is not acted on before it, so what always holds is
    2 x 250 + 1500 - 2 x latency = 2 s less twice the latency (1998 ms here).
(The member's own suspicions of others need no line: it hears nobody, so it is the only
suspecter, and its window is the full 4 s.)";

/// How long the survivors took to bury a member whose fault never ends: ms from its
/// start to the last survivor's `Dead`, per run that got there, sorted.
struct Burial {
    times: Vec<u64>,
    /// Runs in which some survivor had not declared it `Dead` two minutes on.
    unburied: usize,
}

impl Burial {
    fn median(&self) -> Option<u64> {
        self.times.get(self.times.len() / 2).copied()
    }
    fn worst(&self) -> Option<u64> {
        self.times.last().copied()
    }
    /// Median and worst, when every run buried the member.
    fn all(&self) -> Option<(u64, u64)> {
        if self.unburied > 0 {
            return None;
        }
        self.median().zip(self.worst())
    }
}

impl std::fmt::Display for Burial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.median().zip(self.worst()) {
            Some((med, worst)) => write!(f, "{med} / {worst}")?,
            None => write!(f, "never")?,
        }
        if self.unburied > 0 {
            write!(f, " ({} runs not buried)", self.unburied)?;
        }
        Ok(())
    }
}

/// How long `detector` takes to bury a member that really died: the cut never ends.
fn burial_ms(detector: Detector, n: usize, seeds: u64) -> Burial {
    burial_under(detector, Fault::Cut, 0, n, seeds)
}

/// [`burial_ms`] for a member with a fault of `shape` that never ends, on a network
/// that loses `loss_permille` of its datagrams.
fn burial_under(
    detector: Detector,
    shape: Fault,
    loss_permille: u64,
    n: usize,
    seeds: u64,
) -> Burial {
    let variant = fault_under(detector, shape, FOREVER_MS, loss_permille);
    // Two minutes after the fault began.
    let runs = sweep(n, seeds, &variant, 120_000);
    let mut times: Vec<u64> = runs.iter().filter_map(|o| o.buried_after).collect();
    times.sort_unstable();
    Burial {
        unburied: runs.len() - times.len(),
        times,
    }
}

const CUT_SIZES: [usize; 5] = [3, 5, 10, 14, 30];
/// 2 s and 2.5 s sit either side of where the last probe's bound ends
/// ([`last_probe_bound_ms`]: 2098 ms).
const CUTS_MS: [u64; 7] = [500, 1000, 1500, 2000, 2500, 3000, 5000];

/// One cell of a report: `runs with any DEAD / runs with a DEAD after the fault ended
/// [DEAD transitions median..worst; SUSPECT median; longest eviction, s; unsettled]`.
fn show(label: &str, c: &CutCell) -> String {
    #[allow(clippy::cast_precision_loss)] // report formatting of small numbers
    let evicted_s = c.longest_eviction as f64 / 1000.0;
    format!(
        " | {label} {:>2}/{:>2} [{}..{}; {}; {evicted_s:.0}s; {}]",
        c.any_dead, c.dead_after_cut, c.transitions.0, c.transitions.1, c.suspicions, c.unsettled,
    )
}

fn seconds(ms: u64) -> String {
    #[allow(clippy::cast_precision_loss)] // report formatting of small numbers
    let s = ms as f64 / 1000.0;
    format!("{s:.1}s")
}

const CELL_LEGEND: &str = "runs with any DEAD / runs in which a node turned a member DEAD \
     after the fault ended [DEAD transitions median..worst; SUSPECT transitions after the \
     fault, median; longest eviction after the fault, s; unsettled runs]";

/// The measurement behind ADR 0087 §7, ADR 0016's amendment of 2026-10-13 and issue
/// #975: for each detector, cluster size and cut length, how many of 32 seeds end with
/// a healthy member declared `Dead`; the same in a cluster part-way through the upgrade
/// from v1.1.0; and what each detector costs when the member really is dead. Not an
/// assertion —
/// `cargo test -p mqtt-cluster --release --test swim_formation_sim false_dead_report -- --ignored --nocapture`.
#[test]
#[ignore = "measurement report for issues #973 and #975; run with --ignored --nocapture"]
fn false_dead_report() {
    let seeds = 32;
    let detectors = [
        DEFAULT,
        NO_PROBE,
        LOG_N,
        DOUBLED,
        MEMBERLIST,
        LAST_PROBE_LOG_N,
    ];
    println!("false DEAD after one member is cut off, {seeds} seeds per cell: {CELL_LEGEND}");
    // The default's last probes, from the same runs as its row.
    let mut probes = Vec::new();
    for detector in detectors {
        println!("\n{}", detector.0);
        for n in CUT_SIZES {
            let mut line = format!("  N={n:<3}");
            let mut counts = format!("  N={n:<3}");
            for for_ms in CUTS_MS {
                let c = cut_cell(detector, n, for_ms, seeds);
                assert_eq!(c.runs, usize::try_from(seeds).expect("small"));
                line.push_str(&show(&seconds(for_ms), &c));
                let (sent, spared, silent) = c.last_probes;
                let _ = write!(counts, " | {} {sent}/{spared}/{silent}", seconds(for_ms));
            }
            println!("{line}");
            if detector.0 == DEFAULT.0 {
                probes.push(counts);
            }
        }
    }
    println!(
        "\n{}: its last probes over the {seeds} runs of a cell — sent / answered by the \
         suspect / silent",
        DEFAULT.0
    );
    for line in probes {
        println!("{line}");
    }
    // A rolling upgrade: only the upgraded nodes probe a suspect once more; a v1.1.0
    // node is probed and answers as it always has, and declares as it always has.
    println!("\nthe default in a cluster part-way through the upgrade from v1.1.0, N=14");
    for (label, flavor) in [
        (
            "every node upgraded",
            all_current as fn(usize, usize) -> Flavor,
        ),
        ("one node left on v1.1.0", one_left_on_v1_1),
        ("half the nodes on v1.1.0", half_upgraded),
        ("one node upgraded", one_upgraded),
        ("every node on v1.1.0", all_v1_1),
    ] {
        let mut line = format!("  {label:<25}");
        for for_ms in CUTS_MS {
            let c = cut_cell_of(&cut_under_mixed(DEFAULT, flavor, for_ms), 14, seeds);
            line.push_str(&show(&seconds(for_ms), &c));
        }
        println!("{line}");
    }
    println!(
        "\na member that really died: ms until the last survivor holds it DEAD, median / worst"
    );
    for detector in detectors {
        let cells: Vec<String> = CUT_SIZES
            .into_iter()
            .map(|n| format!("N={n} {}", burial_ms(detector, n, 16)))
            .collect();
        println!("  {:<44} | {}", detector.0, cells.join(" | "));
    }
}

/// The measurement behind ADR 0016's amendments of 2026-10-13, beyond a clean cut on a
/// lossless network: the default and the same detector without its last probe, under
/// packet loss, a member reachable one way only, one link lost one way, a member whose
/// datagrams arrive late, and a node that was itself stalled — as a member and as the
/// prober whose last probe is out. Each pair of rows runs the same seeds, and under
/// loss the same network ([`Sim::lost`]). Not an assertion —
/// `cargo test -p mqtt-cluster --release --test swim_formation_sim fault_shapes_report -- --ignored --nocapture`.
#[test]
#[ignore = "measurement report for issue #975; run with --ignored --nocapture"]
fn fault_shapes_report() {
    let seeds = 32;
    let pair = [DEFAULT, NO_PROBE];
    let losses = [50, 100, 200];
    println!("{seeds} seeds per cell: {CELL_LEGEND}");

    println!("\nno member at fault; every datagram lost with the given probability");
    for detector in pair {
        println!("  {}", detector.0);
        let columns = losses.map(|loss| (format!("{}%", loss / 10), loss_under(detector, loss)));
        print_grid(&columns, seeds);
    }

    println!(
        "\none member cut off for 1.5 s on a network that has that loss from the moment of \
         the cut"
    );
    for detector in pair {
        println!("  {}", detector.0);
        let columns = losses.map(|loss| {
            (
                format!("{}%", loss / 10),
                fault_under(detector, Fault::Cut, 1500, loss),
            )
        });
        print_grid(&columns, seeds);
    }

    println!(
        "\na member that really died, on a network that has that loss from the moment it \
         does: ms until the last survivor holds it DEAD, median / worst of 16"
    );
    for detector in pair {
        println!("  {}", detector.0);
        for loss in losses {
            let cells = burials(detector, Fault::Cut, loss);
            println!("    {:>2}% | {cells}", loss / 10);
        }
    }

    println!(
        "\na member reachable one way only, for good: ms until the last survivor holds it \
         DEAD, median / worst of 16"
    );
    for (label, shape) in [
        ("deaf (hears nothing, is heard)", Fault::Deaf),
        ("mute (hears, is not heard)", Fault::Mute),
    ] {
        for detector in pair {
            let cells = burials(detector, shape, 0);
            println!("  {label:<31} {:<40} | {cells}", detector.0);
        }
    }

    println!("\na member reachable one way only, for a while");
    for (label, shape) in [("deaf", Fault::Deaf), ("mute", Fault::Mute)] {
        for detector in pair {
            println!("  {label}, {}", detector.0);
            let columns = [1500, 2000, 3000]
                .map(|for_ms| (seconds(for_ms), fault_under(detector, shape, for_ms, 0)));
            print_grid(&columns, seeds);
        }
    }

    println!(
        "\none link lost one way (what the founder sends one member never arrives), for a \
         minute and for good"
    );
    for detector in pair {
        println!("  {}", detector.0);
        let columns = [("60s", 60_000), ("for good", FOREVER_MS)].map(|(label, for_ms)| {
            (
                label.to_string(),
                fault_under(detector, Fault::DeafToOne, for_ms, 0),
            )
        });
        print_grid(&columns, seeds);
    }

    println!("\none member's datagrams arrive late, for 20 s");
    for detector in pair {
        println!("  {}", detector.0);
        let columns = [200, 300, 600, 1000, 2000].map(|late| {
            (
                format!("{late}ms"),
                fault_under(detector, Fault::Slow(late), 20_000, 0),
            )
        });
        print_grid(&columns, seeds);
    }
    report_stalled_nodes(seeds);
}

/// The part of [`fault_shapes_report`] about a node that was itself stalled (issue
/// #1002): a node that stops and resumes is a silent member AND a prober whose
/// deadlines all passed while it was away. The second row of each block is the
/// default's state machine on the driver loop as it was before it read its socket ahead
/// of a tick.
fn report_stalled_nodes(seeds: u64) {
    let stalled_rows = [
        (DEFAULT.0, DEFAULT, Shell::Current),
        (
            "default, on the driver loop before this change",
            DEFAULT,
            Shell::TickFirst,
        ),
        (NO_PROBE.0, NO_PROBE, Shell::Current),
    ];
    println!(
        "\none node stopped and resumed (its ticks stop, what is sent to it queues on its \
         socket), nobody else at fault"
    );
    for (label, detector, shell) in stalled_rows {
        println!("  {label}");
        let columns = [1000, 2000, 3000, 5000, 10_000].map(|for_ms| {
            (
                seconds(for_ms),
                Variant {
                    shell,
                    ..fault_under(detector, Fault::Stall, for_ms, 0)
                },
            )
        });
        print_grid(&columns, seeds);
    }
    report_stalled_probers(seeds);
    report_late_nodes(seeds);
}

/// The stalled PROBER, at the worst moment: per cell, the runs in which a node was
/// stopped on its last probe, and of those the runs in which that node gave up on a
/// probe of its own, the runs in which the member that had been cut off was turned
/// `Dead`, and the runs in which the stopped node itself was (it is silent for as long
/// as it is stopped, which is a second fault).
fn report_stalled_probers(seeds: u64) {
    println!(
        "\none member cut off for 2 s, and the first node to send a last probe stopped for \
         the given time from the instant it has sent it: runs in which a node was so \
         stopped / of those, runs in which it gave up on a last probe of its own / runs in \
         which the member cut off was turned DEAD / runs in which the stopped node was \
         [last probes over all runs: sent, answered, silent]"
    );
    for (label, shell) in [
        (DEFAULT.0, Shell::Current),
        (
            "default, on the driver loop before this change",
            Shell::TickFirst,
        ),
    ] {
        println!("  {label}");
        for n in CUT_SIZES {
            let mut line = format!("    N={n:<3}");
            for stall in [300, 1000, 2000] {
                let variant = Variant {
                    shell,
                    stall_prober_ms: Some(stall),
                    ..cut_under(DEFAULT, PROMISED_CUT_MS)
                };
                let c = cut_cell_of(&variant, n, seeds);
                let _ = write!(
                    line,
                    " | {} {:>2}/{:>2}/{:>2}/{:>2} [{}, {}, {}]",
                    seconds(stall),
                    c.prober_stalled,
                    c.prober_gave_up,
                    c.dead_about_isolated,
                    c.dead_about_prober,
                    c.last_probes.0,
                    c.last_probes.1,
                    c.last_probes.2,
                );
            }
            println!("{line}");
        }
    }
}

/// One block of a report: a line per cluster size, a cell per `(label, variant)`.
fn print_grid(columns: &[(String, Variant)], seeds: u64) {
    for n in CUT_SIZES {
        let mut line = format!("    N={n:<3}");
        for (label, variant) in columns {
            line.push_str(&show(label, &cut_cell_of(variant, n, seeds)));
        }
        println!("{line}");
    }
}

/// One line of a report: per cluster size, how long the survivors took to bury a
/// member whose fault of `shape` never ends, over 16 seeds.
fn burials(detector: Detector, shape: Fault, loss_permille: u64) -> String {
    CUT_SIZES
        .into_iter()
        .map(|n| {
            format!(
                "N={n} {}",
                burial_under(detector, shape, loss_permille, n, 16)
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The two seconds of the reports' "2 s" column: a cut the simulation's grid spares,
/// and two milliseconds more than a broker is promised.
const PROMISED_CUT_MS: u64 = 2000;
/// Where the bound ends on the simulation's exact tick grid, as the documents give it
/// (ADR 0016's amendment for issue #1002, ADR 0087 §7).
const GRID_BOUND_MS: u64 = 2098;
/// What the documents promise of a broker: two seconds less twice the network's
/// latency, which at this simulation's latency is this.
const DOCUMENTED_FLOOR_MS: u64 = 1998;

/// The bound the default detector keeps ON THE SIMULATION'S TICK GRID (ADR 0016's
/// amendments of 2026-10-13 for issues #975 and #1002, ADR 0087 T10): a member of a
/// formed cluster that is cut off — nothing in, nothing out — for up to 2 s is
/// declared `Dead` by no node, and not suspected for long either: every view is whole
/// again within the window. What a broker is promised, whose ticks are on no grid, is
/// the next test's.
///
/// The bound is arithmetic ([`BOUND_DERIVATION`]), so it is tested as arithmetic: the
/// figure is computed from the broker's own timeouts and tick, must cover two
/// seconds, and must be the figure the documents give for the grid; and a cut of
/// exactly that length — to the millisecond the last one the argument covers — ends
/// in no `Dead` at 5, 14 and 30 nodes. Change a timeout or the tick and this fails
/// with the derivation. Past the bound nothing is promised: the report's 2.5 s column
/// is not zero.
///
/// A member that really died is still buried by every survivor, one ack timeout later.
#[test]
fn a_member_cut_off_for_up_to_two_seconds_is_not_declared_dead() {
    let cfg = Config::default();
    let bound = last_probe_bound_ms(&cfg);
    let timings = format!(
        "ack timeout {} ms, suspicion window {} ms down to {} ms, driver tick {TICK_MS} ms, \
         latency up to {MAX_LATENCY_MS} ms: the bound is {bound} ms",
        cfg.ack_timeout_ms, cfg.suspicion_timeout_ms, cfg.suspicion_min_timeout_ms,
    );
    assert!(
        bound >= PROMISED_CUT_MS,
        "the last probe no longer covers a {PROMISED_CUT_MS} ms cut.\n{timings}\n\n{BOUND_DERIVATION}"
    );
    assert_eq!(
        bound, GRID_BOUND_MS,
        "the bound has moved; the documents that state it must move with it.\n{timings}\n\n\
         {BOUND_DERIVATION}"
    );
    // A handful of runs per size: this is every tier's test, and the 32-seed table is
    // the report's.
    for (n, cuts) in [
        (5, &[1500, PROMISED_CUT_MS, bound][..]),
        (14, &[500, PROMISED_CUT_MS, bound]),
        (30, &[bound]),
    ] {
        for &for_ms in cuts {
            let c = cut_cell(DEFAULT, n, for_ms, seeds(n));
            assert_eq!(
                c.any_dead, 0,
                "N={n}, cut {for_ms} ms: {} of {} runs turned a member DEAD.\n{timings}\n\n\
                 {BOUND_DERIVATION}",
                c.any_dead, c.runs
            );
            assert_eq!(c.unsettled, 0, "N={n}, cut {for_ms} ms");
        }
    }
    let without = burial_ms(NO_PROBE, 14, 3)
        .all()
        .expect("a dead member is buried without the probe");
    let with = burial_ms(DEFAULT, 14, 3)
        .all()
        .expect("a dead member is still buried");
    assert!(
        with.1 <= without.1 + 3000,
        "burying a dead member took {} ms; without the last probe, {} ms",
        with.1,
        without.1
    );
}

/// Every node's ticks late by anything up to a whole tick, each time, for the whole
/// run: no tick is on a grid, and a deadline can be acted on the instant it falls due.
fn late_ticks(variant: &Variant) -> Variant {
    Variant {
        late: Some(Late {
            lo: 0,
            hi: TICK_MS - 1,
            for_ms: FOREVER_MS,
            stalls: false,
        }),
        ..variant.clone()
    }
}

/// Every node's ticks on a grid half as fine as the broker's: a tick lands on every
/// instant a broker's tick up to half a tick late could, a 250 ms deadline included.
fn half_ticks(variant: &Variant) -> Variant {
    Variant {
        tick_ms: TICK_MS / 2,
        ..variant.clone()
    }
}

/// What a BROKER is promised (ADR 0016's amendment for issue #1002): its ticks are on
/// no grid — the timer fires late, the loop is scheduled late, and a late tick shifts
/// every one after it — so the tick's rounding is a margin it cannot count on. With
/// every node's ticks late by a different amount each time, a member cut off for two
/// seconds less twice the network's latency is declared `Dead` by no node: no
/// deadline is acted on before it falls due, so no node suspects the member sooner
/// than two ack timeouts into the cut, nor asks it the last time sooner than the
/// window's floor after that. Arithmetic again, and pinned as arithmetic.
///
/// The same on a 50 ms grid, where a tick lands exactly on each 250 ms deadline — the
/// schedule the grid's margin does not survive.
///
/// Between that and the grid's 2098 ms a verdict needs a schedule in which a
/// prober's ticks land on its deadlines and two more nodes suspect in step. The
/// report sweeps both ways of leaving the grid, 32 seeds a cell, and finds none up
/// to 2.2 s: what is promised is the arithmetic, not the sweep.
#[test]
fn with_ticks_on_no_grid_a_cut_of_two_seconds_less_twice_the_latency_ends_in_no_dead() {
    let cfg = Config::default();
    let floor = last_probe_floor_ms(&cfg);
    assert_eq!(
        floor, DOCUMENTED_FLOOR_MS,
        "what a broker is promised has moved; the documents that state it must move with \
         it: two ack timeouts ({} ms each) and the window's floor ({} ms), less twice the \
         latency ({MAX_LATENCY_MS} ms)",
        cfg.ack_timeout_ms, cfg.suspicion_min_timeout_ms,
    );
    assert!(floor <= PROMISED_CUT_MS && PROMISED_CUT_MS <= last_probe_bound_ms(&cfg));
    for n in [5, 14] {
        for for_ms in [1500, floor] {
            let c = cut_cell_of(&late_ticks(&cut_under(DEFAULT, for_ms)), n, seeds(n));
            assert_eq!(
                c.any_dead, 0,
                "N={n}, cut {for_ms} ms, ticks on no grid: {} of {} runs turned a member DEAD",
                c.any_dead, c.runs
            );
            assert_eq!(c.unsettled, 0, "N={n}, cut {for_ms} ms");
        }
    }
    let c = cut_cell_of(&half_ticks(&cut_under(DEFAULT, floor)), 14, seeds(14));
    assert_eq!(
        c.any_dead, 0,
        "cut {floor} ms, ticks on a 50 ms grid: {} of {} runs turned a member DEAD",
        c.any_dead, c.runs
    );
}

/// Survivors that are all persistently late: from the moment a member dies, every
/// other node's driver wakes 300 to 500 ms late, every time, for 30 s, and does not
/// run in between.
fn all_survivors_late(shell: Shell) -> Variant {
    Variant {
        shell,
        late: Some(Late {
            lo: 300,
            hi: 500,
            for_ms: 30_000,
            stalls: true,
        }),
        ..fault_under(DEFAULT, Fault::Cut, FOREVER_MS, 0)
    }
}

/// How long the survivors take to bury a member that really died, under `variant`.
fn burial_of(variant: &Variant, n: usize, seeds: u64) -> Burial {
    let runs = sweep(n, seeds, variant, 240_000);
    let mut times: Vec<u64> = runs.iter().filter_map(|o| o.buried_after).collect();
    times.sort_unstable();
    Burial {
        unburied: runs.len() - times.len(),
        times,
    }
}

/// Detection of a REAL death must not degrade because the nodes doing the detecting
/// are late (issue #1002's review). A member dies, and from that moment every survivor
/// wakes 300 to 500 ms late, every time, for 30 s — in a cluster of two the one
/// survivor has nobody left to probe but the corpse, so nothing can rest on a probe
/// being answered.
///
/// The state machine's clock is the real one, so a late node takes each of its five
/// steps to the verdict at most one gap between wake-ups (the tick plus the lateness)
/// after it was due, and nothing builds up beyond those. The lone survivor of two
/// holds the member `Dead` within five such gaps of when a node running on time
/// does. In a cluster of fourteen the last survivor's verdict also waits on the
/// rounds it spends on other members first (each at most a gap late: the probe
/// period is kept to its schedule) and on gossip passed between late nodes; it comes
/// within twice the time a cluster on time takes (in the report: 7.0 s in the median
/// against 5.6 s, 8.9 s at worst against 8.3 s).
///
/// (The first version of this change left stalled time out of the clock and raised
/// the node's awareness on every stall. Under this same lateness the lone survivor
/// took 64 s to see the death, against 5.7 s, and fourteen and thirty nodes took 38 s:
/// they saw it only once the lateness was over. The loop before either sees it about
/// a second sooner than this one at 14 and 30 nodes, and for a bad reason: reading
/// its socket after the tick, it raises ten times the suspicions — of healthy peers
/// that are merely late — and the datagrams they cost carry the news.)
#[test]
fn a_real_death_is_seen_on_time_by_survivors_that_are_all_late() {
    for n in [2, 14] {
        let on_time = burial_ms(DEFAULT, n, seeds(n))
            .all()
            .expect("a dead member is buried");
        let late = burial_of(&all_survivors_late(Shell::Current), n, seeds(n))
            .all()
            .unwrap_or_else(|| panic!("N={n}: late survivors did not bury a dead member"));
        let bound = if n == 2 {
            on_time.1 + 5 * (TICK_MS + 500)
        } else {
            2 * on_time.1
        };
        assert!(
            late.1 <= bound,
            "N={n}: on time, the last survivor holds a dead member DEAD after {} ms at \
             worst; every survivor 300-500 ms late: {} ms (bound {bound} ms)",
            on_time.1,
            late.1
        );
    }
}

/// The bound holds when the PROBER is the one that stalls (issue #1002). A member is cut
/// off for two seconds; the first node to send it a last probe stops on the spot — its
/// ticks stop, what is sent to it queues on its socket — for four ack timeouts, and the
/// member's `Ack` is in that queue when it wakes. On this tree's driver loop that node
/// gives up on no probe, and the member that was cut off is turned `Dead` in no run:
/// the queue is read before the tick.
///
/// The simulation must be able to see what it guards against: the same state machine
/// on the loop as it was — the wall clock, the tick ahead of the queue — gives up on
/// the probe, and turns its suspect `Dead`, in EVERY run in which a prober was so
/// stalled. (Its suspect is usually the member that was cut off and sometimes a third
/// node that member had suspected during its outage — a node that was never at fault.)
///
/// Both halves are properties of each run, not counts of how many runs go which way;
/// the seeds only have to supply one run in which a last probe is sent at all (17 of
/// 32 in the report's cell; twelve runs with none has a chance of one in ten thousand).
///
/// What is NOT asserted is that no member at all is turned `Dead`. The node that
/// stopped is itself silent for as long as it is stopped. That alone is inside the
/// bound; but the member that was cut off may still hold it suspect from its own
/// outage, on a window that began then, and ask it the last time while it is stopped.
/// Two faults whose silences add up are past the bound: in the report, one run of the
/// 64 with a prober stopped for 2 s.
#[test]
fn a_prober_stalled_on_its_last_probe_declares_nobody_dead() {
    let n = 14;
    let stalled = |shell| Variant {
        shell,
        stall_prober_ms: Some(4 * Config::default().ack_timeout_ms),
        ..cut_under(DEFAULT, PROMISED_CUT_MS)
    };
    let now = cut_cell_of(&stalled(Shell::Current), n, 12);
    assert!(
        now.prober_stalled > 0,
        "no run of {} sent a last probe: nothing was stalled, nothing was shown",
        now.runs
    );
    assert_eq!(
        now.prober_gave_up, 0,
        "of {} probers stalled on their last probe, {} gave up on one on waking",
        now.prober_stalled, now.prober_gave_up
    );
    assert_eq!(
        now.dead_about_isolated, 0,
        "the member that had been cut off was turned DEAD in {} of {} such runs",
        now.dead_about_isolated, now.prober_stalled
    );
    assert_eq!(now.unsettled, 0);

    let before = cut_cell_of(&stalled(Shell::TickFirst), n, 12);
    assert_eq!(
        before.prober_stalled, now.prober_stalled,
        "the two loops are the same run up to the stall"
    );
    assert_eq!(
        before.prober_gave_up, before.prober_stalled,
        "on the loop as it was, a prober stalled past its ack timeout gives up on waking"
    );
    assert!(
        before.any_dead >= before.prober_stalled && before.dead_about_isolated > 0,
        "and its suspect is DEAD: {} runs with a DEAD, {} of them the member cut off, of \
         {} with a stalled prober",
        before.any_dead,
        before.dead_about_isolated,
        before.prober_stalled
    );
}

/// A node that stops and resumes — a stopped process, a suspended VM — is a silent
/// member and a stalled prober at once. Stopped for two seconds it is declared dead by
/// nobody and declares nobody dead, as a member cut off for two seconds is not: its
/// own deadlines did not run while it was away.
#[test]
fn a_node_stopped_for_two_seconds_neither_dies_nor_declares() {
    for n in [5, 14] {
        let c = cut_cell_of(
            &fault_under(DEFAULT, Fault::Stall, PROMISED_CUT_MS, 0),
            n,
            seeds(n),
        );
        assert_eq!(c.any_dead, 0, "N={n}: {} of {} runs", c.any_dead, c.runs);
        assert_eq!(c.unsettled, 0, "N={n}");
    }
}

/// What the last direct probe is for, pinned from the other side: with it off
/// (`confirm_before_dead = false` — v1.1.0's rule, and how this simulation models a
/// v1.1.0 node) a member of a formed 14-node cluster that is cut off for the two
/// seconds the default spares, half the window, is declared `Dead` by some node in two
/// schedules of three (21 of 32 seeds in the report), after the cut has ended, and
/// each such verdict stands for the tombstone's 30 s.
///
/// "Some schedule ends in a `Dead`" is a statement about seeds, and a change to SWIM's
/// traffic that has nothing to do with this rule moves which seeds. So the sweep is
/// wide enough not to care: at the report's rate the chance that twelve runs show none
/// is three in a million. (That the default spares the very same cut is not a matter of
/// seeds at all: `a_member_cut_off_for_up_to_two_seconds_is_not_declared_dead`.)
#[test]
fn without_the_last_probe_a_short_cut_ends_in_a_false_dead() {
    let c = cut_cell(NO_PROBE, 14, PROMISED_CUT_MS, 12);
    assert!(
        c.dead_after_cut >= 1,
        "no run of {} turned a reachable member DEAD",
        c.runs
    );
    assert!(
        c.longest_eviction >= Config::default().dead_ttl_ms - 1000,
        "a false verdict stands until its tombstone is pruned: {} ms",
        c.longest_eviction
    );
    assert_eq!(c.unsettled, 0, "and is then healed (issue #971)");
    // The v1.1.0 model is the same rule: a cluster of v1.1.0 nodes sees it too.
    let c = cut_cell_of(&cut_under_mixed(DEFAULT, all_v1_1, PROMISED_CUT_MS), 14, 12);
    assert!(
        c.dead_after_cut >= 1,
        "v1.1.0: no run of {} turned a reachable member DEAD",
        c.runs
    );
}

/// A cluster part-way through the upgrade from v1.1.0 gets the bound only as it
/// upgrades: a v1.1.0 node declares a suspect `Dead` without asking it, and a `Dead`
/// it declared is adopted from gossip by the upgraded nodes like any other. With half
/// of 14 nodes still on v1.1.0 a 2 s cut — which no upgraded cluster turns into a
/// verdict — still ends in a false `Dead` in some schedules (14 of 32 in the report;
/// 21 with every node on v1.1.0), and every such verdict is healed.
///
/// A statement about seeds, like the test above; sixteen runs showing none has a
/// chance of one in ten thousand at the report's rate.
#[test]
fn a_v1_1_node_still_declares_what_the_last_probe_would_have_spared() {
    let c = cut_cell_of(
        &cut_under_mixed(DEFAULT, half_upgraded, PROMISED_CUT_MS),
        14,
        16,
    );
    assert!(
        c.any_dead >= 1,
        "the limit this test documents is gone: no run of {} turned a member DEAD",
        c.runs
    );
    assert_eq!(c.unsettled, 0);
}

/// The last probe spares a suspect only on a round trip between the prober and the
/// suspect, so it keeps no member that cannot be reached: one that is gone, one that
/// hears nothing (its own datagrams still arriving — acks to nothing, probes of its
/// own), and one that hears everything and cannot answer are each declared `Dead` by
/// every survivor, about as soon as without the probe.
#[test]
fn a_member_that_cannot_complete_a_round_trip_is_declared_dead_as_before() {
    for shape in [Fault::Cut, Fault::Deaf, Fault::Mute] {
        for n in [5, 14] {
            let without = burial_under(NO_PROBE, shape, 0, n, 3)
                .all()
                .unwrap_or_else(|| panic!("{shape:?}, N={n}: not buried without the probe"));
            let with = burial_under(DEFAULT, shape, 0, n, 3)
                .all()
                .unwrap_or_else(|| panic!("{shape:?}, N={n}: a silent member was kept"));
            assert!(
                with.1 <= without.1 + 3000,
                "{shape:?}, N={n}: buried after {} ms; without the last probe, {} ms",
                with.1,
                without.1
            );
        }
    }
}

/// One link lost one way — what one node sends a member never arrives, everything
/// else flows — raises no verdict and no suspicion, with the probe as without it: the
/// node's probes of the member are rescued by its helpers (ADR 0016, issue #971), so
/// no suspicion is ever raised for a last probe to confirm. What the node cannot
/// deliver to the member in the meantime is a link that is down, not a member that is
/// gone (ADR 0087 D1).
#[test]
fn one_link_lost_one_way_raises_no_verdict() {
    for detector in [DEFAULT, NO_PROBE] {
        let c = cut_cell_of(&fault_under(detector, Fault::DeafToOne, 60_000, 0), 14, 3);
        assert_eq!(c.any_dead, 0, "{}", detector.0);
    }
}

/// Packet loss. The probe and its answer are two datagrams, and either can be lost;
/// then the member is declared `Dead` as it would have been. So on a lossy network
/// the default is not a guarantee. What holds:
///
/// - a member that really died is still buried by every survivor;
/// - the probe adds no verdict of its own. Every `Dead` a node declares is a last
///   probe of its own that ran out in silence — a round trip the network lost — and
///   every `Dead` a node adopts is about a member some node had so declared. That is
///   a property of each member in each run, not a count over seeds;
/// - and it removes some: over sixteen seeds of a 10-node cluster losing one datagram
///   in five, no more runs end in a false `Dead` with the probe than without (in the
///   report, 10 of 32 against 25). The two arms of a seed run on the same network —
///   a datagram's loss and its delay are functions of where and when it is sent
///   ([`Sim::lost`], [`Sim::latency`]) — so this compares detectors and not draws; it is still a count over seeds, and at
///   the report's rates the count going the other way has a chance of under one in a
///   thousand.
#[test]
fn packet_loss_neither_keeps_a_dead_member_nor_adds_false_verdicts() {
    let buried = burial_under(DEFAULT, Fault::Cut, 100, 14, 3);
    assert_eq!(buried.unburied, 0, "a dead member was kept under loss");

    let n = 10;
    let with = cut_cell_of(&loss_under(DEFAULT, 200), n, 16);
    let without = cut_cell_of(&loss_under(NO_PROBE, 200), n, 16);
    assert!(
        with.last_probes.0 > 0 && with.last_probes.2 > 0,
        "one datagram in five lost, and no last probe was sent or none was lost: {:?}",
        with.last_probes
    );
    assert_eq!(
        with.declared_deads, with.last_probes.2,
        "a node declared a member DEAD other than on a last probe that ran out"
    );
    assert_eq!(
        with.untraced_deads, 0,
        "a node adopted a DEAD about a member that no node had declared"
    );
    assert!(
        with.any_dead <= without.any_dead,
        "runs with a false DEAD under 20% loss: {} with the last probe, {} without",
        with.any_dead,
        without.any_dead
    );
}

/// The part of [`fault_shapes_report`] about nodes that are LATE (issue #1002's
/// review): a member that really died while every survivor is persistently late, and
/// where the two-second bound ends when no tick is on a grid.
fn report_late_nodes(seeds: u64) {
    println!(
        "\na member that really died: ms until the last survivor holds it DEAD, median / \
         worst of 16 — with every survivor on time, and with every survivor's driver \
         waking 300-500 ms late and not running in between, for 30 s"
    );
    let sizes = [2, 3, 14, 30];
    let line = |label: &str, burial: &dyn Fn(usize) -> Burial| {
        let cells: Vec<String> = sizes
            .into_iter()
            .map(|n| format!("N={n} {}", burial(n)))
            .collect();
        println!("  {label:<44} | {}", cells.join(" | "));
    };
    line("on time", &|n| burial_ms(DEFAULT, n, 16));
    line("late, this tree", &|n| {
        burial_of(&all_survivors_late(Shell::Current), n, 16)
    });
    line("late, the driver loop before this change", &|n| {
        burial_of(&all_survivors_late(Shell::TickFirst), n, 16)
    });

    println!(
        "\nevery node's ticks 0-99 ms late, each time, for the whole run (no tick on a \
         grid): one member cut off"
    );
    let cuts = [1900, 1998, 2000, 2050, 2098, 2200, 2500];
    let columns = cuts.map(|for_ms| {
        (
            format!("{for_ms}ms"),
            late_ticks(&cut_under(DEFAULT, for_ms)),
        )
    });
    print_grid(&columns, seeds);
    println!(
        "\nevery node's ticks on a 50 ms grid (every instant a tick up to 50 ms late can \
         land on, deadlines included): one member cut off"
    );
    let columns = cuts.map(|for_ms| {
        (
            format!("{for_ms}ms"),
            half_ticks(&cut_under(DEFAULT, for_ms)),
        )
    });
    print_grid(&columns, seeds);
}
