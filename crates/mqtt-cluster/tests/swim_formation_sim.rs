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
//! pure function of `(n, seed, variant)` and a failure reruns identically.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use mqtt_cluster::swim::{Action, Config, Kind, MemberState, Message, Swim};
use mqtt_cluster::swim_auth::{
    GossipSign, GossipVerify, OpenReject, SwimAuth, VerifiedIdentity, KEY_LEN,
};
use mqtt_cluster::swim_driver::{CertRequests, Gossip, MembershipEvent, Output};
use mqtt_cluster::NodeId;

/// The broker's driver tick (`SWIM_TICK` in `mqttd`).
const TICK_MS: u64 = 100;
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

/// v1.1.0's state-machine rules: this change's two additions off.
fn v1_1_cfg() -> Config {
    Config {
        reprime_on_rescued_probe: false,
        greet_pruned: false,
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
    /// After formation has had `at_ms`, cut node `node` off (nothing in, nothing out)
    /// for `for_ms`. Times in the outcome are then measured from the end of the cut.
    isolate: Option<Isolate>,
}

#[derive(Clone, Copy)]
struct Isolate {
    at_ms: u64,
    for_ms: u64,
}

impl Variant {
    /// The scale rig's formation, every node on `flavor`.
    fn rig(flavor: fn(usize, usize) -> Flavor) -> Self {
        Self {
            flavor,
            preshared_certs: false,
            arm_founder: true,
            stagger_ms: (2500, 4500),
            isolate: None,
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
    /// Global time this process started; its clock is `now - started`, like the driver's.
    started: u64,
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
        if let Some(iso) = sim.variant.isolate {
            // Cut off a node that is neither the founder nor a seed (node 0 and 1 are
            // every follower's seeds, and unacquainted seeds are re-greeted anyway).
            let node = 2 + usize::try_from(sim.rng.range(0, (n - 3) as u64)).expect("small");
            let from = t + iso.at_ms;
            sim.cut = Some((node, from, from + iso.for_ms));
            sim.out.measured_from = from + iso.for_ms;
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
            Endpoint::cfg(flavor),
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
        });
        if !restarted {
            let phase = self.rng.range(0, TICK_MS - 1);
            self.push(self.now + phase, Ev::Tick(i));
        }
    }

    fn is_cut(&self, i: usize) -> bool {
        self.cut
            .is_some_and(|(node, from, until)| node == i && self.now >= from && self.now < until)
    }

    fn counting(&self) -> bool {
        self.variant.isolate.is_none() || self.now >= self.out.measured_from
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
                    if self.is_cut(from) || self.is_cut(j) {
                        continue;
                    }
                    // A private network: well under a millisecond, sometimes over.
                    let lat = self.rng.range(0, 1);
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
                            if self.counting() {
                                self.out.deads[from] += 1;
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

    fn hand_over(&mut self, to: usize, from: usize, bytes: &[u8]) {
        if self.is_cut(to) || self.is_cut(from) {
            return;
        }
        let Some(node) = self.nodes[to].as_mut() else {
            return; // not started yet: the datagram hits a closed port
        };
        let now = self.now - node.started;
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
                Ev::Tick(i) => {
                    let node = self.nodes[i].as_mut().expect("ticking node is up");
                    let now = self.now - node.started;
                    let emitted = node.endpoint.tick(now);
                    self.apply(i, emitted);
                    self.push(self.now + TICK_MS, Ev::Tick(i));
                }
                Ev::Deliver { to, from, bytes } => self.hand_over(to, from, &bytes),
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
    let rows: [(&str, Variant); 11] = [
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
            "this tree, founder not re-armed",
            Variant {
                arm_founder: false,
                ..Variant::rig(all_current)
            },
        ),
        ("one node left on v1.1.0", Variant::rig(one_left_on_v1_1)),
        ("half upgraded", Variant::rig(half_upgraded)),
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
            at_ms: 60_000,
            for_ms,
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
