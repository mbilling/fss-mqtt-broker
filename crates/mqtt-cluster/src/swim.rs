//! SWIM membership and failure detection — pure, sans-I/O state machine.
//!
//! This implements the core of the SWIM protocol (Scalable Weakly-consistent
//! Infection-style process group Membership):
//!
//! - **Failure detection** by periodic random probing: a direct `Ping`, and on
//!   timeout an indirect `PingReq` fanned to `k` helpers before concluding failure.
//! - **Suspicion**: a node that fails probing is marked `Suspect`, not immediately
//!   `Dead`; only after a suspicion timeout does it become `Dead`. This tolerates
//!   transient slowness and lets the victim refute.
//! - **Incarnation numbers + refutation**: each node owns an incarnation counter.
//!   On hearing itself suspected, it bumps its incarnation and gossips `Alive`,
//!   which supersedes the suspicion everywhere.
//! - **Infection-style dissemination**: membership updates piggyback on protocol
//!   messages and are re-broadcast a bounded number of times (`~log N`).
//!
//! The state machine is deliberately **I/O-free and clock-free**: callers feed it
//! `tick(now)` and `handle(msg, now)` where `now` is a millisecond clock, and it
//! returns [`Action`]s (datagrams to send, membership changes observed). The async
//! UDP driver lives in [`crate::swim_driver`]. This keeps every protocol rule
//! unit-testable without sockets or sleeps.

use crate::NodeId;
use serde::{Deserialize, Serialize};
// BTreeMap/BTreeSet (not Hash*) so iteration order is deterministic: the SWIM state machine
// is then a pure function of its inputs, which the deterministic simulation harness
// (ADR 0024-T7, tests/swim_sim.rs) relies on for seed-reproducible runs. Member sets are
// small, so the ordered-map cost is irrelevant.
use std::collections::{BTreeMap, BTreeSet};

/// A node-controlled version counter used to order conflicting membership claims.
pub type Incarnation = u64;

/// The membership state of a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberState {
    /// Responding to probes.
    Alive,
    /// Failed probing; suspected down pending refutation or the suspicion timeout.
    Suspect,
    /// Confirmed failed.
    Dead,
}

impl MemberState {
    /// Tie-break precedence at equal incarnation: `Dead` > `Suspect` > `Alive`.
    fn precedence(self) -> u8 {
        match self {
            MemberState::Alive => 0,
            MemberState::Suspect => 1,
            MemberState::Dead => 2,
        }
    }
}

/// Timing and fan-out parameters for the protocol.
// The bools are five independent rules, each on or off: four that a v1.1.0 node lacks
// (issues #971, #975 and #1002) and one measured candidate (issue #973). Not an
// encodable state.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct Config {
    /// Time between probe rounds (the SWIM protocol period `T`), in ms.
    pub protocol_period_ms: u64,
    /// How long to wait for a direct `Ack` before falling back to indirect probes.
    pub ack_timeout_ms: u64,
    /// How long a member stays `Suspect` before being declared `Dead` when only **one**
    /// node suspects it (ADR 0016 §3). This is the *maximum* suspicion window; it shrinks
    /// toward `suspicion_min_timeout_ms` as independent suspicions accumulate.
    pub suspicion_timeout_ms: u64,
    /// The floor the suspicion window shrinks to once `suspicion_confirmations` distinct
    /// nodes independently suspect the same peer (ADR 0016 §3). Clamped to be `<=`
    /// `suspicion_timeout_ms`.
    pub suspicion_min_timeout_ms: u64,
    /// Number of **distinct** independent suspecters at which the suspicion window
    /// reaches its floor (ADR 0016 §3). One prober alone holds the full window; the
    /// window interpolates from max (1 suspecter) to min (this many). Treated as `>= 2`.
    pub suspicion_confirmations: u8,
    /// How long a `Dead` member is kept as a tombstone (ADR 0016 phase 1): during this
    /// window no gossip can revive it, after which it is pruned and the id may rejoin.
    /// Set comfortably above the gossip drain time so a stale refutation cannot outlive
    /// the tombstone.
    pub dead_ttl_ms: u64,
    /// Number of helpers (`k`) asked to probe indirectly.
    pub indirect_probes: usize,
    /// Maximum membership updates piggybacked per outgoing message.
    pub gossip_fanout: usize,
    /// Multiplier on the `~log2(N)` re-broadcast count for each update.
    pub gossip_multiplier: u32,
    /// Upper bound on the Lifeguard local-health awareness score (ADR 0016 §2). The
    /// `ack`/`suspicion` timeouts are scaled by `(1 + awareness)`, so this caps how much
    /// a locally-degraded node slows its own failure detection. `0` disables awareness
    /// (timeouts never scale).
    pub awareness_max: u8,
    /// Consecutive fully-failed probe rounds (direct AND indirect unanswered) after
    /// which this node considers itself [`isolated`](Swim::isolated) (issue #368).
    /// Round-robin probing means the failures are against *distinct* targets, so the
    /// threshold trades detection delay against false positives from a run of
    /// genuinely-dead targets. Clamped to `>= 1`.
    pub isolation_rounds: u32,
    /// Consecutive unanswered direct probes of ONE member after which the next probe
    /// of it is accompanied by a `Join` re-greet — the certificate-carrying prime
    /// frame (issue #383). Heals the auth-layer deadlock where a receiver that lost
    /// our certificate drops every fingerprint-sealed datagram (`cert-miss`) and we
    /// never learn: to us the peer is merely deaf, and after this many silent
    /// probes we reintroduce ourselves with the full certificate.
    pub reprime_after: u32,
    /// What counts as an unanswered probe for `reprime_after` (issue #971). `true`
    /// (the default): a probe whose DIRECT ack never came, whether or not helpers
    /// then reached the member — an `IndirectAck` proves the member is alive, not
    /// that it can hear *us*, and a member that drops our datagrams as `cert-miss`
    /// is rescued by helpers every round. `false` is v1.1.0's rule (only a probe
    /// that helpers could not rescue either counts, and an indirect ack resets the
    /// count), kept so the formation simulation can run a v1.1.0 node beside a
    /// current one.
    pub reprime_on_rescued_probe: bool,
    /// Greet a member once, with a certificate-carrying `Join`, at the moment its
    /// `Dead` tombstone is pruned (issue #971). After the prune this node holds
    /// nothing about the member and would never address it again; if the `Dead`
    /// was false — and above all if the two nodes evicted EACH OTHER, so neither
    /// probes the other — nothing else re-introduces them in a quiet cluster. A
    /// live member answers with a `Sync` and is re-admitted first-hand; a dead one
    /// costs a single datagram per prune. `false` is v1.1.0's behaviour.
    pub greet_pruned: bool,
    /// Scale the suspicion window with the size of the cluster: both ends of it are
    /// multiplied by `max(1, log10(n))`, `n` counting this node and every member it
    /// holds that is not `Dead` — the SWIM paper's rule (and memberlist's), because a refutation needs
    /// a number of gossip rounds that grows with `log n` to get back. `false` (the
    /// default) is the fixed window every release so far has run. **A measurement
    /// knob (issue #973 / ADR 0087), not operator configuration**: nothing sets it
    /// outside the formation simulation.
    pub suspicion_scales_with_log_n: bool,
    /// Probe a suspect once more, directly, before declaring it `Dead`. When its
    /// suspicion window lapses the member is sent one `Ping` carrying the suspicion
    /// itself and is given one ack timeout — `ack_timeout_ms` scaled by this node's
    /// own health, like every ack timeout (ADR 0016 §2), so a node that has lately
    /// been the slow one waits longer. Only a member that stays silent is declared
    /// `Dead`, one ack timeout later than it would have been.
    ///
    /// A member that answers is **never** declared dead on that window, whatever its
    /// `Ack` carries: the `Ack` to that probe is proof of life in itself. Usually it
    /// also carries the refutation — a node told it is suspected bumps its
    /// incarnation, and with this on puts that refutation at the head of its gossip
    /// queue — and the suspicion is cleared at once, first-hand. When it does not
    /// (our claim was stale: the member had already refuted someone else's and that
    /// refutation's retransmit budget is spent; or the member is an older build that
    /// queues its refutation last), the window is restarted and the member is sent
    /// one `Join`, whose `Sync` reply states its current incarnation first-hand.
    ///
    /// No new wire element, and no cooperation needed from the suspect beyond what a
    /// v1.1.0 node already does: answer a `Ping` with an `Ack` and a `Join` with a
    /// `Sync`.
    ///
    /// The probe saves a suspect only on a round trip between this node and the
    /// suspect: our `Ping` has to reach it and its `Ack` — authenticated as the
    /// suspect's exactly as the `Ack` to an ordinary probe is, and matched to this
    /// probe's sequence number — has to come back. A relay's word does not count, so a
    /// member this node can reach in one direction only, or not at all, is declared
    /// `Dead` as it was without the probe.
    ///
    /// `true` is the default since ADR 0016's amendment of 2026-10-13 (issue #975,
    /// ADR 0087 T10). `false` is v1.1.0's rule — the window lapses and the member is
    /// `Dead` — kept so the formation simulation can run a v1.1.0 node beside a
    /// current one. **Not operator configuration**: none of the detector's settings
    /// is.
    pub confirm_before_dead: bool,
    /// Keep the probe period from when a round was DUE rather than from when it
    /// started (issue #1002). A node that wakes late starts its round late; counted
    /// from that start, every later round is late by as much again, and a member whose
    /// turn is several rounds off waits that many latenesses to be probed. `true` (the
    /// default): late by less than a period, the next round is due one period after
    /// the last was due; a whole period or more behind, the node starts again from
    /// now and makes up no rounds. A node whose rounds start on time is unaffected.
    /// `false` is v1.1.0's rule, kept so the formation simulation can run a v1.1.0
    /// node. **Not operator configuration.**
    pub period_from_schedule: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            protocol_period_ms: 1000,
            ack_timeout_ms: 250,
            suspicion_timeout_ms: 4000,
            suspicion_min_timeout_ms: 1500,
            suspicion_confirmations: 3,
            dead_ttl_ms: 30_000,
            indirect_probes: 3,
            gossip_fanout: 6,
            gossip_multiplier: 3,
            awareness_max: 8,
            isolation_rounds: 5,
            reprime_after: 3,
            reprime_on_rescued_probe: true,
            greet_pruned: true,
            suspicion_scales_with_log_n: false,
            confirm_before_dead: true,
            period_from_schedule: true,
        }
    }
}

/// A peer in the local membership view.
#[derive(Debug, Clone)]
pub struct Member {
    /// The peer's node id.
    pub id: NodeId,
    /// The peer's SWIM datagram address.
    pub addr: String,
    /// The peer's inter-node routing (TCP peer-link) address, gossiped so the
    /// routing layer knows where to dial. Empty if not yet learned.
    pub peer_addr: String,
    /// The latest incarnation we have observed for it.
    pub incarnation: Incarnation,
    /// The process generation (issue #92) those observations belong to. Claims
    /// carrying an older generation are about a previous life and are discarded.
    pub generation: u64,
    /// Its current state in our view.
    pub state: MemberState,
    /// Its self-advertised failure-domain label (rack/zone), learned from gossip
    /// (ADR 0016 T5). `None` until the node advertises one; once learned it is not
    /// erased by a claimant that never learned it (same rule as `peer_addr`).
    pub failure_domain: Option<String>,
    /// Clock time (ms) when it entered `state`; drives the suspicion timeout.
    state_since: u64,
    /// When this member's `Dead` tombstone is pruned (ADR 0016 phase 1). `Some` iff
    /// the member is `Dead`; while set, gossip cannot revive the member.
    tombstone_deadline: Option<u64>,
    /// Distinct nodes that independently suspect this member at its current incarnation
    /// (ADR 0016 §3). Its size shrinks the effective suspicion window; reset whenever the
    /// member's `(incarnation, state)` identity changes.
    suspecters: BTreeSet<NodeId>,
    /// Consecutive probes of OURS this member did not answer DIRECTLY (issue #971:
    /// helpers reaching it does not show that it hears us). Reset by its direct
    /// ack. At `Config::reprime_after` the next probe is accompanied by a `Join`
    /// re-greet — the certificate-carrying prime frame that heals a receiver-side
    /// cert-cache loss (issue #383): its `Sync` reply re-primes both directions.
    /// (With `Config::reprime_on_rescued_probe` off, v1.1.0's rule: only probes
    /// that helpers could not rescue either, reset by any ack.)
    unanswered_probes: u32,
    /// `Config::confirm_before_dead`: the last direct probe sent at the end of this
    /// member's suspicion window, as `(its seq, when it gives up)`. `Some` only while
    /// that probe is outstanding; cleared when the member answers it, when the member
    /// stops being `Suspect` or starts a new life, and when the probe runs out inside
    /// a window that has grown since it was sent (`expire_suspects`).
    confirm: Option<(u64, u64)>,
}

/// True when `addr` names the unspecified host (`0.0.0.0` / `[::]`) or is empty —
/// an address that cannot be dialed from another machine (issue #396). A node
/// bound to the unspecified address self-claims it verbatim; on the receiving
/// host, dialing it loops back to the receiver's OWN socket, so every relayed
/// record carrying it is a dead link (and a probe of it self-acks — see
/// [`Swim::handle`]). Loopback is deliberately NOT in this set: single-host
/// clusters address each other via `127.0.0.1:<port>` legitimately.
fn unroutable(addr: &str) -> bool {
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    host.is_empty() || host == "0.0.0.0" || host == "[::]" || host == "::"
}

/// A membership update disseminated via gossip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Update {
    /// Subject node id.
    pub id: String,
    /// Subject SWIM address.
    pub addr: String,
    /// Subject routing (peer-link) address. Empty if the claimant never learned it.
    pub peer_addr: String,
    /// Subject incarnation the claim is about.
    pub incarnation: Incarnation,
    /// The subject's process GENERATION this claim is about (issue #92).
    ///
    /// Incarnation numbers live inside one process life: a restarted node has no
    /// memory of them and re-enters at the bottom of the range, so a `Dead` claim
    /// about its previous life outranks the new process's `Alive` and kills it —
    /// forever, since every node that applies a claim re-gossips it. The
    /// generation is a monotonic per-start token that says *which life* a claim is
    /// about, so a newer life supersedes any claim about an older one and a claim
    /// about an older life is discarded outright. Appended field: a pre-1.0 wire
    /// reshape (ADR 0039), same clean-break rules as ADR 0052.
    #[serde(default)]
    pub generation: u64,
    /// Claimed state.
    pub state: MemberState,
    /// For a `Suspect` claim, the id of the node asserting it (ADR 0016 §3), preserved
    /// through re-broadcast so receivers can count **distinct** independent suspecters
    /// of the same peer. `None` for `Alive`/`Dead` and for full-state relays.
    #[serde(default)]
    pub suspecter: Option<String>,
    /// The subject node's self-advertised failure-domain label (ADR 0016 T5), carried
    /// so the cluster topology auto-propagates without a static cluster-uniform map.
    /// `None` when the claimant has not (yet) learned the subject's label.
    #[serde(default)]
    pub failure_domain: Option<String>,
}

/// The kind of a SWIM datagram.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    /// Direct liveness probe.
    Ping {
        /// Probe sequence number, echoed in the `Ack`.
        seq: u64,
    },
    /// Response to a `Ping` (direct or relayed).
    Ack {
        /// The sequence number being acknowledged.
        seq: u64,
    },
    /// Request that the receiver probe `target` on the sender's behalf.
    PingReq {
        /// Node id to probe.
        target: String,
        /// Address to probe.
        target_addr: String,
    },
    /// Sent by a helper back to the requester when an indirect probe succeeded.
    IndirectAck {
        /// The node that was successfully reached.
        target: String,
    },
    /// Join request: "add me and send me the membership".
    Join,
    /// Full-state response to a `Join` (members carried in `gossip`).
    Sync,
}

/// A SWIM datagram: a typed message plus piggybacked membership gossip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// Sender node id.
    pub from: String,
    /// Sender SWIM address.
    pub from_addr: String,
    /// Sender routing (peer-link) address, so first contact teaches it.
    pub from_peer_addr: String,
    /// Sender's self-advertised failure-domain label (ADR 0016 T5), so first contact
    /// teaches it directly — the same reason `from_peer_addr` rides here.
    #[serde(default)]
    pub from_domain: Option<String>,
    /// The sender's own process generation (issue #92), so first contact learns
    /// which life it is talking to — otherwise a member learned from a datagram
    /// would start at generation 0 and a stale `Dead` claim about a real earlier
    /// life would outrank it.
    #[serde(default)]
    pub from_generation: u64,
    /// The message kind.
    pub kind: Kind,
    /// Piggybacked membership updates.
    pub gossip: Vec<Update>,
    /// The sender's cluster identity (ADR 0054 T2), stamped by the DRIVER at
    /// send time — the pure state machine never learns it. `None` from a joiner
    /// that has not adopted one yet, or from a pre-0054 build. The driver drops
    /// (and counts `cluster-mismatch`) any datagram carrying a *different*
    /// identity than ours: gossip from a separately-founded cluster is contained,
    /// not merged. Appended field: a pre-1.0 wire reshape (ADR 0039), same
    /// clean-break rules as ADR 0052.
    #[serde(default)]
    pub cluster_id: Option<String>,
}

impl Message {
    /// Decode a SWIM datagram payload exactly as the driver does after it
    /// clears authentication. Returns `None` on any malformed input — never
    /// panics. Strict (ADR 0052): trailing bytes after a valid message are
    /// malformed. Input size is bounded ahead of this call by the driver's
    /// 64 KiB receive buffer, and postcard cannot allocate beyond its input.
    /// Public so the ADR 0044 P5 fuzz target exercises the real gossip parser
    /// rather than a reimplementation.
    #[must_use]
    pub fn decode(payload: &[u8]) -> Option<Self> {
        match postcard::take_from_bytes(payload) {
            Ok((msg, [])) => Some(msg),
            _ => None,
        }
    }
}

/// An effect the driver must carry out.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Send `msg` to datagram address `to`.
    Send {
        /// Destination SWIM address.
        to: String,
        /// The datagram to send.
        msg: Message,
    },
    /// A member's observed state changed (for the routing layer to react to).
    StateChange {
        /// The member.
        id: NodeId,
        /// Its SWIM address.
        addr: String,
        /// Its routing (peer-link) address; empty if not yet learned.
        peer_addr: String,
        /// Its new state.
        state: MemberState,
        /// Its self-advertised failure-domain label (ADR 0016 T5); `None` if not yet
        /// learned. Carried to the routing/placement layer so the domain map tracks
        /// membership.
        domain: Option<String>,
    },
    /// A member is **confirmed gone** (ADR 0087 D2, sub-decision O1 as decided on
    /// 2026-10-14): the process this node knew under `id` no longer exists, and
    /// whatever was being held for it may be resolved. Raised in two cases:
    ///
    /// - its `Dead` tombstone was pruned (`Config::dead_ttl_ms` after the verdict)
    ///   and the parting greeting sent with the prune (issue #971) went unanswered
    ///   for `Config::ack_timeout_ms` — a live member answers it with a `Sync` and is
    ///   re-admitted, and then nothing is raised;
    /// - a **new generation** of the same id joined: the old process is gone by
    ///   definition. Raised before the `StateChange` that reports the new life.
    ///
    /// Membership says nothing here about links: the routing layer adds that half of
    /// O1 ("and no link") itself.
    Gone {
        /// The member.
        id: NodeId,
        /// Its SWIM address, as last known.
        addr: String,
        /// Its routing (peer-link) address, as last known; empty if never learned.
        peer_addr: String,
    },
}

/// A member whose tombstone was pruned, waiting for [`Action::Gone`]: the parting
/// greeting has been sent and an answer would re-admit it.
#[derive(Debug, Clone)]
struct Departed {
    /// The life that was pruned. A later life joining confirms it gone at once.
    generation: u64,
    addr: String,
    peer_addr: String,
    /// When the greeting counts as unanswered and the member as gone.
    confirm_at: u64,
}

/// What became of the last direct probes this node has sent
/// (`Config::confirm_before_dead`), counted since it started. For the broker's metrics:
/// a member that is spared again and again shows here, where the `suspect` gauge only
/// shows that someone is suspected at the moment it is read.
///
/// `sent - spared - silent` is the number still outstanding plus the number overtaken:
/// the suspicion ended some other way while the probe was out (a refutation or a `Dead`
/// that arrived by gossip, a restart of the member), or the window grew under it and it
/// was dropped unanswered for a fresh one later.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LastProbes {
    /// Probes sent: one per suspect each time its window lapses.
    pub sent: u64,
    /// Probes the suspect itself answered in time, so it was not declared `Dead`: its
    /// `Ack` to that probe, or its own refutation arriving first-hand while the probe
    /// was out.
    pub spared: u64,
    /// Probes that ran their ack timeout unanswered: the member was declared `Dead`.
    pub silent: u64,
}

/// State of the single in-flight probe.
#[derive(Debug)]
struct Probe {
    target: NodeId,
    seq: u64,
    /// When a direct `Ack` must arrive before we escalate to indirect probes.
    ack_deadline: u64,
    /// Once indirect probes are sent, when we give up and suspect the target.
    indirect_deadline: Option<u64>,
}

/// The SWIM protocol state machine for one node.
#[derive(Debug)]
pub struct Swim {
    local: NodeId,
    local_addr: String,
    /// This node's routing (peer-link) address, advertised via gossip.
    local_peer_addr: String,
    /// This node's own failure-domain label (ADR 0016 T5), stamped onto every
    /// self-update it emits so peers learn the cluster topology from gossip.
    local_domain: Option<String>,
    incarnation: Incarnation,
    /// This process's generation (issue #92) — supplied by the caller so the state
    /// machine stays clock-free. Must be strictly greater on each restart of the
    /// same node id; the broker uses the wall clock at start.
    generation: u64,
    cfg: Config,
    members: BTreeMap<NodeId, Member>,
    seeds: Vec<String>,
    /// Updates pending dissemination, each with a remaining re-broadcast count.
    gossip: Vec<(Update, u32)>,
    next_probe_at: u64,
    probe: Option<Probe>,
    seq: u64,
    /// Relayed probes we issued for a `PingReq`: our seq -> (requester addr, target).
    relays: BTreeMap<u64, (String, NodeId)>,
    probe_order: Vec<NodeId>,
    probe_idx: usize,
    rng: u64,
    bootstrapped: bool,
    /// Lifeguard local-health awareness (ADR 0016 §2): rises when we must refute a
    /// suspicion about ourselves (the unambiguous signal that we are the slow one),
    /// decays on a clean probe. An unanswered probe of ours does NOT raise it — without
    /// NACKs that is as likely the target's death as our slowness (see `advance_probe`).
    /// Scales our `ack`/`suspicion` timeouts by `(1 + awareness)` so a degraded node
    /// stops blaming healthy peers. `0` ⇒ the configured timeouts.
    awareness: u8,
    /// Set once this node begins a voluntary, graceful departure ([`leave`](Self::leave),
    /// ADR 0019 §2). While leaving we stop refuting `Dead` claims about ourselves so the
    /// announced departure sticks rather than being overridden by self-refutation.
    leaving: bool,
    /// Consecutive probe rounds of ours that concluded with NO ack, direct or
    /// indirect (issue #368). Distinct from `awareness`, which deliberately does
    /// **not** rise on unanswered probes (the target may simply be dead): this
    /// counter exists precisely for the case awareness cannot see — a one-way
    /// network failure where our outbound datagrams vanish while inbound gossip
    /// keeps painting a fresh-looking `Alive` view. Reset by any ack of any
    /// probe of ours.
    failed_probe_rounds: u32,
    /// SWIM addresses of members seen starting a NEW LIFE (a higher generation)
    /// since the driver last asked ([`take_new_lives`](Self::take_new_lives)). A set
    /// of member addresses, so bounded by the membership however rarely it is drained.
    new_lives: BTreeSet<String>,
    /// What became of the last direct probes sent so far
    /// ([`last_probes`](Self::last_probes)).
    last_probes: LastProbes,
    /// Members pruned and not yet confirmed gone ([`Action::Gone`]): at most one
    /// entry per pruned member, for `Config::ack_timeout_ms` each.
    departed: BTreeMap<NodeId, Departed>,
}

impl Swim {
    /// Create a node with the given identity, SWIM address, routing (peer-link)
    /// address, config and seed addresses.
    #[must_use]
    pub fn new(
        local: NodeId,
        local_addr: String,
        local_peer_addr: String,
        local_domain: Option<String>,
        generation: u64,
        cfg: Config,
        seeds: Vec<String>,
    ) -> Self {
        // Seed the PRNG from the node id so behaviour is deterministic per node
        // yet differs across nodes.
        let mut rng = 0xff51_afd7_ed55_8ccd;
        for b in local.0.bytes() {
            rng ^= u64::from(b);
            rng = rng.wrapping_mul(0x0100_0000_01b3);
        }
        Self {
            local,
            local_addr,
            local_peer_addr,
            local_domain,
            incarnation: 1,
            generation,
            cfg,
            members: BTreeMap::new(),
            seeds,
            gossip: Vec::new(),
            next_probe_at: 0,
            probe: None,
            seq: 0,
            relays: BTreeMap::new(),
            probe_order: Vec::new(),
            probe_idx: 0,
            rng: rng | 1,
            bootstrapped: false,
            awareness: 0,
            leaving: false,
            failed_probe_rounds: 0,
            new_lives: BTreeSet::new(),
            last_probes: LastProbes::default(),
            departed: BTreeMap::new(),
        }
    }

    /// What became of the last direct probes this node has sent since it started.
    #[must_use]
    pub fn last_probes(&self) -> LastProbes {
        self.last_probes
    }

    /// The Lifeguard local-health score (ADR 0016 §2): `0` for a healthy node, up to
    /// `Config::awareness_max`. This node's ack timeouts and suspicion windows are
    /// `(1 + awareness)` times the configured ones.
    #[must_use]
    pub fn awareness(&self) -> u8 {
        self.awareness
    }

    /// The SWIM addresses of members that have started a new life (a restart: a
    /// higher generation) since this was last called, and forget them.
    ///
    /// For the driver (issue #971): a restarted process starts with an empty
    /// certificate cache, so whatever this node remembers about having shown it our
    /// certificate is about a process that no longer exists. The driver un-primes
    /// these addresses before it seals anything else, so the very next datagram to
    /// each — typically the reply to the datagram that revealed the restart —
    /// carries the full certificate, and the restarted member never has to drop one
    /// of ours.
    pub fn take_new_lives(&mut self) -> Vec<String> {
        std::mem::take(&mut self.new_lives).into_iter().collect()
    }

    /// This node's id.
    #[must_use]
    pub fn local(&self) -> &NodeId {
        &self.local
    }

    /// A snapshot of all known peers (excluding this node).
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.members.values().cloned().collect()
    }

    /// The peers currently believed `Alive`.
    #[must_use]
    pub fn alive(&self) -> Vec<Member> {
        self.members
            .values()
            .filter(|m| m.state == MemberState::Alive)
            .cloned()
            .collect()
    }

    /// Consecutive probe rounds of ours that concluded unanswered (see
    /// [`isolated`](Self::isolated)).
    #[must_use]
    pub fn failed_probe_rounds(&self) -> u32 {
        self.failed_probe_rounds
    }

    /// True while this node's own probes have gone unanswered for at least
    /// `isolation_rounds` consecutive rounds (issue #368): every claim in the local
    /// membership view is then UNCONFIRMED by us — under a one-way network failure
    /// (outbound lost, inbound intact) the view keeps looking fresh and fully
    /// `Alive` while the rest of the cluster is busy evicting us. Readers of the
    /// view (statusz, operators, tooling) should treat it as suspect while this
    /// holds. Cleared by any ack of any probe of ours.
    #[must_use]
    pub fn isolated(&self) -> bool {
        self.failed_probe_rounds >= self.cfg.isolation_rounds.max(1)
    }

    fn xorshift(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    /// The Lifeguard local-health multiplier `(1 + awareness)` (ADR 0016 §2).
    fn health_multiplier(&self) -> u64 {
        1 + u64::from(self.awareness)
    }

    /// `ack_timeout` scaled by local health: a degraded node waits longer for acks
    /// before escalating, so it stops mistaking its own slowness for a peer's.
    fn scaled_ack_timeout(&self) -> u64 {
        self.cfg.ack_timeout_ms * self.health_multiplier()
    }

    /// The effective suspicion window for `m` before it is declared `Dead`, combining
    /// both Lifeguard mechanisms: it interpolates from `suspicion_timeout_ms` (one
    /// suspecter) down to `suspicion_min_timeout_ms` as independent suspecters reach
    /// `suspicion_confirmations` (§3), then is scaled by `(1 + awareness)` for local
    /// health (§2). A single prober therefore holds the full window; only independent
    /// confirmation fast-tracks `Dead`.
    fn effective_suspicion_timeout(&self, m: &Member) -> u64 {
        let max = self.cfg.suspicion_timeout_ms;
        let min = self.cfg.suspicion_min_timeout_ms.min(max);
        let k = u64::from(self.cfg.suspicion_confirmations).max(2);
        let confirmations = m.suspecters.len() as u64;
        let base = if confirmations <= 1 {
            max
        } else if confirmations >= k {
            min
        } else {
            // Linear from max (1 suspecter) to min (k suspecters).
            max - (max - min) * (confirmations - 1) / (k - 1)
        };
        base * self.size_multiplier_permille() / 1000 * self.health_multiplier()
    }

    /// `max(1, log10(n))` in thousandths, `n` being this node plus every member it
    /// holds that is not `Dead` (a tombstone is not part of the cluster a refutation
    /// has to cross) — the factor `Config::suspicion_scales_with_log_n` applies to the
    /// suspicion window. `1000` when the knob is off, and up to ten nodes either way.
    fn size_multiplier_permille(&self) -> u64 {
        if !self.cfg.suspicion_scales_with_log_n {
            return 1000;
        }
        let size = self
            .members
            .values()
            .filter(|m| m.state != MemberState::Dead)
            .count()
            + 1;
        // A cluster size: exact in an `f64`, and its log10 is a small positive number.
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let permille = ((size as f64).log10() * 1000.0) as u64;
        permille.max(1000)
    }

    /// Raise awareness (capped at `awareness_max`): a signal that *we* are the slow one
    /// — having to refute a suspicion about ourselves. (Not a stall of this node's own
    /// driver: that was tried and withdrawn, ADR 0016's amendment for issue #1002. It
    /// only decays on a clean probe round, and a node whose peers have died has none.)
    fn raise_awareness(&mut self) {
        self.awareness = (self.awareness + 1).min(self.cfg.awareness_max);
    }

    /// Lower awareness on a clean round (a probe of ours was acked).
    fn lower_awareness(&mut self) {
        self.awareness = self.awareness.saturating_sub(1);
    }

    /// `~log2(N)` re-broadcasts, scaled by the configured multiplier.
    fn transmit_limit(&self) -> u32 {
        let n = (self.members.len() + 2) as u64;
        let bits = 64 - n.leading_zeros(); // floor(log2 n) + 1
        (self.cfg.gossip_multiplier * bits).max(1)
    }

    /// Queue an update for dissemination (resetting any existing entry for it).
    fn enqueue_gossip(&mut self, update: Update) {
        let limit = self.transmit_limit();
        self.gossip.retain(|(u, _)| u.id != update.id);
        self.gossip.push((update, limit));
    }

    /// Take up to `gossip_fanout` updates to piggyback, decrementing their counts.
    fn take_gossip(&mut self) -> Vec<Update> {
        let mut out = Vec::new();
        for (update, remaining) in &mut self.gossip {
            if out.len() >= self.cfg.gossip_fanout {
                break;
            }
            out.push(update.clone());
            *remaining = remaining.saturating_sub(1);
        }
        self.gossip.retain(|(_, r)| *r > 0);
        out
    }

    fn message(&mut self, kind: Kind) -> Message {
        Message {
            from: self.local.0.clone(),
            from_addr: self.local_addr.clone(),
            from_peer_addr: self.local_peer_addr.clone(),
            from_domain: self.local_domain.clone(),
            from_generation: self.generation,
            cluster_id: None, // stamped by the driver at send time (ADR 0054)
            kind,
            gossip: self.take_gossip(),
        }
    }

    /// This node's claim about itself: `Alive`, this life, at its current incarnation.
    fn self_update(&self) -> Update {
        Update {
            id: self.local.0.clone(),
            addr: self.local_addr.clone(),
            peer_addr: self.local_peer_addr.clone(),
            incarnation: self.incarnation,
            generation: self.generation,
            state: MemberState::Alive,
            suspecter: None,
            failure_domain: self.local_domain.clone(),
        }
    }

    /// A `Join` greeting to `addr` — the frame the driver seals with this node's FULL
    /// certificate and the receiver answers with a `Sync` sealed with its own, so one
    /// exchange teaches both sides the other's certificate (issue #971). It carries
    /// only this node's claim about itself (so the receiver learns the incarnation we
    /// run at, as in `tick`'s bootstrap greeting) and does NOT draw on the gossip
    /// queue: the driver sends it to a datagram's source address before that source
    /// is authenticated, and pending membership updates must not have their
    /// retransmit budget spent on an address that may not be a member at all.
    ///
    /// Used by the driver when it drops a datagram for `cert-miss`, and by `tick`
    /// when a tombstone is pruned.
    #[must_use]
    pub fn greet(&self, addr: &str) -> Action {
        Action::Send {
            to: addr.to_string(),
            msg: Message {
                from: self.local.0.clone(),
                from_addr: self.local_addr.clone(),
                from_peer_addr: self.local_peer_addr.clone(),
                from_domain: self.local_domain.clone(),
                from_generation: self.generation,
                cluster_id: None, // stamped by the driver at send time (ADR 0054)
                kind: Kind::Join,
                gossip: vec![self.self_update()],
            },
        }
    }

    /// Apply one membership update, returning any observable state change.
    ///
    /// Handles self-refutation: a `Suspect`/`Dead` claim about us at an incarnation
    /// `>=` ours triggers a bump-and-`Alive` to override it everywhere.
    ///
    /// Third-party/relayed form: the tombstone fence applies in full. The message
    /// handler uses [`apply_update_from`](Self::apply_update_from) so a FIRST-HAND
    /// self-claim can pierce the fence (issue #383).
    fn apply_update(&mut self, u: &Update, now: u64, out: &mut Vec<Action>) {
        self.apply_update_from(u, now, out, false);
    }

    /// [`apply_update`](Self::apply_update) with provenance: `first_hand` is true iff
    /// the update is a claim **about the sender itself**, arriving in the sender's own
    /// datagram (in the signed posture the sender identity is certificate-bound, and
    /// anti-replay windows reject captured datagrams — so first-hand is trustworthy).
    #[allow(clippy::too_many_lines)]
    fn apply_update_from(&mut self, u: &Update, now: u64, out: &mut Vec<Action>, first_hand: bool) {
        if u.id == self.local.0 {
            // A claim about an EARLIER life of this id is not about this process
            // (issue #92): incarnations are only comparable within one life, so
            // refuting it would bump our counter for nothing. Drop it.
            // (A claim at a HIGHER generation means another process is running with
            // our id — a duplicate-id misconfiguration. We do not try to win that
            // fight: our refutation would carry the lower generation and lose.)
            if u.generation < self.generation {
                return;
            }
            // Once we have announced a graceful leave we do not refute `Dead` about
            // ourselves — including our own departure gossip echoed back (ADR 0019 §2).
            if u.state != MemberState::Alive && u.incarnation >= self.incarnation && !self.leaving {
                self.incarnation = u.incarnation + 1;
                let refute = Update {
                    id: self.local.0.clone(),
                    addr: self.local_addr.clone(),
                    peer_addr: self.local_peer_addr.clone(),
                    incarnation: self.incarnation,
                    generation: self.generation,
                    state: MemberState::Alive,
                    suspecter: None,
                    failure_domain: self.local_domain.clone(),
                };
                self.enqueue_gossip(refute);
                if self.cfg.confirm_before_dead {
                    // The reply to the datagram that carried the claim must carry
                    // the refutation, whatever else is waiting to be gossiped.
                    self.gossip.rotate_right(1);
                }
                // Having to refute ourselves signals we are the slow one (ADR 0016 §2).
                self.raise_awareness();
            }
            return;
        }

        let id = NodeId(u.id.clone());
        if let Some(m) = self.members.get_mut(&id) {
            // Generation orders process LIVES of the same id (issue #92), and it is
            // checked FIRST because it decides what the other rules even apply to.
            if u.generation < m.generation {
                return; // about a previous life; says nothing about this one
            }
            // A newer life supersedes everything we hold about the older one —
            // including its tombstone, which was about a process that really did die.
            let new_life = u.generation > m.generation;
            // Tombstone fence (ADR 0016 phase 1): while a `Dead` member is tombstoned,
            // no non-`Dead` gossip can revive it — not even a higher incarnation (e.g.
            // the node's own last refutation still in flight when it died). Only the
            // prune in `tick` clears the tombstone, after which the id may rejoin.
            // The fence is scoped to the life it was raised for: a *restart* is not a
            // resurrection, and blocking it is what left a rolled pod dead forever.
            //
            // FIRST-HAND EXCEPTION (issue #383): a live node FALSELY declared dead
            // refutes with a higher-incarnation Alive — and if only relays could carry
            // it, the fence livelocks: every evictor's re-application of the still-
            // circulating Dead claim re-arms the fence with fresh gossip budget, the
            // pruned-then-readded member is re-killed by the same claim, and a healthy
            // node stays evicted FOREVER (the 5-node curve incident, five runs). A
            // higher-incarnation Alive about the SENDER ITSELF, in the sender's own
            // authenticated datagram, is proof of life a crashed node can never fake —
            // #92's protection (stale relayed refutations of a truly dead node) holds,
            // because those arrive relayed, never first-hand.
            let pierces =
                first_hand && u.state == MemberState::Alive && u.incarnation > m.incarnation;
            if !new_life
                && m.tombstone_deadline.is_some()
                && u.state != MemberState::Dead
                && !pierces
            {
                return;
            }
            if pierces && m.tombstone_deadline.is_some() {
                m.tombstone_deadline = None;
            }
            // Record an independent suspecter of the *current* incarnation even when the
            // update does not supersede (a second node suspecting an already-`Suspect`
            // peer) — this is how confirmations accumulate (ADR 0016 §3).
            if !new_life
                && u.state == MemberState::Suspect
                && m.state == MemberState::Suspect
                && u.incarnation == m.incarnation
            {
                if let Some(sus) = &u.suspecter {
                    m.suspecters.insert(NodeId(sus.clone()));
                }
            }
            let supersedes = new_life
                || u.incarnation > m.incarnation
                || (u.incarnation == m.incarnation && u.state.precedence() > m.state.precedence());
            if !supersedes {
                return;
            }
            let changed = m.state != u.state;
            // A new life resets the incarnation space, so treat it as an advance: the
            // suspecter set below belongs to the old process and must not carry over.
            let inc_advanced = new_life || u.incarnation > m.incarnation;
            m.generation = u.generation;
            m.incarnation = u.incarnation;
            if new_life {
                // The old process is gone by definition (ADR 0087 D2): say so before
                // the new life is reported, so the routing layer resolves what it
                // held for the old one first. (Once: a confirmation of a pruned
                // life still pending is this one.)
                self.departed.remove(&id);
                out.push(Action::Gone {
                    id: id.clone(),
                    addr: m.addr.clone(),
                    peer_addr: m.peer_addr.clone(),
                });
                // Our probe history is about the old process too (issue #783): its
                // missed probes are not this life's, and a probe of ours still in
                // flight was aimed at the corpse — left to conclude, it would
                // suspect the new life the moment it came up.
                m.unanswered_probes = 0;
                if self.probe.as_ref().is_some_and(|p| p.target == id) {
                    self.probe = None;
                }
            }
            // An unroutable claim must not overwrite an address we can actually
            // dial (issue #396): a node bound to `0.0.0.0` self-claims it in every
            // update it emits — including its own refutations, which supersede by
            // incarnation — and without this guard one such claim clobbers the
            // source-learned address and every later probe of the member dials
            // our own loopback. Same shape as `peer_addr`'s empty-claim rule.
            if !unroutable(&u.addr) {
                m.addr.clone_from(&u.addr);
            }
            if new_life {
                // A new process holds none of the certificates the old one had
                // cached — ours included (issue #971). Tell the driver, so the next
                // datagram we send this member carries it again.
                self.new_lives.insert(m.addr.clone());
            }
            // Never let a claimant that hasn't learned the routing address yet
            // erase one we already know.
            let prev_peer_addr = m.peer_addr.clone();
            if !u.peer_addr.is_empty() {
                m.peer_addr.clone_from(&u.peer_addr);
            }
            // Same rule for the failure-domain label (ADR 0016 T5): a relay that never
            // learned the subject's label must not blank out one we already hold.
            let domain_changed = u.failure_domain.is_some() && u.failure_domain != m.failure_domain;
            if domain_changed {
                m.failure_domain.clone_from(&u.failure_domain);
            }
            // A ROUTING ADDRESS that moved is news for the dial layer even when the
            // state did not change (issue #92): a node that comes back at a new
            // address is `Alive -> Alive`, and without an event the dialer keeps the
            // address it was spawned with forever.
            let addr_changed = !u.peer_addr.is_empty() && u.peer_addr != prev_peer_addr;
            // A last direct probe that is out belongs to the suspicion it was sent for,
            // and that ends when the member stops being `Suspect` or starts a new life.
            // A `Suspect` claim at a higher incarnation does not end it: the member is
            // still suspected, its window still runs from `state_since`, and its `Ack`
            // to the probe that is out is as much proof of life as it was — dropping
            // the probe here would ignore that `Ack` and send a second probe on the
            // next tick.
            if changed || new_life {
                let answered = first_hand && u.state == MemberState::Alive && !new_life;
                if m.confirm.take().is_some() && answered {
                    // The suspect's own refutation, in its own datagram, while the
                    // probe was out: it answered.
                    self.last_probes.spared += 1;
                }
            }
            // The `(incarnation, state)` identity changed: reset the suspecter set,
            // seeding it from this update if it is a fresh `Suspect` (ADR 0016 §3).
            if changed || inc_advanced {
                m.suspecters.clear();
                if u.state == MemberState::Suspect {
                    if let Some(sus) = &u.suspecter {
                        m.suspecters.insert(NodeId(sus.clone()));
                    }
                }
            }
            if changed || new_life {
                m.state = u.state;
                m.state_since = now;
                m.tombstone_deadline = if u.state == MemberState::Dead {
                    Some(now + self.cfg.dead_ttl_ms)
                } else {
                    None
                };
            }
            // Surface the change to the routing/placement layer when the state changed
            // *or* only the failure-domain label did (ADR 0016 T5) — otherwise a label
            // learned after the member is already known would never reach placement.
            if changed || domain_changed || addr_changed {
                out.push(Action::StateChange {
                    id: id.clone(),
                    addr: u.addr.clone(),
                    peer_addr: m.peer_addr.clone(),
                    state: m.state,
                    domain: m.failure_domain.clone(),
                });
            }
        } else {
            // A member pruned a moment ago is heard of again. A later life confirms
            // the pruned one gone now; the same life alive answered the parting
            // greeting (or is otherwise alive) and is not gone; a claim that it is
            // suspect or dead leaves the confirmation to run its course.
            if let Some(departed) = self.departed.get(&id) {
                if u.generation > departed.generation {
                    if let Some(d) = self.departed.remove(&id) {
                        out.push(Action::Gone {
                            id: id.clone(),
                            addr: d.addr,
                            peer_addr: d.peer_addr,
                        });
                    }
                } else if u.generation == departed.generation && u.state == MemberState::Alive {
                    self.departed.remove(&id);
                }
            }
            self.members.insert(
                id.clone(),
                Member {
                    id: id.clone(),
                    addr: u.addr.clone(),
                    peer_addr: u.peer_addr.clone(),
                    incarnation: u.incarnation,
                    generation: u.generation,
                    state: u.state,
                    failure_domain: u.failure_domain.clone(),
                    state_since: now,
                    tombstone_deadline: if u.state == MemberState::Dead {
                        Some(now + self.cfg.dead_ttl_ms)
                    } else {
                        None
                    },
                    suspecters: match (u.state, &u.suspecter) {
                        (MemberState::Suspect, Some(sus)) => {
                            let mut s = BTreeSet::new();
                            s.insert(NodeId(sus.clone()));
                            s
                        }
                        _ => BTreeSet::new(),
                    },
                    unanswered_probes: 0,
                    confirm: None,
                },
            );
            out.push(Action::StateChange {
                id,
                addr: u.addr.clone(),
                peer_addr: u.peer_addr.clone(),
                state: u.state,
                domain: u.failure_domain.clone(),
            });
        }
        // Relay with the best address we hold, not the claim's verbatim one
        // (issue #396): an update whose subject self-claimed an unroutable bind
        // would otherwise poison every third party's record — the receiver can
        // never repair it, because repairing requires hearing from the subject
        // first-hand at an address it cannot dial. Substituting our
        // source-learned address turns every hop that knows better into a cure
        // instead of a carrier.
        let mut relay = u.clone();
        if unroutable(&relay.addr) {
            if let Some(m) = self.members.get(&NodeId(relay.id.clone())) {
                if !unroutable(&m.addr) {
                    relay.addr.clone_from(&m.addr);
                }
            }
        }
        self.enqueue_gossip(relay);
    }

    /// Locally declare a member `Suspect`/`Dead` and gossip it.
    fn declare(&mut self, id: &NodeId, state: MemberState, now: u64, out: &mut Vec<Action>) {
        let Some(m) = self.members.get(id) else {
            return;
        };
        let update = Update {
            id: id.0.clone(),
            addr: m.addr.clone(),
            peer_addr: m.peer_addr.clone(),
            incarnation: m.incarnation,
            // The claim names the life we observed, so it can never kill a later one.
            generation: m.generation,
            state,
            // Stamp ourselves as the suspecter so independent suspicions are countable
            // through re-broadcast (ADR 0016 §3).
            suspecter: if state == MemberState::Suspect {
                Some(self.local.0.clone())
            } else {
                None
            },
            // Relay the subject's known label so a suspicion/death does not blank it.
            failure_domain: m.failure_domain.clone(),
        };
        self.apply_update(&update, now, out);
    }

    /// Begin a voluntary, graceful departure (ADR 0019 §2): announce ourselves `Dead`
    /// directly to every known peer so they remove us from the ring **immediately**,
    /// rather than waiting out failure detection (suspicion → dead). Returns the
    /// datagrams to send; the announcement is also queued as gossip so a final probe
    /// re-broadcasts it.
    ///
    /// We gossip `Dead` at our *current* incarnation (not a bumped one): a peer holding
    /// us `Alive` at that incarnation is superseded by `Dead`'s higher precedence, and
    /// the resulting tombstone fences any of our own in-flight `Alive` gossip — the same
    /// mechanism that protects a crashed node's last refutation. Delivery is best-effort
    /// over UDP; a lost announcement simply falls back to ordinary failure detection.
    ///
    /// Sets the leaving flag so we stop refuting `Dead` about ourselves (see
    /// `apply_update`). Idempotent: calling it again just re-announces.
    pub fn leave(&mut self) -> Vec<Action> {
        self.leaving = true;
        let departure = Update {
            id: self.local.0.clone(),
            addr: self.local_addr.clone(),
            peer_addr: self.local_peer_addr.clone(),
            incarnation: self.incarnation,
            generation: self.generation,
            state: MemberState::Dead,
            suspecter: None,
            failure_domain: self.local_domain.clone(),
        };
        // Announce directly to every peer we are not already treating as gone, carrying
        // the departure as the message's gossip (a `Sync` is a pure state-merge on the
        // receiver, so it has no other side effect).
        let mut out = Vec::new();
        for m in self.members.values() {
            if m.state == MemberState::Dead {
                continue;
            }
            out.push(Action::Send {
                to: m.addr.clone(),
                msg: Message {
                    from: self.local.0.clone(),
                    from_addr: self.local_addr.clone(),
                    from_peer_addr: self.local_peer_addr.clone(),
                    from_domain: self.local_domain.clone(),
                    from_generation: self.generation,
                    cluster_id: None, // stamped by the driver at send time (ADR 0054)
                    kind: Kind::Sync,
                    gossip: vec![departure.clone()],
                },
            });
        }
        // Also queue it for the normal re-broadcast path (a final tick piggybacks it).
        self.enqueue_gossip(departure);
        out
    }

    /// Advance the protocol clock to `now`, returning datagrams to send and
    /// membership changes observed.
    pub fn tick(&mut self, now: u64) -> Vec<Action> {
        let mut out = Vec::new();
        self.prune_tombstones(now, &mut out);
        if !self.bootstrapped {
            self.bootstrapped = true;
            self.next_probe_at = now + self.cfg.protocol_period_ms;
            // Announce ourselves, so the greetings below (and the first datagrams
            // after them) carry a FIRST-HAND claim of this life at its real
            // incarnation (issue #783). A peer that still holds an earlier life of
            // this id — `Dead` after a graceful leave — is superseded at once; and
            // one that learned us only from a datagram header (which carries no
            // incarnation, so it records 0) learns the incarnation we actually run
            // at, without which a Suspect it raised at 0 is one we would never
            // refute (ours is higher) and it would kill this life.
            let me = Update {
                id: self.local.0.clone(),
                addr: self.local_addr.clone(),
                peer_addr: self.local_peer_addr.clone(),
                incarnation: self.incarnation,
                generation: self.generation,
                state: MemberState::Alive,
                suspecter: None,
                failure_domain: self.local_domain.clone(),
            };
            self.enqueue_gossip(me);
            // Greet seeds so they add us and send their membership back.
            let seeds = self.seeds.clone();
            for addr in seeds {
                if addr != self.local_addr {
                    let msg = self.message(Kind::Join);
                    out.push(Action::Send { to: addr, msg });
                }
            }
        }

        self.advance_probe(now, &mut out);
        self.expire_suspects(now, &mut out);

        if now >= self.next_probe_at && self.probe.is_none() {
            self.start_probe(now, &mut out);
            // Re-greet every seed we are not yet acquainted with, once per
            // protocol period (ADR 0044 P2 find): the one-shot bootstrap
            // greeting races the seed's own socket bind when a fleet starts
            // simultaneously (systemd, k8s) — a lost first Join otherwise
            // leaves this node PERMANENTLY outside a cluster that formed
            // around it. Alive-member seeds cost nothing here; a dead seed
            // gets a harmless periodic dribble that doubles as its re-entry
            // greeting after a restart.
            self.greet_unacquainted_seeds(&mut out);
            // The next round is due a period after this one was DUE, not after the
            // moment this node got round to it: a node that wakes late would
            // otherwise stretch every period by its lateness, and the member whose
            // turn is five rounds off would wait five latenesses for it. A node that
            // is a whole period or more behind starts again from now — it does not
            // make up rounds.
            let period = self.cfg.protocol_period_ms;
            let on_schedule = self.cfg.period_from_schedule;
            self.next_probe_at = if on_schedule && now - self.next_probe_at < period {
                self.next_probe_at + period
            } else {
                now + period
            };
        }
        out
    }

    /// Send a `Join` greeting to every configured seed address that is not yet
    /// an alive member of our view (see `tick` for why this must repeat).
    fn greet_unacquainted_seeds(&mut self, out: &mut Vec<Action>) {
        let seeds = self.seeds.clone();
        for addr in seeds {
            if addr == self.local_addr {
                continue;
            }
            let acquainted = self
                .members
                .values()
                .any(|m| m.addr == addr && m.state == MemberState::Alive);
            if !acquainted {
                let msg = self.message(Kind::Join);
                out.push(Action::Send { to: addr, msg });
            }
        }
    }

    fn advance_probe(&mut self, now: u64, out: &mut Vec<Action>) {
        let Some(p) = &self.probe else { return };
        if p.indirect_deadline.is_none() && now >= p.ack_deadline {
            // No direct ack: ask k helpers to probe the target indirectly.
            let target = p.target.clone();
            let target_addr = self
                .members
                .get(&target)
                .map_or_else(String::new, |m| m.addr.clone());
            let helpers = self.random_alive_helpers(&target);
            for addr in helpers {
                let msg = self.message(Kind::PingReq {
                    target: target.0.clone(),
                    target_addr: target_addr.clone(),
                });
                out.push(Action::Send { to: addr, msg });
            }
            let indirect_deadline = now + self.scaled_ack_timeout();
            if let Some(p) = &mut self.probe {
                p.indirect_deadline = Some(indirect_deadline);
            }
            // The member did not answer us DIRECTLY (issue #971). Counted here, not
            // when the indirect round fails too: helpers reaching the member says it
            // is alive, not that it hears us — and a member that drops our datagrams
            // as `cert-miss` is rescued by its helpers every single round, so a count
            // that an indirect ack resets never reaches the re-greet.
            if self.cfg.reprime_on_rescued_probe {
                if let Some(m) = self.members.get_mut(&target) {
                    m.unanswered_probes = m.unanswered_probes.saturating_add(1);
                }
            }
        } else if let Some(idl) = p.indirect_deadline {
            if now >= idl {
                // Indirect probing also failed: suspect the target. We do NOT raise our
                // own awareness here — without NACKs an unanswered probe is ambiguous
                // (the target may simply be dead), so blaming our local health would
                // wrongly slow detection of genuinely-dead peers (ADR 0016 §2). Only
                // self-refutation, an unambiguous "others cannot reach us", raises it.
                // The isolation counter (issue #368) is the accumulation that resolves
                // the ambiguity: one silent target means nothing, but a consecutive
                // RUN of silent round-robin targets means the silence is ours.
                self.failed_probe_rounds = self.failed_probe_rounds.saturating_add(1);
                let target = p.target.clone();
                if !self.cfg.reprime_on_rescued_probe {
                    if let Some(m) = self.members.get_mut(&target) {
                        // Per-member deafness (issue #383), v1.1.0's rule: counted
                        // only when helpers could not reach the member either.
                        m.unanswered_probes = m.unanswered_probes.saturating_add(1);
                    }
                }
                self.probe = None;
                self.declare(&target, MemberState::Suspect, now, out);
            }
        }
    }

    /// Remove tombstoned `Dead` members whose `dead_ttl_ms` has elapsed (ADR 0016
    /// phase 1). By now stale gossip has drained, so the id may rejoin as a fresh
    /// member without having to out-race a lingering refutation.
    ///
    /// Each pruned member gets one parting `Join` (issue #971, `Config::greet_pruned`):
    /// from here on this node holds nothing about it and would never address it
    /// again, so a member that was declared `Dead` falsely — and that, having been
    /// declared dead by us, may equally have declared US dead and stopped probing —
    /// would stay out of this node's view until some third party happened to gossip
    /// about it. If it is alive it answers with a `Sync` (first-hand, so it is
    /// re-admitted at once); if it is not, this is one datagram to a closed port.
    /// A node that is itself leaving greets nobody.
    ///
    /// A pruned member is confirmed gone ([`Action::Gone`]) once the greeting has gone
    /// unanswered for `Config::ack_timeout_ms` — at once when no greeting is sent.
    fn prune_tombstones(&mut self, now: u64, out: &mut Vec<Action>) {
        let mut pruned = Vec::new();
        let mut gone = Vec::new();
        self.members.retain(|_, m| {
            let keep = m.tombstone_deadline.is_none_or(|d| now < d);
            if !keep {
                if !unroutable(&m.addr) {
                    pruned.push(m.addr.clone());
                }
                gone.push((
                    m.id.clone(),
                    m.generation,
                    m.addr.clone(),
                    m.peer_addr.clone(),
                ));
            }
            keep
        });
        let greets = self.cfg.greet_pruned && !self.leaving;
        for (id, generation, addr, peer_addr) in gone {
            let wait = if greets && !unroutable(&addr) {
                self.cfg.ack_timeout_ms
            } else {
                0
            };
            self.departed.insert(
                id,
                Departed {
                    generation,
                    addr,
                    peer_addr,
                    confirm_at: now + wait,
                },
            );
        }
        if greets {
            for addr in pruned {
                out.push(self.greet(&addr));
            }
        }
        self.confirm_departed(now, out);
    }

    /// Raise [`Action::Gone`] for every pruned member whose parting greeting has gone
    /// unanswered for long enough.
    fn confirm_departed(&mut self, now: u64, out: &mut Vec<Action>) {
        if self.departed.is_empty() {
            return;
        }
        let due: Vec<NodeId> = self
            .departed
            .iter()
            .filter(|(_, d)| d.confirm_at <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in due {
            if let Some(d) = self.departed.remove(&id) {
                out.push(Action::Gone {
                    id,
                    addr: d.addr,
                    peer_addr: d.peer_addr,
                });
            }
        }
    }

    fn expire_suspects(&mut self, now: u64, out: &mut Vec<Action>) {
        // A last probe that ran out unanswered while the member's window is NOT lapsed:
        // the window grew after the probe was sent (this node's awareness rose, or a
        // suspicion at a higher incarnation reset the suspecters). The verdict is not
        // due, so the probe is dropped — and when the longer window lapses the member
        // is asked afresh, instead of being declared `Dead` on the silence of a probe
        // sent a window ago.
        let overtaken: Vec<NodeId> = self
            .members
            .values()
            .filter(|m| {
                m.state == MemberState::Suspect
                    && m.confirm.is_some_and(|(_, deadline)| now >= deadline)
                    && now.saturating_sub(m.state_since) < self.effective_suspicion_timeout(m)
            })
            .map(|m| m.id.clone())
            .collect();
        for id in overtaken {
            if let Some(m) = self.members.get_mut(&id) {
                m.confirm = None;
            }
        }
        let timed_out: Vec<NodeId> = self
            .members
            .values()
            .filter(|m| {
                m.state == MemberState::Suspect
                    && now.saturating_sub(m.state_since) >= self.effective_suspicion_timeout(m)
            })
            .map(|m| m.id.clone())
            .collect();
        for id in timed_out {
            if self.cfg.confirm_before_dead {
                match self.members.get(&id).and_then(|m| m.confirm) {
                    // The window has lapsed: ask the suspect itself, once.
                    None => {
                        self.confirm_suspect(&id, now, out);
                        continue;
                    }
                    // Asked, and still inside its ack timeout.
                    Some((_, deadline)) if now < deadline => continue,
                    // Asked, and silent: dead.
                    Some(_) => self.last_probes.silent += 1,
                }
            }
            self.declare(&id, MemberState::Dead, now, out);
        }
    }

    /// `Config::confirm_before_dead`: send `id` one `Ping` that carries our suspicion
    /// of it, and give it one ack timeout (the health-scaled one, as for any probe:
    /// 250 ms on a healthy node). If it is alive and hears us, applying that
    /// claim makes it bump its incarnation and its `Ack` carries the refutation
    /// first-hand, which clears the suspicion (and this probe) in `apply_update`; if
    /// its `Ack` carries no refutation, [`on_confirm_ack`](Self::on_confirm_ack)
    /// still keeps it.
    fn confirm_suspect(&mut self, id: &NodeId, now: u64, out: &mut Vec<Action>) {
        let deadline = now + self.scaled_ack_timeout();
        self.seq += 1;
        let seq = self.seq;
        let Some(m) = self.members.get_mut(id) else {
            return;
        };
        m.confirm = Some((seq, deadline));
        self.last_probes.sent += 1;
        let claim = Update {
            id: id.0.clone(),
            addr: m.addr.clone(),
            peer_addr: m.peer_addr.clone(),
            incarnation: m.incarnation,
            generation: m.generation,
            state: MemberState::Suspect,
            suspecter: Some(self.local.0.clone()),
            failure_domain: m.failure_domain.clone(),
        };
        let to = m.addr.clone();
        // The claim and nothing else, and not out of the gossip queue. Every update
        // there has a bounded number of retransmissions, and a node whose windows for
        // many suspects lapse on one tick (a partition healing, a rack coming back)
        // would otherwise spend them on members that may not be there to hear —
        // among them this node's own refutation, which it puts first in the queue.
        let msg = Message {
            from: self.local.0.clone(),
            from_addr: self.local_addr.clone(),
            from_peer_addr: self.local_peer_addr.clone(),
            from_domain: self.local_domain.clone(),
            from_generation: self.generation,
            cluster_id: None, // stamped by the driver at send time (ADR 0054)
            kind: Kind::Ping { seq },
            gossip: vec![claim],
        };
        out.push(Action::Send { to, msg });
    }

    /// `Config::confirm_before_dead`: `from` sent an `Ack` with `seq`. If that is the
    /// answer to the last direct probe of a member we still hold `Suspect`, the member
    /// is alive and hears us — proof no relay can fake — so it is not declared `Dead`
    /// on this window: the window starts again from now, at its full length (the
    /// suspicions that had shortened it are answered by the same proof).
    ///
    /// Still holding it `Suspect` here means the `Ack` carried no refutation that
    /// supersedes our claim (one that did has already cleared the probe in
    /// `apply_update`): our claim was stale, or the member queued its refutation
    /// behind other gossip. Only the member can state its incarnation, so it is asked:
    /// one `Join` that carries the suspicion as it is held now, answered by a `Sync`
    /// that leads with the member's own claim about itself, first-hand. Returns whether
    /// the `Ack` was that answer.
    ///
    /// `generation` is the life the `Ack` came from (the datagram's `from_generation`)
    /// and must be the life held for the member. An `Ack` from an EARLIER life — the
    /// same id restarted in place with a lower generation (its host's wall clock
    /// stepped back), or a second process misconfigured with this id — is not the
    /// suspect's answer: everything that process says about itself is discarded as a
    /// previous life's (`apply_update_from`), so it could never clear the suspicion,
    /// and sparing it would hold the member `Suspect` for good — asked every window,
    /// never `Dead`, never pruned, its address never updated. Unanswered, the held
    /// life is declared `Dead` and pruned, and the process is then learned afresh at
    /// the generation it actually runs. (A LATER life never gets here as a suspect:
    /// `handle` has already made it a new, `Alive` life.)
    fn on_confirm_ack(
        &mut self,
        from: &NodeId,
        generation: u64,
        seq: u64,
        now: u64,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(m) = self.members.get_mut(from) else {
            return false;
        };
        if m.state != MemberState::Suspect
            || m.confirm.map(|(s, _)| s) != Some(seq)
            || generation != m.generation
        {
            return false;
        }
        m.confirm = None;
        self.last_probes.spared += 1;
        m.state_since = now;
        m.suspecters.clear();
        m.unanswered_probes = 0; // it hears us
        let addr = m.addr.clone();
        // What we still hold against it, as we hold it NOW — which is not always what
        // the probe carried: a suspicion at a higher incarnation may have arrived
        // while the probe was out. Told of that one, the member refutes it, and its
        // `Sync` supersedes; told nothing, it would answer `Alive` at the incarnation
        // we already suspect, which supersedes nothing, and sit `Suspect` for another
        // whole window. (A claim that is stale to it is ignored, and its `Sync` then
        // carries the higher incarnation it is already at.)
        let held = Update {
            id: from.0.clone(),
            addr: m.addr.clone(),
            peer_addr: m.peer_addr.clone(),
            incarnation: m.incarnation,
            generation: m.generation,
            state: MemberState::Suspect,
            suspecter: Some(self.local.0.clone()),
            failure_domain: m.failure_domain.clone(),
        };
        self.failed_probe_rounds = 0; // and our outbound path works
        let mut ask = self.greet(&addr);
        if let Action::Send { msg, .. } = &mut ask {
            msg.gossip.push(held);
        }
        out.push(ask);
        true
    }

    fn start_probe(&mut self, now: u64, out: &mut Vec<Action>) {
        let Some(target) = self.next_probe_target() else {
            return;
        };
        let Some(addr) = self.members.get(&target).map(|m| m.addr.clone()) else {
            return;
        };
        self.seq += 1;
        let seq = self.seq;
        let msg = self.message(Kind::Ping { seq });
        out.push(Action::Send {
            to: addr.clone(),
            msg,
        });
        // Deaf-peer re-prime (issue #383): a member that has ignored `reprime_after`
        // consecutive probes of ours may simply be DROPPING our fingerprint-sealed
        // datagrams (it lost our certificate — cert-miss). Reintroduce ourselves with
        // a `Join`: the driver seals Join/Sync frames with the FULL certificate, and
        // the peer's `Sync` reply re-primes our cache with its certificate in turn.
        // Cheap and self-limiting: only fires while the deafness persists.
        let deaf = self
            .members
            .get(&target)
            .is_some_and(|m| m.unanswered_probes >= self.cfg.reprime_after.max(1));
        if deaf {
            let greet = self.message(Kind::Join);
            out.push(Action::Send {
                to: addr,
                msg: greet,
            });
        }
        let ack_deadline = now + self.scaled_ack_timeout();
        self.probe = Some(Probe {
            target,
            seq,
            ack_deadline,
            indirect_deadline: None,
        });
    }

    /// Round-robin over a per-round shuffle of alive members.
    fn next_probe_target(&mut self) -> Option<NodeId> {
        if self.probe_idx >= self.probe_order.len() {
            self.probe_order = self
                .members
                .values()
                .filter(|m| m.state != MemberState::Dead)
                .map(|m| m.id.clone())
                .collect();
            // Fisher-Yates shuffle.
            let len = self.probe_order.len();
            for i in (1..len).rev() {
                let j = usize::try_from(self.xorshift() % (i as u64 + 1)).unwrap_or(0);
                self.probe_order.swap(i, j);
            }
            self.probe_idx = 0;
        }
        let item = self.probe_order.get(self.probe_idx).cloned();
        self.probe_idx += 1;
        item
    }

    fn random_alive_helpers(&mut self, exclude: &NodeId) -> Vec<String> {
        let mut candidates: Vec<String> = self
            .members
            .values()
            .filter(|m| m.state == MemberState::Alive && &m.id != exclude)
            .map(|m| m.addr.clone())
            .collect();
        // Shuffle and take k.
        let len = candidates.len();
        for i in (1..len).rev() {
            let j = usize::try_from(self.xorshift() % (i as u64 + 1)).unwrap_or(0);
            candidates.swap(i, j);
        }
        candidates.truncate(self.cfg.indirect_probes);
        candidates
    }

    /// Handle an inbound datagram at clock `now`.
    pub fn handle(&mut self, msg: Message, now: u64) -> Vec<Action> {
        let mut out = Vec::new();

        // A datagram from OURSELVES is a reflection, not a peer (issue #396): a
        // probe of a member recorded at an unroutable address lands on our own
        // socket, and answering it would ack our own probe — falsely confirming
        // the unreachable member alive, forever. Drop reflections outright.
        if msg.from == self.local.0 {
            return out;
        }

        // Learn the sender as an alive member if new; if known, refresh its SWIM
        // address from where this datagram ACTUALLY came from (the driver stamps
        // `from_addr` with the UDP source). Refreshing on EVERY datagram — not
        // just first contact — is what lets a record poisoned by an unroutable
        // relay heal the moment the member speaks to us directly (issue #396).
        let from_id = NodeId(msg.from.clone());
        if from_id != self.local {
            if let Some(m) = self.members.get_mut(&from_id) {
                if m.addr != msg.from_addr && !unroutable(&msg.from_addr) {
                    m.addr.clone_from(&msg.from_addr);
                }
                // A datagram from a NEWER LIFE of a known member is itself proof that
                // the new process is alive (issue #783). Before this, a node that left
                // gracefully stayed `Dead` on every peer after its restart: its own
                // datagrams only refreshed the address, its Join carried no claim about
                // itself, and — seeded only to the founder, as deployed — no survivor
                // re-greeted it to draw out its full state; once the tombstone was
                // pruned each survivor re-learned the old life's `Dead` from another,
                // re-declared every `dead_ttl_ms`, never re-admitted. (A crash-restart
                // hid this: peers still held the old life `Alive`, and acks do not look
                // at generations.) The bootstrap self-announcement in `tick` now also
                // carries the new life; this is the half that does not depend on that
                // claim's gossip budget lasting. First-hand, as in #383: the sender's
                // own authenticated datagram, which a dead process cannot send.
                if msg.from_generation > m.generation {
                    let update = Update {
                        id: msg.from.clone(),
                        addr: msg.from_addr.clone(),
                        peer_addr: msg.from_peer_addr.clone(),
                        incarnation: 0,
                        generation: msg.from_generation,
                        state: MemberState::Alive,
                        suspecter: None,
                        failure_domain: msg.from_domain.clone(),
                    };
                    self.apply_update_from(&update, now, &mut out, true);
                }
            } else {
                let update = Update {
                    id: msg.from.clone(),
                    addr: msg.from_addr.clone(),
                    peer_addr: msg.from_peer_addr.clone(),
                    incarnation: 0,
                    generation: msg.from_generation,
                    state: MemberState::Alive,
                    suspecter: None,
                    // First contact teaches the sender's label directly (ADR 0016 T5).
                    failure_domain: msg.from_domain.clone(),
                };
                self.apply_update_from(&update, now, &mut out, true);
            }
        }

        // Merge piggybacked gossip. A claim about the SENDER ITSELF is first-hand
        // (issue #383) — its own refutation may pierce a tombstone; everything else
        // is a relay and stays fully fenced.
        for u in &msg.gossip {
            let first_hand = u.id == msg.from;
            self.apply_update_from(u, now, &mut out, first_hand);
        }

        match msg.kind {
            Kind::Ping { seq } => {
                let reply = self.message(Kind::Ack { seq });
                out.push(Action::Send {
                    to: msg.from_addr,
                    msg: reply,
                });
            }
            Kind::Ack { seq } => {
                self.on_ack(&from_id, msg.from_generation, seq, now, &mut out);
            }
            Kind::PingReq {
                target,
                target_addr,
            } => {
                // Relay: probe the target, remembering who to answer. If the
                // requester only knows the target at an unroutable address
                // (issue #396) but WE hold a dialable one — typically source-
                // learned from the target's own datagrams — probe that instead:
                // the indirect ack that follows is exactly how the requester's
                // poisoned record gets repaired.
                let dial = if unroutable(&target_addr) {
                    self.members
                        .get(&NodeId(target.clone()))
                        .filter(|m| !unroutable(&m.addr))
                        .map_or(target_addr, |m| m.addr.clone())
                } else {
                    target_addr
                };
                self.seq += 1;
                let relay_seq = self.seq;
                self.relays
                    .insert(relay_seq, (msg.from_addr, NodeId(target.clone())));
                let ping = self.message(Kind::Ping { seq: relay_seq });
                out.push(Action::Send {
                    to: dial,
                    msg: ping,
                });
            }
            Kind::IndirectAck { target } => {
                if let Some(p) = &self.probe {
                    if p.target.0 == target {
                        self.probe = None;
                        self.lower_awareness(); // an indirect probe still succeeded
                        self.failed_probe_rounds = 0; // our PingReq reached a helper

                        // The member is alive — but it still has not answered US,
                        // so the deaf-peer count stands (issue #971).
                        if !self.cfg.reprime_on_rescued_probe {
                            if let Some(m) = self.members.get_mut(&NodeId(target)) {
                                m.unanswered_probes = 0;
                            }
                        }
                    }
                }
            }
            Kind::Join => {
                // Reply with our full membership view as gossip.
                self.gossip_full_state();
                let reply = self.message(Kind::Sync);
                out.push(Action::Send {
                    to: msg.from_addr,
                    msg: reply,
                });
            }
            Kind::Sync => {} // gossip already merged above
        }
        out
    }

    fn on_ack(
        &mut self,
        from: &NodeId,
        generation: u64,
        seq: u64,
        now: u64,
        out: &mut Vec<Action>,
    ) {
        // The answer to a suspect's last direct probe (`Config::confirm_before_dead`)?
        if self.on_confirm_ack(from, generation, seq, now, out) {
            return;
        }
        // Direct ack for our own probe?
        if let Some(p) = &self.probe {
            if p.seq == seq {
                let target = p.target.clone();
                self.probe = None;
                self.lower_awareness(); // a clean round (ADR 0016 §2)
                self.failed_probe_rounds = 0; // our outbound path demonstrably works
                if let Some(m) = self.members.get_mut(&target) {
                    m.unanswered_probes = 0; // this member can hear us again
                }
                return;
            }
        }
        // Ack for a probe we relayed on someone's behalf?
        if let Some((requester, target)) = self.relays.remove(&seq) {
            let reply = self.message(Kind::IndirectAck {
                target: target.0.clone(),
            });
            out.push(Action::Send {
                to: requester,
                msg: reply,
            });
        }
    }

    /// Push our entire view (including ourselves) into the gossip buffer.
    fn gossip_full_state(&mut self) {
        let mut updates = vec![Update {
            id: self.local.0.clone(),
            addr: self.local_addr.clone(),
            peer_addr: self.local_peer_addr.clone(),
            incarnation: self.incarnation,
            generation: self.generation,
            state: MemberState::Alive,
            suspecter: None,
            failure_domain: self.local_domain.clone(),
        }];
        for m in self.members.values() {
            updates.push(Update {
                id: m.id.0.clone(),
                addr: m.addr.clone(),
                peer_addr: m.peer_addr.clone(),
                incarnation: m.incarnation,
                generation: m.generation,
                state: m.state,
                // A full-state relay does not assert independent suspicion (ADR 0016 §3);
                // real suspecters propagate via the normal gossip re-broadcast path.
                suspecter: None,
                failure_domain: m.failure_domain.clone(),
            });
        }
        for u in updates {
            self.enqueue_gossip(u);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Action, Config, Kind, MemberState, Message, Swim, Update};
    use crate::NodeId;

    /// Brisk timings, and the last direct probe before `Dead` OFF: the tests of the
    /// window, the tombstone and the generations state exactly when a member becomes
    /// `Dead`, and the probe (one more ack timeout, one more datagram) has its own
    /// tests below, which switch it on — as does every simulation and every cluster
    /// suite, which run the default.
    fn fast_cfg() -> Config {
        Config {
            protocol_period_ms: 100,
            ack_timeout_ms: 20,
            suspicion_timeout_ms: 200,
            suspicion_min_timeout_ms: 80,
            suspicion_confirmations: 3,
            dead_ttl_ms: 2000,
            indirect_probes: 2,
            gossip_fanout: 8,
            gossip_multiplier: 3,
            awareness_max: 8,
            isolation_rounds: 3,
            reprime_after: 3,
            reprime_on_rescued_probe: true,
            greet_pruned: true,
            suspicion_scales_with_log_n: false,
            confirm_before_dead: false,
            period_from_schedule: true,
        }
    }

    /// Test convention: a node at SWIM address `addr` has routing address `addr-peer`.
    fn peer_addr_of(addr: &str) -> String {
        format!("{addr}-peer")
    }

    fn node(id: &str, addr: &str, seeds: &[&str]) -> Swim {
        Swim::new(
            NodeId(id.to_string()),
            addr.to_string(),
            peer_addr_of(addr),
            None,
            1,
            fast_cfg(),
            seeds.iter().map(|s| (*s).to_string()).collect(),
        )
    }

    /// A node that advertises its own failure-domain label (ADR 0016 T5).
    fn node_in_domain(id: &str, addr: &str, seeds: &[&str], domain: &str) -> Swim {
        Swim::new(
            NodeId(id.to_string()),
            addr.to_string(),
            peer_addr_of(addr),
            Some(domain.to_string()),
            1,
            fast_cfg(),
            seeds.iter().map(|s| (*s).to_string()).collect(),
        )
    }

    fn alive_update(id: &str, addr: &str, inc: u64) -> Update {
        Update {
            id: id.to_string(),
            addr: addr.to_string(),
            peer_addr: peer_addr_of(addr),
            incarnation: inc,
            generation: 1,
            state: MemberState::Alive,
            suspecter: None,
            failure_domain: None,
        }
    }

    fn dead_update(id: &str, addr: &str, inc: u64) -> Update {
        Update {
            state: MemberState::Dead,
            ..alive_update(id, addr, inc)
        }
    }

    fn suspect_update(id: &str, addr: &str, inc: u64) -> Update {
        Update {
            state: MemberState::Suspect,
            ..alive_update(id, addr, inc)
        }
    }

    /// A `Suspect` claim about `id` asserted by node `by` (ADR 0016 §3).
    fn suspect_from(id: &str, addr: &str, inc: u64, by: &str) -> Update {
        Update {
            suspecter: Some(by.to_string()),
            ..suspect_update(id, addr, inc)
        }
    }

    fn m(from: &str, from_addr: &str, kind: Kind, gossip: Vec<Update>) -> Message {
        Message {
            from: from.to_string(),
            from_addr: from_addr.to_string(),
            from_peer_addr: peer_addr_of(from_addr),
            from_domain: None,
            from_generation: 1,
            cluster_id: None,
            kind,
            gossip,
        }
    }

    #[test]
    fn learns_a_new_member_from_gossip() {
        let mut s = node("a", "a:1", &[]);
        let msg = m("b", "b:1", Kind::Sync, vec![alive_update("c", "c:1", 0)]);
        let actions = s.handle(msg, 0);
        // b (sender) and c (gossip) both become known.
        let ids: Vec<_> = s.members().into_iter().map(|m| m.id.0).collect();
        assert!(ids.contains(&"b".to_string()));
        assert!(ids.contains(&"c".to_string()));
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::StateChange { id, .. } if id.0 == "c")));
    }

    #[test]
    fn ping_is_acked() {
        let mut s = node("a", "a:1", &[]);
        let ping = m("b", "b:1", Kind::Ping { seq: 7 }, vec![]);
        let actions = s.handle(ping, 0);
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Send { to, msg } if to == "b:1" && matches!(msg.kind, Kind::Ack { seq: 7 })
        )));
    }

    #[test]
    fn peer_addr_is_learned_from_first_contact_and_kept_on_empty_claims() {
        let mut s = node("a", "a:1", &[]);
        // First contact teaches b's routing address from the message envelope.
        s.handle(m("b", "b:1", Kind::Join, vec![]), 0);
        let b = s.members().into_iter().find(|m| m.id.0 == "b").unwrap();
        assert_eq!(b.peer_addr, peer_addr_of("b:1"));

        // A later claim that lacks the routing address must not erase it.
        let mut out = Vec::new();
        s.apply_update(
            &Update {
                id: "b".into(),
                addr: "b:1".into(),
                peer_addr: String::new(),
                incarnation: 5,
                generation: 1,
                state: MemberState::Alive,
                suspecter: None,
                failure_domain: None,
            },
            1,
            &mut out,
        );
        let b = s.members().into_iter().find(|m| m.id.0 == "b").unwrap();
        assert_eq!(b.peer_addr, peer_addr_of("b:1"));
        assert_eq!(b.incarnation, 5);
    }

    #[test]
    fn higher_incarnation_supersedes() {
        let mut s = node("a", "a:1", &[]);
        s.handle(
            m("x", "x:1", Kind::Sync, vec![alive_update("b", "b:1", 0)]),
            0,
        );
        // Suspect b at incarnation 0.
        let mut out = Vec::new();
        s.apply_update(
            &Update {
                id: "b".into(),
                addr: "b:1".into(),
                peer_addr: peer_addr_of("b:1"),
                incarnation: 0,
                generation: 1,
                state: MemberState::Suspect,
                suspecter: None,
                failure_domain: None,
            },
            1,
            &mut out,
        );
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        // A fresh Alive at higher incarnation clears the suspicion.
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 1), 2, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
    }

    #[test]
    fn refutes_suspicion_about_self() {
        let mut s = node("a", "a:1", &[]);
        let start_inc = current_incarnation(&s);
        let mut out = Vec::new();
        s.apply_update(
            &Update {
                id: "a".into(),
                addr: "a:1".into(),
                peer_addr: peer_addr_of("a:1"),
                incarnation: start_inc,
                generation: 1,
                state: MemberState::Suspect,
                suspecter: None,
                failure_domain: None,
            },
            0,
            &mut out,
        );
        // We bumped our incarnation and queued an Alive refutation.
        assert!(current_incarnation(&s) > start_inc);
        let gossiped = s.take_gossip();
        assert!(gossiped
            .iter()
            .any(|u| u.id == "a" && u.state == MemberState::Alive));
    }

    /// ADR 0019 §2: a graceful leave announces ourselves `Dead` directly to every known
    /// (non-dead) peer, carrying the departure as gossip, and queues it for re-broadcast.
    #[test]
    fn leave_announces_self_as_dead_to_every_known_peer() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.apply_update(&alive_update("c", "c:1", 0), 0, &mut out);
        let inc = current_incarnation(&s);

        let actions = s.leave();

        // One direct announcement to each peer, each carrying our `Dead` at our
        // current incarnation (no bump).
        let mut targets: Vec<String> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { to, msg } => {
                    assert!(
                        msg.gossip.iter().any(|u| u.id == "a"
                            && u.state == MemberState::Dead
                            && u.incarnation == inc),
                        "each leave datagram carries our Dead departure"
                    );
                    Some(to.clone())
                }
                Action::StateChange { .. } | Action::Gone { .. } => None,
            })
            .collect();
        targets.sort();
        assert_eq!(targets, vec!["b:1".to_string(), "c:1".to_string()]);
        // We did not bump our own incarnation to leave.
        assert_eq!(current_incarnation(&s), inc);
    }

    /// A peer that receives a graceful-leave announcement marks the leaver `Dead`
    /// **immediately** — no suspicion window — so placement drops it at once.
    #[test]
    fn a_peer_marks_a_leaving_node_dead_immediately() {
        let mut leaver = node("a", "a:1", &[]);
        let mut peer = node("b", "b:1", &[]);
        // Each knows the other as alive.
        let mut out = Vec::new();
        leaver.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        peer.apply_update(&alive_update("a", "a:1", 0), 0, &mut out);

        // The leaver announces departure; deliver its datagram to the peer.
        let actions = leaver.leave();
        let leave_msg = actions
            .into_iter()
            .find_map(|a| match a {
                Action::Send { to, msg } if to == "b:1" => Some(msg),
                _ => None,
            })
            .expect("a leave datagram addressed to b");
        let observed = peer.handle(leave_msg, 0);

        assert_eq!(member_state(&peer, "a"), Some(MemberState::Dead));
        assert!(
            observed
                .iter()
                .any(|a| matches!(a, Action::StateChange { id, state, .. }
                    if id.0 == "a" && *state == MemberState::Dead)),
            "the peer emits a Dead state change for the leaver"
        );
    }

    /// Issue #783: a node that left GRACEFULLY is re-admitted when it restarts. The
    /// peers hold it `Dead` and tombstoned, and the restarted process (a newer
    /// generation) greets a seed with a Join. Either half of the fix revives it on its
    /// own: the Join's first-hand self-claim, and — should that claim not be on board
    /// (its gossip budget spent, say) — the datagram's newer generation alone. A
    /// still-circulating `Dead` about the old life must not undo it, on the greeted
    /// peer or on a third node that hears of the new life by relay.
    #[test]
    fn a_gracefully_left_node_is_readmitted_when_it_restarts() {
        let mut leaver = node("a", "a:1", &[]);
        let mut b = node("b", "b:1", &[]);
        let mut c = node("c", "c:1", &[]);
        let mut d = node("d", "d:1", &[]);
        let mut out = Vec::new();
        for peer in ["b", "c", "d"] {
            leaver.apply_update(&alive_update(peer, &format!("{peer}:1"), 0), 0, &mut out);
        }
        for peer in [&mut b, &mut c, &mut d] {
            peer.apply_update(&alive_update("a", "a:1", 0), 0, &mut out);
        }
        b.apply_update(&alive_update("c", "c:1", 0), 0, &mut out);
        c.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);

        // The graceful stop: a's Dead departure reaches every peer.
        for action in leaver.leave() {
            if let Action::Send { to, msg } = action {
                match to.as_str() {
                    "b:1" => drop(b.handle(msg, 0)),
                    "c:1" => drop(c.handle(msg, 0)),
                    "d:1" => drop(d.handle(msg, 0)),
                    _ => {}
                }
            }
        }
        for peer in [&b, &c, &d] {
            assert_eq!(member_state(peer, "a"), Some(MemberState::Dead));
        }

        // The restart: same id and address, a newer generation, seeded at b.
        let mut restarted = Swim::new(
            NodeId("a".into()),
            "a:1".into(),
            peer_addr_of("a:1"),
            None,
            2,
            fast_cfg(),
            vec!["b:1".into()],
        );
        let join = restarted
            .tick(10)
            .into_iter()
            .find_map(|a| match a {
                Action::Send { to, msg } if to == "b:1" && matches!(msg.kind, Kind::Join) => {
                    Some(msg)
                }
                _ => None,
            })
            .expect("the restarted node greets its seed");
        assert!(
            join.gossip.iter().any(|u| u.id == "a"
                && u.generation == 2
                && u.incarnation == 1
                && u.state == MemberState::Alive),
            "the greeting carries a first-hand claim of the new life at its real incarnation"
        );

        // Half one: the self-claim alone revives it (d gets the Join as sent).
        drop(d.handle(join.clone(), 10));
        assert_eq!(
            member_state(&d, "a"),
            Some(MemberState::Alive),
            "the self-claim re-admits the new life"
        );

        // Half two: with the claim stripped, the datagram's newer generation alone does.
        let bare = Message {
            gossip: Vec::new(),
            ..join
        };
        let observed = b.handle(bare, 10);
        assert_eq!(
            member_state(&b, "a"),
            Some(MemberState::Alive),
            "the greeted peer re-admits the new life from its own datagram"
        );
        assert!(
            observed
                .iter()
                .any(|a| matches!(a, Action::StateChange { id, state, .. }
                if id.0 == "a" && *state == MemberState::Alive)),
            "and tells the link layer, so it dials the restarted node"
        );

        // The old life's Dead, still relayed by c, says nothing about the new life.
        drop(b.handle(
            m("c", "c:1", Kind::Sync, vec![dead_update("a", "a:1", 1)]),
            11,
        ));
        assert_eq!(member_state(&b, "a"), Some(MemberState::Alive));

        // c hears of the new life from b's piggybacked gossip and re-admits it too.
        let gossip = b.take_gossip();
        assert!(
            gossip
                .iter()
                .any(|u| u.id == "a" && u.generation == 2 && u.state == MemberState::Alive),
            "b relays the new life"
        );
        drop(c.handle(m("b", "b:1", Kind::Sync, gossip), 12));
        assert_eq!(
            member_state(&c, "a"),
            Some(MemberState::Alive),
            "a third node re-admits the new life by relay"
        );
    }

    /// Once leaving, a node does **not** refute `Dead` about itself — even its own
    /// departure gossip echoed back by a peer — so the leave is not undone.
    #[test]
    fn a_leaving_node_does_not_refute_its_own_dead() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        let inc = current_incarnation(&s);
        let _ = s.leave();
        let _ = s.take_gossip(); // drain the queued departure so we inspect only the echo

        // A peer re-gossips our Dead back to us.
        s.apply_update(&dead_update("a", "a:1", inc), 1, &mut out);

        // No self-refutation: incarnation unchanged and no Alive-about-self queued.
        assert_eq!(current_incarnation(&s), inc, "leaving suppresses the bump");
        assert!(
            !s.take_gossip()
                .iter()
                .any(|u| u.id == "a" && u.state == MemberState::Alive),
            "a leaving node does not queue an Alive refutation about itself"
        );
    }

    /// Drive one full probe round against a silent network at round-start `t`
    /// (`fast_cfg` timings: ack deadline +20, indirect deadline +40), then deliver
    /// inbound gossip re-aliving `b` at incarnation `inc` — the #368 shape where
    /// our outbound datagrams vanish while inbound keeps the view looking fresh.
    fn fail_round_against_fresh_gossip(s: &mut Swim, t: u64, inc: u64) {
        let mut out = Vec::new();
        s.tick(t); //          Ping goes out (and vanishes)
        s.tick(t + 21); //     ack deadline -> indirect escalation (no helpers)
        s.tick(t + 41); //     indirect deadline -> the round concludes unanswered
        s.apply_update(&alive_update("b", "b:1", inc), t + 50, &mut out);
    }

    #[test]
    fn a_run_of_unanswered_probe_rounds_marks_the_node_isolated_while_gossip_keeps_the_view_fresh()
    {
        // Issue #368's one-way failure: outbound lost, inbound intact. The local
        // view stays fully Alive and current-looking the whole time — isolation
        // is the signal that none of it is confirmed by us.
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0); // bootstrap
        for round in 1..=3u64 {
            fail_round_against_fresh_gossip(&mut s, round * 100, round);
            assert_eq!(
                member_state(&s, "b"),
                Some(MemberState::Alive),
                "inbound gossip keeps the view deceptively fresh"
            );
            assert_eq!(s.failed_probe_rounds(), u32::try_from(round).unwrap());
            assert_eq!(
                s.isolated(),
                round >= 3,
                "isolation trips at the configured 3 consecutive rounds"
            );
        }
    }

    #[test]
    fn a_single_ack_of_our_probe_clears_isolation() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0);
        for round in 1..=3u64 {
            fail_round_against_fresh_gossip(&mut s, round * 100, round);
        }
        assert!(s.isolated());

        // Round 4: the network heals and b's ack gets through.
        let actions = s.tick(400);
        let seq = actions
            .iter()
            .find_map(|a| match a {
                Action::Send { msg, .. } => match &msg.kind {
                    Kind::Ping { seq } => Some(*seq),
                    _ => None,
                },
                Action::StateChange { .. } | Action::Gone { .. } => None,
            })
            .expect("round 4 sends a Ping");
        s.handle(m("b", "b:1", Kind::Ack { seq }, vec![]), 401);
        assert!(!s.isolated(), "one confirmed round trip clears isolation");
        assert_eq!(s.failed_probe_rounds(), 0);
    }

    /// Issue #383 (the livelock half): a LIVE node falsely declared dead refutes
    /// with a higher-incarnation Alive in its OWN datagram — first-hand — and that
    /// pierces the tombstone fence. The same claim arriving as a third-party relay
    /// stays fenced (#92's crashed-node protection intact: a dead node can never
    /// send first-hand).
    #[test]
    fn a_first_hand_refutation_pierces_the_tombstone_but_a_relay_does_not() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.apply_update(&dead_update("b", "b:1", 0), 0, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));

        // A RELAY: node c's datagram carries a higher-incarnation Alive about b —
        // fenced, exactly as before this change.
        s.handle(
            m("c", "c:1", Kind::Sync, vec![alive_update("b", "b:1", 7)]),
            1,
        );
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Dead),
            "a relayed refutation must stay fenced (#92)"
        );

        // FIRST-HAND: b's own datagram carries its refutation — the fence opens,
        // because a crashed node cannot produce this message.
        s.handle(
            m("b", "b:1", Kind::Sync, vec![alive_update("b", "b:1", 8)]),
            2,
        );
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Alive),
            "the member's own higher-incarnation Alive is proof of life"
        );
    }

    /// Issue #383 (the livelock, end to end in miniature): after the first-hand
    /// revival, a still-circulating stale Dead claim (same incarnation it was
    /// killed at) must NOT re-kill the member — the refutation's higher
    /// incarnation outranks it. This is the loop that kept a healthy node
    /// evicted for the whole 5-node measurement window.
    #[test]
    fn a_stale_circulating_dead_claim_cannot_rekill_the_refuted_member() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.apply_update(&dead_update("b", "b:1", 0), 0, &mut out);
        s.handle(
            m("b", "b:1", Kind::Sync, vec![alive_update("b", "b:1", 1)]),
            1,
        );
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
        // The old Dead claim (incarnation 0) is still circulating among evictors.
        s.handle(
            m("c", "c:1", Kind::Sync, vec![dead_update("b", "b:1", 0)]),
            2,
        );
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Alive),
            "a stale lower-incarnation Dead claim must not re-kill the refuted member"
        );
    }

    /// Issue #383 (the cert half): a member that ignores `reprime_after`
    /// consecutive probes gets the NEXT probe accompanied by a `Join` re-greet —
    /// the frame the driver seals with the full certificate, healing a receiver
    /// whose cert cache lost us (every fingerprint-sealed datagram of ours was
    /// dropping as cert-miss, and we could not know).
    #[test]
    fn a_deaf_member_gets_a_certificate_carrying_regreet() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0); // bootstrap
        let joins = |actions: &[Action]| {
            actions
                .iter()
                .filter(|a| matches!(a, Action::Send { msg, .. } if matches!(msg.kind, Kind::Join)))
                .count()
        };
        // Rounds 1-3: silent — no re-greet yet (threshold is 3 unanswered).
        for round in 1..=3u64 {
            let t = round * 100;
            let actions = s.tick(t);
            assert_eq!(joins(&actions), 0, "round {round}: below the threshold");
            s.tick(t + 21);
            s.tick(t + 41); // concludes unanswered; counter -> round
                            // Keep b probe-able: fresh first-hand Alive each round.
            s.handle(
                m(
                    "b",
                    "b:1",
                    Kind::Sync,
                    vec![alive_update("b", "b:1", round)],
                ),
                t + 50,
            );
        }
        // Round 4: the probe of the now-deaf b carries a Join re-greet.
        let actions = s.tick(400);
        assert_eq!(
            joins(&actions),
            1,
            "the deaf member is re-greeted: {actions:?}"
        );
        // An ack resets the deafness: the next round has no re-greet.
        let seq = actions
            .iter()
            .find_map(|a| match a {
                Action::Send { msg, .. } => match &msg.kind {
                    Kind::Ping { seq } => Some(*seq),
                    _ => None,
                },
                Action::StateChange { .. } | Action::Gone { .. } => None,
            })
            .expect("round 4 sends a Ping");
        s.handle(m("b", "b:1", Kind::Ack { seq }, vec![]), 401);
        let actions = s.tick(500);
        assert_eq!(
            joins(&actions),
            0,
            "an answered member is no longer re-greeted"
        );
    }

    #[test]
    fn probe_failure_leads_to_suspect_then_dead() {
        let mut s = node("a", "a:1", &[]);
        // Know one peer, b, and no helpers, so indirect probing finds nobody.
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);

        // First tick only bootstraps; the probe starts one period later.
        s.tick(0);
        let actions = s.tick(100); // start probe (Ping to b)
        assert!(actions.iter().any(|a| matches!(
            a, Action::Send { msg, .. } if matches!(msg.kind, Kind::Ping { .. })
        )));

        // No ack: ack deadline (120) escalates to indirect; indirect deadline (140)
        // with no helpers concludes failure -> Suspect.
        s.tick(120);
        s.tick(141);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));

        // After the suspicion timeout (since 141, +200) -> Dead.
        let actions = s.tick(342);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
        assert!(actions.iter().any(|a| matches!(
            a, Action::StateChange { id, state: MemberState::Dead, .. } if id.0 == "b"
        )));
    }

    /// ADR 0016 phase 1: once a member is `Dead` it is tombstoned, and no gossiped
    /// update revives it — not even a higher-incarnation `Alive` (e.g. the node's own
    /// last refutation still in flight when it died). This is the resurrection that
    /// corrupted the recovery replica set after a takeover.
    #[test]
    fn a_dead_member_is_not_revived_by_stale_higher_incarnation_gossip() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.apply_update(&dead_update("b", "b:1", 0), 0, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));

        // A stale Alive about b at a much higher incarnation arrives — it must NOT
        // revive the tombstone.
        s.apply_update(&alive_update("b", "b:1", 99), 1, &mut out);
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Dead),
            "a tombstoned dead node stays dead"
        );
        // Nor does a Suspect (a downgrade attempt) move it off Dead.
        s.apply_update(
            &Update {
                state: MemberState::Suspect,
                ..alive_update("b", "b:1", 50)
            },
            2,
            &mut out,
        );
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
    }

    /// A RESTARTED node must not be killed by its previous life's `Dead` claim.
    ///
    /// This is issue #92, found by the ADR 0055 T7 operator e2e on a Kubernetes
    /// rolling restart. The pod keeps its node id across a restart, and a fresh
    /// process has no memory of its incarnation, so its `Alive` re-enters at the
    /// same low incarnation the `Dead` claim about its *previous* life carries —
    /// and `Dead` outranks `Alive` at equal incarnation. Every node that applies
    /// that claim re-gossips it, so the claim never drains: the member is revived
    /// when its tombstone is pruned and re-killed seconds later, forever, at
    /// exactly the `dead_ttl_ms` period. Downstream, each `Dead` drops the peer
    /// link and its routing state, so the durable lease group never elects and the
    /// pod stays `NotReady` for good.
    #[test]
    fn a_restarted_node_is_not_re_killed_by_its_previous_lifes_dead_claim() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();

        // b lives, then genuinely dies; the Dead claim starts circulating.
        s.apply_update(&alive_update("b", "b:1", 1), 0, &mut out);
        let stale_dead = dead_update("b", "b:1", 1);
        s.apply_update(&stale_dead, 0, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));

        // The tombstone drains and b's pod restarts: same id, new address, and a
        // fresh process — so incarnation is back to where a new node starts.
        let ttl = fast_cfg().dead_ttl_ms;
        s.tick(ttl + 1);
        out.clear();
        s.apply_update(
            &Update {
                generation: 2,
                ..alive_update("b", "b:2", 1)
            },
            ttl + 2,
            &mut out,
        );
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Alive),
            "the restarted node rejoins once the tombstone is pruned"
        );

        // The previous life's claim is still in flight somewhere in the cluster.
        out.clear();
        s.apply_update(&stale_dead, ttl + 3, &mut out);
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Alive),
            "a Dead claim about a node's PREVIOUS life must not kill the new one"
        );
        assert!(
            !out.iter().any(|a| matches!(
                a, Action::StateChange { id, state: MemberState::Dead, .. } if id.0 == "b"
            )),
            "and it must not tear down the peer link either"
        );
    }

    /// A node that comes back at a NEW routing address must reach the dial layer,
    /// even though its state never left `Alive` (issue #92). Without this event the
    /// dialer keeps redialing the address it was spawned with, forever.
    #[test]
    fn a_changed_routing_address_is_surfaced_even_when_the_state_is_unchanged() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 1), 0, &mut out);

        out.clear();
        let moved = Update {
            peer_addr: "b:2-peer".to_string(),
            ..alive_update("b", "b:1", 2)
        };
        s.apply_update(&moved, 1, &mut out);
        let surfaced = out.iter().find_map(|a| match a {
            Action::StateChange { id, peer_addr, .. } if id.0 == "b" => Some(peer_addr.clone()),
            _ => None,
        });
        assert_eq!(
            surfaced.as_deref(),
            Some("b:2-peer"),
            "an address move must raise a membership event carrying the NEW address"
        );
    }

    /// The tombstone fence is scoped to the life it was raised for: a claim from a
    /// NEWER life clears it (a restart is not a resurrection), while the dead
    /// process's own in-flight refutation — same life — is still fenced out. This is
    /// the guarantee ADR 0016 phase 1 bought, kept intact while fixing issue #92.
    #[test]
    fn a_tombstone_fences_the_dead_life_but_not_a_new_one() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 1), 0, &mut out);
        s.apply_update(&dead_update("b", "b:1", 1), 0, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));

        // Same life, higher incarnation: still fenced (the original guarantee).
        s.apply_update(&alive_update("b", "b:1", 99), 1, &mut out);
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Dead),
            "the dead process's own last refutation must not revive it"
        );

        // A NEW life clears the tombstone immediately — no waiting out dead_ttl_ms.
        s.apply_update(
            &Update {
                generation: 2,
                ..alive_update("b", "b:2", 1)
            },
            2,
            &mut out,
        );
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Alive),
            "a restarted node rejoins at once; it is not the corpse the fence is for"
        );
    }

    /// Claims about an older life are inert in every direction: they must not
    /// resurrect, re-kill, or even downgrade the current life.
    #[test]
    fn claims_about_a_previous_life_are_ignored_whatever_they_say() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(
            &Update {
                generation: 7,
                ..alive_update("b", "b:1", 1)
            },
            0,
            &mut out,
        );

        for stale in [
            dead_update("b", "b:1", 99),
            suspect_update("b", "b:1", 99),
            alive_update("b", "b:1", 99),
        ] {
            out.clear();
            s.apply_update(&stale, 1, &mut out); // generation 1 < 7
            assert_eq!(
                member_state(&s, "b"),
                Some(MemberState::Alive),
                "a claim about generation 1 says nothing about generation 7"
            );
            assert!(out.is_empty(), "and it raises no membership event");
        }
    }

    /// ADR 0016 phase 1: a tombstone is pruned after `dead_ttl_ms` (by when stale
    /// gossip has drained), after which the id may rejoin fresh.
    #[test]
    fn a_tombstone_is_pruned_after_its_ttl_and_the_id_can_rejoin() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.apply_update(&dead_update("b", "b:1", 0), 0, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));

        let ttl = fast_cfg().dead_ttl_ms;
        // Before the TTL, the tombstone still fences a revive.
        s.tick(ttl / 2);
        s.apply_update(&alive_update("b", "b:1", 99), ttl / 2, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));

        // After the TTL, the tombstone is pruned.
        s.tick(ttl + 1);
        assert_eq!(
            member_state(&s, "b"),
            None,
            "the tombstone is pruned after its TTL"
        );

        // The id can rejoin fresh (e.g. a restarted node greets us).
        s.handle(m("b", "b:1", Kind::Join, vec![]), ttl + 2);
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Alive),
            "the id rejoins as a fresh member after the tombstone expires"
        );
    }

    #[test]
    fn direct_ack_clears_the_probe() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0); // bootstrap
        let actions = s.tick(100); // start probe (Ping to b)
        let seq = ping_seq(&actions).expect("a ping was sent");
        // b acks.
        s.handle(m("b", "b:1", Kind::Ack { seq }, vec![]), 110);
        // The probe is resolved: advancing time does not suspect b.
        s.tick(300);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
    }

    /// The helper side of indirect probing: a `PingReq` is relayed as a fresh
    /// `Ping`, and the target's `Ack` is reported back to the original
    /// requester as an `IndirectAck`.
    #[test]
    fn helper_relays_pingreq_and_reports_indirect_ack() {
        let mut h = node("h", "h:1", &[]);
        let actions = h.handle(
            m(
                "a",
                "a:1",
                Kind::PingReq {
                    target: "c".into(),
                    target_addr: "c:1".into(),
                },
                vec![],
            ),
            0,
        );
        let relayed_seq = actions
            .iter()
            .find_map(|a| match a {
                Action::Send { to, msg } if to == "c:1" => match msg.kind {
                    Kind::Ping { seq } => Some(seq),
                    _ => None,
                },
                _ => None,
            })
            .expect("helper relays a Ping to the target");

        // The target acks the relayed ping: the requester gets an IndirectAck.
        let actions = h.handle(m("c", "c:1", Kind::Ack { seq: relayed_seq }, vec![]), 10);
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Send { to, msg } if to == "a:1"
                && matches!(&msg.kind, Kind::IndirectAck { target } if target == "c")
        )));
    }

    /// The requester side: an `IndirectAck` for the in-flight probe target
    /// resolves the probe, so the target is never suspected.
    #[test]
    fn indirect_ack_rescues_probed_target() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);

        s.tick(0); // bootstrap
        s.tick(100); // Ping b
        s.tick(120); // direct ack missed -> indirect phase (deadline 140)
        s.handle(
            m("c", "c:1", Kind::IndirectAck { target: "b".into() }, vec![]),
            130,
        );
        // Past every deadline: b must still be Alive.
        s.tick(141);
        s.tick(400);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
    }

    /// At equal incarnation the claim with higher precedence wins:
    /// `Dead` > `Suspect` > `Alive`, and never backwards.
    #[test]
    fn equal_incarnation_uses_state_precedence() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        let claim = |state| Update {
            id: "b".into(),
            addr: "b:1".into(),
            peer_addr: peer_addr_of("b:1"),
            incarnation: 5,
            generation: 1,
            state,
            suspecter: None,
            failure_domain: None,
        };

        s.apply_update(&claim(MemberState::Alive), 0, &mut out);
        s.apply_update(&claim(MemberState::Suspect), 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        // Alive at the same incarnation cannot clear the suspicion...
        s.apply_update(&claim(MemberState::Alive), 2, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        // ...Dead supersedes Suspect, and nothing walks Dead back.
        s.apply_update(&claim(MemberState::Dead), 3, &mut out);
        s.apply_update(&claim(MemberState::Suspect), 4, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
    }

    /// An update is piggybacked `~gossip_multiplier * log2(N)` times, then
    /// dropped from the gossip buffer — dissemination must terminate.
    #[test]
    fn gossip_updates_stop_after_transmit_limit() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);

        // One member: n = 1 + 2 = 3, floor(log2 3)+1 = 2 bits, limit = 3*2 = 6.
        let mut transmissions = 0;
        while !s.take_gossip().is_empty() {
            transmissions += 1;
            assert!(transmissions <= 6, "gossip never expired");
        }
        assert_eq!(transmissions, 6);
    }

    /// Dead members are excluded from the probe rotation.
    #[test]
    fn dead_members_are_not_probed() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(
            &Update {
                id: "b".into(),
                addr: "b:1".into(),
                peer_addr: peer_addr_of("b:1"),
                incarnation: 0,
                generation: 1,
                state: MemberState::Dead,
                suspecter: None,
                failure_domain: None,
            },
            0,
            &mut out,
        );

        s.tick(0); // bootstrap
        for now in [100, 200, 300, 400] {
            let actions = s.tick(now);
            assert!(
                !actions.iter().any(|a| matches!(
                    a,
                    Action::Send { msg, .. } if matches!(msg.kind, Kind::Ping { .. })
                )),
                "a dead member was probed at t={now}"
            );
        }
    }

    /// A member that moves (new addresses at a higher incarnation) has both its
    /// SWIM and routing addresses adopted.
    #[test]
    fn address_change_at_higher_incarnation_is_adopted() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.apply_update(&alive_update("b", "b:9", 1), 1, &mut out);

        let b = s.members().into_iter().find(|m| m.id.0 == "b").unwrap();
        assert_eq!(b.addr, "b:9");
        assert_eq!(b.peer_addr, peer_addr_of("b:9"));
        assert_eq!(b.incarnation, 1);
    }

    #[test]
    fn join_triggers_full_state_sync() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("c", "c:1", 5), 0, &mut out);
        let actions = s.handle(m("b", "b:1", Kind::Join, vec![]), 0);
        // We reply with Sync carrying our known members (a self + c).
        let sync = actions.iter().find_map(|a| match a {
            Action::Send { to, msg } if to == "b:1" && matches!(msg.kind, Kind::Sync) => Some(msg),
            _ => None,
        });
        let sync = sync.expect("a Sync reply");
        assert!(sync.gossip.iter().any(|u| u.id == "a"));
        assert!(sync.gossip.iter().any(|u| u.id == "c"));
    }

    /// A seed whose first greeting was lost (its socket bound late — the
    /// simultaneous fleet start, ADR 0044 P2) is RE-greeted on every protocol
    /// period until acquainted; once the seed is an alive member the greeting
    /// stops. Without the retry, a node whose one-shot bootstrap Join raced
    /// the seed's bind stays outside the cluster forever.
    #[test]
    fn an_unacquainted_seed_is_re_greeted_until_it_answers() {
        let period = fast_cfg().protocol_period_ms;
        let mut s = node("b", "b:1", &["a:1"]);
        let joins_to_a = |actions: &[Action]| {
            actions
                .iter()
                .filter(|a| {
                    matches!(a, Action::Send { to, msg } if to == "a:1" && matches!(msg.kind, Kind::Join))
                })
                .count()
        };
        // Bootstrap tick greets the seed once (the original behaviour)...
        assert_eq!(joins_to_a(&s.tick(0)), 1, "bootstrap greeting");
        // ...and, the greeting having gone unanswered, every protocol period
        // re-greets it.
        assert_eq!(joins_to_a(&s.tick(period)), 1, "first re-greeting");
        assert_eq!(joins_to_a(&s.tick(period * 2)), 1, "second re-greeting");
        // The seed answers (its socket finally bound): acquainted — silence.
        let mut out = Vec::new();
        s.apply_update(&alive_update("a", "a:1", 1), period * 2, &mut out);
        assert_eq!(
            joins_to_a(&s.tick(period * 3)),
            0,
            "an acquainted seed is not greeted again"
        );
    }

    // --- ADR 0016 phase 2 §2: Lifeguard local-health awareness -----------------

    /// A locally-degraded node (raised awareness) holds a `Suspect` peer longer before
    /// declaring it `Dead` — its `suspicion_timeout` is scaled by `(1 + awareness)` —
    /// so a healthy peer whose refutation is merely slow is not falsely evicted.
    #[test]
    fn awareness_scales_the_suspicion_timeout() {
        let base = fast_cfg().suspicion_timeout_ms; // 200

        // Healthy node (awareness 0): Dead right after the base timeout.
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&suspect_update("b", "b:1", 0), 0, &mut out);
        s.expire_suspects(base + 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));

        // Degraded node (awareness 2): timeout scaled to 3× base.
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&suspect_update("b", "b:1", 0), 0, &mut out);
        s.awareness = 2;
        // Past the base timeout but within the scaled window: still Suspect.
        s.expire_suspects(base * 2, &mut out);
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Suspect),
            "a degraded node must wait longer before declaring Dead"
        );
        // Past the scaled window (3×): Dead.
        s.expire_suspects(base * 3 + 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
    }

    /// Awareness rises when we must refute a suspicion about ourselves (an unambiguous
    /// "peers cannot reach us" signal) and decays on a clean probe round.
    #[test]
    fn awareness_rises_on_self_refutation_and_decays_on_a_clean_probe() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        assert_eq!(s.awareness, 0);

        // A suspicion about ourselves forces a refutation and raises awareness.
        let inc = current_incarnation(&s);
        s.apply_update(&suspect_update("a", "a:1", inc), 0, &mut out);
        assert_eq!(s.awareness, 1, "self-refutation raises awareness");

        // A successful probe round (direct ack) decays it back.
        s.tick(0); // bootstrap
        let actions = s.tick(100); // Ping b
        let seq = ping_seq(&actions).expect("a ping was sent");
        s.handle(m("b", "b:1", Kind::Ack { seq }, vec![]), 110);
        assert_eq!(s.awareness, 0, "a clean probe decays awareness");
    }

    /// Awareness is capped at `awareness_max`, bounding how slow a degraded node gets.
    #[test]
    fn awareness_is_capped_at_awareness_max() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        let cap = fast_cfg().awareness_max;
        // Force many self-refutations; each must raise incarnation so the next applies.
        for _ in 0..(u32::from(cap) + 5) {
            let inc = current_incarnation(&s);
            s.apply_update(&suspect_update("a", "a:1", inc), 0, &mut out);
        }
        assert_eq!(s.awareness, cap, "awareness saturates at awareness_max");
    }

    // --- ADR 0016 phase 2 §3: independent-suspicion confirmation ----------------

    /// One prober's suspicion alone holds the **full** suspicion window — a single
    /// (possibly contended) node cannot unilaterally fast-track a peer to `Dead`.
    #[test]
    fn one_probers_suspicion_alone_holds_the_full_window() {
        let cfg = fast_cfg();
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));

        // Just before the max window: still Suspect (not fast-tracked to the floor).
        s.expire_suspects(cfg.suspicion_timeout_ms - 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        // At the max window: Dead.
        s.expire_suspects(cfg.suspicion_timeout_ms + 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
    }

    /// Independent suspicions from **distinct** nodes shrink the window toward the
    /// floor, fast-tracking `Dead` once `suspicion_confirmations` is reached.
    #[test]
    fn independent_suspicions_shrink_the_window_to_the_floor() {
        let cfg = fast_cfg();
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        // Three distinct nodes independently suspect b at the same incarnation.
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        s.apply_update(&suspect_from("b", "b:1", 0, "y"), 0, &mut out);
        s.apply_update(&suspect_from("b", "b:1", 0, "z"), 0, &mut out);

        // At >= the floor it is Dead — much sooner than the single-prober full window.
        s.expire_suspects(cfg.suspicion_min_timeout_ms - 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        s.expire_suspects(cfg.suspicion_min_timeout_ms + 1, &mut out);
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Dead),
            "{} independent suspicions reach the floor",
            cfg.suspicion_confirmations
        );
    }

    /// Repeated suspicion from the **same** node counts once — confirmations require
    /// distinct suspecters, so a single node re-asserting cannot fast-track `Dead`.
    #[test]
    fn duplicate_suspicion_from_one_node_does_not_fast_track() {
        let cfg = fast_cfg();
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        for _ in 0..5 {
            s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        }
        // Still one distinct suspecter → full window holds past the floor.
        s.expire_suspects(cfg.suspicion_min_timeout_ms + 1, &mut out);
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Suspect),
            "duplicate suspicions from one node must not fast-track Dead"
        );
    }

    /// A refutation resets accumulated suspicions: after the victim refutes to a higher
    /// incarnation, a fresh single suspicion holds the full window again (the prior
    /// confirmations do not carry over).
    #[test]
    fn refutation_resets_accumulated_suspicions() {
        let cfg = fast_cfg();
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        s.apply_update(&suspect_from("b", "b:1", 0, "y"), 0, &mut out);

        // b refutes at a higher incarnation: back to Alive, suspecters cleared.
        s.apply_update(&alive_update("b", "b:1", 1), 0, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));

        // Re-suspected at the new incarnation by a single node: full window again.
        s.apply_update(&suspect_from("b", "b:1", 1, "x"), 10, &mut out);
        s.expire_suspects(10 + cfg.suspicion_min_timeout_ms + 1, &mut out);
        assert_eq!(
            member_state(&s, "b"),
            Some(MemberState::Suspect),
            "after a refutation the window resets to full, not the prior floor"
        );
    }

    // --- issues #973, #975 / ADR 0087: the last direct probe (the default) and the
    // --- window that scales with log n (off) ----------------------------------

    fn node_with(id: &str, addr: &str, cfg: Config) -> Swim {
        Swim::new(
            NodeId(id.to_string()),
            addr.to_string(),
            peer_addr_of(addr),
            None,
            1,
            cfg,
            Vec::new(),
        )
    }

    /// Every datagram in `actions`, as `(destination, message)`.
    fn sends(actions: &[Action]) -> Vec<(&str, &Message)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { to, msg } => Some((to.as_str(), msg)),
                Action::StateChange { .. } | Action::Gone { .. } => None,
            })
            .collect()
    }

    /// `confirm_before_dead`: when the window lapses the suspect is not declared dead
    /// but sent one `Ping` carrying the suspicion; it is declared dead one ack timeout
    /// later if it stays silent, and it is asked only once.
    #[test]
    fn a_suspect_is_probed_once_more_before_it_is_declared_dead() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut s = node_with("a", "a:1", cfg.clone());
        let mut out = Vec::new();
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        // Spend the claim's place in the gossip queue, so the probe below is shown to
        // carry it on its own account.
        while !s.take_gossip().is_empty() {}

        let lapsed = cfg.suspicion_timeout_ms + 1;
        out.clear();
        s.expire_suspects(lapsed, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        let sent = sends(&out);
        assert_eq!(sent.len(), 1, "exactly one last probe: {out:?}");
        let (to, msg) = sent[0];
        assert_eq!(to, "b:1");
        assert!(matches!(msg.kind, Kind::Ping { .. }));
        assert!(
            msg.gossip.iter().any(|u| u.id == "b"
                && u.state == MemberState::Suspect
                && u.incarnation == 0
                && u.suspecter.as_deref() == Some("a")),
            "the probe tells the suspect what it is suspected of: {:?}",
            msg.gossip
        );

        // Inside its ack timeout: still suspect, and not asked again.
        out.clear();
        s.expire_suspects(lapsed + cfg.ack_timeout_ms - 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        assert!(out.is_empty(), "asked once: {out:?}");

        // Silent: dead, one ack timeout after the window.
        s.expire_suspects(lapsed + cfg.ack_timeout_ms, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
    }

    /// `confirm_before_dead`: a suspect that answers the last probe with its refutation
    /// stays a member, and a later suspicion of it gets a last probe of its own.
    #[test]
    fn a_suspect_that_refutes_in_its_ack_is_not_declared_dead() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut s = node_with("a", "a:1", cfg.clone());
        let mut out = Vec::new();
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        let lapsed = cfg.suspicion_timeout_ms + 1;
        out.clear();
        s.expire_suspects(lapsed, &mut out);
        let seq = ping_seq(&out).expect("the last probe");

        // b heard the suspicion, bumped its incarnation and says so in its ack.
        s.handle(
            m(
                "b",
                "b:1",
                Kind::Ack { seq },
                vec![alive_update("b", "b:1", 1)],
            ),
            lapsed + 5,
        );
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
        // Counted as answered: the usual way a probe is, its refutation riding its ack.
        let counted = s.last_probes();
        assert_eq!((counted.sent, counted.spared, counted.silent), (1, 1, 0));
        s.expire_suspects(lapsed + 10 * cfg.ack_timeout_ms, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
        // A refutation that comes round by gossip instead ends the suspicion too, and
        // is not the suspect's answer to the probe.
        let mut relayed = node_with("a", "a:1", cfg.clone());
        relayed.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        relayed.expire_suspects(lapsed, &mut out);
        relayed.handle(
            m("c", "c:1", Kind::Sync, vec![alive_update("b", "b:1", 1)]),
            lapsed + 5,
        );
        assert_eq!(member_state(&relayed, "b"), Some(MemberState::Alive));
        let counted = relayed.last_probes();
        assert_eq!((counted.sent, counted.spared, counted.silent), (1, 0, 0));

        // Suspected again at the new incarnation: the window, then a fresh last probe.
        let again = lapsed + 100;
        s.apply_update(&suspect_from("b", "b:1", 1, "x"), again, &mut out);
        out.clear();
        s.expire_suspects(again + cfg.suspicion_timeout_ms + 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        assert!(
            ping_seq(&out).is_some(),
            "a new suspicion is confirmed anew"
        );
    }

    /// `confirm_before_dead`: an `Ack` to the last probe saves the member even when
    /// it carries no refutation. The suspect here is one incarnation AHEAD of the
    /// claim (it refuted someone else's suspicion, that refutation never reached this
    /// node, and its retransmit budget is spent), so it drops our claim as stale and
    /// its gossip queue has nothing about itself: the `Ack` is all there is. The
    /// member must not be declared `Dead` one ack timeout after it answered directly;
    /// it is asked with a `Join`, and its `Sync` clears the suspicion first-hand.
    #[test]
    fn a_suspect_ahead_of_a_stale_claim_is_saved_by_its_ack_alone() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut prober = node_with("a", "a:1", cfg.clone());
        let mut suspect = node_with("b", "b:1", cfg.clone());
        let mut out = Vec::new();

        // b refutes a claim from elsewhere: one incarnation up, and the refutation's
        // budget is then spent — its queue holds nothing about itself.
        let stale = current_incarnation(&suspect);
        suspect.apply_update(&suspect_from("b", "b:1", stale, "x"), 0, &mut out);
        assert_eq!(current_incarnation(&suspect), stale + 1);
        while !suspect.take_gossip().is_empty() {}

        // a still holds the old suspicion; its window lapses and it asks b itself.
        prober.apply_update(&suspect_from("b", "b:1", stale, "x"), 0, &mut out);
        let lapsed = cfg.suspicion_timeout_ms + 1;
        out.clear();
        prober.expire_suspects(lapsed, &mut out);
        let (_, ping) = sends(&out)[0];
        let ping = ping.clone();

        // b answers. Our claim is stale to it, so the ack says nothing about b.
        let replies = suspect.handle(ping, lapsed + 1);
        let (_, ack) = *sends(&replies)
            .iter()
            .find(|(to, msg)| *to == "a:1" && matches!(msg.kind, Kind::Ack { .. }))
            .expect("b acks the probe");
        assert!(
            !ack.gossip.iter().any(|u| u.id == "b"),
            "the case under test: no refutation rides the ack: {:?}",
            ack.gossip
        );

        // The ack alone keeps b: not dead when the probe's deadline passes, nor at
        // any time inside the restarted window; and b is asked for its own claim.
        let asked = prober.handle(ack.clone(), lapsed + 2);
        let (_, join) = *sends(&asked)
            .iter()
            .find(|(to, msg)| *to == "b:1" && matches!(msg.kind, Kind::Join))
            .expect("b is asked to state its incarnation");
        out.clear();
        prober.expire_suspects(lapsed + 2 + cfg.ack_timeout_ms + 1, &mut out);
        prober.expire_suspects(lapsed + cfg.suspicion_timeout_ms, &mut out);
        assert_eq!(member_state(&prober, "b"), Some(MemberState::Suspect));
        assert!(
            sends(&out).is_empty(),
            "no second probe inside the new window"
        );

        // b's Sync leads with its own claim, first-hand: the suspicion is over.
        let synced = suspect.handle(join.clone(), lapsed + 3);
        let (_, sync) = *sends(&synced)
            .iter()
            .find(|(to, msg)| *to == "a:1" && matches!(msg.kind, Kind::Sync))
            .expect("b answers the Join");
        prober.handle(sync.clone(), lapsed + 4);
        assert_eq!(member_state(&prober, "b"), Some(MemberState::Alive));
    }

    /// `confirm_before_dead`: only the suspect's own `Ack` to that very probe counts.
    /// An `Ack` with the probe's seq from another node, or the suspect's `Ack` to some
    /// other seq, leaves the probe outstanding and the member is declared `Dead` when
    /// it lapses.
    #[test]
    fn only_the_suspects_own_ack_to_the_last_probe_keeps_it() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut s = node_with("a", "a:1", cfg.clone());
        let mut out = Vec::new();
        s.apply_update(&alive_update("c", "c:1", 0), 0, &mut out);
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        let lapsed = cfg.suspicion_timeout_ms + 1;
        out.clear();
        s.expire_suspects(lapsed, &mut out);
        let seq = ping_seq(&out).expect("the last probe");

        s.handle(m("c", "c:1", Kind::Ack { seq }, vec![]), lapsed + 1);
        s.handle(
            m("b", "b:1", Kind::Ack { seq: seq + 100 }, vec![]),
            lapsed + 2,
        );
        s.expire_suspects(lapsed + cfg.ack_timeout_ms, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
    }

    /// `confirm_before_dead`: a node told it is suspected puts its refutation at the
    /// head of its gossip, so the reply to that very datagram carries it however much
    /// else is queued. (Off, the refutation queues behind everything else, as before.)
    #[test]
    fn a_node_told_it_is_suspected_answers_with_its_refutation_first() {
        for (on, refutation_leads) in [(true, true), (false, false)] {
            let cfg = Config {
                confirm_before_dead: on,
                ..fast_cfg()
            };
            let fanout = cfg.gossip_fanout;
            let mut s = node_with("a", "a:1", cfg);
            let mut out = Vec::new();
            // More pending updates than one datagram carries.
            for i in 0..(fanout + 4) {
                s.apply_update(
                    &alive_update(&format!("n{i}"), &format!("n{i}:1"), 0),
                    0,
                    &mut out,
                );
            }
            let inc = current_incarnation(&s);
            let out = s.handle(
                m(
                    "x",
                    "x:1",
                    Kind::Ping { seq: 7 },
                    vec![suspect_from("a", "a:1", inc, "x")],
                ),
                1,
            );
            let sent = sends(&out);
            let (_, ack) = sent
                .iter()
                .find(|(to, msg)| *to == "x:1" && matches!(msg.kind, Kind::Ack { seq: 7 }))
                .expect("the ping is acked");
            let carries_refutation = ack
                .gossip
                .iter()
                .any(|u| u.id == "a" && u.state == MemberState::Alive && u.incarnation == inc + 1);
            assert_eq!(
                carries_refutation, refutation_leads,
                "confirm_before_dead={on}: {:?}",
                ack.gossip
            );
            if on {
                assert_eq!(ack.gossip[0].id, "a", "the refutation is first");
            }
        }
    }

    /// `confirm_before_dead`: a suspect that answers the last probe but whose own claim
    /// never arrives — its `Ack` carries no refutation and the `Sync` it is asked for is
    /// lost, window after window — stays `Suspect`, is never declared `Dead` while it
    /// answers, and costs exactly one `Ping` and one `Join` per window: the restarted
    /// window is a full one, nothing is sent inside it, and its lapse sends one probe
    /// more. The first window it does not answer ends in `Dead`.
    #[test]
    fn a_suspect_that_answers_without_ever_refuting_is_asked_once_per_window() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut s = node_with("a", "a:1", cfg.clone());
        let mut out = Vec::new();
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);

        // `now` is the start of the current window.
        let mut now = 0;
        for window in 1..=3 {
            // Nothing inside the window; one probe when it lapses; none while that
            // probe is outstanding.
            out.clear();
            s.expire_suspects(now + cfg.suspicion_timeout_ms - 1, &mut out);
            assert!(sends(&out).is_empty(), "window {window}: {out:?}");
            let lapsed = now + cfg.suspicion_timeout_ms;
            s.expire_suspects(lapsed, &mut out);
            s.expire_suspects(lapsed + 1, &mut out);
            let sent = sends(&out);
            assert_eq!(sent.len(), 1, "window {window}: one probe: {out:?}");
            assert_eq!(sent[0].0, "b:1");
            let seq = ping_seq(&out).expect("the last probe");

            // b answers, saying nothing about itself: kept, and asked once.
            let asked = s.handle(m("b", "b:1", Kind::Ack { seq }, vec![]), lapsed + 2);
            let sent = sends(&asked);
            assert_eq!(sent.len(), 1, "window {window}: one request: {asked:?}");
            assert!(sent[0].0 == "b:1" && matches!(sent[0].1.kind, Kind::Join));
            assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
            // The probe's own deadline passing changes nothing: it was answered.
            out.clear();
            s.expire_suspects(lapsed + 2 + cfg.ack_timeout_ms, &mut out);
            assert!(sends(&out).is_empty(), "window {window}: {out:?}");
            assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
            now = lapsed + 2;
        }

        // A window it does not answer: one probe, one ack timeout, dead.
        out.clear();
        let lapsed = now + cfg.suspicion_timeout_ms;
        s.expire_suspects(lapsed, &mut out);
        assert_eq!(sends(&out).len(), 1);
        s.expire_suspects(lapsed + cfg.ack_timeout_ms, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
    }

    /// The last direct probe is the default (ADR 0016's amendment of 2026-10-13): a
    /// node built with `Config::default()`, as the broker builds it, does not declare
    /// a suspect `Dead` when its window lapses but asks it, and declares it one ack
    /// timeout later. With the switch off — v1.1.0's rule — the window is all there is.
    #[test]
    fn by_default_a_suspect_is_asked_before_it_is_declared_dead() {
        let default = Config::default();
        assert!(default.confirm_before_dead);
        for on in [true, false] {
            let cfg = Config {
                confirm_before_dead: on,
                ..Config::default()
            };
            let mut s = node_with("a", "a:1", cfg.clone());
            let mut out = Vec::new();
            s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
            let lapsed = cfg.suspicion_timeout_ms;
            out.clear();
            s.expire_suspects(lapsed, &mut out);
            if on {
                assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
                assert!(ping_seq(&out).is_some(), "the suspect is asked");
                s.expire_suspects(lapsed + cfg.ack_timeout_ms, &mut out);
            } else {
                assert!(ping_seq(&out).is_none(), "v1.1.0 asks nobody");
            }
            assert_eq!(member_state(&s, "b"), Some(MemberState::Dead), "on={on}");
        }
    }

    /// What the last probe can cost in datagrams, at worst: one `Ping`, of the usual
    /// bounded size, per suspect per lapsed window — however many times the clock is
    /// advanced inside the probe's ack timeout, and however the suspicions arrived.
    /// Once the members are `Dead` nothing more is sent about them. So a node made
    /// to suspect every member at once (a partition; a signed member lying) sends at
    /// most one probe to each, where without the rule it sent none and declared them
    /// all dead.
    #[test]
    fn the_last_probe_is_one_datagram_per_suspect_per_window() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut s = node_with("a", "a:1", cfg.clone());
        let mut out = Vec::new();
        let suspects = 12;
        for i in 0..suspects {
            let (id, addr) = (format!("n{i}"), format!("n{i}:1"));
            // Three suspecters each: the shortest window the detector has.
            for by in ["x", "y", "z"] {
                s.apply_update(&suspect_from(&id, &addr, 0, by), 0, &mut out);
            }
        }
        out.clear();
        let queued = s.gossip.clone();
        let lapsed = cfg.suspicion_min_timeout_ms;
        for dt in 0..cfg.ack_timeout_ms {
            s.expire_suspects(lapsed + dt, &mut out);
        }
        let sent = sends(&out);
        assert_eq!(sent.len(), suspects, "one probe per suspect: {out:?}");
        let mut probed: Vec<&str> = sent.iter().map(|(to, _)| *to).collect();
        probed.sort_unstable();
        probed.dedup();
        assert_eq!(probed.len(), suspects, "each suspect once");
        for (to, msg) in &sent {
            assert!(matches!(msg.kind, Kind::Ping { .. }));
            // The suspicion of the member it is sent to, and nothing else.
            assert_eq!(msg.gossip.len(), 1, "{:?}", msg.gossip);
            assert_eq!(format!("{}:1", msg.gossip[0].id), *to);
            assert_eq!(msg.gossip[0].state, MemberState::Suspect);
        }
        // Twelve windows lapsed on one tick, and the gossip queue is what it was: no
        // update lost a retransmission to a probe (the twelve suspicions themselves
        // are queued — more than one datagram's worth — and would have been drawn on).
        assert!(queued.len() > cfg.gossip_fanout);
        assert_eq!(s.gossip, queued, "the probes drew on the retransmit budget");
        assert_eq!(s.last_probes().sent, suspects as u64);

        // Silent, all of them: dead one ack timeout on, and nothing more is sent.
        out.clear();
        s.expire_suspects(lapsed + cfg.ack_timeout_ms, &mut out);
        s.expire_suspects(lapsed + 10 * cfg.ack_timeout_ms, &mut out);
        assert!(sends(&out).is_empty(), "{out:?}");
        for i in 0..suspects {
            assert_eq!(member_state(&s, &format!("n{i}")), Some(MemberState::Dead));
        }
        let counted = s.last_probes();
        assert_eq!((counted.spared, counted.silent), (0, suspects as u64));
    }

    /// A message from `from` in process life `generation`.
    fn m_of_life(from: &str, generation: u64, kind: Kind, gossip: Vec<Update>) -> Message {
        Message {
            from_generation: generation,
            ..m(from, &format!("{from}:1"), kind, gossip)
        }
    }

    /// The last probe is answered only by the life that is suspected. `b` is held
    /// `Suspect` in life 5 when the same id comes up in place with a LOWER generation
    /// (its host's clock stepped back across the restart; or a second process was
    /// given `b`'s id). That process acks the probe — and nothing it says about itself
    /// can ever clear the suspicion, because every claim it makes is about "an earlier
    /// life" and is discarded. Spared on that `Ack`, `b` would stay `Suspect` for good.
    /// So the `Ack` does not count: life 5 is declared `Dead`, its tombstone is pruned,
    /// and the process that is actually there is learned as what it is.
    #[test]
    fn an_ack_from_an_earlier_life_does_not_answer_the_last_probe() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let in_life = |generation, update: Update| Update {
            generation,
            ..update
        };
        let suspected = || {
            let mut s = node_with("a", "a:1", cfg.clone());
            let mut out = Vec::new();
            s.apply_update(&in_life(5, alive_update("b", "b:1", 0)), 0, &mut out);
            s.apply_update(&in_life(5, suspect_from("b", "b:1", 0, "x")), 0, &mut out);
            out.clear();
            s.expire_suspects(cfg.suspicion_timeout_ms, &mut out);
            let seq = ping_seq(&out).expect("the last probe");
            (s, seq)
        };
        let lapsed = cfg.suspicion_timeout_ms;

        // The life that is suspected answers: kept, and asked for its claim.
        let (mut s, seq) = suspected();
        let asked = s.handle(m_of_life("b", 5, Kind::Ack { seq }, vec![]), lapsed + 1);
        assert_eq!(sends(&asked).len(), 1, "{asked:?}");
        s.expire_suspects(lapsed + cfg.ack_timeout_ms, &mut Vec::new());
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        assert_eq!(s.last_probes().spared, 1);

        // An earlier life answers, even with a claim about itself: not the suspect's
        // answer. Nothing is sent back, and the probe runs out.
        let (mut s, seq) = suspected();
        let stale_self = in_life(3, alive_update("b", "b:1", 9));
        let asked = s.handle(
            m_of_life("b", 3, Kind::Ack { seq }, vec![stale_self.clone()]),
            lapsed + 1,
        );
        assert!(sends(&asked).is_empty(), "{asked:?}");
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        let mut out = Vec::new();
        s.expire_suspects(lapsed + cfg.ack_timeout_ms, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
        let probes = s.last_probes();
        assert_eq!((probes.sent, probes.spared, probes.silent), (1, 0, 1));

        // While life 5's tombstone stands the lower generation is still nobody.
        s.handle(
            m_of_life("b", 3, Kind::Sync, vec![stale_self.clone()]),
            lapsed + 100,
        );
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
        // Pruned (and greeted, issue #971): its `Sync` is first contact with a member
        // this node holds nothing about, and it is admitted at the life it runs.
        let pruned_at = lapsed + cfg.ack_timeout_ms + cfg.dead_ttl_ms;
        let greeted = s.tick(pruned_at);
        assert!(
            sends(&greeted)
                .iter()
                .any(|(to, msg)| *to == "b:1" && matches!(msg.kind, Kind::Join)),
            "{greeted:?}"
        );
        assert_eq!(member_state(&s, "b"), None);
        s.handle(
            m_of_life("b", 3, Kind::Sync, vec![stale_self]),
            pruned_at + 5,
        );
        let b = s
            .members()
            .into_iter()
            .find(|m| m.id.0 == "b")
            .expect("re-learned");
        assert_eq!((b.state, b.generation), (MemberState::Alive, 3));
    }

    /// A `Suspect` claim at a higher incarnation, arriving while the last probe is out,
    /// leaves that probe standing: the member is still suspected on the same window,
    /// so no second probe is sent, and its `Ack` to the first still keeps it. (The
    /// claim used to drop the probe: the `Ack` in flight was then ignored and a second
    /// `Ping` went out on the next tick — two datagrams in one window.)
    #[test]
    fn a_suspicion_at_a_higher_incarnation_leaves_the_last_probe_standing() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut s = node_with("a", "a:1", cfg.clone());
        let mut out = Vec::new();
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        let lapsed = cfg.suspicion_timeout_ms;
        out.clear();
        s.expire_suspects(lapsed, &mut out);
        let seq = ping_seq(&out).expect("the last probe");

        // b had refuted to incarnation 1 and x suspects it again: relayed here.
        out.clear();
        s.apply_update(&suspect_from("b", "b:1", 1, "x"), lapsed + 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        out.clear();
        for dt in 2..cfg.ack_timeout_ms {
            s.expire_suspects(lapsed + dt, &mut out);
        }
        assert!(sends(&out).is_empty(), "a second probe: {out:?}");
        assert_eq!(s.last_probes().sent, 1);

        // Its ack to the probe that is out keeps it, as it would have.
        let asked = s.handle(
            m("b", "b:1", Kind::Ack { seq }, vec![]),
            lapsed + cfg.ack_timeout_ms - 1,
        );
        assert!(sends(&asked)
            .iter()
            .any(|(to, msg)| *to == "b:1" && matches!(msg.kind, Kind::Join)));
        s.expire_suspects(lapsed + 2 * cfg.ack_timeout_ms, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        assert_eq!(s.last_probes().spared, 1);

        // And unanswered it is silent, as any last probe: the same start, no ack.
        let mut s = node_with("a", "a:1", cfg.clone());
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        s.expire_suspects(lapsed, &mut out);
        s.apply_update(&suspect_from("b", "b:1", 1, "x"), lapsed + 1, &mut out);
        s.expire_suspects(lapsed + cfg.ack_timeout_ms, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
    }

    /// A last probe whose window grows under it is not the last word. This node's
    /// awareness rises while the probe is out (it had to refute a suspicion of
    /// itself), which doubles the window: the verdict is no longer due. The probe
    /// runs out unanswered and is dropped; when the longer window lapses the member
    /// is asked AFRESH and given a full ack timeout. (It used to be declared `Dead`
    /// at that moment on the silence of the probe sent a window earlier.)
    #[test]
    fn a_probe_overtaken_by_a_longer_window_is_sent_afresh_when_it_lapses() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut s = node_with("a", "a:1", cfg.clone());
        let mut out = Vec::new();
        s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        let window = cfg.suspicion_timeout_ms;
        out.clear();
        s.expire_suspects(window, &mut out);
        let first = ping_seq(&out).expect("the last probe");

        // Someone suspects US: we refute, and our awareness goes to 1.
        let inc = current_incarnation(&s);
        s.apply_update(&suspect_from("a", "a:1", inc, "x"), window + 1, &mut out);
        assert_eq!(s.awareness(), 1);

        // The first probe's deadline passes inside the doubled window: no verdict,
        // nothing sent, all the way to the new lapse.
        out.clear();
        s.expire_suspects(window + cfg.ack_timeout_ms, &mut out);
        s.expire_suspects(2 * window - 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        assert!(sends(&out).is_empty(), "{out:?}");

        // The doubled window lapses: a fresh probe, not a verdict.
        s.expire_suspects(2 * window, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        let second = ping_seq(&out).expect("a fresh probe");
        assert_ne!(second, first);
        // It has the health-scaled ack timeout to answer in, and is silent.
        s.expire_suspects(2 * window + 2 * cfg.ack_timeout_ms - 1, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect));
        s.expire_suspects(2 * window + 2 * cfg.ack_timeout_ms, &mut out);
        assert_eq!(member_state(&s, "b"), Some(MemberState::Dead));
        let probes = s.last_probes();
        assert_eq!((probes.sent, probes.spared, probes.silent), (2, 0, 1));
    }

    /// A suspect spared on its `Ack` to a probe that carried a STALE claim is cleared at
    /// once, not a window later. The probe went out about incarnation 0; while it was
    /// out, a suspicion at incarnation 1 arrived (the member had refuted, and was
    /// suspected again). The member, at 1, ignores the probe's claim and acks with
    /// nothing about itself. The `Join` it is then sent carries the suspicion as held
    /// NOW — at 1 — so it refutes that, and its `Sync` says `Alive` at 2. (Asked
    /// without the claim it answered `Alive` at 1, which does not supersede `Suspect`
    /// at 1, and stayed suspect for another full window.)
    #[test]
    fn a_suspect_spared_on_a_stale_claim_is_asked_with_the_claim_held_now() {
        let cfg = Config {
            confirm_before_dead: true,
            ..fast_cfg()
        };
        let mut prober = node_with("a", "a:1", cfg.clone());
        let mut suspect = node_with("b", "b:1", cfg.clone());
        let mut out = Vec::new();
        // b is at incarnation 1 and has nothing left to gossip about itself.
        suspect.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        while !suspect.take_gossip().is_empty() {}
        suspect.incarnation = 1;

        prober.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
        let lapsed = cfg.suspicion_timeout_ms;
        out.clear();
        prober.expire_suspects(lapsed, &mut out);
        let ping = sends(&out)[0].1.clone();
        prober.apply_update(&suspect_from("b", "b:1", 1, "x"), lapsed + 1, &mut out);

        let replies = suspect.handle(ping, lapsed + 2);
        let ack = sends(&replies)[0].1.clone();
        assert!(!ack.gossip.iter().any(|u| u.id == "b"), "{:?}", ack.gossip);
        let asked = prober.handle(ack, lapsed + 3);
        let join = sends(&asked)[0].1.clone();
        assert!(matches!(join.kind, Kind::Join));
        assert!(
            join.gossip.iter().any(|u| u.id == "b"
                && u.state == MemberState::Suspect
                && u.incarnation == 1
                && u.suspecter.as_deref() == Some("a")),
            "the request carries the suspicion as held now: {:?}",
            join.gossip
        );
        assert_eq!(member_state(&prober, "b"), Some(MemberState::Suspect));

        let synced = suspect.handle(join, lapsed + 4);
        let sync = sends(&synced)[0].1.clone();
        assert!(matches!(sync.kind, Kind::Sync));
        prober.handle(sync, lapsed + 5);
        assert_eq!(member_state(&prober, "b"), Some(MemberState::Alive));
        assert_eq!(current_incarnation(&suspect), 2);
    }

    /// The probe period is kept from when a round was DUE, not from when a late node
    /// got round to it: late by less than a period, the next round is still due one
    /// period after the last was. (It used to be due a period after the late start, so
    /// a node that woke late stretched every period by its lateness, and a member
    /// whose turn was several rounds off waited that many latenesses.) A node a whole
    /// period or more behind starts again from now: rounds are not made up.
    #[test]
    fn the_probe_period_runs_from_when_a_round_was_due_not_from_a_late_tick() {
        let period = fast_cfg().protocol_period_ms;
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0); // bootstrap: the first round is due one period on
        let round = |s: &mut Swim, at: u64| -> bool {
            let Some(seq) = ping_seq(&s.tick(at)) else {
                return false;
            };
            s.handle(m("b", "b:1", Kind::Ack { seq }, vec![]), at + 1);
            true
        };
        assert!(!round(&mut s, period - 1), "not due yet");
        // 40 % of a period late: the round runs, and the next is due on schedule.
        let late = period * 2 / 5;
        assert!(round(&mut s, period + late));
        assert!(!round(&mut s, 2 * period - 1), "the next is not due early");
        assert!(
            round(&mut s, 2 * period),
            "the next round is due a period after the last was DUE"
        );
        // Several periods behind: one round now, and the next a full period on.
        assert!(round(&mut s, 7 * period + 3));
        assert!(!round(&mut s, 8 * period + 2), "rounds are not made up");
        assert!(round(&mut s, 8 * period + 3));

        // v1.1.0's rule: a period after the late START.
        let mut old = node_with(
            "a",
            "a:1",
            Config {
                period_from_schedule: false,
                ..fast_cfg()
            },
        );
        old.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        old.tick(0);
        assert!(round(&mut old, period + late));
        assert!(!round(&mut old, 2 * period));
        assert!(round(&mut old, 2 * period + late));
    }

    /// `suspicion_scales_with_log_n`: the window is multiplied by `max(1, log10 n)` —
    /// unchanged up to ten nodes, about 1.48 times as long at thirty — and only when
    /// the knob is on.
    #[test]
    fn the_suspicion_window_grows_with_log_n_only_when_asked() {
        let window = fast_cfg().suspicion_timeout_ms;
        // (scaled, nodes in the cluster, still suspect at, dead by)
        for (scaled, nodes, alive_at, dead_at) in [
            (false, 30, window - 1, window + 1),
            (true, 10, window - 1, window + 1),
            (true, 30, window * 147 / 100, window * 148 / 100 + 1),
            (true, 100, window * 2 - 1, window * 2 + 1),
        ] {
            let cfg = Config {
                suspicion_scales_with_log_n: scaled,
                ..fast_cfg()
            };
            let mut s = node_with("a", "a:1", cfg);
            let mut out = Vec::new();
            // `nodes` counts this node; `b` is one of the others.
            for i in 0..(nodes - 2) {
                s.apply_update(
                    &alive_update(&format!("n{i}"), &format!("n{i}:1"), 0),
                    0,
                    &mut out,
                );
            }
            s.apply_update(&suspect_from("b", "b:1", 0, "x"), 0, &mut out);
            let ctx = format!("scaled={scaled} nodes={nodes}");
            s.expire_suspects(alive_at, &mut out);
            assert_eq!(member_state(&s, "b"), Some(MemberState::Suspect), "{ctx}");
            s.expire_suspects(dead_at, &mut out);
            assert_eq!(member_state(&s, "b"), Some(MemberState::Dead), "{ctx}");
        }
    }

    // --- helpers ---------------------------------------------------------------

    fn member_state(s: &Swim, id: &str) -> Option<MemberState> {
        s.members()
            .into_iter()
            .find(|m| m.id.0 == id)
            .map(|m| m.state)
    }

    fn current_incarnation(s: &Swim) -> u64 {
        s.incarnation
    }

    fn ping_seq(actions: &[Action]) -> Option<u64> {
        actions.iter().find_map(|a| match a {
            Action::Send { msg, .. } => match msg.kind {
                Kind::Ping { seq } => Some(seq),
                _ => None,
            },
            Action::StateChange { .. } | Action::Gone { .. } => None,
        })
    }

    /// The failure-domain label held for member `id`, if any (ADR 0016 T5).
    fn member_domain(s: &Swim, id: &str) -> Option<String> {
        s.members()
            .into_iter()
            .find(|m| m.id.0 == id)
            .and_then(|m| m.failure_domain)
    }

    #[test]
    fn a_receiver_learns_a_peers_gossiped_failure_domain() {
        let mut b = node("b", "b:1", &[]);
        let update = Update {
            failure_domain: Some("rack-a".into()),
            ..alive_update("a", "a:1", 1)
        };
        let actions = b.handle(m("a", "a:1", Kind::Sync, vec![update]), 0);
        assert_eq!(member_domain(&b, "a").as_deref(), Some("rack-a"));
        // The label reaches the routing/placement layer via a StateChange (placement
        // applies last-wins, so the *final* event for "a" must carry it).
        let learned = actions
            .iter()
            .filter_map(|act| match act {
                Action::StateChange { id, domain, .. } if id.0 == "a" => Some(domain.clone()),
                _ => None,
            })
            .next_back();
        assert_eq!(learned, Some(Some("rack-a".into())));
    }

    #[test]
    fn an_unlabelled_relay_does_not_erase_a_known_domain() {
        let mut b = node("b", "b:1", &[]);
        b.handle(
            m(
                "a",
                "a:1",
                Kind::Sync,
                vec![Update {
                    failure_domain: Some("rack-a".into()),
                    ..alive_update("a", "a:1", 1)
                }],
            ),
            0,
        );
        // A later, superseding claim about "a" that never learned the label must not blank it.
        b.handle(
            m("c", "c:1", Kind::Sync, vec![alive_update("a", "a:1", 5)]),
            1,
        );
        assert_eq!(member_domain(&b, "a").as_deref(), Some("rack-a"));
    }

    #[test]
    fn first_contact_teaches_the_senders_domain() {
        let mut b = node("b", "b:1", &[]);
        // A bare Ping from a labelled sender with no piggybacked gossip: the sender's
        // label rides in `from_domain`, learned on first contact (ADR 0016 T5).
        let ping = Message {
            from: "a".into(),
            from_addr: "a:1".into(),
            from_peer_addr: peer_addr_of("a:1"),
            from_domain: Some("rack-a".into()),
            from_generation: 1,
            cluster_id: None,
            kind: Kind::Ping { seq: 7 },
            gossip: vec![],
        };
        b.handle(ping, 0);
        assert_eq!(member_domain(&b, "a").as_deref(), Some("rack-a"));
    }

    #[test]
    fn a_node_advertises_its_own_domain_on_outgoing_gossip() {
        let mut a = node_in_domain("a", "a:1", &[], "rack-a");
        // A Join from "b" makes "a" answer with a full-state Sync.
        let actions = a.handle(m("b", "b:1", Kind::Join, vec![]), 0);
        let sync = actions.iter().find_map(|act| match act {
            Action::Send { msg, .. } if matches!(msg.kind, Kind::Sync) => Some(msg),
            _ => None,
        });
        let sync = sync.expect("a Join is answered with a Sync");
        // The message header carries the sender's own label...
        assert_eq!(sync.from_domain.as_deref(), Some("rack-a"));
        // ...and its self-update in the piggybacked gossip does too.
        let self_update = sync.gossip.iter().find(|u| u.id == "a").unwrap();
        assert_eq!(self_update.failure_domain.as_deref(), Some("rack-a"));
    }

    fn swim_addr_of(s: &Swim, id: &str) -> String {
        s.members()
            .into_iter()
            .find(|m| m.id.0 == id)
            .map_or_else(|| panic!("{id} not in members"), |m| m.addr)
    }

    #[test]
    fn an_unroutable_self_claim_cannot_overwrite_a_source_learned_address() {
        // Issue #396: a node bound to 0.0.0.0 stamps that bind into every
        // self-update it emits — including refutations, which supersede by
        // incarnation. The source-learned address must survive them.
        let mut s = node("a", "a:1", &[]);
        s.handle(m("b", "b:1", Kind::Join, vec![]), 0);
        assert_eq!(swim_addr_of(&s, "b"), "b:1");
        // b's own (first-hand) higher-incarnation self-claim carries its bind.
        s.handle(
            m(
                "b",
                "b:1",
                Kind::Sync,
                vec![alive_update("b", "0.0.0.0:7946", 5)],
            ),
            1,
        );
        let b = s.members().into_iter().find(|m| m.id.0 == "b").unwrap();
        assert_eq!(b.addr, "b:1", "the unroutable claim must not stick");
        assert_eq!(b.incarnation, 5, "the claim's incarnation still applies");
    }

    #[test]
    fn a_relay_poisoned_record_heals_on_first_direct_contact() {
        // Learned only via relay, c is recorded at the poison address...
        let mut s = node("a", "a:1", &[]);
        s.handle(
            m(
                "x",
                "x:1",
                Kind::Sync,
                vec![alive_update("c", "0.0.0.0:7946", 2)],
            ),
            0,
        );
        assert_eq!(swim_addr_of(&s, "c"), "0.0.0.0:7946");
        // ...until c speaks to us directly: the datagram's UDP source (stamped
        // into `from_addr` by the driver) repairs the record — on ANY datagram,
        // not just first contact.
        s.handle(m("c", "c:9", Kind::Ping { seq: 7 }, vec![]), 1);
        assert_eq!(swim_addr_of(&s, "c"), "c:9");
    }

    #[test]
    fn gossip_about_an_unroutable_subject_is_relayed_at_our_learned_address() {
        // a knows b's real address first-hand; a relayed claim about b carrying
        // b's 0.0.0.0 bind must be re-broadcast at the address a actually holds
        // — every hop that knows better is a cure, not a carrier (issue #396).
        let mut s = node("a", "a:1", &[]);
        s.handle(m("b", "b:1", Kind::Join, vec![]), 0);
        s.handle(
            m(
                "x",
                "x:1",
                Kind::Sync,
                vec![alive_update("b", "0.0.0.0:7946", 5)],
            ),
            1,
        );
        // Any outbound datagram piggybacks the queued gossip; b's entry must
        // carry the learned address.
        let sync = s
            .handle(m("y", "y:1", Kind::Join, vec![]), 2)
            .into_iter()
            .find_map(|a| match a {
                Action::Send { msg, .. } if msg.kind == Kind::Sync => Some(msg),
                _ => None,
            })
            .expect("a Sync reply");
        let b = sync.gossip.iter().find(|u| u.id == "b").unwrap();
        assert_eq!(b.addr, "b:1");
    }

    #[test]
    fn a_ping_req_naming_an_unroutable_target_is_relayed_to_our_learned_address() {
        // The helper holds the target's real (source-learned) address; the
        // requester only holds the poison. The relayed probe must dial the real
        // one — its indirect ack is how the requester's record gets repaired.
        let mut s = node("a", "a:1", &[]);
        s.handle(m("t", "t:5", Kind::Join, vec![]), 0);
        let out = s.handle(
            m(
                "r",
                "r:1",
                Kind::PingReq {
                    target: "t".into(),
                    target_addr: "0.0.0.0:7946".into(),
                },
                vec![],
            ),
            1,
        );
        let ping_to = out
            .iter()
            .find_map(|a| match a {
                Action::Send { to, msg } if matches!(msg.kind, Kind::Ping { .. }) => {
                    Some(to.clone())
                }
                _ => None,
            })
            .expect("a relayed Ping");
        assert_eq!(ping_to, "t:5");
    }

    #[test]
    fn a_reflected_datagram_from_ourselves_is_ignored() {
        // Probing a member recorded at an unroutable address lands on our OWN
        // socket (dialing 0.0.0.0 loops back). Without the reflection guard we
        // would ack our own probe and confirm the unreachable member alive
        // forever (issue #396 — how the ghosted node's view stayed green).
        let mut s = node("a", "a:1", &[]);
        s.handle(
            m(
                "x",
                "x:1",
                Kind::Sync,
                vec![alive_update("z", "0.0.0.0:7946", 1)],
            ),
            0,
        );
        // Drive a probe round and capture the Ping's seq wherever it went.
        let mut now = 100;
        let mut seq = None;
        for _ in 0..10 {
            for a in s.tick(now) {
                if let Action::Send { msg, .. } = a {
                    if let Kind::Ping { seq: q } = msg.kind {
                        if s.members().iter().any(|m| m.id.0 == "z") {
                            seq = Some(q);
                        }
                    }
                }
            }
            if seq.is_some() {
                break;
            }
            now += 100;
        }
        let seq = seq.expect("a probe was issued");
        // The reflected datagrams: our own Ping arrives back at us, and the Ack
        // we would send to "ourselves at 0.0.0.0" arrives too. Both must be dropped
        // — no reply, no self-ack.
        let refl = s.handle(m("a", "a:1", Kind::Ping { seq }, vec![]), now);
        assert!(refl.is_empty(), "a reflected Ping must not be answered");
        s.handle(m("a", "a:1", Kind::Ack { seq }, vec![]), now);
        // With no self-ack the probe round runs to its honest conclusion:
        // z (unreachable) becomes Suspect once both deadlines lapse.
        for _ in 0..20 {
            now += 100;
            s.tick(now);
        }
        let z = s.members().into_iter().find(|m| m.id.0 == "z").unwrap();
        assert_ne!(
            z.state,
            MemberState::Alive,
            "an unreachable member must not be kept alive by our own reflection"
        );
    }

    #[test]
    fn a_fully_removed_member_is_readmitted_on_direct_contact_and_a_refutation_sticks() {
        // Issue #393's terminal ghost, replayed with routable addresses: once the
        // address poison (issue #396) is out of the picture, the existing
        // machinery — first-contact re-admission, the tombstone, and the
        // first-hand pierce (issue #383) — converges back to Alive.
        let mut s = node("a", "a:1", &[]);
        s.handle(m("b", "b:1", Kind::Join, vec![]), 0);
        let mut out = Vec::new();
        s.apply_update(&dead_update("b", "b:1", 3), 10, &mut out);
        // The tombstone expires and the member is pruned entirely.
        s.tick(10 + 2000 + 100);
        assert!(
            !s.members().iter().any(|m| m.id.0 == "b"),
            "b should be pruned after dead_ttl"
        );
        // b speaks to us directly: re-admitted on first contact.
        let now = 3000;
        s.handle(m("b", "b:1", Kind::Ping { seq: 1 }, vec![]), now);
        assert!(s.members().iter().any(|m| m.id.0 == "b"));
        // A stale Dead claim still circulating at the old incarnation re-kills
        // the freshly re-admitted (incarnation 0) record...
        let mut out = Vec::new();
        s.apply_update(&dead_update("b", "b:1", 3), now + 1, &mut out);
        // ...but b's own first-hand refutation at a higher incarnation pierces
        // the fence (issue #383) and stays, because no poisoned third party is
        // left to manufacture fresh kills.
        s.handle(
            m("b", "b:1", Kind::Sync, vec![alive_update("b", "b:1", 4)]),
            now + 2,
        );
        let b = s.members().into_iter().find(|m| m.id.0 == "b").unwrap();
        assert_eq!(b.state, MemberState::Alive);
        assert_eq!(b.addr, "b:1");
    }

    /// The `Join` greetings in `actions`, as the addresses they go to.
    fn joins_to(actions: &[Action]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { to, msg } if matches!(msg.kind, Kind::Join) => Some(to.clone()),
                _ => None,
            })
            .collect()
    }

    /// Node `a`, bootstrapped, holding `b` alive, asking one helper per failed probe.
    fn node_holding_b(cfg: &Config) -> Swim {
        let mut s = Swim::new(
            NodeId("a".to_string()),
            "a:1".to_string(),
            peer_addr_of("a:1"),
            None,
            1,
            Config {
                indirect_probes: 1,
                ..cfg.clone()
            },
            Vec::new(),
        );
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0); // bootstrap
        s
    }

    /// Issue #971: a member that drops our datagrams as `cert-miss` never acks us
    /// directly, but its helpers reach it every round. That rescue used to reset the
    /// deaf-peer count, so the certificate-carrying re-greet of #383 never fired and
    /// the pair stayed one-sided for minutes. A probe that only helpers rescued now
    /// counts; only the member's own ack clears it.
    #[test]
    fn a_probe_rescued_only_by_helpers_still_counts_toward_the_regreet() {
        let mut s = node_holding_b(&fast_cfg());
        // c is the helper. It is probed too (and answers directly), in whatever order
        // the shuffle draws, so the loop counts the probes of b.
        let mut out = Vec::new();
        s.apply_update(&alive_update("c", "c:1", 0), 0, &mut out);
        let mut t = 100;
        let mut probes_of_b = 0;
        let mut regreeted_at = None;
        // Bounded: b and c alternate at worst, so 3 unanswered probes of b plus the
        // re-greeting one take at most 8 rounds.
        for _ in 0..8 {
            let started = s.tick(t);
            let pinged_b = started.iter().any(|a| {
                matches!(a, Action::Send { to, msg } if to == "b:1" && matches!(msg.kind, Kind::Ping { .. }))
            });
            if pinged_b {
                if joins_to(&started) == vec!["b:1".to_string()] {
                    regreeted_at = Some(probes_of_b);
                    break;
                }
                probes_of_b += 1;
                s.tick(t + 21); // no direct ack: the helper is asked
                s.handle(
                    m(
                        "c",
                        "c:1",
                        Kind::IndirectAck {
                            target: "b".to_string(),
                        },
                        vec![],
                    ),
                    t + 25,
                );
                assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
            } else {
                // c's turn: it answers directly.
                let seq = ping_seq(&started).expect("a probe of c");
                s.handle(m("c", "c:1", Kind::Ack { seq }, vec![]), t + 1);
            }
            t += 100;
        }
        assert_eq!(
            regreeted_at,
            Some(3),
            "the fourth probe of a member whose last three were rescued only by helpers \
             is accompanied by a Join"
        );

        // b answers that probe itself: it hears us again, and the re-greets stop.
        let seq = s.seq;
        s.handle(m("b", "b:1", Kind::Ack { seq }, vec![]), t + 1);
        for _ in 0..4 {
            t += 100;
            let started = s.tick(t);
            assert!(
                joins_to(&started).is_empty(),
                "an answered member is not re-greeted: {started:?}"
            );
            if let Some(seq) = ping_seq(&started) {
                let from = if started
                    .iter()
                    .any(|a| matches!(a, Action::Send { to, .. } if to == "b:1"))
                {
                    "b"
                } else {
                    "c"
                };
                s.handle(
                    m(from, &format!("{from}:1"), Kind::Ack { seq }, vec![]),
                    t + 1,
                );
            }
        }
    }

    /// v1.1.0's rule, kept behind `reprime_on_rescued_probe: false` for the
    /// mixed-version simulation: an indirect ack resets the count, so a member
    /// rescued by helpers every round is never re-greeted.
    #[test]
    fn with_the_v1_1_rule_a_rescued_probe_resets_the_regreet_count() {
        let mut s = node_holding_b(&Config {
            reprime_on_rescued_probe: false,
            ..fast_cfg()
        });
        let mut out = Vec::new();
        s.apply_update(&alive_update("c", "c:1", 0), 0, &mut out);
        let mut t = 100;
        for _ in 0..12 {
            let started = s.tick(t);
            assert!(
                joins_to(&started).is_empty(),
                "never re-greeted: {started:?}"
            );
            let pinged_b = started
                .iter()
                .any(|a| matches!(a, Action::Send { to, .. } if to == "b:1"));
            if pinged_b {
                s.tick(t + 21);
                s.handle(
                    m(
                        "c",
                        "c:1",
                        Kind::IndirectAck {
                            target: "b".to_string(),
                        },
                        vec![],
                    ),
                    t + 25,
                );
            } else if let Some(seq) = ping_seq(&started) {
                s.handle(m("c", "c:1", Kind::Ack { seq }, vec![]), t + 1);
            }
            t += 100;
        }
    }

    /// Issue #971: at the moment a tombstone is pruned this node forgets the member
    /// and would never address it again. If the `Dead` was false and the member has
    /// stopped probing us too (it declared US dead in the same episode), nothing
    /// re-introduces the pair. So the prune sends the member one `Join` — once.
    #[test]
    fn a_pruned_member_is_greeted_once_as_its_tombstone_goes() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0); // bootstrap
        s.apply_update(&dead_update("b", "b:1", 0), 10, &mut out);
        let ttl = fast_cfg().dead_ttl_ms;

        // While the tombstone stands: nothing is sent to a dead member.
        let before = s.tick(10 + ttl - 1);
        assert!(joins_to(&before).is_empty(), "{before:?}");

        // The prune: one certificate-carrying greeting, carrying our own claim.
        let pruned = s.tick(10 + ttl);
        assert_eq!(member_state(&s, "b"), None);
        assert_eq!(joins_to(&pruned), vec!["b:1".to_string()]);
        let greeting = pruned
            .iter()
            .find_map(|a| match a {
                Action::Send { msg, .. } if matches!(msg.kind, Kind::Join) => Some(msg),
                _ => None,
            })
            .unwrap();
        assert_eq!(greeting.gossip, vec![alive_update("a", "a:1", 1)]);

        // Once. The member is gone from the view; nothing addresses it again.
        for i in 1..=30u64 {
            let later = s.tick(10 + ttl + i * 100);
            assert!(joins_to(&later).is_empty(), "{later:?}");
        }

        // If the member is alive it answers with a Sync, and is back first-hand.
        s.handle(
            m("b", "b:1", Kind::Sync, vec![alive_update("b", "b:1", 1)]),
            10 + ttl + 5000,
        );
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
    }

    /// The parting greeting has two exceptions: v1.1.0's rule (off), and a node that
    /// is itself leaving — it must not invite anyone back.
    #[test]
    fn no_parting_greeting_under_the_v1_1_rule_or_while_leaving() {
        let ttl = fast_cfg().dead_ttl_ms;
        let mut out = Vec::new();

        let mut old = Swim::new(
            NodeId("a".to_string()),
            "a:1".to_string(),
            peer_addr_of("a:1"),
            None,
            1,
            Config {
                greet_pruned: false,
                ..fast_cfg()
            },
            Vec::new(),
        );
        old.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        old.tick(0);
        old.apply_update(&dead_update("b", "b:1", 0), 10, &mut out);
        let pruned = old.tick(10 + ttl);
        assert_eq!(member_state(&old, "b"), None);
        assert!(joins_to(&pruned).is_empty(), "{pruned:?}");

        let mut leaving = node("a", "a:1", &[]);
        leaving.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        leaving.tick(0);
        leaving.apply_update(&dead_update("b", "b:1", 0), 10, &mut out);
        let _ = leaving.leave();
        let pruned = leaving.tick(10 + ttl);
        assert_eq!(member_state(&leaving, "b"), None);
        assert!(joins_to(&pruned).is_empty(), "{pruned:?}");
    }

    /// Issue #971: a member's restart (a higher generation) is reported to the
    /// driver, once, by its SWIM address — whether we learn of it first-hand or from
    /// a relay — so the driver can show the new process our certificate again. A
    /// first sighting, a refutation and a stale claim are not restarts.
    #[test]
    fn a_members_new_life_is_reported_to_the_driver_exactly_once() {
        let mut s = node("a", "a:1", &[]);
        // First sighting of b and c (generation 1): nothing to re-prime.
        s.handle(
            m("b", "b:1", Kind::Join, vec![alive_update("c", "c:1", 0)]),
            0,
        );
        assert!(s.take_new_lives().is_empty());
        // A higher incarnation of the same life: not a restart.
        s.handle(
            m("b", "b:1", Kind::Sync, vec![alive_update("b", "b:1", 5)]),
            1,
        );
        assert!(s.take_new_lives().is_empty());

        // b's own datagram from a new process: first-hand.
        let mut from_new_b = m("b", "b:1", Kind::Ping { seq: 1 }, vec![]);
        from_new_b.from_generation = 2;
        s.handle(from_new_b.clone(), 2);
        assert_eq!(s.take_new_lives(), vec!["b:1".to_string()]);
        assert!(s.take_new_lives().is_empty(), "reported once");
        // More datagrams from the same life: nothing new.
        s.handle(from_new_b, 3);
        assert!(s.take_new_lives().is_empty());

        // c's restart, learned from a relay.
        let relayed = Update {
            generation: 2,
            ..alive_update("c", "c:1", 1)
        };
        s.handle(m("b", "b:1", Kind::Sync, vec![relayed.clone()]), 4);
        assert_eq!(s.take_new_lives(), vec!["c:1".to_string()]);
        // A claim about c's previous life changes nothing and reports nothing.
        s.handle(
            m("b", "b:1", Kind::Sync, vec![dead_update("c", "c:1", 9)]),
            5,
        );
        assert!(s.take_new_lives().is_empty());
        assert_eq!(member_state(&s, "c"), Some(MemberState::Alive));
    }

    /// `greet` is what the driver sends to the source of a datagram it could not
    /// authenticate to a node (issue #971). It must say only who we are, and must
    /// not spend pending updates' retransmit budget on that address.
    #[test]
    fn a_greeting_carries_only_our_own_claim_and_leaves_the_gossip_queue_alone() {
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.apply_update(&dead_update("c", "c:1", 4), 0, &mut out);
        let queued = s.gossip.clone();
        assert_eq!(queued.len(), 2);

        let Action::Send { to, msg } = s.greet("203.0.113.9:7946") else {
            panic!("a greeting is a datagram");
        };
        assert_eq!(to, "203.0.113.9:7946");
        assert_eq!(msg.kind, Kind::Join);
        assert_eq!(msg.gossip, vec![alive_update("a", "a:1", 1)]);
        assert_eq!(s.gossip, queued, "the queue and its budgets are untouched");
        assert!(
            s.members().iter().all(|m| m.addr != "203.0.113.9:7946"),
            "greeting an address does not make it a member"
        );
    }

    // ---- ADR 0087 D2 (O1, decided 2026-10-14): a member confirmed gone ----

    /// The `Gone` actions in `actions`, by member id.
    fn gone_in(actions: &[Action]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Gone { id, .. } => Some(id.0.clone()),
                _ => None,
            })
            .collect()
    }

    /// The tombstone is pruned and the parting greeting goes out; when it has been
    /// unanswered for an ack timeout, the member is confirmed gone — once, with the
    /// addresses it was known by. Nothing is said while the tombstone stands.
    #[test]
    fn a_pruned_member_whose_greeting_goes_unanswered_is_confirmed_gone() {
        let cfg = fast_cfg();
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0);
        s.apply_update(&dead_update("b", "b:1", 0), 10, &mut out);

        for t in [10, 10 + cfg.dead_ttl_ms - 1] {
            assert!(
                gone_in(&s.tick(t)).is_empty(),
                "the tombstone stands at {t}"
            );
        }
        let pruned_at = 10 + cfg.dead_ttl_ms;
        let pruned = s.tick(pruned_at);
        assert_eq!(joins_to(&pruned), vec!["b:1".to_string()], "greeted");
        assert!(
            gone_in(&pruned).is_empty(),
            "not before the greeting had its chance"
        );
        assert!(gone_in(&s.tick(pruned_at + cfg.ack_timeout_ms - 1)).is_empty());

        let confirmed = s.tick(pruned_at + cfg.ack_timeout_ms);
        assert_eq!(
            confirmed
                .iter()
                .filter_map(|a| match a {
                    Action::Gone {
                        id,
                        addr,
                        peer_addr,
                    } => {
                        Some((id.0.as_str(), addr.as_str(), peer_addr.as_str()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![("b", "b:1", "b:1-peer")]
        );
        for i in 1..=20u64 {
            let later = s.tick(pruned_at + cfg.ack_timeout_ms + i * 100);
            assert!(gone_in(&later).is_empty(), "once: {later:?}");
        }
    }

    /// A member that answers the parting greeting — the verdict was false, and it is
    /// alive — is re-admitted and never confirmed gone.
    #[test]
    fn a_pruned_member_that_answers_the_greeting_is_not_confirmed_gone() {
        let cfg = fast_cfg();
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0);
        s.apply_update(&dead_update("b", "b:1", 0), 10, &mut out);
        let pruned_at = 10 + cfg.dead_ttl_ms;
        s.tick(pruned_at);

        let answer = s.handle(
            m("b", "b:1", Kind::Sync, vec![alive_update("b", "b:1", 1)]),
            pruned_at + 1,
        );
        assert!(gone_in(&answer).is_empty());
        assert_eq!(member_state(&s, "b"), Some(MemberState::Alive));
        for i in 0..=20u64 {
            let later = s.tick(pruned_at + cfg.ack_timeout_ms + i * 100);
            assert!(gone_in(&later).is_empty(), "{later:?}");
        }
    }

    /// Without a greeting to wait for — v1.1.0's rule, or a node that is leaving —
    /// the member is confirmed gone at the prune.
    #[test]
    fn a_member_pruned_without_a_greeting_is_confirmed_gone_at_the_prune() {
        let cfg = Config {
            greet_pruned: false,
            ..fast_cfg()
        };
        let mut s = Swim::new(
            NodeId("a".to_string()),
            "a:1".to_string(),
            peer_addr_of("a:1"),
            None,
            1,
            cfg.clone(),
            Vec::new(),
        );
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0);
        s.apply_update(&dead_update("b", "b:1", 0), 10, &mut out);
        assert_eq!(
            gone_in(&s.tick(10 + cfg.dead_ttl_ms)),
            vec!["b".to_string()]
        );
    }

    /// A new generation of the id is the old process gone by definition: confirmed
    /// at once, and reported BEFORE the new life's state change, whether the old
    /// life was held dead, alive, or pruned and waiting out its greeting.
    #[test]
    fn a_new_generation_confirms_the_old_one_gone_before_its_own_state_change() {
        let cfg = fast_cfg();
        let newer = |state| Update {
            generation: 2,
            state,
            ..alive_update("b", "b:1", 0)
        };
        let gone_then_alive = |actions: &[Action]| {
            let gone = actions
                .iter()
                .position(|a| matches!(a, Action::Gone { id, .. } if id.0 == "b"));
            let alive = actions.iter().position(|a| {
                matches!(a, Action::StateChange { id, state: MemberState::Alive, .. } if id.0 == "b")
            });
            assert_eq!(gone_in(actions), vec!["b".to_string()], "{actions:?}");
            assert!(gone < alive, "Gone comes first: {actions:?}");
        };

        // Held dead (the tombstone standing).
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.apply_update(&dead_update("b", "b:1", 0), 10, &mut out);
        let mut out = Vec::new();
        s.apply_update(&newer(MemberState::Alive), 20, &mut out);
        gone_then_alive(&out);

        // Alive (a restart nobody had noticed): confirmed all the same. Alive to
        // alive at the same address is no state change, so none follows.
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        let mut out = Vec::new();
        s.apply_update(&newer(MemberState::Alive), 20, &mut out);
        assert_eq!(gone_in(&out), vec!["b".to_string()], "{out:?}");

        // Pruned, its greeting not yet unanswered: the new life confirms it now, and
        // the greeting's own confirmation does not follow.
        let mut s = node("a", "a:1", &[]);
        let mut out = Vec::new();
        s.apply_update(&alive_update("b", "b:1", 0), 0, &mut out);
        s.tick(0);
        s.apply_update(&dead_update("b", "b:1", 0), 10, &mut out);
        let pruned_at = 10 + cfg.dead_ttl_ms;
        s.tick(pruned_at);
        let mut out = Vec::new();
        s.apply_update(&newer(MemberState::Alive), pruned_at + 1, &mut out);
        gone_then_alive(&out);
        for i in 0..=20u64 {
            let later = s.tick(pruned_at + cfg.ack_timeout_ms + i * 100);
            assert!(gone_in(&later).is_empty(), "once: {later:?}");
        }
    }
}
