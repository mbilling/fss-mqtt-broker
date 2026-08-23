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
    /// Consecutive direct probes of OURS this member never answered (directly or via
    /// helpers). Reset by any ack that clears a probe of it. At
    /// `Config::reprime_after` the next probe is accompanied by a `Join` re-greet —
    /// the certificate-carrying prime frame that heals a receiver-side cert-cache
    /// loss (issue #383): its `Sync` reply re-primes both directions.
    unanswered_probes: u32,
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
    /// Lifeguard local-health awareness (ADR 0016 §2): rises when our *own* probes go
    /// unanswered or we must refute ourselves (signals we are the slow one), decays on a
    /// clean probe. Scales our `ack`/`suspicion` timeouts by `(1 + awareness)` so a
    /// degraded node stops blaming healthy peers. `0` ⇒ today's timeouts.
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
        }
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
        base * self.health_multiplier()
    }

    /// Raise awareness (capped at `awareness_max`): a signal that *we* are the slow one
    /// — an unanswered probe of ours, or having to refute a suspicion about ourselves.
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
            m.addr.clone_from(&u.addr);
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
        self.enqueue_gossip(u.clone());
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
        self.prune_tombstones(now);
        if !self.bootstrapped {
            self.bootstrapped = true;
            self.next_probe_at = now + self.cfg.protocol_period_ms;
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
            self.next_probe_at = now + self.cfg.protocol_period_ms;
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
                if let Some(m) = self.members.get_mut(&target) {
                    // Per-member deafness (issue #383): drives the Join re-greet.
                    m.unanswered_probes = m.unanswered_probes.saturating_add(1);
                }
                self.probe = None;
                self.declare(&target, MemberState::Suspect, now, out);
            }
        }
    }

    /// Remove tombstoned `Dead` members whose `dead_ttl_ms` has elapsed (ADR 0016
    /// phase 1). By now stale gossip has drained, so the id may rejoin as a fresh
    /// member without having to out-race a lingering refutation.
    fn prune_tombstones(&mut self, now: u64) {
        self.members
            .retain(|_, m| m.tombstone_deadline.is_none_or(|d| now < d));
    }

    fn expire_suspects(&mut self, now: u64, out: &mut Vec<Action>) {
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
            self.declare(&id, MemberState::Dead, now, out);
        }
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

        // Learn the sender as an alive member if new.
        let from_id = NodeId(msg.from.clone());
        if from_id != self.local && !self.members.contains_key(&from_id) {
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
            Kind::Ack { seq } => self.on_ack(seq, &mut out),
            Kind::PingReq {
                target,
                target_addr,
            } => {
                // Relay: probe the target, remembering who to answer.
                self.seq += 1;
                let relay_seq = self.seq;
                self.relays
                    .insert(relay_seq, (msg.from_addr, NodeId(target.clone())));
                let ping = self.message(Kind::Ping { seq: relay_seq });
                out.push(Action::Send {
                    to: target_addr,
                    msg: ping,
                });
            }
            Kind::IndirectAck { target } => {
                if let Some(p) = &self.probe {
                    if p.target.0 == target {
                        self.probe = None;
                        self.lower_awareness(); // an indirect probe still succeeded
                        self.failed_probe_rounds = 0; // our PingReq reached a helper
                        if let Some(m) = self.members.get_mut(&NodeId(target)) {
                            m.unanswered_probes = 0;
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

    fn on_ack(&mut self, seq: u64, out: &mut Vec<Action>) {
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
                Action::StateChange { .. } => None,
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
                Action::StateChange { .. } => None,
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
                Action::StateChange { .. } => None,
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
            Action::StateChange { .. } => None,
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
}
