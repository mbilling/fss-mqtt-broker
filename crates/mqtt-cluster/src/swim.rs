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
use std::collections::{HashMap, HashSet};

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
    /// Its current state in our view.
    pub state: MemberState,
    /// Clock time (ms) when it entered `state`; drives the suspicion timeout.
    state_since: u64,
    /// When this member's `Dead` tombstone is pruned (ADR 0016 phase 1). `Some` iff
    /// the member is `Dead`; while set, gossip cannot revive the member.
    tombstone_deadline: Option<u64>,
    /// Distinct nodes that independently suspect this member at its current incarnation
    /// (ADR 0016 §3). Its size shrinks the effective suspicion window; reset whenever the
    /// member's `(incarnation, state)` identity changes.
    suspecters: HashSet<NodeId>,
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
    /// Claimed state.
    pub state: MemberState,
    /// For a `Suspect` claim, the id of the node asserting it (ADR 0016 §3), preserved
    /// through re-broadcast so receivers can count **distinct** independent suspecters
    /// of the same peer. `None` for `Alive`/`Dead` and for full-state relays.
    #[serde(default)]
    pub suspecter: Option<String>,
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
    /// The message kind.
    pub kind: Kind,
    /// Piggybacked membership updates.
    pub gossip: Vec<Update>,
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
    incarnation: Incarnation,
    cfg: Config,
    members: HashMap<NodeId, Member>,
    seeds: Vec<String>,
    /// Updates pending dissemination, each with a remaining re-broadcast count.
    gossip: Vec<(Update, u32)>,
    next_probe_at: u64,
    probe: Option<Probe>,
    seq: u64,
    /// Relayed probes we issued for a `PingReq`: our seq -> (requester addr, target).
    relays: HashMap<u64, (String, NodeId)>,
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
}

impl Swim {
    /// Create a node with the given identity, SWIM address, routing (peer-link)
    /// address, config and seed addresses.
    #[must_use]
    pub fn new(
        local: NodeId,
        local_addr: String,
        local_peer_addr: String,
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
            incarnation: 1,
            cfg,
            members: HashMap::new(),
            seeds,
            gossip: Vec::new(),
            next_probe_at: 0,
            probe: None,
            seq: 0,
            relays: HashMap::new(),
            probe_order: Vec::new(),
            probe_idx: 0,
            rng: rng | 1,
            bootstrapped: false,
            awareness: 0,
            leaving: false,
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
            kind,
            gossip: self.take_gossip(),
        }
    }

    /// Apply one membership update, returning any observable state change.
    ///
    /// Handles self-refutation: a `Suspect`/`Dead` claim about us at an incarnation
    /// `>=` ours triggers a bump-and-`Alive` to override it everywhere.
    fn apply_update(&mut self, u: &Update, now: u64, out: &mut Vec<Action>) {
        if u.id == self.local.0 {
            // Once we have announced a graceful leave we do not refute `Dead` about
            // ourselves — including our own departure gossip echoed back (ADR 0019 §2).
            if u.state != MemberState::Alive && u.incarnation >= self.incarnation && !self.leaving {
                self.incarnation = u.incarnation + 1;
                let refute = Update {
                    id: self.local.0.clone(),
                    addr: self.local_addr.clone(),
                    peer_addr: self.local_peer_addr.clone(),
                    incarnation: self.incarnation,
                    state: MemberState::Alive,
                    suspecter: None,
                };
                self.enqueue_gossip(refute);
                // Having to refute ourselves signals we are the slow one (ADR 0016 §2).
                self.raise_awareness();
            }
            return;
        }

        let id = NodeId(u.id.clone());
        if let Some(m) = self.members.get_mut(&id) {
            // Tombstone fence (ADR 0016 phase 1): while a `Dead` member is tombstoned,
            // no non-`Dead` gossip can revive it — not even a higher incarnation (e.g.
            // the node's own last refutation still in flight when it died). Only the
            // prune in `tick` clears the tombstone, after which the id may rejoin.
            if m.tombstone_deadline.is_some() && u.state != MemberState::Dead {
                return;
            }
            // Record an independent suspecter of the *current* incarnation even when the
            // update does not supersede (a second node suspecting an already-`Suspect`
            // peer) — this is how confirmations accumulate (ADR 0016 §3).
            if u.state == MemberState::Suspect
                && m.state == MemberState::Suspect
                && u.incarnation == m.incarnation
            {
                if let Some(sus) = &u.suspecter {
                    m.suspecters.insert(NodeId(sus.clone()));
                }
            }
            let supersedes = u.incarnation > m.incarnation
                || (u.incarnation == m.incarnation && u.state.precedence() > m.state.precedence());
            if !supersedes {
                return;
            }
            let changed = m.state != u.state;
            let inc_advanced = u.incarnation > m.incarnation;
            m.incarnation = u.incarnation;
            m.addr.clone_from(&u.addr);
            // Never let a claimant that hasn't learned the routing address yet
            // erase one we already know.
            if !u.peer_addr.is_empty() {
                m.peer_addr.clone_from(&u.peer_addr);
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
            if changed {
                m.state = u.state;
                m.state_since = now;
                m.tombstone_deadline = if u.state == MemberState::Dead {
                    Some(now + self.cfg.dead_ttl_ms)
                } else {
                    None
                };
                out.push(Action::StateChange {
                    id: id.clone(),
                    addr: u.addr.clone(),
                    peer_addr: m.peer_addr.clone(),
                    state: u.state,
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
                    state: u.state,
                    state_since: now,
                    tombstone_deadline: if u.state == MemberState::Dead {
                        Some(now + self.cfg.dead_ttl_ms)
                    } else {
                        None
                    },
                    suspecters: match (u.state, &u.suspecter) {
                        (MemberState::Suspect, Some(sus)) => {
                            let mut s = HashSet::new();
                            s.insert(NodeId(sus.clone()));
                            s
                        }
                        _ => HashSet::new(),
                    },
                },
            );
            out.push(Action::StateChange {
                id,
                addr: u.addr.clone(),
                peer_addr: u.peer_addr.clone(),
                state: u.state,
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
            state,
            // Stamp ourselves as the suspecter so independent suspicions are countable
            // through re-broadcast (ADR 0016 §3).
            suspecter: if state == MemberState::Suspect {
                Some(self.local.0.clone())
            } else {
                None
            },
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
            state: MemberState::Dead,
            suspecter: None,
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
            self.next_probe_at = now + self.cfg.protocol_period_ms;
        }
        out
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
                let target = p.target.clone();
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
            .retain(|_, m| m.tombstone_deadline.map_or(true, |d| now < d));
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
        out.push(Action::Send { to: addr, msg });
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
                state: MemberState::Alive,
                suspecter: None,
            };
            self.apply_update(&update, now, &mut out);
        }

        // Merge piggybacked gossip.
        for u in &msg.gossip {
            self.apply_update(u, now, &mut out);
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
                self.probe = None;
                self.lower_awareness(); // a clean round (ADR 0016 §2)
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
            state: MemberState::Alive,
            suspecter: None,
        }];
        for m in self.members.values() {
            updates.push(Update {
                id: m.id.0.clone(),
                addr: m.addr.clone(),
                peer_addr: m.peer_addr.clone(),
                incarnation: m.incarnation,
                state: m.state,
                // A full-state relay does not assert independent suspicion (ADR 0016 §3);
                // real suspecters propagate via the normal gossip re-broadcast path.
                suspecter: None,
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
            state: MemberState::Alive,
            suspecter: None,
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
                state: MemberState::Alive,
                suspecter: None,
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
                state: MemberState::Suspect,
                suspecter: None,
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
                state: MemberState::Suspect,
                suspecter: None,
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
            state,
            suspecter: None,
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
                state: MemberState::Dead,
                suspecter: None,
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
}
