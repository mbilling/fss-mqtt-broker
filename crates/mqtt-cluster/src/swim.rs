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
use std::collections::HashMap;

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
    /// How long a member stays `Suspect` before being declared `Dead`.
    pub suspicion_timeout_ms: u64,
    /// Number of helpers (`k`) asked to probe indirectly.
    pub indirect_probes: usize,
    /// Maximum membership updates piggybacked per outgoing message.
    pub gossip_fanout: usize,
    /// Multiplier on the `~log2(N)` re-broadcast count for each update.
    pub gossip_multiplier: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            protocol_period_ms: 1000,
            ack_timeout_ms: 250,
            suspicion_timeout_ms: 4000,
            indirect_probes: 3,
            gossip_fanout: 6,
            gossip_multiplier: 3,
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
            if u.state != MemberState::Alive && u.incarnation >= self.incarnation {
                self.incarnation = u.incarnation + 1;
                let refute = Update {
                    id: self.local.0.clone(),
                    addr: self.local_addr.clone(),
                    peer_addr: self.local_peer_addr.clone(),
                    incarnation: self.incarnation,
                    state: MemberState::Alive,
                };
                self.enqueue_gossip(refute);
            }
            return;
        }

        let id = NodeId(u.id.clone());
        if let Some(m) = self.members.get_mut(&id) {
            let supersedes = u.incarnation > m.incarnation
                || (u.incarnation == m.incarnation && u.state.precedence() > m.state.precedence());
            if !supersedes {
                return;
            }
            let changed = m.state != u.state;
            m.incarnation = u.incarnation;
            m.addr.clone_from(&u.addr);
            // Never let a claimant that hasn't learned the routing address yet
            // erase one we already know.
            if !u.peer_addr.is_empty() {
                m.peer_addr.clone_from(&u.peer_addr);
            }
            if changed {
                m.state = u.state;
                m.state_since = now;
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
        };
        self.apply_update(&update, now, out);
    }

    /// Advance the protocol clock to `now`, returning datagrams to send and
    /// membership changes observed.
    pub fn tick(&mut self, now: u64) -> Vec<Action> {
        let mut out = Vec::new();
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
            if let Some(p) = &mut self.probe {
                p.indirect_deadline = Some(now + self.cfg.ack_timeout_ms);
            }
        } else if let Some(idl) = p.indirect_deadline {
            if now >= idl {
                // Indirect probing also failed: suspect the target.
                let target = p.target.clone();
                self.probe = None;
                self.declare(&target, MemberState::Suspect, now, out);
            }
        }
    }

    fn expire_suspects(&mut self, now: u64, out: &mut Vec<Action>) {
        let timed_out: Vec<NodeId> = self
            .members
            .values()
            .filter(|m| {
                m.state == MemberState::Suspect
                    && now.saturating_sub(m.state_since) >= self.cfg.suspicion_timeout_ms
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
        self.probe = Some(Probe {
            target,
            seq,
            ack_deadline: now + self.cfg.ack_timeout_ms,
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
        }];
        for m in self.members.values() {
            updates.push(Update {
                id: m.id.0.clone(),
                addr: m.addr.clone(),
                peer_addr: m.peer_addr.clone(),
                incarnation: m.incarnation,
                state: m.state,
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
            indirect_probes: 2,
            gossip_fanout: 8,
            gossip_multiplier: 3,
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
