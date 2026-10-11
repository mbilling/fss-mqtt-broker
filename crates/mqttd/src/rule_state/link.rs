//! One peer link's share of keeping `eventual` namespaces alike
//! ([ADR 0085](../../../../docs/adr/0085-rule-state-store.md) D15): what it sends, and
//! what it does with what arrives.
//!
//! A [`StateLink`] belongs to the task that runs the link. That task asks it for the
//! next frame when it has nothing more urgent to write ([`StateLink::next`]) and hands
//! it every state frame it reads ([`StateLink::receive`]), which is applied to the
//! store there and then: a state frame is never a hub command.
//!
//! # Sending what changed
//!
//! The link reads on in each shard's order of change from where it last stopped and
//! sends the keys it finds with their whole states, at most [`FRAME_BUDGET`] bytes a
//! frame. Nothing is queued for it anywhere: what it has yet to send is its distance
//! from the end of that order, which is at most the keys there are, and a link that
//! is slow, or busy with other frames, simply reads on later and finds each key once,
//! with what it holds by then. When it has caught up it waits [`SHIP_PAUSE`] before
//! it looks again, so that a burst of writes leaves as one frame; the first write
//! after that wakes it.
//!
//! # Repair
//!
//! When the link comes up, and every [`REPAIR_EVERY_MS`] after, each side sends the
//! root of its copy's digest. Of the two, the node whose id sorts first answers a
//! root that is not its own with the digest's parts; the other compares them with
//! its own, asks for the parts that differ ([`StateWant`](PeerMessage::StateWant))
//! and sends its own keys of those parts, as the first then does. So two copies that
//! agree exchange a few bytes, and two that differ in a few keys exchange the digest
//! (8 KiB) and the keys of the parts those few are in, about a thousandth of the
//! namespace each. That is how a restarted node is filled, a partition healed, and
//! a state that was refused offered again.
//!
//! A node repairs a namespace over one link at a time. A node that joins has a link
//! to every member, and every one of them would otherwise send it the whole
//! namespace; with one at a time the first fills it and the others, compared when
//! that is done, find little or nothing to send. A side lists the keys it will send
//! when the repair starts, so it does not send back what the other side is sending
//! it, and says when it has sent them by sending its root once more: a repair is
//! over for a side when it has sent its own keys and heard that. A side that hears
//! nothing for [`REPAIR_HOLD`] takes it to be over.
//!
//! # Bounds
//!
//! A frame names a namespace and how its sender declares it. One for a namespace
//! this node's rules do not declare `eventual`, or declare otherwise, is refused
//! whole and counted. A summary of the wrong size is refused. Each key of a
//! [`StateKeys`](PeerMessage::StateKeys) frame is checked and merged on its own
//! ([`Replica::merge`]). What the link itself holds is a position per shard, at most
//! one digest the peer sent, and during a repair the keys (not the values) of the
//! parts being repaired.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use mqtt_cluster::peer::{PeerMessage, STATE_PARTS};

use super::replica::{Begin, Cursors, Refused, Replica, PART_WORDS, REPAIR_HOLD};
use super::{Namespace, Store, SHARDS};

/// How often two members compare their copies of a namespace, unless the store was
/// told otherwise: 30 seconds.
pub(super) const REPAIR_EVERY_MS: u64 = 30_000;

/// The most bytes of keys and states one frame is filled to. One key's state can be
/// larger (a full set is 265 KiB) and then travels alone.
const FRAME_BUDGET: usize = 64 * 1024;

/// How long a link that has sent everything waits before it looks for more.
const SHIP_PAUSE: Duration = Duration::from_millis(5);

/// How long a link with nothing to do sleeps at most.
const IDLE_TICK: Duration = Duration::from_secs(1);

/// A repair under way: the keys to send, as they were listed when it began.
#[derive(Debug)]
struct Repairing {
    /// Per shard, its keys of the parts that differ.
    keys: Vec<Vec<Arc<[u8]>>>,
    /// The shard being sent, and how many of its keys are.
    shard: usize,
    sent: usize,
}

/// What the peer sent to compare.
#[derive(Debug)]
enum Theirs {
    /// Its digest's root.
    Root(u64),
    /// Its digest's parts.
    Parts(Vec<u64>),
}

/// The link's view of one namespace.
#[derive(Debug)]
struct NamespaceLink {
    name: Arc<str>,
    namespace: Weak<Namespace>,
    cursors: Arc<Cursors>,
    /// The shard to read changes from next.
    from: usize,
    /// When to send the digest's root next.
    next_root: Instant,
    /// Whether the peer is owed the digest's parts.
    owes_parts: bool,
    /// The parts to ask the peer for.
    want: Option<[u64; PART_WORDS]>,
    repairing: Option<Repairing>,
    /// The comparison under way on this link, which holds the namespace's one
    /// repair on this node.
    comparing: Option<Comparing>,
    /// Whether the peer is owed the root again: the sign that this side has sent
    /// what the comparison had it send.
    owes_end: bool,
    /// A comparison the peer began while another link was repairing this
    /// namespace: taken up when that one is done.
    deferred: Option<Theirs>,
}

/// How far a comparison has got.
#[derive(Debug, Clone, Copy)]
struct Comparing {
    /// This side has sent its keys of the parts that differ, or has none to send.
    sent: bool,
    /// The peer has said it has sent its own.
    heard: bool,
    /// When to give the peer up, unless something of the comparison happens first.
    until: Instant,
}

impl NamespaceLink {
    fn new(namespace: &Arc<Namespace>, replica: &Replica, now: Instant) -> Self {
        Self {
            name: namespace.name.clone(),
            namespace: Arc::downgrade(namespace),
            cursors: replica.link(namespace.shared.clock.now_ms()),
            from: 0,
            next_root: now,
            owes_parts: false,
            want: None,
            repairing: None,
            comparing: None,
            owes_end: false,
            deferred: None,
        }
    }

    /// Something of the comparison under way happened: the peer is still there.
    fn keep(&mut self, replica: &Replica, id: u64, now: Instant) {
        if let Some(comparing) = &mut self.comparing {
            comparing.until = now + REPAIR_HOLD;
            replica.repair_keep(id, now);
        }
    }

    /// End the comparison under way if both sides are done, or the peer has gone
    /// quiet: the namespace's one repair is then another link's to start.
    fn finish(&mut self, ns: &Namespace, replica: &Replica, id: u64, now: Instant) {
        let Some(comparing) = self.comparing else {
            return;
        };
        let lost = now >= comparing.until && self.repairing.is_none();
        if (comparing.sent && comparing.heard) || lost {
            self.comparing = None;
            Self::end(ns, replica, id);
        }
    }

    /// Give up the namespace's one repair, and wake the links that wait for it.
    fn end(ns: &Namespace, replica: &Replica, id: u64) {
        replica.repair_end(id);
        ns.shared.wake.links.notify_waiters();
    }

    /// Begin a comparison the peer asked for with `theirs`, the namespace's one
    /// repair being this link's.
    fn begin(&mut self, ns: &Namespace, replica: &Replica, theirs: &Theirs, id: u64, now: Instant) {
        if self.compare(ns, replica, theirs) {
            self.comparing = Some(Comparing {
                sent: false,
                heard: false,
                until: now + REPAIR_HOLD,
            });
        } else {
            Self::end(ns, replica, id);
            // Parts that all agree: the peer, which is waiting to be asked for
            // some, is told there is nothing to ask for.
            self.owes_end = matches!(theirs, Theirs::Parts(_));
        }
    }

    /// Start sending this copy's keys of `parts`: they are listed now, before any
    /// of the peer's arrive, so that none of those is sent back to it.
    fn repair(&mut self, ns: &Namespace, replica: &Replica, parts: &[u64; PART_WORDS]) {
        self.repairing = Some(Repairing {
            keys: (0..SHARDS)
                .map(|shard| replica.keys_in_parts(ns, shard, parts))
                .collect(),
            shard: 0,
            sent: 0,
        });
    }

    /// Compare this copy with what the peer sent of its own, the namespace's one
    /// repair being this link's to start: answer a root that differs with the
    /// parts, and a digest's parts by asking for those that differ and sending this
    /// copy's keys of them. Whether there is anything to do.
    fn compare(&mut self, ns: &Namespace, replica: &Replica, theirs: &Theirs) -> bool {
        let mine = replica.digest(ns);
        match theirs {
            Theirs::Root(root) => {
                self.owes_parts = mine.root != *root;
                self.owes_parts
            }
            Theirs::Parts(parts) => {
                let mut differ = [0u64; PART_WORDS];
                for (part, (ours, theirs)) in mine.parts.iter().zip(parts).enumerate() {
                    if ours != theirs {
                        differ[part / 64] |= 1 << (part % 64);
                    }
                }
                let any = differ.iter().any(|word| *word != 0);
                if any {
                    self.want = Some(differ);
                    self.repair(ns, replica, &differ);
                }
                any
            }
        }
    }

    /// The next keys of the repair under way, if any are left to send. `None`:
    /// there is no repair, or it has just sent its last.
    fn repair_keys(&mut self, replica: &Replica) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let repairing = self.repairing.as_mut()?;
        let mut out = Vec::new();
        while let Some(keys) = repairing.keys.get(repairing.shard) {
            if repairing.sent < keys.len() {
                let rest = &keys[repairing.sent..];
                repairing.sent += replica.take_keys(repairing.shard, rest, FRAME_BUDGET, &mut out);
                if !out.is_empty() {
                    return Some(out);
                }
            } else {
                repairing.shard += 1;
                repairing.sent = 0;
            }
        }
        self.repairing = None;
        None
    }
}

/// What one peer link sends and takes of this node's `eventual` namespaces. Made
/// when a link that negotiated peer protocol 14 comes up, dropped with it.
#[derive(Debug)]
pub struct StateLink {
    store: Arc<Store>,
    /// Which of the node's links this is: who holds a namespace's one repair.
    id: u64,
    /// Whether this node's id sorts before the peer's: the side that answers a
    /// differing root with its digest's parts.
    answers: bool,
    /// The store's count of namespaces made, when `namespaces` was last brought up
    /// to date.
    generation: Option<u64>,
    /// By name.
    namespaces: Vec<NamespaceLink>,
    /// The namespace whose keys go first next time.
    turn: usize,
    /// When to look for changed keys again; `None`: now.
    not_before: Option<Instant>,
}

/// The time the link's own task goes by: the runtime's clock, which a test may hold
/// still or move, and which the link's sleeps are measured on.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

impl StateLink {
    /// The link from the node `local` to the node `remote`, over `store`.
    #[must_use]
    pub fn new(store: Arc<Store>, local: &str, remote: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self {
            store,
            id: NEXT.fetch_add(1, Relaxed),
            answers: local < remote,
            generation: None,
            namespaces: Vec::new(),
            turn: 0,
            not_before: None,
        }
    }

    /// Bring the list of namespaces up to date with the rules loaded now.
    fn sync(&mut self, now: Instant) {
        let generation = self.store.shared.generation.load(SeqCst);
        let gone = |link: &NamespaceLink| link.namespace.strong_count() == 0;
        if self.generation == Some(generation) && !self.namespaces.iter().any(gone) {
            return;
        }
        self.generation = Some(generation);
        let live = self.store.eventual();
        let of = |link: &NamespaceLink, ns: &Arc<Namespace>| {
            std::ptr::eq(link.namespace.as_ptr(), Arc::as_ptr(ns))
        };
        self.namespaces
            .retain(|link| live.iter().any(|ns| of(link, ns)));
        for ns in &live {
            if let Some(replica) = &ns.replica {
                if !self.namespaces.iter().any(|link| of(link, ns)) {
                    self.namespaces.push(NamespaceLink::new(ns, replica, now));
                }
            }
        }
        self.namespaces.sort_by(|a, b| a.name.cmp(&b.name));
    }

    /// Whether any namespace has changes this link has not read.
    fn has_changes(&self) -> bool {
        let wall_ms = self.store.shared.clock.now_ms();
        let mut any = false;
        for link in &self.namespaces {
            if let Some(ns) = link.namespace.upgrade() {
                if let Some(replica) = &ns.replica {
                    any |= replica.has_changes(&link.cursors, wall_ms);
                }
            }
        }
        any
    }

    /// The next frame to send, if there is one at `now`: an answer or a summary
    /// first (small, and what a repair waits for), then keys of parts that differ,
    /// then keys that changed.
    pub(super) fn poll(&mut self, now: Instant) -> Option<PeerMessage> {
        self.sync(now);
        let every = Duration::from_millis(self.store.shared.repair_every_ms.load(Relaxed));
        for link in &mut self.namespaces {
            let Some(ns) = link.namespace.upgrade() else {
                continue;
            };
            let Some(replica) = &ns.replica else {
                continue;
            };
            link.finish(&ns, replica, self.id, now);
            // A comparison that waited for another link's repair to be done.
            if link.deferred.is_some()
                && link.comparing.is_none()
                && replica.repair_begin(self.id, now) != Begin::Busy
            {
                if let Some(theirs) = link.deferred.take() {
                    link.begin(&ns, replica, &theirs, self.id, now);
                }
            }
            if let Some(parts) = link.want.take() {
                return Some(PeerMessage::StateWant {
                    namespace: link.name.to_string(),
                    declaration: replica.declaration(),
                    parts: parts.to_vec(),
                });
            }
            let owed = std::mem::take(&mut link.owes_parts);
            // While a comparison is under way the root is its last word, not a new
            // comparison's first.
            let ended = !owed && std::mem::take(&mut link.owes_end);
            if owed || ended || (now >= link.next_root && link.comparing.is_none()) {
                if !owed {
                    link.next_root = now + every;
                }
                let digest = replica.digest(&ns);
                return Some(PeerMessage::StateDigest {
                    namespace: link.name.to_string(),
                    declaration: replica.declaration(),
                    root: digest.root,
                    parts: if owed {
                        digest.parts.to_vec()
                    } else {
                        Vec::new()
                    },
                });
            }
        }
        let count = self.namespaces.len();
        let wall_ms = self.store.shared.clock.now_ms();
        for changed in [false, true] {
            if changed && self.not_before.is_some_and(|at| now < at) {
                return None;
            }
            for step in 0..count {
                let at = (self.turn + step) % count;
                let link = &mut self.namespaces[at];
                let Some(ns) = link.namespace.upgrade() else {
                    continue;
                };
                let Some(replica) = &ns.replica else {
                    continue;
                };
                let mut entries = Vec::new();
                if !changed {
                    if link.repairing.is_none() {
                        continue;
                    }
                    if let Some(keys) = link.repair_keys(replica) {
                        // Still at it: the namespace's one repair stays this link's.
                        link.keep(replica, self.id, now);
                        entries = keys;
                    } else {
                        // All sent: say so, and wait to hear the same.
                        link.owes_end = true;
                        if let Some(comparing) = &mut link.comparing {
                            comparing.sent = true;
                        }
                        link.finish(&ns, replica, self.id, now);
                        return self.poll(now);
                    }
                } else if replica.has_changes(&link.cursors, wall_ms) {
                    replica.take_changed(&link.cursors, &mut link.from, FRAME_BUDGET, &mut entries);
                }
                if entries.is_empty() {
                    continue;
                }
                let frame = PeerMessage::StateKeys {
                    namespace: link.name.to_string(),
                    declaration: replica.declaration(),
                    entries,
                };
                self.turn = (at + 1) % count;
                // Caught up: what is written from now on leaves together.
                if changed && !self.has_changes() {
                    self.not_before = Some(now + SHIP_PAUSE);
                }
                return Some(frame);
            }
        }
        None
    }

    /// The next frame to send, if there is one now. For a link task that is busy
    /// writing other frames and gives state a turn between them.
    pub fn try_next(&mut self) -> Option<PeerMessage> {
        self.poll(now())
    }

    /// The next frame to send, waiting until there is one. Safe to drop at any point
    /// and call again: everything it has decided is in the link, and a frame is only
    /// made as it is returned.
    pub async fn next(&mut self) -> PeerMessage {
        let store = self.store.clone();
        let wake = &store.shared.wake;
        loop {
            let now = now();
            if let Some(frame) = self.poll(now) {
                return frame;
            }
            // Between two rounds the link does not listen, so the writes made
            // meanwhile cost their writers nothing to tell it of.
            if let Some(at) = self.not_before.filter(|at| now < *at) {
                tokio::time::sleep_until(at.into()).await;
                continue;
            }
            // Listen first, then say so, then look once more: a write made after the
            // look finds the flag down and wakes this (see `Wake::ping`).
            let told = wake.links.notified();
            tokio::pin!(told);
            told.as_mut().enable();
            wake.told.store(false, SeqCst);
            if self.has_changes() || self.generation != Some(store.shared.generation.load(SeqCst)) {
                continue;
            }
            // The next thing time alone brings: a root to send, or a comparison
            // whose peer has gone quiet. Never a moment already past.
            let mut until = now + IDLE_TICK;
            for link in &self.namespaces {
                until = until.min(match &link.comparing {
                    Some(comparing) => comparing.until,
                    None => link.next_root,
                });
            }
            let until = until.max(now + Duration::from_millis(1));
            if self.namespaces.is_empty() {
                // No `eventual` namespace: nothing to do until one is declared.
                told.await;
            } else {
                tokio::select! {
                    () = &mut told => {}
                    () = tokio::time::sleep_until(until.into()) => {}
                }
            }
        }
    }

    /// Take a state frame the peer sent: checked against this node's declaration
    /// and applied to the store here, on the link's own task.
    pub fn receive(&mut self, frame: PeerMessage) {
        self.receive_at(frame, now());
    }

    pub(super) fn receive_at(&mut self, frame: PeerMessage, now: Instant) {
        let (name, declaration) = match &frame {
            PeerMessage::StateKeys {
                namespace,
                declaration,
                ..
            }
            | PeerMessage::StateDigest {
                namespace,
                declaration,
                ..
            }
            | PeerMessage::StateWant {
                namespace,
                declaration,
                ..
            } => (namespace.as_str(), *declaration),
            _ => return,
        };
        self.sync(now);
        let found = self
            .namespaces
            .binary_search_by(|link| (*link.name).cmp(name))
            .ok()
            .map(|at| &mut self.namespaces[at])
            .and_then(|link| Some((link.namespace.upgrade()?, link)));
        let Some((ns, link)) = found else {
            self.store.shared.undeclared.fetch_add(1, Relaxed);
            return;
        };
        let Some(replica) = &ns.replica else {
            return;
        };
        if replica.declaration() != declaration {
            replica.refuse(Refused::Declaration);
            return;
        }
        match frame {
            PeerMessage::StateKeys { entries, .. } => {
                for (key, state) in &entries {
                    // Counted where it is refused; the others are still merged.
                    let _ = replica.merge(&ns, key, state);
                }
                link.keep(replica, self.id, now);
            }
            // One comparison at a time, on this link and on this node: while keys are
            // on their way the copies differ, and comparing them again would send
            // those keys twice. One that another link's repair keeps from starting
            // is kept, and started when that repair is done.
            PeerMessage::StateDigest { root, parts, .. }
                if parts.is_empty() || parts.len() == STATE_PARTS =>
            {
                if let Some(comparing) = &mut link.comparing {
                    // The root again: the peer has sent what it had to send. If it
                    // asked for nothing before saying so, there is nothing to send.
                    if parts.is_empty() {
                        comparing.heard = true;
                        comparing.sent |= link.repairing.is_none() && link.want.is_none();
                        link.finish(&ns, replica, self.id, now);
                    }
                    return;
                }
                let theirs = if parts.is_empty() {
                    if !self.answers {
                        return;
                    }
                    Theirs::Root(root)
                } else {
                    Theirs::Parts(parts)
                };
                if replica.repair_begin(self.id, now) == Begin::Busy {
                    link.deferred = Some(theirs);
                } else {
                    link.begin(&ns, replica, &theirs, self.id, now);
                }
            }
            PeerMessage::StateWant { parts, .. } if parts.len() == PART_WORDS => {
                // Asked for after this side sent its digest's parts: sent, whoever
                // else is repairing. A second request while the first is being
                // answered is left to the next comparison.
                if link.repairing.is_none() {
                    let mut asked = [0u64; PART_WORDS];
                    asked.copy_from_slice(&parts);
                    link.repair(&ns, replica, &asked);
                    let comparing = link.comparing.get_or_insert(Comparing {
                        sent: false,
                        heard: false,
                        until: now,
                    });
                    comparing.sent = false;
                    link.keep(replica, self.id, now);
                }
            }
            _ => replica.refuse(Refused::Malformed),
        }
    }
}
