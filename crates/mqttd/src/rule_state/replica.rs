//! An `eventual` namespace's keys ([ADR 0085](../../../../docs/adr/0085-rule-state-store.md)
//! D4, D5, D15): this node's copy of a namespace every member of the cluster holds.
//!
//! A key is kept as the state that merges ([`KeyState`]): the latest delete it has
//! seen, what it holds since and when that expires. A rule's call is one local write
//! to that state under one shard's lock, stamped by the node's clock, exactly as in a
//! `local` namespace: nothing here waits, sends or allocates in proportion to anything
//! but the key. What makes the copies agree happens off that path, in each peer link's
//! own task ([`StateLink`](super::StateLink)), and this file gives it three things.
//!
//! * **What changed, in order.** A shard numbers its keys' local changes. A key has
//!   one place in that order, its latest change, so a key written a thousand times
//!   while a link is slow is in it once, and what waits for a link is never more than
//!   the keys there are. A link keeps a position per shard and reads on from it
//!   ([`Replica::take_changed`]); the store keeps nothing per link, so a slow or absent
//!   peer costs no memory.
//! * **A merge** for a state that arrives ([`Replica::merge`]): a join, so order and
//!   repetition do not matter. It is bounded as a rule's write is: a key that is new
//!   here needs room under `max_keys`, `max_bytes` and the store's ceiling, a key that
//!   would grow needs the bytes, and a state that fits neither is refused and counted,
//!   and what this node held is kept. A state merged in is not sent on: every member
//!   has a link to the node that wrote it.
//! * **A digest** of the copy ([`Replica::digest`]): one number for each of
//!   [`STATE_PARTS`] parts, a key's part being the top bits of a fixed hash of the
//!   key. Two members whose digests differ exchange the keys of the parts that
//!   differ, which is how a restarted node is filled and a partition healed.
//!
//! A deleted key stays, as a tombstone, for the namespace's tombstone window, and
//! counts as a key until then. A key whose value has expired reads as absent at once
//! and is forgotten when nothing would be lost by it
//! ([`KeyState::forgettable`]).
//!
//! # A key's expiry as an event (D7)
//!
//! While a rule on this node selects `$events/state/expired`, an expired key is not
//! simply forgotten: it **waits** ([`Waiting`]) until one member has raised its event.
//! Every member whose rules select the event says so on its links, and the one that a
//! hash of the namespace and the key picks among them ([`View::owner`], the cluster's
//! rendezvous hash) raises it; the others keep the key until the picked member says
//! how far it has got ([`Peer::said`]), and take it over if that member's link goes
//! first. What waits is this node's own business. It is kept beside the map, not in
//! it, and is never sent: a key that could be forgotten leaves the map when it would
//! have anyway, and one that could not (its delete's window is still open) stays in
//! the map exactly as every other member holds it, with a note here that its event
//! is owed. So the digest of a copy with keys waiting is the digest of one without.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{
    AtomicBool, AtomicU64, AtomicUsize,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use mqtt_cluster::peer::STATE_PARTS;
use mqtt_cluster::NodeId;
use mqtt_rules::crdt::{
    normal, stable_hash, Counts, CountsError, Data, KeyState, Write, WriteError, Writer,
};
use mqtt_rules::state::{
    remembered, written_len, NamespaceSettings, StateError, StateOp, StateReply, MAX_KEY_BYTES,
    MAX_SET_ELEMENTS,
};
use mqtt_rules::Value;

use super::{
    add_within, done, lock, lock_shard, op_key, seconds, wide, Handle, Namespace, Refusal, Step,
    ELEMENT_OVERHEAD, ENTRY_OVERHEAD, NUMBER_BYTES, PURGE_BATCH, PURGE_EVERY_MS, SHARDS,
    SKETCH_OVERHEAD,
};

/// What a key of an `eventual` namespace is accounted beyond a `local` one's: its
/// delete's stamp, its hashes and its places in the orders of change and of
/// forgetting.
const REPLICA_OVERHEAD: usize = 96;

/// What a key is accounted before its own bytes and its value's.
const BASE: usize = ENTRY_OVERHEAD + REPLICA_OVERHEAD;

/// What each node's counts in a counter are accounted beyond the node's id.
const COUNTER_ROW_BYTES: usize = 32;

/// The most keys a link encodes under one hold of a shard's lock: what a rule's call
/// on that shard can be made to wait for.
const HELD_KEYS: usize = 32;

/// The most keys one pass of the sweep looks at in one copy: a mass expiry is worked
/// through over several passes, and the task that sweeps is never kept for longer.
const SWEEP_KEYS: usize = 4096;

/// How many of a shard's unread changes [`Replica::stats`] counts one by one.
const COUNTED: usize = 64;

/// The words of a bitmap of [`STATE_PARTS`] bits.
pub(super) const PART_WORDS: usize = STATE_PARTS / 64;

/// The bits of a key's hash that are not its part.
const PART_SHIFT: u32 = 64 - STATE_PARTS.trailing_zeros();

const _: () = assert!(STATE_PARTS.is_power_of_two() && STATE_PARTS.is_multiple_of(64));

/// Why a state, or a frame, a peer sent was not taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// The sender declares the namespace otherwise than this node does.
    Declaration,
    /// The state is stamped more than 60 seconds ahead of this node's clock (D6).
    ClockSkew,
    /// A key that is new here, or one that would grow, and the namespace is at its
    /// `max_keys` or `max_bytes`.
    NamespaceFull,
    /// The same, at the store's ceiling.
    StoreFull,
    /// A key that is new here while the broker is over its memory watermark.
    Brownout,
    /// Not a key, not a state, or not a summary of the size it must have.
    Malformed,
}

impl Refused {
    /// Every reason, in the order they are counted in.
    pub const ALL: [Self; 6] = [
        Self::Declaration,
        Self::ClockSkew,
        Self::NamespaceFull,
        Self::StoreFull,
        Self::Brownout,
        Self::Malformed,
    ];

    /// The `reason` label of `mqttd_rule_state_replication_refused_total`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Declaration => "declaration",
            Self::ClockSkew => "clock_skew",
            Self::NamespaceFull => "namespace_full",
            Self::StoreFull => "store_full",
            Self::Brownout => "brownout",
            Self::Malformed => "malformed",
        }
    }
}

/// How replication of one `eventual` namespace stands ([`Store::replication`](super::Store::replication)).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Replication {
    /// The peer links that carry it now.
    pub links: usize,
    /// The most keys any one link has yet to send: at most the keys there are.
    pub pending: u64,
    /// How long the link furthest behind has had something to send, in
    /// milliseconds: the time since it last had sent everything. `0` when every
    /// link has.
    pub lag_ms: u64,
    /// States of other nodes merged into this copy.
    pub merged: u64,
    /// States and frames refused, in the order of [`Refused::ALL`].
    pub refused: [u64; Refused::ALL.len()],
    /// The members one of which raises a key's expiry event (D7), as this node
    /// sees them: itself and every peer whose rules select the event, over a link
    /// whose copies have been compared. `0` while this node's own rules do not
    /// select it: it then raises none and keeps no key for one.
    pub expiry_members: usize,
}

impl Replication {
    /// How often states or frames were refused for `reason`.
    #[must_use]
    pub fn refused(&self, reason: Refused) -> u64 {
        self.refused[reason as usize]
    }
}

#[derive(Debug)]
struct Entry {
    /// Its key: the one allocation the map and the two orders share.
    key: Arc<[u8]>,
    state: KeyState,
    /// What it is accounted as: [`BASE`], its key and what it holds.
    bytes: usize,
    /// [`stable_hash`] of the key: its part, and half of its hash.
    key_hash: u64,
    /// The hash of the key and its state as it is now; `0`: not worked out since the
    /// state last changed.
    hash: u64,
    /// Its place in the shard's order of change; `0`: this node has not changed it.
    changed: u64,
    /// Its place in the shard's order of forgetting; `0`: not in it.
    due: u64,
    /// The expiry that has been dealt with: counted, and where its event is owed
    /// noted in the shard's waiting keys. `0`: none. A write or a merge that moves
    /// the expiry makes this an old one. Not part of the state: never sent, never
    /// hashed.
    evented: u64,
    /// Where the shard's waiting keys note this key while it stays in the map; `0`:
    /// they do not.
    waiting: u64,
}

/// An expired key whose event has yet to be raised, by this node or by another.
#[derive(Debug)]
struct Waiting {
    key: Arc<[u8]>,
    /// When it expired.
    expired_ms: u64,
    /// The link to the member that is to raise the event; `0`: this node is.
    owner: u64,
    /// What it held and what that is accounted as, once the key is out of the map
    /// or holds something else; `None` while the map still holds it as it expired.
    body: Option<(KeyState, usize)>,
}

/// An expired key the sweep hands over for its event.
#[derive(Debug)]
pub(super) struct Raised {
    pub(super) key: Arc<[u8]>,
    /// The key's state as it expired: what it held is read out of it with no lock
    /// held.
    pub(super) state: KeyState,
    pub(super) expired_ms: u64,
}

/// Another member, as this node's link to it knows it: what picking the member that
/// raises a key's expiry event goes by. Held by the link; the copy holds it weakly,
/// so a link that is gone is a member that is gone.
#[derive(Debug)]
pub(super) struct Peer {
    /// Which of the node's links this is.
    link: u64,
    node: NodeId,
    /// Whether its rules select the expiry event, as it last said.
    raises: AtomicBool,
    /// Whether its copy has been compared with this one since the link came up: a
    /// member that has just started holds nothing yet, and would raise nothing.
    settled: AtomicBool,
    /// The second up to which it has nothing left to raise, as it last said.
    swept: AtomicU64,
}

impl Peer {
    /// What the peer said of itself
    /// ([`StateExpiry`](mqtt_cluster::peer::PeerMessage::StateExpiry)).
    pub(super) fn said(&self, raises: bool, swept: u64) {
        self.raises.store(raises, Relaxed);
        self.swept.store(swept, Relaxed);
    }

    /// Its copy and this one have been compared.
    pub(super) fn settle(&self) {
        self.settled.store(true, Relaxed);
    }

    pub(super) fn settled(&self) -> bool {
        self.settled.load(Relaxed)
    }
}

/// The members that raise a namespace's expiry events, as this node sees them now:
/// itself, and every peer that says it does over a link whose copies were compared.
struct View {
    /// This node first, then `peers`' nodes.
    nodes: Vec<NodeId>,
    peers: Vec<Arc<Peer>>,
    namespace: Arc<str>,
}

impl View {
    /// Who raises the event of `key`: the link to that member, or `0` for this node.
    /// The same rendezvous hash that places sessions (`mqtt_cluster::hrw`), over the
    /// namespace and the key, so every member with the same view picks the same one.
    fn owner(&self, key: &[u8]) -> u64 {
        if self.peers.is_empty() {
            return 0;
        }
        let mut named = Vec::with_capacity(self.namespace.len() + 1 + key.len());
        named.extend_from_slice(self.namespace.as_bytes());
        named.push(0);
        named.extend_from_slice(key);
        let picked = mqtt_cluster::hrw::owner(&named, &self.nodes);
        self.nodes
            .iter()
            .zip(std::iter::once(0).chain(self.peers.iter().map(|peer| peer.link)))
            .find(|(node, _)| Some(*node) == picked)
            .map_or(0, |(_, link)| link)
    }

    fn peer(&self, link: u64) -> Option<&Peer> {
        self.peers
            .iter()
            .find(|peer| peer.link == link)
            .map(|peer| &**peer)
    }
}

/// What looking at a shard's keys that are due did.
#[derive(Debug, Default, Clone, Copy)]
struct Visited {
    /// Keys looked at.
    looked: usize,
    /// Keys taken out with their room given back.
    removed: usize,
}

/// What a pass of the sweep over one copy found ([`Replica::forget_due`]).
#[derive(Debug, Default)]
pub(super) struct Forgotten {
    /// Keys forgotten with no event owed by this node.
    pub(super) removed: usize,
    /// The second the next key is due in; `None`: none is.
    pub(super) next_due: Option<u64>,
    /// Whether keys are due that this pass did not get to.
    pub(super) more: bool,
    /// Per shard that has some, the second its earliest key this node is to raise
    /// the event of expired in.
    pub(super) held: Vec<(usize, u64)>,
}

#[derive(Debug, Default)]
struct Shard {
    map: HashMap<Arc<[u8]>, Entry>,
    /// The keys this node changed, by the number of their latest change.
    changed: BTreeMap<u64, Arc<[u8]>>,
    /// The keys that will be forgettable, by the second they will be.
    due: BTreeSet<(u64, Arc<[u8]>)>,
    /// The number of the latest change.
    last: u64,
    /// The furthest any link has read. A key changed after that is still ahead of
    /// every link, and changing it again does not move it.
    read_to: u64,
    /// How often a key was put into or taken out of `map`, by a rule's write, a merge
    /// or forgetting: what tells an operator's listing, which walks the map a few
    /// keys per hold of the lock, that the map changed under it (D10). **Every
    /// `map.insert` and `map.remove` in this file counts here.**
    changes: u64,
    /// The expired keys whose event this node is to raise, by the second they
    /// expired in and the order they were found in. Empty, and no allocation, unless
    /// a rule on this node selects the event.
    held: BTreeMap<(u64, u64), Waiting>,
    /// Those another member is to raise: kept until it says it has, or is gone.
    standby: BTreeMap<(u64, u64), Waiting>,
    /// The next place in `held` and `standby`.
    waits: u64,
}

impl Shard {
    /// Note a key whose event is owed; where.
    fn wait(&mut self, waiting: Waiting) -> u64 {
        self.waits += 1;
        let at = (waiting.expired_ms.div_ceil(1000), self.waits);
        if waiting.owner == 0 {
            self.held.insert(at, waiting);
        } else {
            self.standby.insert(at, waiting);
        }
        self.waits
    }

    /// The note of a key that is still in the map, which expired at `expired_ms`.
    fn waiting_mut(&mut self, expired_ms: u64, place: u64) -> Option<&mut Waiting> {
        let at = (expired_ms.div_ceil(1000), place);
        match self.held.get_mut(&at) {
            Some(waiting) => Some(waiting),
            None => self.standby.get_mut(&at),
        }
    }
}

/// What a shard tells readers that do not take its lock. A cache line of its own:
/// the threads writing to one shard do not take it from those writing to another.
#[derive(Debug, Default)]
#[repr(align(64))]
struct Marks {
    /// The number of the shard's latest change.
    last: AtomicU64,
    /// Raised by every change to the shard, a merge included: what a digest is
    /// good for.
    mutations: AtomicU64,
    /// The rules' calls answered on this shard's keys, by operation
    /// ([`OPS`](super::OPS)): counted beside the shard, so counting shares nothing
    /// between shards (D10).
    ops: [AtomicU64; super::OPS.len()],
}

/// A link's place in each shard's order of change: everything up to it has been
/// handed to that link.
#[derive(Debug)]
pub(super) struct Cursors {
    at: Box<[AtomicU64]>,
    /// When the link last found nothing ahead of it, by the wall clock.
    caught_up_ms: AtomicU64,
}

/// One namespace's summary, and what it is good for.
#[derive(Debug)]
pub(super) struct Digest {
    /// A hash over `parts`.
    pub(super) root: u64,
    /// Per part, the hashes of its keys folded together.
    pub(super) parts: Box<[u64]>,
    /// The namespace's count of changes when it was made.
    mutations: u64,
    /// The first moment a key it covers can be forgotten.
    good_until_ms: u64,
}

/// One `eventual` namespace's keys.
#[derive(Debug)]
pub(super) struct Replica {
    shards: Box<[Mutex<Shard>]>,
    /// What each shard tells readers that do not take its lock.
    marks: Box<[Marks]>,
    /// The declaration the rules loaded last gave: the bounds a merge keeps to.
    max_keys: AtomicUsize,
    max_bytes: AtomicU64,
    window_ms: AtomicU64,
    declaration: AtomicU64,
    digest: Mutex<Option<Arc<Digest>>>,
    links: Mutex<Vec<Weak<Cursors>>>,
    /// The link that is repairing this copy, and until when it is taken to be.
    repairing: Mutex<Option<(u64, Instant)>>,
    refused: [AtomicU64; Refused::ALL.len()],
    merged: AtomicU64,
    /// Keys whose expiry has been dealt with (D10's `expired`): forgotten, or their
    /// event handed over, or left to the member that raised it. Not the deleted
    /// ones, whose tombstone's window ran out.
    expired: AtomicU64,
    /// The other members, as the links to them know them.
    peers: Mutex<Vec<Weak<Peer>>>,
    /// The keys that wait for their event, here or on another member.
    waiting: AtomicU64,
    /// Those of them another member is to raise.
    standing_by: AtomicU64,
    /// The links of the members last picked among: a change means every key that
    /// stands by is looked at again.
    picked_among: Mutex<Vec<u64>>,
    /// The second up to which this node has nothing left to raise.
    swept: AtomicU64,
    /// Raised when `swept` is worth telling the peers at once.
    announce: AtomicU64,
}

/// What a link that wants to start a repair is told ([`Replica::repair_begin`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Begin {
    /// It may: the namespace's one repair is this link's now.
    Started,
    /// It already has one under way.
    Mine,
    /// Another link has.
    Busy,
}

/// How long a repair is taken to be under way after the last sign of it: after that
/// another link may start one, so a link that died in the middle holds nothing up.
pub(super) const REPAIR_HOLD: Duration = Duration::from_secs(5);

/// The fingerprint of a declaration: of what two nodes must agree on for their
/// copies to merge. The mode, the form a state travels in, and the tombstone window
/// (two nodes that forget deletes at different times would bring each other's
/// deleted keys back). The bounds are not in it: a node may hold fewer keys.
pub(super) fn declaration_of(settings: &NamespaceSettings) -> u64 {
    let mut bytes = b"mqttd rule state: eventual, form 1, window ".to_vec();
    bytes.extend_from_slice(&settings.tombstone_window_secs.to_le_bytes());
    stable_hash(&bytes)
}

/// The hash of a key and its encoded state. Never `0`.
fn entry_hash(key_hash: u64, state: &[u8]) -> u64 {
    let mut both = [0u8; 16];
    both[..8].copy_from_slice(&key_hash.to_le_bytes());
    both[8..].copy_from_slice(&stable_hash(state).to_le_bytes());
    stable_hash(&both).max(1)
}

/// The part a key is summarised in.
fn part_of(key_hash: u64) -> usize {
    usize::try_from(key_hash >> PART_SHIFT).unwrap_or(0)
}

/// What a counter is accounted as.
fn counter_bytes(counts: &Counts) -> usize {
    NUMBER_BYTES + counts.nodes().saturating_sub(1) * COUNTER_ROW_BYTES + counts.node_bytes()
}

/// What a key's data is accounted as, worked out from the data: for a state that
/// arrives. A rule's own write knows what it added.
fn weigh(data: &Data) -> usize {
    match data {
        Data::Lww { value, .. }
        | Data::First { value, .. }
        | Data::Max(value)
        | Data::Min(value) => written_len(value),
        Data::Sum(counts) => counter_bytes(counts),
        Data::Union(elements) => elements
            .iter()
            .map(|element| ELEMENT_OVERHEAD + written_len(element))
            .sum(),
        Data::Hll(sketch) => SKETCH_OVERHEAD + sketch.bytes(),
    }
}

/// The second in which `state` is next to be looked at if nothing changes: when what
/// it holds expires, unless that expiry has been dealt with (`evented`), and then
/// when it is forgettable. `0`: never.
fn due_slot(state: &KeyState, window_ms: u64, evented: u64) -> u64 {
    let window_over = state.deleted().ms().saturating_add(window_ms);
    let at = match state.data() {
        None => window_over,
        Some(_) if state.expires_ms() == 0 => return 0,
        Some(_) if state.expires_ms() != evented => state.expires_ms(),
        Some(_) => state.expires_ms().max(window_over),
    };
    at.div_ceil(1000).max(1)
}

/// Whether what `state` holds has expired by `now_ms`.
fn expired(state: &KeyState, now_ms: u64) -> bool {
    state.data().is_some() && state.expires_ms() != 0 && state.expires_ms() <= now_ms
}

/// Put `entry` at `slot` in the order of forgetting.
fn set_due(due: &mut BTreeSet<(u64, Arc<[u8]>)>, entry: &mut Entry, slot: u64) {
    if entry.due == slot {
        return;
    }
    if entry.due != 0 {
        due.remove(&(entry.due, entry.key.clone()));
    }
    if slot != 0 {
        due.insert((slot, entry.key.clone()));
    }
    entry.due = slot;
}

/// Account a change in the size of a key that stays, whatever the bounds: a rule's
/// write to a key that exists is bounded per key, not by the namespace's room.
fn resize(ns: &Namespace, entry: &mut Entry, bytes: usize) {
    if bytes == entry.bytes {
        // The usual write: nothing to tell the counts every thread shares.
        return;
    }
    if bytes > entry.bytes {
        let more = wide(bytes - entry.bytes);
        ns.bytes.fetch_add(more, Relaxed);
        ns.shared.bytes.fetch_add(more, Relaxed);
    } else {
        let less = wide(entry.bytes - bytes);
        ns.bytes.fetch_sub(less, Relaxed);
        ns.shared.bytes.fetch_sub(less, Relaxed);
    }
    entry.bytes = bytes;
}

/// Take `key` out of `shard`'s map and orders. Its room is still accounted.
fn detach(shard: &mut Shard, key: &[u8]) -> Option<Entry> {
    let entry = shard.map.remove(key)?;
    shard.changes = shard.changes.wrapping_add(1);
    if entry.changed != 0 {
        shard.changed.remove(&entry.changed);
    }
    if entry.due != 0 {
        shard.due.remove(&(entry.due, entry.key.clone()));
    }
    Some(entry)
}

/// Give back the room of a key that is gone.
fn release(ns: &Namespace, bytes: usize) {
    ns.keys.fetch_sub(1, Relaxed);
    ns.bytes.fetch_sub(wide(bytes), Relaxed);
    ns.shared.bytes.fetch_sub(wide(bytes), Relaxed);
}

/// Take `key` out of `shard` and give its room back.
fn remove(ns: &Namespace, shard: &mut Shard, key: &[u8]) {
    if let Some(entry) = detach(shard, key) {
        release(ns, entry.bytes);
    }
}

/// The time a call is made at.
struct Now<'a> {
    by: Writer<'a>,
    /// The clock's physical part: what expiries are judged by.
    ms: u64,
}

/// A reply that carries a value and charges nothing to the build budget.
fn reply(value: Option<Value>) -> StateReply {
    StateReply {
        value,
        read_bytes: 0,
    }
}

/// What a key holds, as the reply of the write that left it so: the value, the
/// count, the estimate.
fn now_held(state: &KeyState) -> StateReply {
    reply(state.data().map(Data::read))
}

impl Replica {
    pub(super) fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            marks: (0..SHARDS).map(|_| Marks::default()).collect(),
            max_keys: AtomicUsize::new(0),
            max_bytes: AtomicU64::new(0),
            window_ms: AtomicU64::new(0),
            declaration: AtomicU64::new(0),
            digest: Mutex::new(None),
            links: Mutex::new(Vec::new()),
            repairing: Mutex::new(None),
            refused: Default::default(),
            merged: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            peers: Mutex::new(Vec::new()),
            waiting: AtomicU64::new(0),
            standing_by: AtomicU64::new(0),
            picked_among: Mutex::new(Vec::new()),
            swept: AtomicU64::new(0),
            announce: AtomicU64::new(0),
        }
    }

    /// Take the declaration of the rules being loaded: its bounds are the ones a
    /// merge keeps to from now on.
    pub(super) fn declare(&self, settings: &NamespaceSettings) {
        self.max_keys.store(settings.max_keys, Relaxed);
        self.max_bytes.store(settings.max_bytes, Relaxed);
        self.window_ms
            .store(settings.tombstone_window_secs.saturating_mul(1000), Relaxed);
        self.declaration.store(declaration_of(settings), Relaxed);
    }

    /// The fingerprint of the declaration in force.
    pub(super) fn declaration(&self) -> u64 {
        self.declaration.load(Relaxed)
    }

    /// Count something a peer sent and this node did not take.
    pub(super) fn refuse(&self, reason: Refused) {
        self.refused[reason as usize].fetch_add(1, Relaxed);
    }

    // -- a rule's call ------------------------------------------------------------------

    /// One state call of a rule: as in a `local` namespace, one operation under one
    /// shard's lock. A write is stamped, and its key takes its place in the order
    /// of change; the links are told afterwards, with the lock released.
    pub(super) fn call(&self, handle: &Handle, op: &StateOp<'_>) -> Result<StateReply, StateError> {
        let reply = self.apply(handle, op)?;
        let at = handle.namespace.shard_of(op_key(op));
        self.marks[at].ops[super::admin::op_index(op)].fetch_add(1, Relaxed);
        Ok(reply)
    }

    /// [`call`](Self::call), not counted as a rule's: also what an operator's delete
    /// is (D10), which is a `kv_del` in every other respect, stamped and sent.
    pub(super) fn apply(
        &self,
        handle: &Handle,
        op: &StateOp<'_>,
    ) -> Result<StateReply, StateError> {
        self.with_room(handle, || self.attempt(handle, op))
    }

    /// Try `attempt` until it is done, making room once where a new key found none.
    fn with_room<T>(
        &self,
        handle: &Handle,
        mut attempt: impl FnMut() -> Result<Step<T>, StateError>,
    ) -> Result<T, StateError> {
        let mut made_room = false;
        loop {
            let refusal = match attempt()? {
                Step::Done(reply) => return Ok(reply),
                Step::NeedRoom(refusal) => refusal,
            };
            // Keys that can be forgotten, anywhere in the namespace, are room.
            if !matches!(refusal, Refusal::Brownout) && !made_room && self.purge_all(handle) {
                made_room = true;
                continue;
            }
            return Err(handle.full(refusal));
        }
    }

    /// Remember the idempotency key whose entry is `seen` (D8), for the namespace's
    /// dedupe window from now: whether this copy did not remember it already. The
    /// entry is a key like any other, written first (`first`), so it is sent to every
    /// member, and two members' entries merge to the earlier. One this copy holds is
    /// left as it is, its expiry included, and nothing is sent.
    pub(super) fn remember(&self, handle: &Handle, seen: &[u8]) -> Result<bool, StateError> {
        let ns = &*handle.namespace;
        let node = ns.shared.node();
        let at = ns.shard_of(seen);
        let new = self.with_room(handle, || {
            let wall_ms = ns.shared.clock.now_ms();
            let now = Now {
                by: Writer {
                    node: &node.counts_as,
                    clock: &node.clock,
                    wall_ms,
                },
                ms: node.clock.physical_ms(wall_ms),
            };
            let mut guard = lock_shard(&self.shards[at]);
            let held = guard.map.get(seen);
            if held.is_some_and(|entry| entry.state.live(now.ms).is_some()) {
                return Ok(Step::Done(false));
            }
            let lapsed = held.is_some_and(|entry| expired(&entry.state, now.ms));
            let since = Value::Int(i64::try_from(now.ms).unwrap_or(i64::MAX));
            let window = Some(handle.settings.dedupe_window_secs);
            let write = Write::SetOnce(&since);
            self.write(
                handle,
                &mut guard,
                at,
                seen,
                &write,
                window,
                NUMBER_BYTES,
                &now,
                lapsed,
                |_| true,
            )
        })?;
        if new {
            ns.shared.wake.ping();
        }
        Ok(new)
    }

    /// What the counter `op` is about holds now, for a `kv_incr` that adds nothing to
    /// it (its idempotency key is remembered): `0` when there is none. Not a write.
    pub(super) fn count(
        &self,
        handle: &Handle,
        op: &StateOp<'_>,
    ) -> Result<StateReply, StateError> {
        let ns = &*handle.namespace;
        let now_ms = Self::now_ms(ns);
        let key = op_key(op);
        let at = ns.shard_of(key);
        let count = {
            let shard = lock_shard(&self.shards[at]);
            match shard.map.get(key).and_then(|e| e.state.live(now_ms)) {
                None => Value::Int(0),
                Some(data @ Data::Sum(_)) => data.read(),
                Some(held) => return Err(ns.wrong_type(held, "sum")),
            }
        };
        self.marks[at].ops[super::admin::op_index(op)].fetch_add(1, Relaxed);
        Ok(reply(Some(count)))
    }

    #[allow(clippy::too_many_lines)]
    fn attempt(&self, handle: &Handle, op: &StateOp<'_>) -> Result<Step<StateReply>, StateError> {
        let ns = &*handle.namespace;
        let node = ns.shared.node();
        let wall_ms = ns.shared.clock.now_ms();
        let now = Now {
            by: Writer {
                node: &node.counts_as,
                clock: &node.clock,
                wall_ms,
            },
            ms: node.clock.physical_ms(wall_ms),
        };
        let key = op_key(op);
        let at = ns.shard_of(key);
        let mut wrote = false;
        let step = {
            let mut guard = lock_shard(&self.shards[at]);
            let shard = &mut *guard;
            let held = shard.map.get(key);
            // Whether what the key held has expired: a write then starts it afresh,
            // and what it held may be owed an event (D7).
            let lapsed = held.is_some_and(|entry| expired(&entry.state, now.ms));
            // What the key holds now, and what that is accounted as.
            let live = held.and_then(|entry| {
                let data = entry.state.live(now.ms)?;
                Some((data, entry.bytes.saturating_sub(BASE + key.len())))
            });
            macro_rules! write {
                ($write:expr, $ttl:expr, $carried:expr, $reply:expr) => {{
                    let write = $write;
                    let step = self.write(
                        handle, shard, at, key, &write, $ttl, $carried, &now, lapsed, $reply,
                    )?;
                    wrote = true;
                    step
                }};
            }
            match *op {
                StateOp::Get { .. } => {
                    let Some((data, bytes)) = live else {
                        return Ok(done(None));
                    };
                    // A sketch reads as its estimate, not as its registers.
                    let read_bytes = if matches!(data, Data::Hll(_)) {
                        NUMBER_BYTES
                    } else {
                        bytes
                    };
                    Step::Done(StateReply {
                        value: Some(data.read()),
                        read_bytes,
                    })
                }
                StateOp::Ttl { .. } => {
                    let expires_ms = live
                        .and_then(|_| shard.map.get(key))
                        .map_or(0, |entry| entry.state.expires_ms());
                    done((expires_ms != 0).then(|| {
                        let secs = expires_ms.saturating_sub(now.ms).div_ceil(1000);
                        Value::Int(i64::try_from(secs).unwrap_or(i64::MAX))
                    }))
                }
                StateOp::HllCount { .. } => match live {
                    None => done(Some(Value::Int(0))),
                    Some((data @ Data::Hll(_), _)) => done(Some(data.read())),
                    Some((held, _)) => return Err(ns.wrong_type(held, "hll")),
                },
                StateOp::Set { value, ttl, .. } => {
                    write!(Write::Set(value.value), ttl, value.bytes, |_| reply(None))
                }
                StateOp::SetOnce { value, ttl, .. } => {
                    if let Some((Data::First { value: first, .. }, bytes)) = live {
                        let first = StateReply {
                            value: Some(first.clone()),
                            read_bytes: bytes,
                        };
                        if ttl.is_none() {
                            return Ok(Step::Done(first));
                        }
                        write!(Write::SetOnce(value.value), ttl, value.bytes, |_| first)
                    } else {
                        write!(Write::SetOnce(value.value), ttl, value.bytes, now_held)
                    }
                }
                StateOp::Incr { by, ttl, .. } => write!(Write::Incr(by), ttl, 0, now_held),
                // Two keys, so never one write: `Handle::incr_once` takes it apart.
                StateOp::IncrOnce { .. } => Step::NeedRoom(Refusal::Other),
                StateOp::Extreme {
                    value,
                    largest,
                    ttl,
                    ..
                } => write!(
                    Write::Extreme {
                        value: value.value,
                        largest
                    },
                    ttl,
                    0,
                    now_held
                ),
                StateOp::Add { element, ttl, .. } => {
                    write!(Write::Add(element.value), ttl, element.bytes, |state| {
                        let count = match state.data() {
                            Some(Data::Union(elements)) => elements.len(),
                            _ => 0,
                        };
                        reply(Some(Value::Int(i64::try_from(count).unwrap_or(i64::MAX))))
                    })
                }
                StateOp::HllAdd { hash, ttl, .. } => write!(Write::HllAdd(hash), ttl, 0, now_held),
                // A key that holds nothing here is left alone: no tombstone is made
                // for a key this node has never seen, or has already seen deleted.
                StateOp::Del { .. } if live.is_none() => done(Some(Value::Bool(false))),
                StateOp::Del { .. } => {
                    write!(Write::Delete, None, 0, |_| reply(Some(Value::Bool(true))))
                }
                StateOp::Held { holds, ttl, .. } => match (live, holds) {
                    (None, false) => done(None),
                    (None, true) => {
                        let since = Value::Int(i64::try_from(now.ms).unwrap_or(i64::MAX));
                        write!(Write::SetOnce(&since), ttl, NUMBER_BYTES, |_| reply(Some(
                            Value::Int(0)
                        )))
                    }
                    (
                        Some((
                            Data::First {
                                value: Value::Int(since),
                                ..
                            },
                            _,
                        )),
                        true,
                    ) => {
                        let for_secs = seconds(u64::try_from(*since).unwrap_or(0), now.ms);
                        if ttl.is_none() {
                            return Ok(done(Some(for_secs)));
                        }
                        // Only the expiry moves: the key holds its first time. (Were
                        // it to expire in this very instant, this is the time it
                        // starts again from.)
                        let again = Value::Int(i64::try_from(now.ms).unwrap_or(i64::MAX));
                        write!(Write::SetOnce(&again), ttl, NUMBER_BYTES, |_| reply(Some(
                            for_secs
                        )))
                    }
                    (Some((Data::First { .. }, _)), true) => {
                        return Err(ns.refuse(
                            Refusal::WrongType,
                            "the key holds a value kv_set_once stored, not the time kv_held \
                             records"
                                .to_string(),
                        ));
                    }
                    (Some((Data::First { .. }, _)), false) => {
                        write!(Write::Delete, None, 0, |_| reply(None))
                    }
                    (Some((held, _)), _) => return Err(ns.wrong_type(held, "first")),
                },
                StateOp::Changed { value, ttl, .. } => {
                    // Compared as it would be stored: as every node will hold it.
                    let stored = normal(value.value)
                        .map_err(|e| ns.refuse(Refusal::Other, e.to_string()))?;
                    let changed = match live {
                        None => true,
                        Some((Data::Lww { value: held, .. }, _)) => !held.exact_eq(&stored),
                        Some((held, _)) => return Err(ns.wrong_type(held, "lww")),
                    };
                    if !changed && ttl.is_none() {
                        return Ok(done(Some(Value::Bool(false))));
                    }
                    // An expiry travels with its write, so setting one is a write.
                    write!(Write::Set(&stored), ttl, value.bytes, |_| reply(Some(
                        Value::Bool(changed)
                    )))
                }
            }
        };
        if wrote {
            ns.shared.wake.ping();
        }
        Ok(step)
    }

    /// One write to `key` in its shard: the state changes, its room is accounted and
    /// the key takes its place in the orders of change and of forgetting. `carried`
    /// is the size of the value or element the write brings. `lapsed`: what the key
    /// held has expired. `answer` reads the reply off the state the write left.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn write<R>(
        &self,
        handle: &Handle,
        shard: &mut Shard,
        at: usize,
        key: &[u8],
        write: &Write<'_>,
        ttl: Option<u64>,
        carried: usize,
        now: &Now<'_>,
        lapsed: bool,
        answer: impl FnOnce(&KeyState) -> R,
    ) -> Result<Step<R>, StateError> {
        let ns = &*handle.namespace;
        let settings = &handle.settings;
        let base = BASE + key.len();
        let new = !shard.map.contains_key(key);
        if new {
            if ns.shared.brownout.load(Relaxed) {
                return Ok(Step::NeedRoom(Refusal::Brownout));
            }
            // Room for the key and for what this write puts in it.
            let first = base
                + match write {
                    Write::Set(_) | Write::SetOnce(_) => carried,
                    Write::Add(_) => ELEMENT_OVERHEAD + carried,
                    Write::Extreme { value, .. } => written_len(value),
                    Write::Incr(_) => NUMBER_BYTES,
                    Write::HllAdd(_) => SKETCH_OVERHEAD + 32,
                    Write::Delete => 0,
                };
            if let Err(refusal) = ns.reserve(settings, first) {
                // Keys this shard can forget are room, too.
                if !self.purge(ns, shard, now.ms) {
                    return Ok(Step::NeedRoom(refusal));
                }
                if let Err(refusal) = ns.reserve(settings, first) {
                    return Ok(Step::NeedRoom(refusal));
                }
            }
            let key: Arc<[u8]> = Arc::from(key);
            shard.changes = shard.changes.wrapping_add(1);
            shard.map.insert(
                key.clone(),
                Entry {
                    key_hash: stable_hash(&key),
                    key,
                    state: KeyState::default(),
                    bytes: first,
                    hash: 0,
                    changed: 0,
                    due: 0,
                    evented: 0,
                    waiting: 0,
                },
            );
        } else if lapsed {
            // What the key held is kept for its event, where that is owed: it needs
            // room of its own beside what is being written, as a new key would.
            let kept = self.displace(ns, shard, key, now.ms, |bytes| ns.reserve(settings, bytes));
            if let Err(refusal) = kept {
                return Ok(Step::NeedRoom(refusal));
            }
        }
        let Shard {
            map,
            changed,
            due,
            last,
            read_to,
            ..
        } = &mut *shard;
        let Some(entry) = map.get_mut(key) else {
            return Ok(Step::NeedRoom(Refusal::Other));
        };
        let was_live = entry.state.live(now.ms).is_some();
        let elements_before = match entry.state.data() {
            Some(Data::Union(elements)) if was_live => elements.len(),
            _ => 0,
        };
        let moved = match entry
            .state
            .apply(write, ttl, settings.default_ttl_secs, &now.by)
        {
            Ok(moved) => moved,
            Err(refused) => {
                let held = entry.state.data().map(Data::type_name);
                if new {
                    remove(ns, shard, key);
                }
                return Err(Self::refused_write(ns, refused, held, write));
            }
        };
        let reply = answer(&entry.state);
        if !moved {
            return Ok(Step::Done(reply));
        }
        let before = entry.bytes.saturating_sub(base);
        let data_bytes = match entry.state.data() {
            None => 0,
            Some(Data::First { .. }) if was_live => before,
            Some(Data::Lww { .. } | Data::First { .. }) => carried,
            Some(Data::Max(value) | Data::Min(value)) => written_len(value),
            Some(Data::Sum(counts)) => counter_bytes(counts),
            Some(Data::Union(_)) if !was_live => ELEMENT_OVERHEAD + carried,
            Some(Data::Union(elements)) if elements.len() > elements_before => {
                before + ELEMENT_OVERHEAD + carried
            }
            Some(Data::Union(_)) => before,
            Some(Data::Hll(sketch)) => SKETCH_OVERHEAD + sketch.bytes(),
        };
        resize(ns, entry, base + data_bytes);
        entry.hash = 0;
        // A key no link has read past is sent with whatever it holds when a link
        // gets to it: it keeps its place. Otherwise it goes to the end of the order.
        if entry.changed == 0 || entry.changed <= *read_to {
            if entry.changed != 0 {
                changed.remove(&entry.changed);
            }
            *last += 1;
            entry.changed = *last;
            changed.insert(*last, entry.key.clone());
            // `SeqCst`, and before the links are told: see `Wake::ping`.
            self.marks[at].last.store(*last, SeqCst);
        }
        let slot = due_slot(&entry.state, self.window_ms.load(Relaxed), entry.evented);
        set_due(due, entry, slot);
        self.marks[at].mutations.fetch_add(1, Relaxed);
        Ok(Step::Done(reply))
    }

    // -- a key's expiry as an event (D7) --------------------------------------------------

    /// The link to another member registers it: `remote`, over the node's link
    /// `link`.
    pub(super) fn join(&self, link: u64, remote: &str) -> Arc<Peer> {
        let peer = Arc::new(Peer {
            link,
            node: NodeId(remote.to_string()),
            raises: AtomicBool::new(false),
            settled: AtomicBool::new(false),
            swept: AtomicU64::new(0),
        });
        let mut peers = lock(&self.peers);
        peers.retain(|peer| peer.strong_count() > 0);
        peers.push(Arc::downgrade(&peer));
        peer
    }

    /// The members that raise this namespace's expiry events now. Of two links to
    /// one node (an old one not yet gone) the newer counts.
    fn view(&self, ns: &Namespace) -> View {
        let mut peers: Vec<Arc<Peer>> = lock(&self.peers)
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|peer| peer.raises.load(Relaxed) && peer.settled.load(Relaxed))
            .collect();
        peers.sort_by(|a, b| (&a.node.0, b.link).cmp(&(&b.node.0, a.link)));
        peers.dedup_by(|later, first| later.node == first.node);
        let me = NodeId(ns.shared.node().id.to_string());
        View {
            nodes: std::iter::once(me)
                .chain(peers.iter().map(|peer| peer.node.clone()))
                .collect(),
            peers,
            namespace: ns.name.clone(),
        }
    }

    /// The second up to which this node has nothing left to raise, and the count
    /// that is raised when that is worth telling the peers at once.
    pub(super) fn swept(&self) -> (u64, u64) {
        // The count first: it is raised after the second is stored, so a reader
        // that sees the count has the second it stands for, or a later one.
        let announce = self.announce.load(SeqCst);
        (self.swept.load(SeqCst), announce)
    }

    /// A key waits for its event, to be raised by `owner`.
    fn count_waiting(&self, owner: u64) {
        self.waiting.fetch_add(1, Relaxed);
        if owner != 0 {
            self.standing_by.fetch_add(1, Relaxed);
        }
    }

    /// A key that waited no longer does: its expiry has been dealt with.
    fn count_dealt(&self, owner: u64) {
        self.waiting.fetch_sub(1, Relaxed);
        if owner != 0 {
            self.standing_by.fetch_sub(1, Relaxed);
        }
        self.expired.fetch_add(1, Relaxed);
    }

    /// `key` is about to hold something else than what expired in it (a rule writes
    /// it, or a state arrives): what it held is kept for its event, where that is
    /// owed and has not been raised. `reserve` finds the room that takes: the key
    /// then counts twice, as what waits and as what it holds next. `Err`: there is
    /// none, and nothing was changed.
    fn displace(
        &self,
        ns: &Namespace,
        shard: &mut Shard,
        key: &[u8],
        now_ms: u64,
        reserve: impl FnOnce(usize) -> Result<(), Refusal>,
    ) -> Result<(), Refusal> {
        let Some(entry) = shard.map.get_mut(key) else {
            return Ok(());
        };
        if !expired(&entry.state, now_ms) {
            return Ok(());
        }
        let expired_ms = entry.state.expires_ms();
        let bytes = entry.bytes;
        if entry.waiting != 0 {
            // Owed, and noted while the map held it: the note takes what it held.
            reserve(bytes)?;
            let place = std::mem::take(&mut entry.waiting);
            let state = entry.state.clone();
            match shard.waiting_mut(expired_ms, place) {
                Some(waiting) => waiting.body = Some((state, bytes)),
                None => release(ns, bytes),
            }
            return Ok(());
        }
        if entry.evented == expired_ms {
            return Ok(());
        }
        // A remembered idempotency key (D8) raises no event: nothing is kept of it.
        if !ns.shared.expiry_events.load(Relaxed) || remembered(key).is_some() {
            entry.evented = expired_ms;
            self.expired.fetch_add(1, Relaxed);
            return Ok(());
        }
        reserve(bytes)?;
        entry.evented = expired_ms;
        let (key, state) = (entry.key.clone(), entry.state.clone());
        let owner = self.view(ns).owner(&key);
        shard.wait(Waiting {
            key,
            expired_ms,
            owner,
            body: Some((state, bytes)),
        });
        self.count_waiting(owner);
        Ok(())
    }

    /// Look at the keys of `shard` that are due, at most [`PURGE_BATCH`] of them: the
    /// caller holds the shard's lock. A key whose value expired has that expiry
    /// dealt with, once: counted, or, where a rule selects the event, noted as
    /// waiting for it. A key nothing is lost by forgetting leaves the map.
    fn visit(
        &self,
        ns: &Namespace,
        shard: &mut Shard,
        now_ms: u64,
        view: &mut Option<View>,
    ) -> Visited {
        let window_ms = self.window_ms.load(Relaxed);
        let raises = ns.shared.expiry_events.load(Relaxed);
        let mut visited = Visited::default();
        while visited.looked < PURGE_BATCH {
            let is_due = shard
                .due
                .first()
                .is_some_and(|(slot, _)| slot.saturating_mul(1000) <= now_ms);
            let Some((slot, key)) = is_due.then(|| shard.due.pop_first()).flatten() else {
                break;
            };
            visited.looked += 1;
            let Some(entry) = shard.map.get_mut(&*key) else {
                continue;
            };
            if entry.due != slot {
                continue;
            }
            entry.due = 0;
            let expired_ms = entry.state.expires_ms();
            let forgettable = entry.state.forgettable(now_ms, window_ms);
            let owed = expired(&entry.state, now_ms) && entry.evented != expired_ms;
            if owed {
                entry.evented = expired_ms;
            }
            // A remembered idempotency key (D8) raises no event, on any member: it is
            // forgotten as where no rule selects the event, and waits for nothing.
            let events = raises && remembered(&key).is_none();
            if owed && !events {
                self.expired.fetch_add(1, Relaxed);
            }
            if owed && events {
                let owner = view.get_or_insert_with(|| self.view(ns)).owner(&key);
                self.count_waiting(owner);
                let body = if forgettable {
                    // Out of the map, as on every member; its room stays accounted
                    // until its event has been dealt with.
                    detach(shard, &key).map(|entry| (entry.state, entry.bytes))
                } else {
                    None
                };
                let place = shard.wait(Waiting {
                    key: key.clone(),
                    expired_ms,
                    owner,
                    body,
                });
                if forgettable {
                    continue;
                }
                if let Some(entry) = shard.map.get_mut(&*key) {
                    entry.waiting = place;
                }
            } else if forgettable {
                if entry.waiting != 0 {
                    // Its event is still owed: the note takes what it held, and its
                    // room with it.
                    let place = entry.waiting;
                    let noted = entry.evented;
                    let body = detach(shard, &key).map(|entry| (entry.state, entry.bytes));
                    match (shard.waiting_mut(noted, place), body) {
                        (Some(waiting), Some(body)) => waiting.body = Some(body),
                        (_, Some((_, bytes))) => release(ns, bytes),
                        _ => {}
                    }
                } else {
                    remove(ns, shard, &key);
                    visited.removed += 1;
                }
                continue;
            }
            // Not to be forgotten yet (its delete's window is open, or was made
            // longer since): it is due again when it can be.
            if let Some(entry) = shard.map.get_mut(&*key) {
                let slot = due_slot(&entry.state, window_ms, entry.evented);
                set_due(&mut shard.due, entry, slot);
            }
        }
        visited
    }

    /// What `waiting` held, taken out of `shard`: from the note, or from the map
    /// where the key still is as it expired. Its room is given back.
    fn take_waiting(
        ns: &Namespace,
        shard: &mut Shard,
        at: (u64, u64),
        waiting: Waiting,
    ) -> Option<Raised> {
        let state = match waiting.body {
            Some((state, bytes)) => {
                release(ns, bytes);
                Some(state)
            }
            None => shard
                .map
                .get_mut(&*waiting.key)
                .filter(|entry| entry.waiting == at.1)
                .map(|entry| {
                    entry.waiting = 0;
                    entry.state.clone()
                }),
        }?;
        Some(Raised {
            key: waiting.key,
            state,
            expired_ms: waiting.expired_ms,
        })
    }

    /// One pass of the sweep over this copy, apart from handing events over: every
    /// key that is due is looked at ([`visit`](Self::visit)), a batch per hold of a
    /// shard's lock, and the keys that stand by for another member's event are let
    /// go where that member has got past them, or taken over where it is gone.
    /// Without a rule on the event nothing waits, and what did is let go.
    pub(super) fn forget_due(&self, ns: &Namespace) -> Forgotten {
        let now_ms = Self::now_ms(ns);
        let events = ns.shared.expiry_events.load(Relaxed);
        let mut view = events.then(|| self.view(ns));
        // How far the keys that stand by are looked at: as far as any member has
        // got, and all of them when the members are not those of the last pass.
        let mut reach = u64::MAX;
        if let Some(view) = &view {
            let links: Vec<u64> = view.peers.iter().map(|peer| peer.link).collect();
            let mut last = lock(&self.picked_among);
            if *last == links {
                let swept = view.peers.iter().map(|peer| peer.swept.load(Relaxed));
                reach = swept.max().unwrap_or(0);
            }
            *last = links;
        }
        let mut found = Forgotten::default();
        let mut looked = 0;
        for (at, shard) in self.shards.iter().enumerate() {
            let mut guard = lock_shard(shard);
            while looked < SWEEP_KEYS {
                let visited = self.visit(ns, &mut guard, now_ms, &mut view);
                found.removed += visited.removed;
                looked += visited.looked;
                if visited.looked < PURGE_BATCH {
                    break;
                }
                // A rule's call on this shard waits for one batch at the most.
                drop(guard);
                guard = lock_shard(shard);
            }
            if !events && (!guard.held.is_empty() || !guard.standby.is_empty()) {
                self.let_go(ns, &mut guard);
            }
            if let Some(view) = view.as_ref().filter(|_| !guard.standby.is_empty()) {
                drop(guard);
                self.stand_by(ns, shard, view, reach);
                guard = lock_shard(shard);
            }
            if let Some((slot, _)) = guard.due.first() {
                found.next_due = Some(found.next_due.map_or(*slot, |due| due.min(*slot)));
                found.more |= slot.saturating_mul(1000) <= now_ms;
            }
            if let Some(((slot, _), _)) = guard.held.first_key_value() {
                found.held.push((at, *slot));
            }
        }
        found
    }

    /// No rule selects the event any more: what waited in `shard` is let go.
    fn let_go(&self, ns: &Namespace, shard: &mut Shard) {
        let held = std::mem::take(&mut shard.held);
        let standby = std::mem::take(&mut shard.standby);
        for (at, waiting) in held.into_iter().chain(standby) {
            let owner = waiting.owner;
            let _ = Self::take_waiting(ns, shard, at, waiting);
            self.count_dealt(owner);
        }
    }

    /// The keys of `shard` that stand by for another member's event, those that
    /// expired in the second `reach` or before: one whose member has got past it is
    /// let go; one whose member is gone, or no longer raises, is given to the member
    /// picked now, which may be this node. A batch per hold of the lock.
    fn stand_by(&self, ns: &Namespace, shard: &Mutex<Shard>, view: &View, reach: u64) {
        let mut from = (0, 0);
        loop {
            let mut guard = lock_shard(shard);
            let shard = &mut *guard;
            let some: Vec<(u64, u64)> = shard
                .standby
                .range(from..=(reach, u64::MAX))
                .take(PURGE_BATCH)
                .map(|(at, _)| *at)
                .collect();
            for at in &some {
                let Some(waiting) = shard.standby.get_mut(at) else {
                    continue;
                };
                let mut owner = view.peer(waiting.owner);
                if owner.is_none() {
                    let picked = view.owner(&waiting.key);
                    if picked == 0 {
                        // This node's to raise now.
                        if let Some(mut waiting) = shard.standby.remove(at) {
                            waiting.owner = 0;
                            shard.held.insert(*at, waiting);
                            self.standing_by.fetch_sub(1, Relaxed);
                        }
                        continue;
                    }
                    waiting.owner = picked;
                    owner = view.peer(picked);
                }
                if owner.is_some_and(|peer| peer.swept.load(Relaxed) >= at.0) {
                    if let Some(waiting) = shard.standby.remove(at) {
                        let owner = waiting.owner;
                        let _ = Self::take_waiting(ns, shard, *at, waiting);
                        self.count_dealt(owner);
                    }
                }
            }
            match some.last() {
                Some(last) if some.len() == PURGE_BATCH => from = (last.0, last.1 + 1),
                _ => return,
            }
        }
    }

    /// Take out of shard `at` the keys this node is to raise the event of that
    /// expired in the second `slot` or before, earliest first, at most `max`, giving
    /// each to `each`. Returns the second the shard's next such key expired in.
    pub(super) fn pop_held(
        &self,
        ns: &Namespace,
        at: usize,
        slot: u64,
        max: usize,
        each: &mut dyn FnMut(Raised),
    ) -> Option<u64> {
        let mut guard = lock_shard(&self.shards[at]);
        let shard = &mut *guard;
        let mut taken = 0;
        while taken < max {
            let Some(first) = shard
                .held
                .first_entry()
                .filter(|first| first.key().0 <= slot)
            else {
                break;
            };
            let (place, waiting) = first.remove_entry();
            self.count_dealt(0);
            if let Some(raised) = Self::take_waiting(ns, shard, place, waiting) {
                each(raised);
                taken += 1;
            }
        }
        shard.held.first_key_value().map(|((slot, _), _)| *slot)
    }

    /// After a pass of the sweep: this node has nothing left to raise that expired
    /// in the second `swept` or before. `raised`: whether the pass handed any event
    /// of this namespace over. Whether the peers are to be told at once: they are
    /// when the pass got somewhere, and when this node took keys over and is behind
    /// where it said it was.
    pub(super) fn note_swept(&self, swept: u64, raised: bool) -> bool {
        let before = self.swept.swap(swept, SeqCst);
        let tell = swept != before && (raised || swept < before);
        if tell {
            self.announce.fetch_add(1, SeqCst);
        }
        tell
    }

    /// Take `key` out if its value has expired and its event has not been raised,
    /// wherever it is: for a replayed sequence, which is one node and raises every
    /// key's event itself.
    pub(super) fn take_expired(&self, ns: &Namespace, key: &[u8]) -> Option<Raised> {
        let now_ms = Self::now_ms(ns);
        let window_ms = self.window_ms.load(Relaxed);
        let mut guard = lock_shard(&self.shards[ns.shard_of(key)]);
        let shard = &mut *guard;
        let noted = shard
            .held
            .iter()
            .chain(shard.standby.iter())
            .find(|(_, waiting)| &*waiting.key == key)
            .map(|(at, waiting)| (*at, waiting.owner));
        if let Some((at, owner)) = noted {
            let waiting = if owner == 0 {
                shard.held.remove(&at)
            } else {
                shard.standby.remove(&at)
            }?;
            self.count_dealt(owner);
            return Self::take_waiting(ns, shard, at, waiting);
        }
        let entry = shard.map.get_mut(key)?;
        let expired_ms = entry.state.expires_ms();
        if !expired(&entry.state, now_ms) || entry.evented == expired_ms {
            return None;
        }
        entry.evented = expired_ms;
        self.expired.fetch_add(1, Relaxed);
        let raised = Raised {
            key: entry.key.clone(),
            state: entry.state.clone(),
            expired_ms,
        };
        if entry.state.forgettable(now_ms, window_ms) {
            remove(ns, shard, key);
        } else {
            let slot = due_slot(&entry.state, window_ms, expired_ms);
            set_due(&mut shard.due, entry, slot);
        }
        Some(raised)
    }

    /// A write its key refused, as the rule's error.
    fn refused_write(
        ns: &Namespace,
        refused: WriteError,
        held: Option<&'static str>,
        write: &Write<'_>,
    ) -> StateError {
        let wanted = match write {
            Write::Set(_) | Write::Delete => "lww",
            Write::SetOnce(_) => "first",
            Write::Extreme { largest: true, .. } => "max",
            Write::Extreme { largest: false, .. } => "min",
            Write::Incr(_) => "sum",
            Write::Add(_) => "union",
            Write::HllAdd(_) => "hll",
        };
        match refused {
            WriteError::WrongType { held } => ns.wrong_type_named(held, wanted),
            WriteError::SetFull => ns.refuse(
                Refusal::Other,
                format!("the set holds {MAX_SET_ELEMENTS} elements, the most a set may"),
            ),
            WriteError::Counter(CountsError::Overflow) => {
                ns.refuse(Refusal::Other, "the counter would pass 64 bits".to_string())
            }
            WriteError::Counter(CountsError::TooManyNodes | CountsError::NodeId) => ns.refuse(
                Refusal::Other,
                "the counter holds counts of as many nodes as it may".to_string(),
            ),
            WriteError::Value(why) => ns.refuse(
                Refusal::Other,
                format!(
                    "{why}{}",
                    held.map_or(String::new(), |held| format!(" (the key holds `{held}`)"))
                ),
            ),
        }
    }

    // -- forgetting ---------------------------------------------------------------------

    /// Forget keys of `shard` that nothing is lost by forgetting, at most
    /// [`PURGE_BATCH`] of them: the caller holds the shard's lock. Whether any went
    /// and gave its room back (one that waits for its event keeps its room).
    fn purge(&self, ns: &Namespace, shard: &mut Shard, now_ms: u64) -> bool {
        self.visit(ns, shard, now_ms, &mut None).removed > 0
    }

    /// Look through the shards for keys to forget, one lock at a time, and stop at
    /// the first that had some. A look that finds none anywhere is not repeated
    /// within a second.
    fn purge_all(&self, handle: &Handle) -> bool {
        let ns = &*handle.namespace;
        let wall_ms = ns.shared.clock.now_ms();
        let last = ns.purged_ms.load(Relaxed);
        if last != 0 && wall_ms < last.saturating_add(PURGE_EVERY_MS) {
            return false;
        }
        let now_ms = ns.shared.node().clock.physical_ms(wall_ms);
        for shard in &self.shards {
            if self.purge(ns, &mut lock_shard(shard), now_ms) {
                return true;
            }
        }
        ns.purged_ms.store(wall_ms.max(1), Relaxed);
        false
    }

    // -- what a peer sent ---------------------------------------------------------------

    /// Merge the state `bytes` of `key`, as another node sent it. `Ok`: whether this
    /// copy changed. `Err`: why it was not taken; this copy is as it was, and the
    /// refusal is counted.
    pub(super) fn merge(&self, ns: &Namespace, key: &[u8], bytes: &[u8]) -> Result<bool, Refused> {
        let merged = self.merge_inner(ns, key, bytes);
        match merged {
            Ok(true) => {
                self.merged.fetch_add(1, Relaxed);
                self.marks[ns.shard_of(key)].mutations.fetch_add(1, Relaxed);
            }
            Ok(false) => {}
            Err(reason) => self.refuse(reason),
        }
        merged
    }

    fn merge_inner(&self, ns: &Namespace, key: &[u8], bytes: &[u8]) -> Result<bool, Refused> {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(Refused::Malformed);
        }
        let theirs = KeyState::decode(bytes).map_err(|_| Refused::Malformed)?;
        let node = ns.shared.node();
        let wall_ms = ns.shared.clock.now_ms();
        // A stamp too far ahead is not followed (D6): the state waits for this node's
        // clock, and the next repair offers it again.
        node.clock
            .observe(theirs.latest(), wall_ms)
            .map_err(|_| Refused::ClockSkew)?;
        let now_ms = node.clock.physical_ms(wall_ms);
        let window_ms = self.window_ms.load(Relaxed);
        let key_hash = stable_hash(key);
        let their_hash = entry_hash(key_hash, bytes);
        let base = BASE + key.len();
        let mut guard = lock_shard(&self.shards[ns.shard_of(key)]);
        let shard = &mut *guard;
        let Some(entry) = shard.map.get_mut(key) else {
            // Nothing is held of it: a state that could be forgotten at once is not
            // taken up at all.
            if theirs.forgettable(now_ms, window_ms) {
                return Ok(false);
            }
            if ns.shared.brownout.load(Relaxed) {
                return Err(Refused::Brownout);
            }
            let bytes = base + theirs.data().map_or(0, weigh);
            let (max_keys, max_bytes) = (self.max_keys.load(Relaxed), self.max_bytes.load(Relaxed));
            if let Err(refusal) = ns.reserve_within(max_keys, max_bytes, bytes) {
                let again = if self.purge(ns, shard, now_ms) {
                    ns.reserve_within(max_keys, max_bytes, bytes)
                } else {
                    Err(refusal)
                };
                match again {
                    Ok(()) => {}
                    Err(Refusal::StoreFull) => return Err(Refused::StoreFull),
                    Err(_) => return Err(Refused::NamespaceFull),
                }
            }
            let key: Arc<[u8]> = Arc::from(key);
            let mut entry = Entry {
                key: key.clone(),
                state: theirs,
                bytes,
                key_hash,
                // This copy is theirs exactly, and a state has one encoding.
                hash: their_hash,
                changed: 0,
                due: 0,
                evented: 0,
                waiting: 0,
            };
            // What had already expired when it arrived (a key remembered for its
            // delete, sent to a node that is being filled) expired before this node
            // held it: no event is owed for it here.
            if expired(&entry.state, now_ms) {
                entry.evented = entry.state.expires_ms();
            }
            let slot = due_slot(&entry.state, window_ms, entry.evented);
            set_due(&mut shard.due, &mut entry, slot);
            shard.changes = shard.changes.wrapping_add(1);
            shard.map.insert(key, entry);
            return Ok(true);
        };
        if entry.hash == their_hash {
            return Ok(false);
        }
        // Merged aside, so that a state that does not fit leaves this one as it was.
        let mut merged = entry.state.clone();
        merged.merge(&theirs);
        // What this copy held had expired here before this state arrived, and the
        // state would undo that (a later expiry, another value, a delete): the expiry
        // stands, as this node saw it, and what the key held is kept for its event.
        if expired(&entry.state, now_ms)
            && (merged.data().is_none() || merged.expires_ms() != entry.state.expires_ms())
        {
            let (max_keys, max_bytes) = (self.max_keys.load(Relaxed), self.max_bytes.load(Relaxed));
            self.displace(ns, shard, key, now_ms, |bytes| {
                ns.reserve_within(max_keys, max_bytes, bytes)
            })
            .map_err(|refusal| match refusal {
                Refusal::StoreFull => Refused::StoreFull,
                _ => Refused::NamespaceFull,
            })?;
        }
        let Some(entry) = shard.map.get_mut(key) else {
            return Ok(false);
        };
        let bytes = base + merged.data().map_or(0, weigh);
        if bytes > entry.bytes {
            let more = wide(bytes - entry.bytes);
            if !add_within(&ns.bytes, more, self.max_bytes.load(Relaxed)) {
                return Err(Refused::NamespaceFull);
            }
            if !add_within(&ns.shared.bytes, more, ns.shared.max_bytes) {
                ns.bytes.fetch_sub(more, Relaxed);
                return Err(Refused::StoreFull);
            }
            entry.bytes = bytes;
        } else {
            resize(ns, entry, bytes);
        }
        entry.state = merged;
        entry.hash = 0;
        let slot = due_slot(&entry.state, window_ms, entry.evented);
        set_due(&mut shard.due, entry, slot);
        Ok(true)
    }

    // -- what a link sends --------------------------------------------------------------

    /// The link `link` wants to start a repair of this copy. One at a time: a node
    /// that joins has a link to every member, and each would otherwise send it the
    /// whole namespace. The others start theirs when this one is done, and by then
    /// find little to send.
    pub(super) fn repair_begin(&self, link: u64, now: Instant) -> Begin {
        let mut repairing = lock(&self.repairing);
        match *repairing {
            Some((holder, until)) if now < until => {
                if holder == link {
                    Begin::Mine
                } else {
                    Begin::Busy
                }
            }
            _ => {
                *repairing = Some((link, now + REPAIR_HOLD));
                Begin::Started
            }
        }
    }

    /// The link `link` is still sending what its repair has to send.
    pub(super) fn repair_keep(&self, link: u64, now: Instant) {
        *lock(&self.repairing) = Some((link, now + REPAIR_HOLD));
    }

    /// The link `link` has sent what its repair had to send.
    pub(super) fn repair_end(&self, link: u64) {
        let mut repairing = lock(&self.repairing);
        if matches!(*repairing, Some((holder, _)) if holder == link) {
            *repairing = None;
        }
    }

    /// A new link's place in each shard: at its end. What the copies differ in from
    /// before is found by comparing digests, not by sending everything.
    pub(super) fn link(&self, wall_ms: u64) -> Arc<Cursors> {
        let cursors = Arc::new(Cursors {
            at: (0..SHARDS).map(|_| AtomicU64::new(0)).collect(),
            caught_up_ms: AtomicU64::new(wall_ms),
        });
        for (shard, cursor) in self.shards.iter().zip(cursors.at.iter()) {
            let mut shard = lock_shard(shard);
            cursor.store(shard.last, Relaxed);
            shard.read_to = shard.last;
        }
        let mut links = lock(&self.links);
        links.retain(|link| link.strong_count() > 0);
        links.push(Arc::downgrade(&cursors));
        cursors
    }

    /// Whether anything was changed that `cursors`' link has not read. A link that
    /// has read everything is noted as caught up at `wall_ms`: what its lag is
    /// measured from.
    pub(super) fn has_changes(&self, cursors: &Cursors, wall_ms: u64) -> bool {
        let ahead = self
            .marks
            .iter()
            .zip(cursors.at.iter())
            .any(|(marks, cursor)| marks.last.load(SeqCst) > cursor.load(Relaxed));
        if !ahead {
            cursors.caught_up_ms.store(wall_ms, Relaxed);
        }
        ahead
    }

    /// Append to `out` the keys changed since `cursors`, with their states, until
    /// `out` holds `budget` bytes, moving `cursors` past them. Starts at shard
    /// `*from` and leaves there the shard to go on with.
    pub(super) fn take_changed(
        &self,
        cursors: &Cursors,
        from: &mut usize,
        budget: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let mut bytes = 0;
        // Round the shards, a few keys from each at a time, until the frame is full
        // or a round finds nothing.
        loop {
            let mut found = false;
            for step in 0..SHARDS {
                let at = (*from + step) % SHARDS;
                let cursor = cursors.at[at].load(Relaxed);
                if self.marks[at].last.load(SeqCst) <= cursor {
                    continue;
                }
                let mut guard = lock_shard(&self.shards[at]);
                let Shard {
                    map,
                    changed,
                    last,
                    read_to,
                    ..
                } = &mut *guard;
                // To the end of the order, unless this hold or the frame ends first.
                let mut read = *last;
                let mut held = 0;
                for (number, key) in changed.range(cursor.saturating_add(1)..) {
                    let Some(entry) = map.get_mut(&**key) else {
                        continue;
                    };
                    let state = entry.state.encoded();
                    entry.hash = entry_hash(entry.key_hash, &state);
                    bytes += key.len() + state.len();
                    out.push((key.to_vec(), state));
                    held += 1;
                    if bytes >= budget || held >= HELD_KEYS {
                        read = *number;
                        break;
                    }
                }
                found |= held > 0;
                cursors.at[at].store(read, Relaxed);
                *read_to = (*read_to).max(read);
                if bytes >= budget {
                    *from = at;
                    return;
                }
            }
            if !found {
                return;
            }
        }
    }

    /// The keys of shard `at` that are in one of `parts` and are worth sending.
    pub(super) fn keys_in_parts(
        &self,
        ns: &Namespace,
        at: usize,
        parts: &[u64; PART_WORDS],
    ) -> Vec<Arc<[u8]>> {
        let now_ms = ns.shared.node().clock.physical_ms(ns.shared.clock.now_ms());
        let window_ms = self.window_ms.load(Relaxed);
        lock_shard(&self.shards[at])
            .map
            .values()
            .filter(|entry| {
                let part = part_of(entry.key_hash);
                parts[part / 64] & (1 << (part % 64)) != 0
                    && !entry.state.forgettable(now_ms, window_ms)
            })
            .map(|entry| entry.key.clone())
            .collect()
    }

    /// Append to `out` the states of `keys`, which are of shard `at`, until `out`
    /// holds `budget` bytes. How many of `keys` were dealt with.
    pub(super) fn take_keys(
        &self,
        at: usize,
        keys: &[Arc<[u8]>],
        budget: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> usize {
        let mut bytes = 0;
        let mut taken = 0;
        // A few keys under each hold of the lock: a rule's call on this shard waits
        // for one hold at most.
        for some in keys.chunks(HELD_KEYS) {
            let mut guard = lock_shard(&self.shards[at]);
            for key in some {
                taken += 1;
                let Some(entry) = guard.map.get_mut(&**key) else {
                    continue;
                };
                let state = entry.state.encoded();
                entry.hash = entry_hash(entry.key_hash, &state);
                bytes += key.len() + state.len();
                out.push((key.to_vec(), state));
                if bytes >= budget {
                    return taken;
                }
            }
        }
        taken
    }

    /// The summary of this copy: reused while nothing changed and nothing it covers
    /// can have been forgotten; otherwise made by looking at every key once, a shard
    /// at a time. Keys that could be forgotten are not in it, whether or not they
    /// have been yet, so that two nodes forgetting at slightly different moments
    /// still agree.
    pub(super) fn digest(&self, ns: &Namespace) -> Arc<Digest> {
        let now_ms = ns.shared.node().clock.physical_ms(ns.shared.clock.now_ms());
        let mutations: u64 = self
            .marks
            .iter()
            .map(|marks| marks.mutations.load(Relaxed))
            .sum();
        if let Some(digest) = lock(&self.digest).as_ref() {
            if digest.mutations == mutations && now_ms < digest.good_until_ms {
                return digest.clone();
            }
        }
        let window_ms = self.window_ms.load(Relaxed);
        let mut parts = vec![0u64; STATE_PARTS].into_boxed_slice();
        let mut good_until_ms = u64::MAX;
        for shard in &self.shards {
            let mut guard = lock_shard(shard);
            self.purge(ns, &mut guard, now_ms);
            for entry in guard.map.values_mut() {
                if entry.state.forgettable(now_ms, window_ms) {
                    continue;
                }
                if entry.hash == 0 {
                    entry.hash = entry_hash(entry.key_hash, &entry.state.encoded());
                }
                parts[part_of(entry.key_hash)] ^= entry.hash;
            }
            if let Some((slot, _)) = guard.due.first() {
                good_until_ms = good_until_ms.min(slot.saturating_mul(1000));
            }
        }
        let mut bytes = Vec::with_capacity(STATE_PARTS * 8);
        for part in &parts {
            bytes.extend_from_slice(&part.to_le_bytes());
        }
        let digest = Arc::new(Digest {
            root: stable_hash(&bytes),
            parts,
            mutations,
            good_until_ms: good_until_ms.max(now_ms.saturating_add(1)),
        });
        *lock(&self.digest) = Some(digest.clone());
        digest
    }

    /// Append every key this copy holds a value of now ([`Store::held`](super::Store::held)).
    pub(super) fn held(&self, ns: &Namespace, out: &mut Vec<super::HeldKey>) {
        let now_ms = ns.shared.node().clock.physical_ms(ns.shared.clock.now_ms());
        for shard in &self.shards {
            let guard = lock_shard(shard);
            out.extend(guard.map.values().filter_map(|entry| {
                let data = entry.state.live(now_ms)?;
                Some(super::HeldKey {
                    namespace: ns.name.clone(),
                    key: entry.key.clone(),
                    kind: data.type_name(),
                    value: data.read(),
                    expires_ms: entry.state.expires_ms(),
                })
            }));
        }
    }

    // -- what an operator sees (D10) ---------------------------------------------------

    /// The time this copy judges expiries by.
    pub(super) fn now_ms(ns: &Namespace) -> u64 {
        ns.shared.node().clock.physical_ms(ns.shared.clock.now_ms())
    }

    /// One hold of shard `at`'s lock for an operator's listing: `each` is shown the
    /// next `batch` keys of the map after the first `seen`, those that hold a value
    /// now (a deleted key this copy still remembers is not a key to list). How many
    /// were looked at; `None` when the map changed since the hold that set `changes`.
    pub(super) fn look(
        &self,
        at: usize,
        seen: usize,
        batch: usize,
        changes: &mut Option<u64>,
        now_ms: u64,
        each: &mut dyn FnMut(super::admin::Seen<'_>),
    ) -> Option<usize> {
        let shard = lock_shard(&self.shards[at]);
        if *changes.get_or_insert(shard.changes) != shard.changes {
            return None;
        }
        let mut looked = 0;
        for entry in shard.map.values().skip(seen).take(batch) {
            looked += 1;
            if let Some(data) = entry.state.live(now_ms) {
                each(super::admin::Seen {
                    key: &entry.key,
                    type_name: data.type_name(),
                    bytes: entry.bytes,
                    expires_ms: entry.state.expires_ms(),
                });
            }
        }
        Some(looked)
    }

    /// What `key` holds now, for an operator's read: its type, what `kv_get` would
    /// read, its expiry, and what it is accounted as. Not a write, so nothing is
    /// stamped or sent.
    pub(super) fn peek(
        &self,
        ns: &Namespace,
        key: &[u8],
    ) -> Option<(&'static str, Value, u64, usize)> {
        let now_ms = Self::now_ms(ns);
        let shard = lock_shard(&self.shards[ns.shard_of(key)]);
        let entry = shard.map.get(key)?;
        let data = entry.state.live(now_ms)?;
        Some((
            data.type_name(),
            data.read(),
            entry.state.expires_ms(),
            entry.bytes,
        ))
    }

    /// The rules' calls answered, by operation; keys forgotten whose value had
    /// expired; and keys that can be forgotten now and are still held.
    pub(super) fn counts(&self, ns: &Namespace) -> ([u64; super::OPS.len()], u64, u64) {
        let now_ms = Self::now_ms(ns);
        let mut ops = [0; super::OPS.len()];
        let mut backlog = 0u64;
        for (shard, marks) in self.shards.iter().zip(self.marks.iter()) {
            for (total, n) in ops.iter_mut().zip(&marks.ops) {
                *total += n.load(Relaxed);
            }
            // The order of forgetting is by second: a few entries a hold.
            let shard = lock_shard(shard);
            backlog += wide(
                shard
                    .due
                    .iter()
                    .take(COUNTED * 16)
                    .take_while(|(slot, _)| slot.saturating_mul(1000) <= now_ms)
                    .count(),
            );
        }
        (
            ops,
            self.expired.load(Relaxed),
            backlog + self.waiting.load(Relaxed),
        )
    }

    /// How replication stands, the wall clock reading `wall_ms`.
    pub(super) fn stats(&self, ns: &Namespace, wall_ms: u64) -> Replication {
        let links: Vec<Arc<Cursors>> = lock(&self.links).iter().filter_map(Weak::upgrade).collect();
        let mut stats = Replication {
            links: links.len(),
            merged: self.merged.load(Relaxed),
            expiry_members: if ns.shared.expiry_events.load(Relaxed) {
                self.view(ns).nodes.len()
            } else {
                0
            },
            ..Replication::default()
        };
        for (count, refused) in stats.refused.iter_mut().zip(&self.refused) {
            *count = refused.load(Relaxed);
        }
        for cursors in &links {
            let mut pending = 0u64;
            for (at, cursor) in cursors.at.iter().enumerate() {
                let cursor = cursor.load(Relaxed);
                if self.marks[at].last.load(Relaxed) <= cursor {
                    continue;
                }
                let shard = lock_shard(&self.shards[at]);
                let ahead = shard.changed.range(cursor.saturating_add(1)..);
                // Counted while that is cheap; past it, a shard's unread changes are
                // at most the keys it has changed.
                let counted = ahead.take(COUNTED + 1).count();
                pending += wide(if counted > COUNTED {
                    shard.changed.len()
                } else {
                    counted
                });
            }
            if pending > 0 {
                let since = cursors.caught_up_ms.load(Relaxed);
                stats.lag_ms = stats.lag_ms.max(wall_ms.saturating_sub(since));
            }
            stats.pending = stats.pending.max(pending);
        }
        stats
    }

    /// The states this copy holds, encoded, in key order: what two copies that
    /// converged have the same of. Tombstones included; what could be forgotten not.
    #[cfg(test)]
    pub(super) fn snapshot(&self, ns: &Namespace) -> Vec<(Vec<u8>, Vec<u8>)> {
        let now_ms = ns.shared.node().clock.physical_ms(ns.shared.clock.now_ms());
        let window_ms = self.window_ms.load(Relaxed);
        let mut all: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for shard in &self.shards {
            let guard = lock_shard(shard);
            all.extend(
                guard
                    .map
                    .values()
                    .filter(|entry| !entry.state.forgettable(now_ms, window_ms))
                    .map(|entry| (entry.key.to_vec(), entry.state.encoded())),
            );
        }
        all.sort();
        all
    }
}
