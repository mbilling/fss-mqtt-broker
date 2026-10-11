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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{
    AtomicU64, AtomicUsize,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use mqtt_cluster::peer::STATE_PARTS;
use mqtt_rules::crdt::{
    normal, stable_hash, Counts, CountsError, Data, KeyState, Write, WriteError, Writer,
};
use mqtt_rules::state::{
    written_len, NamespaceSettings, StateError, StateOp, StateReply, MAX_KEY_BYTES,
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
    /// Keys forgotten whose value had expired (D10's `expired`): not the deleted
    /// ones, whose tombstone's window ran out.
    expired: AtomicU64,
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

/// The second from which `state` is forgettable if nothing changes; `0`: never.
fn due_slot(state: &KeyState, window_ms: u64) -> u64 {
    let window_over = state.deleted().ms().saturating_add(window_ms);
    let at = match state.data() {
        None => window_over,
        Some(_) if state.expires_ms() == 0 => return 0,
        Some(_) => state.expires_ms().max(window_over),
    };
    at.div_ceil(1000).max(1)
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

/// Take `key` out of `shard` and give its room back.
fn remove(ns: &Namespace, shard: &mut Shard, key: &[u8]) {
    let Some(entry) = shard.map.remove(key) else {
        return;
    };
    shard.changes = shard.changes.wrapping_add(1);
    if entry.changed != 0 {
        shard.changed.remove(&entry.changed);
    }
    if entry.due != 0 {
        shard.due.remove(&(entry.due, entry.key.clone()));
    }
    ns.keys.fetch_sub(1, Relaxed);
    ns.bytes.fetch_sub(wide(entry.bytes), Relaxed);
    ns.shared.bytes.fetch_sub(wide(entry.bytes), Relaxed);
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
        let mut made_room = false;
        loop {
            let refusal = match self.attempt(handle, op)? {
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
            // What the key holds now, and what that is accounted as.
            let live = shard.map.get(key).and_then(|entry| {
                let data = entry.state.live(now.ms)?;
                Some((data, entry.bytes.saturating_sub(BASE + key.len())))
            });
            macro_rules! write {
                ($write:expr, $ttl:expr, $carried:expr, $reply:expr) => {{
                    let write = $write;
                    let step =
                        self.write(handle, shard, at, key, &write, $ttl, $carried, &now, $reply)?;
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
    /// is the size of the value or element the write brings. `answer` reads the reply
    /// off the state the write left.
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
                },
            );
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
        let slot = due_slot(&entry.state, self.window_ms.load(Relaxed));
        set_due(due, entry, slot);
        self.marks[at].mutations.fetch_add(1, Relaxed);
        Ok(Step::Done(reply))
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
    /// [`PURGE_BATCH`] of them: the caller holds the shard's lock. Whether any went.
    fn purge(&self, ns: &Namespace, shard: &mut Shard, now_ms: u64) -> bool {
        let window_ms = self.window_ms.load(Relaxed);
        let mut any = false;
        for _ in 0..PURGE_BATCH {
            let is_due = shard
                .due
                .first()
                .is_some_and(|(slot, _)| slot.saturating_mul(1000) <= now_ms);
            let Some((slot, key)) = is_due.then(|| shard.due.pop_first()).flatten() else {
                break;
            };
            let Some(entry) = shard.map.get_mut(&*key) else {
                continue;
            };
            if entry.due != slot {
                continue;
            }
            entry.due = 0;
            if entry.state.forgettable(now_ms, window_ms) {
                if entry.state.data().is_some() {
                    self.expired.fetch_add(1, Relaxed);
                }
                remove(ns, shard, &key);
                any = true;
            } else {
                // The window was made longer since: it is due later.
                let slot = due_slot(&entry.state, window_ms);
                set_due(&mut shard.due, entry, slot);
            }
        }
        any
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
            };
            let slot = due_slot(&entry.state, window_ms);
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
        let slot = due_slot(&entry.state, window_ms);
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
        each: &mut dyn FnMut(&Arc<[u8]>, &'static str),
    ) -> Option<usize> {
        let shard = lock_shard(&self.shards[at]);
        if *changes.get_or_insert(shard.changes) != shard.changes {
            return None;
        }
        let mut looked = 0;
        for entry in shard.map.values().skip(seen).take(batch) {
            looked += 1;
            if let Some(data) = entry.state.live(now_ms) {
                each(&entry.key, data.type_name());
            }
        }
        Some(looked)
    }

    /// What `key` holds now, for an operator's read: its type, what `kv_get` would
    /// read, and its expiry. Not a write, so nothing is stamped or sent.
    pub(super) fn peek(&self, ns: &Namespace, key: &[u8]) -> Option<(&'static str, Value, u64)> {
        let now_ms = Self::now_ms(ns);
        let shard = lock_shard(&self.shards[ns.shard_of(key)]);
        let entry = shard.map.get(key)?;
        let data = entry.state.live(now_ms)?;
        Some((data.type_name(), data.read(), entry.state.expires_ms()))
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
        (ops, self.expired.load(Relaxed), backlog)
    }

    /// How replication stands, the wall clock reading `wall_ms`.
    pub(super) fn stats(&self, wall_ms: u64) -> Replication {
        let links: Vec<Arc<Cursors>> = lock(&self.links).iter().filter_map(Weak::upgrade).collect();
        let mut stats = Replication {
            links: links.len(),
            merged: self.merged.load(Relaxed),
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
