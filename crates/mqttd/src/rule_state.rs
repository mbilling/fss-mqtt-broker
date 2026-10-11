//! The store rules keep their state in ([ADR 0085](../../../docs/adr/0085-rule-state-store.md)):
//! namespaces of small keyed values, in memory. A `local` namespace lives on this node
//! only and is what this file is about; an `eventual` one is held by every member of
//! the cluster and is kept in [`replica`], sent and repaired by [`link`].
//!
//! `mqtt-rules` defines what a state call is ([`StateOp`]) and checks its arguments;
//! this is the [`StateStore`] the broker hands it. One [`Store`] serves the running
//! rules. Every dry run (`mqttd --rule-test`, `--check-rules`, the admin API's rule
//! test) gets a store of its own that is dropped afterwards ([`Store::scratch`]), so no
//! test touches the running one (D11).
//!
//! # Where a call runs, and what it may take
//!
//! A state call runs on the task that evaluates the rule (a publisher's connection
//! task, for its publishes), synchronously, and never waits (D14). Many connection
//! tasks on several worker threads share one store, so:
//!
//! * A namespace is [`SHARDS`] maps, each behind its own mutex; a key's hash picks its
//!   map. A call takes **one** shard's lock for **one** map operation and releases it:
//!   nothing is awaited, sent, logged or evaluated under it, and no call holds two
//!   shard locks. Two calls wait for each other only when their keys share a shard.
//! * Counts and bytes are atomics, per namespace and for the store. A new key
//!   *reserves* its room with a compare-and-swap that never passes the bound, so the
//!   bounds hold exactly under any interleaving and nothing takes a store-wide lock.
//! * An `lru` namespace keeps each shard's keys in order of use. Making room looks at
//!   every shard's oldest key, one lock at a time, and removes the oldest of those:
//!   the least recently used key of the namespace, at the price of [`SHARDS`] short
//!   locks per eviction.
//!
//! The measured cost of a call, alone and under contention, is in `docs/RULES.md`
//! (`cargo bench -p mqttd --bench rule_state`).
//!
//! # Bounds
//!
//! A key that does not exist yet is refused, and counted by reason, when the broker
//! is over its memory watermark ([`BROWNOUT`]), when the namespace holds `max_keys`
//! keys or `max_bytes` bytes ([`NAMESPACE_FULL`]), or when the store holds
//! `rules.state_max_bytes` across its namespaces ([`STORE_FULL`]). With
//! `eviction = "lru"` the namespace's least recently used key makes room instead. A
//! write to a key that exists is never refused for room: it is bounded per key (a
//! value of 4 KiB, a set of 1,024 elements of 256 bytes, a sketch of 12 KiB).
//!
//! # Expiry
//!
//! A key past its expiry reads as absent from that moment. It is removed by the call
//! that finds it, and when room is needed: each shard keeps its keys in order of
//! expiry (to the second), so removing expired ones costs what there is to remove, and
//! a call removes a bounded number of them however many have expired.
//! The sweep that removes them on time and raises `$events/state/expired` is D7's, a
//! later task, and will walk the same order.
//!
//! # A namespace's life
//!
//! The store holds its namespaces weakly; a loaded rule set holds them. A namespace a
//! reload still declares (or a rule that kept its id still uses) is the same one,
//! with its keys, and the settings are the new set's: changed bounds apply to the
//! calls of the rules that were loaded with them. A namespace no running set holds
//! any more is dropped with its keys when the last set that held it is (D1).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{
    AtomicBool, AtomicU64, AtomicUsize,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use mqtt_rules::crdt::{Counts, CountsError, Data, Elements, Hlc, Hll, SetFull, Stamp};
use mqtt_rules::state::{
    Consistency, Eviction, NamespaceSettings, StateError, StateNamespace, StateOp, StateReply,
    StateStore, Stored, BROWNOUT, MAX_SET_ELEMENTS, NAMESPACE_FULL, STORE_FULL, WRONG_TYPE,
};
use mqtt_rules::Value;

mod link;
mod replica;

pub use link::StateLink;
pub use replica::{Refused, Replication};

/// The maps a namespace's keys are spread over, each behind its own lock.
pub const SHARDS: usize = 64;

/// What a key is accounted beyond its own bytes and its value's: the map's slot, the
/// entry, the key's allocation and its places in the orders of expiry and of use.
/// Without it a namespace of very small values would hold several times its
/// `max_bytes`.
pub const ENTRY_OVERHEAD: usize = 128;

/// What an element of a set is accounted beyond its own bytes.
const ELEMENT_OVERHEAD: usize = 32;

/// What a sketch is accounted beyond its registers: its histogram and the list's
/// header.
const SKETCH_OVERHEAD: usize = 128;

/// What a number read out of the store is charged to the message's build budget: a
/// counter's count, a sketch's estimate.
const NUMBER_BYTES: usize = 8;

/// Whose counts a `local` namespace's counters keep: this node's, and nothing tells
/// nodes apart until a namespace is replicated.
const LOCAL_NODE: &str = "";

/// The stamp of a write in a `local` namespace: none. Its keys are never merged with
/// another node's, so nothing orders its writes but the shard's lock, and the publish
/// path does not pay for a clock.
const UNSTAMPED: Stamp = Stamp::ZERO;

/// How often, at most, a full namespace that found no expired key in any shard looks
/// again.
const PURGE_EVERY_MS: u64 = 1_000;

/// The most expired keys removed under one hold of a shard's lock.
const PURGE_BATCH: usize = 64;

/// How many keys one call evicts from an `lru` namespace before it gives up (a value
/// as large as many small ones needs several).
const MAX_EVICTIONS_PER_CALL: usize = 64;

/// The store's time, in milliseconds since the Unix epoch.
#[derive(Debug, Clone)]
enum Clock {
    System,
    /// A clock a test drives.
    Manual(Arc<AtomicU64>),
}

impl Clock {
    fn now_ms(&self) -> u64 {
        match self {
            Self::System => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            Self::Manual(ms) => ms.load(Relaxed),
        }
    }
}

/// Who this node is to the others, and its clock: what the writes of an `eventual`
/// namespace are stamped with (D6), and whose counts its counters raise.
#[derive(Debug)]
struct Node {
    /// Whose counts a counter's increments on this node raise: the node's id and
    /// when this run of it began. The store is in memory, so a node that restarts
    /// has forgotten what it had counted, and the others have not: counting on
    /// under the same name, its new counts would be taken for old ones already
    /// counted (the larger of two is kept) and lost. Each run counts under a name of
    /// its own instead, and what an earlier run counted stays counted.
    counts_as: Box<str>,
    clock: Hlc,
}

impl Node {
    fn new(id: &str) -> Self {
        // When this run began, in milliseconds; never the same twice in one
        // process, where a test may start two stores in one millisecond.
        static LAST: AtomicU64 = AtomicU64::new(0);
        let wall_ms = Clock::System.now_ms();
        let mut began = wall_ms;
        let _ = LAST.fetch_update(Relaxed, Relaxed, |last| {
            began = wall_ms.max(last.saturating_add(1));
            Some(began)
        });
        Self {
            counts_as: format!("{id}#{began:x}").into_boxed_str(),
            clock: Hlc::new(id),
        }
    }
}

/// How a write to an `eventual` namespace tells the peer links there is something to
/// send, without paying for it on every write: the first write after a link went to
/// sleep wakes the links, and the writes after it only read a flag.
#[derive(Debug, Default)]
struct Wake {
    /// Whether the links have been told since one of them last looked.
    told: AtomicBool,
    links: tokio::sync::Notify,
}

impl Wake {
    /// There is something to send. The caller has already made it visible (a shard's
    /// latest version, stored `SeqCst`): a link clears the flag and then looks, so one
    /// of the two sees the other.
    fn ping(&self) {
        if !self.told.load(SeqCst) && !self.told.swap(true, SeqCst) {
            self.links.notify_waiters();
        }
    }
}

/// What every namespace of one store shares.
#[derive(Debug)]
struct Shared {
    max_bytes: u64,
    bytes: AtomicU64,
    brownout: AtomicBool,
    clock: Clock,
    /// Set once, before the rules are loaded ([`Store::set_node`]).
    node: OnceLock<Node>,
    wake: Wake,
    /// Raised whenever a namespace is made: what a peer link compares to know that
    /// its list of `eventual` namespaces is out of date.
    generation: AtomicU64,
    /// How often two members compare an `eventual` namespace, in milliseconds.
    repair_every_ms: AtomicU64,
    /// State frames refused because this node declares no such `eventual` namespace.
    undeclared: AtomicU64,
}

impl Shared {
    /// This node. A store nobody named (a dry run's) is the node with no name.
    fn node(&self) -> &Node {
        self.node.get_or_init(|| Node::new(""))
    }
}

/// The rule state of one broker process, or of one dry run.
#[derive(Debug)]
pub struct Store {
    shared: Arc<Shared>,
    /// The namespaces loaded rule sets hold, by name.
    namespaces: Mutex<HashMap<String, Weak<Namespace>>>,
    /// A scratch store's namespaces, held for as long as the store is: the rules of a
    /// dry run are evaluated one at a time, with nothing else to hold what the first
    /// wrote for the next. `None` for the broker's store, whose namespaces live while
    /// loaded rules hold them.
    kept: Option<Mutex<Vec<Arc<Namespace>>>>,
    /// How often a connection's dictionary was refused room.
    dictionary_refused: AtomicU64,
}

/// One namespace's counts ([`Store::stats`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NamespaceStats {
    /// Keys held, expired ones not yet removed included.
    pub keys: usize,
    /// Bytes accounted to them.
    pub bytes: u64,
    /// New keys refused because the namespace was full.
    pub refused_namespace_full: u64,
    /// New keys refused because the store was at its ceiling.
    pub refused_store_full: u64,
    /// New keys refused during a brownout.
    pub refused_brownout: u64,
    /// Calls refused for a key of another type.
    pub refused_wrong_type: u64,
    /// Calls refused for anything else (a full set, a counter past 64 bits).
    pub refused_other: u64,
    /// Keys an `lru` namespace evicted to make room.
    pub evicted: u64,
}

impl Store {
    /// A store whose namespaces may hold `max_bytes` together.
    #[must_use]
    pub fn new(max_bytes: u64) -> Arc<Self> {
        Self::with_clock(max_bytes, Clock::System)
    }

    /// An empty store for one dry run, under the same ceiling as `like` (or the
    /// default one): its state calls never reach a running store.
    #[must_use]
    pub fn scratch(like: Option<&Store>) -> Arc<Self> {
        let max_bytes = like.map_or(mqtt_config::Rules::default().state_max_bytes, |store| {
            store.shared.max_bytes
        });
        Self::scratch_on(max_bytes, Clock::System)
    }

    /// An empty store for one replayed sequence (`mqttd --rule-test --sequence`, D11),
    /// under a ceiling of `max_bytes`, on the sequence's clock: `now_ms`, in
    /// milliseconds, which the replay moves from step to step. Nothing in it reads the
    /// wall clock, so a key expires when the sequence says so.
    #[must_use]
    pub fn scratch_at(max_bytes: u64, now_ms: Arc<AtomicU64>) -> Arc<Self> {
        Self::scratch_on(max_bytes, Clock::Manual(now_ms))
    }

    fn scratch_on(max_bytes: u64, clock: Clock) -> Arc<Self> {
        let mut store = Self::with_clock(max_bytes, clock);
        if let Some(store) = Arc::get_mut(&mut store) {
            store.kept = Some(Mutex::new(Vec::new()));
        }
        store
    }

    /// A store on a clock the caller drives, in milliseconds: for tests of expiry and
    /// of `kv_held`, which must not wait for real time.
    #[must_use]
    pub fn with_manual_clock(max_bytes: u64, now_ms: Arc<AtomicU64>) -> Arc<Self> {
        Self::with_clock(max_bytes, Clock::Manual(now_ms))
    }

    fn with_clock(max_bytes: u64, clock: Clock) -> Arc<Self> {
        Arc::new(Self {
            shared: Arc::new(Shared {
                max_bytes,
                bytes: AtomicU64::new(0),
                brownout: AtomicBool::new(false),
                clock,
                node: OnceLock::new(),
                wake: Wake::default(),
                generation: AtomicU64::new(0),
                repair_every_ms: AtomicU64::new(link::REPAIR_EVERY_MS),
                undeclared: AtomicU64::new(0),
            }),
            namespaces: Mutex::new(HashMap::new()),
            kept: None,
            dictionary_refused: AtomicU64::new(0),
        })
    }

    /// Name this node, once, before any rules are loaded: the id its counters count
    /// under and its clock breaks ties with in `eventual` namespaces. Later calls
    /// change nothing.
    pub fn set_node(&self, id: &str) {
        let _ = self.shared.node.set(Node::new(id));
    }

    /// How often two members compare their copies of an `eventual` namespace (they
    /// also do when their link comes up). For tests, which cannot wait 30 seconds.
    pub fn set_repair_interval(&self, every: std::time::Duration) {
        let ms = u64::try_from(every.as_millis()).unwrap_or(u64::MAX).max(1);
        self.shared.repair_every_ms.store(ms, Relaxed);
    }

    /// The `eventual` namespaces loaded rule sets hold now.
    fn eventual(&self) -> Vec<Arc<Namespace>> {
        lock(&self.namespaces)
            .values()
            .filter_map(Weak::upgrade)
            .filter(|ns| ns.replica.is_some())
            .collect()
    }

    /// How replication of one `eventual` namespace stands; `None` when no loaded rule
    /// set holds it, or it is `local`.
    #[must_use]
    pub fn replication(&self, namespace: &str) -> Option<Replication> {
        let ns = lock(&self.namespaces).get(namespace)?.upgrade()?;
        let replica = ns.replica.as_ref()?;
        Some(replica.stats(self.shared.clock.now_ms()))
    }

    /// State frames refused because they named a namespace this node's rules do not
    /// declare `eventual`.
    #[must_use]
    pub fn replication_undeclared(&self) -> u64 {
        self.shared.undeclared.load(Relaxed)
    }

    /// Whether the broker is over its memory watermark (ADR 0041): while it is, a key
    /// that does not exist yet is refused; existing keys still update and delete.
    pub fn set_brownout(&self, on: bool) {
        self.shared.brownout.store(on, Relaxed);
    }

    /// Bytes accounted across every namespace and every connection's dictionary.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.shared.bytes.load(Relaxed)
    }

    /// How often a connection's dictionary (`proc_dict_*`) was refused room: at the
    /// ceiling, or in a brownout.
    #[must_use]
    pub fn dictionary_refused(&self) -> u64 {
        self.dictionary_refused.load(Relaxed)
    }

    /// The namespaces loaded rule sets hold now, in name order.
    #[must_use]
    pub fn namespaces(&self) -> Vec<String> {
        let mut names: Vec<String> = lock(&self.namespaces)
            .iter()
            .filter(|(_, ns)| ns.strong_count() > 0)
            .map(|(name, _)| name.clone())
            .collect();
        names.sort_unstable();
        names
    }

    /// One namespace's counts; `None` when no loaded rule set holds it.
    #[must_use]
    pub fn stats(&self, namespace: &str) -> Option<NamespaceStats> {
        let ns = lock(&self.namespaces).get(namespace)?.upgrade()?;
        Some(NamespaceStats {
            keys: ns.keys.load(Relaxed),
            bytes: ns.bytes.load(Relaxed),
            refused_namespace_full: ns.refused[Refusal::NamespaceFull as usize].load(Relaxed),
            refused_store_full: ns.refused[Refusal::StoreFull as usize].load(Relaxed),
            refused_brownout: ns.refused[Refusal::Brownout as usize].load(Relaxed),
            refused_wrong_type: ns.refused[Refusal::WrongType as usize].load(Relaxed),
            refused_other: ns.refused[Refusal::Other as usize].load(Relaxed),
            evicted: ns.evicted.load(Relaxed),
        })
    }
}

impl StateStore for Store {
    fn max_bytes(&self) -> u64 {
        self.shared.max_bytes
    }

    fn open(
        &self,
        name: &str,
        settings: &NamespaceSettings,
    ) -> Result<Arc<dyn StateNamespace>, String> {
        let mut namespaces = lock(&self.namespaces);
        let eventual = settings.consistency == Consistency::Eventual;
        let namespace = if let Some(namespace) = namespaces.get(name).and_then(Weak::upgrade) {
            // D1: a namespace does not change its mode while rules hold it. Its keys
            // are kept in a different form, and the other members hold them or do not.
            if namespace.replica.is_some() != eventual {
                let (was, now) = if eventual {
                    ("local", "eventual")
                } else {
                    ("eventual", "local")
                };
                return Err(format!(
                    "[state.{name}]: consistency = \"{now}\", and the rules running now hold \
                     this namespace as \"{was}\": a namespace cannot change its mode; declare \
                     a new one"
                ));
            }
            namespace
        } else {
            // What rule sets dropped is forgotten here, so the map holds at most the
            // namespaces of the sets alive now.
            namespaces.retain(|_, ns| ns.strong_count() > 0);
            let namespace = Arc::new(Namespace::new(name, self.shared.clone(), eventual));
            namespaces.insert(name.to_string(), Arc::downgrade(&namespace));
            if let Some(kept) = &self.kept {
                lock(kept).push(namespace.clone());
            }
            self.shared.generation.fetch_add(1, SeqCst);
            namespace
        };
        if let Some(replica) = &namespace.replica {
            replica.declare(settings);
            // A link learns of a new namespace as it learns of a write.
            self.shared.wake.ping();
        }
        Ok(Arc::new(Handle {
            namespace,
            settings: settings.clone(),
        }))
    }

    /// What connections' dictionaries hold (`proc_dict_*`, D13) is in no namespace,
    /// and counts against the ceiling like any key: refused past it, and in a brownout.
    fn dictionary_grow(&self, bytes: u64) -> Result<(), StateError> {
        let refused = |reason, message: String| {
            self.dictionary_refused.fetch_add(1, Relaxed);
            Err(StateError { reason, message })
        };
        if self.shared.brownout.load(Relaxed) {
            return refused(
                BROWNOUT,
                "the broker is over its memory watermark: a connection's dictionary cannot \
                 grow until it is back under"
                    .to_string(),
            );
        }
        if !add_within(&self.shared.bytes, bytes, self.shared.max_bytes) {
            return refused(
                STORE_FULL,
                format!(
                    "rule state is at its ceiling of {} bytes (`rules.state_max_bytes`), which \
                     the connections' dictionaries count against",
                    self.shared.max_bytes
                ),
            );
        }
        Ok(())
    }

    fn dictionary_shrink(&self, bytes: u64) {
        self.shared.bytes.fetch_sub(bytes, Relaxed);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic under a shard's lock leaves a map that is still a map.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How many times a call tries a shard's lock before it waits for it.
const LOCK_SPINS: usize = 24;

/// Take a shard's lock for one map operation. The lock is held for about a hundred
/// nanoseconds, far less than it costs a thread to be put to sleep and woken, so a
/// caller that finds it taken tries again a few times before it waits: measured (see
/// the module's benchmark), that is the difference between a collision costing tens
/// of nanoseconds and costing microseconds on a platform whose mutex does not spin.
fn lock_shard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    for _ in 0..LOCK_SPINS {
        match mutex.try_lock() {
            Ok(guard) => return guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => return poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => std::hint::spin_loop(),
        }
    }
    lock(mutex)
}

/// A shard with no key in the order of use.
const NOT_IN_ORDER: u64 = u64::MAX;

/// What a refused call is counted under.
#[derive(Debug, Clone, Copy)]
enum Refusal {
    NamespaceFull,
    StoreFull,
    Brownout,
    WrongType,
    Other,
}

const REFUSALS: usize = 5;

#[derive(Debug)]
struct Entry {
    /// Its key: the one allocation the map and the two orders share.
    key: Arc<[u8]>,
    /// What it holds, in the type that merges (D4): the type is its creating
    /// operation's.
    data: Data,
    /// When it expires, in the store's milliseconds; `0`: never.
    expires_ms: u64,
    /// What it is accounted as: [`ENTRY_OVERHEAD`], its key and its data.
    bytes: usize,
    /// Its place in the shard's order of use; `0`: not in it.
    used: u64,
}

impl Entry {
    fn expired(&self, now_ms: u64) -> bool {
        self.expires_ms != 0 && self.expires_ms <= now_ms
    }

    /// What its data weighs: what a read of it is charged.
    fn data_bytes(&self) -> usize {
        self.bytes.saturating_sub(ENTRY_OVERHEAD + self.key.len())
    }
}

/// The second an expiry falls in, rounded up: by then the key has expired. `0` for no
/// expiry.
fn expiry_slot(expires_ms: u64) -> u64 {
    expires_ms.div_ceil(1000)
}

#[derive(Debug, Default)]
struct Shard {
    map: HashMap<Arc<[u8]>, Entry>,
    /// The keys of an `lru` namespace in order of use, oldest first.
    used: BTreeMap<u64, Arc<[u8]>>,
    /// The keys that expire, by [`expiry_slot`].
    expiry: BTreeSet<(u64, Arc<[u8]>)>,
}

impl Shard {
    /// Take `key` out of the map and both orders.
    fn take(&mut self, key: &[u8]) -> Option<Entry> {
        let entry = self.map.remove(key)?;
        if entry.used != 0 {
            self.used.remove(&entry.used);
        }
        if entry.expires_ms != 0 {
            self.expiry
                .remove(&(expiry_slot(entry.expires_ms), entry.key.clone()));
        }
        Some(entry)
    }
}

/// One namespace: its keys, wherever the rules that call it were loaded from.
#[derive(Debug)]
struct Namespace {
    name: Arc<str>,
    shared: Arc<Shared>,
    hasher: RandomState,
    shards: Box<[Mutex<Shard>]>,
    keys: AtomicUsize,
    bytes: AtomicU64,
    /// The next place in the order of use.
    tick: AtomicU64,
    /// Each shard's oldest place in the order of use ([`NOT_IN_ORDER`]: none), kept
    /// beside the shards so that making room reads [`SHARDS`] numbers instead of
    /// taking [`SHARDS`] locks.
    oldest: Box<[AtomicU64]>,
    /// When the shards were last looked through for expired keys and had none.
    purged_ms: AtomicU64,
    refused: [AtomicU64; REFUSALS],
    evicted: AtomicU64,
    /// An `eventual` namespace's keys: every call goes there, and `shards` stays
    /// empty. The counts and the refusals above are kept either way.
    replica: Option<replica::Replica>,
}

impl Drop for Namespace {
    fn drop(&mut self) {
        // Its keys go with it; the store's total gives their bytes back.
        self.shared
            .bytes
            .fetch_sub(self.bytes.load(Relaxed), Relaxed);
    }
}

/// A namespace as one loaded rule set calls it: with that set's settings.
#[derive(Debug)]
struct Handle {
    namespace: Arc<Namespace>,
    settings: NamespaceSettings,
}

/// What a call does next when its key has no room.
enum Step<T> {
    Done(T),
    /// A new key needs room.
    NeedRoom(Refusal),
}

fn wide(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// Add `n` to `total` unless that would pass `max`; whether it was added.
fn add_within(total: &AtomicU64, n: u64, max: u64) -> bool {
    total
        .fetch_update(Relaxed, Relaxed, |held| {
            held.checked_add(n).filter(|after| *after <= max)
        })
        .is_ok()
}

/// A value as the store keeps it. A binary is copied: one cut out of a payload shares
/// the payload's buffer, and keeping it would keep the whole message.
fn owned(v: &Value) -> Value {
    fn has_binary(v: &Value) -> bool {
        match v {
            Value::Bin(_) => true,
            Value::Array(items) => items.iter().any(has_binary),
            Value::Map(map) => map.iter().any(|(_, v)| has_binary(v)),
            _ => false,
        }
    }
    fn copied(v: &Value) -> Value {
        match v {
            Value::Bin(b) => Value::Bin(bytes::Bytes::copy_from_slice(b)),
            Value::Array(items) => Value::from(items.iter().map(copied).collect::<Vec<_>>()),
            Value::Map(map) => Value::from(
                map.iter()
                    .map(|(k, v)| (k.clone(), copied(v)))
                    .collect::<mqtt_rules::Map>(),
            ),
            other => other.clone(),
        }
    }
    if has_binary(v) {
        copied(v)
    } else {
        v.clone()
    }
}

impl Namespace {
    fn new(name: &str, shared: Arc<Shared>, eventual: bool) -> Self {
        Self {
            replica: eventual.then(replica::Replica::new),
            name: Arc::from(name),
            shared,
            hasher: RandomState::new(),
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            keys: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            tick: AtomicU64::new(0),
            oldest: (0..SHARDS).map(|_| AtomicU64::new(NOT_IN_ORDER)).collect(),
            purged_ms: AtomicU64::new(0),
            refused: Default::default(),
            evicted: AtomicU64::new(0),
        }
    }

    /// Which shard holds `key`.
    fn shard_of(&self, key: &[u8]) -> usize {
        // The high bits: the shard's own map hashes the key again with its own keys.
        let hash = self.hasher.hash_one(key);
        usize::try_from(hash >> 32).unwrap_or(0) % SHARDS
    }

    /// Publish shard `at`'s oldest place in the order of use, when it changed, for
    /// [`evict_one`](Self::evict_one) to read without the shard's lock. Called with
    /// the lock held, before it is released.
    fn note_oldest(&self, at: usize, shard: &Shard) {
        let oldest = shard
            .used
            .first_key_value()
            .map_or(NOT_IN_ORDER, |(used, _)| *used);
        // Read first: the oldest key of a shard changes far less often than its keys
        // are used, and a store would take the cache line from every reader.
        if self.oldest[at].load(Relaxed) != oldest {
            self.oldest[at].store(oldest, Relaxed);
        }
    }

    fn refuse(&self, refusal: Refusal, message: String) -> StateError {
        self.refused[refusal as usize].fetch_add(1, Relaxed);
        StateError {
            reason: match refusal {
                Refusal::NamespaceFull => NAMESPACE_FULL,
                Refusal::StoreFull => STORE_FULL,
                Refusal::Brownout => BROWNOUT,
                Refusal::WrongType => WRONG_TYPE,
                Refusal::Other => "refused",
            },
            message,
        }
    }

    fn wrong_type(&self, held: &Data, wanted: &str) -> StateError {
        self.wrong_type_named(held.type_name(), wanted)
    }

    fn wrong_type_named(&self, held: &str, wanted: &str) -> StateError {
        self.refuse(
            Refusal::WrongType,
            format!(
                "the key holds a value of type `{held}`, and this is an operation on type \
                 `{wanted}` (kv_del deletes a key of any type)"
            ),
        )
    }

    /// Give back what one removed entry was accounted as.
    fn release(&self, entry: &Entry) {
        self.keys.fetch_sub(1, Relaxed);
        self.bytes.fetch_sub(wide(entry.bytes), Relaxed);
        self.shared.bytes.fetch_sub(wide(entry.bytes), Relaxed);
    }

    /// Account a change in the size of an entry that stays. A write to a key that
    /// exists is not refused for room: it is bounded per key.
    fn resize(&self, entry: &mut Entry, bytes: usize) {
        if bytes >= entry.bytes {
            let more = wide(bytes - entry.bytes);
            self.bytes.fetch_add(more, Relaxed);
            self.shared.bytes.fetch_add(more, Relaxed);
        } else {
            let less = wide(entry.bytes - bytes);
            self.bytes.fetch_sub(less, Relaxed);
            self.shared.bytes.fetch_sub(less, Relaxed);
        }
        entry.bytes = bytes;
    }

    /// Reserve room for one new key of `bytes`, or say which bound is in the way.
    /// No count is ever past its bound, and nothing is left reserved on refusal.
    fn reserve(&self, settings: &NamespaceSettings, bytes: usize) -> Result<(), Refusal> {
        self.reserve_within(settings.max_keys, settings.max_bytes, bytes)
    }

    /// [`reserve`](Self::reserve), under bounds given as numbers.
    fn reserve_within(&self, max_keys: usize, max_bytes: u64, bytes: usize) -> Result<(), Refusal> {
        let bytes = wide(bytes);
        if self
            .keys
            .fetch_update(Relaxed, Relaxed, |keys| {
                (keys < max_keys).then_some(keys + 1)
            })
            .is_err()
        {
            return Err(Refusal::NamespaceFull);
        }
        if !add_within(&self.bytes, bytes, max_bytes) {
            self.keys.fetch_sub(1, Relaxed);
            return Err(Refusal::NamespaceFull);
        }
        if !add_within(&self.shared.bytes, bytes, self.shared.max_bytes) {
            self.bytes.fetch_sub(bytes, Relaxed);
            self.keys.fetch_sub(1, Relaxed);
            return Err(Refusal::StoreFull);
        }
        Ok(())
    }

    /// Remove `key` from `shard`, giving its room back.
    fn remove(&self, shard: &mut Shard, key: &[u8]) -> bool {
        match shard.take(key) {
            Some(entry) => {
                self.release(&entry);
                true
            }
            None => false,
        }
    }

    /// Remove keys of `shard` whose expiry has passed, at most [`PURGE_BATCH`] of them:
    /// the caller holds the shard's lock, and a whole site's keys can expire at once.
    /// Returns whether any went.
    fn purge(&self, shard: &mut Shard, now_ms: u64) -> bool {
        let mut any = false;
        for _ in 0..PURGE_BATCH {
            let due = shard
                .expiry
                .first()
                .is_some_and(|(slot, _)| slot.saturating_mul(1000) <= now_ms);
            let Some((_, key)) = due.then(|| shard.expiry.pop_first()).flatten() else {
                break;
            };
            if shard.map.get(&*key).is_some_and(|e| e.expired(now_ms)) {
                any |= self.remove(shard, &key);
            }
        }
        any
    }

    /// Look through the shards for expired keys, one lock at a time, and stop at the
    /// first that had some: room for the caller's key, at a bounded price. A look that
    /// finds none anywhere is not repeated within a second. Returns whether any went.
    fn purge_all(&self) -> bool {
        let now_ms = self.shared.clock.now_ms();
        let last = self.purged_ms.load(Relaxed);
        if last != 0 && now_ms < last.saturating_add(PURGE_EVERY_MS) {
            return false;
        }
        for (at, shard) in self.shards.iter().enumerate() {
            let mut shard = lock_shard(shard);
            let any = self.purge(&mut shard, now_ms);
            self.note_oldest(at, &shard);
            if any {
                return true;
            }
        }
        self.purged_ms.store(now_ms.max(1), Relaxed);
        false
    }

    /// After a write to (or a read of) `entry`: set its expiry when the call named a
    /// TTL, and in an `lru` namespace note that it was used now.
    fn after(
        &self,
        settings: &NamespaceSettings,
        entry: &mut Entry,
        used: &mut BTreeMap<u64, Arc<[u8]>>,
        expiry: &mut BTreeSet<(u64, Arc<[u8]>)>,
        ttl: Option<u64>,
        now_ms: u64,
    ) {
        if let Some(secs) = ttl {
            let expires_ms = now_ms.saturating_add(secs.saturating_mul(1000)).max(1);
            let (was, now) = (expiry_slot(entry.expires_ms), expiry_slot(expires_ms));
            // A key refreshed many times a second moves in the order once a second.
            if was != now {
                if entry.expires_ms != 0 {
                    expiry.remove(&(was, entry.key.clone()));
                }
                expiry.insert((now, entry.key.clone()));
            }
            entry.expires_ms = expires_ms;
        }
        if settings.eviction == Eviction::Lru {
            let now = self.tick.fetch_add(1, Relaxed) + 1;
            if entry.used != 0 {
                used.remove(&entry.used);
            }
            used.insert(now, entry.key.clone());
            entry.used = now;
        }
    }

    /// Evict the namespace's least recently used key: the oldest of the shards'
    /// oldest, read without their locks, then one shard's lock to remove it.
    fn evict_one(&self) -> bool {
        let oldest = self
            .oldest
            .iter()
            .enumerate()
            .map(|(at, used)| (used.load(Relaxed), at))
            .filter(|(used, _)| *used != NOT_IN_ORDER)
            .min();
        if let Some((_, at)) = oldest {
            let mut shard = lock_shard(&self.shards[at]);
            // Whatever is oldest there now: the key seen above may have been used since.
            let key = shard.used.first_key_value().map(|(_, key)| key.clone());
            let evicted = key.is_some_and(|key| self.remove(&mut shard, &key));
            self.note_oldest(at, &shard);
            if evicted {
                self.evicted.fetch_add(1, Relaxed);
                return true;
            }
        }
        // Keys written before the namespace was declared `lru` are in no order of use:
        // any of them makes room.
        for shard in &self.shards {
            let mut shard = lock_shard(shard);
            let key = shard.map.keys().next().cloned();
            if key.is_some_and(|key| self.remove(&mut shard, &key)) {
                self.evicted.fetch_add(1, Relaxed);
                return true;
            }
        }
        false
    }

    /// Insert a new key, reserving its room.
    #[allow(clippy::too_many_arguments)]
    fn insert(
        &self,
        settings: &NamespaceSettings,
        shard: &mut Shard,
        key: &[u8],
        data: Data,
        data_bytes: usize,
        ttl: Option<u64>,
        now_ms: u64,
    ) -> Result<(), Refusal> {
        if self.shared.brownout.load(Relaxed) {
            return Err(Refusal::Brownout);
        }
        let bytes = ENTRY_OVERHEAD + key.len() + data_bytes;
        if let Err(refusal) = self.reserve(settings, bytes) {
            // Expired keys this shard still holds are room, too.
            if !self.purge(shard, now_ms) {
                return Err(refusal);
            }
            self.reserve(settings, bytes)?;
        }
        let key: Arc<[u8]> = Arc::from(key);
        let mut entry = Entry {
            key: key.clone(),
            data,
            expires_ms: 0,
            bytes,
            used: 0,
        };
        self.after(
            settings,
            &mut entry,
            &mut shard.used,
            &mut shard.expiry,
            ttl.or(settings.default_ttl_secs),
            now_ms,
        );
        shard.map.insert(key, entry);
        Ok(())
    }
}

/// Whole seconds from `since_ms` to `now_ms`.
fn seconds(since_ms: u64, now_ms: u64) -> Value {
    Value::Int(i64::try_from(now_ms.saturating_sub(since_ms) / 1000).unwrap_or(i64::MAX))
}

/// A sketch's estimate, as a rule reads it.
fn estimate(sketch: &Hll) -> Value {
    Value::Int(i64::try_from(sketch.count()).unwrap_or(i64::MAX))
}

/// A counter that starts at `by` (`kv_incr`).
fn counter(by: i64) -> Data {
    let mut counts = Counts::default();
    // A counter of no node takes any first step.
    let _: Result<i64, CountsError> = counts.add(LOCAL_NODE, by);
    Data::Sum(counts)
}

/// A set of one element (`kv_add`).
fn set_of(identity: &[u8], element: &Value) -> Data {
    let mut elements = Elements::default();
    // An empty set is not full.
    let _: Result<bool, SetFull> = elements.insert(identity, element, owned);
    Data::Union(elements)
}

/// A value written last (`kv_set`, `kv_changed`, `kv_store_put`).
fn lww(value: &Value) -> Data {
    Data::Lww {
        stamp: UNSTAMPED,
        value: owned(value),
    }
}

/// A value written first (`kv_set_once`, `kv_held`).
fn first(value: Value) -> Data {
    Data::First {
        stamp: UNSTAMPED,
        value,
    }
}

fn done(value: Option<Value>) -> Step<StateReply> {
    Step::Done(StateReply {
        value,
        read_bytes: 0,
    })
}

/// The key `op` is about.
fn op_key<'a>(op: &StateOp<'a>) -> &'a [u8] {
    match op {
        StateOp::Get { key }
        | StateOp::Set { key, .. }
        | StateOp::SetOnce { key, .. }
        | StateOp::Incr { key, .. }
        | StateOp::Extreme { key, .. }
        | StateOp::Add { key, .. }
        | StateOp::Del { key }
        | StateOp::Ttl { key }
        | StateOp::Held { key, .. }
        | StateOp::Changed { key, .. }
        | StateOp::HllAdd { key, .. }
        | StateOp::HllCount { key } => key,
    }
}

impl Handle {
    /// One attempt at `op`: its shard's lock, the one map operation, the lock released.
    fn attempt(&self, op: &StateOp<'_>) -> Result<Step<StateReply>, StateError> {
        let ns = &*self.namespace;
        let now_ms = ns.shared.clock.now_ms();
        let key = op_key(op);
        let at = ns.shard_of(key);
        let mut shard = lock_shard(&ns.shards[at]);
        let step = self.apply(op, key, &mut shard, now_ms);
        if !shard.used.is_empty() || ns.oldest[at].load(Relaxed) != NOT_IN_ORDER {
            ns.note_oldest(at, &shard);
        }
        step
    }

    /// `op` on `key`, in the shard that holds it.
    #[allow(clippy::too_many_lines)]
    fn apply(
        &self,
        op: &StateOp<'_>,
        key: &[u8],
        shard: &mut Shard,
        now_ms: u64,
    ) -> Result<Step<StateReply>, StateError> {
        let ns = &*self.namespace;
        let settings = &self.settings;
        // A key past its expiry reads as absent, and is removed by whoever finds it.
        if shard.map.get(key).is_some_and(|e| e.expired(now_ms)) {
            ns.remove(shard, key);
        }
        // A new key: reserve, insert, and answer `$reply`; or ask for room.
        macro_rules! insert {
            ($data:expr, $bytes:expr, $ttl:expr, $reply:expr) => {{
                match ns.insert(settings, shard, key, $data, $bytes, $ttl, now_ms) {
                    Ok(()) => done($reply),
                    Err(refusal) => Step::NeedRoom(refusal),
                }
            }};
        }
        // The entry was written or read: its expiry, and its place in the order of use.
        macro_rules! after {
            ($entry:expr, $ttl:expr) => {
                ns.after(
                    settings,
                    $entry,
                    &mut shard.used,
                    &mut shard.expiry,
                    $ttl,
                    now_ms,
                )
            };
        }
        let step = match *op {
            StateOp::Get { .. } => {
                let Some(entry) = shard.map.get_mut(key) else {
                    return Ok(done(None));
                };
                let value = entry.data.read();
                // A sketch reads as its estimate, not as its registers.
                let read_bytes = match entry.data {
                    Data::Hll(_) => NUMBER_BYTES,
                    _ => entry.data_bytes(),
                };
                after!(entry, None);
                Step::Done(StateReply {
                    value: Some(value),
                    read_bytes,
                })
            }
            StateOp::Set { value, ttl, .. } => match shard.map.get_mut(key) {
                Some(entry) => {
                    if !matches!(entry.data, Data::Lww { .. }) {
                        return Err(ns.wrong_type(&entry.data, "lww"));
                    }
                    entry.data = lww(value.value);
                    ns.resize(entry, ENTRY_OVERHEAD + key.len() + value.bytes);
                    after!(entry, ttl);
                    done(None)
                }
                None => insert!(lww(value.value), value.bytes, ttl, None),
            },
            StateOp::SetOnce { value, ttl, .. } => match shard.map.get_mut(key) {
                Some(entry) => {
                    let Data::First { value: held, .. } = &entry.data else {
                        return Err(ns.wrong_type(&entry.data, "first"));
                    };
                    let held = held.clone();
                    let read_bytes = entry.data_bytes();
                    after!(entry, ttl);
                    Step::Done(StateReply {
                        value: Some(held),
                        read_bytes,
                    })
                }
                None => insert!(
                    first(owned(value.value)),
                    value.bytes,
                    ttl,
                    Some(value.value.clone())
                ),
            },
            StateOp::Incr { by, ttl, .. } => match shard.map.get_mut(key) {
                Some(entry) => {
                    let Data::Sum(counts) = &mut entry.data else {
                        return Err(ns.wrong_type(&entry.data, "sum"));
                    };
                    let sum = match counts.add(LOCAL_NODE, by) {
                        Ok(sum) => sum,
                        Err(CountsError::Overflow) => {
                            return Err(ns.refuse(
                                Refusal::Other,
                                "the counter would pass 64 bits".to_string(),
                            ));
                        }
                        Err(CountsError::TooManyNodes | CountsError::NodeId) => {
                            return Err(ns.refuse(
                                Refusal::Other,
                                "the counter holds counts of as many nodes as it may".to_string(),
                            ));
                        }
                    };
                    after!(entry, ttl);
                    done(Some(Value::Int(sum)))
                }
                None => insert!(counter(by), NUMBER_BYTES, ttl, Some(Value::Int(by))),
            },
            StateOp::Extreme {
                value,
                largest,
                ttl,
                ..
            } => {
                let extreme = |v: Value| {
                    if largest {
                        Data::Max(v)
                    } else {
                        Data::Min(v)
                    }
                };
                if let Some(entry) = shard.map.get_mut(key) {
                    let ((Data::Max(held), true) | (Data::Min(held), false)) =
                        (&entry.data, largest)
                    else {
                        let wanted = if largest { "max" } else { "min" };
                        return Err(ns.wrong_type(&entry.data, wanted));
                    };
                    let order = value.value.term_cmp(held);
                    let wins = if largest {
                        order == std::cmp::Ordering::Greater
                    } else {
                        order == std::cmp::Ordering::Less
                    };
                    let now_held = if wins {
                        let v = owned(value.value);
                        entry.data = extreme(v.clone());
                        ns.resize(entry, ENTRY_OVERHEAD + key.len() + value.bytes);
                        v
                    } else {
                        held.clone()
                    };
                    after!(entry, ttl);
                    done(Some(now_held))
                } else {
                    let v = owned(value.value);
                    insert!(extreme(v.clone()), value.bytes, ttl, Some(v))
                }
            }
            StateOp::Add {
                element,
                identity,
                ttl,
                ..
            } => match shard.map.get_mut(key) {
                Some(entry) => {
                    let Data::Union(elements) = &mut entry.data else {
                        return Err(ns.wrong_type(&entry.data, "union"));
                    };
                    // Kept in term order, and for values that order calls equal (`1`
                    // and `1.0`) in the order of their identities.
                    let Ok(is_new) = elements.insert(identity, element.value, owned) else {
                        return Err(ns.refuse(
                            Refusal::Other,
                            format!(
                                "the set holds {MAX_SET_ELEMENTS} elements, the most a set may"
                            ),
                        ));
                    };
                    let count = Value::Int(i64::try_from(elements.len()).unwrap_or(i64::MAX));
                    if is_new {
                        let bytes = entry.bytes + ELEMENT_OVERHEAD + element.bytes;
                        ns.resize(entry, bytes);
                    }
                    after!(entry, ttl);
                    done(Some(count))
                }
                None => insert!(
                    set_of(identity, element.value),
                    ELEMENT_OVERHEAD + element.bytes,
                    ttl,
                    Some(Value::Int(1))
                ),
            },
            StateOp::Del { .. } => done(Some(Value::Bool(ns.remove(shard, key)))),
            StateOp::Ttl { .. } => {
                let Some(entry) = shard.map.get_mut(key) else {
                    return Ok(done(None));
                };
                let left = (entry.expires_ms != 0).then(|| {
                    let secs = entry.expires_ms.saturating_sub(now_ms).div_ceil(1000);
                    Value::Int(i64::try_from(secs).unwrap_or(i64::MAX))
                });
                after!(entry, None);
                done(left)
            }
            StateOp::Held { holds, ttl, .. } => match shard.map.get_mut(key) {
                Some(entry) => {
                    let Data::First { value: since, .. } = &entry.data else {
                        return Err(ns.wrong_type(&entry.data, "first"));
                    };
                    if holds {
                        let Value::Int(since) = since else {
                            return Err(ns.refuse(
                                Refusal::WrongType,
                                "the key holds a value kv_set_once stored, not the time kv_held \
                                 records"
                                    .to_string(),
                            ));
                        };
                        let held = seconds(u64::try_from(*since).unwrap_or(0), now_ms);
                        after!(entry, ttl);
                        done(Some(held))
                    } else {
                        ns.remove(shard, key);
                        done(None)
                    }
                }
                None if holds => {
                    let since = Value::Int(i64::try_from(now_ms).unwrap_or(i64::MAX));
                    insert!(first(since), 8, ttl, Some(Value::Int(0)))
                }
                None => done(None),
            },
            StateOp::Changed { value, ttl, .. } => match shard.map.get_mut(key) {
                Some(entry) => {
                    let Data::Lww { value: held, .. } = &entry.data else {
                        return Err(ns.wrong_type(&entry.data, "lww"));
                    };
                    let changed = !held.exact_eq(value.value);
                    if changed {
                        entry.data = lww(value.value);
                        ns.resize(entry, ENTRY_OVERHEAD + key.len() + value.bytes);
                    }
                    after!(entry, ttl);
                    done(Some(Value::Bool(changed)))
                }
                None => insert!(lww(value.value), value.bytes, ttl, Some(Value::Bool(true))),
            },
            StateOp::HllAdd { hash, ttl, .. } => {
                if let Some(entry) = shard.map.get_mut(key) {
                    let Data::Hll(sketch) = &mut entry.data else {
                        return Err(ns.wrong_type(&entry.data, "hll"));
                    };
                    let before = sketch.bytes();
                    sketch.add(hash);
                    let (now, count) = (sketch.bytes(), estimate(sketch));
                    if now != before {
                        // A sketch only grows, up to its 12 KiB.
                        let bytes = entry.bytes + now.saturating_sub(before);
                        ns.resize(entry, bytes);
                    }
                    after!(entry, ttl);
                    done(Some(count))
                } else {
                    let mut sketch = Box::<Hll>::default();
                    sketch.add(hash);
                    let (bytes, count) = (SKETCH_OVERHEAD + sketch.bytes(), estimate(&sketch));
                    insert!(Data::Hll(sketch), bytes, ttl, Some(count))
                }
            }
            StateOp::HllCount { .. } => {
                let Some(entry) = shard.map.get_mut(key) else {
                    return Ok(done(Some(Value::Int(0))));
                };
                let Data::Hll(sketch) = &entry.data else {
                    return Err(ns.wrong_type(&entry.data, "hll"));
                };
                let count = estimate(sketch);
                after!(entry, None);
                done(Some(count))
            }
        };
        Ok(step)
    }

    fn full(&self, refusal: Refusal) -> StateError {
        let ns = &*self.namespace;
        let message = match refusal {
            Refusal::Brownout => format!(
                "the broker is over its memory watermark, so namespace `{}` takes no new key \
                 (existing keys still update and delete)",
                ns.name
            ),
            Refusal::StoreFull => format!(
                "rule state holds {} bytes across its namespaces, and a new key in `{}` would \
                 pass the store's ceiling of {} (`rules.state_max_bytes`)",
                ns.shared.bytes.load(Relaxed),
                ns.name,
                ns.shared.max_bytes
            ),
            _ => format!(
                "namespace `{}` holds {} keys and {} bytes (max_keys {}, max_bytes {}); a new \
                 key does not fit",
                ns.name,
                ns.keys.load(Relaxed),
                ns.bytes.load(Relaxed),
                self.settings.max_keys,
                self.settings.max_bytes
            ),
        };
        ns.refuse(refusal, message)
    }
}

impl StateNamespace for Handle {
    fn call(&self, op: &StateOp<'_>) -> Result<StateReply, StateError> {
        if let Some(replica) = &self.namespace.replica {
            return replica.call(self, op);
        }
        let mut evictions = 0;
        loop {
            // The shard's lock is released before anything else is done about room.
            let refusal = match self.attempt(op)? {
                Step::Done(reply) => return Ok(reply),
                Step::NeedRoom(refusal) => refusal,
            };
            if matches!(refusal, Refusal::Brownout) {
                return Err(self.full(refusal));
            }
            // Expired keys anywhere in the namespace are room; then, in an `lru`
            // namespace, the least recently used key is.
            if self.namespace.purge_all() {
                continue;
            }
            if self.settings.eviction == Eviction::Lru
                && evictions < MAX_EVICTIONS_PER_CALL
                && self.namespace.evict_one()
            {
                evictions += 1;
                continue;
            }
            return Err(self.full(refusal));
        }
    }
}

/// A value with its size, for callers outside the rule engine (tests, the benchmark).
#[must_use]
pub fn stored(value: &Value) -> Stored<'_> {
    Stored {
        value,
        bytes: mqtt_rules::state::written_len(value),
    }
}

mod held;
pub use held::HeldKey;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_replica;
