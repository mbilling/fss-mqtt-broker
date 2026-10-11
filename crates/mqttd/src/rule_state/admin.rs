//! What an operator sees and does in the store (ADR 0085 D10): each namespace's
//! counts, a listing of its keys, one key's value, and a delete.
//!
//! Nothing here is on the publish path, and everything here shares the shards' locks
//! with it, so:
//!
//! * **A lock is held for one bounded piece of work.** Counting takes each shard's lock
//!   to add up a few numbers. Counting the keys whose expiry has passed walks the order
//!   of expiry at most [`SCAN_BATCH`] entries per hold. Listing walks a shard's map
//!   [`WALK_BATCH`] keys per hold, taking a reference to each key it selects and
//!   nothing else; it sorts, cuts and formats with no lock held. No two shard locks
//!   are held at once.
//! * **A walk in pieces is still a walk of the whole shard.** A hash map has no place
//!   to come back to once its lock is released: a key put in or taken out meanwhile can
//!   move the others. Each shard counts those changes, and a walk that finds the count
//!   moved starts that shard again. A shard whose keys come and go too fast for that
//!   ([`WALK_RETRIES`] starts) is walked under one hold instead, which costs a call on
//!   one of its keys a wait of that pass, once.
//! * **A read is not a use.** Listing a key or reading its value does not move it in an
//!   `lru` namespace's order of use, removes no expired key (the sweep that raises the
//!   expiry event does that, D7) and counts as no call of a rule.
//! * **A request is bounded**: a page of at most [`MAX_PAGE_KEYS`] keys and
//!   [`MAX_PAGE_BYTES`] of them, within [`Limits::time`]. A page that reaches a bound
//!   early is short, and says where the next one starts.
//!
//! # The order of a listing, and its cursor
//!
//! Keys are listed shard by shard, and within a shard in the order of their bytes. A
//! [`Cursor`] is a place in that order: the shard, and the last key listed from it. A
//! key's shard never changes while its namespace lives, and neither does its place in
//! its shard, so whatever is written meanwhile:
//!
//! * a key that is there from the first page to the last is listed exactly once;
//! * no key is ever listed twice;
//! * a key inserted while the listing runs is listed when its place is after the
//!   cursor and not when it is before; a key deleted before the listing reaches its
//!   place is not listed.
//!
//! A namespace dropped and declared again spreads its keys anew, so a cursor names the
//! namespace it was given by ([`KeysError::StaleCursor`]).
//!
//! # One selection
//!
//! Which keys a listing shows is decided by one function, [`Selector::matches`], over
//! one walk, [`Namespace::select`]. Whatever later selects keys by more than a prefix
//! (an expiry before a time, no expiry), or does something to the
//! keys selected, adds a predicate there and walks the same way.

use std::ops::Bound;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mqtt_rules::state::{
    Consistency, NamespaceSettings, StateOp, BROWNOUT, NAMESPACE_FULL, STORE_FULL, WRONG_TYPE,
};
use mqtt_rules::Value;

use super::replica::Replica;
use super::{lock, lock_shard, Handle, Namespace, Shard, Store, REFUSALS, SHARDS};

/// The operations a call is counted under (`mqttd_rule_state_ops_total{op}`):
/// `kv_store_put`, `kv_store_get` and `kv_store_del` count as `set`, `get` and `del`.
pub const OPS: [&str; 13] = [
    "get",
    "set",
    "set_once",
    "incr",
    "max",
    "min",
    "add",
    "del",
    "ttl",
    "held",
    "changed",
    "hll_add",
    "hll_count",
];

/// How a call ended (`mqttd_rule_state_ops_total{result}`): `ok`, or why the store
/// refused it. A call whose arguments the rule engine refused (a key too long, a bad
/// TTL, the message's budget) never reaches the store and is its rule's failed
/// evaluation only.
pub const RESULTS: [&str; 1 + REFUSALS] = [
    "ok",
    NAMESPACE_FULL,
    STORE_FULL,
    BROWNOUT,
    WRONG_TYPE,
    "refused",
];

/// The most keys one page of a listing holds.
pub const MAX_PAGE_KEYS: usize = 1000;

/// The most bytes of keys one page holds (it always holds one key).
pub const MAX_PAGE_BYTES: usize = 256 * 1024;

/// How long a listing looks for a page's keys before it answers with what it has: a
/// prefix few keys match can take a walk of every shard.
pub const PAGE_TIME: Duration = Duration::from_millis(50);

/// The most entries of a shard's order of expiry counted under one hold of its lock.
const SCAN_BATCH: usize = 1024;

/// The most keys of a shard's map a listing looks at under one hold of its lock.
pub const WALK_BATCH: usize = 64;

/// How often a listing starts a shard again because its map changed between two holds,
/// before it walks the shard under one hold.
pub const WALK_RETRIES: usize = 4;

/// Which of [`OPS`] `op` is counted under.
pub(super) fn op_index(op: &StateOp<'_>) -> usize {
    match op {
        StateOp::Get { .. } => 0,
        StateOp::Set { .. } => 1,
        StateOp::SetOnce { .. } => 2,
        StateOp::Incr { .. } | StateOp::IncrOnce { .. } => 3,
        StateOp::Extreme { largest: true, .. } => 4,
        StateOp::Extreme { largest: false, .. } => 5,
        StateOp::Add { .. } => 6,
        StateOp::Del { .. } => 7,
        StateOp::Ttl { .. } => 8,
        StateOp::Held { .. } => 9,
        StateOp::Changed { .. } => 10,
        StateOp::HllAdd { .. } => 11,
        StateOp::HllCount { .. } => 12,
    }
}

/// One namespace's counts, as of one look at it. The counters are since the namespace
/// was made: a namespace dropped and declared again starts from zero, and is another
/// [`instance`](Self::instance).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NamespaceCounts {
    /// Which namespace of this name the counts are of.
    pub instance: u64,
    /// Keys held, expired ones not yet removed included.
    pub keys: u64,
    /// Bytes accounted to them.
    pub bytes: u64,
    /// Calls answered, by operation ([`OPS`]).
    pub ok: [u64; OPS.len()],
    /// Calls the store refused, by operation ([`OPS`]) and reason (the entries of
    /// [`RESULTS`] after `ok`).
    pub failed: [[u64; REFUSALS]; OPS.len()],
    /// Keys removed because their expiry had passed.
    pub expired: u64,
    /// Keys whose expiry has passed and that are still held.
    pub expiry_backlog: u64,
    /// Keys an `lru` namespace evicted to make room.
    pub evicted: u64,
    /// Keys an operator deleted.
    pub deleted: u64,
}

impl NamespaceCounts {
    /// Every call answered.
    #[must_use]
    pub fn calls_ok(&self) -> u64 {
        self.ok.iter().sum()
    }

    /// Every call refused, by reason.
    #[must_use]
    pub fn refused(&self) -> [u64; REFUSALS] {
        let mut by_reason = [0; REFUSALS];
        for op in &self.failed {
            for (total, n) in by_reason.iter_mut().zip(op) {
                *total += n;
            }
        }
        by_reason
    }
}

/// What the store holds as a whole.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreTotals {
    /// Bytes accounted across every namespace and every connection's dictionary.
    pub bytes: u64,
    /// The ceiling (`rules.state_max_bytes`).
    pub max_bytes: u64,
    /// Whether new keys are refused because the broker is over its memory watermark.
    pub brownout: bool,
    /// The part of `bytes` that connections' dictionaries hold (`proc_dict_*`).
    pub dictionary_bytes: u64,
    /// How often a dictionary was refused room.
    pub dictionary_refused: u64,
}

/// Which keys of a namespace a walk selects.
#[derive(Debug, Clone, Copy, Default)]
pub struct Selector<'a> {
    prefix: &'a [u8],
    /// The keys are `kv_store_*` keys ([`mqtt_rules::state::term_key`]): the prefix is
    /// of a string key's bytes, and a key that is not a string has none.
    terms: bool,
}

/// What comes before a string's bytes in its [`mqtt_rules::state::term_key`]: the tag
/// and the length.
const STRING_TERM_HEADER: usize = 9;

/// The tag of a string in a [`mqtt_rules::state::term_key`].
const STRING_TERM_TAG: u8 = 7;

impl<'a> Selector<'a> {
    /// The keys that start with `prefix` (every key, when it is empty).
    #[must_use]
    pub fn prefix(prefix: &'a [u8]) -> Self {
        Self {
            prefix,
            terms: false,
        }
    }

    /// In EMQX's table, whose keys are any term: the string keys that start with
    /// `prefix` (every key of any type, when it is empty).
    #[must_use]
    pub fn string_prefix(prefix: &'a [u8]) -> Self {
        Self {
            prefix,
            terms: true,
        }
    }

    /// Whether the walk selects `key`, which holds a value now. The one place a key
    /// is selected.
    fn matches(&self, key: &[u8]) -> bool {
        if self.prefix.is_empty() {
            return true;
        }
        if self.terms {
            key.first() == Some(&STRING_TERM_TAG)
                && key
                    .get(STRING_TERM_HEADER..)
                    .is_some_and(|text| text.starts_with(self.prefix))
        } else {
            key.starts_with(self.prefix)
        }
    }
}

/// The bounds of one page.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// The most keys.
    pub keys: usize,
    /// The most bytes of keys (one key always fits).
    pub bytes: usize,
    /// How long to look. A page looks through at least one shard however short this is.
    pub time: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            keys: MAX_PAGE_KEYS,
            bytes: MAX_PAGE_BYTES,
            time: PAGE_TIME,
        }
    }
}

/// A place in a namespace's listing: after `after` in shard `shard`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    instance: u64,
    shard: usize,
    after: Vec<u8>,
}

impl Cursor {
    /// The cursor as text that needs no escaping in a query string.
    #[must_use]
    pub fn encode(&self) -> String {
        use std::fmt::Write;
        let mut out = format!("1-{:016x}-{}-", self.instance, self.shard);
        for byte in &self.after {
            let _ = write!(out, "{byte:02x}");
        }
        out
    }

    /// The cursor [`encode`](Self::encode) wrote; `None` for any other text.
    #[must_use]
    pub fn decode(text: &str) -> Option<Self> {
        let mut parts = text.splitn(4, '-');
        let (version, instance, shard, after) =
            (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
        if version != "1" || instance.len() != 16 || after.len() % 2 != 0 || !after.is_ascii() {
            return None;
        }
        let instance = u64::from_str_radix(instance, 16).ok()?;
        let shard = shard.parse::<usize>().ok().filter(|at| *at < SHARDS)?;
        let after = after
            .as_bytes()
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
            .collect::<Option<Vec<u8>>>()?;
        Some(Self {
            instance,
            shard,
            after,
        })
    }
}

/// One listed key. A struct, so that what a listing says of a key can grow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyInfo {
    /// The key's bytes, as the store holds them.
    pub key: Arc<[u8]>,
    /// The type of the operation that created it (D4).
    pub type_name: &'static str,
}

/// One page of a listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Page {
    /// The keys, in the listing's order.
    pub keys: Vec<KeyInfo>,
    /// Where the next page starts; `None` when this was the last. A page can be short,
    /// or empty, and still have one.
    pub next: Option<Cursor>,
}

/// One key, as an operator reads it.
#[derive(Debug, Clone)]
pub struct KeyView {
    /// The type of the operation that created it (D4).
    pub type_name: &'static str,
    /// What `kv_get` would read: the value, a counter's count, a set's elements, a
    /// sketch's estimate.
    pub value: Value,
    /// When it expires, in milliseconds since the Unix epoch; `0`: never.
    pub expires_ms: u64,
}

/// Why a listing was not made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeysError {
    /// No loaded rule set holds a namespace of that name.
    NoNamespace,
    /// The cursor is of another namespace of that name: one that was dropped since.
    StaleCursor,
}

impl Namespace {
    /// Count a refused call under its operation and reason.
    pub(super) fn count_failed(&self, op: &StateOp<'_>, reason: &str) {
        let reason = RESULTS[1..]
            .iter()
            .position(|known| *known == reason)
            .unwrap_or(REFUSALS - 1);
        self.failed[op_index(op) * REFUSALS + reason].fetch_add(1, Relaxed);
    }

    /// The namespace's counts: each shard's lock once for its numbers, and again for
    /// each [`SCAN_BATCH`] of its keys whose expiry has passed.
    fn counts(&self) -> NamespaceCounts {
        let now_ms = self.shared.clock.now_ms();
        let mut counts = NamespaceCounts {
            instance: self.instance,
            keys: super::wide(self.keys.load(Relaxed)),
            bytes: self.bytes.load(Relaxed),
            evicted: self.evicted.load(Relaxed),
            deleted: self.admin_deleted.load(Relaxed),
            ..NamespaceCounts::default()
        };
        for (op, by_reason) in counts.failed.iter_mut().enumerate() {
            for (reason, n) in by_reason.iter_mut().enumerate() {
                *n = self.failed[op * REFUSALS + reason].load(Relaxed);
            }
        }
        if let Some(replica) = &self.replica {
            (counts.ok, counts.expired, counts.expiry_backlog) = replica.counts(self);
            return counts;
        }
        for shard in &self.shards {
            let mut from: Option<Due> = None;
            let mut first = true;
            loop {
                let shard = lock_shard(shard);
                if std::mem::take(&mut first) {
                    for (total, n) in counts.ok.iter_mut().zip(&shard.ops) {
                        *total += n;
                    }
                    counts.expired += shard.expired;
                    // The keys held for their expiry events: out of the map, and
                    // still to be removed.
                    counts.expiry_backlog += super::wide(shard.held.len());
                }
                let (due, last) = due_in(&shard, from.as_ref(), now_ms);
                counts.expiry_backlog += super::wide(due);
                match last {
                    Some(last) if due == SCAN_BATCH => from = Some(last),
                    _ => break,
                }
            }
        }
        counts
    }

    /// One hold of shard `at`'s lock for a listing: `each` is shown the next `batch`
    /// keys of its map after the first `seen` that hold a value at `now_ms`, wherever
    /// the namespace keeps its keys. How many were looked at; `None` when the map
    /// changed since the hold that set `changes`.
    fn look(
        &self,
        at: usize,
        seen: usize,
        batch: usize,
        changes: &mut Option<u64>,
        now_ms: u64,
        each: &mut dyn FnMut(&Arc<[u8]>, &'static str),
    ) -> Option<usize> {
        if let Some(replica) = &self.replica {
            return replica.look(at, seen, batch, changes, now_ms, each);
        }
        let shard = lock_shard(&self.shards[at]);
        if *changes.get_or_insert(shard.changes) != shard.changes {
            return None;
        }
        let mut looked = 0;
        for entry in shard.map.values().skip(seen).take(batch) {
            looked += 1;
            if !entry.expired(now_ms) {
                each(&entry.key, entry.data.type_name());
            }
        }
        Some(looked)
    }

    /// The time the namespace judges expiries by.
    fn now_ms(&self) -> u64 {
        if self.replica.is_some() {
            Replica::now_ms(self)
        } else {
            self.shared.clock.now_ms()
        }
    }

    /// Every key of shard `at` after `after` that `selector` selects, the smallest
    /// `keep` of them at least, in no order. `None` when the shard's map changed
    /// between two holds of its lock: what was gathered may then lack a key.
    ///
    /// With `batch` keys looked at per hold; `usize::MAX` is the whole shard under one.
    fn gather(
        &self,
        at: usize,
        selector: &Selector<'_>,
        after: Option<&[u8]>,
        keep: usize,
        batch: usize,
        now_ms: u64,
    ) -> Option<Vec<KeyInfo>> {
        let by_key = |a: &KeyInfo, b: &KeyInfo| a.key.cmp(&b.key);
        let mut found: Vec<KeyInfo> = Vec::new();
        let (mut seen, mut changes) = (0usize, None);
        loop {
            // Room for a whole batch, made with no lock held.
            found.reserve(batch.min(1024));
            let mut each = |key: &Arc<[u8]>, type_name: &'static str| {
                if after.is_none_or(|after| &**key > after) && selector.matches(key) {
                    found.push(KeyInfo {
                        key: key.clone(),
                        type_name,
                    });
                }
            };
            let looked = self.look(at, seen, batch, &mut changes, now_ms, &mut each)?;
            seen += looked;
            // What cannot be among the first `keep` is let go as the walk goes, so a
            // large shard does not become a large list.
            if found.len() >= keep.saturating_mul(2).max(1024) {
                found.select_nth_unstable_by(keep - 1, by_key);
                found.truncate(keep);
                // One more than asked stays: it says there are more.
            }
            if looked < batch {
                return Some(found);
            }
        }
    }

    /// The first `want` keys of shard `at` after `after` that `selector` selects, in
    /// order of their bytes, cut where they pass `room` bytes (one key always fits);
    /// and whether that was every such key the shard has.
    fn next_in(
        &self,
        at: usize,
        selector: &Selector<'_>,
        after: Option<&[u8]>,
        want: usize,
        room: usize,
        now_ms: u64,
    ) -> (Vec<KeyInfo>, bool) {
        // One more than wanted, to know whether there are more.
        let keep = want.saturating_add(1);
        let mut found = (0..WALK_RETRIES)
            .find_map(|_| self.gather(at, selector, after, keep, WALK_BATCH, now_ms))
            .or_else(|| self.gather(at, selector, after, keep, usize::MAX, now_ms))
            .unwrap_or_default();
        found.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let mut more = found.len() > want;
        found.truncate(want);
        let mut bytes = 0usize;
        let fits = found
            .iter()
            .take_while(|listed| {
                bytes += listed.key.len();
                bytes <= room
            })
            .count()
            .max(1)
            .min(found.len());
        more |= fits < found.len();
        found.truncate(fits);
        (found, !more)
    }

    /// The next keys `selector` selects, in the listing's order, from `from` on.
    ///
    /// The one walk over a namespace's keys: shard by shard, a few keys of a shard
    /// per hold of its lock.
    fn select(&self, selector: &Selector<'_>, from: Option<&Cursor>, limits: &Limits) -> Page {
        let now_ms = self.now_ms();
        let started = Instant::now();
        let (mut at, mut after) = match from {
            Some(cursor) => (cursor.shard, Some(cursor.after.clone())),
            None => (0, None),
        };
        let mut page = Page::default();
        let mut bytes = 0usize;
        while at < SHARDS {
            let want = limits.keys.saturating_sub(page.keys.len()).max(1);
            let room = limits.bytes.saturating_sub(bytes);
            let (keys, whole) = self.next_in(at, selector, after.as_deref(), want, room, now_ms);
            bytes += keys.iter().map(|k| k.key.len()).sum::<usize>();
            if let Some(last) = keys.last() {
                after = Some(last.key.to_vec());
            }
            page.keys.extend(keys);
            if whole {
                // Nothing more in this shard: the next one, from its start.
                at += 1;
                after = None;
            }
            let full = page.keys.len() >= limits.keys || bytes >= limits.bytes;
            if at < SHARDS && (full || !whole || started.elapsed() >= limits.time) {
                page.next = Some(Cursor {
                    instance: self.instance,
                    shard: at,
                    after: after.unwrap_or_default(),
                });
                break;
            }
        }
        page
    }
}

/// An entry of a shard's order of expiry: the second it falls in, and the key.
type Due = (u64, Arc<[u8]>);

/// How many entries of `shard`'s order of expiry after `from` are due at `now_ms`, at
/// most [`SCAN_BATCH`], and the last of them.
fn due_in(shard: &Shard, from: Option<&Due>, now_ms: u64) -> (usize, Option<Due>) {
    let range = match from {
        Some(from) => shard
            .expiry
            .range::<Due, _>((Bound::Excluded(from), Bound::Unbounded)),
        None => shard.expiry.range::<Due, _>(..),
    };
    let mut due = 0;
    let mut last = None;
    for entry in range.take(SCAN_BATCH) {
        if entry.0.saturating_mul(1000) > now_ms {
            break;
        }
        due += 1;
        last = Some(entry);
    }
    (due, last.cloned())
}

impl Store {
    /// The namespace `name`, while a loaded rule set holds it.
    fn namespace(&self, name: &str) -> Option<Arc<Namespace>> {
        lock(&self.namespaces).get(name)?.upgrade()
    }

    /// What the store holds as a whole.
    #[must_use]
    pub fn totals(&self) -> StoreTotals {
        StoreTotals {
            bytes: self.shared.bytes.load(Relaxed),
            max_bytes: self.shared.max_bytes,
            brownout: self.shared.brownout.load(Relaxed),
            dictionary_bytes: self.dictionary_bytes.load(Relaxed),
            dictionary_refused: self.dictionary_refused.load(Relaxed),
        }
    }

    /// One namespace's counts; `None` when no loaded rule set holds it.
    #[must_use]
    pub fn counts(&self, namespace: &str) -> Option<NamespaceCounts> {
        Some(self.namespace(namespace)?.counts())
    }

    /// One page of the keys of `namespace` that `selector` selects, from `from` on
    /// (the start, for `None`), within `limits`. See the module's documentation for
    /// the order and for what a write does to a listing that is under way.
    ///
    /// # Errors
    /// No such namespace, or a cursor of another namespace of that name.
    pub fn keys(
        &self,
        namespace: &str,
        selector: &Selector<'_>,
        from: Option<&Cursor>,
        limits: &Limits,
    ) -> Result<Page, KeysError> {
        let ns = self.namespace(namespace).ok_or(KeysError::NoNamespace)?;
        if from.is_some_and(|cursor| cursor.instance != ns.instance) {
            return Err(KeysError::StaleCursor);
        }
        Ok(ns.select(selector, from, limits))
    }

    /// What `key` of `namespace` holds: `Ok(None)` when there is no such key, or its
    /// expiry has passed.
    ///
    /// # Errors
    /// No such namespace.
    pub fn key(&self, namespace: &str, key: &[u8]) -> Result<Option<KeyView>, KeysError> {
        let ns = self.namespace(namespace).ok_or(KeysError::NoNamespace)?;
        if let Some(replica) = &ns.replica {
            return Ok(replica
                .peek(&ns, key)
                .map(|(type_name, value, expires_ms)| KeyView {
                    type_name,
                    value,
                    expires_ms,
                }));
        }
        let now_ms = ns.shared.clock.now_ms();
        let shard = lock_shard(&ns.shards[ns.shard_of(key)]);
        Ok(shard
            .map
            .get(key)
            .filter(|entry| !entry.expired(now_ms))
            .map(|entry| KeyView {
                type_name: entry.data.type_name(),
                value: entry.data.read(),
                expires_ms: entry.expires_ms,
            }))
    }

    /// Delete `key` of `namespace`, whatever its type: an operator's delete (D10).
    /// Whether a value was there.
    ///
    /// In an `eventual` namespace it is a replicated delete, exactly a rule's
    /// `kv_del`: stamped by this node's clock, kept as a tombstone for the namespace's
    /// tombstone window and sent to every member. A key that holds nothing here is
    /// left alone, as `kv_del` leaves it: no tombstone is made for a key this node
    /// does not hold.
    ///
    /// # Errors
    /// No such namespace.
    pub fn delete_key(&self, namespace: &str, key: &[u8]) -> Result<bool, KeysError> {
        let ns = self.namespace(namespace).ok_or(KeysError::NoNamespace)?;
        if ns.replica.is_some() {
            // A delete makes no key and sets no expiry, so it reads no bound.
            let handle = Handle {
                settings: NamespaceSettings {
                    consistency: Consistency::Eventual,
                    ..NamespaceSettings::defaults(self.shared.max_bytes)
                },
                namespace: ns,
            };
            let deleted = handle
                .namespace
                .replica
                .as_ref()
                .and_then(|replica| replica.apply(&handle, &StateOp::Del { key }).ok())
                .and_then(|reply| reply.value)
                .is_some_and(|was| was.is_true());
            if deleted {
                handle.namespace.admin_deleted.fetch_add(1, Relaxed);
            }
            return Ok(deleted);
        }
        let now_ms = ns.shared.clock.now_ms();
        let at = ns.shard_of(key);
        let mut shard = lock_shard(&ns.shards[at]);
        let expired = shard.map.get(key).is_some_and(|e| e.expired(now_ms));
        let removed = if expired {
            // It read as absent already: its expiry removes it, not the operator, and
            // where a rule selects the expiry event it is held for that.
            ns.expire(&mut shard, key);
            true
        } else {
            ns.remove(&mut shard, key)
        };
        ns.note_oldest(at, &shard);
        drop(shard);
        let deleted = removed && !expired;
        if deleted {
            ns.admin_deleted.fetch_add(1, Relaxed);
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests;
