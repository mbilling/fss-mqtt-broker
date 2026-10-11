//! The sweep that removes expired keys, and what `$events/state/expired` is raised
//! from ([ADR 0085](../../../../docs/adr/0085-rule-state-store.md) D7).
//!
//! # Which namespaces
//!
//! `local` ones: the keys this file's sweep walks are those of
//! [`Namespace::shards`](super::Namespace). An `eventual` namespace keeps its keys
//! apart ([`replica`](super::replica)), expires them on every member at once, and
//! raises nothing yet: which member raises the event there, and holding a replicated
//! key for it, is the cluster half of D7 and is not built.
//!
//! # Finding what expired
//!
//! Each shard already keeps its keys in order of expiry, to the second
//! ([`Shard::expiry`](super::Shard)). A sweep ([`Store::sweep`]) reads every shard's
//! earliest second under that shard's lock, one lock at a time and for that one read,
//! and then takes keys out of the shards that have some due, earliest second first:
//! at most [`PURGE_BATCH`] keys under one hold of a lock, and nothing but taking them
//! out of the maps is done under it. Their values are read, their room is given back
//! and they are dropped after the lock is released. A sweep therefore costs the
//! number of shards plus the number of keys that expired, never the number of keys
//! held, and a rule's call waits for one batch at the most.
//!
//! # Without a rule on the event
//!
//! Nothing here is on a call's path. A key found expired is removed by the call that
//! finds it, as before, and the sweep removes the rest, all that is due, with no
//! value read and no pacing.
//!
//! # With one: held, late rather than lost
//!
//! While a loaded rule selects the event ([`Store::set_expiry_events`]), an expired
//! key is removed **only by the sweep**, as its event is handed over:
//!
//! * It **reads as absent** from the moment it expires, whoever asks.
//! * A call that finds it (a read, a write of the same key, an eviction) takes it out
//!   of the map and **holds** it in its shard, in order of expiry. A write then
//!   creates the key anew; the old one still raises its event, with the value it
//!   held.
//! * Until its event is handed over it **counts toward `max_keys`, `max_bytes` and
//!   the store's ceiling.** That is the bound on the backlog: a namespace never owes
//!   more events than it may hold keys. A full namespace whose keys have expired
//!   refuses new keys (`namespace_full`) until the sweep has caught up; expired keys
//!   are not "room" while their events are owed.
//! * The sweep hands over at most `rules.state_expiry_per_sec` a second, a tenth of a
//!   second's worth at a time ([`Pace`]), earliest second first. Within one second
//!   the order is the shards', not the milliseconds'.
//! * **None is shed.** What ends a backlog without events: the namespace is dropped
//!   by a reload (D1: with its keys), no rule selects the event any more (the held
//!   keys are then simply removed), or the process stops (the store is in memory:
//!   nothing is raised after a restart for what expired before it).
//! * An `lru` eviction of a key that has **not** expired is not an expiry and raises
//!   nothing.
//!
//! # A rule on the event may write state
//!
//! Its calls go through the store like any rule's, the expired key included. A key
//! lives at least a second (the shortest TTL), a sweep takes a bounded number of keys
//! and evaluates after it has let go of every lock, and the pace bounds events a
//! second, so a rule that re-creates the key it is told about gets one event a second
//! for it and cannot make the sweep spin.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Weak};

use mqtt_rules::state::{remembered, KV_STORE_NAMESPACE};
use mqtt_rules::Value;

use super::{
    expiry_slot, lock, lock_shard, wide, Entry, Namespace, Shard, Store, NOT_IN_ORDER, PURGE_BATCH,
};

/// A key the sweep removed at its expiry: what `$events/state/expired` reports.
#[derive(Debug, Clone)]
pub struct Expired {
    /// Its namespace: a declared name, or `rule:<id>`.
    pub namespace: Arc<str>,
    /// The key.
    pub key: Arc<[u8]>,
    /// The type its creating operation gave it (`lww`, `first`, `max`, `min`, `sum`,
    /// `union`, `hll`).
    pub kind: &'static str,
    /// What `kv_get` read last: the value, a counter's count, a set's elements, a
    /// sketch's estimate.
    pub value: Value,
    /// When it expired, in the store's milliseconds.
    pub expired_ms: u64,
}

impl Expired {
    fn of(namespace: &Namespace, entry: &Entry) -> Self {
        Self {
            namespace: namespace.name.clone(),
            key: entry.key.clone(),
            kind: entry.data.type_name(),
            value: entry.data.read(),
            expired_ms: entry.expires_ms,
        }
    }
}

/// What one [`Store::sweep`] did.
#[derive(Debug, Default)]
pub struct Swept {
    /// The keys removed whose event is owed, in order of expiry (to the second):
    /// empty unless a rule selects the event.
    pub expired: Vec<Expired>,
    /// The keys removed with no event owed.
    pub removed: usize,
    /// Whether more keys are due now than the sweep was allowed to take.
    pub more: bool,
    /// When the earliest key it saw that is not due yet will be, in the store's
    /// milliseconds; `None` when no key it saw expires.
    pub next_due_ms: Option<u64>,
}

/// The pace of expiry events: at most `per_sec` a second, a tenth of a second's worth
/// at a time (one at least), so a mass expiry is handed over evenly and never as one
/// burst of a whole second's allowance.
#[derive(Debug, Clone)]
pub struct Pace {
    per_sec: u64,
    /// What may be handed over now, in thousandths of an event.
    credit: u64,
    last_ms: Option<u64>,
}

impl Pace {
    /// A pace of `per_sec` events a second (one at least).
    #[must_use]
    pub fn new(per_sec: u32) -> Self {
        let per_sec = u64::from(per_sec.max(1));
        Self {
            per_sec,
            credit: 0,
            last_ms: None,
        }
    }

    /// The most that waits to be spent: a tenth of a second's events, one at least.
    fn cap(&self) -> u64 {
        (self.per_sec * 100).max(1000)
    }

    /// How many events may be handed over at `now_ms`.
    pub fn allow(&mut self, now_ms: u64) -> usize {
        let earned = match self.last_ms {
            // The first step is a full one.
            None => self.cap(),
            Some(last) => now_ms.saturating_sub(last).saturating_mul(self.per_sec),
        };
        self.last_ms = Some(self.last_ms.map_or(now_ms, |last| last.max(now_ms)));
        self.credit = self.credit.saturating_add(earned).min(self.cap());
        usize::try_from(self.credit / 1000).unwrap_or(usize::MAX)
    }

    /// `n` events were handed over.
    pub fn spend(&mut self, n: usize) {
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        self.credit = self.credit.saturating_sub(n.saturating_mul(1000));
    }

    /// Milliseconds until a full step may be handed over again.
    #[must_use]
    pub fn wait_ms(&self) -> u64 {
        (self.cap() - self.credit.min(self.cap()))
            .div_ceil(self.per_sec)
            .max(1)
    }
}

impl Shard {
    /// Hold an expired `entry`, already out of the map, for its event.
    fn hold(&mut self, entry: Entry) {
        self.held_seq += 1;
        self.held
            .insert((expiry_slot(entry.expires_ms), self.held_seq), entry);
    }

    /// The second the earliest key that expires, or has expired and is held, falls in.
    fn first_expiry(&self) -> Option<u64> {
        let live = self.expiry.first().map(|(slot, _)| *slot);
        let held = self.held.first_key_value().map(|((slot, _), _)| *slot);
        match (live, held) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Take out the key that expired earliest, if the second it expired in is `slot`
    /// or before: a held one first, then one still in the map.
    fn pop_expired(&mut self, slot: u64) -> Option<Entry> {
        loop {
            let live = self.expiry.first().map(|(at, _)| *at);
            let held = self.held.first_key_value().map(|(at, _)| *at);
            if let Some(at) = held.filter(|at| at.0 <= slot && live.is_none_or(|l| at.0 <= l)) {
                return self.held.remove(&at);
            }
            if live.is_none_or(|at| at > slot) {
                return None;
            }
            let (at, key) = self.expiry.pop_first()?;
            // The order names a key the map holds with that expiry; anything else is
            // dropped from the order and the next one looked at.
            if self
                .map
                .get(&*key)
                .is_some_and(|e| expiry_slot(e.expires_ms) == at)
            {
                return self.take(&key);
            }
        }
    }
}

impl Namespace {
    /// `key` was found past its expiry: take it out of the map. Where a rule selects
    /// the expiry event it is held for the sweep, still accounted; otherwise its room
    /// is given back now.
    pub(super) fn expire(&self, shard: &mut Shard, key: &[u8]) {
        let Some(entry) = shard.take(key) else {
            return;
        };
        // A remembered idempotency key (D8) raises no event, so nothing is owed for it.
        if self.shared.expiry_events.load(Relaxed) && remembered(&entry.key).is_none() {
            shard.hold(entry);
        } else {
            self.release(&entry);
            shard.expired += 1;
        }
    }

    /// Before an `lru` eviction from `shard`: its least recently used keys that have
    /// expired are held for their events, where those are owed, so that what is
    /// evicted next is a key that has not expired. Whether any was held.
    pub(super) fn hold_expired_oldest(&self, shard: &mut Shard) -> bool {
        if !self.shared.expiry_events.load(Relaxed) {
            return false;
        }
        let now_ms = self.shared.clock.now_ms();
        let mut any = false;
        for _ in 0..PURGE_BATCH {
            let Some(key) = shard.used.first_key_value().map(|(_, key)| key.clone()) else {
                break;
            };
            if !shard.map.get(&*key).is_some_and(|e| e.expired(now_ms)) {
                break;
            }
            self.expire(shard, &key);
            any = true;
        }
        any
    }
}

/// The shards with a key due: (the second, the namespace, the shard), earliest first.
type Due = BinaryHeap<Reverse<(u64, usize, usize)>>;

/// Sort a shard by the second its earliest key expires in: due now, or the next the
/// sweep has to come back for.
fn sort(due: &mut Due, swept: &mut Swept, now_ms: u64, first: Option<u64>, at: (usize, usize)) {
    let Some(slot) = first else {
        return;
    };
    let due_ms = slot.saturating_mul(1000);
    if due_ms <= now_ms {
        due.push(Reverse((slot, at.0, at.1)));
    } else {
        swept.next_due_ms = Some(swept.next_due_ms.map_or(due_ms, |ms| ms.min(due_ms)));
    }
}

impl Store {
    /// Whether a loaded rule selects `$events/state/expired`. While one does, expired
    /// keys are held until [`sweep`](Self::sweep) returns them; while none does, they
    /// are removed by whoever finds them and the sweep returns none.
    pub fn set_expiry_events(&self, wanted: bool) {
        self.shared.expiry_events.store(wanted, Relaxed);
    }

    /// Whether expired keys are held for their events now.
    #[must_use]
    pub fn expiry_events(&self) -> bool {
        self.shared.expiry_events.load(Relaxed)
    }

    /// The most expiry events handed over a second (`rules.state_expiry_per_sec`).
    pub fn set_expiry_per_sec(&self, per_sec: u32) {
        self.shared.expiry_per_sec.store(per_sec.max(1), Relaxed);
    }

    /// The most expiry events handed over a second.
    #[must_use]
    pub fn expiry_per_sec(&self) -> u32 {
        self.shared.expiry_per_sec.load(Relaxed)
    }

    /// The store's time now, in milliseconds since the Unix epoch.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.shared.clock.now_ms()
    }

    /// Whether any loaded rule set holds a namespace: with none there is nothing to
    /// sweep.
    #[must_use]
    pub fn has_namespaces(&self) -> bool {
        lock(&self.namespaces)
            .values()
            .any(|ns| ns.strong_count() > 0)
    }

    /// Remove keys whose expiry has passed, at most `max` of them, earliest second
    /// first across every namespace. Where a rule selects the expiry event they are
    /// returned for it, with the value each held; otherwise they are only counted.
    /// Their room is given back here: the caller raises the events next, and nothing
    /// else holds what is returned.
    ///
    /// One shard's lock at a time: once to read its earliest second, and again for
    /// each batch of at most [`PURGE_BATCH`] keys taken out of it. Values are read
    /// and dropped after the lock is released.
    #[must_use]
    pub fn sweep(&self, max: usize) -> Swept {
        let now_ms = self.shared.clock.now_ms();
        let events = self.shared.expiry_events.load(Relaxed);
        let namespaces: Vec<Arc<Namespace>> = lock(&self.namespaces)
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        let mut swept = Swept::default();
        // (the second, the namespace, the shard) of every shard with a key due.
        let mut due: Due = BinaryHeap::new();
        for (n, namespace) in namespaces.iter().enumerate() {
            if namespace.keys.load(Relaxed) == 0 {
                continue;
            }
            for (at, shard) in namespace.shards.iter().enumerate() {
                let first = lock_shard(shard).first_expiry();
                sort(&mut due, &mut swept, now_ms, first, (n, at));
            }
        }
        let mut left = max;
        let mut taken: Vec<(usize, Entry)> = Vec::new();
        while let Some(Reverse((slot, n, at))) = due.pop() {
            if left == 0 {
                swept.more = true;
                break;
            }
            let namespace = &namespaces[n];
            let mut shard = lock_shard(&namespace.shards[at]);
            let before = taken.len();
            // What counts toward `max`: every key, or, where events are raised, the
            // keys that raise one. A remembered idempotency key (D8) raises none, and
            // the events' pace must not hold up its removal.
            let mut counted = 0;
            while taken.len() - before < PURGE_BATCH && counted < left {
                let Some(entry) = shard.pop_expired(slot) else {
                    break;
                };
                counted += usize::from(!events || remembered(&entry.key).is_none());
                taken.push((n, entry));
            }
            left -= counted;
            shard.expired += wide(taken.len() - before);
            let first = shard.first_expiry();
            if !shard.used.is_empty() || namespace.oldest[at].load(Relaxed) != NOT_IN_ORDER {
                namespace.note_oldest(at, &shard);
            }
            drop(shard);
            sort(&mut due, &mut swept, now_ms, first, (n, at));
        }
        for (n, entry) in &taken {
            let namespace = &namespaces[*n];
            namespace.release(entry);
            if events && remembered(&entry.key).is_none() {
                swept.expired.push(Expired::of(namespace, entry));
            } else {
                swept.removed += 1;
            }
        }
        swept
    }

    /// Take `key` of `namespace` out if its expiry has passed, wherever it is (still
    /// in the map, or held for its event), and return what its event reports. For a
    /// replayed sequence (`mqttd --rule-test --sequence`), which raises each key's
    /// event at the key's own expiry time and so asks for them one by one; the
    /// broker's sweep is [`sweep`](Self::sweep). A remembered idempotency key (D8) is
    /// taken out like any key and reports nothing: it raises no event.
    #[must_use]
    pub fn take_expired(&self, namespace: &str, key: &[u8]) -> Option<Expired> {
        let namespace = lock(&self.namespaces).get(namespace)?.upgrade()?;
        let now_ms = self.shared.clock.now_ms();
        let mut shard = lock_shard(&namespace.shards[namespace.shard_of(key)]);
        let entry = if shard.map.get(key).is_some_and(|e| e.expired(now_ms)) {
            shard.take(key)
        } else {
            let at = shard
                .held
                .iter()
                .find(|(_, entry)| &*entry.key == key)
                .map(|(at, _)| *at);
            at.and_then(|at| shard.held.remove(&at))
        }?;
        shard.expired += 1;
        drop(shard);
        namespace.release(&entry);
        remembered(&entry.key)
            .is_none()
            .then(|| Expired::of(&namespace, &entry))
    }

    /// Make `key` of `namespace` expire now, whatever expiry it had: it reads as
    /// absent at once, and where a rule selects `$events/state/expired` the sweep
    /// raises its event at the sweep's pace, as for any expired key, with this moment
    /// as `expired_at`. Returns whether the key was there.
    ///
    /// **The entry point for an operator's purge that asks for events** (ADR 0085,
    /// amended 2026-10-11: `events=true`, T13). A purge that asks for none deletes
    /// its keys instead. EMQX's `kv_store` table has no expiry and raises none, so
    /// its keys are left alone here.
    pub fn expire_now(&self, namespace: &str, key: &[u8]) -> bool {
        if namespace == KV_STORE_NAMESPACE {
            return false;
        }
        let Some(namespace) = lock(&self.namespaces)
            .get(namespace)
            .and_then(Weak::upgrade)
        else {
            return false;
        };
        let now_ms = self.shared.clock.now_ms();
        let at = namespace.shard_of(key);
        let mut shard = lock_shard(&namespace.shards[at]);
        let Some(entry) = shard.map.get_mut(key) else {
            return false;
        };
        // A key already past its expiry keeps the time it expired at.
        if !entry.expired(now_ms) {
            let was = (expiry_slot(entry.expires_ms), entry.key.clone());
            let had = entry.expires_ms != 0;
            entry.expires_ms = now_ms.max(1);
            let now = (expiry_slot(entry.expires_ms), entry.key.clone());
            if had {
                shard.expiry.remove(&was);
            }
            shard.expiry.insert(now);
        }
        namespace.expire(&mut shard, key);
        if !shard.used.is_empty() || namespace.oldest[at].load(Relaxed) != NOT_IN_ORDER {
            namespace.note_oldest(at, &shard);
        }
        true
    }
}

#[cfg(test)]
mod tests;
