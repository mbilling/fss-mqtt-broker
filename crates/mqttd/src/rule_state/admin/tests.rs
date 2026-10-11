//! What an operator sees and does in the store (ADR 0085 D10): counts, listings, one
//! key, a delete.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use mqtt_rules::state::{
    term_key, Eviction, NamespaceSettings, StateNamespace, StateOp, StateStore,
};
use mqtt_rules::Value;

use super::super::{stored, Store, ENTRY_OVERHEAD, REFUSALS, SHARDS};
use super::{
    ByExpiry, Cursor, KeyInfo, KeysError, Limits, Page, Selector, Values, OPS, RESULTS, SCAN_BATCH,
    WALK_BATCH,
};

const CEILING: u64 = 256 << 20;

/// A store on a clock the test drives, with one namespace `n` open.
struct Rig {
    store: Arc<Store>,
    clock: Arc<AtomicU64>,
    ns: Arc<dyn StateNamespace>,
}

fn rig_with(settings: &NamespaceSettings) -> Rig {
    let clock = Arc::new(AtomicU64::new(1_800_000_000_000));
    let store = Store::with_manual_clock(CEILING, clock.clone());
    let ns = store.open("n", settings).unwrap();
    Rig { store, clock, ns }
}

fn rig() -> Rig {
    rig_with(&NamespaceSettings {
        default_ttl_secs: None,
        ..NamespaceSettings::defaults(CEILING)
    })
}

impl Rig {
    fn set(&self, key: &[u8], ttl: Option<u64>) {
        self.ns
            .call(&StateOp::Set {
                key,
                value: stored(&Value::Int(1)),
                ttl,
            })
            .unwrap();
    }

    fn del(&self, key: &[u8]) {
        self.ns.call(&StateOp::Del { key }).unwrap();
    }

    fn page(&self, selector: &Selector<'_>, from: Option<&Cursor>, limits: &Limits) -> Page {
        self.store.keys("n", selector, from, limits).unwrap()
    }

    /// Every page of a listing, start to end: the keys in the order listed, and how
    /// many pages it took.
    fn listing(&self, selector: &Selector<'_>, limits: &Limits) -> (Vec<Vec<u8>>, usize) {
        let (mut keys, mut pages, mut from) = (Vec::new(), 0, None);
        loop {
            let page = self.page(selector, from.as_ref(), limits);
            pages += 1;
            assert!(
                page.keys.len() <= limits.keys,
                "a page holds at most its limit"
            );
            keys.extend(page.keys.iter().map(|k| k.key.to_vec()));
            match page.next {
                Some(next) => from = Some(next),
                None => return (keys, pages),
            }
            assert!(
                pages <= 10 * SHARDS + keys.len(),
                "the listing does not end"
            );
        }
    }

    /// Where `key` lies in a listing's order.
    fn place(&self, key: &[u8]) -> (usize, Vec<u8>) {
        let ns = self.store.namespace("n").unwrap();
        (ns.shard_of(key), key.to_vec())
    }
}

fn limit(keys: usize) -> Limits {
    Limits {
        keys,
        ..Limits::default()
    }
}

fn all() -> Selector<'static> {
    Selector::prefix(b"")
}

fn name(i: usize) -> Vec<u8> {
    format!("key:{i:05}").into_bytes()
}

/// A listing pages through every key exactly once, at most a page's keys at a time, in
/// an order that is the same each time; and a prefix selects.
#[test]
fn a_listing_pages_through_every_key_exactly_once() {
    let r = rig();
    assert_eq!(r.listing(&all(), &limit(100)), (Vec::new(), 1));
    for i in 0..5_000 {
        r.set(&name(i), None);
    }
    r.set(b"other", None);
    r.set(b"\xff\x00 not text", None);
    let (keys, pages) = r.listing(&all(), &limit(100));
    assert_eq!(keys.len(), 5_002);
    assert!(pages >= 51, "{pages} pages of at most 100 keys");
    let distinct: BTreeSet<&Vec<u8>> = keys.iter().collect();
    assert_eq!(distinct.len(), 5_002, "no key is listed twice");
    assert!(distinct.contains(&b"\xff\x00 not text".to_vec()));
    // The order is by shard, then by the key's bytes; and it is the same again.
    let places: Vec<(usize, Vec<u8>)> = keys.iter().map(|k| r.place(k)).collect();
    assert!(places.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(r.listing(&all(), &limit(1000)).0, keys);
    assert_eq!(r.listing(&all(), &limit(7)).0, keys);
    // The largest page is 1,000 keys.
    let first = r.page(&all(), None, &Limits::default());
    assert_eq!(first.keys.len(), 1000);
    assert!(first.next.is_some());
    // A prefix.
    let (some, _) = r.listing(&Selector::prefix(b"key:0000"), &limit(3));
    assert_eq!(
        some.iter().collect::<BTreeSet<_>>(),
        (0..10).map(name).collect::<Vec<_>>().iter().collect()
    );
    assert_eq!(
        r.listing(&Selector::prefix(b"oth"), &limit(3)).0,
        [b"other"]
    );
    assert_eq!(r.listing(&Selector::prefix(b"nope"), &limit(3)).0.len(), 0);
    // Each key says its type.
    r.ns.call(&StateOp::Incr {
        key: b"counter",
        by: 1,
        ttl: None,
    })
    .unwrap();
    let types: BTreeMap<Vec<u8>, &str> = r
        .page(&Selector::prefix(b"o"), None, &limit(10))
        .keys
        .into_iter()
        .chain(r.page(&Selector::prefix(b"counter"), None, &limit(10)).keys)
        .map(|k| (k.key.to_vec(), k.type_name))
        .collect();
    assert_eq!(types[&b"other".to_vec()], "lww");
    assert_eq!(types[&b"counter".to_vec()], "sum");
}

/// What a write does to a listing that is under way. A key that is there from the
/// first page to the last is listed exactly once; no key is listed twice; a key
/// inserted meanwhile is listed exactly when its place is after the cursor; a key
/// deleted before the listing reaches its place is not listed, and one deleted after
/// it was listed stays listed.
#[test]
fn a_write_during_a_listing_neither_repeats_nor_hides_a_key_that_stays() {
    let r = rig();
    for i in 0..2_000 {
        r.set(&name(i), None);
    }
    let first = r.page(&all(), None, &limit(500));
    let cursor = first.next.clone().expect("more than one page");
    let at = (cursor.shard, cursor.after.clone());
    let mut listed: Vec<Vec<u8>> = first.keys.iter().map(|k| k.key.to_vec()).collect();
    // Meanwhile: every third key is deleted, and 1,000 new ones are inserted.
    let deleted: BTreeSet<Vec<u8>> = (0..2_000).step_by(3).map(name).collect();
    for key in &deleted {
        r.del(key);
    }
    let inserted: BTreeSet<Vec<u8>> = (0..1_000)
        .map(|i| format!("new:{i}").into_bytes())
        .collect();
    for key in &inserted {
        r.set(key, None);
    }
    // One of the deleted keys comes back, as another value: it is one key.
    let back = name(3);
    r.set(&back, None);
    let mut from = Some(cursor);
    while let Some(cursor) = from {
        let page = r.page(&all(), Some(&cursor), &limit(500));
        listed.extend(page.keys.iter().map(|k| k.key.to_vec()));
        from = page.next;
    }
    let seen: BTreeSet<&Vec<u8>> = listed.iter().collect();
    assert_eq!(seen.len(), listed.len(), "no key is listed twice");
    for i in 0..2_000 {
        let key = name(i);
        let before_cursor = r.place(&key) <= at;
        if !deleted.contains(&key) {
            assert!(seen.contains(&key), "a key that stayed is listed: {i}");
        } else if key == back {
            // Listed once: on the first page, or where it now is, after the cursor.
            assert!(seen.contains(&key));
        } else {
            // Deleted: listed only if the first page had already listed it.
            assert_eq!(seen.contains(&key), before_cursor, "{i}");
        }
    }
    let (mut after, mut before) = (0, 0);
    for key in &inserted {
        let before_cursor = r.place(key) <= at;
        assert_eq!(seen.contains(key), !before_cursor);
        if before_cursor {
            before += 1;
        } else {
            after += 1;
        }
    }
    assert!(after > 0 && before > 0, "both cases were tried");
}

/// The same with the writes on other threads while the listing runs: the keys that
/// stay are each listed once, and nothing is listed twice, in every listing.
#[test]
fn listings_stay_exact_for_stable_keys_while_other_threads_write() {
    const WRITERS: usize = 4;
    const LISTINGS: usize = 20;
    let r = rig();
    let stable: BTreeSet<Vec<u8>> = (0..3_000).map(name).collect();
    for key in &stable {
        r.set(key, None);
    }
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        for t in 0..WRITERS {
            let (ns, stop) = (r.ns.clone(), &stop);
            scope.spawn(move || {
                let mut i = 0usize;
                while !stop.load(Relaxed) {
                    let key = format!("churn:{t}:{}", i % 500).into_bytes();
                    let _ = ns.call(&StateOp::Incr {
                        key: &key,
                        by: 1,
                        ttl: None,
                    });
                    if i.is_multiple_of(3) {
                        let _ = ns.call(&StateOp::Del { key: &key });
                    }
                    i += 1;
                }
            });
        }
        for _ in 0..LISTINGS {
            let (keys, _) = r.listing(&all(), &limit(250));
            let seen: BTreeSet<&Vec<u8>> = keys.iter().collect();
            assert_eq!(seen.len(), keys.len(), "no key is listed twice");
            assert!(stable.iter().all(|k| seen.contains(k)));
            assert!(keys
                .iter()
                .all(|k| stable.contains(k) || k.starts_with(b"churn:")));
        }
        stop.store(true, Relaxed);
    });
}

/// A page is bounded in bytes and in time as well as in keys: one that reaches a bound
/// is short and says where the next starts, and the listing is whole all the same.
#[test]
fn a_page_is_bounded_in_bytes_and_in_time() {
    let r = rig();
    for i in 0..1_500 {
        r.set(&name(i), None);
    }
    let whole = r.listing(&all(), &Limits::default()).0;
    assert_eq!(whole.len(), 1_500);
    // 9 bytes a key: a 90-byte page holds 10.
    let bytes = Limits {
        bytes: 90,
        ..Limits::default()
    };
    let page = r.page(&all(), None, &bytes);
    assert_eq!(page.keys.len(), 10);
    assert!(page.next.is_some());
    assert_eq!(r.listing(&all(), &bytes).0, whole);
    // A key larger than the page's bytes still fits a page of its own.
    let one = Limits {
        bytes: 1,
        ..Limits::default()
    };
    assert_eq!(r.page(&all(), None, &one).keys.len(), 1);
    assert_eq!(r.listing(&all(), &one).0, whole);
    // No time at all: a page looks through one shard, and the listing still ends.
    let hurried = Limits {
        time: Duration::ZERO,
        ..Limits::default()
    };
    let (keys, pages) = r.listing(&all(), &hurried);
    assert_eq!((keys, pages), (whole, SHARDS));
    // A prefix nothing matches, in no time: empty pages, each with a cursor, to the end.
    let (keys, pages) = r.listing(&Selector::prefix(b"nope"), &hurried);
    assert_eq!((keys.len(), pages), (0, SHARDS));
}

/// A cursor is text that needs no escaping, reads back as itself, and is refused when
/// it is anything else, or another namespace's.
#[test]
fn a_cursor_reads_back_and_a_stale_or_broken_one_is_refused() {
    let r = rig();
    for i in 0..300 {
        r.set(&name(i), None);
    }
    r.set(b"\xff\x00/+&=%", None);
    let mut from = None;
    loop {
        let page = r.page(&all(), from.as_ref(), &limit(1));
        let Some(next) = page.next else { break };
        let text = next.encode();
        assert!(
            text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "{text}"
        );
        assert_eq!(Cursor::decode(&text), Some(next.clone()));
        from = Some(next);
    }
    for broken in [
        "",
        "x",
        "1",
        "2-0000000000000000-0-",
        "1-00-0-",
        "1-0000000000000000-64-",
        "1-0000000000000000-x-",
        "1-0000000000000000-0-abc",
        "1-0000000000000000-0-zz",
        "1-0000000000000000-0-é0",
    ] {
        assert_eq!(Cursor::decode(broken), None, "{broken:?}");
    }
    // The namespace is dropped and opened again: its keys are spread anew, and a
    // cursor of the old one is not taken up.
    let cursor = r.page(&all(), None, &limit(10)).next.unwrap();
    let settings = NamespaceSettings::defaults(CEILING);
    let Rig { store, ns, .. } = r;
    drop(ns);
    assert_eq!(
        store.keys("n", &all(), None, &limit(10)).err(),
        Some(KeysError::NoNamespace)
    );
    let _again = store.open("n", &settings).unwrap();
    assert_eq!(
        store.keys("n", &all(), Some(&cursor), &limit(10)).err(),
        Some(KeysError::StaleCursor)
    );
    let again = store.keys("n", &all(), None, &limit(10)).unwrap();
    assert!(again.keys.is_empty() && again.next.is_none());
    assert_eq!(store.key("gone", b"k").err(), Some(KeysError::NoNamespace));
    assert_eq!(store.delete_key("gone", b"k"), Err(KeysError::NoNamespace));
    assert_eq!(store.counts("gone"), None);
}

/// In EMQX's table a key is any term: a prefix is of a string key's text, and a key
/// that is not a string matches none.
#[test]
fn a_prefix_in_emqxs_table_is_of_a_string_keys_text() {
    let r = rig();
    for term in [
        Value::from("dev-1"),
        Value::from("dev-2"),
        Value::from("other"),
        Value::Int(1),
        Value::Float(1.0),
        Value::from(vec![Value::from("dev-3")]),
    ] {
        r.set(&term_key(&term), None);
    }
    assert_eq!(
        r.listing(&Selector::string_prefix(b""), &limit(10)).0.len(),
        6
    );
    let (keys, _) = r.listing(&Selector::string_prefix(b"dev-"), &limit(10));
    assert_eq!(
        keys.iter().collect::<BTreeSet<_>>(),
        [
            term_key(&Value::from("dev-1")),
            term_key(&Value::from("dev-2"))
        ]
        .iter()
        .collect()
    );
    // The same bytes as a plain prefix select by the term's own bytes: nothing.
    assert_eq!(r.listing(&Selector::prefix(b"dev-"), &limit(10)).0.len(), 0);
}

/// Reading is not using: a listing and a read of one key leave an `lru` namespace's
/// order of use as it was, count as no call, and remove nothing.
#[test]
fn reading_a_key_is_not_a_use_and_counts_as_no_call() {
    let r = rig_with(&NamespaceSettings {
        max_keys: 3,
        eviction: Eviction::Lru,
        default_ttl_secs: None,
        ..NamespaceSettings::defaults(CEILING)
    });
    for key in [b"a", b"b", b"c"] {
        r.set(key, None);
    }
    let before = r.store.counts("n").unwrap();
    // The operator reads the oldest key, and lists everything.
    let view = r.store.key("n", b"a").unwrap().unwrap();
    assert_eq!(
        (
            view.type_name,
            view.value.to_json().unwrap(),
            view.expires_ms
        ),
        ("lww", "1".to_string(), 0)
    );
    assert_eq!(r.listing(&all(), &limit(10)).0.len(), 3);
    assert_eq!(r.store.counts("n").unwrap(), before);
    // `a` is still the least recently used: a fourth key evicts it. Had the read been
    // a use, `b` would have gone.
    r.set(b"d", None);
    assert!(r.store.key("n", b"a").unwrap().is_none());
    assert!(r.store.key("n", b"b").unwrap().is_some());
    assert_eq!(r.store.counts("n").unwrap().evicted, 1);
}

/// A key past its expiry reads as absent to an operator too, at once, and stays held
/// until a call of a rule (later, the sweep) removes it: listing and reading remove
/// nothing. It is counted as backlog meanwhile, and as expired when it goes.
#[test]
fn an_expired_key_is_absent_to_an_operator_and_counted_as_backlog() {
    let r = rig();
    r.set(b"short", Some(10));
    r.set(b"long", Some(100));
    r.set(b"never", None);
    let view = r.store.key("n", b"short").unwrap().unwrap();
    assert_eq!(view.expires_ms, r.clock.load(Relaxed) + 10_000);
    let counts = r.store.counts("n").unwrap();
    assert_eq!(
        (counts.keys, counts.expiry_backlog, counts.expired),
        (3, 0, 0)
    );
    r.clock.fetch_add(10_000, Relaxed);
    assert!(r.store.key("n", b"short").unwrap().is_none());
    let (keys, _) = r.listing(&all(), &limit(10));
    assert_eq!(
        keys.iter().collect::<BTreeSet<_>>(),
        [b"long".to_vec(), b"never".to_vec()].iter().collect()
    );
    let counts = r.store.counts("n").unwrap();
    assert_eq!(
        (counts.keys, counts.expiry_backlog, counts.expired),
        (3, 1, 0)
    );
    // An operator's delete of it finds nothing to delete; the key goes as expired.
    assert_eq!(r.store.delete_key("n", b"short"), Ok(false));
    let counts = r.store.counts("n").unwrap();
    assert_eq!(
        (
            counts.keys,
            counts.expiry_backlog,
            counts.expired,
            counts.deleted
        ),
        (2, 0, 1, 0)
    );
    // A rule's call that finds an expired key removes it, and that is counted.
    r.clock.fetch_add(90_000, Relaxed);
    assert_eq!(r.store.counts("n").unwrap().expiry_backlog, 1);
    assert!(r
        .ns
        .call(&StateOp::Get { key: b"long" })
        .unwrap()
        .value
        .is_none());
    let counts = r.store.counts("n").unwrap();
    assert_eq!(
        (counts.keys, counts.expiry_backlog, counts.expired),
        (1, 0, 2)
    );
}

/// The backlog is counted a batch at a time under a shard's lock, and is whole however
/// many keys have expired.
#[test]
fn a_large_backlog_is_counted_whole() {
    const KEYS: usize = 80_000;
    let r = rig();
    for i in 0..KEYS {
        r.set(&name(i), Some(1 + (i % 3) as u64));
    }
    let ns = r.store.namespace("n").unwrap();
    let fullest = ns
        .shards
        .iter()
        .map(|s| super::super::lock(s).expiry.len())
        .max()
        .unwrap();
    assert!(fullest > SCAN_BATCH, "a shard holds more than one batch");
    assert_eq!(r.store.counts("n").unwrap().expiry_backlog, 0);
    r.clock.fetch_add(1_000, Relaxed);
    let due = (0..KEYS).filter(|i| i % 3 == 0).count() as u64;
    assert_eq!(r.store.counts("n").unwrap().expiry_backlog, due);
    r.clock.fetch_add(2_000, Relaxed);
    let counts = r.store.counts("n").unwrap();
    assert_eq!(
        (counts.expiry_backlog, counts.keys),
        (KEYS as u64, KEYS as u64)
    );
    assert_eq!(r.listing(&all(), &Limits::default()).0.len(), 0);
}

/// Calls are counted by operation and by how they ended; a refusal by its reason.
#[test]
fn calls_are_counted_by_operation_and_result() {
    let r = rig_with(&NamespaceSettings {
        max_keys: 2,
        default_ttl_secs: None,
        ..NamespaceSettings::defaults(CEILING)
    });
    let op = |name: &str| OPS.iter().position(|o| *o == name).unwrap();
    let reason = |name: &str| RESULTS[1..].iter().position(|r| *r == name).unwrap();
    let one = Value::Int(1);
    r.set(b"a", None);
    r.set(b"a", None);
    let incr = |key: &[u8]| {
        r.ns.call(&StateOp::Incr {
            key,
            by: 1,
            ttl: None,
        })
    };
    incr(b"n").unwrap();
    // A third key: the namespace is full.
    assert_eq!(incr(b"m").unwrap_err().reason, "namespace_full");
    assert_eq!(incr(b"m").unwrap_err().reason, "namespace_full");
    // Another type's operation.
    assert_eq!(incr(b"a").unwrap_err().reason, "wrong_type");
    for (largest, _) in [(true, "max"), (false, "min")] {
        let _ = r.ns.call(&StateOp::Extreme {
            key: b"a",
            value: stored(&one),
            largest,
            ttl: None,
        });
    }
    r.ns.call(&StateOp::Get { key: b"a" }).unwrap();
    r.ns.call(&StateOp::Get { key: b"missing" }).unwrap();
    r.ns.call(&StateOp::Ttl { key: b"a" }).unwrap();
    r.ns.call(&StateOp::HllCount { key: b"missing" }).unwrap();
    r.del(b"n");
    r.store.set_brownout(true);
    assert_eq!(incr(b"m").unwrap_err().reason, "brownout");
    r.store.set_brownout(false);
    let counts = r.store.counts("n").unwrap();
    let mut ok = [0; OPS.len()];
    for (name, n) in [
        ("set", 2),
        ("incr", 1),
        ("get", 2),
        ("ttl", 1),
        ("hll_count", 1),
        ("del", 1),
    ] {
        ok[op(name)] = n;
    }
    assert_eq!(counts.ok, ok);
    let mut failed = [[0; REFUSALS]; OPS.len()];
    failed[op("incr")][reason("namespace_full")] = 2;
    failed[op("incr")][reason("wrong_type")] = 1;
    failed[op("incr")][reason("brownout")] = 1;
    failed[op("max")][reason("wrong_type")] = 1;
    failed[op("min")][reason("wrong_type")] = 1;
    assert_eq!(counts.failed, failed);
    assert_eq!(counts.calls_ok(), 8);
    let mut by_reason = [0; REFUSALS];
    by_reason[reason("namespace_full")] = 2;
    by_reason[reason("wrong_type")] = 3;
    by_reason[reason("brownout")] = 1;
    assert_eq!(counts.refused(), by_reason);
    assert_eq!(
        (counts.keys, counts.bytes),
        (1, (ENTRY_OVERHEAD + 2) as u64)
    );
    // What `stats` said before says the same.
    let stats = r.store.stats("n").unwrap();
    assert_eq!(
        (stats.refused_namespace_full, stats.refused_wrong_type),
        (2, 3)
    );
}

/// An operator's delete removes a key of any type, says whether one was there, gives
/// its room back and is counted apart from the rules' own deletes.
#[test]
fn an_operators_delete_removes_a_key_of_any_type() {
    let r = rig();
    r.set(b"plain", None);
    r.ns.call(&StateOp::Incr {
        key: b"counter",
        by: 5,
        ttl: None,
    })
    .unwrap();
    let before = r.store.counts("n").unwrap();
    assert_eq!(r.store.delete_key("n", b"counter"), Ok(true));
    assert_eq!(r.store.delete_key("n", b"counter"), Ok(false));
    assert_eq!(r.store.delete_key("n", b"never-there"), Ok(false));
    assert!(r.store.key("n", b"counter").unwrap().is_none());
    assert_eq!(r.store.delete_key("n", b"plain"), Ok(true));
    let counts = r.store.counts("n").unwrap();
    assert_eq!((counts.keys, counts.bytes, counts.deleted), (0, 0, 2));
    assert_eq!(r.store.bytes(), 0);
    assert_eq!(counts.ok, before.ok, "not a call of a rule");
    // The key is free again, for any type.
    r.set(b"counter", None);
    assert_eq!(
        r.store.key("n", b"counter").unwrap().unwrap().type_name,
        "lww"
    );
}

/// The store's totals: what its namespaces and the connections' dictionaries hold
/// against the ceiling, and how often a dictionary was refused room.
#[test]
fn the_totals_tell_namespaces_from_dictionaries() {
    let store = Store::new(1_000);
    let ns = store
        .open("n", &NamespaceSettings::defaults(1_000))
        .unwrap();
    ns.call(&StateOp::Set {
        key: b"k",
        value: stored(&Value::Int(1)),
        ttl: None,
    })
    .unwrap();
    let held = (ENTRY_OVERHEAD + 2) as u64;
    store.dictionary_grow(300).unwrap();
    store.dictionary_grow(200).unwrap();
    store.dictionary_shrink(100);
    assert_eq!(
        store.dictionary_grow(1_000).unwrap_err().reason,
        "store_full"
    );
    store.set_brownout(true);
    assert_eq!(store.dictionary_grow(1).unwrap_err().reason, "brownout");
    let totals = store.totals();
    assert_eq!(
        (
            totals.bytes,
            totals.max_bytes,
            totals.brownout,
            totals.dictionary_bytes,
            totals.dictionary_refused
        ),
        (held + 400, 1_000, true, 400, 2)
    );
    assert_eq!(store.counts("n").unwrap().bytes, held);
}

// -- what a listing says of a key, and what it selects by (T12) -------------------------

/// The start of the rig's clock.
const START_MS: u64 = 1_800_000_000_000;

fn within(secs: u64) -> Selector<'static> {
    all().expiring(ByExpiry::Within(Duration::from_secs(secs)))
}

fn never() -> Selector<'static> {
    all().expiring(ByExpiry::Never)
}

impl Rig {
    /// Every page of a listing: each key as it was listed.
    fn infos(&self, selector: &Selector<'_>, limits: &Limits) -> Vec<KeyInfo> {
        let (mut keys, mut from) = (Vec::new(), None);
        for _ in 0..100_000 {
            let page = self.page(selector, from.as_ref(), limits);
            assert!(page.keys.len() <= limits.keys);
            keys.extend(page.keys);
            match page.next {
                Some(next) => from = Some(next),
                None => return keys,
            }
        }
        panic!("the listing does not end");
    }

    /// The keys a listing selects, sorted, after checking that none came twice.
    fn selected(&self, selector: &Selector<'_>, limits: &Limits) -> Vec<String> {
        let (keys, _) = self.listing(selector, limits);
        let distinct: BTreeSet<String> = keys
            .iter()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect();
        assert_eq!(distinct.len(), keys.len(), "no key is listed twice");
        distinct.into_iter().collect()
    }

    fn advance(&self, secs: u64) {
        self.clock.fetch_add(secs * 1000, Relaxed);
    }
}

/// What a value takes, for a test: the length of its JSON.
fn json_len(value: &Value) -> usize {
    value.to_json().map_or(0, |text| text.len())
}

fn with_values(room: usize, keys: usize) -> Limits {
    Limits {
        keys,
        values: Some(Values {
            room,
            size: json_len,
        }),
        ..Limits::default()
    }
}

/// A listing says of each key its type, what the namespace accounts it as, and when
/// it expires: all of it in the entry already. Nothing was added to an entry for it:
/// a key still costs its key, its value and 128 bytes, and the sizes listed add up to
/// what the namespace counts.
#[test]
fn a_listing_says_each_keys_type_size_and_expiry() {
    let r = rig();
    r.set(b"plain", None);
    r.set(b"short", Some(90));
    r.ns.call(&StateOp::Incr {
        key: b"counter",
        by: 5,
        ttl: Some(3600),
    })
    .unwrap();
    r.ns.call(&StateOp::Add {
        key: b"members",
        identity: b"dev-1",
        element: stored(&Value::from("dev-1")),
        ttl: None,
    })
    .unwrap();
    let listed = r.infos(&all(), &limit(10));
    let by_key: BTreeMap<&[u8], &KeyInfo> = listed.iter().map(|k| (&*k.key, k)).collect();
    let shown = |key: &[u8]| {
        let k = by_key[key];
        assert!(k.value.is_none(), "no value unless values are asked for");
        (k.type_name, k.expires_ms)
    };
    assert_eq!(shown(b"plain"), ("lww", 0));
    assert_eq!(shown(b"short"), ("lww", START_MS + 90_000));
    assert_eq!(shown(b"counter"), ("sum", START_MS + 3_600_000));
    assert_eq!(shown(b"members"), ("union", 0));
    assert_eq!(ENTRY_OVERHEAD, 128);
    assert!(by_key[&b"plain"[..]].bytes > ENTRY_OVERHEAD + b"plain".len());
    assert!(by_key[&b"members"[..]].bytes > by_key[&b"plain"[..]].bytes);
    let counts = r.store.counts("n").unwrap();
    assert_eq!(
        listed.iter().map(|k| k.bytes as u64).sum::<u64>(),
        counts.bytes,
        "the sizes listed are what the namespace accounts"
    );
    // What a listing says is what a read of the key says.
    for k in &listed {
        let view = r.store.key("n", &k.key).unwrap().unwrap();
        assert_eq!(
            (view.type_name, view.expires_ms, view.bytes),
            (k.type_name, k.expires_ms, k.bytes)
        );
    }
}

/// Each filter alone, and combined with the others and a prefix, selects exactly the
/// keys written that way, on a clock the test drives: `Within` the keys that expire
/// within that long of now (the last millisecond included, and not one that has
/// expired), `Never` the keys with no expiry, both only among the prefix's keys. However
/// the listing is paged.
#[test]
fn the_filters_select_exactly_the_keys_written_that_way() {
    let r = rig();
    // Per prefix: 40 keys that never expire, 40 that expire in a minute, 40 in an
    // hour, 40 in a day.
    let ttls = [(None, "never"), (Some(60), "min"), (Some(3600), "hour")];
    let ttls = ttls.iter().chain(&[(Some(86_400), "day")]);
    let mut model: BTreeMap<String, u64> = BTreeMap::new();
    for (ttl, kind) in ttls {
        for prefix in ["a", "b"] {
            for i in 0..40 {
                let key = format!("{prefix}:{kind}:{i:02}");
                r.set(key.as_bytes(), *ttl);
                model.insert(key, ttl.map_or(0, |secs| START_MS + secs * 1000));
            }
        }
    }
    let expected = |prefix: &str, by: ByExpiry, now_ms: u64| -> Vec<String> {
        model
            .iter()
            .filter(|(key, expires_ms)| {
                let live = **expires_ms == 0 || **expires_ms > now_ms;
                let selected = match by {
                    ByExpiry::Any => true,
                    ByExpiry::Never => **expires_ms == 0,
                    ByExpiry::Within(d) => {
                        let within = u64::try_from(d.as_millis()).unwrap();
                        **expires_ms != 0 && **expires_ms <= now_ms + within
                    }
                };
                live && selected && key.starts_with(prefix)
            })
            .map(|(key, _)| key.clone())
            .collect()
    };
    let hurried = Limits {
        time: Duration::ZERO,
        ..limit(7)
    };
    let check = |now_ms: u64| {
        for by in [
            ByExpiry::Any,
            ByExpiry::Never,
            ByExpiry::Within(Duration::ZERO),
            ByExpiry::Within(Duration::from_secs(59)),
            ByExpiry::Within(Duration::from_mins(1)),
            ByExpiry::Within(Duration::from_mins(10)),
            ByExpiry::Within(Duration::from_hours(1)),
            ByExpiry::Within(Duration::from_hours(30 * 24)),
        ] {
            for prefix in ["", "a:", "b:hour", "nope"] {
                let selector = Selector::prefix(prefix.as_bytes()).expiring(by);
                let want = expected(prefix, by, now_ms);
                for limits in [limit(1000), limit(7), hurried] {
                    assert_eq!(
                        r.selected(&selector, &limits),
                        want,
                        "{by:?} {prefix:?} {limits:?}"
                    );
                }
            }
        }
    };
    check(START_MS);
    // Exactly the keys of a minute within a minute, of an hour within an hour.
    assert_eq!(r.selected(&within(59), &limit(100)).len(), 0);
    assert_eq!(r.selected(&within(60), &limit(100)).len(), 80);
    assert_eq!(r.selected(&within(3600), &limit(100)).len(), 160);
    assert_eq!(r.selected(&never(), &limit(100)).len(), 80);
    // A millisecond before the minute is up they are within anything; then they have
    // expired, and are within nothing.
    r.clock.fetch_add(59_999, Relaxed);
    check(START_MS + 59_999);
    assert_eq!(
        r.selected(&within(0), &limit(100)).len(),
        0,
        "within no time"
    );
    assert_eq!(r.selected(&within(1), &limit(100)).len(), 80);
    r.clock.fetch_add(1, Relaxed);
    check(START_MS + 60_000);
    assert_eq!(r.selected(&within(1), &limit(100)).len(), 0);
    // Later the hour's keys come within ten minutes.
    r.advance(3_000);
    check(START_MS + 3_060_000);
    assert_eq!(r.selected(&within(600), &limit(100)).len(), 80);
    // A listing removes nothing: the expired keys are still held.
    assert_eq!(r.store.counts("n").unwrap().keys, 320);
    // With a rule on the expiry event, a call that finds its key expired holds it for
    // the event: out of the map, still counted, and listed by no filter.
    r.store.set_expiry_events(true);
    for i in 0..40 {
        let key = format!("a:min:{i:02}");
        assert!(r
            .ns
            .call(&StateOp::Get {
                key: key.as_bytes()
            })
            .unwrap()
            .value
            .is_none());
    }
    assert_eq!(r.store.counts("n").unwrap().keys, 320);
    check(START_MS + 3_060_000);
}

/// EMQX's table is listed like any namespace. Its keys never expire, so `Never`
/// selects every one of them and `Within` none; a prefix there is of a string key's
/// text, and combines.
#[test]
fn emqxs_table_is_listed_with_sizes_and_every_key_has_no_expiry() {
    let clock = Arc::new(AtomicU64::new(START_MS));
    let store = Store::with_manual_clock(CEILING, clock);
    let ns = store
        .open(
            mqtt_rules::state::KV_STORE_NAMESPACE,
            &NamespaceSettings {
                default_ttl_secs: None,
                ..NamespaceSettings::defaults(CEILING)
            },
        )
        .unwrap();
    let terms = [
        Value::from("dev-1"),
        Value::from("dev-2"),
        Value::Int(1),
        Value::from(vec![Value::from("dev-3")]),
    ];
    for term in &terms {
        ns.call(&StateOp::Set {
            key: &term_key(term),
            value: stored(&Value::from("held")),
            ttl: None,
        })
        .unwrap();
    }
    let list = |selector: Selector<'_>| {
        store
            .keys(
                mqtt_rules::state::KV_STORE_NAMESPACE,
                &selector,
                None,
                &with_values(1 << 20, 10),
            )
            .unwrap()
            .keys
    };
    let all = list(Selector::string_prefix(b""));
    assert_eq!(all.len(), 4);
    for k in &all {
        assert_eq!((k.type_name, k.expires_ms), ("lww", 0));
        assert!(k.bytes > ENTRY_OVERHEAD);
        assert_eq!(k.value.as_ref().unwrap().to_json().unwrap(), "\"held\"");
    }
    assert_eq!(
        list(Selector::string_prefix(b"").expiring(ByExpiry::Never)).len(),
        4
    );
    let day = ByExpiry::Within(Duration::from_hours(24));
    assert_eq!(list(Selector::string_prefix(b"").expiring(day)).len(), 0);
    assert_eq!(
        list(Selector::string_prefix(b"dev-").expiring(ByExpiry::Never)).len(),
        2
    );
}

/// A page with values carries each key's value as a read of it does, and ends where
/// its values pass their room: such a page is short and says where the next starts,
/// one value larger than the room is a page of its own, and the listing is whole all
/// the same. Without `values` a page carries none.
#[test]
fn a_page_with_values_ends_where_its_values_pass_their_room() {
    let r = rig();
    let text = "x".repeat(98);
    for i in 0..300 {
        r.ns.call(&StateOp::Set {
            key: &name(i),
            value: stored(&Value::from(text.as_str())),
            ttl: Some(60),
        })
        .unwrap();
    }
    let large = "y".repeat(3_000);
    r.ns.call(&StateOp::Set {
        key: b"large",
        value: stored(&Value::from(large.as_str())),
        ttl: None,
    })
    .unwrap();
    let whole = r.listing(&all(), &limit(1000)).0;
    assert_eq!(whole.len(), 301);
    assert!(r
        .infos(&all(), &limit(1000))
        .iter()
        .all(|k| k.value.is_none()));
    // Each value is 100 bytes as JSON: 1,000 bytes of room is ten keys a page, until
    // the large one, which has a page to itself.
    let limits = with_values(1_000, 1000);
    let (mut pages, mut from, mut keys) = (Vec::new(), None, Vec::new());
    loop {
        let page = r.page(&all(), from.as_ref(), &limits);
        let sizes: Vec<usize> = page
            .keys
            .iter()
            .map(|k| json_len(k.value.as_ref().expect("a value for every key")))
            .collect();
        assert!(
            sizes.iter().sum::<usize>() <= 1_000 || sizes.len() == 1,
            "{sizes:?}"
        );
        for k in &page.keys {
            let view = r.store.key("n", &k.key).unwrap().unwrap();
            assert_eq!(
                k.value.as_ref().unwrap().to_json().unwrap(),
                view.value.to_json().unwrap()
            );
            assert_eq!(
                (k.type_name, k.bytes, k.expires_ms),
                (view.type_name, view.bytes, view.expires_ms)
            );
        }
        keys.extend(page.keys.iter().map(|k| k.key.to_vec()));
        pages.push(sizes);
        match page.next {
            Some(next) => from = Some(next),
            None => break,
        }
        assert!(pages.len() < 1_000, "the listing does not end");
    }
    assert_eq!(keys, whole, "every key once, in the listing's order");
    assert!(pages.contains(&vec![3_002]), "the large value, alone");
    assert!(pages.iter().filter(|sizes| sizes.len() == 10).count() >= 20);
    assert!(pages.iter().all(|sizes| sizes.len() <= 10));
    // Room for everything: the bound on keys is what ends a page.
    let roomy = with_values(1 << 20, 50);
    assert_eq!(r.page(&all(), None, &roomy).keys.len(), 50);
    assert_eq!(r.listing(&all(), &roomy).0, whole);
    // With a filter: the values of the keys selected, and of no other.
    let selected = r.infos(&never(), &with_values(1 << 20, 50));
    assert_eq!(selected.len(), 1);
    assert_eq!(json_len(selected[0].value.as_ref().unwrap()), 3_002);
}

/// What a change of expiry does to a filtered listing that is under way, page by
/// page. A key that is selected from the first page to the last is listed exactly
/// once. One whose expiry changes meanwhile is judged when the listing reaches it:
/// after the cursor it is listed if it is selected then, before the cursor it stays
/// as it was listed or not listed. No key is listed twice.
#[test]
fn a_key_whose_expiry_changes_during_a_listing_is_judged_where_the_listing_reaches_it() {
    for by in [ByExpiry::Within(Duration::from_mins(10)), ByExpiry::Never] {
        let r = rig();
        // What `by` selects, and what it does not.
        let (inside, outside) = match by {
            ByExpiry::Never => (None, Some(60)),
            _ => (Some(60), None),
        };
        for i in 0..1_000 {
            r.set(&name(i), inside);
            r.set(format!("out:{i:04}").as_bytes(), outside);
        }
        let selector = all().expiring(by);
        let first = r.page(&selector, None, &limit(300));
        assert_eq!(first.keys.len(), 300);
        let cursor = first.next.clone().unwrap();
        let at = (cursor.shard, cursor.after.clone());
        let mut listed: Vec<Vec<u8>> = first.keys.iter().map(|k| k.key.to_vec()).collect();
        // Meanwhile every fourth selected key stops being selected, and every fourth
        // of the others starts. (A key is given another expiry by deleting it and
        // writing it again where the new one is none: a write without a TTL keeps
        // the expiry a key has.)
        let change = |key: &[u8], ttl: Option<u64>| {
            r.del(key);
            r.set(key, ttl);
        };
        for i in (0..1_000).step_by(4) {
            change(&name(i), outside);
            change(format!("out:{i:04}").as_bytes(), inside);
        }
        // And some stay selected with another expiry: still listed exactly once.
        if inside.is_some() {
            for i in (1..1_000).step_by(4) {
                r.set(&name(i), Some(120));
            }
        }
        let mut from = Some(cursor);
        while let Some(cursor) = from {
            let page = r.page(&selector, Some(&cursor), &limit(300));
            listed.extend(page.keys.iter().map(|k| k.key.to_vec()));
            from = page.next;
        }
        let seen: BTreeSet<&Vec<u8>> = listed.iter().collect();
        assert_eq!(seen.len(), listed.len(), "{by:?}: no key is listed twice");
        let (mut started_after, mut stopped_before) = (0, 0);
        for i in 0..1_000 {
            let (key, other) = (name(i), format!("out:{i:04}").into_bytes());
            if i % 4 != 0 {
                assert!(seen.contains(&key), "{by:?}: {i} stayed selected");
                assert!(!seen.contains(&other), "{by:?}: {i} was never selected");
                continue;
            }
            // Stopped being selected: listed only if the first page had it.
            let before = r.place(&key) <= at;
            assert_eq!(seen.contains(&key), before, "{by:?}: {i} stopped");
            stopped_before += usize::from(before);
            // Started being selected: listed only where the listing had yet to come.
            let before = r.place(&other) <= at;
            assert_eq!(seen.contains(&other), !before, "{by:?}: {i} started");
            started_after += usize::from(!before);
        }
        assert!(
            started_after > 0 && stopped_before > 0,
            "both cases were tried"
        );
    }
}

/// The same with the changes on other threads while filtered listings run: keys that
/// stay selected are each listed exactly once in every listing, keys that are never
/// selected never, and a key whose expiry is changing may or may not be listed, and
/// never twice. `Within` walks the order of expiry and `Never` the map; both keep
/// the promise while keys come and go around them.
#[test]
fn filtered_listings_stay_exact_for_keys_that_stay_selected_while_others_change() {
    const WRITERS: usize = 4;
    const LISTINGS: usize = 12;
    let r = rig();
    let soon: BTreeSet<Vec<u8>> = (0..1_500).map(name).collect();
    let fixed: BTreeSet<Vec<u8>> = (0..1_500)
        .map(|i| format!("fixed:{i:04}").into_bytes())
        .collect();
    for key in &soon {
        r.set(key, Some(60));
    }
    for key in &fixed {
        r.set(key, None);
    }
    for i in 0..500 {
        r.set(format!("late:{i}").as_bytes(), Some(86_400));
    }
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        for t in 0..WRITERS {
            let (ns, stop) = (r.ns.clone(), &stop);
            scope.spawn(move || {
                let mut i = 0usize;
                while !stop.load(Relaxed) {
                    // Keys that come and go, with and without an expiry, and keys
                    // whose expiry moves in and out of what a listing selects.
                    let churn = format!("churn:{t}:{}", i % 300).into_bytes();
                    let ttl = [None, Some(30), Some(7_200)][i % 3];
                    let _ = ns.call(&StateOp::Set {
                        key: &churn,
                        value: stored(&Value::Int(1)),
                        ttl,
                    });
                    if i.is_multiple_of(2) {
                        let _ = ns.call(&StateOp::Del { key: &churn });
                    }
                    let moving = format!("moving:{t}:{}", i % 100).into_bytes();
                    let _ = ns.call(&StateOp::Set {
                        key: &moving,
                        value: stored(&Value::Int(1)),
                        ttl: Some([20, 40, 7_200][i % 3]),
                    });
                    i += 1;
                }
            });
        }
        let other = |k: &Vec<u8>| k.starts_with(b"churn:") || k.starts_with(b"moving:");
        for round in 0..LISTINGS {
            let limits = limit([250, 1000, 33][round % 3]);
            let (keys, _) = r.listing(&within(600), &limits);
            let seen: BTreeSet<&Vec<u8>> = keys.iter().collect();
            assert_eq!(seen.len(), keys.len(), "no key is listed twice");
            assert!(soon.iter().all(|k| seen.contains(k)));
            assert!(keys.iter().all(|k| soon.contains(k) || other(k)));
            let (keys, _) = r.listing(&never(), &limits);
            let seen: BTreeSet<&Vec<u8>> = keys.iter().collect();
            assert_eq!(seen.len(), keys.len(), "no key is listed twice");
            assert!(fixed.iter().all(|k| seen.contains(k)));
            assert!(keys
                .iter()
                .all(|k| fixed.contains(k) || k.starts_with(b"churn:")));
        }
        stop.store(true, Relaxed);
    });
}

/// A `Within` listing of a `local` namespace walks the shard's order of expiry, not
/// its map, and selects what a walk of the map would: among many keys that expire
/// later, across more keys of one second than one hold looks at, in pages of any
/// size, and while the map of every shard changes all the time.
#[test]
fn a_listing_by_expiry_walks_the_order_of_expiry() {
    let r = rig();
    for i in 0..20_000 {
        r.set(&name(i), Some(86_400));
    }
    for i in 0..50 {
        r.set(format!("due:{i:02}").as_bytes(), Some(30));
    }
    // More keys of one second than one hold looks at: the walk goes on from where a
    // hold ended, within the second.
    for i in 0..(3 * WALK_BATCH + 5) {
        r.set(format!("burst:{i:03}").as_bytes(), Some(45));
    }
    let by_order = r.selected(&within(60), &limit(1000));
    assert_eq!(by_order.len(), 50 + 3 * WALK_BATCH + 5);
    // What the map's walk selects, judged the same way.
    let by_map: Vec<String> = r
        .infos(&all(), &limit(1000))
        .iter()
        .filter(|k| k.expires_ms != 0 && k.expires_ms <= START_MS + 60_000)
        .map(|k| String::from_utf8(k.key.to_vec()).unwrap())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(by_order, by_map);
    // Small pages go through the same keys, in the listing's order.
    let (paged, _) = r.listing(&within(60), &limit(3));
    let places: Vec<(usize, Vec<u8>)> = paged.iter().map(|k| r.place(k)).collect();
    assert!(places.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(paged.len(), by_order.len());
    // The map of every shard changes between any two holds: a walk of the map would
    // start over, the order's walk has nothing to start over.
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let (ns, stop) = (r.ns.clone(), &stop);
        scope.spawn(move || {
            let mut i = 0usize;
            while !stop.load(Relaxed) {
                let key = format!("churn:{}", i % 5_000).into_bytes();
                let _ = ns.call(&StateOp::Set {
                    key: &key,
                    value: stored(&Value::Int(1)),
                    ttl: None,
                });
                let _ = ns.call(&StateOp::Del { key: &key });
                i += 1;
            }
        });
        for _ in 0..20 {
            assert_eq!(r.selected(&within(60), &limit(1000)), by_order);
        }
        stop.store(true, Relaxed);
    });
}
