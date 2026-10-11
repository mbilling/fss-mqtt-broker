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
use super::{Cursor, KeysError, Limits, Page, Selector, OPS, RESULTS, SCAN_BATCH};

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
        store.keys("n", &all(), None, &limit(10)),
        Err(KeysError::NoNamespace)
    );
    let _again = store.open("n", &settings).unwrap();
    assert_eq!(
        store.keys("n", &all(), Some(&cursor), &limit(10)),
        Err(KeysError::StaleCursor)
    );
    assert_eq!(
        store.keys("n", &all(), None, &limit(10)),
        Ok(Page::default())
    );
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
