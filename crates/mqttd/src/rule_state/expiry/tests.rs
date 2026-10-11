//! The sweep and the held keys (ADR 0085 D7), on a clock the tests drive.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use mqtt_rules::state::{
    Eviction, NamespaceSettings, StateNamespace, StateOp, StateStore, NAMESPACE_FULL,
};
use mqtt_rules::Value;

use super::super::{stored, Store};
use super::Pace;

const START_MS: u64 = 1_800_000_000_000;

fn store() -> (Arc<Store>, Arc<AtomicU64>) {
    let clock = Arc::new(AtomicU64::new(START_MS));
    (Store::with_manual_clock(256 << 20, clock.clone()), clock)
}

fn settings(store: &Store) -> NamespaceSettings {
    NamespaceSettings::defaults(store.max_bytes())
}

fn open(store: &Store, name: &str, settings: &NamespaceSettings) -> Arc<dyn StateNamespace> {
    store.open(name, settings).unwrap()
}

fn set(ns: &dyn StateNamespace, key: &str, value: i64, ttl: u64) {
    ns.call(&StateOp::Set {
        key: key.as_bytes(),
        value: stored(&Value::Int(value)),
        ttl: Some(ttl),
    })
    .unwrap();
}

/// What `kv_get` reads, as JSON.
fn get(ns: &dyn StateNamespace, key: &str) -> Option<String> {
    ns.call(&StateOp::Get {
        key: key.as_bytes(),
    })
    .unwrap()
    .value
    .map(|v| json(&v))
}

fn json(v: &Value) -> String {
    v.to_json().unwrap()
}

fn keys(store: &Store, namespace: &str) -> usize {
    store.stats(namespace).unwrap().keys
}

fn text(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

/// D7 with no rule on the event: the sweep removes what has expired, returns no
/// event, and says when the next key is due. A key not yet due stays.
#[test]
fn without_a_rule_on_the_event_the_sweep_only_removes() {
    let (store, clock) = store();
    let ns = open(&store, "a", &settings(&store));
    set(&*ns, "soon", 1, 10);
    set(&*ns, "later", 2, 60);
    assert_eq!(store.sweep(100).removed, 0, "nothing is due yet");
    clock.store(START_MS + 10_000, Relaxed);
    let swept = store.sweep(100);
    assert_eq!(
        (swept.removed, swept.expired.len(), swept.more),
        (1, 0, false)
    );
    assert_eq!(swept.next_due_ms, Some(START_MS + 60_000));
    assert_eq!(store.counts("a").unwrap().expired, 1);
    assert_eq!(keys(&store, "a"), 1);
    assert_eq!(get(&*ns, "later"), Some("2".to_string()));
    // And a call that finds its key expired removes it itself, as before.
    clock.store(START_MS + 60_000, Relaxed);
    assert_eq!(get(&*ns, "later"), None);
    assert_eq!(keys(&store, "a"), 0);
    assert_eq!(store.counts("a").unwrap().expired, 2);
    assert_eq!(store.bytes(), 0);
}

/// D7: with a rule on the event, a key past its expiry reads as absent at once and
/// is held, still counted, until the sweep returns it with the value it held, its
/// type and when it expired.
#[test]
fn an_expired_key_reads_as_absent_at_once_and_is_held_until_the_sweep_returns_it() {
    let (store, clock) = store();
    store.set_expiry_events(true);
    let ns = open(&store, "seen", &settings(&store));
    set(&*ns, "d1", 7, 10);
    clock.store(START_MS + 10_000, Relaxed);
    assert_eq!(get(&*ns, "d1"), None, "absent from the moment it expires");
    assert_eq!(keys(&store, "seen"), 1, "held for its event");
    assert!(store.held().is_empty(), "and not listed");
    // D10: it is in the backlog, and counted as expired once it is removed.
    let counts = store.counts("seen").unwrap();
    assert_eq!((counts.expired, counts.expiry_backlog), (0, 1));
    let swept = store.sweep(100);
    assert_eq!(swept.removed, 0);
    let [expired] = &swept.expired[..] else {
        panic!("one event: {swept:?}")
    };
    assert_eq!(&*expired.namespace, "seen");
    assert_eq!(text(&expired.key), "d1");
    assert_eq!(json(&expired.value), "7");
    assert_eq!(expired.kind, "lww");
    assert_eq!(expired.expired_ms, START_MS + 10_000);
    assert_eq!((keys(&store, "seen"), store.bytes()), (0, 0));
    assert!(store.sweep(100).expired.is_empty(), "once");
    let counts = store.counts("seen").unwrap();
    assert_eq!((counts.expired, counts.expiry_backlog), (1, 0));
}

/// D7: a write that finds its key expired creates it anew, and the expired one
/// still raises its event, with the value it held. Then the new one does.
#[test]
fn a_key_written_again_after_it_expired_still_raises_the_event_it_owed() {
    let (store, clock) = store();
    store.set_expiry_events(true);
    let ns = open(&store, "seen", &settings(&store));
    set(&*ns, "d1", 1, 10);
    clock.store(START_MS + 10_500, Relaxed);
    set(&*ns, "d1", 2, 10);
    assert_eq!(get(&*ns, "d1"), Some("2".to_string()));
    assert_eq!(keys(&store, "seen"), 2, "the new key and the one held");
    clock.store(START_MS + 11_000, Relaxed);
    let swept = store.sweep(100);
    assert_eq!(swept.expired.len(), 1);
    assert_eq!(json(&swept.expired[0].value), "1");
    assert_eq!(swept.expired[0].expired_ms, START_MS + 10_000);
    assert_eq!(
        get(&*ns, "d1"),
        Some("2".to_string()),
        "the new key is untouched"
    );
    clock.store(START_MS + 21_000, Relaxed);
    let swept = store.sweep(100);
    assert_eq!(swept.expired.len(), 1);
    assert_eq!(json(&swept.expired[0].value), "2");
    assert_eq!(keys(&store, "seen"), 0);
}

/// D7: a mass expiry is handed over at the configured rate, in order of expiry,
/// and none is shed. 5,000 keys in two namespaces expire over five seconds; at 1,000
/// a second, a tenth of a second's worth at a time, they take five seconds of sweeps
/// of at most 100, come out second by second, each exactly once, and until a key's
/// turn it reads as absent and keeps its room.
#[test]
fn a_mass_expiry_is_handed_over_at_the_rate_in_order_and_none_is_shed() {
    let (store, clock) = store();
    store.set_expiry_events(true);
    let a = open(&store, "a", &settings(&store));
    let b = open(&store, "b", &settings(&store));
    for i in 0..5000u64 {
        let ns = if i % 2 == 0 { &a } else { &b };
        set(
            &**ns,
            &format!("k{i}"),
            i64::try_from(i).unwrap(),
            10 + i % 5,
        );
    }
    // Every key has expired before the first sweep: the whole site fell silent.
    let mut now = START_MS + 20_000;
    clock.store(now, Relaxed);
    assert_eq!(get(&*a, "k0"), None);
    assert_eq!(keys(&store, "a") + keys(&store, "b"), 5000);
    let mut pace = Pace::new(1000);
    let mut seen = std::collections::HashSet::new();
    let mut last_ms = 0;
    let mut steps = 0;
    loop {
        let allowed = pace.allow(now);
        assert!(allowed <= 100, "a tenth of a second's worth: {allowed}");
        let swept = store.sweep(allowed);
        pace.spend(swept.expired.len());
        assert!(swept.expired.len() <= allowed);
        for expired in &swept.expired {
            assert!(expired.expired_ms >= last_ms, "in order of expiry");
            last_ms = expired.expired_ms;
            assert!(seen.insert((expired.namespace.clone(), expired.key.clone())));
        }
        assert_eq!(
            keys(&store, "a") + keys(&store, "b"),
            5000 - seen.len(),
            "a key keeps its room until its event is handed over"
        );
        if !swept.more {
            break;
        }
        steps += 1;
        now += pace.wait_ms();
        clock.store(now, Relaxed);
    }
    assert_eq!(seen.len(), 5000, "none shed");
    assert_eq!(steps, 49, "5,000 events at 1,000 a second, 100 at a time");
    assert_eq!(now - (START_MS + 20_000), 4900);
    assert_eq!(store.bytes(), 0);
}

/// D7: the backlog is bounded by the namespace's own bounds. Expired keys whose
/// events are owed are not room: a full namespace refuses a new key until the sweep
/// has handed some over.
#[test]
fn expired_keys_are_not_room_while_their_events_are_owed() {
    let (store, clock) = store();
    let two = NamespaceSettings {
        max_keys: 2,
        ..settings(&store)
    };
    let ns = open(&store, "small", &two);
    let new_key = |key: &str| {
        ns.call(&StateOp::Set {
            key: key.as_bytes(),
            value: stored(&Value::Int(0)),
            ttl: Some(10),
        })
    };
    new_key("a").unwrap();
    new_key("b").unwrap();
    clock.store(START_MS + 10_000, Relaxed);
    store.set_expiry_events(true);
    let refused = new_key("c").unwrap_err();
    assert_eq!(refused.reason, NAMESPACE_FULL);
    assert_eq!(store.sweep(1).expired.len(), 1);
    new_key("c").unwrap();
    assert_eq!(new_key("d").unwrap_err().reason, NAMESPACE_FULL);
    // Without a rule on the event the same keys are room at once.
    store.set_expiry_events(false);
    new_key("d").unwrap();
}

/// D7: an `lru` namespace evicts a key that has not expired; one that has is held
/// for its event instead. An eviction is not an expiry and raises nothing.
#[test]
fn an_lru_namespace_holds_an_expired_key_for_its_event_and_evicts_a_live_one() {
    let (store, clock) = store();
    store.set_expiry_events(true);
    let lru = NamespaceSettings {
        max_keys: 3,
        eviction: Eviction::Lru,
        ..settings(&store)
    };
    let ns = open(&store, "cache", &lru);
    set(&*ns, "old", 1, 10);
    set(&*ns, "live", 2, 600);
    clock.store(START_MS + 10_000, Relaxed);
    set(&*ns, "new", 3, 600);
    // Full: `old` has expired and is the least recently used; `live` is evicted.
    set(&*ns, "more", 4, 600);
    assert_eq!(get(&*ns, "live"), None);
    assert_eq!(get(&*ns, "new"), Some("3".to_string()));
    assert_eq!(get(&*ns, "more"), Some("4".to_string()));
    let swept = store.sweep(100);
    let raised: Vec<String> = swept.expired.iter().map(|e| text(&e.key)).collect();
    assert_eq!(
        raised,
        ["old"],
        "the expired key's event, none for the eviction"
    );
}

/// D7: when no rule selects the event any more, what was held is simply removed.
#[test]
fn held_keys_are_removed_without_events_once_no_rule_selects_the_event() {
    let (store, clock) = store();
    store.set_expiry_events(true);
    let ns = open(&store, "a", &settings(&store));
    set(&*ns, "k", 1, 10);
    clock.store(START_MS + 10_000, Relaxed);
    assert_eq!(get(&*ns, "k"), None);
    store.set_expiry_events(false);
    let swept = store.sweep(100);
    assert_eq!((swept.removed, swept.expired.len()), (1, 0));
    assert_eq!(keys(&store, "a"), 0);
}

/// The purge's entry point (ADR 0085, amended 2026-10-11: `events=true`):
/// `expire_now` makes a key expire now, whatever expiry it had. It reads as absent
/// at once and the sweep raises its event with this moment as its expiry; without a
/// rule on the event it is removed. EMQX's table raises none.
#[test]
fn expire_now_raises_the_event_as_if_the_key_had_expired_at_that_moment() {
    let (store, clock) = store();
    store.set_expiry_events(true);
    let ns = open(&store, "a", &settings(&store));
    set(&*ns, "k", 5, 600);
    clock.store(START_MS + 2500, Relaxed);
    assert!(store.expire_now("a", b"k"));
    assert!(!store.expire_now("a", b"k"), "it is gone");
    assert!(!store.expire_now("a", b"never"));
    assert!(!store.expire_now("nowhere", b"k"));
    assert_eq!(get(&*ns, "k"), None);
    assert!(store.sweep(100).expired.is_empty(), "due with its second");
    clock.store(START_MS + 3000, Relaxed);
    let swept = store.sweep(100);
    assert_eq!(swept.expired.len(), 1);
    assert_eq!(json(&swept.expired[0].value), "5");
    assert_eq!(swept.expired[0].expired_ms, START_MS + 2500);
    assert!(
        store.sweep(100).next_due_ms.is_none(),
        "its old expiry is gone"
    );

    store.set_expiry_events(false);
    set(&*ns, "k", 6, 600);
    assert!(store.expire_now("a", b"k"));
    assert_eq!((keys(&store, "a"), store.bytes()), (0, 0));

    let table = open(
        &store,
        mqtt_rules::state::KV_STORE_NAMESPACE,
        &NamespaceSettings::kv_store(store.max_bytes()),
    );
    table
        .call(&StateOp::Set {
            key: b"k",
            value: stored(&Value::Int(1)),
            ttl: None,
        })
        .unwrap();
    assert!(!store.expire_now(mqtt_rules::state::KV_STORE_NAMESPACE, b"k"));
}

/// The pace: at most the rate a second, a tenth of a second's worth at a time and
/// one event at least, with nothing saved up over an idle time.
#[test]
fn the_pace_allows_a_tenth_of_a_seconds_events_at_a_time_and_saves_none_up() {
    let mut pace = Pace::new(1000);
    assert_eq!(pace.allow(0), 100, "the first step is a full one");
    pace.spend(100);
    assert_eq!(pace.allow(0), 0);
    assert_eq!(pace.wait_ms(), 100);
    assert_eq!(pace.allow(30), 30);
    pace.spend(30);
    assert_eq!(pace.allow(60_000), 100, "an idle minute saves nothing up");
    // One event a second: one at a time, a second apart.
    let mut slow = Pace::new(1);
    assert_eq!(slow.allow(0), 1);
    slow.spend(1);
    assert_eq!((slow.allow(999), slow.wait_ms()), (0, 1));
    assert_eq!(slow.allow(1000), 1);
    // A clock that steps back earns nothing and breaks nothing.
    let mut back = Pace::new(1000);
    let allowed = back.allow(5000);
    back.spend(allowed);
    assert_eq!(back.allow(1000), 0);
    assert_eq!(back.allow(5010), 10);
}
